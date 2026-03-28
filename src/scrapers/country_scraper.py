from __future__ import annotations

"""
Country scraper – discovers and scrapes ESN national-organisation pages
from accounts.esn.org, then upserts the results into the ``esn_countries``
Supabase table.

Extracted from the monolithic ``section_scraper.py`` to follow the modular
``BaseScraper`` pipeline architecture.
"""

import asyncio
import json
import logging
import random
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, Tag
from supabase import Client

from src.menu_scraper_funcs import (
    create_async_client,
    fetch_html_async,
    safe_text,
)
from src.scrapers.base_scraper import BaseScraper

logger: logging.Logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ACCOUNTS_BASE: str = "https://accounts.esn.org/"
ACCOUNTS_LIST_URL: str = urljoin(ACCOUNTS_BASE, "")

# ---------------------------------------------------------------------------
# Private helpers (ported from section_scraper.py)
# ---------------------------------------------------------------------------


def _clean_text(text: Optional[str]) -> str:
    """Strip newlines, tabs, and excess whitespace."""
    if text is None:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _accounts_absolute(href: Optional[str]) -> Optional[str]:
    """Convert a relative accounts.esn.org href to an absolute URL."""
    if not href:
        return None
    return urljoin(ACCOUNTS_BASE, href)


def _country_code_from_url(page_url: str) -> Optional[str]:
    """Extract the ISO-3166-1 alpha-2 country code from a /country/<CC> URL."""
    try:
        path = urlparse(page_url).path.strip("/")
        parts = [p for p in path.split("/") if p]
        if len(parts) >= 2 and parts[0].lower() == "country":
            return parts[1].upper()
        return None
    except (ValueError, IndexError):
        return None


def _social_key_from_label(label: str) -> Optional[str]:
    """Map a title / aria-label string to a canonical social-network key."""
    low = label.lower()
    if "facebook" in low:
        return "facebook"
    if "instagram" in low:
        return "instagram"
    if "twitter" in low:
        return "twitter"
    if re.search(r"\bx\b", label, flags=re.IGNORECASE):
        return "twitter"
    return None


def _collect_social_links(soup: BeautifulSoup) -> Dict[str, str]:
    """Scrape social-media links from the sidebar flex block."""
    out: Dict[str, str] = {}
    anchors: List[Tag] = []
    for sel in ("div.d-flex.my-3", 'div[class="__"]'):
        for block in soup.select(sel):
            anchors.extend(block.find_all("a", href=True))
    for a in anchors:
        label = (
            (a.get("title") or "") + " " + (a.get("aria-label") or "")
        ).strip()
        key = _social_key_from_label(label)
        if not key or key in out:
            continue
        href = a.get("href")
        if not href:
            continue
        abs_h = _accounts_absolute(href) or href
        out[key] = abs_h
    return out


def _find_website_href(soup: BeautifulSoup) -> str:
    """Return the href of the first anchor whose visible text is 'Website'."""
    for a in soup.find_all("a", href=True):
        if _clean_text(safe_text(a)) == "Website":
            return _accounts_absolute(a.get("href")) or ""
    return ""


def _parse_country_details(
    html: Optional[str],
    page_url: str,
) -> Dict[str, Any]:
    """Parse a single country page into a flat dict matching ``esn_countries``."""
    code = _country_code_from_url(page_url)
    record: Dict[str, Any] = {
        "country_code": code,
        "country_name": None,
        "url": page_url,
        "email": "",
        "website": "",
        "social_links": {},
    }

    if not html:
        return record

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return record

    try:
        h1 = soup.select_one("h1.page-header")
        name = _clean_text(safe_text(h1))
        if name:
            record["country_name"] = name
        if not record["country_name"]:
            title_tag = soup.find("title")
            raw = _clean_text(safe_text(title_tag))
            if raw:
                record["country_name"] = (
                    raw.split("|")[0].strip() if "|" in raw else raw
                )

        email_node = soup.select_one(
            "div.field--name-field-email div.field--item"
        )
        record["email"] = _clean_text(safe_text(email_node))
        record["website"] = _find_website_href(soup)
        record["social_links"] = _collect_social_links(soup)
    except (AttributeError, TypeError, KeyError):
        pass

    return record


# ---------------------------------------------------------------------------
# CountryScraper
# ---------------------------------------------------------------------------


class CountryScraper(BaseScraper):
    """Scrape ESN national-organisation pages and upsert into ``esn_countries``.

    Pipeline stages
    ---------------
    1. ``fetch_data``   – discover ``/country/<CC>`` URLs, then fetch each page.
    2. ``parse_data``   – extract structured records from the fetched HTML.
    3. ``upsert_to_db`` – upsert records into Supabase with conflict on ``country_code``.
    """

    # Concurrency knobs
    DEFAULT_CONCURRENCY: int = 3
    UPSERT_BATCH_SIZE: int = 50

    def __init__(
        self,
        *,
        concurrency: int = DEFAULT_CONCURRENCY,
        limit: int = 0,
    ) -> None:
        super().__init__(name="CountryScraper")
        self._concurrency = concurrency
        self._limit = limit  # 0 → no limit

    # ------------------------------------------------------------------
    # 1. fetch_data  (async)
    # ------------------------------------------------------------------

    async def fetch_data(self) -> List[Tuple[str, Optional[str]]]:
        """Discover country page URLs, then fetch their HTML.

        Returns:
            A list of ``(url, html | None)`` tuples.
        """
        semaphore = asyncio.Semaphore(self._concurrency)

        async with create_async_client(self._concurrency) as client:
            # --- discover country URLs from the index page ---
            urls = await self._discover_country_urls(client, semaphore)

            if self._limit > 0:
                urls = urls[: self._limit]

            self._logger.info(
                "Discovered %d country URLs (limit=%d).",
                len(urls),
                self._limit,
            )

            # --- fetch each country page sequentially with polite delay ---
            results: List[Tuple[str, Optional[str]]] = []
            total = len(urls)

            for index, url in enumerate(urls, start=1):
                self._logger.info(
                    "[%d/%d] Fetching country page %s …", index, total, url
                )
                if index > 1:
                    await asyncio.sleep(random.uniform(1.0, 2.5))
                try:
                    html = await fetch_html_async(client, url, semaphore)
                except Exception as exc:
                    self._logger.warning("Fetch error for %s: %s", url, exc)
                    html = None
                results.append((url, html))

        return results

    # ------------------------------------------------------------------
    # 2. parse_data
    # ------------------------------------------------------------------

    def parse_data(
        self,
        raw_data: List[Tuple[str, Optional[str]]],
    ) -> List[Dict[str, Any]]:
        """Parse each ``(url, html)`` tuple into a flat country record.

        Records whose HTML could not be fetched are still included
        (with ``None`` / empty defaults) so they are visible in logs.
        """
        records: List[Dict[str, Any]] = []

        for url, html in raw_data:
            try:
                record = _parse_country_details(html, url)
            except Exception as exc:
                self._logger.warning("Parse error for %s: %s", url, exc)
                record = _parse_country_details(None, url)
            records.append(record)

        return records

    # ------------------------------------------------------------------
    # 3. upsert_to_db
    # ------------------------------------------------------------------

    def upsert_to_db(
        self,
        supabase_client: Client,
        data: List[Dict[str, Any]],
    ) -> None:
        """Upsert country records into ``esn_countries``.

        Conflict resolution is on ``country_code``.  Every record receives
        a fresh ``last_scraped_at`` timestamp; the database trigger handles
        ``updated_at`` on actual data changes.
        """
        if not data:
            self._logger.info("No country records to upsert.")
            return

        now_iso = datetime.now(timezone.utc).isoformat()

        # Prepare rows: serialise social_links to JSON string if needed,
        # and stamp last_scraped_at.
        rows: List[Dict[str, Any]] = []
        for record in data:
            if not record.get("country_code"):
                self._logger.debug(
                    "Skipping record with no country_code: %s", record
                )
                continue
            row = {
                "country_code": record["country_code"],
                "country_name": record.get("country_name"),
                "url": record.get("url"),
                "email": record.get("email", ""),
                "website": record.get("website", ""),
                "social_links": (
                    record.get("social_links")
                    if isinstance(record.get("social_links"), dict)
                    else {}
                ),
                "last_scraped_at": now_iso,
            }
            rows.append(row)

        if not rows:
            self._logger.info("All records were filtered (no country_code).")
            return

        # Batch upsert
        table = supabase_client.table("esn_countries")
        for i in range(0, len(rows), self.UPSERT_BATCH_SIZE):
            batch = rows[i : i + self.UPSERT_BATCH_SIZE]
            self._logger.info(
                "Upserting batch %d–%d of %d …",
                i + 1,
                min(i + self.UPSERT_BATCH_SIZE, len(rows)),
                len(rows),
            )
            table.upsert(
                batch,
                on_conflict="country_code",
            ).execute()

        self._logger.info(
            "Successfully upserted %d country records.", len(rows)
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _discover_country_urls(
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
    ) -> List[str]:
        """Fetch the accounts.esn.org index page and extract ``/country/<CC>`` links."""
        try:
            html = await fetch_html_async(client, ACCOUNTS_LIST_URL, semaphore)
            if not html:
                return []

            soup = BeautifulSoup(html, "html.parser")
            seen: set[str] = set()
            urls: List[str] = []

            for a in soup.find_all("a", href=True):
                abs_url = _accounts_absolute(a.get("href"))
                if not abs_url:
                    continue
                path = urlparse(abs_url).path.rstrip("/")
                if not re.match(r"^/country/[^/]+$", path):
                    continue
                normalized = urljoin(ACCOUNTS_BASE, path.lstrip("/"))
                if normalized in seen:
                    continue
                seen.add(normalized)
                urls.append(normalized)

            return sorted(urls)
        except Exception as exc:
            logger.error("Country discovery error: %s", exc)
            return []
