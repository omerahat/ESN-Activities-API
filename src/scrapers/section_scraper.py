from __future__ import annotations

"""
Section scraper – discovers and scrapes ESN local-section pages from
accounts.esn.org, then upserts the results into the ``esn_sections``
Supabase table.

Extracted from the monolithic ``section_scraper.py`` to follow the modular
``BaseScraper`` pipeline architecture.
"""

import asyncio
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
# Private helpers (ported from the old section_scraper.py)
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


def _country_code_from_section_url(page_url: str) -> Optional[str]:
    """Extract the ISO-3166-1 alpha-2 code from ``/section/<CC>-<slug>`` URLs.

    Example
    -------
    ``https://accounts.esn.org/section/tr-esn-bilkent`` → ``"TR"``
    """
    try:
        path = urlparse(page_url).path.strip("/")
        parts = [p for p in path.split("/") if p]
        if "section" not in parts:
            return None
        idx = parts.index("section")
        if idx + 1 >= len(parts):
            return None
        slug = parts[idx + 1]
        token = slug.split("-")[0]
        return token.upper() if token else None
    except (ValueError, IndexError):
        return None


def _field_item_text(soup: BeautifulSoup, field_class: str) -> str:
    """Return the text of a ``div.<field_class> div.field--item`` node."""
    node = soup.select_one(f"div.{field_class} div.field--item")
    return _clean_text(safe_text(node))


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


def _parse_section_details(
    html: Optional[str],
    page_url: str,
) -> Dict[str, Any]:
    """Parse a single section page into a flat dict matching ``esn_sections``."""
    record: Dict[str, Any] = {
        "section_name": None,
        "country_code": _country_code_from_section_url(page_url),
        "city": "",
        "logo_url": None,
        "address": "",
        "university_name": "",
        "university_website": None,
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
        # --- Section name ---
        h1 = soup.select_one("h1.page-header")
        record["section_name"] = _clean_text(safe_text(h1)) or None
        record["country_code"] = _country_code_from_section_url(page_url)

        # --- City ---
        record["city"] = _field_item_text(soup, "field--name-field-city")

        # --- Logo ---
        logo = soup.select_one("div.group__field-pseudo-group-logo img")
        if logo and logo.get("src"):
            record["logo_url"] = _accounts_absolute(logo["src"])

        # --- Address ---
        addr_block = soup.select_one(
            "div.field--name-field-address div.field--item"
        )
        if addr_block:
            parts: List[str] = []
            for cls in (
                "address-line1",
                "address-line2",
                "postal-code",
                "locality",
                "country",
            ):
                span = addr_block.find("span", class_=cls)
                t = _clean_text(safe_text(span))
                if t:
                    parts.append(t)
            record["address"] = ", ".join(parts)

        # --- University ---
        record["university_name"] = _field_item_text(
            soup, "field--name-field-university-name"
        )
        uni_a = soup.select_one(
            "div.field--name-field-university-website a"
        )
        if uni_a and uni_a.get("href"):
            record["university_website"] = _accounts_absolute(uni_a["href"])

        # --- Contact ---
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
# SectionScraper
# ---------------------------------------------------------------------------


class SectionScraper(BaseScraper):
    """Scrape ESN local-section pages and upsert into ``esn_sections``.

    Pipeline stages
    ---------------
    1. ``fetch_data``   – discover section URLs from the accounts index,
       then fetch each section page.
    2. ``parse_data``   – extract structured records from the fetched HTML.
    3. ``upsert_to_db`` – upsert records into Supabase with conflict on
       ``section_name``.
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
        super().__init__(name="SectionScraper")
        self._concurrency = concurrency
        self._limit = limit  # 0 → no limit

    # ------------------------------------------------------------------
    # 1. fetch_data  (async)
    # ------------------------------------------------------------------

    async def fetch_data(self) -> List[Tuple[str, Optional[str]]]:
        """Discover section page URLs, then fetch their HTML.

        Returns:
            A list of ``(url, html | None)`` tuples.
        """
        semaphore = asyncio.Semaphore(self._concurrency)

        async with create_async_client(self._concurrency) as client:
            urls = await self._discover_section_urls(client, semaphore)

            if self._limit > 0:
                urls = urls[: self._limit]

            self._logger.info(
                "Discovered %d section URLs (limit=%d).",
                len(urls),
                self._limit,
            )

            results: List[Tuple[str, Optional[str]]] = []
            total = len(urls)

            for index, url in enumerate(urls, start=1):
                self._logger.info(
                    "[%d/%d] Fetching section page %s …", index, total, url
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
        """Parse each ``(url, html)`` tuple into a flat section record.

        Records whose HTML could not be fetched are still included (with
        ``None`` / empty defaults) so they are visible in logs.
        """
        records: List[Dict[str, Any]] = []

        for url, html in raw_data:
            try:
                record = _parse_section_details(html, url)
            except Exception as exc:
                self._logger.warning("Parse error for %s: %s", url, exc)
                record = _parse_section_details(None, url)
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
        """Upsert section records into ``esn_sections``.

        Conflict resolution is on ``section_name``.  Every record receives
        a fresh ``last_scraped_at`` timestamp; the database trigger handles
        ``updated_at`` on actual data changes.

        Records without a ``section_name`` are skipped (cannot satisfy the
        unique constraint).  The ``country_code`` is validated to be a
        2-character uppercase string matching the FK expectation.
        """
        if not data:
            self._logger.info("No section records to upsert.")
            return

        now_iso = datetime.now(timezone.utc).isoformat()

        rows: List[Dict[str, Any]] = []
        for record in data:
            section_name = record.get("section_name")
            if not section_name:
                self._logger.debug(
                    "Skipping record with no section_name: %s", record
                )
                continue

            # Validate / normalise country_code for the FK relation
            cc = record.get("country_code")
            if cc:
                cc = cc.strip().upper()[:2]
                if len(cc) != 2 or not cc.isalpha():
                    self._logger.debug(
                        "Invalid country_code '%s' for section '%s'; "
                        "setting to None.",
                        cc,
                        section_name,
                    )
                    cc = None

            row: Dict[str, Any] = {
                "section_name": section_name,
                "country_code": cc,
                "city": record.get("city", ""),
                "logo_url": record.get("logo_url"),
                "address": record.get("address", ""),
                "university_name": record.get("university_name", ""),
                "university_website": record.get("university_website"),
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
            self._logger.info("All records were filtered (no section_name).")
            return

        # Batch upsert
        table = supabase_client.table("esn_sections")
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
                on_conflict="section_name",
            ).execute()

        self._logger.info(
            "Successfully upserted %d section records.", len(rows)
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _discover_section_urls(
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
    ) -> List[str]:
        """Fetch the accounts.esn.org index and extract section links.

        Section links are found inside ``div.geolocation-location`` blocks
        with an ``h2.field-content.location-title`` heading.
        """
        try:
            html = await fetch_html_async(client, ACCOUNTS_LIST_URL, semaphore)
            if not html:
                return []

            soup = BeautifulSoup(html, "html.parser")
            seen: set[str] = set()
            urls: List[str] = []

            for block in soup.select("div.geolocation-location"):
                try:
                    h2 = block.select_one("h2.field-content.location-title")
                    if not h2:
                        continue
                    a = h2.find("a", href=True)
                    if not a:
                        continue
                    href = a.get("href")
                    abs_url = _accounts_absolute(href)
                    if not abs_url or abs_url in seen:
                        continue
                    seen.add(abs_url)
                    urls.append(abs_url)
                except (AttributeError, TypeError):
                    continue

            return urls
        except Exception as exc:
            logger.error("Section discovery error: %s", exc)
            return []
