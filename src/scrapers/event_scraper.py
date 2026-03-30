from __future__ import annotations

"""
Event scraper – two-phase pipeline that scrapes the ESN activities feed
(sequential pagination over ``activities.esn.org/?page=N``) *and* dives into
each event's detail page for JSONB-ready structured data, then upserts
everything into ``esn_events``.

Merges the logic previously split across ``menu_scraper_main.py``,
``menu_scraper_funcs.py``, ``enrich_events_with_details.py``, and
``detail_scraper_funcs.py`` into a single ``BaseScraper`` subclass.
"""

import asyncio
import logging
import random
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup, Tag
from supabase import Client

from src.menu_scraper_funcs import (
    create_async_client,
    fetch_html_async,
    is_upcoming,
    parse_event_date,
    safe_text,
    to_absolute_url,
)
from src.detail_scraper_funcs import parse_event_details
from src.scrapers.base_scraper import BaseScraper

logger: logging.Logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ACTIVITIES_BASE: str = "https://activities.esn.org/activities"


def _feed_listing_url(page: int) -> str:
    """Build activities feed URL for a given zero-based page index."""
    return f"{ACTIVITIES_BASE}?page={page}"


# Regex used by feed parsing
DATE_PATTERN = re.compile(
    r"(?P<start>\d{2}/\d{2}/\d{4})(?:\s*-\s*(?P<end>\d{2}/\d{2}/\d{4}))?"
)
LOCATION_PATTERN = re.compile(r"^[^,\n]+,\s*[A-Z]{2}$")

# ---------------------------------------------------------------------------
# Private helpers – feed-card parsing (from menu_scraper_funcs.py)
# ---------------------------------------------------------------------------


def _find_event_container(activity_anchor: Tag) -> Tag:
    """Walk up from the anchor to find the event card wrapper."""
    article = activity_anchor.find_parent(
        "article", class_=re.compile(r"activities-mini-preview")
    )
    if article:
        return article
    return (
        activity_anchor.parent
        if isinstance(activity_anchor.parent, Tag)
        else activity_anchor
    )


def _extract_date_text(container: Tag) -> Optional[str]:
    """Return the first snippet inside *container* that contains a date."""
    for text in container.stripped_strings:
        if DATE_PATTERN.search(text):
            return text.strip()
    return None


def _extract_location(container: Tag) -> Optional[str]:
    """Heuristic location extraction from card text."""
    for text in container.stripped_strings:
        normalized = re.sub(r"\s+", " ", text).strip()
        normalized = normalized.replace(" , ", ", ")
        if LOCATION_PATTERN.match(normalized):
            return normalized
    return None


def _extract_event_name(container: Tag, fallback_anchor: Tag) -> Optional[str]:
    """Prefer the card-title anchor text, then fall back to <a title>."""
    title_anchor = container.select_one(
        ".eg-c-card-title a[href*='/activity/']"
    )
    if title_anchor:
        title_text = safe_text(title_anchor)
        if title_text:
            return title_text

    fallback_title = fallback_anchor.get("title")
    if fallback_title:
        return re.sub(r"^\s*Activity\s+", "", fallback_title).strip()
    return safe_text(fallback_anchor)


def _parse_feed_page(html: str) -> List[Dict[str, Any]]:
    """Parse a single feed listing page into basic event records."""
    soup = BeautifulSoup(html, "html.parser")
    events: List[Dict[str, Any]] = []
    seen_links: set[str] = set()

    for card in soup.select("article.activities-mini-preview"):
        anchor = card.select_one('a[href*="/activity/"]')
        if not anchor:
            continue
        href = anchor.get("href")
        absolute_link = to_absolute_url(href)
        if not absolute_link or absolute_link in seen_links:
            continue
        seen_links.add(absolute_link)

        container = _find_event_container(anchor)

        organizer_anchor = container.select_one('a[href*="/organisation/"]')

        date_node = container.select_one(".act-date")
        date_text = safe_text(date_node) or _extract_date_text(container)
        structured_date = parse_event_date(date_text)

        location_city = safe_text(container.select_one(".act-location-city"))
        location_rest = safe_text(container.select_one(".act-location-rest"))
        location: Optional[str] = None
        if location_city or location_rest:
            location = (
                f"{location_city or ''}{location_rest or ''}"
                .replace(" , ", ", ")
                .strip()
            )
        if not location:
            location = _extract_location(container)

        # Organizer text must exactly match esn_sections(section_name).
        # safe_text already strips and normalises whitespace, which is the
        # same treatment applied by the SectionScraper when writing
        # section_name, so the values will match.
        organizer_text = safe_text(organizer_anchor) if organizer_anchor else None

        events.append(
            {
                "event_name": _extract_event_name(container, anchor),
                "organizer_section": organizer_text,
                "event_date": structured_date,
                "is_upcoming": is_upcoming(structured_date),
                "organizer_section_website_link": to_absolute_url(
                    organizer_anchor.get("href") if organizer_anchor else None
                ),
                "location": location,
                "event_page_link": absolute_link,
            }
        )
    return events


# ---------------------------------------------------------------------------
# EventScraper
# ---------------------------------------------------------------------------


class EventScraper(BaseScraper):
    """Two-phase ESN event scraper → ``esn_events``.

    Phase 1 – **Feed** : walk ``activities.esn.org/?page=N`` sequentially until
        the listing is exhausted (no HTML or no events when ``stop_on_empty``).
        When ``end_page > start_page``, discovery stops after page ``end_page``
        (inclusive) for partial runs; when ``end_page == start_page``, pagination
        continues until the feed ends.
    Phase 2 – **Detail** : for every ``event_page_link``, fetch the detail page
        and parse rich JSONB fields (description, causes, SDGs, participants …).

    Pipeline stages
    ---------------
    1. ``fetch_data``   – runs both phases, returns enriched event dicts.
    2. ``parse_data``   – light normalisation pass (already parsed during fetch).
    3. ``upsert_to_db`` – upsert into ``esn_events``; conflict on ``event_page_link``.
    """

    DEFAULT_CONCURRENCY: int = 20
    UPSERT_BATCH_SIZE: int = 50

    def __init__(
        self,
        *,
        start_page: int = 0,
        end_page: int = 0,
        stop_on_empty: bool = True,
        concurrency: int = DEFAULT_CONCURRENCY,
        max_retries: int = 3,
        backoff_base: float = 1.0,
        jitter_ms: float = 100.0,
        timeout: float = 20.0,
    ) -> None:
        super().__init__(name="EventScraper")
        self._start_page = start_page
        self._end_page = end_page
        self._stop_on_empty = stop_on_empty
        self._concurrency = concurrency
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._jitter_ms = jitter_ms
        self._timeout = timeout

    # ------------------------------------------------------------------
    # 1. fetch_data  (async – two phases)
    # ------------------------------------------------------------------

    async def fetch_data(self) -> List[Dict[str, Any]]:
        """Run Phase 1 (feed) then Phase 2 (detail enrichment).

        Returns:
            A list of event dicts, each containing both the basic feed
            fields **and** an embedded ``details`` dict with JSONB-ready
            structured data from the detail page.
        """
        semaphore = asyncio.Semaphore(self._concurrency)

        async with create_async_client(
            self._concurrency, timeout=self._timeout
        ) as client:
            # ---- Phase 1: paginated feed scraping ----
            events = await self._scrape_feed(client, semaphore)
            if self._end_page > self._start_page:
                self._logger.info(
                    "Phase 1 complete: %d unique events "
                    "(feed pages %d–%d inclusive, capped).",
                    len(events),
                    self._start_page,
                    self._end_page,
                )
            else:
                self._logger.info(
                    "Phase 1 complete: %d unique events from full feed discovery "
                    "(starting at page %d).",
                    len(events),
                    self._start_page,
                )

            # ---- Phase 2: detail page enrichment ----
            events = await self._enrich_details(client, semaphore, events)
            self._logger.info("Phase 2 complete: detail enrichment finished.")

        return events

    # ------------------------------------------------------------------
    # 2. parse_data
    # ------------------------------------------------------------------

    def parse_data(
        self,
        raw_data: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Normalise and flatten events for ``esn_events`` storage.

        The heavy parsing already happened during ``fetch_data``; this
        stage just ensures every record has the expected keys and the
        ``organizer_section`` text is clean for the FK match.
        """
        records: List[Dict[str, Any]] = []

        for event in raw_data:
            organizer = event.get("organizer_section")
            if isinstance(organizer, str):
                organizer = " ".join(organizer.split()).strip() or None

            record: Dict[str, Any] = {
                "event_name": event.get("event_name"),
                "organizer_section": organizer,
                "event_date": event.get("event_date", {}),
                "is_upcoming": event.get("is_upcoming", True),
                "organizer_section_website_link": event.get(
                    "organizer_section_website_link"
                ),
                "location": event.get("location"),
                "event_page_link": event.get("event_page_link"),
                "details": event.get("details"),
            }
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
        """Upsert event records into ``esn_events``.

        Conflict resolution on ``event_page_link``.  Every row receives a
        fresh ``last_scraped_at``; the DB trigger handles ``updated_at``.

        ``event_date`` is stored as JSONB (dict with ``raw``, ``start``,
        ``end`` keys).

        Rows whose ``organizer_section`` is missing from ``esn_sections``
        are nullified before upsert to satisfy the FK on ``section_name``.
        """
        if not data:
            self._logger.info("No event records to upsert.")
            return

        sections_resp = (
            supabase_client.table("esn_sections")
            .select("section_name")
            .execute()
        )
        raw_sections = sections_resp.data or []
        valid_sections: set[str] = {
            r["section_name"]
            for r in raw_sections
            if isinstance(r, dict) and r.get("section_name")
        }

        orphaned_count = 0
        for event in data:
            org = event.get("organizer_section")
            if isinstance(org, str):
                org = org.strip() or None
            if org and org not in valid_sections:
                orphaned_count += 1
            if not org or org not in valid_sections:
                event["organizer_section"] = None

        if orphaned_count:
            self._logger.warning(
                "Warning: Found %d orphaned events with unknown sections. "
                "Nullifying their organizer fields.",
                orphaned_count,
            )

        now_iso = datetime.now(timezone.utc).isoformat()

        rows: List[Dict[str, Any]] = []
        for record in data:
            link = record.get("event_page_link")
            if not link:
                self._logger.debug(
                    "Skipping record without event_page_link: %s", record
                )
                continue

            row: Dict[str, Any] = {
                "event_name": record.get("event_name"),
                "organizer_section": record.get("organizer_section"),
                "event_date": record.get("event_date", {}),
                "is_upcoming": record.get("is_upcoming", True),
                "organizer_section_website_link": record.get(
                    "organizer_section_website_link"
                ),
                "location": record.get("location"),
                "event_page_link": link,
                "details": record.get("details"),
                "last_scraped_at": now_iso,
            }
            rows.append(row)

        if not rows:
            self._logger.info(
                "All records filtered (no event_page_link)."
            )
            return

        table = supabase_client.table("esn_events")
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
                on_conflict="event_page_link",
            ).execute()

        self._logger.info(
            "Successfully upserted %d event records.", len(rows)
        )

    # ------------------------------------------------------------------
    # Internal: Phase-1 feed scraping
    # ------------------------------------------------------------------

    async def _scrape_feed(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
    ) -> List[Dict[str, Any]]:
        """Scrape the activities listing page by page until the feed ends.

        Order follows ascending ``page`` indices; duplicates are dropped via
        ``seen_links`` on ``event_page_link``.
        """
        all_events: List[Dict[str, Any]] = []
        seen_links: set[str] = set()
        page_number = self._start_page
        pages_fetched = 0
        capped_range = self._end_page > self._start_page

        while True:
            if capped_range and page_number > self._end_page:
                break

            url = _feed_listing_url(page_number)
            self._logger.debug("Fetching feed page %d …", page_number)
            html = await fetch_html_async(
                client,
                url,
                semaphore,
                max_retries=self._max_retries,
                backoff_base=self._backoff_base,
                jitter_ms=self._jitter_ms,
            )
            if not html:
                self._logger.info(
                    "Feed page %d: no HTML (empty response or non-200); stopping.",
                    page_number,
                )
                break

            page_events = await asyncio.to_thread(_parse_feed_page, html)

            if self._stop_on_empty and not page_events:
                self._logger.info(
                    "Page %d returned 0 events; stopping (stop_on_empty).",
                    page_number,
                )
                break

            for event in page_events:
                link = event.get("event_page_link")
                if link and link in seen_links:
                    continue
                if link:
                    seen_links.add(link)
                all_events.append(event)

            pages_fetched += 1
            self._logger.info(
                "Discovered %d urls across %d pages so far…",
                len(seen_links),
                pages_fetched,
            )
            self._logger.info(
                "Page %d: %d events on this page.",
                page_number,
                len(page_events),
            )

            page_number += 1

        return all_events

    # ------------------------------------------------------------------
    # Internal: Phase-2 detail enrichment
    # ------------------------------------------------------------------

    async def _fetch_single_detail(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        index: int,
        url: str,
        *,
        total: int,
        progress_lock: asyncio.Lock,
        completed_holder: List[int],
    ) -> Tuple[int, str, Optional[str]]:
        """Fetch one detail page under the shared semaphore; return HTML or None."""
        html: Optional[str] = None
        try:
            async with semaphore:
                await asyncio.sleep(random.uniform(0.1, 0.3))
                html = await fetch_html_async(
                    client,
                    url,
                    None,
                    max_retries=self._max_retries,
                    backoff_base=self._backoff_base,
                    jitter_ms=0,
                )
        except Exception:
            html = None

        async with progress_lock:
            completed_holder[0] += 1
            c = completed_holder[0]
            if c % 500 == 0 or c == total:
                self._logger.info("Fetched %d/%d events...", c, total)

        return (index, url, html)

    async def _enrich_details(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        events: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Fetch and parse each event's detail page concurrently.

        Enriches each event dict in-place with a ``details`` key
        (structured JSONB data from ``parse_event_details``).
        """
        work: List[Tuple[int, str]] = []
        for idx, ev in enumerate(events):
            link = ev.get("event_page_link")
            if link and isinstance(link, str):
                work.append((idx, link))

        if not work:
            return events

        total = len(work)
        progress_lock = asyncio.Lock()
        completed_holder: List[int] = [0]

        results = await asyncio.gather(
            *[
                self._fetch_single_detail(
                    client,
                    semaphore,
                    i,
                    u,
                    total=total,
                    progress_lock=progress_lock,
                    completed_holder=completed_holder,
                )
                for i, u in work
            ]
        )

        failed = sum(1 for _i, _u, h in results if h is None)
        if failed:
            self._logger.info(
                "Detail fetch returned no HTML for %d/%d URLs "
                "(empty details).",
                failed,
                total,
            )

        for idx, _url, html in results:
            events[idx]["details"] = parse_event_details(html or "")

        return events
