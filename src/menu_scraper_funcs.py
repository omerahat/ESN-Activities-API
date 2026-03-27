import asyncio
import email.utils
import json
import random
import re
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import httpx
import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://activities.esn.org"
TARGET_URL = f"{BASE_URL}/activities?page=0"

# Date can be either "03/05/2026" or "11/05/2026 - 16/05/2026".
DATE_PATTERN = re.compile(
    r"(?P<start>\d{2}/\d{2}/\d{4})(?:\s*-\s*(?P<end>\d{2}/\d{2}/\d{4}))?"
)

# Heuristic for location lines like "Nicosia , CY" or "Lofoten Islands , NO".
LOCATION_PATTERN = re.compile(r"^[^,\n]+,\s*[A-Z]{2}$")


def safe_text(node: Optional[Tag]) -> Optional[str]:
    """Return node text or None if node is missing/empty."""
    if not node:
        return None
    text = node.get_text(" ", strip=True)
    return text if text else None


def to_absolute_url(href: Optional[str]) -> Optional[str]:
    """Normalize relative links to absolute URLs."""
    if not href:
        return None
    return urljoin(BASE_URL, href)


def parse_single_date(value: str) -> Optional[date]:
    """Parse dd/mm/yyyy into a date object."""
    try:
        return datetime.strptime(value, "%d/%m/%Y").date()
    except ValueError:
        return None


def parse_event_date(raw_date: Optional[str]) -> Dict[str, Optional[str]]:
    """
    Parse event date string into a structured object.
    Returns start/end in ISO format or None when unavailable.
    """
    if not raw_date:
        return {"raw": None, "start": None, "end": None}

    match = DATE_PATTERN.search(raw_date)
    if not match:
        return {"raw": raw_date, "start": None, "end": None}

    start_obj = parse_single_date(match.group("start"))
    end_group = match.group("end")
    end_obj = parse_single_date(end_group) if end_group else start_obj

    return {
        "raw": match.group(0),
        "start": start_obj.isoformat() if start_obj else None,
        "end": end_obj.isoformat() if end_obj else None,
    }


def is_upcoming(event_date: Dict[str, Optional[str]]) -> Optional[bool]:
    """Return True when event start date is today or in the future."""
    start = event_date.get("start")
    if not start:
        return None
    try:
        start_date = datetime.strptime(start, "%Y-%m-%d").date()
    except ValueError:
        return None
    return start_date >= date.today()


def default_request_headers() -> Dict[str, str]:
    """Shared browser-like headers for sync and async HTTP clients."""
    return {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }


def create_requests_session(pool_connections: int = 20, pool_maxsize: int = 20) -> requests.Session:
    """
    Session with pooled connections for sequential multi-page scraping.
    Tune pool_* to be >= typical worker count if using ThreadPoolExecutor.
    """
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=pool_connections, pool_maxsize=pool_maxsize)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(default_request_headers())
    return session


def create_async_client(
    max_concurrent: int, timeout: float = 20.0
) -> httpx.AsyncClient:
    """
    Single shared AsyncClient for parallel fetches with keep-alive limits.
    max_connections should be >= concurrency for headroom.
    """
    max_conn = max(max_concurrent * 2, 20)
    keepalive = max(max_concurrent, 10)
    limits = httpx.Limits(max_connections=max_conn, max_keepalive_connections=keepalive)
    return httpx.AsyncClient(
        headers=default_request_headers(),
        limits=limits,
        timeout=httpx.Timeout(timeout),
        follow_redirects=True,
    )


def _parse_retry_after_seconds(value: Optional[str]) -> Optional[float]:
    """Parse Retry-After as seconds (int) or HTTP-date."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        dt = email.utils.parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return max(0.0, (dt - now).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


def fetch_html(url: str, session: Optional[requests.Session] = None) -> Optional[str]:
    """Fetch HTML safely; pass session for connection reuse across many URLs."""
    try:
        if session is not None:
            response = session.get(url, timeout=20)
        else:
            response = requests.get(url, headers=default_request_headers(), timeout=20)
        response.raise_for_status()
        return response.text
    except requests.RequestException as exc:
        print(f"Request error while fetching {url}: {exc}")
        return None


async def _fetch_html_async_with_retries(
    client: httpx.AsyncClient,
    url: str,
    semaphore: asyncio.Semaphore,
    max_retries: int,
    backoff_base: float,
    jitter_ms: float,
) -> Optional[str]:
    """
    Bounded-concurrency fetch with jitter and exponential backoff on 429/502/503.
    Respects Retry-After when present.
    """
    async with semaphore:
        pre_jitter = random.uniform(0, jitter_ms / 1000.0) if jitter_ms > 0 else 0.0
        if pre_jitter:
            await asyncio.sleep(pre_jitter)
        for attempt in range(max_retries + 1):
            try:
                response = await client.get(url)
                if response.status_code == 200:
                    return response.text
                if response.status_code in (429, 502, 503) and attempt < max_retries:
                    ra = _parse_retry_after_seconds(response.headers.get("Retry-After"))
                    wait = (
                        ra
                        if ra is not None
                        else backoff_base * (2**attempt)
                    ) + random.uniform(0, jitter_ms / 1000.0)
                    await asyncio.sleep(wait)
                    continue
                print(f"HTTP {response.status_code} for {url}")
                return None
            except httpx.RequestError as exc:
                if attempt < max_retries:
                    wait = backoff_base * (2**attempt) + random.uniform(
                        0, jitter_ms / 1000.0
                    )
                    await asyncio.sleep(wait)
                    continue
                print(f"Request error while fetching {url}: {exc}")
                return None
        return None


async def _scrape_single_page_async(
    client: httpx.AsyncClient,
    page: int,
    semaphore: asyncio.Semaphore,
    max_retries: int,
    backoff_base: float,
    jitter_ms: float,
) -> Tuple[int, List[Dict[str, Any]]]:
    print(f"Scraping page {page} ...", flush=True)
    url = build_page_url(page)
    html = await _fetch_html_async_with_retries(
        client, url, semaphore, max_retries, backoff_base, jitter_ms
    )
    if not html:
        return (page, [])
    events = await asyncio.to_thread(parse_events, html)
    return (page, events)


def build_page_url(page: int) -> str:
    """Build activities listing URL for a specific page index."""
    return f"{BASE_URL}/activities?page={page}"


def find_event_container(activity_anchor: Tag) -> Tag:
    """
    Pick the event card container from a known anchor.
    If this site changes, update this to the real repeating event wrapper.
    """
    article = activity_anchor.find_parent("article", class_=re.compile(r"activities-mini-preview"))
    if article:
        return article
    return activity_anchor.parent if isinstance(activity_anchor.parent, Tag) else activity_anchor


def extract_date_text(container: Tag) -> Optional[str]:
    """
    Search text near each event for dd/mm/yyyy or date ranges.
    If dates live in a dedicated element, replace with container.select_one(...).
    """
    for text in container.stripped_strings:
        if DATE_PATTERN.search(text):
            return text.strip()
    return None


def extract_location(container: Tag) -> Optional[str]:
    """
    Heuristic location extraction from card text.
    If a stable CSS selector exists, replace this with direct select_one(...).
    """
    for text in container.stripped_strings:
        normalized = re.sub(r"\s+", " ", text).strip()
        # Harmonize "City , CC" to "City, CC" for cleaner output.
        normalized = normalized.replace(" , ", ", ")
        if LOCATION_PATTERN.match(normalized):
            return normalized
    return None


def extract_event_name(container: Tag, fallback_anchor: Tag) -> Optional[str]:
    """
    Prefer card title anchor text, then fallback to anchor title attribute.
    Update selector if title moves in future HTML revisions.
    """
    title_anchor = container.select_one(".eg-c-card-title a[href*='/activity/']")
    if title_anchor:
        title_text = safe_text(title_anchor)
        if title_text:
            return title_text

    fallback_title = fallback_anchor.get("title")
    if fallback_title:
        # Site title often looks like: "Activity Adventure trip to Lofoten"
        return re.sub(r"^\s*Activity\s+", "", fallback_title).strip()
    return safe_text(fallback_anchor)


def parse_events(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    events: List[Dict[str, Any]] = []
    seen_event_links = set()

    # Selector assumption: event cards are article.activities-mini-preview.
    # Update this selector to the repeating card wrapper if structure changes.
    event_cards = soup.select("article.activities-mini-preview")

    for card in event_cards:
        # Selector assumption: event page link contains "/activity/".
        # Many cards contain two such anchors (image + title), so we select one.
        anchor = card.select_one('a[href*="/activity/"]')
        if not anchor:
            continue
        href = anchor.get("href")
        absolute_event_link = to_absolute_url(href)
        if not absolute_event_link or absolute_event_link in seen_event_links:
            continue
        seen_event_links.add(absolute_event_link)

        container = find_event_container(anchor)

        # Selector assumption: organizer section link contains "/organisation/".
        # If structure changes, replace with card-specific selector for section link.
        organizer_anchor = container.select_one('a[href*="/organisation/"]')

        # Date selector assumption based on current class naming.
        # If changed, use the new date element selector before falling back.
        date_node = container.select_one(".act-date")
        date_text = safe_text(date_node) or extract_date_text(container)
        structured_date = parse_event_date(date_text)

        # Location selector assumption based on split city/rest spans.
        # If missing in future HTML, fallback heuristic below still applies.
        location_city = safe_text(container.select_one(".act-location-city"))
        location_rest = safe_text(container.select_one(".act-location-rest"))
        location = None
        if location_city or location_rest:
            location = f"{location_city or ''}{location_rest or ''}".replace(" , ", ", ").strip()
        if not location:
            location = extract_location(container)

        event = {
            "event_name": extract_event_name(container, anchor),
            "organizer_section": safe_text(organizer_anchor),
            "event_date": structured_date,
            "is_upcoming": is_upcoming(structured_date),
            "organizer_section_website_link": to_absolute_url(
                organizer_anchor.get("href") if organizer_anchor else None
            ),
            "location": location,
            "event_page_link": absolute_event_link,
        }
        events.append(event)

    return events


def scrape_events(
    url: str = TARGET_URL, session: Optional[requests.Session] = None
) -> List[Dict[str, Any]]:
    html = fetch_html(url, session=session)
    if not html:
        return []
    return parse_events(html)


def scrape_events_by_page(
    page: int = 0, session: Optional[requests.Session] = None
) -> List[Dict[str, Any]]:
    """Scrape a single activities page by index (0-based)."""
    return scrape_events(build_page_url(page), session=session)


def scrape_events_multi_page(
    start_page: int = 0, end_page: int = 0, stop_on_empty: bool = True
) -> List[Dict[str, Any]]:
    """
    Scrape events across a page range, inclusive, deduplicated by event_page_link.
    Example: start_page=0, end_page=3 scrapes pages 0,1,2,3.
    """
    if start_page < 0 or end_page < 0:
        raise ValueError("start_page and end_page must be >= 0")
    if end_page < start_page:
        raise ValueError("end_page must be greater than or equal to start_page")

    all_events: List[Dict[str, Any]] = []
    seen_links = set()

    with create_requests_session() as session:
        for page in range(start_page, end_page + 1):
            print(f"Scraping page {page}")
            page_events = scrape_events_by_page(page, session=session)
            if not page_events and stop_on_empty:
                break
            print(f"Found {len(page_events)} events on page {page}")
            for event in page_events:
                event_link = event.get("event_page_link")
                if event_link and event_link in seen_links:
                    continue
                if event_link:
                    seen_links.add(event_link)
                all_events.append(event)

    return all_events


async def scrape_events_multi_page_async(
    start_page: int = 0,
    end_page: int = 0,
    *,
    stop_on_empty: bool = True,
    max_concurrent: int = 10,
    max_retries: int = 3,
    backoff_base: float = 1.0,
    jitter_ms: float = 100.0,
    timeout: float = 20.0,
) -> List[Dict[str, Any]]:
    """
    Scrape events in parallel with one shared httpx.AsyncClient (connection reuse).
    Pages are merged in order; deduplicated by event_page_link.
    """
    if start_page < 0 or end_page < 0:
        raise ValueError("start_page and end_page must be >= 0")
    if end_page < start_page:
        raise ValueError("end_page must be greater than or equal to start_page")

    pages = list(range(start_page, end_page + 1))
    semaphore = asyncio.Semaphore(max_concurrent)

    async with create_async_client(max_concurrent, timeout=timeout) as client:
        tasks = [
            _scrape_single_page_async(
                client,
                page,
                semaphore,
                max_retries,
                backoff_base,
                jitter_ms,
            )
            for page in pages
        ]
        page_results = await asyncio.gather(*tasks)

    page_results.sort(key=lambda item: item[0])

    all_events: List[Dict[str, Any]] = []
    seen_links = set()

    for page, page_events in page_results:
        if stop_on_empty and not page_events:
            break
        print(f"Page {page}: {len(page_events)} events")
        for event in page_events:
            event_link = event.get("event_page_link")
            if event_link and event_link in seen_links:
                continue
            if event_link:
                seen_links.add(event_link)
            all_events.append(event)

    return all_events


def save_to_file(data: List[Dict[str, Any]], filename: str) -> None:
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

