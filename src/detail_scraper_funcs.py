import asyncio
import json
import re
from typing import Any, Dict, Optional

import httpx
import requests
from bs4 import BeautifulSoup, Tag

from src.menu_scraper_funcs import fetch_html, fetch_html_async, safe_text, to_absolute_url

_INT_IN_TEXT = re.compile(r"\d+")


def _empty_details() -> Dict[str, Any]:
    """Stable shape when HTML is missing or fetch failed."""
    return {
        "main_image_url": None,
        "detailed_location": "",
        "total_participants": None,
        "causes": [],
        "types_of_activity": [],
        "goal_of_activity": None,
        "description": None,
        "registration_link": None,
        "sdgs": [],
        "objectives": [],
        "outcomes": None,
    }


def _parse_int_from_text(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    match = _INT_IN_TEXT.search(raw)
    if not match:
        return None
    try:
        return int(match.group(0))
    except ValueError:
        return None


def _normalize_block_text(node: Optional[Tag]) -> Optional[str]:
    text = safe_text(node)
    if text is None:
        return None
    return " ".join(text.split())


def _strip_outcomes_label(text: str) -> str:
    """Remove a leading 'Outcomes' label from combined field text."""
    cleaned = text.strip()
    cleaned = re.sub(r"^\s*outcomes\s*:?\s*", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def parse_event_details(html: str, base_url: str = "https://activities.esn.org") -> Dict[str, Any]:
    """
    Parse an ESN activity detail page HTML into a dictionary for JSONB storage.
    Missing elements yield None, empty string, or empty list as appropriate.
    """
    del base_url  # Reserved for API consistency; URLs resolved via to_absolute_url in menu_scraper_funcs.

    out: Dict[str, Any] = _empty_details()
    if not html:
        return out

    soup = BeautifulSoup(html, "html.parser")

    # 1. main_image_url
    img = soup.select_one("picture img.img-fluid")
    if img and img.get("src"):
        out["main_image_url"] = to_absolute_url(img.get("src"))

    # 2. detailed_location
    loc_block = soup.select_one(
        "div.ct-physical-activity__field-ct-act-location div.highlight-data-text"
    )
    if loc_block:
        spans = loc_block.find_all("span")
        parts = [s.get_text(strip=True) for s in spans if s.get_text(strip=True)]
        out["detailed_location"] = ", ".join(parts)

    # 3. total_participants
    part_el = soup.select_one("div.highlight-data-text-big")
    out["total_participants"] = _parse_int_from_text(safe_text(part_el))

    # 4. causes
    for a in soup.select("div.activity-cause a"):
        t = a.get_text(strip=True)
        if t:
            out["causes"].append(t)

    # 5. types_of_activity
    for a in soup.select("div.activity-type a"):
        t = a.get_text(strip=True)
        if t:
            out["types_of_activity"].append(t)

    # 6. goal_of_activity
    goal = soup.select_one(
        "div.ct-physical-activity__field-ct-act-goal-activity div.field__item"
    )
    out["goal_of_activity"] = _normalize_block_text(goal)

    # 7. description
    desc = soup.select_one(
        "div.ct-physical-activity__field-ct-act-description div.field__item"
    )
    out["description"] = _normalize_block_text(desc)

    # 8. registration_link
    reg_a = soup.select_one("div.ct-physical-activity__field-ct-act-link-registrat a")
    if reg_a and reg_a.get("href"):
        out["registration_link"] = to_absolute_url(reg_a.get("href"))

    # 9. sdgs
    for sdg_img in soup.select("img.sdg-logo-icon"):
        title = sdg_img.get("title")
        if title and str(title).strip():
            out["sdgs"].append(str(title).strip())

    # 10. objectives
    for badge in soup.select("div.activity__objective span.badge"):
        t = badge.get_text(strip=True)
        if t:
            out["objectives"].append(t)

    # 11. outcomes
    outcomes_el = soup.select_one("div.ct-physical-activity__field-ct-act-res-pos-aspect")
    if outcomes_el:
        raw = outcomes_el.get_text(" ", strip=True)
        if raw:
            out["outcomes"] = _strip_outcomes_label(raw) or None
        else:
            out["outcomes"] = None

    return out


def scrape_event_details(url: str, session: Optional[requests.Session] = None) -> Dict[str, Any]:
    """Fetch a detail page and parse it; returns empty-shaped dict on fetch failure."""
    html = fetch_html(url, session)
    if html is None:
        return _empty_details()
    return parse_event_details(html)


async def scrape_event_details_async(
    url: str,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    *,
    max_retries: int = 3,
    backoff_base: float = 1.0,
    jitter_ms: float = 100.0,
) -> Dict[str, Any]:
    """Fetch a detail page with async HTTP (retries/backoff) and parse HTML."""
    html = await fetch_html_async(
        client,
        url,
        semaphore,
        max_retries=max_retries,
        backoff_base=backoff_base,
        jitter_ms=jitter_ms,
    )
    if html is None:
        return _empty_details()
    return parse_event_details(html)


if __name__ == "__main__":
    URL_1 = "https://activities.esn.org/activity/franco-german-cafe-linguistic-and-cultural-exchange-20677"
    URL_2 = "https://activities.esn.org/activity/bowling-45263"
    for u in (URL_1, URL_2):
        result = scrape_event_details(u)
        print(json.dumps(result, indent=4, ensure_ascii=False))
