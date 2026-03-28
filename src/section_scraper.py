"""
Discover ESN section URLs from accounts.esn.org and scrape contact/location details per section.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Repo root on sys.path so `python src/section_scraper.py` works, not only `python -m src.section_scraper`.
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import argparse
import asyncio
import json
import random
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, Tag

from src.menu_scraper_funcs import (
    create_async_client,
    fetch_html_async,
    safe_text,
)

ACCOUNTS_BASE = "https://accounts.esn.org/"
ACCOUNTS_LIST_URL = urljoin(ACCOUNTS_BASE, "")
ESN_COUNTRIES_JSON = REPO_ROOT / "esn_countries.json"
ESN_SECTIONS_JSON = REPO_ROOT / "esn_sections.json"


def clean_text(text: Optional[str]) -> str:
    """Strip newlines, tabs, and excess whitespace."""
    if text is None:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def accounts_absolute(href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    return urljoin(ACCOUNTS_BASE, href)


def country_code_from_url(page_url: str) -> Optional[str]:
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


def country_code_from_country_url(page_url: str) -> Optional[str]:
    try:
        path = urlparse(page_url).path.strip("/")
        parts = [p for p in path.split("/") if p]
        if len(parts) >= 2 and parts[0].lower() == "country":
            return parts[1].upper()
        return None
    except (ValueError, IndexError):
        return None


async def get_all_country_urls(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> List[str]:
    try:
        html = await fetch_html_async(client, ACCOUNTS_LIST_URL, semaphore)
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        seen: set[str] = set()
        urls: List[str] = []
        for a in soup.find_all("a", href=True):
            abs_url = accounts_absolute(a.get("href"))
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
        print(f"Country discovery error: {exc}")
        return []


async def get_all_section_urls(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> List[str]:
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
                abs_url = accounts_absolute(href)
                if not abs_url or abs_url in seen:
                    continue
                seen.add(abs_url)
                urls.append(abs_url)
            except (AttributeError, TypeError):
                continue
        return urls
    except Exception as exc:
        print(f"Discovery error: {exc}")
        return []


def _field_item_text(soup: BeautifulSoup, field_class: str) -> str:
    node = soup.select_one(f"div.{field_class} div.field--item")
    return clean_text(safe_text(node))


def _social_key_from_label(label: str) -> Optional[str]:
    """Map title/aria-label to facebook | instagram | twitter; first match wins."""
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
    out: Dict[str, str] = {}
    anchors: List[Tag] = []
    for sel in ("div.d-flex.my-3", 'div[class="__"]'):
        for block in soup.select(sel):
            anchors.extend(block.find_all("a", href=True))
    for a in anchors:
        label = ((a.get("title") or "") + " " + (a.get("aria-label") or "")).strip()
        key = _social_key_from_label(label)
        if not key or key in out:
            continue
        href = a.get("href")
        if not href:
            continue
        abs_h = accounts_absolute(href) or href
        out[key] = abs_h
    return out


def _find_website_href(soup: BeautifulSoup) -> str:
    for a in soup.find_all("a", href=True):
        if clean_text(safe_text(a)) == "Website":
            return accounts_absolute(a.get("href")) or ""
    return ""


def parse_country_details(html: Optional[str], page_url: str) -> Dict[str, Any]:
    code = country_code_from_country_url(page_url)
    empty: Dict[str, Any] = {
        "country_code": code,
        "country_name": None,
        "url": page_url,
        "email": "",
        "website": "",
        "social_links": {},
    }
    if not html:
        return empty

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return empty

    try:
        h1 = soup.select_one("h1.page-header")
        name = clean_text(safe_text(h1))
        if name:
            empty["country_name"] = name
        if not empty["country_name"]:
            title_tag = soup.find("title")
            raw = clean_text(safe_text(title_tag))
            if raw:
                empty["country_name"] = raw.split("|")[0].strip() if "|" in raw else raw

        email_node = soup.select_one("div.field--name-field-email div.field--item")
        empty["email"] = clean_text(safe_text(email_node))

        empty["website"] = _find_website_href(soup)
        empty["social_links"] = _collect_social_links(soup)
    except (AttributeError, TypeError, KeyError):
        pass

    return empty


def parse_section_details(html: Optional[str], page_url: str) -> Dict[str, Any]:
    empty: Dict[str, Any] = {
        "section_name": None,
        "country_code": country_code_from_url(page_url),
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
        return empty

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return empty

    try:
        h1 = soup.select_one("h1.page-header")
        empty["section_name"] = clean_text(safe_text(h1)) or None
        empty["country_code"] = country_code_from_url(page_url)

        empty["city"] = _field_item_text(soup, "field--name-field-city")

        logo = soup.select_one("div.group__field-pseudo-group-logo img")
        if logo and logo.get("src"):
            empty["logo_url"] = accounts_absolute(logo["src"])

        addr_block = soup.select_one("div.field--name-field-address div.field--item")
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
                t = clean_text(safe_text(span))
                if t:
                    parts.append(t)
            empty["address"] = ", ".join(parts)

        empty["university_name"] = _field_item_text(soup, "field--name-field-university-name")

        uni_a = soup.select_one("div.field--name-field-university-website a")
        if uni_a and uni_a.get("href"):
            empty["university_website"] = accounts_absolute(uni_a["href"])

        email_node = soup.select_one("div.field--name-field-email div.field--item")
        empty["email"] = clean_text(safe_text(email_node))

        empty["website"] = _find_website_href(soup)
        empty["social_links"] = _collect_social_links(soup)
    except (AttributeError, TypeError, KeyError):
        pass

    return empty


async def scrape_all_countries(limit: int = 0) -> List[Dict[str, Any]]:
    sem = asyncio.Semaphore(1)
    async with create_async_client(1) as client:
        urls = await get_all_country_urls(client, sem)
        if limit > 0:
            urls = urls[:limit]
        total = len(urls)
        results: List[Dict[str, Any]] = []

        for index, url in enumerate(urls, start=1):
            print(f"[{index}/{total}] Scraping country {url}...")
            if index > 1:
                await asyncio.sleep(random.uniform(1, 2.5))
            try:
                html = await fetch_html_async(client, url, sem)
            except Exception as exc:
                print(f"Fetch error for {url}: {exc}")
                html = None

            if html is None:
                row = parse_country_details("", url)
                row["error"] = "fetch_failed"
                results.append(row)
                continue

            try:
                results.append(parse_country_details(html, url))
            except Exception as exc:
                print(f"Parse error for {url}: {exc}")
                row = parse_country_details("", url)
                row["error"] = "parse_failed"
                results.append(row)

        return results


async def scrape_all_sections(limit: int = 0) -> List[Dict[str, Any]]:
    sem = asyncio.Semaphore(1)
    async with create_async_client(1) as client:
        urls = await get_all_section_urls(client, sem)
        if limit > 0:
            urls = urls[:limit]
        total = len(urls)
        results: List[Dict[str, Any]] = []

        for index, url in enumerate(urls, start=1):
            print(f"[{index}/{total}] Scraping details for {url}...")
            if index > 1:
                await asyncio.sleep(random.uniform(1, 2.5))
            try:
                html = await fetch_html_async(client, url, sem)
            except Exception as exc:
                print(f"Fetch error for {url}: {exc}")
                html = None

            if html is None:
                row = parse_section_details("", url)
                row["error"] = "fetch_failed"
                row["url"] = url
                results.append(row)
                continue

            try:
                results.append(parse_section_details(html, url))
            except Exception as exc:
                print(f"Parse error for {url}: {exc}")
                row = parse_section_details("", url)
                row["error"] = "parse_failed"
                row["url"] = url
                results.append(row)

        return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape ESN country and section data from accounts.esn.org",
    )
    parser.add_argument(
        "--mode",
        choices=["all", "countries", "sections"],
        default="all",
        help="What to scrape (default: all).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max URLs to process per scraper; 0 means no limit.",
    )
    return parser.parse_args()


async def run_cli(args: argparse.Namespace) -> None:
    if args.mode in ("all", "countries"):
        print("Starting country scraping...")
        countries = await scrape_all_countries(args.limit)
        path = ESN_COUNTRIES_JSON
        with path.open("w", encoding="utf-8") as f:
            json.dump(countries, f, indent=4, ensure_ascii=False)
        print(f"Saved {len(countries)} countries to {path}")

    if args.mode in ("all", "sections"):
        print("Starting section scraping...")
        sections = await scrape_all_sections(args.limit)
        path = ESN_SECTIONS_JSON
        with path.open("w", encoding="utf-8") as f:
            json.dump(sections, f, indent=4, ensure_ascii=False)
        print(f"Saved {len(sections)} sections to {path}")


if __name__ == "__main__":
    asyncio.run(run_cli(parse_args()))
