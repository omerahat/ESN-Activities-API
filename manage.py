#!/usr/bin/env python3
"""
CLI entrypoint for running ESN scrapers.

Usage
-----
    python manage.py scrape --target countries
    python manage.py scrape --target sections
    python manage.py scrape --target events --start-page 0 --end-page 5
    python manage.py scrape --target all

When ``--target all``, scrapers run in FK-safe order:
    Countries → Sections → Events
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from typing import Any

from dotenv import load_dotenv
from supabase import Client, create_client

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("manage")

# ---------------------------------------------------------------------------
# Supabase client factory
# ---------------------------------------------------------------------------


def _init_supabase() -> Client:
    """Create and return an authenticated Supabase client."""
    load_dotenv()
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        logger.error(
            "SUPABASE_URL and SUPABASE_KEY must be set in the environment "
            "(or in .env)."
        )
        sys.exit(1)
    return create_client(url, key)


# ---------------------------------------------------------------------------
# CLI definition
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="manage.py",
        description="ESN Activities API – management CLI.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ---- scrape sub-command ----
    scrape_parser = subparsers.add_parser(
        "scrape",
        help="Run one or more scrapers.",
    )
    scrape_parser.add_argument(
        "--target",
        choices=["countries", "sections", "events", "all"],
        required=True,
        help=(
            "Which scraper(s) to run. 'all' runs in FK-safe order: "
            "countries → sections → events."
        ),
    )

    # Event-specific knobs
    scrape_parser.add_argument(
        "--start-page",
        type=int,
        default=0,
        help="First page index for the events feed (inclusive). Default: 0.",
    )
    scrape_parser.add_argument(
        "--end-page",
        type=int,
        default=0,
        help="Last page index for the events feed (inclusive). Default: 0.",
    )
    scrape_parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Max concurrent HTTP requests (events). Default: 10.",
    )
    scrape_parser.add_argument(
        "--continue-on-empty",
        action="store_true",
        help="Don't stop the events feed scraper when a page returns 0 events.",
    )

    # Country / Section knobs
    scrape_parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max URLs to process for countries/sections scrapers (0 = all).",
    )

    # Archive
    scrape_parser.add_argument(
        "--archive",
        action="store_true",
        help="Save scraped data as JSON in the data/ directory.",
    )

    return parser


# ---------------------------------------------------------------------------
# Scraper runners
# ---------------------------------------------------------------------------


async def _run_countries(
    client: Client,
    *,
    limit: int = 0,
    archive: bool = False,
) -> None:
    from src.scrapers.country_scraper import CountryScraper

    scraper = CountryScraper(limit=limit)
    await scraper.run(
        client,
        archive_filename="countries.json" if archive else None,
    )


async def _run_sections(
    client: Client,
    *,
    limit: int = 0,
    archive: bool = False,
) -> None:
    from src.scrapers.section_scraper import SectionScraper

    scraper = SectionScraper(limit=limit)
    await scraper.run(
        client,
        archive_filename="sections.json" if archive else None,
    )


async def _run_events(
    client: Client,
    *,
    start_page: int = 0,
    end_page: int = 0,
    concurrency: int = 10,
    stop_on_empty: bool = True,
    archive: bool = False,
) -> None:
    from src.scrapers.event_scraper import EventScraper

    scraper = EventScraper(
        start_page=start_page,
        end_page=end_page,
        concurrency=concurrency,
        stop_on_empty=stop_on_empty,
    )
    await scraper.run(
        client,
        archive_filename="events.json" if archive else None,
    )


# Ordered pipeline mapping (FK-safe: countries → sections → events)
_TARGET_ORDER = ["countries", "sections", "events"]


async def _dispatch(args: argparse.Namespace, client: Client) -> None:
    """Run the requested scraper target(s) in the correct order."""
    targets = _TARGET_ORDER if args.target == "all" else [args.target]

    for target in targets:
        logger.info("=" * 60)
        logger.info("Starting: %s", target.upper())
        logger.info("=" * 60)

        try:
            if target == "countries":
                await _run_countries(
                    client, limit=args.limit, archive=args.archive
                )
            elif target == "sections":
                await _run_sections(
                    client, limit=args.limit, archive=args.archive
                )
            elif target == "events":
                await _run_events(
                    client,
                    start_page=args.start_page,
                    end_page=args.end_page,
                    concurrency=args.concurrency,
                    stop_on_empty=not args.continue_on_empty,
                    archive=args.archive,
                )
        except Exception:
            logger.exception("Scraper '%s' failed.", target)
            sys.exit(1)

        logger.info("Finished: %s ✓", target.upper())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "scrape":
        client = _init_supabase()
        asyncio.run(_dispatch(args, client))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
