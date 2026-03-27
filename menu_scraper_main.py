import argparse
import asyncio
import json

from src.menu_scraper_funcs import (
    save_to_file,
    scrape_events_multi_page,
    scrape_events_multi_page_async,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape ESN activities across several pages.")
    parser.add_argument(
        "--start-page",
        type=int,
        default=0,
        help="First page index to scrape (inclusive). Default: 0",
    )
    parser.add_argument(
        "--end-page",
        type=int,
        default=0,
        help="Last page index to scrape (inclusive). Default: 0",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="events.json",
        help="Path to write JSON results. Default: events.json",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Do not write a JSON file (print to stdout only).",
    )
    parser.add_argument(
        "--continue-on-empty",
        action="store_true",
        help="Continue scraping the page range even if a page returns no events.",
    )
    parser.add_argument(
        "--async-fetch",
        action="store_true",
        dest="use_async",
        help="Use parallel httpx + asyncio (connection reuse, bounded concurrency).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Max concurrent requests when using --async-fetch. Default: 10",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Retries for 429/502/503 and transient errors (async). Default: 3",
    )
    parser.add_argument(
        "--backoff-base",
        type=float,
        default=1.0,
        help="Base seconds for exponential backoff when async retries. Default: 1.0",
    )
    parser.add_argument(
        "--jitter-ms",
        type=float,
        default=100.0,
        help="Random delay spread in ms before each async request and on retries. Default: 100",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP timeout in seconds for async client. Default: 20",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.use_async:
        events = asyncio.run(
            scrape_events_multi_page_async(
                start_page=args.start_page,
                end_page=args.end_page,
                stop_on_empty=not args.continue_on_empty,
                max_concurrent=args.concurrency,
                max_retries=args.max_retries,
                backoff_base=args.backoff_base,
                jitter_ms=args.jitter_ms,
                timeout=args.timeout,
            )
        )
    else:
        events = scrape_events_multi_page(
            start_page=args.start_page,
            end_page=args.end_page,
            stop_on_empty=not args.continue_on_empty,
        )

    print(json.dumps(events, indent=4, ensure_ascii=False))

    if not args.no_save:
        save_to_file(events, args.output)
        print(f"\nSaved {len(events)} events to {args.output}", flush=True)


if __name__ == "__main__":
    main()
