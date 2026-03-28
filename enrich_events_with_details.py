"""
Merge Phase 2 detail pages into each event in a JSON array (events.json shape).

Reads each object's event_page_link, fetches detail HTML concurrently (httpx), parses
with parse_event_details, and sets event["details"]. Writes the full array back with
an atomic replace (same path).

Example:
  python3 enrich_events_with_details.py --limit 2 --concurrency 40
  python3 enrich_events_with_details.py --skip-existing --concurrency 20

Back up events.json before a full run; the script prints a reminder on startup.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

from src.detail_scraper_funcs import parse_event_details, scrape_event_details_async
from src.menu_scraper_funcs import create_async_client

DEFAULT_FILE = "events.json"
DEFAULT_CONCURRENCY = 40
DEFAULT_PROGRESS_EVERY = 100
DEFAULT_TIMEOUT = 20.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich ESN events JSON with per-activity details (Phase 2 async scraper)."
    )
    parser.add_argument(
        "--file",
        "-f",
        type=Path,
        default=Path(DEFAULT_FILE),
        help=f"Path to JSON array file. Default: {DEFAULT_FILE}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help="If > 0, only enrich this many events starting at --offset. Default: 0 (all in range).",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        metavar="N",
        help="Index of first event to enrich. Default: 0.",
    )
    parser.add_argument(
        "--concurrency",
        "-c",
        type=int,
        default=DEFAULT_CONCURRENCY,
        metavar="N",
        help=f"Max concurrent HTTP requests. Default: {DEFAULT_CONCURRENCY}.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Retries on 429/502/503 and transient errors. Default: 3.",
    )
    parser.add_argument(
        "--backoff-base",
        type=float,
        default=1.0,
        help="Base seconds for exponential backoff between retries. Default: 1.0.",
    )
    parser.add_argument(
        "--jitter-ms",
        type=float,
        default=100.0,
        help="Random jitter in milliseconds added to waits. Default: 100.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help=f"Per-request timeout in seconds. Default: {DEFAULT_TIMEOUT}.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=DEFAULT_PROGRESS_EVERY,
        metavar="N",
        help=f"Print progress every N completed HTTP fetches. Default: {DEFAULT_PROGRESS_EVERY}.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip events whose details already look populated (resume-friendly).",
    )
    return parser.parse_args()


def details_looks_populated(details: Any) -> bool:
    """True when details dict appears to have real scraped data (not only empty placeholders)."""
    if not isinstance(details, dict):
        return False
    if details.get("main_image_url"):
        return True
    if details.get("description") or details.get("goal_of_activity"):
        return True
    if details.get("causes") or details.get("types_of_activity"):
        return True
    if details.get("sdgs") or details.get("objectives"):
        return True
    if details.get("registration_link") or details.get("outcomes"):
        return True
    if details.get("total_participants") is not None:
        return True
    if details.get("detailed_location"):
        return True
    return False


def atomic_write_json(path: Path, data: Any) -> None:
    """Write JSON to path via a temp file in the same directory, then os.replace."""
    path = path.resolve()
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        suffix=".json.tmp",
        prefix=path.name + ".",
        dir=str(parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmp_f:
            json.dump(data, tmp_f, ensure_ascii=False, indent=4)
            tmp_f.flush()
            os.fsync(tmp_f.fileno())
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


async def _run_http_tasks(
    args: argparse.Namespace,
    work: List[Tuple[int, str]],
) -> List[Tuple[int, Dict[str, Any]]]:
    """Fetch and parse details for each (index, url); returns list of (index, details)."""
    total_tasks = len(work)
    if total_tasks == 0:
        return []

    concurrency = max(1, args.concurrency)
    semaphore = asyncio.Semaphore(concurrency)
    progress_lock = asyncio.Lock()
    completed = 0
    start_t = time.monotonic()

    async with create_async_client(concurrency, timeout=args.timeout) as client:

        async def fetch_one(ii: int, uurl: str) -> Tuple[int, Dict[str, Any]]:
            nonlocal completed
            try:
                details = await scrape_event_details_async(
                    uurl,
                    client,
                    semaphore,
                    max_retries=args.max_retries,
                    backoff_base=args.backoff_base,
                    jitter_ms=args.jitter_ms,
                )
            except Exception as exc:
                print(f"Unexpected error for {uurl}: {exc}", file=sys.stderr)
                details = parse_event_details("")

            async with progress_lock:
                completed += 1
                elapsed = time.monotonic() - start_t
                pe = max(1, args.progress_every)
                if completed % pe == 0 or completed == total_tasks:
                    pct = 100.0 * completed / total_tasks
                    eta_s = (
                        (elapsed / completed) * (total_tasks - completed)
                        if completed > 0
                        else 0.0
                    )
                    print(
                        f"Progress: {completed}/{total_tasks} ({pct:.1f}%) "
                        f"elapsed {elapsed:.1f}s ETA {eta_s:.1f}s",
                        flush=True,
                    )
            return (ii, details)

        results = await asyncio.gather(
            *[fetch_one(i, u) for i, u in work],
            return_exceptions=False,
        )
    return list(results)


def main() -> None:
    asyncio.run(async_main())


async def async_main() -> None:
    args = parse_args()
    path: Path = args.file
    if not path.is_file():
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)

    print(
        "Tip: back up your JSON before a long run (cp events.json events.json.bak).",
        flush=True,
    )

    with path.open(encoding="utf-8") as f:
        events = json.load(f)

    if not isinstance(events, list):
        print("JSON root must be an array of event objects.", file=sys.stderr)
        sys.exit(1)

    n = len(events)
    start = max(0, args.offset)
    if start >= n:
        print(f"Offset {start} is past end of list ({n} events).", file=sys.stderr)
        sys.exit(1)

    end = n
    if args.limit > 0:
        end = min(start + args.limit, n)

    work: List[Tuple[int, str]] = []
    skipped_non_dict = 0
    skipped_existing = 0
    skipped_no_link = 0

    for i in range(start, end):
        ev = events[i]
        if not isinstance(ev, dict):
            skipped_non_dict += 1
            continue

        if args.skip_existing and details_looks_populated(ev.get("details")):
            skipped_existing += 1
            continue

        link = ev.get("event_page_link")
        if not link or not isinstance(link, str):
            skipped_no_link += 1
            ev["details"] = parse_event_details("")
            continue

        work.append((i, link))

    print(
        f"Starting async enrich: {len(work)} HTTP task(s), index range [{start}, {end - 1}], "
        f"concurrency={args.concurrency}, "
        f"skipped_non_dict={skipped_non_dict}, skipped_existing={skipped_existing}",
        flush=True,
    )

    if work:
        http_results = await _run_http_tasks(args, work)
        for idx, details in http_results:
            events[idx]["details"] = details

    atomic_write_json(path, events)

    http_done = len(work)
    skipped_other = skipped_non_dict + skipped_existing
    print(
        f"Done. Wrote {path} ({n} events). "
        f"HTTP fetches this run: {http_done}; "
        f"skipped (non-dict or --skip-existing): {skipped_other}; "
        f"missing event_page_link (empty details applied): {skipped_no_link}.",
        flush=True,
    )


if __name__ == "__main__":
    main()
