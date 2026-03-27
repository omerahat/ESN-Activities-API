"""
Load events.json and upsert rows into Supabase (table esn_events by default).

Environment (e.g. .env — not committed):
  SUPABASE_URL                      — project URL
  SUPABASE_SERVICE_ROLE_KEY or      — service role secret (bypasses RLS for bulk load)
  SUPABASE_KEY                      — same as above if the other name is not set

Apply the migration first: supabase/migrations/20250327120000_create_esn_events.sql
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Iterator

from dotenv import load_dotenv
from supabase import create_client


DEFAULT_TABLE = "esn_events"
DEFAULT_FILE = "events.json"
CONFLICT_COLUMN = "event_page_link"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload scraped ESN events JSON to Supabase in batches."
    )
    parser.add_argument(
        "--file",
        "-f",
        type=Path,
        default=Path(DEFAULT_FILE),
        help=f"Path to JSON array file. Default: {DEFAULT_FILE}",
    )
    parser.add_argument(
        "--table",
        "-t",
        type=str,
        default=DEFAULT_TABLE,
        help=f"Supabase table name. Default: {DEFAULT_TABLE}",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        metavar="N",
        help="Rows per upsert request. Default: 500",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help="If > 0, only process the first N events (for testing). Default: 0 (all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Load and map rows but do not call Supabase; print summary and sample.",
    )
    return parser.parse_args()


def event_to_row(obj: dict[str, Any]) -> dict[str, Any] | None:
    link = obj.get("event_page_link")
    if not link or not isinstance(link, str):
        return None
    name = obj.get("event_name")
    if name is None or not isinstance(name, str):
        return None
    ed = obj.get("event_date")
    if not isinstance(ed, dict):
        return None
    return {
        "event_name": name,
        "organizer_section": obj.get("organizer_section"),
        "event_date": ed,
        "is_upcoming": bool(obj.get("is_upcoming", False)),
        "organizer_section_website_link": obj.get("organizer_section_website_link"),
        "location": obj.get("location"),
        "event_page_link": link,
    }


def batched(rows: list[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def get_supabase_credentials_or_exit() -> tuple[str, str]:
    """Load .env and return (url, key). Fail fast before reading a large JSON file."""
    load_dotenv()
    url = os.environ.get("SUPABASE_URL", "").strip()
    key = (
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        or os.environ.get("SUPABASE_KEY", "").strip()
    )
    if not url or not key:
        print(
            "Missing SUPABASE_URL or a Supabase key "
            "(SUPABASE_SERVICE_ROLE_KEY or SUPABASE_KEY).",
            file=sys.stderr,
        )
        sys.exit(1)
    return url, key


def main() -> None:
    args = parse_args()
    url = ""
    key = ""
    if not args.dry_run:
        url, key = get_supabase_credentials_or_exit()

    path: Path = args.file
    if not path.is_file():
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)

    with path.open(encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        print("JSON root must be an array of event objects.", file=sys.stderr)
        sys.exit(1)

    if args.limit and args.limit > 0:
        data = data[: args.limit]

    rows: list[dict[str, Any]] = []
    skipped = 0
    for item in data:
        if not isinstance(item, dict):
            skipped += 1
            continue
        row = event_to_row(item)
        if row is None:
            skipped += 1
            continue
        rows.append(row)

    total = len(rows)
    batch_size = max(1, args.batch_size)
    num_batches = (total + batch_size - 1) // batch_size if total else 0

    print(
        f"Loaded {len(data)} objects from {path}; "
        f"{total} valid rows, {skipped} skipped; "
        f"{num_batches} batch(es) of up to {batch_size}."
    )

    if args.dry_run:
        if rows:
            sample = rows[0]
            print("Dry-run sample row keys:", list(sample.keys()))
            print("Dry-run sample event_page_link:", sample.get("event_page_link"))
        return

    client = create_client(url, key)
    table = args.table

    for bi, batch in enumerate(batched(rows, batch_size)):
        try:
            client.table(table).upsert(batch, on_conflict=CONFLICT_COLUMN).execute()
        except Exception as e:
            print(f"Batch {bi + 1}/{num_batches} failed: {e}", file=sys.stderr)
            sys.exit(1)
        print(f"Batch {bi + 1}/{num_batches} OK ({len(batch)} rows).", flush=True)

    print(f"Done. Upserted {total} rows into {table!r}.")


if __name__ == "__main__":
    main()
