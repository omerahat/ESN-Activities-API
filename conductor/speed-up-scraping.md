# Implementation Plan: Speed Up ESN Scraping

## Objective
Speed up the ESN Activities scraper pipeline to drastically reduce total execution time while maintaining exact data parity and ensuring the bot remains undetectable.

## Key Files & Context
- `src/scrapers/event_scraper.py`: Core logic for Phase 1 (feed scraping) and Phase 2 (detail enrichment).
- `manage.py`: CLI orchestration and default concurrency settings.
- `src/menu_scraper_funcs.py`: Shared async HTTP fetching functions with jitter and backoff.

## Implementation Steps

### 1. Concurrent Feed Scraping (Phase 1)
Currently, `event_scraper.py` fetches the paginated feed strictly sequentially (`while True`, page by page).
- **Change:** Refactor `_scrape_feed` to fetch pages in concurrent batches (e.g., chunks of 5-10 pages at a time).
- **Mechanism:** Use `asyncio.gather` for a batch of pages. If any page in the batch returns 0 events (and `stop_on_empty` is True), we process the valid pages from that batch, discard the empty ones, and break out of the loop.
- **Benefit:** Reduces Phase 1 time by a factor of the batch size without risking missed events.

### 2. Optimize Delay Handling (Phase 2)
In `EventScraper._fetch_single_detail`, an artificial delay (`await asyncio.sleep(random.uniform(0.1, 0.3))`) is currently placed **inside** the semaphore block.
- **Change:** Remove or relocate the `asyncio.sleep` to outside the semaphore, OR rely entirely on the built-in `jitter_ms` of `fetch_html_async` which applies pre-request jitter. 
- **Mechanism:** By preventing tasks from holding a semaphore slot while simply sleeping, we free up concurrency for active HTTP I/O.
- **Stealth:** The `fetch_html_async` function natively supports `jitter_ms` (randomized wait before request) and exponential backoff on rate limits (429), ensuring requests appear natural without artificially starving the connection pool.

### 3. Increase Concurrency Defaults
- **Change:** Increase the default `--concurrency` argument in `manage.py` from 10 to a higher safe baseline (e.g., 20 or 30).
- **Change:** Increase `EventScraper.DEFAULT_CONCURRENCY` to match.
- **Mechanism:** Combined with exponential backoff on 429 (Too Many Requests), the scraper will naturally find the fastest safe speed the server can handle.

## Verification & Testing
1. **Dry Run:** Execute `python manage.py scrape --target events --limit 50` (or similar) to verify no errors occur.
2. **Data Integrity:** Run the scraper and compare output (e.g., using `--archive` flag) to a previously generated JSON archive to ensure all fields (causes, sdgs, dates) remain identical.
3. **Detection Monitoring:** Observe logs for `HTTP 429` (Too Many Requests) or `503` errors. The backoff mechanism should smoothly handle these, but if they are too frequent, the concurrency default can be dialed back slightly.