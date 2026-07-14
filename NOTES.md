# Notes – Google Maps Scraper

## Flow

1. `main.py` loads place IDs from Parquet file.
2. Splits into batches (`BATCH_SIZE`).
3. Spawns `MAX_WORKERS` parallel processes.
4. Each worker:
   - Starts Chrome driver (unique binary & profile, forced English locale).
   - Visits `https://www.google.com/maps/place/?q=place_id:{ID}`.
   - Refreshes page to load dynamic content.
   - Clicks consent popup if appears.
   - Scrapes:
     - **Overview**: name, rating, total reviews, price range, phone, links.
     - **Reviews**: rating, total count, keyword tags, up to 50 latest reviews.
     - **About**: amenities/facilities with availability status.
5. Data is buffered in memory and flushed to **shard** Parquet files every `FLUSH_INTERVAL` places (default 25).
6. At the end of the batch, all shards for each data type (overview, review, about) are merged into a single final `.parquet` file, then the shards are deleted.
7. Driver restarts every `RESTART_EVERY` places (default 15) to prevent memory bloat.
8. Batch numbers auto-increment per output dir (`get_next_batch_number`), or can be forced with `--batch-start` — useful for keeping batch numbers unique/ordered when running multiple notebooks/sessions in parallel.
9.  Logs are saved in `data/output/logs/`.

## Key Configs

| Var | Default | Description |
|-----|---------|-------------|
| `CHROME_VERSION_MAIN` | 135 | Must match installed Chrome version |
| `MAX_WORKERS` | 2 | Parallel workers (safe for 8-16GB RAM) |
| `BATCH_SIZE` | 100 | Places per batch |
| `FLUSH_INTERVAL` | 25 | Flush to disk every N places (creates shard) |
| `RESTART_EVERY` | 15 | Restart driver every N places |

## Language / Locale Handling

Google determines the consent page and Maps UI language based on the server's IP geolocation, not the machine's system language. If a scraping environment happens to run from an IP that geolocates outside the target region, the consent popup can render in an unexpected language — which breaks any consent-handler logic that only looks for English (or another specific language) text. Once that happens, every subsequent place gets stuck on the consent page and fails silently, since the scraper never leaves that page.

Fix: force English locale at the Chrome level so this can't recur regardless of server IP:
- `--lang=en-US`
- `--accept-lang=en-US,en`

Both flags are set in `_build_driver()` for every worker.

## Optimizations

- Unique driver executable & profile dir per worker (`/tmp/uc_driver_*`, `/tmp/uc_profile_*`, includes timestamp to avoid stale `SingletonLock` collisions on restart)
- Retry with backoff on driver creation itself (`get_driver`), not just on scraping steps
- Explicit `BrowserCrashedError` raised from scrape functions instead of string-matching exception messages — more reliable crash detection than guessing from `str(exc)`
- Buffer writes instead of `pd.concat` per record
- Random scroll patterns for reviews

## Common Issues

- **Segfault**: Usually due to binary collision – fixed with `driver_executable_path` per worker.
- **Memory bloat**: Caused by renderer processes – fixed with `RESTART_EVERY`.
- **No reviews**: Some places have zero reviews; handled gracefully.
- **Consent page in wrong language / scraping fails on every place**: See "Language / Locale Handling" above — check debug logs for `Current URL` starting with `consent.google.com` in an unexpected language.

## Output Structure

Each Parquet file contains:
- `place_id`: Unique Google Maps identifier
- `overview`: dict with name, rating, price_range, phone, links
- `review`: dict with rating, total_reviews, keyword_tags, reviews list
- `about`: dict with facility/amenity categories and availability