# Notes – Google Maps Scraper

## Flow

1. `main.py` loads place IDs from Parquet file.
2. Splits into batches (`BATCH_SIZE`).
3. Spawns `MAX_WORKERS` parallel processes.
4. Each worker:
   - Starts Chrome driver (unique binary & profile).
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
8. Logs are saved in `data/output/logs/`.

## Key Configs

| Var | Default | Description |
|-----|---------|-------------|
| `CHROME_VERSION_MAIN` | 135 | Must match installed Chrome version |
| `MAX_WORKERS` | 2 | Parallel workers (safe for 8-16GB RAM) |
| `BATCH_SIZE` | 100 | Places per batch |
| `FLUSH_INTERVAL` | 25 | Flush to disk every N places (creates shard) |
| `RESTART_EVERY` | 15 | Restart driver every N places |

## Optimizations

- Site isolation disabled (`--disable-site-isolation-trials`)
- Unique driver executable per worker (`/tmp/uc_driver_*`)
- Buffer writes instead of `pd.concat` per record
- Random scroll patterns for reviews

## Common Issues

- **Segfault**: Usually due to binary collision – fixed with `driver_executable_path` per worker.
- **Memory bloat**: Caused by renderer processes – fixed with `RESTART_EVERY`.
- **No reviews**: Some places have zero reviews; handled gracefully.

## Output Structure

Each Parquet file contains:
- `place_id`: Unique Google Maps identifier
- `overview`: dict with name, rating, price_range, phone, links
- `review`: dict with rating, total_reviews, keyword_tags, reviews list
- `about`: dict with facility/amenity categories and availability