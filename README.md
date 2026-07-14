# Coffee Shop Google Maps Scraper

Parallel scraper for coffee shop business data from Google Maps using `undetected-chromedriver`.  
Extracts overview, reviews, and about section.

## Quick Start

```bash
git clone https://github.com/mkhafidm/coffee-shop-gmaps-scraper.git
cd coffee-shop-gmaps-scraper
pip install -r requirements.txt
python main.py
```

Edit `config.py` to set `CHROME_VERSION_MAIN`, `MAX_WORKERS`, and file paths.

### Usage

| Command                                       | What it does                                                    |
| :-------------------------------------------- | :-------------------------------------------------------------- |
| `python main.py`                              | Run with default config.                                        |
| `python main.py --workers 4 --batch-size 100` | Use 4 workers, 100 places per batch.                            |
| `python main.py --start 500`                  | Resume from place index 500.                                    |
| `python main.py --start 1000 --end 2000`      | Scrape a specific index range.                                  |
| `python main.py --batch-start 10`             | Force batch numbering to start at 10 instead of auto-detecting. |

### Output
Results go to `data/output/`:
- `overview/`, `review/`, `about/` → final `.parquet` files per batch.
Temporary `_shard*.parquet` files are auto-merged and deleted after each batch finishes.

### Disclaimer

For educational purposes only. Users are responsible for complying with Google's Terms of Service.