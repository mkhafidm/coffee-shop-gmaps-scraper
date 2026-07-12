# Google Maps Scraper

Parallel scraper for Google Maps business data using `undetected-chromedriver`.  
Extracts overview, reviews, and about section.

## Quick Start

```bash
git clone https://github.com/yourusername/gmaps-scraper.git
cd gmaps-scraper
pip install -r requirements.txt
python main.py
Edit config.py to set CHROME_VERSION_MAIN, MAX_WORKERS, and file paths.

Usage
python main.py → runs test (2 workers, 5 places each)

python main.py --workers 2 --batch-size 60 → production with 2 workers

python main.py --start 500 → resume from place index 500

Notes
Driver restarts every 15 places to free memory

Reviews are limited to 50 per place

Output is saved as Parquet in data/output/

Disclaimer
For educational purposes only. Users are responsible for complying with Google's ToS.