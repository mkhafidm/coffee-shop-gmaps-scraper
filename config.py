import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Paths
INPUT_PARQUET = os.path.join(BASE_DIR, "data", "input", "kedai_kopi_all.parquet")
OUTPUT_DIR_BASE = os.path.join(BASE_DIR, "data", "output")

# Batch
BATCH_SIZE = 100
START_INDEX = 0

# Delays (seconds)
RECORD_DELAY_RANGE = (3, 7)
BATCH_DELAY_RANGE = (40, 70)

# Retry
MAX_RETRIES = 5

# Chrome
HEADLESS = True
CHROME_VERSION_MAIN = 135
MAX_WORKERS = 2

# Memory optimization
FLUSH_INTERVAL = 5
RESTART_EVERY = 15

# User Agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.7049.114 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.6998.89 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.6943.60 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.7049.114 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.6998.89 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.7049.114 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.7049.114 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.6998.89 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.7049.114 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Apple Silicon Mac OS X 14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.7049.114 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.7049.114 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.7049.114 Safari/537.36",
    "Mozilla/5.0 (X11; Fedora; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.6998.89 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.6943.60 Safari/537.36",
]

# Proxies (optional)
PROXIES = [

]