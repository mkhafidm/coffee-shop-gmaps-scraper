import gc
import os
import random
import time
import json
import logging
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Tuple, Any, Optional
import pandas as pd
import undetected_chromedriver as uc
from src import overview, review, about, utils
import config


# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------

def load_place_ids(file_path: str) -> List[str]:
    """Load unique place IDs from a Parquet file."""
    df = pd.read_parquet(file_path)
    return df["place_id"].dropna().unique().tolist()


def split_into_batches(items: List[Any], batch_size: int):
    """Yield successive batches of size batch_size."""
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]


# -----------------------------------------------------------------------------
# Driver and logger setup
# -----------------------------------------------------------------------------

def get_driver(worker_id: int = 0):
    """
    Create an undetected Chrome driver with isolated binary and profile.
    Each worker gets its own driver executable path to avoid file collisions.
    """
    user_agent = random.choice(config.USER_AGENTS) if config.USER_AGENTS else ""
    proxy = random.choice(config.PROXIES) if config.PROXIES else None

    options = uc.ChromeOptions()
    if config.HEADLESS:
        options.add_argument("--headless")
    options.add_argument(f"--user-agent={user_agent}")
    if proxy:
        options.add_argument(f"--proxy-server={proxy}")

    # Memory & stability
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--js-flags=--max-old-space-size=512")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--remote-debugging-port=0")

    # Disable site isolation to reduce renderer processes
    options.add_argument("--disable-site-isolation-trials")
    options.add_argument("--disable-features=IsolateOrigins,site-per-process")
    options.add_argument("--process-per-site")
    options.add_argument("--disk-cache-size=1")
    options.add_argument("--media-cache-size=1")
    options.add_argument("--aggressive-cache-discard")

    # Unique user data directory per worker
    profile_dir = f"/tmp/uc_profile_{worker_id}_{os.getpid()}"
    options.add_argument(f"--user-data-dir={profile_dir}")

    # Unique driver binary per worker (avoids segfault)
    driver_exe_path = f"/tmp/uc_driver_{worker_id}_{os.getpid()}"

    return uc.Chrome(
        options=options,
        version_main=config.CHROME_VERSION_MAIN,
        driver_executable_path=driver_exe_path,
        user_multi_procs=True,
    )


def setup_logger(batch_number: int, output_dir: str) -> logging.Logger:
    """Configure logger for a specific batch."""
    log_dir = os.path.join(output_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(f"batch_{batch_number}")
    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s - %(message)s")
    file_handler = logging.FileHandler(
        os.path.join(log_dir, f"batch_{batch_number:03}.log")
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logger.setLevel(logging.INFO)
    return logger


# -----------------------------------------------------------------------------
# Batch processing
# -----------------------------------------------------------------------------

def _flush_buffer(buffer_data: List[dict], filepath: str) -> None:
    """Write buffer (list of dicts) to Parquet file, appending if exists."""
    if not buffer_data:
        return
    df_new = pd.DataFrame(buffer_data)
    if os.path.exists(filepath):
        df_existing = pd.read_parquet(filepath)
        pd.concat([df_existing, df_new], ignore_index=True).to_parquet(filepath, index=False)
    else:
        df_new.to_parquet(filepath, index=False)


def process_batch(
    batch: List[str],
    batch_number: int,
    output_dir: str,
    worker_index: int = 0,
    flush_interval: int = config.FLUSH_INTERVAL,
    restart_every: int = config.RESTART_EVERY,
):
    """
    Scrape a batch of places.

    Args:
        batch: List of place IDs.
        batch_number: Batch number for naming files.
        output_dir: Root output directory.
        worker_index: Worker ID for driver isolation.
        flush_interval: Write to disk every N places.
        restart_every: Restart Chrome driver every N places.
    """
    logger = setup_logger(batch_number, output_dir)

    # File paths for each data type
    filepaths = {
        "overview": os.path.join(output_dir, "overview", f"batch_{batch_number:03}_overview.parquet"),
        "review":   os.path.join(output_dir, "review",   f"batch_{batch_number:03}_review.parquet"),
        "about":    os.path.join(output_dir, "about",    f"batch_{batch_number:03}_about.parquet"),
    }
    for path in filepaths.values():
        os.makedirs(os.path.dirname(path), exist_ok=True)

    # Buffers to accumulate before flushing
    overview_buffer: List[dict] = []
    review_buffer: List[dict] = []
    about_buffer: List[dict] = []

    # Initialize driver
    driver = get_driver(worker_id=worker_index)

    try:
        for idx, place_id in enumerate(batch, start=1):
            # Restart driver periodically to free accumulated renderer processes
            if idx > 1 and (idx - 1) % restart_every == 0:
                logger.info(f"Restarting driver after {restart_every} places...")
                driver.quit()
                time.sleep(3)
                driver = get_driver(worker_id=worker_index)

            t0 = time.time()
            logger.info(f"[{idx}/{len(batch)}] {place_id}")

            url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
            driver.get(url)
            time.sleep(random.uniform(1, 2))
            driver.refresh()
            time.sleep(random.uniform(2, 4))
            utils.handle_consent_popup(driver)

            # --- Scrape overview ---
            ov = overview.scrape_overview(driver, place_id) or {"error": "scrape_failed"}
            if ov.get("error"):
                logger.warning(f"Overview failed for {place_id}")
            overview_buffer.append({"place_id": place_id, "overview": ov})

            # --- Scrape reviews ---
            rv = review.scrape_review(driver, place_id) or {}
            rv_to_save = dict(rv)
            rv_to_save["keyword_tags"] = json.dumps(rv.get("keyword_tags", {}), ensure_ascii=False)
            review_buffer.append({"place_id": place_id, "review": rv_to_save})

            # --- Scrape about ---
            ab = about.scrape_about(driver, place_id) or {}
            ab_to_save = {"about": json.dumps(ab.get("about", {}), ensure_ascii=False)}
            about_buffer.append({"place_id": place_id, "about": ab_to_save})

            # --- Flush to disk if buffer is full ---
            if idx % flush_interval == 0:
                _flush_buffer(overview_buffer, filepaths["overview"])
                _flush_buffer(review_buffer,   filepaths["review"])
                _flush_buffer(about_buffer,    filepaths["about"])
                overview_buffer.clear()
                review_buffer.clear()
                about_buffer.clear()
                gc.collect()  # Force release memory

            elapsed = time.time() - t0
            logger.info(f"{place_id} done {elapsed:.1f}s")
            time.sleep(random.uniform(*config.RECORD_DELAY_RANGE))

    finally:
        # Flush remaining data
        _flush_buffer(overview_buffer, filepaths["overview"])
        _flush_buffer(review_buffer,   filepaths["review"])
        _flush_buffer(about_buffer,    filepaths["about"])
        driver.quit()


def run_batch_worker(args: Tuple) -> Tuple[int, str, Optional[str]]:
    """
    Wrapper for process_batch to be used with ProcessPoolExecutor.
    Returns (batch_number, status, error_message).
    """
    batch, batch_number, output_dir, worker_index = args
    logger = setup_logger(batch_number, output_dir)

    # Stagger startup to reduce I/O contention
    stagger_delay = worker_index * random.uniform(8, 12)
    logger.info(f"Worker #{worker_index} waiting {stagger_delay:.1f}s before start...")
    time.sleep(stagger_delay)

    t0 = time.time()
    time.sleep(random.uniform(0, 8))  # extra jitter

    try:
        process_batch(batch, batch_number, output_dir, worker_index)
        logger.info(f"✅ Batch #{batch_number} done {time.time() - t0:.1f}s")
        return (batch_number, "success", None)
    except Exception as e:
        logger.error(f"❌ Batch #{batch_number} failed: {e}")
        return (batch_number, "failed", str(e))


# -----------------------------------------------------------------------------
# Main entry points
# -----------------------------------------------------------------------------

def main():
    """Production runner with command-line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=config.INPUT_PARQUET, help="Input Parquet file path")
    parser.add_argument("--start", type=int, default=config.START_INDEX, help="Index to start from")
    parser.add_argument("--batch-size", type=int, default=config.BATCH_SIZE, help="Size of each batch")
    parser.add_argument("--workers", type=int, default=config.MAX_WORKERS, help="Number of parallel workers")
    parser.add_argument("--output", default=config.OUTPUT_DIR_BASE, help="Output directory")
    args = parser.parse_args()

    all_place_ids = load_place_ids(args.input)
    print(f"Total place_ids: {len(all_place_ids)}")
    place_ids_to_process = all_place_ids[args.start:]
    print(f"Start: {args.start}, Remaining: {len(place_ids_to_process)}")

    batches = list(split_into_batches(place_ids_to_process, args.batch_size))
    offset_batch = args.start // args.batch_size

    tasks = []
    for batch_idx, batch in enumerate(batches, start=1):
        batch_number = batch_idx + offset_batch
        worker_index = batch_idx - 1
        tasks.append(
            (batch, batch_number, args.output, worker_index)
        )

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(run_batch_worker, task): task[1] for task in tasks}
        for future in as_completed(futures):
            batch_num, status, error = future.result()
            if status == "success":
                print(f"✅ Batch #{batch_num} done")
            else:
                print(f"❌ Batch #{batch_num} failed: {error}")


def test_small():
    """Test with a single worker on 5 places."""
    all_ids = load_place_ids(config.INPUT_PARQUET)
    process_batch(all_ids[:5], batch_number=999, output_dir=config.OUTPUT_DIR_BASE)


def test_small_parallel(n_workers: int = 2, places_per_worker: int = 5):
    """Test with parallel workers on a small dataset."""
    all_place_ids = load_place_ids(config.INPUT_PARQUET)
    tasks = []
    for i in range(n_workers):
        start = i * places_per_worker
        end = start + places_per_worker
        batch = all_place_ids[start:end]
        tasks.append((batch, 900 + i, config.OUTPUT_DIR_BASE, i))

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(run_batch_worker, task): task[1] for task in tasks}
        for future in as_completed(futures):
            batch_num, status, error = future.result()
            if status == "success":
                print(f"✅ Batch #{batch_num} done")
            else:
                print(f"❌ Batch #{batch_num} failed: {error}")


if __name__ == "__main__":
    main()
    # test_small_parallel(n_workers=2, places_per_worker=5)