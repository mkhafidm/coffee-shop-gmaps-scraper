import gc
import glob
import os
import random
import re
import time
import json
import logging
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Tuple, Any, Optional, Dict
import pandas as pd
import undetected_chromedriver as uc
from src import overview, review, about, utils
from src.utils import BrowserCrashedError
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


def load_done_place_ids(output_dir: str) -> set:
    """
    Scan finalized (non-shard) overview parquet files and return the set of
    place_ids already processed. A row existing is enough to count as "done" -
    we don't distinguish success/failure here, to avoid re-scraping places
    that consistently fail.
    """
    pattern = os.path.join(output_dir, "overview", "batch_*_overview.parquet")
    done = set()
    for f in glob.glob(pattern):
        if "_shard" in f:
            continue
        try:
            df = pd.read_parquet(f, columns=["place_id"])
            done.update(df["place_id"].tolist())
        except Exception:
            continue
    return done


def get_next_batch_number(output_dir: str) -> int:
    """Find the highest existing finalized batch number and return the next one."""
    pattern = os.path.join(output_dir, "overview", "batch_*_overview.parquet")
    max_num = 0
    for f in glob.glob(pattern):
        if "_shard" in f:
            continue
        match = re.match(r"batch_(\d+)_overview\.parquet", os.path.basename(f))
        if match:
            max_num = max(max_num, int(match.group(1)))
    return max_num + 1


# -----------------------------------------------------------------------------
# Driver and logger setup
# -----------------------------------------------------------------------------

def _build_driver(worker_id: int):
    """Build a single Chrome driver instance. May raise on failure."""
    user_agent = random.choice(config.USER_AGENTS) if config.USER_AGENTS else ""
    proxy = random.choice(config.PROXIES) if config.PROXIES else None

    options = uc.ChromeOptions()
    if config.HEADLESS:
        options.add_argument("--headless")
    options.add_argument(f"--user-agent={user_agent}")
    if proxy:
        options.add_argument(f"--proxy-server={proxy}")

    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--js-flags=--max-old-space-size=4096")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--remote-debugging-port=0")

    options.add_argument("--disk-cache-size=104857600")
    options.add_argument("--media-cache-size=104857600")
    options.add_argument("--aggressive-cache-discard")

    profile_dir = f"/tmp/uc_profile_{worker_id}_{os.getpid()}_{int(time.time())}"
    options.add_argument(f"--user-data-dir={profile_dir}")

    driver_exe_path = f"/tmp/uc_driver_{worker_id}_{os.getpid()}_{int(time.time())}"

    return uc.Chrome(
        options=options,
        version_main=config.CHROME_VERSION_MAIN,
        driver_executable_path=driver_exe_path,
        user_multi_procs=True,
    )


def get_driver(worker_id: int = 0, max_attempts: int = 3):
    """
    Create an undetected Chrome driver, retrying a few times if driver
    creation itself fails (e.g. transient race condition between workers).
    """
    last_exc = None
    for attempt in range(max_attempts):
        try:
            return _build_driver(worker_id)
        except Exception as e:
            last_exc = e
            wait_time = 2 ** attempt + random.uniform(1, 3)
            time.sleep(wait_time)
    raise last_exc


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
# Shard writing and merging
# -----------------------------------------------------------------------------

def merge_shards_and_cleanup(output_dir: str, data_type: str, batch_number: int, logger: logging.Logger):
    """Merge all shards for a batch/data_type into one final file, then delete the shards."""
    data_dir = os.path.join(output_dir, data_type)
    pattern = os.path.join(data_dir, f"batch_{batch_number:03}_{data_type}_shard*.parquet")
    shard_files = sorted(glob.glob(pattern))
    if not shard_files:
        return

    final_path = os.path.join(data_dir, f"batch_{batch_number:03}_{data_type}.parquet")
    dfs = [pd.read_parquet(f) for f in shard_files]
    df_final = pd.concat(dfs, ignore_index=True)
    df_final.to_parquet(final_path, index=False)

    try:
        check = pd.read_parquet(final_path)
        if len(check) == len(df_final):
            for f in shard_files:
                os.remove(f)
        else:
            logger.error(f"Merge verification mismatch for {data_type} batch {batch_number}, keeping shards.")
    except Exception as e:
        logger.error(f"Failed to verify merged file for {data_type} batch {batch_number}: {e}. Keeping shards.")


# -----------------------------------------------------------------------------
# Batch processing
# -----------------------------------------------------------------------------

def process_batch(
    batch: List[str],
    batch_number: int,
    output_dir: str,
    worker_index: int = 0,
    flush_interval: int = config.FLUSH_INTERVAL,
    restart_every: int = config.RESTART_EVERY,
    max_crash_retries: int = 3,
):
    """
    Scrape a batch of places.

    Data is buffered in memory and flushed to a new shard file every
    `flush_interval` places. At the end of the batch, all shards are merged
    into one final parquet file per data type, and the shards are deleted.

    If the browser crashes on a place, the driver is restarted and the same
    place is retried, up to `max_crash_retries` times, before giving up on
    that place and moving on.
    """
    logger = setup_logger(batch_number, output_dir)

    dirs = {
        "overview": os.path.join(output_dir, "overview"),
        "review":   os.path.join(output_dir, "review"),
        "about":    os.path.join(output_dir, "about"),
    }
    for d in dirs.values():
        os.makedirs(d, exist_ok=True)

    buffers: Dict[str, List[dict]] = {"overview": [], "review": [], "about": []}
    shard_counters: Dict[str, int] = {"overview": 0, "review": 0, "about": 0}

    def flush_shard(data_type: str):
        if not buffers[data_type]:
            return
        shard_counters[data_type] += 1
        shard_path = os.path.join(
            dirs[data_type],
            f"batch_{batch_number:03}_{data_type}_shard{shard_counters[data_type]:03}.parquet"
        )
        pd.DataFrame(buffers[data_type]).to_parquet(shard_path, index=False)
        buffers[data_type].clear()

    driver = get_driver(worker_id=worker_index)

    def restart_driver(reason: str):
        nonlocal driver
        logger.warning(f"🔄 Restarting driver due to: {reason}")
        try:
            driver.quit()
        except Exception:
            pass
        time.sleep(3)
        driver = get_driver(worker_id=worker_index)

    try:
        idx = 1
        while idx <= len(batch):
            place_id = batch[idx - 1]

            if idx > 1 and (idx - 1) % restart_every == 0:
                restart_driver(f"scheduled restart after {restart_every} places")

            t0 = time.time()
            logger.info(f"[{idx}/{len(batch)}] {place_id}")

            crash_attempts = 0
            place_done = False

            while not place_done:
                try:
                    url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
                    driver.get(url)
                    time.sleep(random.uniform(1, 2))
                    driver.refresh()
                    time.sleep(random.uniform(2, 4))
                    utils.handle_consent_popup(driver)

                    ov = overview.scrape_overview(driver, place_id) or {"error": "scrape_failed"}
                    if ov.get("error"):
                        logger.warning(f"Overview failed for {place_id}")
                    buffers["overview"].append({"place_id": place_id, "overview": ov})

                    rv = review.scrape_review(driver, place_id) or {}
                    rv_to_save = dict(rv)
                    rv_to_save["keyword_tags"] = json.dumps(rv.get("keyword_tags", {}), ensure_ascii=False)
                    buffers["review"].append({"place_id": place_id, "review": rv_to_save})

                    ab = about.scrape_about(driver, place_id) or {}
                    ab_to_save = {"about": json.dumps(ab.get("about", {}), ensure_ascii=False)}
                    buffers["about"].append({"place_id": place_id, "about": ab_to_save})

                    place_done = True

                except BrowserCrashedError as e:
                    crash_attempts += 1
                    logger.error(
                        f"💥 Browser crashed on {place_id} "
                        f"(attempt {crash_attempts}/{max_crash_retries}): {e}"
                    )
                    if crash_attempts >= max_crash_retries:
                        logger.error(f"❌ Giving up on {place_id} after {max_crash_retries} crash retries.")
                        buffers["overview"].append({"place_id": place_id, "overview": {"error": "browser_crash_exhausted"}})
                        buffers["review"].append({"place_id": place_id, "review": {"error": "browser_crash_exhausted"}})
                        buffers["about"].append({"place_id": place_id, "about": {"error": "browser_crash_exhausted"}})
                        place_done = True
                    else:
                        restart_driver(f"tab/session crash on {place_id}")
                        time.sleep(random.uniform(2, 5))

            for data_type in buffers:
                if len(buffers[data_type]) >= flush_interval:
                    flush_shard(data_type)

            elapsed = time.time() - t0
            logger.info(f"{place_id} done {elapsed:.1f}s")
            time.sleep(random.uniform(*config.RECORD_DELAY_RANGE))
            idx += 1
            gc.collect()

    finally:
        for data_type in buffers:
            flush_shard(data_type)
        for data_type in dirs:
            merge_shards_and_cleanup(output_dir, data_type, batch_number, logger)
        try:
            driver.quit()
        except Exception:
            pass


def run_batch_worker(args: Tuple) -> Tuple[int, str, Optional[str]]:
    """
    Wrapper for process_batch to be used with ProcessPoolExecutor.
    Returns (batch_number, status, error_message).
    """
    batch, batch_number, output_dir, worker_index = args
    logger = setup_logger(batch_number, output_dir)

    stagger_delay = worker_index * random.uniform(8, 12)
    logger.info(f"Worker #{worker_index} waiting {stagger_delay:.1f}s before start...")
    time.sleep(stagger_delay)

    t0 = time.time()
    time.sleep(random.uniform(0, 8))

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
    parser.add_argument("--start", type=int, default=config.START_INDEX, help="Start index (inclusive) - used to split work across notebooks/sessions")
    parser.add_argument("--end", type=int, default=None, help="End index (exclusive), None = until the end of the list")
    parser.add_argument("--batch-size", type=int, default=config.BATCH_SIZE, help="Size of each batch")
    parser.add_argument("--workers", type=int, default=config.MAX_WORKERS, help="Number of parallel workers")
    parser.add_argument("--output", default=config.OUTPUT_DIR_BASE, help="Output directory")
    args = parser.parse_args()

    all_place_ids = load_place_ids(args.input)
    print(f"Total place_ids: {len(all_place_ids)}")

    end_idx = args.end if args.end is not None else len(all_place_ids)
    slice_for_this_run = all_place_ids[args.start:end_idx]
    print(f"Assigned range: [{args.start}:{end_idx}] -> {len(slice_for_this_run)} place_ids")

    done_place_ids = load_done_place_ids(args.output)
    print(f"Already done (in this output dir): {len(done_place_ids)}")

    remaining = [pid for pid in slice_for_this_run if pid not in done_place_ids]
    print(f"Remaining to process: {len(remaining)}")

    batches = list(split_into_batches(remaining, args.batch_size))
    start_batch_number = get_next_batch_number(args.output)

    tasks = []
    for i, batch in enumerate(batches):
        batch_number = start_batch_number + i
        worker_index = i
        tasks.append((batch, batch_number, args.output, worker_index))

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