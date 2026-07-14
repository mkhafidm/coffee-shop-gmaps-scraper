import gc
import glob
import os
import random
import re
import time
import json
import logging
import argparse
import pandas as pd
import config
import undetected_chromedriver as uc
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Tuple, Any, Optional, Dict
from selenium.webdriver.common.by import By
from src import overview, review, about, utils
from src.utils import BrowserCrashedError


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

    options.add_argument("--lang=en-US")
    options.add_argument("--accept-lang=en-US,en")
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
    """Create an undetected Chrome driver, retrying a few times on failure."""
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


def setup_startup_logger(output_dir: str) -> logging.Logger:
    """Logger for startup processes (orphan shard finalization)."""
    log_dir = os.path.join(output_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger("startup")
    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s - [startup] %(message)s")
    file_handler = logging.FileHandler(os.path.join(log_dir, "startup.log"))
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


def finalize_orphan_shards(output_dir: str, logger: logging.Logger):
    """
    Merge leftover shard files from interrupted runs before starting new work.
    This ensures we don't have stale shards hanging around.
    """
    for data_type in ["overview", "review", "about"]:
        data_dir = os.path.join(output_dir, data_type)
        pattern = os.path.join(data_dir, f"batch_*_{data_type}_shard*.parquet")
        shard_files = glob.glob(pattern)

        if not shard_files:
            continue

        batch_numbers = set()
        for f in shard_files:
            match = re.match(
                rf"batch_(\d+)_{data_type}_shard\d+\.parquet",
                os.path.basename(f),
            )
            if match:
                batch_numbers.add(int(match.group(1)))

        if not batch_numbers:
            continue

        logger.info(
            f"Found orphan shards for {data_type}, "
            f"batch numbers={sorted(batch_numbers)} -> finalizing..."
        )

        for bn in sorted(batch_numbers):
            try:
                merge_shards_and_cleanup(output_dir, data_type, bn, logger)
                logger.info(f"Finalized orphan shard(s) for {data_type} batch {bn}")
            except Exception as e:
                logger.error(
                    f"Failed to finalize orphan shard {data_type} batch {bn}: {e}. "
                    f"Shard kept, will retry next run."
                )


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
    Scrape a batch of places. Skips place_ids that already exist in the final
    files for this batch, so we can resume without redoing work.
    """
    logger = setup_logger(batch_number, output_dir)

    dirs = {
        "overview": os.path.join(output_dir, "overview"),
        "review":   os.path.join(output_dir, "review"),
        "about":    os.path.join(output_dir, "about"),
    }
    for d in dirs.values():
        os.makedirs(d, exist_ok=True)

    # Load existing place_ids from this batch's final files (if any)
    existing_place_ids = set()
    for data_type in ["overview", "review", "about"]:
        final_path = os.path.join(dirs[data_type], f"batch_{batch_number:03}_{data_type}.parquet")
        if os.path.exists(final_path):
            try:
                df = pd.read_parquet(final_path, columns=["place_id"])
                existing_place_ids.update(df["place_id"].tolist())
            except Exception as e:
                logger.warning(f"Could not read existing {data_type} file: {e}")

    if existing_place_ids:
        # Filter out already-done place_ids
        original_len = len(batch)
        batch = [pid for pid in batch if pid not in existing_place_ids]
        logger.info(
            f"Batch {batch_number}: {original_len - len(batch)} already done, "
            f"{len(batch)} remaining to process"
        )
    else:
        logger.info(f"Batch {batch_number}: starting fresh ({len(batch)} places)")

    if not batch:
        logger.info(f"Batch {batch_number} already complete, skipping")
        return

    # Setup buffers and driver
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

    # Scrape
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
                    url = f"https://www.google.com/maps/place/?q=place_id:{place_id}&hl=en"
                    driver.get(url)
                    time.sleep(random.uniform(1, 2))
                    driver.refresh()
                    time.sleep(random.uniform(2, 4))
                    utils.handle_consent_popup(driver)
                    time.sleep(1)
                    utils.handle_consent_popup(driver)

                    # Check if place is valid
                    try:
                        h1 = driver.find_element(By.CSS_SELECTOR, "h1.DUwDvf.lfPIob")
                        if not h1.text.strip():
                            raise Exception
                    except Exception:
                        logger.warning(f"Place {place_id} invalid, skipping")
                        buffers["overview"].append({"place_id": place_id, "overview": {"error": "invalid_place"}})
                        buffers["review"].append({"place_id": place_id, "review": {"error": "invalid_place"}})
                        buffers["about"].append({"place_id": place_id, "about": {"error": "invalid_place"}})
                        place_done = True
                        continue

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
                        f"Browser crashed on {place_id} "
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
    """Wrapper for process_batch to be used with ProcessPoolExecutor."""
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
# Main entry point
# -----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=config.INPUT_PARQUET, help="Input Parquet file path")
    parser.add_argument("--start", type=int, default=config.START_INDEX, help="Start index (inclusive)")
    parser.add_argument("--end", type=int, default=None, help="End index (exclusive), None = until end")
    parser.add_argument("--batch-size", type=int, default=config.BATCH_SIZE, help="Size of each batch")
    parser.add_argument("--workers", type=int, default=config.MAX_WORKERS, help="Number of parallel workers")
    parser.add_argument("--output", default=config.OUTPUT_DIR_BASE, help="Output directory")
    parser.add_argument(
        "--batch-start",
        type=int,
        default=None,
        help="Starting batch number (auto-calculated from --start if not given)",
    )
    args = parser.parse_args()

    # --- Clean up orphan shards from previous runs ---
    startup_logger = setup_startup_logger(args.output)
    finalize_orphan_shards(args.output, startup_logger)

    # --- Load all place IDs for the assigned range ---
    all_place_ids = load_place_ids(args.input)
    end_idx = args.end if args.end is not None else len(all_place_ids)
    slice_for_this_run = all_place_ids[args.start:end_idx]
    print(f"Total place_ids: {len(all_place_ids)}")
    print(f"Assigned range: [{args.start}:{end_idx}] -> {len(slice_for_this_run)} place_ids")

    # --- Split into batches ---
    batches = list(split_into_batches(slice_for_this_run, args.batch_size))
    print(f"Number of batches: {len(batches)}")

    # --- Determine starting batch number ---
    if args.batch_start is not None:
        start_batch_number = args.batch_start
    else:
        start_batch_number = (args.start // args.batch_size) + 1
    print(f"Batch numbering starts at: {start_batch_number}")

    # --- Create tasks ---
    tasks = []
    for i, batch in enumerate(batches):
        batch_number = start_batch_number + i
        tasks.append((batch, batch_number, args.output, i))

    # --- Run workers ---
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(run_batch_worker, task): task[1] for task in tasks}
        for future in as_completed(futures):
            batch_num, status, error = future.result()
            if status == "success":
                print(f"✅ Batch #{batch_num} done")
            else:
                print(f"❌ Batch #{batch_num} failed: {error}")


if __name__ == "__main__":
    main()