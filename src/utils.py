import functools
import random
import time
import logging
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import InvalidSessionIdException, NoSuchWindowException, NoSuchElementException


logger = logging.getLogger(__name__)

class BrowserCrashedError(Exception):
    """Browser/tab session is dead - retrying on the same driver is pointless, driver must be restarted."""
    pass

def is_browser_dead(exc: Exception) -> bool:
    if isinstance(exc, (InvalidSessionIdException, NoSuchWindowException)):
        return True
    msg = str(exc).lower()
    signals = ["tab crashed", "invalid session id", "disconnected", "chrome not reachable", "session deleted"]
    return any(s in msg for s in signals)

def retry_on_failure(max_retries: int = 5, task_name: str = "scraping"):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(driver, place_id, *args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(driver, place_id, *args, **kwargs)
                except Exception as e:
                    # ---------- SCREENSHOT ----------
                    try:
                        os.makedirs("screenshots", exist_ok=True)
                        fname = f"error_{place_id}_{task_name}_attempt{attempt}.png".replace("/", "_")
                        driver.save_screenshot(os.path.join("screenshots", fname))
                        logger.info(f"Screenshot saved: {fname}")
                    except Exception as ss_err:
                        logger.warning(f"Failed screenshot: {ss_err}")

                    # ---------- HANDLE CONSENT POPUP ----------
                    try:
                        handle_consent_popup(driver, timeout=3)
                        logger.info("Tried to dismiss consent popup before retry")
                    except Exception as popup_err:
                        logger.warning(f"Failed to handle popup: {popup_err}")

                    if is_browser_dead(e):
                        raise BrowserCrashedError(str(e)) from e

                    wait_time = 2 ** attempt + random.uniform(1, 3)
                    logger.warning(
                        f"⚠️ Error {task_name} for {place_id}, "
                        f"retry {attempt + 1}/{max_retries} in {wait_time:.2f}s - {e}"
                    )
                    time.sleep(wait_time)

            logger.error(f"❌ Failed {task_name} for {place_id} after {max_retries} attempts.")
            return None
        return wrapper
    return decorator

def handle_consent_popup(driver, timeout: int = 5) -> bool:
    reject_texts = [
        "Reject all", "Reject", "Afwijzen", "Alles afwijzen",
        "Tolak", "Tolak semua", "Decline", "Deny", "Refuser", "Ablehnen"
    ]
    for text in reject_texts:
        try:
            btn = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.XPATH, f"//button[contains(., '{text}')]"))
            )
            btn.click()
            return True
        except:
            continue
    return False