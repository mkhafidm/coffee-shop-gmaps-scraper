import functools
import random
import time
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import InvalidSessionIdException, NoSuchWindowException, WebDriverException


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
    """
    Decorator to retry a scraping function with exponential backoff.

    The decorated function must accept (driver, place_id, *args, **kwargs)
    and return a dict or None.

    Args:
        max_retries: Maximum number of attempts.
        task_name: Name of the task for logging.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(driver, place_id, *args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(driver, place_id, *args, **kwargs)
                except Exception as e:
                    if is_browser_dead(e):
                        raise BrowserCrashedError(str(e)) from e

                    wait_time = 2 ** attempt + random.uniform(1, 3)
                    logger.warning(
                        f"⚠️ Error {task_name} for {place_id}, "
                        f"retry {attempt + 1}/{max_retries} in {wait_time:.2f}s - {e}"
                    )
                    time.sleep(wait_time)
            logger.error(
                f"❌ Failed {task_name} for {place_id} after {max_retries} attempts."
            )
            return None
        return wrapper
    return decorator


def handle_consent_popup(driver, timeout: int = 5) -> bool:
    """
    Dismiss Google sign-in / consent modal if it appears.

    Args:
        driver: Selenium WebDriver instance.
        timeout: Maximum time to wait for the popup.

    Returns:
        True if popup was dismissed, False otherwise.
    """
    try:
        dismiss_btn = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//button[contains(., 'Dismiss') "
                    "or @aria-label='Dismiss' "
                    "or @aria-label='Tolak']",
                )
            )
        )
        dismiss_btn.click()
        return True
    except Exception:
        return False