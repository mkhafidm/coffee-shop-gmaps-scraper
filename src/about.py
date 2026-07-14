import time
import random
import logging
from typing import Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from src.utils import retry_on_failure

logger = logging.getLogger(__name__)


def parse_status_from_icon(icon_classes: str) -> Optional[bool]:
    """
    Determine availability status from Google's icon class.

    Args:
        icon_classes: Space-separated class string.

    Returns:
        True if available (SwaGS), False if not available (OazX1c),
        None if unknown pattern.
    """
    classes = icon_classes.split()
    if "OazX1c" in classes:
        return False
    if "SwaGS" in classes:
        return True
    return None


@retry_on_failure(max_retries=5, task_name="about")
def scrape_about(driver, place_id):
    """
    Scrape the "About" section of a Google Maps place.

    Clicks the About button, waits for the section to load, then extracts
    all categories (e.g., Offerings, Accessibility) and their items.

    Returns:
        dict: {"about": {section_title: [{"label": str, "raw_aria_label": str, "available": bool}]}}
    """
    # Click the About button
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((
            By.CSS_SELECTOR,
            'button.hh2c6[aria-label*="About"], button.hh2c6[aria-label*="Tentang"]'
        ))
    ).click()
    time.sleep(random.uniform(1.5, 3.5))

    sections = driver.find_elements(By.CLASS_NAME, 'iL3Qke')
    about_info = {}

    for section in sections:
        section_title = section.text.strip()
        about_info[section_title] = []

        try:
            # The content is usually the next sibling of the section header
            next_sibling = section.find_element(By.XPATH, 'following-sibling::*[1]')
            items = next_sibling.find_elements(By.TAG_NAME, 'li')

            for item in items:
                icon_el = item.find_element(By.CSS_SELECTOR, 'span.f5BGzb')
                icon_classes = icon_el.get_attribute('class') or ""
                is_available = parse_status_from_icon(icon_classes)

                label_el = item.find_element(By.CSS_SELECTOR, 'span[aria-label]')
                raw_label = label_el.get_attribute('aria-label') or ""
                display_text = label_el.text.strip()

                about_info[section_title].append({
                    "label": display_text,
                    "raw_aria_label": raw_label,
                    "available": is_available,
                })
        except Exception as e:
            # Skip this section if structure is unexpected
            logger.debug(f"Could not parse about section '{section_title}' for {place_id}: {e}")
            continue

    return {"about": about_info}