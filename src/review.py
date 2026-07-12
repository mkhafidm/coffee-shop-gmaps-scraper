import re
import time
import random
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from src.utils import retry_on_failure


@retry_on_failure(max_retries=5, task_name="review")
def scrape_review(driver, place_id, max_samples=50):
    """
    Scrape reviews from Google Maps place page.

    Returns:
        dict: {
            "rating": str,
            "total_reviews": str,
            "total_reviews_scraped": int,
            "keyword_tags": dict,
            "reviews": list
        }
    """
    # Click Reviews tab
    try:
        review_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((
                By.CSS_SELECTOR,
                'button.hh2c6[aria-label*="Reviews"], button.hh2c6[aria-label*="Ulasan"]'
            ))
        )
        review_tab.click()
        time.sleep(random.uniform(2, 4))
    except Exception as e:
        print(f"Failed to click Reviews tab: {e}")
        return {
            "rating": None,
            "total_reviews": None,
            "total_reviews_scraped": 0,
            "keyword_tags": {},
            "reviews": []
        }

    # Wait for reviews container
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'PPCwl'))
        )
    except:
        pass

    # Rating
    rating = None
    try:
        rating_el = driver.find_element(By.CSS_SELECTOR, 'div.fontDisplayLarge')
        rating = rating_el.text.strip()
    except:
        pass

    # Total reviews count
    total_reviews = None
    try:
        review_count_elements = driver.find_elements(By.CSS_SELECTOR, 'div.fontBodySmall')
        for el in review_count_elements:
            text = el.text.strip()
            if 'review' in text.lower():
                total_reviews = text
                break
    except:
        pass

    # If zero reviews, return early
    total_review_count = 0
    if total_reviews:
        match = re.search(r'(\d+)', total_reviews)
        if match:
            total_review_count = int(match.group(1))

    if total_review_count == 0:
        return {
            "rating": rating,
            "total_reviews": total_reviews,
            "total_reviews_scraped": 0,
            "keyword_tags": {},
            "reviews": []
        }

    # Keyword tags (e.g., "hangout spot")
    keyword_tags = {}
    try:
        refine_container = driver.find_element(By.CSS_SELECTOR, 'div[aria-label="Refine reviews"]')
        buttons = refine_container.find_elements(By.CSS_SELECTOR, 'button.e2moi[role="radio"]')
        for button in buttons:
            aria_label = button.get_attribute('aria-label')
            if aria_label and 'All reviews' in aria_label:
                continue
            try:
                feature_el = button.find_element(By.CSS_SELECTOR, 'span.uEubGf.fontBodyMedium')
                count_el = button.find_element(By.CSS_SELECTOR, 'span.bC3Nkc.fontBodySmall')
                feature = feature_el.text.strip()
                count_text = count_el.text.strip().replace(',', '')
                if feature and count_text.isdigit():
                    keyword_tags[feature] = int(count_text)
            except:
                continue
    except:
        pass

    # Sort by Newest
    try:
        sort_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Sort']]"))
        )
        sort_btn.click()
        time.sleep(random.uniform(1, 2))

        newest_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//div[@role='menuitemradio' and .//div[text()='Newest']]")
            )
        )
        newest_option.click()
        time.sleep(random.uniform(2, 4))
    except Exception as e:
        print(f"Failed to sort by Newest: {e}. Proceeding with default.")

    # Helper to parse a review card
    def parse_card(card):
        try:
            name = card.find_element(By.CSS_SELECTOR, 'div.d4r55').text.strip()
        except:
            name = ""
        try:
            aria_label = card.find_element(By.CSS_SELECTOR, 'span.kvMYJc').get_attribute('aria-label')
            m = re.search(r'(\d+)\s+stars?', aria_label or "")
            rating_val = int(m.group(1)) if m else 0
        except:
            rating_val = 0
        try:
            text = card.find_element(By.CSS_SELECTOR, 'span.wiI7pd').text.strip()
        except:
            text = ""
        try:
            date = card.find_element(By.CSS_SELECTOR, 'span.rsqaWe').text.strip()
        except:
            date = ""
        try:
            likes_txt = card.find_element(By.CSS_SELECTOR, 'span.pkWtMe').text.strip()
            likes = int(likes_txt) if likes_txt.isdigit() else 0
        except:
            likes = 0

        if not name and not text:
            return None

        key = f"{name}|{date}|{text[:50]}"
        return key, {
            "reviewer_name": name,
            "rating": rating_val,
            "text": text,
            "date": date,
            "likes": likes
        }

    # Locate scroll container
    try:
        scroll_container = driver.find_element(By.CSS_SELECTOR, 'div.m6QErb.DxyBCb.kA9KIf.dS8AEf')
    except:
        try:
            scroll_container = driver.find_element(By.CSS_SELECTOR, 'div[role="main"] .m6QErb.XiKgde')
        except:
            scroll_container = driver.find_element(By.CSS_SELECTOR, 'div.m6QErb.DxyBCb')

    collected = {}
    scroll_attempts = 0
    max_scroll_attempts = 80
    no_new_count = 0
    no_new_limit = 8

    while len(collected) < max_samples and scroll_attempts < max_scroll_attempts:
        cards = driver.find_elements(By.CSS_SELECTOR, 'div.jftiEf')
        before = len(collected)
        for card in cards:
            result = parse_card(card)
            if result:
                key, data = result
                collected[key] = data

        added = len(collected) - before

        if len(collected) >= max_samples:
            print(f"Target {max_samples} reviews reached. Stopping.")
            break

        no_new_count = no_new_count + 1 if added == 0 else 0
        if no_new_count >= no_new_limit:
            print(f"No new reviews after {no_new_limit} scrolls. Stopping.")
            break

        # Random scroll pattern
        scroll_style = random.choices(
            ["small", "medium", "bottom"],
            weights=[0.3, 0.4, 0.3]
        )[0]

        if scroll_style == "small":
            driver.execute_script(
                "arguments[0].scrollBy(0, arguments[0].clientHeight * arguments[1])",
                scroll_container, random.uniform(0.8, 1.2)
            )
        elif scroll_style == "medium":
            driver.execute_script(
                "arguments[0].scrollBy(0, arguments[0].clientHeight * arguments[1])",
                scroll_container, random.uniform(2, 3)
            )
        else:  # bottom
            driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollHeight", scroll_container
            )

        time.sleep(random.uniform(1.5, 4.5))
        scroll_attempts += 1

    parsed_reviews = list(collected.values())[:max_samples]

    return {
        "rating": rating,
        "total_reviews": total_reviews,
        "keyword_tags": keyword_tags,
        "total_reviews_scraped": len(parsed_reviews),
        "reviews": parsed_reviews
    }