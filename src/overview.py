from bs4 import BeautifulSoup
from src.utils import retry_on_failure


def classify_link(item_id: str, href: str) -> str:
    """
    Classify link type based on ID or URL pattern.
    Returns: instagram, facebook, tiktok, whatsapp, website, menu, delivery,
             reservation_or_contact, or other.
    """
    href_lower = href.lower()

    if "instagram.com" in href_lower:
        return "instagram"
    if "facebook.com" in href_lower:
        return "facebook"
    if "tiktok.com" in href_lower:
        return "tiktok"
    if "wa.me" in href_lower or "api.whatsapp.com" in href_lower:
        return "whatsapp"

    mapping = {
        "authority": "website",
        "menu": "menu",
        "services": "delivery",
    }
    if item_id in mapping:
        return mapping[item_id]
    if item_id.startswith("action:"):
        return "reservation_or_contact"

    return "other"


@retry_on_failure(max_retries=5, task_name="overview")
def scrape_overview(driver, place_id):
    """
    Scrape overview data from Google Maps place page.

    Returns:
        dict: {
            "name": str,
            "rating": str,
            "user_total_rating": str,
            "price_range": str,
            "phone": str,
            "links": list of {"type": str, "raw_item_id": str, "label": str, "url": str}
        }
    """
    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Name
    name_tag = soup.select_one('h1.DUwDvf.lfPIob')
    name = name_tag.get_text(strip=True) if name_tag else ""
    if not name:
        raise ValueError(f"Failed to extract name for place_id={place_id}")

    # Rating & total reviews
    rating = ""
    total_rating = ""
    rating_container = soup.find('div', class_='F7nice')
    if rating_container:
        rating_tag = rating_container.select_one('span > span[aria-hidden="true"]')
        rating = rating_tag.get_text(strip=True) if rating_tag else ""

        total_tag = rating_container.find('span', attrs={
            'aria-label': lambda x: x and 'review' in x.lower()
        })
        total_rating = total_tag.get_text(strip=True).strip('()') if total_tag else ""

    # Price range
    price_range = ""
    price_container = soup.find('div', attrs={'jsname': 'tJHJj'})
    if price_container:
        text_div = price_container.find('div')
        if text_div:
            reported_by = text_div.find('div', class_='BfVpR')
            if reported_by:
                reported_by.extract()
            candidate = text_div.get_text(strip=True).replace('\xa0', ' ')
            if candidate.lower().startswith('rp'):
                price_range = candidate

    # Phone
    phone = ""
    phone_tag = soup.find('button', attrs={"data-item-id": lambda x: x and "phone" in x})
    if phone_tag:
        phone_div = phone_tag.find('div', class_='Io6YTe')
        phone = phone_div.get_text(strip=True) if phone_div else ""

    # Links: website, menu, delivery, reservation, social media, etc.
    link_tags = soup.find_all('a', attrs={"data-item-id": True})
    links = []
    for tag in link_tags:
        href = tag.get("href", "").strip()
        if not href:
            continue

        item_id = tag.get("data-item-id", "")
        label_div = tag.find('div', class_='Io6YTe')
        label = label_div.get_text(strip=True) if label_div else ""

        links.append({
            "type": classify_link(item_id, href),
            "raw_item_id": item_id,
            "label": label,
            "url": href
        })

    return {
        "name": name,
        "rating": rating,
        "user_total_rating": total_rating,
        "price_range": price_range,
        "phone": phone,
        "links": links,
    }