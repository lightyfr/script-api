import re
import requests
from bs4 import BeautifulSoup
from supabase_client import supabase

def extract_email(text):
    match = re.search(r'[\w\.-]+@[\w\.-]+', text)
    return match.group(0) if match else None

def scrape_and_store(config):
    url = config["url"]
    selectors = config["selectors"]
    university = config["university"]
    department = config["department"]

    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    faculty_cards = soup.select(selectors["faculty_card"])

    inserted = 0
    for card in faculty_cards:
        name_el = card.select_one(selectors["name"])
        email_el = card.select_one(selectors["email"]) or card.find(string=re.compile("@"))
        title_el = card.select_one(selectors.get("title", ""))

        name = name_el.get_text(strip=True) if name_el else None
        email = email_el.get_text(strip=True) if email_el else extract_email(card.get_text())
        title = title_el.get_text(strip=True) if title_el else None

        if name and email:
            data = {
                "name": name,
                "email": email,
                "title": title,
                "university": university,
                "department": department
            }

            supabase.table("professors").upsert(data, on_conflict=["email"]).execute()
            inserted += 1

    return inserted
