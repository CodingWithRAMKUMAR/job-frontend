import os
import time
import random
import requests
from datetime import datetime
from dotenv import load_dotenv
from jobspy import scrape_jobs

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")   # anon key works here

SEARCH_TERMS = [
    "software engineer", "data analyst", "java developer", "python developer",
    "devops engineer", "data scientist", "cyber security", "frontend developer"
]
LOCATIONS = ["Hyderabad", "Chennai", "Mumbai", "Pune", "Gurugram",
             "Bangalore", "Delhi", "Kolkata", "Ahmedabad", "Noida"]
RESULTS_WANTED = 20
SLEEP_SECONDS = 30

def get_existing_urls():
    url = f"{SUPABASE_URL}/rest/v1/ApplyMore?select=url"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return {item["url"] for item in resp.json()}
    print(f"Failed to fetch existing URLs: {resp.status_code}")
    return set()

def insert_jobs(jobs):
    if not jobs:
        return
    url = f"{SUPABASE_URL}/rest/v1/ApplyMore"
    headers = {
        "Content-Type": "application/json",
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Prefer": "return=minimal"
    }
    batch_size = 50
    for i in range(0, len(jobs), batch_size):
        batch = jobs[i:i+batch_size]
        resp = requests.post(url, headers=headers, json=batch)
        if resp.status_code in (200, 201):
            print(f"Inserted batch of {len(batch)} jobs")
        else:
            print(f"Insert failed: {resp.status_code} - {resp.text}")

def scrape_and_store():
    existing_urls = get_existing_urls()
    new_jobs = []

    for location in LOCATIONS:
        for term in SEARCH_TERMS:
            print(f"Scraping {term} in {location}")
            try:
                jobs_df = scrape_jobs(
                    site_name=["linkedin", "indeed"],
                    search_term=term,
                    location=location,
                    results_wanted=RESULTS_WANTED,
                    hours_old=72,
                    country_indeed='india'
                )
                for _, row in jobs_df.iterrows():
                    url = row.get('job_url', '')
                    if url and url not in existing_urls:
                        new_jobs.append({
                            "title": row.get('title', '')[:200],
                            "company": row.get('company', '')[:100],
                            "location": location,
                            "url": url,
                            "description": row.get('description', '')[:2000],
                            "posted_date": datetime.now().isoformat(),
                            "created_at": datetime.now().isoformat()
                        })
                        existing_urls.add(url)
                time.sleep(random.randint(SLEEP_SECONDS, SLEEP_SECONDS + 20))
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(SLEEP_SECONDS * 2)

    if new_jobs:
        insert_jobs(new_jobs)
        print(f"Inserted {len(new_jobs)} new jobs.")
    else:
        print("No new jobs found.")

if __name__ == "__main__":
    scrape_and_store()
