import os
import time
import random
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client
from jobspy import scrape_jobs

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

SEARCH_TERMS = [
    "software engineer", "data analyst", "java developer", "python developer",
    "devops engineer", "data scientist", "cyber security", "frontend developer"
]
LOCATIONS = ["Hyderabad", "Chennai", "Mumbai", "Pune", "Gurugram",
             "Bangalore", "Delhi", "Kolkata", "Ahmedabad", "Noida"]
RESULTS_WANTED = 20
SLEEP_SECONDS = 30

def get_existing_urls(supabase):
    response = supabase.table("ApplyMore").select("url").execute()
    return {item["url"] for item in response.data}

def scrape_and_store():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    existing = get_existing_urls(supabase)
    new_jobs = []

    for location in LOCATIONS:
        for term in SEARCH_TERMS:
            print(f"Scraping {term} in {location}")
            try:
                jobs = scrape_jobs(
                    site_name=["linkedin", "indeed"],
                    search_term=term,
                    location=location,
                    results_wanted=RESULTS_WANTED,
                    hours_old=72,
                    country_indeed='india'
                )
                for _, row in jobs.iterrows():
                    url = row.get('job_url', '')
                    if url and url not in existing:
                        new_jobs.append({
                            "title": row.get('title', '')[:200],
                            "company": row.get('company', '')[:100],
                            "location": location,
                            "url": url,
                            "description": row.get('description', '')[:2000],
                            "posted_date": datetime.now().isoformat(),
                            "created_at": datetime.now().isoformat()
                        })
                        existing.add(url)
                time.sleep(random.randint(SLEEP_SECONDS, SLEEP_SECONDS + 20))
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(SLEEP_SECONDS * 2)

    if new_jobs:
        batch_size = 50
        for i in range(0, len(new_jobs), batch_size):
            supabase.table("ApplyMore").insert(new_jobs[i:i+batch_size]).execute()
        print(f"Inserted {len(new_jobs)} new jobs.")
    else:
        print("No new jobs found.")

if __name__ == "__main__":
    scrape_and_store()
