import os
import pandas as pd
from datetime import datetime, timezone
from supabase import create_client, Client
from jobspy import scrape_jobs
import time
import random

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

ROLES = [
    "Software Engineer fresher", "Data Analyst entry level", "Python Developer fresher",
    "DevOps Engineer entry level", "Cyber Security fresher", "Java Developer fresher",
    "Frontend Developer fresher", "Backend Developer fresher", "Full Stack fresher",
    "Android Developer fresher", "iOS Developer fresher", "Cloud Engineer fresher",
    "AWS fresher", "Azure fresher", "Network Engineer fresher", "Support Engineer fresher",
    "QA Tester fresher", "Manual Testing fresher", "Automation Testing fresher", "IT Support fresher"
]
LOCATIONS = ["Hyderabad, India", "Bangalore, India", "Chennai, India"]
RESULTS_WANTED = 30
HOURS_OLD = 72

FRESHER_KEYWORDS = ["fresher", "entry level", "graduate", "trainee", "junior", "0-2", "2024", "2025"]
SENIOR_KEYWORDS = ["senior", "lead", "principal", "architect", "manager", "director", "head", "vp", "cto"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def is_fresher_job(title, description):
    if not isinstance(title, str): title = ""
    if not isinstance(description, str): description = ""
    text = (title + " " + description).lower()
    has_fresher = any(kw in text for kw in FRESHER_KEYWORDS)
    has_senior = any(kw in text for kw in SENIOR_KEYWORDS)
    return has_fresher and not has_senior

def get_existing_urls():
    response = supabase.table("ApplyMore").select("url").execute()
    return {row["url"] for row in response.data} if response.data else set()

def batch_upsert_jobs(jobs, batch_size=50):
    for i in range(0, len(jobs), batch_size):
        supabase.table("ApplyMore").insert(jobs[i:i+batch_size]).execute()
        print(f"Inserted batch {i//batch_size + 1} ({len(jobs[i:i+batch_size])} jobs)")

def scrape_all_jobs():
    all_new_jobs = []
    seen_urls = get_existing_urls()
    print(f"Existing jobs: {len(seen_urls)}")
    for location in LOCATIONS:
        for role in ROLES:
            print(f"\n--- {role} in {location} ---")
            try:
                jobs_df = scrape_jobs(
                    site_name=["linkedin", "indeed"],
                    search_term=role,
                    location=location,
                    results_wanted=RESULTS_WANTED,
                    hours_old=HOURS_OLD,
                    country_indeed='india',
                    verbose=2,
                )
                print(f"Raw: {len(jobs_df)}")
                if jobs_df.empty:
                    continue
                for _, job in jobs_df.iterrows():
                    title = job.get('title', '')
                    company = job.get('company', '')
                    url = job.get('job_url', '')
                    desc = job.get('description', '')
                    posted = job.get('date_posted')
                    if not title or not company or not url:
                        continue
                    if url in seen_urls:
                        continue
                    if not is_fresher_job(title, desc):
                        continue
                    if pd.isna(posted):
                        posted_iso = datetime.now(timezone.utc).isoformat()
                    elif isinstance(posted, datetime):
                        posted_iso = posted.isoformat()
                    else:
                        posted_iso = str(posted)
                    new_job = {
                        "title": title, "company": company, "location": location.split(',')[0],
                        "url": url, "description": desc, "posted_date": posted_iso,
                        "created_at": datetime.now(timezone.utc).isoformat()
                    }
                    all_new_jobs.append(new_job)
                    seen_urls.add(url)
                print(f"New so far: {len(all_new_jobs)}")
                time.sleep(random.uniform(3, 7))
            except Exception as e:
                print(f"Error: {e}")
    print(f"\nTotal new: {len(all_new_jobs)}")
    if all_new_jobs:
        batch_upsert_jobs(all_new_jobs)
    else:
        print("No new jobs.")

if __name__ == "__main__":
    scrape_all_jobs()
