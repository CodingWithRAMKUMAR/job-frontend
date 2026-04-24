import os
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta
from supabase import create_client
import re

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
RAPIDAPI_KEY = os.environ["RAPIDAPI_KEY"].strip()
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

ALLOWED_SOURCES = {"linkedin", "indeed", "glassdoor"}
CITIES = ["Hyderabad", "Bangalore", "Chennai"]
SEARCH_TERMS = [
    "fresher software engineer", "graduate engineer trainee", "entry level developer",
    "fresher data analyst", "trainee engineer", "fresher devops", "fresher cybersecurity", "fresher qa"
]
DAYS_BACK = 3
RESULTS_PER_PAGE = 20

FRESHER_WORDS = {"fresher", "entry level", "graduate", "trainee", "junior", "0-1", "0-2", "1 year", "2 years", "2024", "2025", "recent graduate"}
SENIOR_WORDS = {"senior", "lead", "principal", "architect", "manager", "director", "head", "vp", "cto", "staff"}

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def extract_experience_level(title, description):
    text = (title + " " + description).lower()
    if "fresher" in text or "entry level" in text or "graduate" in text or "trainee" in text:
        return "Fresher (0-2 years)"
    match = re.search(r'(\d+)\s*-\s*(\d+)\s*years?', text)
    if match:
        return f"{match.group(1)}-{match.group(2)} years"
    return "Fresher (0-2 years)"

def is_fresher_job(title, description):
    text = (title + " " + description).lower()
    return any(w in text for w in FRESHER_WORDS) and not any(w in text for w in SENIOR_WORDS)

async def fetch_jobs(session, query, city):
    url = "https://jsearch.p.rapidapi.com/search"
    params = {
        "query": f"{query} {city}",
        "page": 1, "num_pages": 1, "country": "in", "date_posted": "week", "results_per_page": RESULTS_PER_PAGE
    }
    headers = {"x-rapidapi-key": RAPIDAPI_KEY, "x-rapidapi-host": "jsearch.p.rapidapi.com"}
    headers = {k: v.replace('\n', '').replace('\r', '').strip() for k, v in headers.items()}
    try:
        async with session.get(url, headers=headers, params=params, timeout=10) as resp:
            if resp.status != 200:
                print(f"API error {resp.status} for {query} in {city}")
                return []
            data = await resp.json()
            return data.get("data", [])
    except Exception as e:
        print(f"Request failed for {query} in {city}: {e}")
        return []

async def send_telegram(session, text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    await session.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True})

async def main():
    print("🚀 ApplyMore DIAGNOSTIC scraper")
    start = datetime.now(timezone.utc)

    # Existing URLs
    existing = supabase.table("ApplyMore").select("url").execute()
    existing_urls = {row["url"] for row in existing.data} if existing.data else set()
    print(f"Existing jobs: {len(existing_urls)}")

    total_raw = 0
    total_source_blocked = 0
    total_duplicate = 0
    total_old = 0
    total_not_fresher = 0
    total_kept = 0

    async with aiohttp.ClientSession() as session:
        for city in CITIES:
            for term in SEARCH_TERMS:
                jobs = await fetch_jobs(session, term, city)
                print(f"  {term} in {city}: {len(jobs)} raw jobs")
                total_raw += len(jobs)
                for job in jobs:
                    title = job.get("job_title")
                    company = job.get("employer_name")
                    url = job.get("job_apply_link")
                    desc = job.get("job_description", "")
                    posted_str = job.get("job_posted_at_datetime_utc") or job.get("job_posted_at")
                    source = job.get("job_publisher", "").lower()
                    if not title or not company or not url:
                        continue
                    # Source filter
                    if source not in ALLOWED_SOURCES:
                        total_source_blocked += 1
                        continue
                    if url in existing_urls:
                        total_duplicate += 1
                        continue
                    # Date filter
                    if posted_str:
                        try:
                            posted_dt = datetime.fromisoformat(posted_str.replace('Z', '+00:00'))
                            if posted_dt < datetime.now(timezone.utc) - timedelta(days=DAYS_BACK):
                                total_old += 1
                                continue
                        except:
                            pass
                    if not is_fresher_job(title, desc):
                        total_not_fresher += 1
                        continue
                    total_kept += 1
                    # (We don't insert in this diagnostic run, just count)

    print("\n=== DIAGNOSTIC SUMMARY ===")
    print(f"Total raw jobs fetched:   {total_raw}")
    print(f"Blocked by source filter: {total_source_blocked}")
    print(f"Duplicate URLs:           {total_duplicate}")
    print(f"Older than {DAYS_BACK} days:   {total_old}")
    print(f"Not fresher (or senior):  {total_not_fresher}")
    print(f"Jobs that would be kept:  {total_kept}")
    print(f"===========================")

    if total_kept == 0:
        print("⚠️ No jobs passed all filters. Check that your allowed sources (LinkedIn, Indeed, Glassdoor) are actually appearing in the API response.")
        # Show a sample of sources from raw jobs
        async with aiohttp.ClientSession() as session:
            sample_jobs = await fetch_jobs(session, SEARCH_TERMS[0], CITIES[0])
            sources = set()
            for job in sample_jobs[:10]:
                src = job.get("job_publisher", "").lower()
                if src:
                    sources.add(src)
            if sources:
                print(f"Sample sources from API: {', '.join(sources)}")
            else:
                print("No sources found – JSearch may not be returning jobs at all.")
    else:
        print("✅ Filters are working. To insert, remove diagnostic prints and re‑enable insertion.")

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    print(f"Finished in {elapsed:.2f}s")

if __name__ == "__main__":
    asyncio.run(main())
