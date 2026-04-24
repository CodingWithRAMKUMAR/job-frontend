import os
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta
from supabase import create_client
import re

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
RAPIDAPI_KEY = os.environ["RAPIDAPI_KEY"].strip()

# Fresher keywords (must appear in title OR description)
FRESHER_KEYWORDS = [
    "fresher", "entry level", "entry-level", "graduate", "recent graduate",
    "0-1", "0-2", "1 year", "2 years", "2024", "2025", "trainee", "intern",
    "junior", "associate", "early career"
]

# Keywords that indicate NOT fresher (exclude)
SENIOR_KEYWORDS = [
    "senior", "lead", "principal", "architect", "manager", "director",
    "head of", "staff engineer", "expert", "vp", "cto"
]

# Cities
ALLOWED_CITIES = {c.lower() for c in [
    "Hyderabad", "Chennai", "Mumbai", "Pune", "Gurugram", "Bangalore",
    "Delhi", "Kolkata", "Ahmedabad", "Noida"
]}

# Search queries (role, city)
SEARCHES = [
    ("Data Analyst", "Hyderabad"), ("Data Analyst", "Bangalore"),
    ("Apprenticeship", "Chennai"), ("Java Developer", "Mumbai"),
    ("Python Developer", "Pune"), ("Cyber Security", "Gurugram"),
    ("DevOps Engineer", "Noida"), ("Data Scientist", "Delhi"),
    ("Software Engineer", "Hyderabad"), ("Software Engineer", "Bangalore"),
]

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def is_fresher_job(title, description):
    """Return True if job is for fresher (based on title or description)."""
    text = (title + " " + description).lower()
    # Must contain at least one fresher keyword
    has_fresher = any(kw in text for kw in FRESHER_KEYWORDS)
    # Must NOT contain senior keywords
    has_senior = any(kw in text for kw in SENIOR_KEYWORDS)
    return has_fresher and not has_senior

def parse_location(loc):
    if not loc:
        return None
    city = loc.split(",")[0].strip()
    return city if city.lower() in ALLOWED_CITIES else None

def is_recent(posted_date_str):
    """Check if job posted within last 7 days."""
    if not posted_date_str:
        return False
    try:
        # JSearch returns ISO format like '2025-04-20T00:00:00Z'
        posted = datetime.fromisoformat(posted_date_str.replace('Z', '+00:00'))
        now = datetime.now(timezone.utc)
        return (now - posted).days <= 7
    except:
        return True  # if parsing fails, assume recent

async def fetch_with_retry(session, role, city, retries=3):
    """Fetch jobs with retry on 429 or other errors."""
    url = "https://jsearch.p.rapidapi.com/search"
    params = {
        "query": f"{role} {city}",
        "page": 1,
        "num_pages": 1,
        "country": "in",
        "date_posted": "week"  # only last 7 days
    }
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": "jsearch.p.rapidapi.com"
    }
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", [])
                elif resp.status == 429:
                    wait = 2 ** attempt  # 1, 2, 4 seconds
                    print(f"Rate limit for {role} in {city}, retry in {wait}s...")
                    await asyncio.sleep(wait)
                else:
                    print(f"Error {resp.status} for {role} in {city}")
                    return []
        except Exception as e:
            print(f"Exception for {role} in {city}: {e}")
            await asyncio.sleep(1)
    return []

async def main():
    print("Starting fresher job scrape...")
    
    # First, fetch existing URLs from Supabase
    existing = supabase.table("ApplyMore").select("url").execute()
    existing_urls = {row["url"] for row in existing.data} if existing.data else set()
    print(f"Existing jobs in DB: {len(existing_urls)}")
    
    new_jobs = []
    seen_urls = set()
    
    async with aiohttp.ClientSession() as session:
        for role, city in SEARCHES:
            print(f"Searching {role} in {city}...")
            jobs = await fetch_with_retry(session, role, city)
            print(f"  Fetched {len(jobs)} raw jobs")
            
            for job in jobs:
                title = job.get("job_title")
                company = job.get("employer_name")
                url = job.get("job_apply_link")
                location_raw = job.get("job_location")
                description = job.get("job_description", "")
                posted_date = job.get("job_posted_at_datetime_utc") or job.get("job_posted_at")
                
                # Basic validation
                if not title or not company or not url:
                    continue
                
                # Duplicate check
                if url in existing_urls or url in seen_urls:
                    continue
                
                # Location filter
                city_parsed = parse_location(location_raw)
                if not city_parsed:
                    continue
                
                # Recent check
                if not is_recent(posted_date):
                    continue
                
                # Fresher filter (key)
                if not is_fresher_job(title, description):
                    continue
                
                # If passed all filters, add to new jobs
                seen_urls.add(url)
                new_jobs.append({
                    "title": title,
                    "company": company,
                    "location": city_parsed,
                    "url": url,
                    "description": description,
                    "posted_date": posted_date or datetime.now(timezone.utc).isoformat(),
                    "created_at": datetime.now(timezone.utc).isoformat()
                })
            
            # Delay between searches to avoid rate limit
            await asyncio.sleep(2)
    
    print(f"\nNew fresher jobs found: {len(new_jobs)}")
    
    if new_jobs:
        # Batch insert into Supabase
        batch_size = 50
        for i in range(0, len(new_jobs), batch_size):
            batch = new_jobs[i:i+batch_size]
            supabase.table("ApplyMore").insert(batch).execute()
            print(f"Inserted batch {i//batch_size + 1} ({len(batch)} jobs)")
        
        # Telegram notification
        token = os.environ.get("TELEGRAM_BOT_TOKEN")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        if token and chat_id:
            async with aiohttp.ClientSession() as session:
                msg = f"✅ ApplyMore: {len(new_jobs)} new fresher jobs added."
                await session.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": msg})
    else:
        print("No new fresher jobs to insert.")
        
        # Optionally send a notification that nothing was found
        token = os.environ.get("TELEGRAM_BOT_TOKEN")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        if token and chat_id:
            async with aiohttp.ClientSession() as session:
                msg = "⚠️ ApplyMore scraper ran but found no new fresher jobs."
                await session.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": msg})
    
    print("Done.")

if __name__ == "__main__":
    asyncio.run(main())
