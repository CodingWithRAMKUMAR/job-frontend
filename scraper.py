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
CITIES = ["Hyderabad", "Bangalore", "Chennai"]   # Only these three
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
                return []
            data = await resp.json()
            return data.get("data", [])
    except:
        return []

async def send_telegram(session, text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    await session.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True})

async def main():
    print("🚀 ApplyMore scraper (Hyderabad, Bangalore, Chennai only)")
    start = datetime.now(timezone.utc)

    # Existing URLs
    existing = supabase.table("ApplyMore").select("url").execute()
    existing_urls = {row["url"] for row in existing.data} if existing.data else set()
    print(f"Existing jobs: {len(existing_urls)}")

    # Parallel fetch
    async with aiohttp.ClientSession() as session:
        tasks = []
        for city in CITIES:
            for term in SEARCH_TERMS:
                tasks.append((city, fetch_jobs(session, term, city)))
        results = await asyncio.gather(*[task[1] for task in tasks])
        city_map = [task[0] for task in tasks]

    # Process with city info
    seen_urls = set()
    new_jobs = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)

    for city, job_list in zip(city_map, results):
        for job in job_list:
            title = job.get("job_title")
            company = job.get("employer_name")
            url = job.get("job_apply_link")
            desc = job.get("job_description", "")
            posted_str = job.get("job_posted_at_datetime_utc") or job.get("job_posted_at")
            source = job.get("job_publisher", "").lower()
            if not title or not company or not url:
                continue
            if source not in ALLOWED_SOURCES:
                continue
            if url in existing_urls or url in seen_urls:
                continue
            if posted_str:
                try:
                    if datetime.fromisoformat(posted_str.replace('Z', '+00:00')) < cutoff:
                        continue
                except:
                    pass
            if not is_fresher_job(title, desc):
                continue
            exp_level = extract_experience_level(title, desc)
            new_jobs.append({
                "title": title,
                "company": company,
                "location": city,          # actual city name
                "url": url,
                "description": desc,
                "posted_date": posted_str or datetime.now(timezone.utc).isoformat(),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "experience_level": exp_level
            })
            seen_urls.add(url)

    print(f"New fresher jobs: {len(new_jobs)}")
    if new_jobs:
        # Insert in batches
        for i in range(0, len(new_jobs), 50):
            batch = new_jobs[i:i+50]
            supabase.table("ApplyMore").insert(batch).execute()
            print(f"Inserted batch {i//50+1}")
        # Telegram alert with ApplyMore links (need to fetch inserted IDs)
        # Fetch last inserted jobs by created_at
        latest = supabase.table("ApplyMore").select("id,title,company").order("created_at", desc=True).limit(len(new_jobs)).execute()
        if latest.data:
            inserted = list(reversed(latest.data))  # oldest first to match new_jobs order
            async with aiohttp.ClientSession() as session:
                lines = [f"✅ <b>ApplyMore – {len(inserted)} new fresher jobs</b>\n"]
                for idx, rec in enumerate(inserted[:10], 1):
                    link = f"https://applymore.vercel.app/job.html?id={rec['id']}"
                    lines.append(
                        f"{idx}. <b>{rec['title']}</b>\n"
                        f"   🏢 {rec['company']}\n"
                        f"   🔗 <a href='{link}'>View & Apply on ApplyMore</a>\n"
                        f"   ⚠️ <b>APPLY ASAP</b>"
                    )
                if len(inserted) > 10:
                    lines.append(f"\n... and {len(inserted)-10} more. <a href='https://applymore.vercel.app'>Browse all</a>")
                else:
                    lines.append(f"\n🌐 <a href='https://applymore.vercel.app'>Visit ApplyMore</a>")
                await send_telegram(session, "\n\n".join(lines))
    else:
        async with aiohttp.ClientSession() as session:
            await send_telegram(session, "⚠️ ApplyMore scraper ran but found no new fresher jobs.")

    print(f"Finished in {(datetime.now(timezone.utc)-start).total_seconds():.2f}s")

if __name__ == "__main__":
    asyncio.run(main())
