import os
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta
from supabase import create_client
import re

# ================= CONFIGURATION =================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
RAPIDAPI_KEY = os.environ["RAPIDAPI_KEY"].strip()
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# Only these job sources (LinkedIn, Indeed, Glassdoor)
ALLOWED_SOURCES = {"linkedin", "indeed", "glassdoor"}

# Only these cities
CITIES = ["Hyderabad", "Bangalore", "Chennai"]

# Search terms for fresher jobs
SEARCH_TERMS = [
    "fresher software engineer",
    "graduate engineer trainee",
    "entry level developer",
    "fresher data analyst",
    "trainee engineer",
    "fresher devops",
    "fresher cybersecurity",
    "fresher qa"
]

DAYS_BACK = 3
RESULTS_PER_PAGE = 20

# Fresher keywords (must appear)
FRESHER_WORDS = {"fresher", "entry level", "graduate", "trainee", "junior", "0-1", "0-2", "1 year", "2 years", "2024", "2025", "recent graduate"}

# Senior keywords (exclude)
SENIOR_WORDS = {"senior", "lead", "principal", "architect", "manager", "director", "head", "vp", "cto", "staff"}

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# ================= HELPER FUNCTIONS =================
def extract_experience_level(title: str, description: str) -> str:
    text = (title + " " + description).lower()
    if any(w in text for w in ["fresher", "entry level", "graduate", "trainee"]):
        return "Fresher (0-2 years)"
    match = re.search(r'(\d+)\s*-\s*(\d+)\s*years?', text)
    if match:
        return f"{match.group(1)}-{match.group(2)} years"
    return "Fresher (0-2 years)"

def is_fresher_job(title: str, description: str) -> bool:
    text = (title + " " + description).lower()
    has_fresher = any(w in text for w in FRESHER_WORDS)
    has_senior = any(w in text for w in SENIOR_WORDS)
    return has_fresher and not has_senior

def clean_description(desc: str, max_len: int = 300) -> str:
    """Truncate description and remove excess whitespace."""
    if not desc:
        return "No description provided."
    desc = re.sub(r'\s+', ' ', desc).strip()
    if len(desc) > max_len:
        desc = desc[:max_len] + "..."
    return desc

async def fetch_jobs(session: aiohttp.ClientSession, query: str, city: str) -> list:
    url = "https://jsearch.p.rapidapi.com/search"
    params = {
        "query": f"{query} {city}",
        "page": 1,
        "num_pages": 1,
        "country": "in",
        "date_posted": "week",
        "results_per_page": RESULTS_PER_PAGE
    }
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": "jsearch.p.rapidapi.com"
    }
    # Remove any hidden newlines or spaces
    headers = {k: v.replace('\n', '').replace('\r', '').strip() for k, v in headers.items()}
    try:
        async with session.get(url, headers=headers, params=params, timeout=10) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            return data.get("data", [])
    except Exception:
        return []

async def send_telegram_message(session: aiohttp.ClientSession, text: str):
    """Send a message to Telegram. No crash if secrets missing."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram credentials missing – skipping notification.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        await session.post(url, json=payload)
    except Exception as e:
        print(f"Telegram send error: {e}")

# ================= MAIN WORKFLOW =================
async def main():
    print("🚀 ApplyMore – final robust scraper started")
    start_time = datetime.now(timezone.utc)

    # 1. Fetch existing URLs from DB (so we don't re-insert)
    existing_resp = supabase.table("ApplyMore").select("url").execute()
    existing_urls = {row["url"] for row in existing_resp.data} if existing_resp.data else set()
    print(f"📊 Existing jobs in DB (to avoid duplicates): {len(existing_urls)}")

    # 2. Parallel API calls for all (city, term) combinations
    async with aiohttp.ClientSession() as session:
        tasks = []
        for city in CITIES:
            for term in SEARCH_TERMS:
                tasks.append((city, term, fetch_jobs(session, term, city)))
        # Gather results while preserving city/term
        results = await asyncio.gather(*[task[2] for task in tasks])
        meta = [(task[0], task[1]) for task in tasks]

    # 3. Process each result with its city
    seen_urls = set()
    new_jobs = []
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    total_raw = 0

    for (city, term), job_list in zip(meta, results):
        total_raw += len(job_list)
        for job in job_list:
            title = job.get("job_title")
            company = job.get("employer_name")
            url = job.get("job_apply_link")
            description = job.get("job_description", "")
            posted_str = job.get("job_posted_at_datetime_utc") or job.get("job_posted_at")
            source = job.get("job_publisher", "").lower()

            # Basic validation
            if not title or not company or not url:
                continue

            # Source filter (LinkedIn, Indeed, Glassdoor only)
            if source not in ALLOWED_SOURCES:
                continue

            # Duplicate check (existing DB + this run)
            if url in existing_urls or url in seen_urls:
                continue

            # Date filter
            if posted_str:
                try:
                    posted_dt = datetime.fromisoformat(posted_str.replace('Z', '+00:00'))
                    if posted_dt < cutoff_date:
                        continue
                except:
                    pass

            # Fresher filter
            if not is_fresher_job(title, description):
                continue

            # If passed all filters, prepare job record
            exp_level = extract_experience_level(title, description)
            new_jobs.append({
                "title": title,
                "company": company,
                "location": city,
                "url": url,
                "description": description,
                "posted_date": posted_str or datetime.now(timezone.utc).isoformat(),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "experience_level": exp_level
            })
            seen_urls.add(url)

    print(f"📡 Raw jobs from API: {total_raw}")
    print(f"✨ New fresher jobs after filters: {len(new_jobs)}")

    # 4. Insert new jobs and send Telegram alert
    inserted_count = 0
    if new_jobs:
        # Insert in batches of 50
        for i in range(0, len(new_jobs), 50):
            batch = new_jobs[i:i+50]
            result = supabase.table("ApplyMore").insert(batch).execute()
            inserted_count += len(result.data) if result.data else 0
            print(f"   Inserted batch {i//50 + 1} ({len(batch)} jobs)")

        # Fetch the newly inserted jobs (to get their IDs) – order by created_at desc, limit to inserted count
        latest_resp = supabase.table("ApplyMore")\
            .select("id, title, company, location, description, experience_level")\
            .order("created_at", desc=True)\
            .limit(inserted_count)\
            .execute()
        if latest_resp.data:
            # Reverse to show oldest first (same order as insertion)
            inserted_jobs = list(reversed(latest_resp.data))
        else:
            inserted_jobs = []

        # Send Telegram alert with ApplyMore links
        async with aiohttp.ClientSession() as session:
            if inserted_jobs:
                lines = [f"✅ <b>ApplyMore – {len(inserted_jobs)} new fresher jobs</b>\n"]
                for idx, job in enumerate(inserted_jobs[:10], 1):
                    job_link = f"https://applymore.vercel.app/job.html?id={job['id']}"
                    desc_short = clean_description(job.get("description", ""), 200)
                    lines.append(
                        f"{idx}. <b>{job['title']}</b>\n"
                        f"   🏢 {job['company']} | 📍 {job['location']}\n"
                        f"   🎓 {job.get('experience_level', 'Fresher')}\n"
                        f"   📝 {desc_short}\n"
                        f"   🔗 <a href='{job_link}'>View & Apply on ApplyMore</a>\n"
                        f"   ⚠️ <b>APPLY ASAP</b>"
                    )
                if len(inserted_jobs) > 10:
                    lines.append(f"\n... and {len(inserted_jobs)-10} more. <a href='https://applymore.vercel.app'>Browse all jobs</a>")
                else:
                    lines.append(f"\n🌐 <a href='https://applymore.vercel.app'>Visit ApplyMore</a>")
                await send_telegram_message(session, "\n\n".join(lines))
            else:
                await send_telegram_message(session, "⚠️ ApplyMore: Jobs were inserted but could not retrieve IDs for Telegram message.")
    else:
        # No new jobs found – send a clear explanation
        async with aiohttp.ClientSession() as session:
            msg = (
                "⚠️ <b>ApplyMore Scraper – No New Jobs</b>\n\n"
                f"• Raw jobs fetched from API: {total_raw}\n"
                f"• After filters (source, duplicate, date, fresher): 0 kept.\n"
                "Possible reasons:\n"
                "  1. No new jobs posted in the last 3 days for these cities.\n"
                "  2. All jobs were from disallowed sources (not LinkedIn/Indeed/Glassdoor).\n"
                "  3. Jobs existed but were senior roles or lacked fresher keywords.\n\n"
                "The scraper will run again in 12 hours."
            )
            await send_telegram_message(session, msg)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    print(f"✅ Finished in {elapsed:.2f} seconds")
    print(f"✅ Inserted {inserted_count} new jobs.")

if __name__ == "__main__":
    asyncio.run(main())
