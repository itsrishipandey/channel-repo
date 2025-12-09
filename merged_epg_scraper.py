#!/usr/bin/env python3
"""
merged_epg_scraper.py

Merged EPG scraper that:
1. Scrapes DishTV schedules from channel.txt
2. Scrapes Tata Play and Jio TV schedules from XML sources
3. Filters XML sources using filter_list.txt
4. Saves all schedules to today/ and tomorrow/ directories
5. Maintains a single log file: scrape_log.log

All timestamps are in Indian Standard Time (IST).
"""

import requests
import gzip
import xml.etree.ElementTree as ET
import json
import os
import sys
import time
import threading
import re
import shutil
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import pytz

# ---------- CONFIG ----------
# DishTV API URLs
SIGNIN_URL = "https://www.dishtv.in/services/epg/signin"
PROGRAMS_URL = "https://epg.mysmartstick.com/dishtv/api/v1/epg/entities/programs"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Origin": "https://www.dishtv.in",
    "Referer": "https://www.dishtv.in/",
}

# Input files
CHANNELS_FILE = "channel.txt"
FILTER_FILE = "filter_list.txt"

# Output directories
OUT_DIR_TODAY = "today"
OUT_DIR_TOMORROW = "tomorrow"

# Scraping settings
MAX_WORKERS = 30
MAX_RETRIES = 3
RETRY_BACKOFF = 1.2

# Log file (fixed name, overwritten each run)
LOG_FILE = "scrape_log.log"

# Indian timezone
IST = pytz.timezone('Asia/Kolkata')

# ---------- END CONFIG ----------

lock = threading.Lock()

# Progress tracking
progress = {
    "total": 0,
    "done": 0
}

# Channel tracking for log
channels_found = {
    "today": [],
    "tomorrow": []
}
channels_not_found = {
    "today": [],
    "tomorrow": []
}


def get_ist_time():
    """Get current time in IST"""
    return datetime.now(IST)


def write_log(line: str):
    """Append a line to the log file (thread-safe)."""
    ts = get_ist_time().strftime("%Y-%m-%d %H:%M:%S IST")
    entry = f"[{ts}] {line}\n"
    with lock:
        with open(LOG_FILE, "a", encoding="utf-8") as lf:
            lf.write(entry)


def simple_progress_bar():
    """Print a simple progress bar (thread-safe)."""
    with lock:
        total = progress["total"]
        done = progress["done"]
        pct = (done / total * 100) if total else 100
        bar_len = 40
        filled = int(bar_len * done / total) if total else bar_len
        bar = "#" * filled + "-" * (bar_len - filled)
        sys.stdout.write(f"\rProgress: |{bar}| {done}/{total} ({pct:.1f}%)")
        sys.stdout.flush()
        if done == total:
            sys.stdout.write("\n")


def clean_directories():
    """Delete all old files in today and tomorrow directories"""
    write_log("Cleaning old files from directories...")
    
    for directory in [OUT_DIR_TODAY, OUT_DIR_TOMORROW]:
        if os.path.exists(directory):
            shutil.rmtree(directory)
            write_log(f"Deleted directory: {directory}")
        os.makedirs(directory, exist_ok=True)
        write_log(f"Created fresh directory: {directory}")


def parse_channel_file(filename: str):
    """Read channel file and return list of tuples: (channelid, channel_name)"""
    channels = []
    if not os.path.exists(filename):
        write_log(f"WARNING: {filename} not found")
        return channels
    
    with open(filename, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                left, right = line.split("=", 1)
                channelid = left.strip()
                name = right.strip()
            else:
                channelid = line.strip()
                name = ""
            channels.append((channelid, name))
    
    write_log(f"Loaded {len(channels)} channels from {filename}")
    return channels


def load_filter_list(filename: str):
    """Load filter list of channel filenames to include"""
    filter_set = set()
    if not os.path.exists(filename):
        write_log(f"WARNING: {filename} not found - will process all XML channels")
        return None
    
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                filter_set.add(line.lower())
    
    write_log(f"Loaded {len(filter_set)} channel filters from {filename}")
    return filter_set


def get_token_session(session: requests.Session):
    """Get authentication token for DishTV API"""
    resp = session.post(SIGNIN_URL, headers=HEADERS, json={}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if "token" not in data:
        raise RuntimeError(f"signin response missing token: {data}")
    return data["token"]


def fetch_epg_for_date(session: requests.Session, channelid: str, date_ddmmyyyy: str):
    """Fetch EPG data from DishTV API for a specific date"""
    payload = {
        "channelid": channelid,
        "date": date_ddmmyyyy,
        "allowPastEvents": True
    }
    resp = session.post(PROGRAMS_URL, headers=HEADERS, json=payload, timeout=20)
    resp.raise_for_status()
    return resp.json()


def time_12h_no_tz(iso_str):
    """Convert ISO time to 12-hour format"""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return dt.strftime("%I:%M %p").lstrip('0')


def sanitize_filename(filename):
    """Sanitize filename for cross-platform compatibility"""
    filename = re.sub(r'[<>:"/\\|?*]', '-', filename)
    filename = filename.strip()
    return filename.lower().replace(' ', '-') + '.json'


def make_slug_from_txt_name(name: str):
    """Make slug from channel name in channel.txt"""
    if not name:
        return "channel.json"
    slug = name.lower().strip().replace(" ", "-")
    slug = "".join(c for c in slug if c.isalnum() or c in "-")
    return slug + ".json"


def format_output_from_epg(epg_data):
    """Format DishTV EPG data to standard output format"""
    programs = epg_data if isinstance(epg_data, list) else epg_data.get("programs", [])
    channel_name = programs[0].get("channelname", "Unknown Channel") if programs else "Unknown Channel"

    if programs:
        dt = datetime.fromisoformat(programs[0]["start"].replace("Z", "+00:00"))
        date_str = dt.strftime("%B %d, %Y")
    else:
        date_str = get_ist_time().strftime("%B %d, %Y")

    schedule = []
    for p in programs:
        schedule.append({
            "show_name": p.get("title", ""),
            "start_time": time_12h_no_tz(p["start"]),
            "end_time": time_12h_no_tz(p["stop"]),
            "show_logo": p.get("programmeurl", "")
        })

    return {
        "channel_name": channel_name,
        "date": date_str,
        "schedule": schedule
    }


def save_json_out(data: dict, out_dir: str, filename: str, channel_identifier: str, period: str):
    """Save JSON output and track success"""
    path = os.path.join(out_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    with lock:
        channels_found[period].append(f"{channel_identifier} -> {filename}")
    
    return path


def attempt_fetch_channel(channel_tuple, date_ddmmyyyy, out_dir, period):
    """Attempt to fetch DishTV channel data with retries"""
    channelid, txt_name = channel_tuple
    last_err = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session = requests.Session()
            session.headers.update(HEADERS)

            token = get_token_session(session)
            session.headers.update({"Authorization": token})

            epg = fetch_epg_for_date(session, channelid, date_ddmmyyyy)

            formatted = format_output_from_epg(epg)
            filename = make_slug_from_txt_name(txt_name)
            
            saved_path = save_json_out(formatted, out_dir, filename, 
                                      f"{channelid} ({txt_name})", period)

            return True, saved_path

        except Exception as e:
            last_err = e
            write_log(f"Attempt {attempt} failed for {channelid} on {date_ddmmyyyy}: {repr(e)}")
            time.sleep(RETRY_BACKOFF ** attempt)

    # Track failed channel
    with lock:
        channels_not_found[period].append(f"{channelid} ({txt_name})")
    
    return False, str(last_err)


def worker_task(channel_tuple, date_ddmmyyyy, out_dir, period):
    """Worker task for parallel DishTV scraping"""
    ok, info = attempt_fetch_channel(channel_tuple, date_ddmmyyyy, out_dir, period)

    channelid, txt_name = channel_tuple
    if ok:
        write_log(f"SUCCESS: {channelid} ({txt_name}) date={date_ddmmyyyy} saved={info}")
    else:
        write_log(f"FAIL: {channelid} ({txt_name}) date={date_ddmmyyyy} reason={info}")

    with lock:
        progress["done"] += 1
    simple_progress_bar()

    return ok


# ========== XML EPG PROCESSING ==========

def parse_xmltv_time(time_str):
    """Parse XMLTV time format and convert to IST"""
    dt_str = time_str.split(' ')[0]
    dt = datetime.strptime(dt_str, '%Y%m%d%H%M%S')
    # Convert to IST
    dt = dt.replace(tzinfo=pytz.UTC).astimezone(IST)
    return dt


def format_time(dt):
    """Format datetime to 12-hour format"""
    return dt.strftime('%I:%M %p').lstrip('0')


def format_date(dt):
    """Format date to 'Month DD, YYYY' format"""
    return dt.strftime('%B %d, %Y')


def download_gz_epg(url):
    """Download and decompress .gz file"""
    write_log(f"Downloading: {url}")
    
    if 'github.com' in url and '/blob/' in url:
        url = url.replace('github.com', 'raw.githubusercontent.com').replace('/blob/', '/')
    
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    
    with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz:
        xml_content = gz.read()
    
    return xml_content.decode('utf-8')


def parse_epg_xml(xml_content):
    """Parse EPG XML and extract channel and programme data"""
    root = ET.fromstring(xml_content)
    
    channels = {}
    for channel in root.findall('channel'):
        channel_id = channel.get('id')
        display_name = channel.find('display-name').text if channel.find('display-name') is not None else channel_id
        icon = channel.find('icon')
        channel_logo = icon.get('src') if icon is not None else ""
        
        channels[channel_id] = {
            'name': display_name,
            'logo': channel_logo
        }
    
    programmes = {}
    for programme in root.findall('programme'):
        channel_id = programme.get('channel')
        
        if channel_id not in programmes:
            programmes[channel_id] = []
        
        title = programme.find('title')
        show_name = title.text if title is not None else "Unknown Show"
        
        start_time = parse_xmltv_time(programme.get('start'))
        end_time = parse_xmltv_time(programme.get('stop'))
        
        icon = programme.find('icon')
        show_logo = icon.get('src') if icon is not None else ""
        
        programmes[channel_id].append({
            'show_name': show_name,
            'start_time': start_time,
            'end_time': end_time,
            'show_logo': show_logo
        })
    
    return channels, programmes


def filter_programmes_by_date(programmes, target_date):
    """Filter programmes for a specific date (full 24 hours)"""
    filtered = []
    midnight_start = datetime.combine(target_date, datetime.min.time())
    midnight_start = IST.localize(midnight_start)
    midnight_end = midnight_start + timedelta(days=1)
    
    for prog in programmes:
        start_dt = prog['start_time']
        end_dt = prog['end_time']
        
        # Include if program overlaps with target date
        if start_dt < midnight_end and end_dt > midnight_start:
            adjusted_prog = prog.copy()
            
            # Adjust start time if program started before target date
            if start_dt < midnight_start:
                adjusted_prog['start_time'] = midnight_start
            
            # Adjust end time if program extends beyond target date
            if end_dt > midnight_end:
                adjusted_prog['end_time'] = midnight_end
            
            filtered.append(adjusted_prog)
    
    filtered.sort(key=lambda x: x['start_time'])
    return filtered


def create_json_schedule(channel_name, channel_logo, programmes, target_date):
    """Create JSON schedule in the specified format"""
    schedule_data = {
        "channel_name": channel_name,
        "date": format_date(target_date),
        "schedule": []
    }
    
    for prog in programmes:
        schedule_data["schedule"].append({
            "show_name": prog['show_name'],
            "start_time": format_time(prog['start_time']),
            "end_time": format_time(prog['end_time']),
            "show_logo": prog['show_logo']
        })
    
    return schedule_data


def process_xml_epg(filter_set, jiotv_data, tataplay_data):
    """Process XML EPG sources with priority: JioTV > Tata Play"""
    write_log("Processing XML EPG sources...")
    
    today = get_ist_time().date()
    tomorrow = today + timedelta(days=1)
    
    # Combine channels from both sources
    all_channels = {}
    
    # First add Tata Play channels
    if tataplay_data:
        channels_tp, programmes_tp = tataplay_data
        for channel_id, channel_info in channels_tp.items():
            filename = sanitize_filename(channel_info['name'])
            all_channels[filename] = {
                'info': channel_info,
                'programmes': programmes_tp.get(channel_id, []),
                'source': 'Tata Play'
            }
    
    # Then add/override with JioTV channels (priority)
    if jiotv_data:
        channels_jio, programmes_jio = jiotv_data
        for channel_id, channel_info in channels_jio.items():
            filename = sanitize_filename(channel_info['name'])
            all_channels[filename] = {
                'info': channel_info,
                'programmes': programmes_jio.get(channel_id, []),
                'source': 'Jio TV'
            }
    
    write_log(f"Total unique channels from XML sources: {len(all_channels)}")
    
    # Process channels based on filter
    processed_count = 0
    for filename, channel_data in all_channels.items():
        # Skip if filter exists and channel not in filter
        if filter_set is not None and filename not in filter_set:
            continue
        
        channel_info = channel_data['info']
        channel_progs = channel_data['programmes']
        source = channel_data['source']
        
        # Process today's schedule
        today_progs = filter_programmes_by_date(channel_progs, today)
        if today_progs:
            today_schedule = create_json_schedule(
                channel_info['name'],
                channel_info['logo'],
                today_progs,
                today
            )
            filepath = os.path.join(OUT_DIR_TODAY, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(today_schedule, f, indent=2, ensure_ascii=False)
            
            with lock:
                channels_found["today"].append(f"{channel_info['name']} ({source}) -> {filename}")
        else:
            with lock:
                channels_not_found["today"].append(f"{channel_info['name']} ({source})")
        
        # Process tomorrow's schedule
        tomorrow_progs = filter_programmes_by_date(channel_progs, tomorrow)
        if tomorrow_progs:
            tomorrow_schedule = create_json_schedule(
                channel_info['name'],
                channel_info['logo'],
                tomorrow_progs,
                tomorrow
            )
            filepath = os.path.join(OUT_DIR_TOMORROW, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(tomorrow_schedule, f, indent=2, ensure_ascii=False)
            
            with lock:
                channels_found["tomorrow"].append(f"{channel_info['name']} ({source}) -> {filename}")
        else:
            with lock:
                channels_not_found["tomorrow"].append(f"{channel_info['name']} ({source})")
        
        processed_count += 1
    
    write_log(f"Processed {processed_count} channels from XML sources")


def fetch_xml_source(name, url):
    """Fetch and parse XML EPG source"""
    try:
        write_log(f"Fetching {name} EPG...")
        xml_content = download_gz_epg(url)
        channels, programmes = parse_epg_xml(xml_content)
        write_log(f"Successfully parsed {name}: {len(channels)} channels")
        return (channels, programmes)
    except Exception as e:
        write_log(f"ERROR fetching {name}: {repr(e)}")
        return None


def write_summary_log():
    """Write summary of all channels found and not found"""
    write_log("\n" + "=" * 70)
    write_log("SCRAPING SUMMARY")
    write_log("=" * 70)
    
    # Today's channels
    write_log(f"\nTODAY - Channels Found: {len(channels_found['today'])}")
    for ch in sorted(channels_found['today']):
        write_log(f"  ✓ {ch}")
    
    write_log(f"\nTODAY - Channels Not Found: {len(channels_not_found['today'])}")
    for ch in sorted(channels_not_found['today']):
        write_log(f"  ✗ {ch}")
    
    # Tomorrow's channels
    write_log(f"\nTOMORROW - Channels Found: {len(channels_found['tomorrow'])}")
    for ch in sorted(channels_found['tomorrow']):
        write_log(f"  ✓ {ch}")
    
    write_log(f"\nTOMORROW - Channels Not Found: {len(channels_not_found['tomorrow'])}")
    for ch in sorted(channels_not_found['tomorrow']):
        write_log(f"  ✗ {ch}")
    
    write_log("\n" + "=" * 70)


def main():
    """Main function"""
    # Initialize log file (overwrite)
    with open(LOG_FILE, "w", encoding="utf-8") as lf:
        start_time = get_ist_time()
        lf.write(f"EPG Scraping started at {start_time.strftime('%Y-%m-%d %H:%M:%S IST')}\n")
        lf.write("=" * 70 + "\n")
    
    print(f"[+] Starting EPG scraping at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S IST')}")
    
    # Clean old files
    clean_directories()
    
    # Load filter list for XML sources
    filter_set = load_filter_list(FILTER_FILE)
    
    # ===== PART 1: DishTV Scraping =====
    print("\n[+] Part 1: Scraping DishTV channels...")
    write_log("\n--- DISHTV SCRAPING ---")
    
    if os.path.exists(CHANNELS_FILE):
        channels = parse_channel_file(CHANNELS_FILE)
        
        if channels:
            today_ist = get_ist_time()
            tomorrow_ist = today_ist + timedelta(days=1)
            
            today_ddmmyyyy = today_ist.strftime("%d/%m/%Y")
            tomorrow_ddmmyyyy = tomorrow_ist.strftime("%d/%m/%Y")
            
            progress["total"] = len(channels) * 2
            progress["done"] = 0
            
            print(f"[+] Scraping {len(channels)} DishTV channels (today + tomorrow)...")
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                
                # Today
                for ch in channels:
                    futures.append(executor.submit(worker_task, ch, today_ddmmyyyy, OUT_DIR_TODAY, "today"))
                
                # Tomorrow
                for ch in channels:
                    futures.append(executor.submit(worker_task, ch, tomorrow_ddmmyyyy, OUT_DIR_TOMORROW, "tomorrow"))
                
                for f in as_completed(futures):
                    pass
            
            print("\n[+] DishTV scraping completed.")
        else:
            write_log("No channels found in channel.txt")
    else:
        write_log(f"WARNING: {CHANNELS_FILE} not found - skipping DishTV scraping")
        print(f"[!] {CHANNELS_FILE} not found - skipping DishTV scraping")
    
    # ===== PART 2: XML EPG Scraping =====
    print("\n[+] Part 2: Scraping XML EPG sources...")
    write_log("\n--- XML EPG SCRAPING ---")
    
    # Fetch Tata Play
    tataplay_data = fetch_xml_source("Tata Play", "https://avkb.short.gy/tsepg.xml.gz")
    
    # Fetch Jio TV
    jiotv_data = fetch_xml_source("Jio TV", "https://avkb.short.gy/jioepg.xml.gz")
    
    # Process XML EPG with priority
    if tataplay_data or jiotv_data:
        process_xml_epg(filter_set, jiotv_data, tataplay_data)
        print("[+] XML EPG processing completed.")
    else:
        write_log("WARNING: No XML EPG data available")
        print("[!] No XML EPG data available")
    
    # Write summary
    write_summary_log()
    
    end_time = get_ist_time()
    write_log(f"\nScraping completed at {end_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
    
    print(f"\n[+] All done! Check {LOG_FILE} for details.")
    print(f"[+] Schedules saved in: {OUT_DIR_TODAY}/ and {OUT_DIR_TOMORROW}/")


if __name__ == "__main__":
    main()
