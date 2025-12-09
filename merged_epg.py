#!/usr/bin/env python3
"""
merged_epg_scraper.py

Merges DishTV and XML EPG (JioTV, Tata Play) scraping.
- Reads channel list from 'channel.txt'
- Scrapes DishTV using API
- Parses JioTV and Tata Play XML/GZ EPGs
- Filters by filter_list.txt
- Saves JSON to ./today/ and ./tomorrow/
- Creates unified log file with IST timestamps
- Cleans old files before each run
"""

import requests
import gzip
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import json
import os
import sys
import time
import threading
import re
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- CONFIG ----------
SIGNIN_URL = "https://www.dishtv.in/services/epg/signin"
PROGRAMS_URL = "https://epg.mysmartstick.com/dishtv/api/v1/epg/entities/programs"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Origin": "https://www.dishtv.in",
    "Referer": "https://www.dishtv.in/",
}

CHANNELS_FILE = "channel.txt"
FILTER_FILE = "filter_list.txt"
OUT_DIR_TODAY = "today"
OUT_DIR_TOMORROW = "tomorrow"

MAX_WORKERS = 30
MAX_RETRIES = 3
RETRY_BACKOFF = 1.2

# IST Timezone offset
IST_OFFSET = timedelta(hours=5, minutes=30)

# ---------- END CONFIG ----------

lock = threading.Lock()

progress = {
    "total": 0,
    "done": 0
}

LOG_FILE = "scrape_log.log"

# Track results for final summary
found_channels_today = []
found_channels_tomorrow = []
not_found_channels_today = []
not_found_channels_tomorrow = []


def get_ist_now():
    """Get current time in IST"""
    return datetime.now() + IST_OFFSET


def write_log(line: str):
    """Append a line to the log file (thread-safe)."""
    ts = get_ist_now().strftime("%Y-%m-%d %H:%M:%S")
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
    """Delete all old JSON files from today and tomorrow directories"""
    for directory in [OUT_DIR_TODAY, OUT_DIR_TOMORROW]:
        if os.path.exists(directory):
            for file in os.listdir(directory):
                if file.endswith(".json"):
                    file_path = os.path.join(directory, file)
                    try:
                        os.remove(file_path)
                    except Exception as e:
                        write_log(f"Failed to delete {file_path}: {str(e)}")


def ensure_dirs():
    """Create output directories"""
    os.makedirs(OUT_DIR_TODAY, exist_ok=True)
    os.makedirs(OUT_DIR_TOMORROW, exist_ok=True)


def parse_channel_file(filename: str):
    """
    Read channel file and return list of tuples: (channelid, channel_name_or_empty)
    Accepts:
      144478=Zee TV
      143831=Star Plus
      54022
    """
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
    
    return channels


def parse_filter_list(filename: str):
    """Read filter_list.txt and return set of channel filenames (without .json)"""
    filters = set()
    if not os.path.exists(filename):
        write_log(f"WARNING: {filename} not found, using all channels")
        return None
    
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                # Remove .json if present
                if line.endswith(".json"):
                    line = line[:-5]
                filters.add(line.lower())
    
    return filters if filters else None


def sanitize_filename(filename):
    """Sanitize filename for Windows/Linux compatibility"""
    filename = re.sub(r'[<>:"/\\|?*]', '-', filename)
    filename = filename.strip()
    return filename.lower().replace(' ', '-') + '.json'


def get_token_session(session: requests.Session):
    """Get auth token from DishTV"""
    resp = session.post(SIGNIN_URL, headers=HEADERS, json={}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if "token" not in data:
        raise RuntimeError(f"signin response missing token: {data}")
    return data["token"]


def fetch_epg_for_date(session: requests.Session, channelid: str, date_ddmmyyyy: str):
    """Fetch EPG from DishTV API"""
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
    return dt.strftime("%I:%M %p").lstrip("0")


def make_slug_from_txt_name(name: str):
    """Make slug from NAME IN channel.txt"""
    if not name:
        return "channel"
    slug = name.lower().strip().replace(" ", "-")
    slug = "".join(c for c in slug if c.isalnum() or c in "-")
    return slug


def format_output_from_epg(epg_data):
    """Formats final output from DishTV"""
    programs = epg_data if isinstance(epg_data, list) else epg_data.get("programs", [])
    channel_name = programs[0].get("channelname", "Unknown Channel") if programs else "Unknown Channel"

    if programs:
        dt = datetime.fromisoformat(programs[0]["start"].replace("Z", "+00:00"))
        date_str = dt.strftime("%B %d, %Y")
    else:
        date_str = datetime.now().strftime("%B %d, %Y")

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


def save_json_out(data: dict, out_dir: str, txt_channel_name: str):
    """Saves JSON using NAME FROM CHANNEL.TXT"""
    filename = make_slug_from_txt_name(txt_channel_name) + ".json"
    path = os.path.join(out_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    return path


def attempt_fetch_channel(channel_tuple, date_ddmmyyyy, out_dir):
    """Attempt to fetch channel from DishTV with retries"""
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

            saved_path = save_json_out(formatted, out_dir, txt_name)

            return True, saved_path

        except Exception as e:
            last_err = e
            write_log(f"Attempt {attempt} failed for {channelid} on {date_ddmmyyyy}: {repr(e)}")
            time.sleep(RETRY_BACKOFF ** attempt)

    return False, str(last_err)


def worker_task_dishtv(channel_tuple, date_ddmmyyyy, out_dir, is_today):
    """Worker task for DishTV scraping"""
    ok, info = attempt_fetch_channel(channel_tuple, date_ddmmyyyy, out_dir)

    channelid, txt_name = channel_tuple
    slug = make_slug_from_txt_name(txt_name) + ".json"
    
    if ok:
        write_log(f"✓ DishTV | {txt_name} ({channelid}) | {date_ddmmyyyy}")
        if is_today:
            found_channels_today.append(slug)
        else:
            found_channels_tomorrow.append(slug)
    else:
        write_log(f"✗ DishTV | {txt_name} ({channelid}) | {date_ddmmyyyy} | {info}")
        if is_today:
            not_found_channels_today.append(slug)
        else:
            not_found_channels_tomorrow.append(slug)

    with lock:
        progress["done"] += 1
    simple_progress_bar()

    return ok


# ========== XML EPG FUNCTIONS ==========

def parse_xmltv_time(time_str):
    """Parse XMLTV time format and convert to IST"""
    dt_str = time_str.split(' ')[0]
    dt = datetime.strptime(dt_str, '%Y%m%d%H%M%S')
    dt = dt + IST_OFFSET
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
    
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz:
        xml_content = gz.read()
    
    return xml_content.decode('utf-8')


def download_xml_epg(url):
    """Download XML file directly"""
    write_log(f"Downloading: {url}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.text


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
    """Filter programmes for a specific date (00:00 to 23:59)"""
    filtered = []
    start_of_day = datetime.combine(target_date, datetime.min.time())
    end_of_day = datetime.combine(target_date, datetime.max.time())
    
    for prog in programmes:
        start_dt = prog['start_time']
        end_dt = prog['end_time']
        
        # Include if programme overlaps with target date
        if start_dt < end_of_day and end_dt > start_of_day:
            adjusted_prog = prog.copy()
            
            # Clip start time to start of day if it's earlier
            if start_dt < start_of_day:
                adjusted_prog['start_time'] = start_of_day
            
            # Clip end time to end of day if it's later
            if end_dt > end_of_day:
                adjusted_prog['end_time'] = end_of_day
            
            filtered.append(adjusted_prog)
    
    # Sort by start time
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


def process_xml_epg(epg_name, epg_url, is_gz=True):
    """Process XML EPG from JioTV or Tata Play"""
    write_log(f"\n=== Processing {epg_name} EPG ===")
    write_log(f"Converting UTC to IST (UTC+5:30)")
    
    try:
        if is_gz:
            xml_content = download_gz_epg(epg_url)
        else:
            xml_content = download_xml_epg(epg_url)
        
        channels, programmes = parse_epg_xml(xml_content)
        write_log(f"✓ Downloaded {epg_name} | Found {len(channels)} channels")
        
        today = datetime.now().date()
        tomorrow = today + timedelta(days=1)
        
        filter_set = parse_filter_list(FILTER_FILE)
        
        processed_count = 0
        for channel_id, channel_info in channels.items():
            if channel_id in programmes:
                channel_name = channel_info['name']
                filename = sanitize_filename(channel_name)
                filename_slug = filename[:-5]  # Remove .json
                
                # Check filter
                if filter_set and filename_slug not in filter_set:
                    continue
                
                channel_progs = programmes[channel_id]
                
                # Today
                today_progs = filter_programmes_by_date(channel_progs, today)
                if today_progs:
                    today_schedule = create_json_schedule(
                        channel_name,
                        channel_info['logo'],
                        today_progs,
                        today
                    )
                    filepath = os.path.join(OUT_DIR_TODAY, filename)
                    with open(filepath, 'w', encoding='utf-8') as f:
                        json.dump(today_schedule, f, indent=2, ensure_ascii=False)
                    
                    write_log(f"✓ {epg_name} | {channel_name} | TODAY")
                    if filename_slug not in found_channels_today:
                        found_channels_today.append(filename_slug)
                else:
                    write_log(f"✗ {epg_name} | {channel_name} | TODAY (no programs)")
                    if filename_slug not in not_found_channels_today:
                        not_found_channels_today.append(filename_slug)
                
                # Tomorrow
                tomorrow_progs = filter_programmes_by_date(channel_progs, tomorrow)
                if tomorrow_progs:
                    tomorrow_schedule = create_json_schedule(
                        channel_name,
                        channel_info['logo'],
                        tomorrow_progs,
                        tomorrow
                    )
                    filepath = os.path.join(OUT_DIR_TOMORROW, filename)
                    with open(filepath, 'w', encoding='utf-8') as f:
                        json.dump(tomorrow_schedule, f, indent=2, ensure_ascii=False)
                    
                    write_log(f"✓ {epg_name} | {channel_name} | TOMORROW")
                    if filename_slug not in found_channels_tomorrow:
                        found_channels_tomorrow.append(filename_slug)
                else:
                    write_log(f"✗ {epg_name} | {channel_name} | TOMORROW (no programs)")
                    if filename_slug not in not_found_channels_tomorrow:
                        not_found_channels_tomorrow.append(filename_slug)
                
                processed_count += 1
        
        write_log(f"✓ {epg_name}: Processed {processed_count} channels")
        
    except Exception as e:
        write_log(f"✗ Error processing {epg_name}: {str(e)}")


def write_final_summary():
    """Write final summary to log"""
    write_log("\n" + "="*60)
    write_log("FINAL SUMMARY")
    write_log("="*60)
    
    write_log("\nCHANNELS FOUND - TODAY:")
    if found_channels_today:
        for ch in sorted(set(found_channels_today)):
            write_log(f"  ✓ {ch}")
    else:
        write_log("  (none)")
    
    write_log("\nCHANNELS NOT FOUND - TODAY:")
    if not_found_channels_today:
        for ch in sorted(set(not_found_channels_today)):
            write_log(f"  ✗ {ch}")
    else:
        write_log("  (none)")
    
    write_log("\nCHANNELS FOUND - TOMORROW:")
    if found_channels_tomorrow:
        for ch in sorted(set(found_channels_tomorrow)):
            write_log(f"  ✓ {ch}")
    else:
        write_log("  (none)")
    
    write_log("\nCHANNELS NOT FOUND - TOMORROW:")
    if not_found_channels_tomorrow:
        for ch in sorted(set(not_found_channels_tomorrow)):
            write_log(f"  ✗ {ch}")
    else:
        write_log("  (none)")
    
    write_log("="*60)


def main():
    """Main function"""
    
    # Clean old files
    clean_directories()
    ensure_dirs()
    
    # Clear log file and write header
    ist_now = get_ist_now()
    with open(LOG_FILE, "w") as lf:
        lf.write(f"[{ist_now.strftime('%Y-%m-%d %H:%M:%S')}] Scrape started\n")
        lf.write(f"[{ist_now.strftime('%Y-%m-%d %H:%M:%S')}] Timezone: IST (UTC+5:30)\n")
    
    write_log("\n=== STARTING MERGED EPG SCRAPER ===\n")
    
    # ========== DISHTV SCRAPING ==========
    write_log("\n=== DISHTV SOURCE ===")
    
    if not os.path.exists(CHANNELS_FILE):
        write_log(f"ERROR: {CHANNELS_FILE} not found")
        write_final_summary()
        return
    
    channels = parse_channel_file(CHANNELS_FILE)
    if not channels:
        write_log(f"ERROR: No channels found in {CHANNELS_FILE}")
        write_final_summary()
        return
    
    today_ddmmyyyy = datetime.now().strftime("%d/%m/%Y")
    tomorrow_ddmmyyyy = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")
    
    write_log(f"Loaded {len(channels)} channels from {CHANNELS_FILE}")
    write_log(f"Scraping for: {today_ddmmyyyy} and {tomorrow_ddmmyyyy}")
    
    progress["total"] = len(channels) * 2
    progress["done"] = 0
    
    print(f"\n[+] Scraping {len(channels)} DishTV channels (today + tomorrow)...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        
        for ch in channels:
            futures.append(executor.submit(worker_task_dishtv, ch, today_ddmmyyyy, OUT_DIR_TODAY, True))
        
        for ch in channels:
            futures.append(executor.submit(worker_task_dishtv, ch, tomorrow_ddmmyyyy, OUT_DIR_TOMORROW, False))
        
        for f in as_completed(futures):
            pass
    
    # ========== XML EPG SCRAPING ==========
    write_log("\n=== XML EPG SOURCES ===")
    
    epg_sources = [
        {
            'name': 'Jio TV',
            'url': 'https://avkb.short.gy/jioepg.xml.gz',
            'is_gz': True
        },
        {
            'name': 'Tata Play',
            'url': 'https://avkb.short.gy/tsepg.xml.gz',
            'is_gz': True
        }
    ]
    
    print(f"\n[+] Processing XML EPG sources...")
    
    for epg in epg_sources:
        process_xml_epg(epg['name'], epg['url'], epg['is_gz'])
    
    print("\n[+] Done.")
    
    write_final_summary()
    
    write_log("\nScrape completed successfully")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        write_log("\nProcess interrupted by user")
    except Exception as e:
        write_log(f"\nUnexpected error: {str(e)}")
