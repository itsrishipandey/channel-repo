#!/usr/bin/env python3
"""
Merged Scraper: DishTV + XMLTV (JioTV/TataPlay)
Features:
 - Cleans old files on run.
 - Scrapes DishTV based on channel.txt.
 - Scrapes XMLTV based on filter_list.txt (Priority: JioTV > TataPlay).
 - Saves everything to ./today/ and ./tomorrow/
 - Single log file: scrape_log.log
 - IST Timezone for calculations.
"""

import requests
import json
import os
import sys
import time
import shutil
import threading
import gzip
import re
import xml.etree.ElementTree as ET
from io import BytesIO
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- CONFIG ----------

# IST Timezone (UTC + 5:30)
IST_OFFSET = timezone(timedelta(hours=5, minutes=30))

# Files
CHANNELS_FILE = "channel.txt"       # For DishTV
FILTER_LIST_FILE = "filter_list.txt" # For XMLTV (Jio/Tata)
LOG_FILE = "scrape_log.log"

# Directories
OUT_DIR_TODAY = "today"
OUT_DIR_TOMORROW = "tomorrow"

# DishTV URLs
DISH_SIGNIN_URL = "https://www.dishtv.in/services/epg/signin"
DISH_PROGRAMS_URL = "https://epg.mysmartstick.com/dishtv/api/v1/epg/entities/programs"
DISH_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Origin": "https://www.dishtv.in",
    "Referer": "https://www.dishtv.in/",
}

# XMLTV Sources (Priority order handled in logic)
XML_SOURCES = [
    {
        'name': 'Tata Play',
        'url': 'https://avkb.short.gy/tsepg.xml.gz',
        'is_gz': True
    },
    {
        'name': 'Jio TV',
        'url': 'https://avkb.short.gy/jioepg.xml.gz',
        'is_gz': True
    }
]

# Threading
MAX_WORKERS = 30
MAX_RETRIES = 3
RETRY_BACKOFF = 1.2

lock = threading.Lock()

# ---------- UTILS ----------

def get_ist_now():
    """Returns current time in IST."""
    return datetime.now(IST_OFFSET)

def write_log(line: str):
    """Append a line to the log file (thread-safe)."""
    ts = get_ist_now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{ts}] {line}\n"
    with lock:
        with open(LOG_FILE, "a", encoding="utf-8") as lf:
            lf.write(entry)
            print(line) # Also print to console for Action logs

def ensure_clean_dirs():
    """Deletes old directories and recreates them."""
    for folder in [OUT_DIR_TODAY, OUT_DIR_TOMORROW]:
        if os.path.exists(folder):
            shutil.rmtree(folder)
        os.makedirs(folder, exist_ok=True)
    
    # Overwrite log file
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"Scrape started at {get_ist_now()}\n")

def sanitize_filename(filename):
    """Sanitize filename to match user requirement (lowercase, hyphens)."""
    filename = re.sub(r'[<>:"/\\|?*]', '-', filename)
    filename = filename.strip()
    slug = filename.lower().replace(' ', '-')
    if not slug.endswith('.json'):
        slug += '.json'
    return slug

# ---------- PART 1: DISH TV SCRAPER ----------

def dishtv_get_token(session: requests.Session):
    resp = session.post(DISH_SIGNIN_URL, headers=DISH_HEADERS, json={}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if "token" not in data:
        raise RuntimeError(f"signin response missing token: {data}")
    return data["token"]

def dishtv_fetch_epg(session: requests.Session, channelid: str, date_ddmmyyyy: str):
    payload = {
        "channelid": channelid,
        "date": date_ddmmyyyy,
        "allowPastEvents": True
    }
    resp = session.post(DISH_PROGRAMS_URL, headers=DISH_HEADERS, json=payload, timeout=20)
    resp.raise_for_status()
    return resp.json()

def dishtv_format_output(epg_data):
    programs = epg_data if isinstance(epg_data, list) else epg_data.get("programs", [])
    channel_name = programs[0].get("channelname", "Unknown") if programs else "Unknown"

    if programs:
        # Parse time assuming UTC from API, convert to display format
        dt = datetime.fromisoformat(programs[0]["start"].replace("Z", "+00:00"))
        # Convert to IST for display date
        dt_ist = dt.astimezone(IST_OFFSET)
        date_str = dt_ist.strftime("%B %d, %Y")
    else:
        date_str = get_ist_now().strftime("%B %d, %Y")

    schedule = []
    for p in programs:
        # DishTV times are UTC iso format
        start_dt = datetime.fromisoformat(p["start"].replace("Z", "+00:00")).astimezone(IST_OFFSET)
        end_dt = datetime.fromisoformat(p["stop"].replace("Z", "+00:00")).astimezone(IST_OFFSET)
        
        schedule.append({
            "show_name": p.get("title", ""),
            "start_time": start_dt.strftime("%I:%M %p").lstrip('0'),
            "end_time": end_dt.strftime("%I:%M %p").lstrip('0'),
            "show_logo": p.get("programmeurl", "")
        })

    return {
        "channel_name": channel_name,
        "date": date_str,
        "schedule": schedule
    }

def dishtv_worker(channel_tuple, date_obj, out_dir):
    channelid, txt_name = channel_tuple
    date_str = date_obj.strftime("%d/%m/%Y")
    last_err = None

    # Determine filename based on txt_name (Script 1 requirement)
    filename = sanitize_filename(txt_name if txt_name else "channel")
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session = requests.Session()
            session.headers.update(DISH_HEADERS)
            token = dishtv_get_token(session)
            session.headers.update({"Authorization": token})
            
            epg = dishtv_fetch_epg(session, channelid, date_str)
            formatted = dishtv_format_output(epg)
            
            path = os.path.join(out_dir, filename)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(formatted, f, indent=2, ensure_ascii=False)
            
            write_log(f"DISH SUCCESS: {channelid} -> {filename} ({date_str})")
            return
            
        except Exception as e:
            last_err = e
            time.sleep(RETRY_BACKOFF ** attempt)

    write_log(f"DISH FAIL: {channelid} ({date_str}) Error: {last_err}")

def run_dishtv_logic():
    write_log("--- Starting DishTV Scraper ---")
    if not os.path.exists(CHANNELS_FILE):
        write_log(f"Skipping DishTV: {CHANNELS_FILE} not found.")
        return

    channels = []
    with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"): continue
            if "=" in line:
                parts = line.split("=", 1)
                channels.append((parts[0].strip(), parts[1].strip()))
            else:
                channels.append((line.strip(), ""))

    now_ist = get_ist_now()
    today_dt = now_ist
    tomorrow_dt = now_ist + timedelta(days=1)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for ch in channels:
            futures.append(executor.submit(dishtv_worker, ch, today_dt, OUT_DIR_TODAY))
            futures.append(executor.submit(dishtv_worker, ch, tomorrow_dt, OUT_DIR_TOMORROW))
        
        for _ in as_completed(futures):
            pass

# ---------- PART 2: XMLTV SCRAPER ----------

def parse_xmltv_time(time_str):
    """Parse XMLTV time (YYYYMMDDHHmmss +HHMM) to IST datetime object"""
    # Time string example: 20251209133000 +0530
    dt_str = time_str.split(' ')[0]
    # Parse as naive first
    dt = datetime.strptime(dt_str, '%Y%m%d%H%M%S')
    # Assume the XML time is in UTC or source time, request asks to calc in Indian Time.
    # The source URLs provided (Tata/Jio) usually provide offsets. 
    # To be safe and consistent with Script 2 logic, we add 5:30 to naive time implies input was UTC.
    dt = dt.replace(tzinfo=timezone.utc) 
    return dt.astimezone(IST_OFFSET)

def xmltv_download(url, is_gz):
    write_log(f"Downloading XMLTV: {url}")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    if is_gz:
        with gzip.GzipFile(fileobj=BytesIO(resp.content)) as gz:
            return gz.read().decode('utf-8')
    return resp.text

def xmltv_parse_and_store(xml_content, store_dict, source_name):
    """Parses XML and updates the store_dict (keyed by filename)"""
    root = ET.fromstring(xml_content)
    
    # Map channel id -> metadata
    channel_meta = {}
    for channel in root.findall('channel'):
        cid = channel.get('id')
        dname = channel.find('display-name').text if channel.find('display-name') is not None else cid
        icon = channel.find('icon')
        logo = icon.get('src') if icon is not None else ""
        channel_meta[cid] = {'name': dname, 'logo': logo}

    # Group programs by channel_id
    programs_map = {}
    for prog in root.findall('programme'):
        cid = prog.get('channel')
        if cid not in programs_map: programs_map[cid] = []
        
        start = parse_xmltv_time(prog.get('start'))
        end = parse_xmltv_time(prog.get('stop'))
        
        title = prog.find('title')
        show_name = title.text if title is not None else "Unknown"
        
        icon = prog.find('icon')
        show_logo = icon.get('src') if icon is not None else ""
        
        programs_map[cid].append({
            'show_name': show_name,
            'start_time': start,
            'end_time': end,
            'show_logo': show_logo
        })

    # Process into daily schedules
    count = 0
    for cid, meta in channel_meta.items():
        # Determine the target filename
        target_filename = sanitize_filename(meta['name'])
        
        if cid in programs_map:
            # Save to store_dict. 
            # If entry exists, it will be overwritten (This handles Priority logic)
            # We store raw program data here, split by dates later
            store_dict[target_filename] = {
                'meta': meta,
                'programs': programs_map[cid],
                'source': source_name
            }
            count += 1
    write_log(f"Parsed {count} channels from {source_name}")

def filter_and_save_xmltv_data(store_dict):
    if not os.path.exists(FILTER_LIST_FILE):
        write_log(f"Skipping XMLTV Save: {FILTER_LIST_FILE} not found.")
        return

    # Load filter list
    valid_filenames = set()
    with open(FILTER_LIST_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip().lower()
            if line:
                if not line.endswith('.json'): line += '.json'
                valid_filenames.add(line)

    write_log(f"Filtering {len(store_dict)} scraped channels against {len(valid_filenames)} allowed files.")

    today_date = get_ist_now().date()
    tomorrow_date = today_date + timedelta(days=1)

    saved_count = 0
    
    for filename, data in store_dict.items():
        if filename not in valid_filenames:
            continue

        meta = data['meta']
        all_progs = data['programs']
        
        # We need to generate files for today and tomorrow
        for target_date, out_dir in [(today_date, OUT_DIR_TODAY), (tomorrow_date, OUT_DIR_TOMORROW)]:
            
            daily_schedule = []
            
            # Filter logic from Script 2 (includes late night spillover)
            midnight_dt = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=IST_OFFSET)
            
            for p in all_progs:
                p_start = p['start_time']
                p_end = p['end_time']
                
                # Logic: Starts on date OR (Starts prev day AND Ends on date AND Ends after midnight)
                if p_start.date() == target_date:
                    daily_schedule.append(p)
                elif p_start.date() == (target_date - timedelta(days=1)) and p_end.date() == target_date:
                    if p_end > midnight_dt:
                        # Adjust start to midnight for display
                        p_adj = p.copy()
                        p_adj['start_time'] = midnight_dt
                        daily_schedule.append(p_adj)

            daily_schedule.sort(key=lambda x: x['start_time'])

            if daily_schedule:
                # Format for JSON
                final_json = {
                    "channel_name": meta['name'],
                    "date": target_date.strftime('%B %d, %Y'),
                    "schedule": []
                }
                
                for p in daily_schedule:
                    final_json['schedule'].append({
                        "show_name": p['show_name'],
                        "start_time": p['start_time'].strftime('%I:%M %p').lstrip('0'),
                        "end_time": p['end_time'].strftime('%I:%M %p').lstrip('0'),
                        "show_logo": p['show_logo']
                    })

                # Write file
                save_path = os.path.join(out_dir, filename)
                with open(save_path, 'w', encoding='utf-8') as f:
                    json.dump(final_json, f, indent=2, ensure_ascii=False)
                
                saved_count += 0.5 # counts as half per day for simple tracking

    write_log(f"XMLTV Save Complete. {int(saved_count)} channels saved successfully from {len(valid_filenames)} requested.")

def run_xmltv_logic():
    write_log("--- Starting XMLTV Scraper ---")
    
    # Store combined data here. Key = filename.
    # We process sources in order. Later sources overwrite earlier ones.
    # Order in XML_SOURCES: Tata Play first, then Jio TV.
    # This automatically satisfies "Priority to JioTV over TataPlay"
    combined_data = {}
    
    for source in XML_SOURCES:
        try:
            content = xmltv_download(source['url'], source['is_gz'])
            xmltv_parse_and_store(content, combined_data, source['name'])
        except Exception as e:
            write_log(f"Error processing {source['name']}: {e}")

    filter_and_save_xmltv_data(combined_data)

# ---------- MAIN ----------

def main():
    ensure_clean_dirs()
    
    # Run Scrapers
    run_dishtv_logic()
    run_xmltv_logic()
    
    write_log("--- All Scraping Done ---")

if __name__ == "__main__":
    main()
