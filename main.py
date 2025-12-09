#!/usr/bin/env python3
"""
Merged Scraper: DishTV + XMLTV (JioTV/TataPlay)
Updates:
 - Fetches full 24-hour schedule (00:00 - 23:59).
 - Detailed Summary Logging (Found/Not Found lists).
 - IST Timezone logic.
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
from datetime import datetime, timedelta, timezone, time as dt_time
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

# XMLTV Sources (Priority: Tata first, overwritten by Jio)
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

# Reporting Containers
report = {
    "dishtv": {
        "today_found": [],
        "today_missing": [],
        "tomorrow_found": [],
        "tomorrow_missing": []
    },
    "xmltv": {
        "today_found": [],
        "today_missing": [],
        "tomorrow_found": [],
        "tomorrow_missing": []
    }
}

# ---------- UTILS ----------

def get_ist_now():
    """Returns current time in IST."""
    return datetime.now(IST_OFFSET)

def write_log(line: str, to_console=True):
    """Append a line to the log file (thread-safe)."""
    ts = get_ist_now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{ts}] {line}\n"
    with lock:
        with open(LOG_FILE, "a", encoding="utf-8") as lf:
            lf.write(entry)
    if to_console:
        print(line)

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
    """Sanitize filename: lowercase, hyphens, ensure .json"""
    filename = re.sub(r'[<>:"/\\|?*]', '-', filename)
    filename = filename.strip()
    slug = filename.lower().replace(' ', '-')
    if not slug.endswith('.json'):
        slug += '.json'
    return slug

def write_summary_log():
    """Writes the final Found/Not Found summary to log."""
    summary = "\n" + "="*50 + "\n"
    summary += "FINAL EXECUTION SUMMARY\n"
    summary += "="*50 + "\n\n"

    # DishTV Section
    summary += "--- DISH TV REPORT ---\n"
    summary += f"Today Found: {len(report['dishtv']['today_found'])}, Missing: {len(report['dishtv']['today_missing'])}\n"
    summary += f"Tomorrow Found: {len(report['dishtv']['tomorrow_found'])}, Missing: {len(report['dishtv']['tomorrow_missing'])}\n"
    
    if report['dishtv']['today_missing']:
        summary += "\n[MISSING TODAY - DISH TV]:\n" + "\n".join(report['dishtv']['today_missing']) + "\n"
    if report['dishtv']['tomorrow_missing']:
        summary += "\n[MISSING TOMORROW - DISH TV]:\n" + "\n".join(report['dishtv']['tomorrow_missing']) + "\n"

    # XMLTV Section
    summary += "\n--- XMLTV REPORT ---\n"
    summary += f"Today Found: {len(report['xmltv']['today_found'])}, Missing: {len(report['xmltv']['today_missing'])}\n"
    summary += f"Tomorrow Found: {len(report['xmltv']['tomorrow_found'])}, Missing: {len(report['xmltv']['tomorrow_missing'])}\n"

    if report['xmltv']['today_missing']:
        summary += "\n[MISSING TODAY - XMLTV]:\n" + "\n".join(report['xmltv']['today_missing']) + "\n"
    if report['xmltv']['tomorrow_missing']:
        summary += "\n[MISSING TOMORROW - XMLTV]:\n" + "\n".join(report['xmltv']['tomorrow_missing']) + "\n"

    summary += "\n" + "="*50 + "\n"
    
    with lock:
        with open(LOG_FILE, "a", encoding="utf-8") as lf:
            lf.write(summary)
    print("Summary written to log file.")

# ---------- PART 1: DISH TV SCRAPER ----------

def dishtv_get_token(session: requests.Session):
    resp = session.post(DISH_SIGNIN_URL, headers=DISH_HEADERS, json={}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if "token" not in data:
        raise RuntimeError(f"signin response missing token: {data}")
    return data["token"]

def dishtv_fetch_epg(session: requests.Session, channelid: str, date_ddmmyyyy: str):
    # 'allowPastEvents' must be true to get data from 00:00 if request time is later
    payload = {
        "channelid": channelid,
        "date": date_ddmmyyyy,
        "allowPastEvents": "true" 
    }
    resp = session.post(DISH_PROGRAMS_URL, headers=DISH_HEADERS, json=payload, timeout=20)
    resp.raise_for_status()
    return resp.json()

def dishtv_format_output(epg_data, target_date_obj):
    programs = epg_data if isinstance(epg_data, list) else epg_data.get("programs", [])
    channel_name = programs[0].get("channelname", "Unknown") if programs else "Unknown"

    date_str = target_date_obj.strftime("%B %d, %Y")

    schedule = []
    for p in programs:
        # DishTV times are UTC iso format
        start_dt = datetime.fromisoformat(p["start"].replace("Z", "+00:00")).astimezone(IST_OFFSET)
        end_dt = datetime.fromisoformat(p["stop"].replace("Z", "+00:00")).astimezone(IST_OFFSET)
        
        # Note: DishTV API usually returns exactly the requested day's blocks if properly queried.
        # We append all returned programs.
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

def dishtv_worker(channel_tuple, date_obj, out_dir, is_tomorrow):
    channelid, txt_name = channel_tuple
    date_str = date_obj.strftime("%d/%m/%Y")
    
    filename = sanitize_filename(txt_name if txt_name else f"dish-{channelid}")
    report_key_found = "tomorrow_found" if is_tomorrow else "today_found"
    report_key_miss = "tomorrow_missing" if is_tomorrow else "today_missing"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session = requests.Session()
            session.headers.update(DISH_HEADERS)
            token = dishtv_get_token(session)
            session.headers.update({"Authorization": token})
            
            epg = dishtv_fetch_epg(session, channelid, date_str)
            
            # Check if empty
            raw_progs = epg if isinstance(epg, list) else epg.get("programs", [])
            if not raw_progs:
                raise ValueError("Empty program list")

            formatted = dishtv_format_output(epg, date_obj)
            
            path = os.path.join(out_dir, filename)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(formatted, f, indent=2, ensure_ascii=False)
            
            # Success Logging
            write_log(f"DISH OK: {channelid} -> {filename} [{date_str}]", to_console=False)
            with lock:
                report["dishtv"][report_key_found].append(f"{txt_name} ({channelid})")
            return
            
        except Exception as e:
            if attempt == MAX_RETRIES:
                write_log(f"DISH FAIL: {channelid} [{date_str}] - {e}")
                with lock:
                    report["dishtv"][report_key_miss].append(f"{txt_name} ({channelid})")
            time.sleep(RETRY_BACKOFF ** attempt)

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
            # Submit Today
            futures.append(executor.submit(dishtv_worker, ch, today_dt, OUT_DIR_TODAY, False))
            # Submit Tomorrow
            futures.append(executor.submit(dishtv_worker, ch, tomorrow_dt, OUT_DIR_TOMORROW, True))
        
        for _ in as_completed(futures):
            pass

# ---------- PART 2: XMLTV SCRAPER ----------

def parse_xmltv_time(time_str):
    """Parse XMLTV time (YYYYMMDDHHmmss +HHMM) to IST datetime object"""
    dt_str = time_str.split(' ')[0]
    dt = datetime.strptime(dt_str, '%Y%m%d%H%M%S')
    # Treat input as UTC, convert to IST
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
    
    channel_meta = {}
    for channel in root.findall('channel'):
        cid = channel.get('id')
        dname = channel.find('display-name').text if channel.find('display-name') is not None else cid
        icon = channel.find('icon')
        logo = icon.get('src') if icon is not None else ""
        channel_meta[cid] = {'name': dname, 'logo': logo}

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

    count = 0
    for cid, meta in channel_meta.items():
        target_filename = sanitize_filename(meta['name'])
        
        if cid in programs_map:
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
    required_files = []
    with open(FILTER_LIST_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip().lower()
            if line:
                if not line.endswith('.json'): line += '.json'
                required_files.append(line)

    write_log(f"Filtering {len(store_dict)} scraped channels against {len(required_files)} requested files.")

    today_date = get_ist_now().date()
    tomorrow_date = today_date + timedelta(days=1)

    for req_filename in required_files:
        
        if req_filename not in store_dict:
            # Log as missing for both days since the channel wasn't found in XML at all
            report["xmltv"]["today_missing"].append(req_filename)
            report["xmltv"]["tomorrow_missing"].append(req_filename)
            continue

        data = store_dict[req_filename]
        meta = data['meta']
        all_progs = data['programs']
        
        # Process Today and Tomorrow
        dates_to_process = [
            (today_date, OUT_DIR_TODAY, "today"),
            (tomorrow_date, OUT_DIR_TOMORROW, "tomorrow")
        ]

        for target_date, out_dir, day_key in dates_to_process:
            
            # 00:00:00 to 23:59:59 IST bounds
            day_start = datetime.combine(target_date, dt_time.min).replace(tzinfo=IST_OFFSET)
            day_end = datetime.combine(target_date, dt_time.max).replace(tzinfo=IST_OFFSET)
            
            daily_schedule = []
            
            for p in all_progs:
                p_start = p['start_time']
                p_end = p['end_time']
                
                # Logic: Check for overlap with the target day
                # (Start < End of Day) AND (End > Start of Day)
                if p_start < day_end and p_end > day_start:
                    
                    # Adjust display times to clamp to the 24h window?
                    # User asked for "Whole 24 hrs schedule", usually this means listing the shows
                    # that overlap this period.
                    
                    # Note: We do NOT clamp the actual data objects (start/end), 
                    # we just include the show in the list.
                    
                    # However, if a show started yesterday 11PM and ends Today 1AM, 
                    # it is the first show of today.
                    
                    daily_schedule.append(p)

            daily_schedule.sort(key=lambda x: x['start_time'])

            if daily_schedule:
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

                save_path = os.path.join(out_dir, req_filename)
                with open(save_path, 'w', encoding='utf-8') as f:
                    json.dump(final_json, f, indent=2, ensure_ascii=False)
                
                report["xmltv"][f"{day_key}_found"].append(req_filename)
            else:
                report["xmltv"][f"{day_key}_missing"].append(req_filename)

def run_xmltv_logic():
    write_log("--- Starting XMLTV Scraper ---")
    combined_data = {}
    
    # Process Tata first, then Jio (Jio overwrites/prioritized)
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
    
    run_dishtv_logic()
    run_xmltv_logic()
    
    write_summary_log()
    print("Done.")

if __name__ == "__main__":
    main()
