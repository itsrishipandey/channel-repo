import requests
import gzip
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import json
import os
import re
from io import BytesIO

def sanitize_filename(filename):
    """Sanitize filename for Windows/Linux compatibility"""
    filename = re.sub(r'[<>:"/\\|?*]', '-', filename)
    filename = filename.strip()
    return filename.lower().replace(' ', '-') + '.json'

def load_filter_list(filter_file='filter_list.txt'):
    """Load the list of channel filenames to keep"""
    if not os.path.exists(filter_file):
        print(f"Warning: {filter_file} not found. No channels will be saved.")
        return set()
    
    with open(filter_file, 'r', encoding='utf-8') as f:
        # Read all lines, strip whitespace, and convert to lowercase
        channels = {line.strip().lower() for line in f if line.strip()}
    
    print(f"Loaded {len(channels)} channels from filter list")
    return channels

def parse_xmltv_time(time_str, convert_to_ist=False):
    """Parse XMLTV time format (YYYYMMDDHHmmss +HHMM)"""
    dt_str = time_str.split(' ')[0]
    dt = datetime.strptime(dt_str, '%Y%m%d%H%M%S')
    
    if convert_to_ist:
        dt = dt + timedelta(hours=5, minutes=30)
    
    return dt

def format_time(dt):
    """Format datetime to 12-hour format"""
    return dt.strftime('%I:%M %p').lstrip('0')

def format_date(dt):
    """Format date to 'Month DD, YYYY' format"""
    return dt.strftime('%B %d, %Y')

def download_gz_epg(url):
    """Download and decompress .gz file"""
    print(f"Downloading: {url}")
    
    if 'github.com' in url and '/blob/' in url:
        url = url.replace('github.com', 'raw.githubusercontent.com').replace('/blob/', '/')
    
    response = requests.get(url)
    response.raise_for_status()
    
    with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz:
        xml_content = gz.read()
    
    return xml_content.decode('utf-8')

def parse_epg_xml(xml_content, convert_to_ist=False):
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
        
        start_time = parse_xmltv_time(programme.get('start'), convert_to_ist)
        end_time = parse_xmltv_time(programme.get('stop'), convert_to_ist)
        
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
    """Filter programmes for a specific date"""
    filtered = []
    midnight_dt = datetime.combine(target_date, datetime.min.time())
    
    for prog in programmes:
        start_dt = prog['start_time']
        end_dt = prog['end_time']
        start_date = start_dt.date()
        end_date = end_dt.date()

        if start_date == target_date:
            filtered.append(prog)
        
        elif start_date == target_date - timedelta(days=1) and end_date == target_date:
            if end_dt > midnight_dt:
                adjusted_prog = prog.copy()
                adjusted_prog['start_time'] = midnight_dt
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

def process_epg_sources(filter_list):
    """Process EPG sources and save only filtered channels"""
    print("\nProcessing Indian Channels EPG...")
    
    # Define EPG sources
    epg_sources = [
        {
            'name': 'Jio TV',
            'url': 'https://avkb.short.gy/jioepg.xml.gz',
            'priority': 1  # Higher priority
        },
        {
            'name': 'Tata Play',
            'url': 'https://avkb.short.gy/tsepg.xml.gz',
            'priority': 2  # Lower priority
        }
    ]
    
    # Dictionary to store all channels from all sources
    all_channels = {}
    all_programmes = {}
    
    # Download and parse all EPG sources
    for epg in epg_sources:
        try:
            print(f"\nDownloading {epg['name']}...")
            xml_content = download_gz_epg(epg['url'])
            channels, programmes = parse_epg_xml(xml_content, convert_to_ist=True)
            
            print(f"  Found {len(channels)} channels in {epg['name']}")
            
            # Merge channels and programmes (higher priority overwrites)
            for channel_id, channel_info in channels.items():
                # Only add if not exists OR if this source has higher priority
                if channel_id not in all_channels or epg['priority'] < all_channels[channel_id].get('priority', 999):
                    all_channels[channel_id] = {
                        **channel_info,
                        'priority': epg['priority'],
                        'source': epg['name']
                    }
                    all_programmes[channel_id] = programmes.get(channel_id, [])
            
        except Exception as e:
            print(f"  ✗ Error processing {epg['name']}: {str(e)}")
    
    # Create output directories
    today_dir = 'today'
    tomorrow_dir = 'tomorrow'
    os.makedirs(today_dir, exist_ok=True)
    os.makedirs(tomorrow_dir, exist_ok=True)
    
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)
    
    saved_count = 0
    skipped_count = 0
    
    # Process each channel
    for channel_id, channel_info in all_channels.items():
        filename = sanitize_filename(channel_info['name'])
        
        # Check if this channel is in the filter list
        if filename not in filter_list:
            skipped_count += 1
            continue
        
        if channel_id in all_programmes:
            channel_progs = all_programmes[channel_id]
            
            # Save today's schedule
            today_progs = filter_programmes_by_date(channel_progs, today)
            if today_progs:
                today_schedule = create_json_schedule(
                    channel_info['name'],
                    channel_info['logo'],
                    today_progs,
                    today
                )
                filepath = os.path.join(today_dir, filename)
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(today_schedule, f, indent=2, ensure_ascii=False)
            
            # Save tomorrow's schedule
            tomorrow_progs = filter_programmes_by_date(channel_progs, tomorrow)
            if tomorrow_progs:
                tomorrow_schedule = create_json_schedule(
                    channel_info['name'],
                    channel_info['logo'],
                    tomorrow_progs,
                    tomorrow
                )
                filepath = os.path.join(tomorrow_dir, filename)
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(tomorrow_schedule, f, indent=2, ensure_ascii=False)
            
            saved_count += 1
            print(f"  ✓ Saved: {filename}")
    
    print(f"\n✓ Processed complete!")
    print(f"  - Saved {saved_count} channels from filter list")
    print(f"  - Skipped {skipped_count} channels not in filter list")
    print(f"  - Today's schedules saved in: {today_dir}/")
    print(f"  - Tomorrow's schedules saved in: {tomorrow_dir}/")

def main():
    """Main function"""
    print("=" * 60)
    print("EPG to JSON Converter (Filtered)")
    print("=" * 60)
    
    # Load filter list
    filter_list = load_filter_list('filter_list.txt')
    
    if not filter_list:
        print("\nNo channels in filter list. Exiting.")
        return
    
    # Process EPG sources with filtering
    process_epg_sources(filter_list)
    
    print("\n" + "=" * 60)
    print("Processing complete!")
    print("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user.")
    except Exception as e:
        print(f"\n\nUnexpected error: {str(e)}")
