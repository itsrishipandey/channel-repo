#!/usr/bin/env python3
"""
download_images.py (GitHub Actions compatible)

Downloads and compresses images from EPG JSON files.
- Reads from today/ and tomorrow/ directories
- Downloads and resizes images to width 250px
- Converts to WebP format
- Saves to downloaded-images/ directory
- Logs failed downloads to image_download_log.log
- Replaces old images with new ones on each run
"""

import os
import sys
import json
import hashlib
import logging
import shutil
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, ParseResult, unquote
from io import BytesIO

import requests
from requests.adapters import HTTPAdapter, Retry
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pytz
from datetime import datetime

# ----- CONFIG -----
FOLDERS = [
    "./today",
    "./tomorrow",
]

OUTPUT_BASE = Path("./downloaded-images")

WP_PREFIX = "https://intvschedule.com/wp-content/uploads/downloaded-images"

TARGET_WIDTH = 250
MAX_WORKERS = 30
REQUEST_TIMEOUT = 20
RETRIES = 3
BACKOFF_FACTOR = 0.5

# Log file (fixed name, overwritten each run)
LOG_FILE = "image_download_log.log"

# Indian timezone
IST = pytz.timezone('Asia/Kolkata')

# ----- LOGGING SETUP -----
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')
    ]
)
logger = logging.getLogger("img-downloader")


def get_ist_time():
    """Get current time in IST"""
    return datetime.now(IST)


def log_with_ist(message, level='info'):
    """Log message with IST timestamp"""
    ts = get_ist_time().strftime("%Y-%m-%d %H:%M:%S IST")
    formatted = f"[{ts}] {message}"
    
    if level == 'info':
        logger.info(message)
    elif level == 'warning':
        logger.warning(message)
    elif level == 'error':
        logger.error(message)


def make_session():
    """Create requests session with retry logic"""
    session = requests.Session()
    retries = Retry(
        total=RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = make_session()
SESSION.headers.update({"User-Agent": "dishtv-image-downloader/2.0"})


def clean_image_directory():
    """Delete all old images before downloading new ones"""
    if OUTPUT_BASE.exists():
        logger.info(f"Cleaning old images from {OUTPUT_BASE}")
        shutil.rmtree(OUTPUT_BASE)
        logger.info(f"Deleted directory: {OUTPUT_BASE}")
    
    OUTPUT_BASE.mkdir(parents=True, exist_ok=True)
    logger.info(f"Created fresh directory: {OUTPUT_BASE}")


def slug_from_filename(json_path: Path) -> str:
    """Generate slug from JSON filename"""
    return json_path.stem.lower().replace(" ", "-")


def parse_and_adjust_size(url: str, target_width: int) -> str:
    """If URL has lock=W×H, adjust it. If not, leave unchanged."""
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=True)

        # If no lock param, leave URL unchanged
        if "lock" not in qs:
            return url

        lock_vals = qs["lock"]
        lock = lock_vals[0]

        if "x" in lock:
            w, h = lock.split("x")
            try:
                w = int(w)
                h = int(h)
                new_h = max(1, round(h * (target_width / w)))
                qs["lock"] = [f"{target_width}x{new_h}"]
            except:
                qs["lock"] = [f"{target_width}x{target_width}"]

        new_query = urlencode(qs, doseq=True)

        new_parsed = ParseResult(
            scheme=parsed.scheme,
            netloc=parsed.netloc,
            path=parsed.path,
            params=parsed.params,
            query=new_query,
            fragment=parsed.fragment
        )

        return urlunparse(new_parsed)

    except:
        return url


def url_basename(url: str) -> str:
    """Extract basename from URL"""
    p = urlparse(url)
    name = os.path.basename(unquote(p.path))
    if not name:
        name = hashlib.md5(url.encode("utf-8")).hexdigest() + ".img"
    return name


def unique_filename_for(url: str, basename: str) -> str:
    """Generate unique filename with hash"""
    base, _ = os.path.splitext(basename)
    h = hashlib.md5(url.encode("utf-8")).hexdigest()[:10]
    return f"{base}_{h}.webp".replace(" ", "_")


def ensure_dir(p: Path):
    """Ensure directory exists"""
    if not p.exists():
        p.mkdir(parents=True, exist_ok=True)


def download_and_convert_to_webp(norm_url: str, save_path: Path, session: requests.Session) -> bool:
    """Download image, resize if needed, convert to WebP"""
    tmp = save_path.with_suffix(".part")

    try:
        resp = session.get(norm_url, stream=True, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()

        content = BytesIO()
        for chunk in resp.iter_content(8192):
            if chunk:
                content.write(chunk)
        content.seek(0)

        try:
            img = Image.open(content)

            # Resize if URL has NO lock param
            if "lock=" not in norm_url:
                w, h = img.size
                new_w = TARGET_WIDTH
                new_h = int((h / w) * new_w)
                img = img.resize((new_w, new_h), Image.LANCZOS)

            if img.mode in ("RGBA", "P"):
                img = img.convert("RGBA")
            else:
                img = img.convert("RGB")

            ensure_dir(save_path.parent)
            img.save(tmp, "WEBP", quality=80, method=6)

            tmp.replace(save_path)
            return True

        except Exception as e:
            logger.warning(f"Image convert failed {norm_url}: {e}")
            tmp.unlink(missing_ok=True)
            return False

    except Exception as e:
        logger.warning(f"Download failed {norm_url}: {e}")
        tmp.unlink(missing_ok=True)
        return False


def process_json_file(json_path: Path, session: requests.Session, failed_downloads: list):
    """Process single JSON file - download images and update URLs"""
    
    per_file_downloaded = {}  # dedupe inside file only

    try:
        data = json.load(open(json_path, "r", encoding="utf-8"))
    except Exception as e:
        logger.error(f"Cannot read JSON {json_path}: {e}")
        return

    slug = slug_from_filename(json_path)
    day = "today" if "today" in str(json_path).lower() else "tomorrow"

    schedule = data.get("schedule", [])
    if not isinstance(schedule, list):
        return

    to_update = []
    for idx, item in enumerate(schedule):
        if isinstance(item, dict) and item.get("show_logo"):
            to_update.append((idx, item["show_logo"]))

    if not to_update:
        return

    norm_entries = {}
    orig_to_norm = {}

    for idx, url in to_update:
        norm = parse_and_adjust_size(url, TARGET_WIDTH)
        orig_to_norm[url] = norm
        norm_entries.setdefault(norm, []).append(idx)

    tasks = []
    for norm_url in norm_entries:
        basename = url_basename(norm_url)
        filename = unique_filename_for(norm_url, basename)

        local_dir = OUTPUT_BASE / slug / day
        ensure_dir(local_dir)
        local_path = local_dir / filename

        if norm_url in per_file_downloaded:
            continue

        if local_path.exists():
            per_file_downloaded[norm_url] = str(local_path)
            continue

        tasks.append((norm_url, local_path))

    if tasks:
        logger.info(f"{json_path.name}: downloading {len(tasks)} images...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {
                ex.submit(download_and_convert_to_webp, url, path, session): (url, path)
                for url, path in tasks
            }

            for fut in tqdm(as_completed(futures), total=len(futures), unit="img", desc=f"{json_path.name}"):
                pass

        for norm_url, local_path in tasks:
            if local_path.exists():
                per_file_downloaded[norm_url] = str(local_path)
            else:
                logger.warning(f"Missing after download: {norm_url}")
                failed_downloads.append({
                    'file': str(json_path),
                    'url': norm_url,
                    'reason': 'File missing after download'
                })

    # Update JSON with new URLs
    for idx, orig_url in to_update:
        norm_url = orig_to_norm[orig_url]
        local = per_file_downloaded.get(norm_url)

        if local:
            filename = os.path.basename(local)
            new_val = f"{WP_PREFIX}/{slug}/{day}/{filename}"
            data["schedule"][idx]["show_logo"] = new_val
        else:
            logger.warning(f"Failed image for {orig_url}")
            failed_downloads.append({
                'file': str(json_path),
                'url': orig_url,
                'reason': 'Download failed'
            })

    # Save updated JSON
    json.dump(data, open(json_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    logger.info(f"Updated JSON: {json_path}")


def gather_json_files(folders):
    """Gather all JSON files from specified folders"""
    files = []
    for folder in folders:
        p = Path(folder)
        if not p.exists():
            logger.warning(f"Folder not found: {folder}")
            continue
        json_files = list(p.glob("*.json"))
        files.extend(json_files)
        logger.info(f"Found {len(json_files)} JSON files in {folder}")
    return files


def write_summary(failed_downloads: list, total_files: int, start_time):
    """Write summary of image download process"""
    end_time = get_ist_time()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("\n" + "=" * 70)
    logger.info("IMAGE DOWNLOAD SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total JSON files processed: {total_files}")
    logger.info(f"Total failed downloads: {len(failed_downloads)}")
    logger.info(f"Duration: {duration:.2f} seconds")
    
    if failed_downloads:
        logger.info("\nFAILED DOWNLOADS:")
        for item in failed_downloads:
            logger.error(f"  File: {item['file']}")
            logger.error(f"  URL: {item['url']}")
            logger.error(f"  Reason: {item['reason']}")
            logger.error("")
    else:
        logger.info("\n✓ All images downloaded successfully!")
    
    logger.info("=" * 70)


def main():
    """Main function"""
    start_time = get_ist_time()
    
    logger.info("=" * 70)
    logger.info(f"Image Download started at {start_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
    logger.info("=" * 70)
    
    # Clean old images
    clean_image_directory()
    
    # Gather all JSON files
    files = gather_json_files(FOLDERS)
    logger.info(f"\nProcessing {len(files)} JSON files total...")
    
    if not files:
        logger.warning("No JSON files found to process!")
        return
    
    # Track failed downloads
    failed_downloads = []
    
    # Process each JSON file
    for file in files:
        process_json_file(file, SESSION, failed_downloads)
    
    # Write summary
    write_summary(failed_downloads, len(files), start_time)
    
    logger.info(f"\nCompleted at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S IST')}")
    logger.info("All done.")


if __name__ == "__main__":
    main()
