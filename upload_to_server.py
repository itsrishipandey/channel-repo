#!/usr/bin/env python3
"""
upload_to_server.py (GitHub Actions compatible)

Uploads EPG JSON files and downloaded images to WordPress server via SSH/SFTP.
- Zips images and JSON folders
- Uploads via SFTP
- Extracts on server
- Runs WP-CLI import
- Cleans up temporary files
"""

import os
import sys
import paramiko
import zipfile
from datetime import datetime
from pathlib import Path
import pytz

# -----------------------------
# CONFIG SECTION
# -----------------------------

# Local paths (GitHub repo structure)
LOCAL_IMAGES_FOLDER = "./downloaded-images"
LOCAL_JSON_TODAY = "./today"
LOCAL_JSON_TOMORROW = "./tomorrow"

# Temporary zip file paths
ZIP_IMAGES_PATH = "./temp-images.zip"
ZIP_TODAY_PATH = "./temp-today.zip"
ZIP_TOMORROW_PATH = "./temp-tomorrow.zip"

# Remote server paths
REMOTE_IMAGES_DIR = "/usr/local/lsws/intvschedule.com/html/wp-content/uploads/downloaded-images/"
REMOTE_JSON_ODD_DIR = "/usr/local/lsws/intvschedule.com/html/wp-content/themes/generatepress_child/schedules/odd/"
REMOTE_JSON_EVEN_DIR = "/usr/local/lsws/intvschedule.com/html/wp-content/themes/generatepress_child/schedules/even/"

# SSH credentials (from environment variables)
SSH_HOST = os.getenv('SSH_HOST')
SSH_PORT = int(os.getenv('SSH_PORT', '22'))
SSH_USER = os.getenv('SSH_USER')
SSH_PASSWORD = os.getenv('SSH_PASSWORD')

# Indian timezone
IST = pytz.timezone('Asia/Kolkata')

# -----------------------------
# LOGGING FUNCTION
# -----------------------------
def log(message):
    """Log message with IST timestamp"""
    timestamp = datetime.now(IST).strftime("[%Y-%m-%d %H:%M:%S IST]")
    print(f"{timestamp} {message}")
    sys.stdout.flush()

# -----------------------------
# VALIDATION
# -----------------------------
def validate_credentials():
    """Validate that SSH credentials are provided"""
    missing = []
    if not SSH_HOST:
        missing.append('SSH_HOST')
    if not SSH_USER:
        missing.append('SSH_USER')
    if not SSH_PASSWORD:
        missing.append('SSH_PASSWORD')
    
    if missing:
        log(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        log("Please set them in GitHub Secrets:")
        for var in missing:
            log(f"  - {var}")
        sys.exit(1)
    
    log(f"SSH Configuration validated: {SSH_USER}@{SSH_HOST}:{SSH_PORT}")

# -----------------------------
# ZIP UTILITY
# -----------------------------
def zip_directory(folder_path, output_zip):
    """Create ZIP archive of directory"""
    if not os.path.exists(folder_path):
        log(f"WARNING: Folder not found: {folder_path}")
        return False
    
    log(f"Creating ZIP: {folder_path} -> {output_zip}")
    
    file_count = 0
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                local_file = os.path.join(root, file)
                # Preserve directory structure in zip
                arcname = os.path.relpath(local_file, folder_path)
                zipf.write(local_file, arcname)
                file_count += 1
    
    zip_size = os.path.getsize(output_zip) / (1024 * 1024)  # MB
    log(f"ZIP created: {output_zip} ({file_count} files, {zip_size:.2f} MB)")
    return True

# -----------------------------
# CLEANUP FUNCTION
# -----------------------------
def cleanup_local_zips():
    """Delete temporary local ZIP files"""
    zip_files = [ZIP_IMAGES_PATH, ZIP_TODAY_PATH, ZIP_TOMORROW_PATH]
    for zip_file in zip_files:
        if os.path.exists(zip_file):
            os.remove(zip_file)
            log(f"Deleted local ZIP: {zip_file}")

# -----------------------------
# MAIN UPLOAD & PROCESS
# -----------------------------
def upload_and_process():
    """Main function to handle all upload operations"""
    
    log("=" * 70)
    log("STARTING UPLOAD PROCESS")
    log("=" * 70)
    
    # Validate credentials
    validate_credentials()
    
    # Step 1: Create ZIP files
    log("\nSTEP 1: Creating ZIP archives...")
    
    images_zipped = zip_directory(LOCAL_IMAGES_FOLDER, ZIP_IMAGES_PATH)
    today_zipped = zip_directory(LOCAL_JSON_TODAY, ZIP_TODAY_PATH)
    tomorrow_zipped = zip_directory(LOCAL_JSON_TOMORROW, ZIP_TOMORROW_PATH)
    
    if not images_zipped:
        log("ERROR: Images folder not found or empty")
        cleanup_local_zips()
        sys.exit(1)
    
    if not today_zipped or not tomorrow_zipped:
        log("WARNING: Some JSON folders not found")
    
    # Step 2: Connect to server
    log("\nSTEP 2: Connecting to server via SSH...")
    
    try:
        # SSH Connection
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(SSH_HOST, SSH_PORT, SSH_USER, SSH_PASSWORD)
        log(f"Connected to {SSH_HOST} as {SSH_USER}")
        
        # SFTP Connection
        sftp = ssh.open_sftp()
        log("SFTP connection established")
        
    except Exception as e:
        log(f"ERROR: Failed to connect to server: {e}")
        cleanup_local_zips()
        sys.exit(1)
    
    try:
        # Step 3: Upload and extract images
        log("\nSTEP 3: Uploading and extracting images...")
        
        remote_images_zip = os.path.join(REMOTE_IMAGES_DIR, "downloaded-images.zip")
        
        # Ensure remote directory exists
        mkdir_cmd = f"mkdir -p '{REMOTE_IMAGES_DIR}'"
        stdin, stdout, stderr = ssh.exec_command(mkdir_cmd)
        stdout.read(); stderr.read()
        
        # Upload images ZIP
        log(f"Uploading: {ZIP_IMAGES_PATH} -> {remote_images_zip}")
        sftp.put(ZIP_IMAGES_PATH, remote_images_zip)
        log("Images ZIP uploaded successfully")
        
        # Extract images on server
        log(f"Extracting images in: {REMOTE_IMAGES_DIR}")
        unzip_cmd = f"cd '{REMOTE_IMAGES_DIR}'; unzip -o downloaded-images.zip"
        stdin, stdout, stderr = ssh.exec_command(unzip_cmd)
        out = stdout.read().decode()
        err = stderr.read().decode()
        
        if out.strip():
            log("Extract output: " + out.strip()[:200])  # First 200 chars
        if err.strip() and "inflating" not in err.lower():
            log("Extract warnings: " + err.strip()[:200])
        
        # Delete remote images ZIP
        delete_cmd = f"rm -f '{remote_images_zip}'"
        stdin, stdout, stderr = ssh.exec_command(delete_cmd)
        stdout.read(); stderr.read()
        log("Deleted remote images ZIP")
        
        # Step 4: Upload and extract JSON files (odd/even logic)
        log("\nSTEP 4: Uploading and extracting JSON files...")
        
        # Get current date in IST timezone
        today_ist = datetime.now(IST)
        today_day = today_ist.day
        is_today_even = (today_day % 2 == 0)
        
        log(f"Current IST time: {today_ist.strftime('%Y-%m-%d %H:%M:%S')}")
        log(f"Current IST date: {today_day}")
        
        if is_today_even:
            mapping = {
                "today": (ZIP_TODAY_PATH, REMOTE_JSON_EVEN_DIR, "today-jsons.zip"),
                "tomorrow": (ZIP_TOMORROW_PATH, REMOTE_JSON_ODD_DIR, "tomorrow-jsons.zip")
            }
            log(f"Today ({today_day}) is EVEN → today/ → even/, tomorrow/ → odd/")
        else:
            mapping = {
                "today": (ZIP_TODAY_PATH, REMOTE_JSON_ODD_DIR, "today-jsons.zip"),
                "tomorrow": (ZIP_TOMORROW_PATH, REMOTE_JSON_EVEN_DIR, "tomorrow-jsons.zip")
            }
            log(f"Today ({today_day}) is ODD → today/ → odd/, tomorrow/ → even/")
        
        # Upload and extract each JSON folder
        for period, (local_zip, remote_dir, remote_zip_name) in mapping.items():
            if not os.path.exists(local_zip):
                log(f"WARNING: Skipping {period} - ZIP not found: {local_zip}")
                continue
            
            log(f"\nProcessing {period.upper()} JSONs:")
            
            remote_zip_path = os.path.join(remote_dir, remote_zip_name)
            
            # Ensure remote directory exists
            mkdir_cmd = f"mkdir -p '{remote_dir}'"
            stdin, stdout, stderr = ssh.exec_command(mkdir_cmd)
            stdout.read(); stderr.read()
            
            # Upload JSON ZIP
            log(f"Uploading: {local_zip} -> {remote_zip_path}")
            sftp.put(local_zip, remote_zip_path)
            log(f"{period.capitalize()} JSON ZIP uploaded")
            
            # Extract JSON on server
            log(f"Extracting in: {remote_dir}")
            unzip_cmd = f"cd '{remote_dir}'; unzip -o '{remote_zip_name}'"
            stdin, stdout, stderr = ssh.exec_command(unzip_cmd)
            out = stdout.read().decode()
            err = stderr.read().decode()
            
            if out.strip():
                log("Extract output: " + out.strip()[:200])
            if err.strip() and "inflating" not in err.lower():
                log("Extract warnings: " + err.strip()[:200])
            
            # Delete remote JSON ZIP
            delete_cmd = f"rm -f '{remote_zip_path}'"
            stdin, stdout, stderr = ssh.exec_command(delete_cmd)
            stdout.read(); stderr.read()
            log(f"Deleted remote {period} JSON ZIP")
        
        log("\nAll files uploaded and extracted successfully")
        
    except Exception as e:
        log(f"ERROR during upload/process: {e}")
        raise
    
    finally:
        # Close connections
        sftp.close()
        ssh.close()
        log("\nConnections closed")
    
    # Step 6: Cleanup local ZIP files
    log("\nSTEP 5: Cleaning up local ZIP files...")
    cleanup_local_zips()
    
    log("\n" + "=" * 70)
    log("UPLOAD PROCESS COMPLETED SUCCESSFULLY")
    log("=" * 70)

# -----------------------------
# ENTRY POINT
# -----------------------------
if __name__ == "__main__":
    try:
        upload_and_process()
    except KeyboardInterrupt:
        log("\nProcess interrupted by user")
        cleanup_local_zips()
        sys.exit(1)
    except Exception as e:
        log(f"\nFATAL ERROR: {e}")
        cleanup_local_zips()
        sys.exit(1)
