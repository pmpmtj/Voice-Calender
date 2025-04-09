#!/usr/bin/env python3
"""
Google Drive File Download for Voice Calendar

This script downloads audio files from Google Drive:
- Authenticates with Google Drive API
- Searches for specific folders configured in the settings
- Downloads audio files to be processed for calendar event creation
- Supports filtering by file types and automatic cleanup

It processes files from the specified Google Drive folders and prepares them
for transcription and analysis for calendar event entries.
"""

import os
import io
import pickle
import sys
import logging
from logging.handlers import RotatingFileHandler
import json

from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Any, Union
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Initialize paths - handling both frozen (PyInstaller) and regular Python execution
if getattr(sys, 'frozen', False):
    # Running as compiled executable
    SCRIPT_DIR = Path(sys._MEIPASS)
else:
    # Running as script
    SCRIPT_DIR = Path(__file__).parent.absolute()

# Project root for path calculations
PROJECT_ROOT = SCRIPT_DIR.parent

# Define log directory
LOGS_DIR = SCRIPT_DIR / "logs"

# Make sure the log directory exists
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Updated path to the configuration file
CONFIG_DIR = PROJECT_ROOT / "project_modules_configs" / "config_dwnload_files"
CONFIG_FILE = CONFIG_DIR / "dwnload_from_gdrive_conf.json"

# Define ensure_directory_exists before it's used
def ensure_directory_exists(directory_path, purpose="directory"):
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        directory_path: Path to the directory to ensure exists
        purpose: Description of the directory for logging messages
        
    Returns:
        True if directory exists or was created successfully
    """
    dir_path = Path(directory_path)
    if dir_path.exists():
        return True
        
    try:
        print(f"Creating {purpose}: {dir_path}")
        dir_path.mkdir(parents=True, exist_ok=True)
        
        # Use print initially, and if logger is available, use it too
        print(f"Created {purpose}: {dir_path}")
        
        # Try to use logger if it's been defined
        logger_obj = globals().get('logger')
        if logger_obj:
            logger_obj.info(f"Created {purpose}: {dir_path}")
            
        return True
    except Exception as e:
        error_msg = f"ERROR: Failed to create {purpose}: {dir_path}. Error: {str(e)}"
        print(error_msg)
        
        # Try to use logger if it's been defined
        logger_obj = globals().get('logger')
        if logger_obj:
            logger_obj.error(error_msg)
            
        print("This directory is required for the script to function. Exiting.")
        sys.exit(1)

# Load configuration
try:
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            CONFIG = json.load(f)
        # Add paths section if missing
        if 'downloads_path' not in CONFIG:
            CONFIG['downloads_path'] = {}
        
        # Handle downloads directory: use config path or default to script_dir/downloads
        downloads_path = CONFIG['downloads_path'].get('downloads_dir', 'downloads')
        if os.path.isabs(downloads_path):
            # If it's an absolute path, use it directly
            DOWNLOADS_DIR = Path(downloads_path)
        else:
            # If it's a relative path, make it relative to the script directory
            DOWNLOADS_DIR = SCRIPT_DIR / downloads_path
            
        # Update the config with the resolved path
        CONFIG['downloads_path']['downloads_dir'] = str(DOWNLOADS_DIR)
    else:
        print(f"ERROR: Config file not found at {CONFIG_FILE}")
        print("Please ensure a valid config file exists before running this script.")
        sys.exit(1)
    
    # Create downloads directory if it doesn't exist
    DOWNLOADS_DIR = Path(CONFIG['downloads_path']['downloads_dir'])
    ensure_directory_exists(DOWNLOADS_DIR, "downloads directory")
    
except json.JSONDecodeError:
    print(f"ERROR: Invalid JSON in config file {CONFIG_FILE}")
    sys.exit(1)

# Configure logging
logging_config = CONFIG.get("logging", {})
log_level = getattr(logging, logging_config.get("level", "INFO"))
log_format = logging_config.get("format", "%(asctime)s - %(levelname)s - %(message)s")
log_file = logging_config.get("log_file", "download_calendar_files.log")
max_size = logging_config.get("max_size_bytes", 1048576)  # Default 1MB
backup_count = logging_config.get("backup_count", 3)

# Set up logger
logger = logging.getLogger("voice_calender.download")
logger.setLevel(log_level)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(log_format))
logger.addHandler(console_handler)

# File handler with rotation
log_path = LOGS_DIR / log_file
file_handler = RotatingFileHandler(
    log_path, maxBytes=max_size, backupCount=backup_count
)
file_handler.setFormatter(logging.Formatter(log_format))
logger.addHandler(file_handler)

# Log initial information
logger.info("Voice Calendar Download Service")
logger.info(f"Logging to {log_path}")

# Get credentials paths from config or use defaults
def get_credentials_paths(config):
    """
    Get credentials file paths with mobile/portable support.
    
    Lookup order:
    1. Default location: SCRIPT_DIR/credentials/[filename from config]
    2. If path in config, interpret as either:
       a. Absolute path if it starts with drive letter
       b. Relative path from SCRIPT_DIR if not absolute
    
    Returns:
        Tuple of (credentials_file_path, token_file_path)
    """
    # Get filenames from config
    credentials_filename = config['auth'].get('credentials_file', 'gdrive_credentials.json')
    token_filename = config['auth'].get('token_file', 'gdrive_token.pickle')
    
    # Default credentials directory is under SCRIPT_DIR
    default_creds_dir = SCRIPT_DIR / "credentials"
    
    # First, check if credentials_path exists in config
    if config and 'credentials_path' in config:
        creds_path = config['credentials_path']
        
        # Determine if it's absolute or relative
        if os.path.isabs(creds_path):
            # It's an absolute path, use it directly
            full_creds_path = Path(creds_path)
            creds_dir = full_creds_path.parent
        else:
            # It's a relative path, make it relative to SCRIPT_DIR
            creds_dir = SCRIPT_DIR / Path(creds_path).parent
            full_creds_path = creds_dir / Path(creds_path).name
        
        # Create token path in same directory
        token_path = creds_dir / token_filename
        
        # Check if the file exists
        if full_creds_path.exists():
            print(f"Using credentials from config path: {full_creds_path}")
            return full_creds_path, token_path
        else:
            print(f"Warning: Credentials file specified in config not found: {full_creds_path}")
    
    # If we get here, use the default location
    ensure_directory_exists(default_creds_dir, "credentials directory")
    default_creds_path = default_creds_dir / credentials_filename
    default_token_path = default_creds_dir / token_filename
    
    print(f"Using default credentials location: {default_creds_path}")
    return default_creds_path, default_token_path

# Set up credentials paths
CREDENTIALS_FILE, TOKEN_FILE = get_credentials_paths(CONFIG)
print("Using credentials from:", CREDENTIALS_FILE)
print("Using token file:", TOKEN_FILE)

def find_folder_by_name(service, folder_name):
    """Find a folder ID by its name in Google Drive.
    
    Args:
        service: Google Drive API service instance
        folder_name: Name of the folder to find
        
    Returns:
        str: Folder ID if found, None otherwise
    """
    try:
        # Search for folders with the given name
        query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and trashed=false"
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)'
        ).execute()
        
        items = results.get('files', [])
        
        if not items:
            logger.warning(f"No folder named '{folder_name}' found.")
            return None
            
        # Return the ID of the first matched folder
        folder_id = items[0]['id']
        logger.info(f"Found folder '{folder_name}' with ID: {folder_id}")
        return folder_id
        
    except Exception as e:
        logger.error(f"Error finding folder '{folder_name}': {str(e)}")
        return None

def list_files_in_folder(service, folder_id, file_extensions=None):
    """List files in a Google Drive folder.
    
    Args:
        service: Google Drive API service instance
        folder_id: ID of the folder to list files from
        file_extensions: Optional dict with 'include' key containing list of file extensions to filter by
        
    Returns:
        list: List of file objects (each containing id, name, mimeType)
    """
    try:
        # The query for files in the specified folder
        query = f"'{folder_id}' in parents and trashed = false"
        
        # Retrieve files with needed fields
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, mimeType)'
        ).execute()
        
        items = results.get('files', [])
        
        # Filter out folders
        items = [item for item in items if item.get('mimeType') != 'application/vnd.google-apps.folder']
        
        # Filter by file extensions if specified
        if file_extensions and 'include' in file_extensions:
            extensions = file_extensions['include']
            items = [item for item in items if any(item['name'].lower().endswith(ext.lower()) for ext in extensions)]
        
        return items
        
    except Exception as e:
        logger.error(f"Error listing files in folder {folder_id}: {str(e)}")
        return []

def check_credentials_file() -> bool:
    """Check if credentials.json exists and provide help if not."""
    if not CREDENTIALS_FILE.exists():
        logger.error(f"'{CREDENTIALS_FILE}' file not found!")
        
        # Get just the filename without the path
        credentials_filename = Path(CREDENTIALS_FILE).name
        credentials_dir = Path(CREDENTIALS_FILE).parent
        
        print("\nCredential file not found. Please do one of the following:")
        print(f"1. Create the directory {credentials_dir} if it doesn't exist")
        print(f"2. Place '{credentials_filename}' in: {credentials_dir}")
        print(f"3. Or update the 'credentials_path' in your config file ({CONFIG_FILE})")
        print("\nTo create your credentials file:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Create a project or select an existing one")
        print("3. Enable the Google Drive API:")
        print("   - Navigate to 'APIs & Services' > 'Library'")
        print("   - Search for 'Google Drive API' and enable it")
        print("4. Create OAuth credentials:")
        print("   - Go to 'APIs & Services' > 'Credentials'")
        print("   - Click 'Create Credentials' > 'OAuth client ID'")
        print("   - Select 'Desktop app' as application type")
        print(f"   - Download the JSON file and rename it to '{credentials_filename}'")
        print(f"   - Place it in: {credentials_dir}")
        print("\nThen run this script again.")
        return False
    return True

def authenticate_google_drive():
    """Authenticate with Google Drive API using OAuth."""
    try:
        creds = None
        
        # The token file stores the user's access and refresh tokens
        if TOKEN_FILE.exists():
            with open(TOKEN_FILE, 'rb') as token:
                creds = pickle.load(token)
                
        # If no valid credentials are available, let the user log in
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not check_credentials_file():
                    sys.exit(1)
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(CREDENTIALS_FILE), CONFIG['api']['scopes'])
                creds = flow.run_local_server(port=0)
                
            # Save the credentials for the next run
            token_dir = TOKEN_FILE.parent
            ensure_directory_exists(token_dir, "token directory")
                
            with open(TOKEN_FILE, 'wb') as token:
                pickle.dump(creds, token)
        
        # Build the service with the credentials
        service = build('drive', 'v3', credentials=creds)
        return service
    except Exception as e:
        logger.error(f"Authentication error: {str(e)}")
        raise

def download_file(service, file_id, file_name=None, download_dir=None):
    """Download a file from Google Drive by ID.
    
    Args:
        service: Google Drive service instance
        file_id: ID of the file to download OR a file object with 'id' and 'name' keys
        file_name: Name of the file to save (optional if file_id is a dict) or full path to save the file to
        download_dir: Optional directory path where to save downloaded file
    
    Returns:
        dict: A dictionary with the download result information
    """
    try:
        # If file_id is a dict (file object), extract the id and name
        if isinstance(file_id, dict):
            file_info = file_id
            file_name = file_info.get('name')
            file_id = file_info.get('id')
        
        # Determine the output path
        if os.path.isabs(file_name) or '/' in file_name or '\\' in file_name:
            # file_name is already a full path
            output_path = Path(file_name)
            # Ensure parent directory exists
            ensure_directory_exists(output_path.parent, "output directory")
            # Extract just the filename for logging
            display_name = output_path.name
        else:
            # file_name is just a filename, use download_dir
            if download_dir:
                download_dir_path = Path(download_dir)
            else:
                download_dir_path = Path(CONFIG['downloads_path']['downloads_dir'])
            
            # Ensure download directory exists
            ensure_directory_exists(download_dir_path, "download directory")
            
            # Generate filename with timestamp if configured
            if CONFIG.get('download', {}).get('add_timestamps', False):
                timestamp_format = CONFIG.get('download', {}).get('timestamp_format', '%Y%m%d_%H%M%S_%f')
                output_filename = generate_filename_with_timestamp(file_name, timestamp_format)
            else:
                output_filename = file_name
                
            # Create the full file path
            output_path = download_dir_path / output_filename
            display_name = file_name
            
        logger.info(f"Downloading {display_name} as {output_path}")
        
        # Create a file handler
        with open(output_path, 'wb') as f:
            # Get the file as media content
            request = service.files().get_media(fileId=file_id)
            
            # Download the file
            downloader = MediaIoBaseDownload(f, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                logger.info(f"Download {int(status.progress() * 100)}% complete.")
        
        logger.info(f"Download complete! Saved as: {output_path}")
        
        return {
            "success": True,
            "original_filename": display_name,
            "saved_as": str(output_path),
            "file_id": file_id
        }
            
    except Exception as e:
        logger.error(f"Error downloading file {file_name}: {str(e)}")
        
        return {
            "success": False,
            "original_filename": file_name,
            "file_id": file_id,
            "error": str(e)
        }

def delete_file(service, file_id, file_name=None):
    """Delete a file from Google Drive.
    
    Args:
        service: Google Drive API service instance
        file_id: ID of the file to delete OR a file object with 'id' and 'name' keys
        file_name: Name of the file (for logging purposes), optional if file_id is a dict
        
    Returns:
        bool: True if deletion was successful, False otherwise
    """
    try:
        # If file_id is a dict (file object), extract the id and name
        if isinstance(file_id, dict):
            file_name = file_id.get('name', 'Unknown file')
            file_id = file_id.get('id')
        
        # Execute the deletion
        logger.info(f"Deleting file: {file_name}")
        service.files().delete(fileId=file_id).execute()
        logger.info(f"File '{file_name}' deleted successfully.")
        return True
    except Exception as e:
        logger.error(f"Error deleting file '{file_name}': {str(e)}")
        return False

def process_folder(service, folder_id, folder_name, dry_run=False):
    """Process files in a Google Drive folder (non-recursively)."""
    try:
        # For the process_folder, we need to use a specific query
        query = f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false"
        results = service.files().list(
            q=query,
            fields="files(id, name, mimeType, size, modifiedTime, fileExtension)",
            pageSize=1000
        ).execute()
        all_items = results.get('files', [])
        
        if not all_items:
            logger.info(f"No files found in folder: {folder_name}")
            return {
                'total_files': 0,
                'processed_files': 0,
                'downloaded_files': 0,
                'skipped_files': 0,
                'error_files': 0,
                'deleted_files': 0,
                'audio_files': 0,
                'image_files': 0,
                'video_files': 0
            }
        
        # Get file extensions
        audio_file_types = CONFIG.get('audio_file_types', {}).get('include', [])
        image_file_types = CONFIG.get('image_file_types', {}).get('include', [])
        video_file_types = CONFIG.get('video_file_types', {}).get('include', [])
        
        # Filter files by type
        audio_items = [item for item in all_items 
                       if any(item['name'].lower().endswith(ext.lower()) 
                              for ext in audio_file_types)] if audio_file_types else []
                              
        image_items = [item for item in all_items 
                      if any(item['name'].lower().endswith(ext.lower()) 
                             for ext in image_file_types)] if image_file_types else []
                             
        video_items = [item for item in all_items 
                      if any(item['name'].lower().endswith(ext.lower()) 
                             for ext in video_file_types)] if video_file_types else []
        
        # Combine all items to download
        items_to_download = audio_items + image_items + video_items
        
        # Count metrics
        stats = {
            'total_files': len(all_items),
            'processed_files': len(all_items),
            'downloaded_files': 0,
            'skipped_files': len(all_items) - len(items_to_download),
            'error_files': 0,
            'deleted_files': 0,
            'audio_files': len(audio_items),
            'image_files': len(image_items),
            'video_files': len(video_items)
        }
        
        # Setup download directory
        base_download_dir = Path(CONFIG['downloads_path']['downloads_dir'])
        
        # Log the types of files found
        logger.info(f"Found {len(audio_items)} audio files, {len(image_items)} image files, and {len(video_items)} video files in folder: {folder_name}")
        
        # Process each file to download
        for item in items_to_download:
            item_id = item['id']
            item_name = item['name']
            
            # If we need creation time for timestamping, get it from file details
            created_time = ''
            if CONFIG.get('download', {}).get('add_timestamps', False):
                try:
                    file_details = service.files().get(fileId=item_id, fields="createdTime").execute()
                    created_time = file_details.get('createdTime', '')
                except Exception as e:
                    logger.warning(f"Couldn't get creation time for {item_name}: {str(e)}")
            
            # Log file being processed
            file_type = "unknown"
            if any(item_name.lower().endswith(ext.lower()) for ext in audio_file_types):
                file_type = "audio"
            elif any(item_name.lower().endswith(ext.lower()) for ext in image_file_types):
                file_type = "image"
            elif any(item_name.lower().endswith(ext.lower()) for ext in video_file_types):
                file_type = "video"
                
            logger.info(f"Processing {file_type} file: {item_name}")
            
            # Generate output path with timestamp if configured
            if CONFIG.get('download', {}).get('add_timestamps', False):
                timestamp_format = CONFIG.get('download', {}).get('timestamp_format', '%Y%m%d_%H%M%S_%f')
                
                # Use the file's creation time from Google Drive if available
                if created_time:
                    try:
                        created_time_dt = datetime.fromisoformat(created_time.replace('Z', '+00:00'))
                        created_time_dt = created_time_dt.astimezone()
                        timestamped_name = created_time_dt.strftime(timestamp_format) + "_" + item_name
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse creation time for {item_name}, using current time instead")
                        timestamped_name = generate_filename_with_timestamp(item_name, timestamp_format)
                else:
                    timestamped_name = generate_filename_with_timestamp(item_name, timestamp_format)
                    
                output_path = base_download_dir / timestamped_name
            else:
                output_path = base_download_dir / item_name
            
            # In dry run mode, just log what would happen
            if dry_run:
                print(f"Would download {file_type} file: {item_name} -> {output_path}")
                if CONFIG.get('download', {}).get('delete_after_download', False):
                    print(f"Would delete file from Google Drive after download: {item_name}")
                stats['downloaded_files'] += 1
                continue
            
            # Download the file
            try:
                download_result = download_file(service, item_id, str(output_path))
                
                if download_result['success']:
                    stats['downloaded_files'] += 1
                    logger.info(f"Successfully downloaded {file_type} file: {item_name}")
                    
                    # Delete file from Google Drive if configured
                    if CONFIG.get('download', {}).get('delete_after_download', False):
                        delete_file(service, item_id, item_name)
                        stats['deleted_files'] += 1
                else:
                    stats['error_files'] += 1
            except Exception as e:
                logger.error(f"Error processing file {item_name}: {str(e)}")
                stats['error_files'] += 1
        
        # Log statistics for this folder
        logger.info(f"Folder '{folder_name}' statistics:")
        logger.info(f"  - Total files: {stats['total_files']}")
        logger.info(f"  - Audio files: {stats['audio_files']}")
        logger.info(f"  - Image files: {stats['image_files']}")
        logger.info(f"  - Video files: {stats['video_files']}")
        logger.info(f"  - Downloaded files: {stats['downloaded_files']}")
        logger.info(f"  - Deleted files: {stats['deleted_files']}")
        
        return stats
        
    except Exception as e:
        logger.exception(f"Error processing folder '{folder_name}': {str(e)}")
        return {
            'total_files': 0,
            'processed_files': 0,
            'downloaded_files': 0,
            'skipped_files': 0,
            'error_files': 1,
            'deleted_files': 0,
            'audio_files': 0,
            'image_files': 0,
            'video_files': 0
        }

def generate_filename_with_timestamp(filename: str, timestamp_format: Optional[str] = None) -> str:
    """
    Generate a filename with a timestamp prefix.
    
    Args:
        filename: The original filename
        timestamp_format: Format string for the timestamp, if None the original filename is returned
    
    Returns:
        The filename with timestamp prefix added
    """
    if not timestamp_format:
        return filename
        
    timestamp = datetime.now().strftime(timestamp_format)
    return f"{timestamp}_{filename}"

def main():
    """Main function to process Google Drive files for calendar."""
    if not check_credentials_file():
        return
    
    try:
        # Check if any file types have their 'include' lists configured
        audio_enabled = 'audio_file_types' in CONFIG and 'include' in CONFIG['audio_file_types'] and CONFIG['audio_file_types']['include']
        image_enabled = 'image_file_types' in CONFIG and 'include' in CONFIG['image_file_types'] and CONFIG['image_file_types']['include']
        video_enabled = 'video_file_types' in CONFIG and 'include' in CONFIG['video_file_types'] and CONFIG['video_file_types']['include']
        
        if not (audio_enabled or image_enabled or video_enabled):
            logger.info("No file types configured for download. Exiting without making API calls to Google Drive.")
            print("No file types configured for download. No files will be downloaded.")
            return
            
        # Authenticate with Google Drive
        service = authenticate_google_drive()
        if not service:
            logger.error("Failed to authenticate with Google Drive.")
            return
            
        # Get target folders from configuration
        target_folders = CONFIG['folders'].get('target_folders', ['root'])
        
        # Check if running in dry run mode
        dry_run = CONFIG.get('dry_run', False)
        if dry_run:
            logger.info("Running in DRY RUN mode - no files will be downloaded or deleted")
            print("\n=== DRY RUN MODE - NO FILES WILL BE DOWNLOADED OR DELETED ===\n")
        
        # Process each target folder
        for folder_name in target_folders:
            if folder_name.lower() == 'root':
                # Root folder has a special ID
                folder_id = 'root'
                logger.info(f"Processing root folder")
            else:
                # Find folder by name
                logger.info(f"Looking for folder: {folder_name}")
                folder_id = find_folder_by_name(service, folder_name)
                
                if not folder_id:
                    logger.warning(f"Folder '{folder_name}' not found. Skipping.")
                    continue
                
                logger.info(f"Processing folder: {folder_name} (ID: {folder_id})")
            
            # Process files in the folder
            process_folder(service, folder_id, folder_name, dry_run=dry_run)
        
        logger.info("Google Drive download process for calendar files completed.")
        
    except Exception as e:
        logger.exception(f"An error occurred during the calendar file download process: {str(e)}")


if __name__ == "__main__":
    main()
