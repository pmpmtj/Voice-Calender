#!/usr/bin/env python3
"""
Voice Calendar Scheduler

This script orchestrates the entire Voice Calendar workflow:
1. Downloads audio files from Google Drive
2. Transcribes the audio files using OpenAI Whisper API
3. Parses the transcriptions to extract calendar events
4. Inserts events into Google Calendar
5. Deletes processed files to prevent duplicate entries

The scheduler runs at configured intervals and handles logging,
error recovery, and state management.
"""

import logging
import logging.handlers
import json
import os
import sys
import time
import traceback
import threading
from datetime import datetime, timedelta
from pathlib import Path

# Import voice calendar components
from voice_calender.download_files_for_calender.download_files_for_calender import main as download_files_main
from voice_calender.transcribe_audio_for_calender.transcribe_audio_for_calender import run_transcribe
from voice_calender.agent_parse_entry_for_calender.agent_parse_entry_for_calender import parse_calendar_entries
from voice_calender.insert_event_in_gcalendar.insert_event_in_gcalendar import GoogleCalendarManager
from voice_calender.db_utils.db_manager import get_calendar_events_by_config_interval, initialize_db, close_all_connections, save_calendar_event
from voice_calender.send_email.send_email import main as send_email_main
from voice_calender.file_utils.delete_files import main as delete_files_main

# === Constants ===
# Initialize paths - handling both frozen (PyInstaller) and regular Python execution
if getattr(sys, 'frozen', False):
    # Running as compiled executable
    SCRIPT_DIR = Path(sys._MEIPASS)
else:
    # Running as script
    SCRIPT_DIR = Path(__file__).parent.absolute()

# Project root for path calculations
PROJECT_ROOT = SCRIPT_DIR.parent

# Print the actual paths for debugging
logger = logging.getLogger(__name__)  # Initialize logger early for debug prints
logger.info(f"SCRIPT_DIR: {SCRIPT_DIR}")
logger.info(f"PROJECT_ROOT: {PROJECT_ROOT}")

# Configuration and state file paths
CONFIG_DIR = PROJECT_ROOT / "project_modules_configs" / "config_app_calender_scheduler"
CONFIG_FILE = CONFIG_DIR / "app_calender_scheduler_config.json"
STATE_FILE = SCRIPT_DIR / 'calendar_pipeline_state.json'
LOG_DIR = SCRIPT_DIR / 'logs'

# Ensure log directory exists
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Initialize logger
logger = logging.getLogger(__name__)

# === Config Handling ===
def load_config():
    """Load configuration from JSON file."""
    # Try multiple possible paths
    possible_config_paths = [
        CONFIG_FILE,
        Path(__file__).parent.parent / "project_modules_configs" / "config_app_calender_scheduler" / "app_calender_scheduler_config.json",
        Path().absolute() / "src" / "voice_calender" / "project_modules_configs" / "config_app_calender_scheduler" / "app_calender_scheduler_config.json",
        Path().absolute().parent / "src" / "voice_calender" / "project_modules_configs" / "config_app_calender_scheduler" / "app_calender_scheduler_config.json"
    ]
    
    config_path = None
    for path in possible_config_paths:
        if path.exists():
            config_path = path
            print(f"Found scheduler config at: {config_path}")
            break
    
    if not config_path:
        paths_tried = "\n".join([str(p) for p in possible_config_paths])
        print(f"Config file not found. Tried paths:\n{paths_tried}")
        sys.exit(1)
        
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        if "scheduler" not in config:
            raise ValueError("Missing 'scheduler' section in config file")
        return config
    except Exception as e:
        print(f"Failed to load configuration: {e}")
        sys.exit(1)

def validate_config(config):
    """Validate configuration structure and values."""
    scheduler = config.get("scheduler", {})
    if "runs_per_day" not in scheduler:
        raise ValueError("Missing 'runs_per_day' in scheduler section")
    if not isinstance(scheduler["runs_per_day"], (int, float)):
        raise ValueError("runs_per_day must be a number")

# === Interval Calculation ===
def calculate_interval_seconds(runs_per_day):
    """Calculate seconds between scheduled runs based on runs per day."""
    return 0 if runs_per_day == 0 else int(86400 / runs_per_day)

def calculate_next_run_time(interval_seconds):
    """Calculate the next run time based on the current time and interval."""
    now = datetime.now()
    return now + timedelta(seconds=interval_seconds)

# === State Management ===
def update_pipeline_state(state_file, updates):
    """Update the pipeline state file with the latest run information."""
    try:
        with open(state_file, 'w') as f:
            json.dump(updates, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to update state file: {e}")

# === Calendar Event Processing ===
def process_calendar_event_files():
    """
    Process JSON files created by the parser and add them to Google Calendar.
    
    Returns:
        tuple: (int, int) Count of successfully inserted events and errors
    """
    # Get path to JSON output directory from agent_parse_entry config
    try:
        # Load scheduler config - try multiple possible paths
        possible_scheduler_config_paths = [
            PROJECT_ROOT / "project_modules_configs" / "config_app_calender_scheduler" / "app_calender_scheduler_config.json",
            Path(__file__).parent.parent / "project_modules_configs" / "config_app_calender_scheduler" / "app_calender_scheduler_config.json",
            Path().absolute() / "src" / "voice_calender" / "project_modules_configs" / "config_app_calender_scheduler" / "app_calender_scheduler_config.json",
            Path().absolute().parent / "src" / "voice_calender" / "project_modules_configs" / "config_app_calender_scheduler" / "app_calender_scheduler_config.json"
        ]
        
        scheduler_config_path = None
        for path in possible_scheduler_config_paths:
            if path.exists():
                scheduler_config_path = path
                logger.info(f"Found scheduler config at: {scheduler_config_path}")
                break
        
        if not scheduler_config_path:
            logger.error(f"Scheduler config file not found. Tried paths: {[str(p) for p in possible_scheduler_config_paths]}")
            return 0, 0
            
        with open(scheduler_config_path, 'r', encoding='utf-8') as f:
            scheduler_config = json.load(f)
            
        # Load validation settings from config
        file_processing_config = scheduler_config.get("file_processing", {})
        event_validation_config = scheduler_config.get("event_validation", {})
        
        archive_processed_files = file_processing_config.get("archive_processed_files", True)
        archive_directory_name = file_processing_config.get("archive_directory_name", "processed")
        default_event_duration_hours = file_processing_config.get("default_event_duration_hours", 1)
        
        required_fields = event_validation_config.get("required_fields", ["summary", "start"])
        start_fields = event_validation_config.get("start_fields", ["dateTime", "date"])
        end_fields = event_validation_config.get("end_fields", ["dateTime", "date"])
        
        # Get path to JSON output directory from agent_parse_entry config - try multiple possible paths
        possible_parse_config_paths = [
            PROJECT_ROOT / "project_modules_configs" / "config_agent_parse_entry" / "agent_parse_entry_config.json",
            Path(__file__).parent.parent / "project_modules_configs" / "config_agent_parse_entry" / "agent_parse_entry_config.json",
            Path().absolute() / "src" / "voice_calender" / "project_modules_configs" / "config_agent_parse_entry" / "agent_parse_entry_config.json",
            Path().absolute().parent / "src" / "voice_calender" / "project_modules_configs" / "config_agent_parse_entry" / "agent_parse_entry_config.json"
        ]
        
        parse_config_path = None
        for path in possible_parse_config_paths:
            if path.exists():
                parse_config_path = path
                logger.info(f"Found parse entry config at: {parse_config_path}")
                break
        
        if not parse_config_path:
            logger.error(f"Parse entry config file not found. Tried paths: {[str(p) for p in possible_parse_config_paths]}")
            return 0, 0
            
        with open(parse_config_path, 'r', encoding='utf-8') as f:
            parse_config = json.load(f)
            
        json_output_dir = parse_config.get("paths", {}).get("json_output_directory")
        
        if not json_output_dir or not os.path.exists(json_output_dir):
            logger.error(f"JSON output directory not found: {json_output_dir}")
            return 0, 0
        
        # Initialize database
        initialize_db()
            
        # Create Google Calendar Manager
        calendar_manager = GoogleCalendarManager()
        
        # Authenticate explicitly before processing events
        try:
            calendar_manager.authenticate()
            logger.info("Successfully authenticated with Google Calendar API")
        except Exception as e:
            logger.error(f"Failed to authenticate with Google Calendar API: {e}")
            return 0, 0
        
        # Find all JSON files in the output directory
        json_files = list(Path(json_output_dir).glob("*.json"))
        
        if not json_files:
            logger.info("No calendar event JSON files found to process")
            return 0, 0
            
        logger.info(f"Found {len(json_files)} calendar event files to process")
        
        # Process each JSON file
        success_count = 0
        error_count = 0
        
        for json_file in json_files:
            try:
                logger.info(f"Processing calendar event file: {json_file}")
                
                # Load the JSON file
                with open(json_file, 'r', encoding='utf-8') as f:
                    event_data = json.load(f)
                
                # Check if event_data is a list of events
                if isinstance(event_data, list):
                    logger.info(f"Processing {len(event_data)} events from file {json_file}")
                    file_success_count = 0
                    
                    for event in event_data:
                        try:
                            # Validate event has required fields based on config
                            missing_fields = []
                            for field in required_fields:
                                if not event.get(field):
                                    missing_fields.append(field)
                            
                            if missing_fields:
                                logger.warning(f"Event in {json_file} missing required fields: {', '.join(missing_fields)}")
                                error_count += 1
                                continue
                            
                            # Validate start date/time
                            has_valid_start = False
                            if 'start' in event:
                                for field in start_fields:
                                    if event['start'].get(field):
                                        has_valid_start = True
                                        break
                            
                            if not has_valid_start:
                                logger.warning(f"Event in {json_file} missing valid start date/time fields: {', '.join(start_fields)}")
                                error_count += 1
                                continue
                            
                            # Ensure end date/time is present if add_end_time_if_missing is enabled
                            if file_processing_config.get("add_end_time_if_missing", True):
                                has_valid_end = False
                                if 'end' in event:
                                    for field in end_fields:
                                        if event['end'].get(field):
                                            has_valid_end = True
                                            break
                                
                                if not has_valid_end:
                                    logger.warning(f"Event in {json_file} missing end date/time - adding one")
                                    
                                    # Copy start to end if missing
                                    if 'start' in event:
                                        if 'end' not in event:
                                            event['end'] = {}
                                        
                                        # Copy date/time fields from start to end
                                        for field in start_fields:
                                            if field in event['start']:
                                                event['end'][field] = event['start'][field]
                                        
                                        # If using dateTime, add configured hours to end time
                                        if 'dateTime' in event['start']:
                                            # Parse datetime and add hours using datetime arithmetic
                                            try:
                                                from datetime import datetime, timedelta
                                                
                                                # Remove timezone info for processing if present
                                                dt_str = event['start']['dateTime']
                                                # Handle different timezone formats
                                                if '+' in dt_str:
                                                    dt_parts = dt_str.split('+')
                                                    dt_str = dt_parts[0]
                                                    tz_part = '+' + dt_parts[1]
                                                elif 'Z' in dt_str:
                                                    dt_str = dt_str.replace('Z', '')
                                                    tz_part = 'Z'
                                                else:
                                                    tz_part = ''
                                                
                                                # Parse datetime
                                                if 'T' in dt_str:
                                                    start_dt = datetime.fromisoformat(dt_str)
                                                else:
                                                    # If only date is provided
                                                    start_dt = datetime.fromisoformat(f"{dt_str}T00:00:00")
                                                
                                                # Add configured hours
                                                end_dt = start_dt + timedelta(hours=default_event_duration_hours)
                                                
                                                # Format back with timezone
                                                end_dt_str = end_dt.isoformat()
                                                if tz_part:
                                                    end_dt_str = end_dt_str + tz_part
                                                
                                                event['end']['dateTime'] = end_dt_str
                                                
                                            except Exception as e:
                                                logger.error(f"Error calculating end time: {e}")
                                                # Fallback to simple string manipulation if datetime parsing fails
                                                dt_parts = event['start']['dateTime'].split('T')
                                                if len(dt_parts) == 2:
                                                    date_part = dt_parts[0]
                                                    time_parts = dt_parts[1].split(':')
                                                    if len(time_parts) >= 2:
                                                        hour = int(time_parts[0])
                                                        new_hour = (hour + default_event_duration_hours) % 24
                                                        time_parts[0] = f"{new_hour:02d}"
                                                        event['end']['dateTime'] = f"{date_part}T{':'.join(time_parts)}"
                            
                            # Fix attendees without email addresses
                            if 'attendees' in event and isinstance(event['attendees'], list):
                                for i, attendee in enumerate(event['attendees']):
                                    if isinstance(attendee, dict) and 'email' not in attendee:
                                        display_name = attendee.get('displayName', f"attendee{i+1}")
                                        sanitized_name = ''.join(c for c in display_name if c.isalnum() or c == ' ').lower().replace(' ', '.')
                                        placeholder_email = f"{sanitized_name}@example.com"
                                        logger.warning(f"Attendee {display_name} is missing email - adding placeholder: {placeholder_email}")
                                        event['attendees'][i]['email'] = placeholder_email
                            
                            # Insert event into Google Calendar
                            logger.info(f"Inserting event: {json.dumps(event)[:200]}...")
                            result = calendar_manager.insert_event(event)
                            
                            if result:
                                logger.info(f"Successfully created calendar event: {event.get('summary')}")
                                success_count += 1
                                file_success_count += 1
                                
                                # Also save to database
                                try:
                                    # Extract fields needed for database insertion
                                    summary = event.get('summary')
                                    start_datetime = None
                                    start_timezone = None
                                    end_datetime = None
                                    end_timezone = None
                                    
                                    if 'start' in event:
                                        if 'dateTime' in event['start']:
                                            start_datetime = event['start']['dateTime']
                                        elif 'date' in event['start']:
                                            start_datetime = event['start']['date']
                                        
                                        start_timezone = event['start'].get('timeZone')
                                    
                                    if 'end' in event:
                                        if 'dateTime' in event['end']:
                                            end_datetime = event['end']['dateTime']
                                        elif 'date' in event['end']:
                                            end_datetime = event['end']['date']
                                        
                                        end_timezone = event['end'].get('timeZone')
                                    
                                    # Extract other optional fields
                                    location = event.get('location')
                                    description = event.get('description')
                                    attendees = event.get('attendees')
                                    recurrence = event.get('recurrence')
                                    reminders = event.get('reminders')
                                    visibility = event.get('visibility')
                                    color_id = event.get('colorId')
                                    transparency = event.get('transparency')
                                    status = event.get('status')
                                    
                                    # Save to database
                                    db_event_id = save_calendar_event(
                                        summary=summary,
                                        start_datetime=start_datetime,
                                        end_datetime=end_datetime,
                                        location=location,
                                        description=description,
                                        start_timezone=start_timezone,
                                        end_timezone=end_timezone,
                                        attendees=attendees,
                                        recurrence=recurrence,
                                        reminders=reminders,
                                        visibility=visibility,
                                        color_id=color_id,
                                        transparency=transparency,
                                        status=status
                                    )
                                    
                                    if db_event_id:
                                        logger.info(f"Event also saved to database with ID {db_event_id}")
                                    else:
                                        logger.warning(f"Event added to Google Calendar but failed to save to database")
                                        
                                except Exception as db_error:
                                    logger.error(f"Error saving event to database: {db_error}")
                            else:
                                logger.error(f"Failed to create calendar event from {json_file}")
                                error_count += 1
                                
                        except Exception as e:
                            logger.error(f"Error processing event from file {json_file}: {e}")
                            logger.error(traceback.format_exc())
                            error_count += 1
                    
                    # Archive or delete the file if at least one event was processed successfully
                    if file_success_count > 0 and archive_processed_files:
                        archive_dir = Path(json_output_dir) / archive_directory_name
                        archive_dir.mkdir(exist_ok=True)
                        
                        try:
                            # Archive the file by moving it to the processed directory
                            json_file.rename(archive_dir / json_file.name)
                            logger.info(f"Moved {json_file.name} to archive directory")
                        except Exception as e:
                            logger.error(f"Error archiving {json_file}: {e}")
                    
                else:
                    # Handle the case where event_data is a single event (not a list)
                    # Validate event has required fields based on config
                    missing_fields = []
                    for field in required_fields:
                        if not event_data.get(field):
                            missing_fields.append(field)
                    
                    if missing_fields:
                        logger.warning(f"Event in {json_file} missing required fields: {', '.join(missing_fields)}")
                        error_count += 1
                        continue
                    
                    # Validate start date/time
                    has_valid_start = False
                    if 'start' in event_data:
                        for field in start_fields:
                            if event_data['start'].get(field):
                                has_valid_start = True
                                break
                    
                    if not has_valid_start:
                        logger.warning(f"Event in {json_file} missing valid start date/time fields: {', '.join(start_fields)}")
                        error_count += 1
                        continue
                    
                    # Ensure end date/time is present if add_end_time_if_missing is enabled
                    if file_processing_config.get("add_end_time_if_missing", True):
                        has_valid_end = False
                        if 'end' in event_data:
                            for field in end_fields:
                                if event_data['end'].get(field):
                                    has_valid_end = True
                                    break
                        
                        if not has_valid_end:
                            logger.warning(f"Event in {json_file} missing end date/time - adding one")
                            
                            # Copy start to end if missing
                            if 'start' in event_data:
                                if 'end' not in event_data:
                                    event_data['end'] = {}
                                
                                # Copy date/time fields from start to end
                                for field in start_fields:
                                    if field in event_data['start']:
                                        event_data['end'][field] = event_data['start'][field]
                                
                                # If using dateTime, add configured hours to end time
                                if 'dateTime' in event_data['start']:
                                    # Parse datetime and add hours using datetime arithmetic
                                    try:
                                        from datetime import datetime, timedelta
                                        
                                        # Remove timezone info for processing if present
                                        dt_str = event_data['start']['dateTime']
                                        # Handle different timezone formats
                                        if '+' in dt_str:
                                            dt_parts = dt_str.split('+')
                                            dt_str = dt_parts[0]
                                            tz_part = '+' + dt_parts[1]
                                        elif 'Z' in dt_str:
                                            dt_str = dt_str.replace('Z', '')
                                            tz_part = 'Z'
                                        else:
                                            tz_part = ''
                                        
                                        # Parse datetime
                                        if 'T' in dt_str:
                                            start_dt = datetime.fromisoformat(dt_str)
                                        else:
                                            # If only date is provided
                                            start_dt = datetime.fromisoformat(f"{dt_str}T00:00:00")
                                        
                                        # Add configured hours
                                        end_dt = start_dt + timedelta(hours=default_event_duration_hours)
                                        
                                        # Format back with timezone
                                        end_dt_str = end_dt.isoformat()
                                        if tz_part:
                                            end_dt_str = end_dt_str + tz_part
                                        
                                        event_data['end']['dateTime'] = end_dt_str
                                        
                                    except Exception as e:
                                        logger.error(f"Error calculating end time: {e}")
                                        # Fallback to simple string manipulation if datetime parsing fails
                                        dt_parts = event_data['start']['dateTime'].split('T')
                                        if len(dt_parts) == 2:
                                            date_part = dt_parts[0]
                                            time_parts = dt_parts[1].split(':')
                                            if len(time_parts) >= 2:
                                                hour = int(time_parts[0])
                                                new_hour = (hour + default_event_duration_hours) % 24
                                                time_parts[0] = f"{new_hour:02d}"
                                                event_data['end']['dateTime'] = f"{date_part}T{':'.join(time_parts)}"
                    
                    # Fix attendees without email addresses
                    if 'attendees' in event_data and isinstance(event_data['attendees'], list):
                        for i, attendee in enumerate(event_data['attendees']):
                            if isinstance(attendee, dict) and 'email' not in attendee:
                                display_name = attendee.get('displayName', f"attendee{i+1}")
                                sanitized_name = ''.join(c for c in display_name if c.isalnum() or c == ' ').lower().replace(' ', '.')
                                placeholder_email = f"{sanitized_name}@example.com"
                                logger.warning(f"Attendee {display_name} is missing email - adding placeholder: {placeholder_email}")
                                event_data['attendees'][i]['email'] = placeholder_email
                    
                    # Insert event into Google Calendar
                    logger.info(f"Inserting event: {json.dumps(event_data)[:200]}...")
                    result = calendar_manager.insert_event(event_data)
                    
                    if result:
                        logger.info(f"Successfully created calendar event: {event_data.get('summary')}")
                        success_count += 1
                        
                        # Also save to database
                        try:
                            # Extract fields needed for database insertion
                            summary = event_data.get('summary')
                            start_datetime = None
                            start_timezone = None
                            end_datetime = None
                            end_timezone = None
                            
                            if 'start' in event_data:
                                if 'dateTime' in event_data['start']:
                                    start_datetime = event_data['start']['dateTime']
                                elif 'date' in event_data['start']:
                                    start_datetime = event_data['start']['date']
                                
                                start_timezone = event_data['start'].get('timeZone')
                            
                            if 'end' in event_data:
                                if 'dateTime' in event_data['end']:
                                    end_datetime = event_data['end']['dateTime']
                                elif 'date' in event_data['end']:
                                    end_datetime = event_data['end']['date']
                                
                                end_timezone = event_data['end'].get('timeZone')
                            
                            # Extract other optional fields
                            location = event_data.get('location')
                            description = event_data.get('description')
                            attendees = event_data.get('attendees')
                            recurrence = event_data.get('recurrence')
                            reminders = event_data.get('reminders')
                            visibility = event_data.get('visibility')
                            color_id = event_data.get('colorId')
                            transparency = event_data.get('transparency')
                            status = event_data.get('status')
                            
                            # Save to database
                            db_event_id = save_calendar_event(
                                summary=summary,
                                start_datetime=start_datetime,
                                end_datetime=end_datetime,
                                location=location,
                                description=description,
                                start_timezone=start_timezone,
                                end_timezone=end_timezone,
                                attendees=attendees,
                                recurrence=recurrence,
                                reminders=reminders,
                                visibility=visibility,
                                color_id=color_id,
                                transparency=transparency,
                                status=status
                            )
                            
                            if db_event_id:
                                logger.info(f"Event also saved to database with ID {db_event_id}")
                            else:
                                logger.warning(f"Event added to Google Calendar but failed to save to database")
                                
                        except Exception as db_error:
                            logger.error(f"Error saving event to database: {db_error}")
                        
                        # Handle the processed file - archive or delete based on config
                        if archive_processed_files:
                            archive_dir = Path(json_output_dir) / archive_directory_name
                            archive_dir.mkdir(exist_ok=True)
                            
                            try:
                                # Archive the file by moving it to the processed directory
                                json_file.rename(archive_dir / json_file.name)
                                logger.info(f"Moved {json_file.name} to archive directory")
                            except Exception as e:
                                logger.error(f"Error archiving {json_file}: {e}")
                    else:
                        logger.error(f"Failed to create calendar event from {json_file}")
                        error_count += 1
                    
            except Exception as e:
                logger.error(f"Error processing calendar event file {json_file}: {e}")
                logger.error(traceback.format_exc())
                error_count += 1
                
        # Close database connections after processing all files
        close_all_connections()
                
        return success_count, error_count
        
    except Exception as e:
        logger.error(f"Error in process_calendar_event_files: {e}")
        logger.error(traceback.format_exc())
        
        # Ensure connections are closed on error
        try:
            close_all_connections()
        except:
            pass
            
        return 0, 0

# === Main Pipeline Implementation ===
def run_pipeline():
    """
    Run the main Voice Calendar pipeline:
    1. Download audio files from Google Drive
    2. Transcribe the downloaded audio files 
    3. Parse transcriptions to extract calendar events
    4. Insert events into Google Calendar
    5. Delete processed files to prevent duplication
    """
    state = {"last_run_time": datetime.now().isoformat()}
    
    try:
        # Step 1: Download files from Google Drive
        logger.info("Starting file download from Google Drive")
        download_files_main()
        logger.info("Completed file download from Google Drive")
        
        # Step 2: Transcribe downloaded audio files
        logger.info("Starting transcription of audio files")
        run_transcribe()
        logger.info("Completed transcription of audio files")
        
        # Step 3: Parse transcriptions to extract calendar events
        logger.info("Starting parsing of transcriptions for calendar events")
        parse_calendar_entries()
        logger.info("Completed parsing of transcriptions for calendar events")
        
        # Step 4: Process calendar events and insert into Google Calendar
        logger.info("Starting insertion of events into Google Calendar")
        success_count, error_count = process_calendar_event_files()
        logger.info(f"Calendar event processing completed: {success_count} events created, {error_count} errors")
        
        # Step 5: Delete processed files to prevent duplication
        logger.info("Starting deletion of processed files")
        try:
            delete_files_main()
            logger.info("Completed deletion of processed files")
        except Exception as e:
            logger.error(f"Error deleting processed files: {e}")
            logger.error(traceback.format_exc())
        
        state["last_run_status"] = "success"
        state["events_created"] = success_count
        state["events_failed"] = error_count
        logger.info("Pipeline execution completed successfully")
    except Exception as e:
        state["last_run_status"] = "failed"
        state["error"] = str(e)
        logger.error(f"Pipeline execution failed: {e}")
        logger.error(traceback.format_exc())
    
    # Update state file
    update_pipeline_state(STATE_FILE, state)
    return state["last_run_status"] == "success"

# === Calendar Event Summary Task ===
def run_calendar_summary_task():
    """
    Run the calendar summary task:
    1. Retrieve calendar events from the database
    2. Format them as an email
    3. Send email with the events
    """
    try:
        # Initialize database connection
        logger.info("Initializing database connection for calendar summary")
        initialize_db()
        
        # Retrieve calendar events from the database
        logger.info("Retrieving calendar events from database")
        events = get_calendar_events_by_config_interval()
        
        if not events:
            logger.warning("No calendar events found for the configured time interval")
            return False
            
        # Format events for email
        logger.info(f"Formatting {len(events)} calendar events for email")
        email_content = format_events_for_email(events)
        
        # Update email config with events
        try:
            # Load the email config - try multiple possible paths
            possible_email_config_paths = [
                PROJECT_ROOT / "project_modules_configs" / "config_send_email" / "email_config.json",
                Path(__file__).parent.parent / "project_modules_configs" / "config_send_email" / "email_config.json",
                Path().absolute() / "src" / "voice_calender" / "project_modules_configs" / "config_send_email" / "email_config.json",
                Path().absolute().parent / "src" / "voice_calender" / "project_modules_configs" / "config_send_email" / "email_config.json"
            ]
            
            email_config_path = None
            for path in possible_email_config_paths:
                if path.exists():
                    email_config_path = path
                    logger.info(f"Found email config at: {email_config_path}")
                    break
            
            if not email_config_path:
                logger.error(f"Email config file not found. Tried paths: {[str(p) for p in possible_email_config_paths]}")
                return False
                
            with open(email_config_path, 'r', encoding='utf-8') as f:
                email_config = json.load(f)
            
            # Update the email message with the events content
            if 'email' in email_config:
                today = datetime.now().strftime("%Y-%m-%d")
                email_config['email']['subject'] = f"Voice Calendar Events Summary for {today}"
                email_config['email']['message'] = email_content
                
                # Save the updated config
                with open(email_config_path, 'w', encoding='utf-8') as f:
                    json.dump(email_config, f, indent=2)
                
                logger.info("Updated email message with calendar events")
            else:
                logger.warning("Email configuration doesn't contain 'email' section")
                return False
        except Exception as e:
            logger.error(f"Error updating email config: {e}")
            logger.error(traceback.format_exc())
            return False
        
        # Send email
        logger.info("Starting email sending process")
        send_email_main()
        logger.info("Completed email sending process")
        
        # Clean up database connections
        close_all_connections()
        
        return True
    except Exception as e:
        logger.error(f"Calendar summary task failed: {e}")
        logger.error(traceback.format_exc())
        # Ensure connections are closed even on error
        try:
            close_all_connections()
        except:
            pass
        return False

def format_events_for_email(events):
    """
    Format calendar events for email content
    
    Args:
        events (list): List of calendar event records from database
        
    Returns:
        str: Formatted email content
    """
    if not events:
        return "No upcoming calendar events found."
    
    formatted_content = "Upcoming Calendar Events:\n\n"
    
    for i, event in enumerate(events, 1):
        # Extract event details
        summary = event.get('summary', 'Untitled Event')
        
        # Format start date/time - using the correct field name from db_manager.py
        start_datetime = event.get('start_datetime')
        if not start_datetime:
            # Try alternate field name based on db_manager.py
            start_datetime = event.get('start_dateTime')
            
        if start_datetime:
            try:
                # If the format is ISO 8601 with a T separator
                if 'T' in start_datetime:
                    dt_parts = start_datetime.split('T')
                    date_part = dt_parts[0]
                    time_part = dt_parts[1].split('+')[0].split('Z')[0]  # Remove timezone if present
                    start_formatted = f"{date_part} at {time_part}"
                else:
                    # Just use as is if not in expected format
                    start_formatted = start_datetime
            except:
                start_formatted = start_datetime
        else:
            start_formatted = "No start time"
        
        # Format location
        location = event.get('location')
        location_text = f"\nLocation: {location}" if location else ""
        
        # Format description (truncate if too long)
        description = event.get('description')
        if description and len(description) > 100:
            description = description[:97] + "..."
        description_text = ""#####################################f"\nDetails: {description}" if description else ""
        
        # Add event to content
        formatted_content += f"{i}. {summary}\n   When: {start_formatted}{location_text}{description_text}\n\n"
    
    # Add footer
    formatted_content += "\nThis email was automatically generated by Voice Calendar.\n"
    
    return formatted_content

# === Future Tasks Scheduler ===
def calculate_seconds_until_daily_task():
    """Calculate seconds until the configured daily task time."""
    now = datetime.now()
    
    # Get time from config, or use fallback values
    config = load_config()
    scheduler_config = config.get("scheduler", {})
    hour = scheduler_config.get("daily_task_hour", 23)
    minute = scheduler_config.get("daily_task_minute", 55)
    
    target_today = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    
    # If it's already past the target time, schedule for tomorrow
    if now >= target_today:
        target_today = target_today + timedelta(days=1)
        
    seconds_until_target = (target_today - now).total_seconds()
    return seconds_until_target

def future_tasks_scheduler():
    """
    Runs the calendar summary task at the scheduled time each day.
    This function runs in an infinite loop in a separate thread.
    It calculates the time until the next scheduled run, sleeps until then,
    and sends the calendar events summary by email.
    """
    while True:
        sleep_time = calculate_seconds_until_daily_task()
        next_run_time = datetime.now() + timedelta(seconds=sleep_time)
        logger.info(f"Next calendar summary task scheduled in {sleep_time:.0f} seconds (at {next_run_time.strftime('%Y-%m-%d %H:%M:%S')})")
        
        time.sleep(sleep_time)
        logger.info("Starting calendar summary task")
        success = run_calendar_summary_task()
        
        if success:
            logger.info("Calendar summary task completed successfully")
        else:
            logger.error("Calendar summary task failed")

# === Setup Logging ===
def setup_logging():
    """Configure logging with console and file handlers."""
    log_file = LOG_DIR / 'app_calender_scheduler.log'
    
    # Create handlers
    console_handler = logging.StreamHandler()
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=1024*1024,  # 1MB
        backupCount=5
    )
    
    # Create formatters
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Set logger level
    logger.setLevel(logging.INFO)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    logger.info(f"Logging to: {log_file}")

# === Main Scheduler ===
def main():
    """Main function to run the Voice Calendar scheduler."""
    # Setup logging
    setup_logging()
    logger.info("Voice Calendar Scheduler starting up")

    try:
        # Log the configuration path being used
        logger.info(f"Using configuration file at: {CONFIG_FILE}")
        
        # Load and validate configuration
        config = load_config()
        validate_config(config)
        interval = calculate_interval_seconds(config["scheduler"]["runs_per_day"])
        
        # Log configuration
        runs_per_day = config["scheduler"]["runs_per_day"]
        logger.info(f"Configuration loaded: {runs_per_day} runs per day (every {interval//60} minutes)")

        # Start future tasks scheduler in parallel thread
        future_tasks_thread = threading.Thread(target=future_tasks_scheduler, daemon=True)
        future_tasks_thread.start()
        
        # Log with the actual configured time
        hour = config["scheduler"].get("daily_task_hour", 23)
        minute = config["scheduler"].get("daily_task_minute", 55)
        logger.info(f"Started future tasks scheduler thread (runs at {hour:02d}:{minute:02d} daily)")

        if interval == 0:
            # Run once mode
            logger.info("Main pipeline: Running once and exiting")
            run_pipeline()
        else:
            # Main loop for recurring execution
            logger.info("Main pipeline: Running in continuous mode")
            while True:
                logger.info("Starting main pipeline execution")
                run_pipeline()
                next_run = calculate_next_run_time(interval)
                logger.info(f"Next main pipeline run at: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
                time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Scheduler interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()
