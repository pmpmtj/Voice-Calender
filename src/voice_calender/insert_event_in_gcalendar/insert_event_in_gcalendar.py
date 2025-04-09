#!/usr/bin/env python3
"""
Module for inserting events into Google Calendar using the Google Calendar API.
Requires credentials to be stored in gcalendar_credentials.json.
"""

import os
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Initialize paths - handling both frozen (PyInstaller) and regular Python execution
if getattr(sys, 'frozen', False):
    # Running as compiled executable
    SCRIPT_DIR = Path(sys._MEIPASS)
else:
    # Running as script
    SCRIPT_DIR = Path(__file__).parent.absolute()

# Project root for path calculations
PROJECT_ROOT = SCRIPT_DIR.parent
WORKSPACE_ROOT = PROJECT_ROOT.parent.parent  # Going up to the Voice-Calendar directory

# Configuration paths
CONFIG_DIR = PROJECT_ROOT / "project_modules_configs" / "config_insert_event_in_gcalendar"
CONFIG_PATH = CONFIG_DIR / "insert_event_in_gcalendar_config.json"

# Initialize logger
logger = logging.getLogger("insert_event_in_gcalendar")

# Default credentials path (fallback if not specified in config)
DEFAULT_CREDENTIALS_DIR = CONFIG_DIR / "credentials_gcalendar"
DEFAULT_CREDENTIALS_PATH = DEFAULT_CREDENTIALS_DIR / "gcalendar_credentials.json"

# Load configuration and get credentials path
def load_config():
    """Load configuration from JSON file and return credentials path"""
    try:
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                config = json.load(f)
                
            # Check if credentials_path is specified in config
            if 'credentials_path' in config:
                credentials_path = config['credentials_path']
                # Handle relative paths
                if not os.path.isabs(credentials_path):
                    credentials_path = Path(WORKSPACE_ROOT / credentials_path).resolve()
                else:
                    credentials_path = Path(credentials_path)
                
                logger.info(f"Using credentials path from config: {credentials_path}")
                return str(credentials_path)
        
        # If config doesn't exist or doesn't specify credentials_path, use default
        logger.info(f"Using default credentials path: {DEFAULT_CREDENTIALS_PATH}")
        return str(DEFAULT_CREDENTIALS_PATH)
    
    except Exception as e:
        logger.error(f"Error loading configuration: {str(e)}")
        logger.info(f"Falling back to default credentials path: {DEFAULT_CREDENTIALS_PATH}")
        return str(DEFAULT_CREDENTIALS_PATH)

# Get the credentials path from config
CREDENTIALS_PATH = load_config()

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/calendar']  # Full access only

class GoogleCalendarManager:
    """Class to manage Google Calendar operations."""
    
    def __init__(self, credentials_path: str = None):
        """
        Initialize the calendar manager.
        
        Args:
            credentials_path: Path to the credentials JSON file
        """
        self.credentials_path = credentials_path or CREDENTIALS_PATH
        self.creds = None
        self.service = None
        logger.info(f"GoogleCalendarManager initialized with credentials path: {self.credentials_path}")
        
    def authenticate(self) -> None:
        """Authenticate with Google Calendar API."""
        # Make sure the credentials file exists
        if not os.path.exists(self.credentials_path):
            error_msg = f"Credentials file not found at: {self.credentials_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
            
        token_path = os.path.join(os.path.dirname(self.credentials_path), 'token.json')
        
        if os.path.exists(token_path):
            self.creds = Credentials.from_authorized_user_file(token_path, SCOPES)
            
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path, SCOPES)
                self.creds = flow.run_local_server(port=0)
                
            # Save the credentials for the next run
            with open(token_path, 'w') as token:
                token.write(self.creds.to_json())
                
        self.service = build('calendar', 'v3', credentials=self.creds)
        
    def insert_event(self, event_data: Dict) -> Optional[Dict]:
        """
        Insert an event into Google Calendar.
        
        Args:
            event_data: Dictionary containing event details
                Required fields:
                - summary: Event title
                - start: Event start time (datetime or date)
                - end: Event end time (datetime or date)
                Optional fields:
                - description: Event description
                - location: Event location
                - attendees: List of attendee email addresses
                
        Returns:
            Dict containing the created event details if successful, None if failed
        """
        try:
            if not self.service:
                self.authenticate()
                
            event = self.service.events().insert(
                calendarId='primary',
                body=event_data
            ).execute()
            
            print(f'Event created: {event.get("htmlLink")}')
            return event
            
        except HttpError as error:
            print(f'An error occurred: {error}')
            return None

def main():
    """Example usage of the GoogleCalendarManager class."""
    # Create calendar manager instance with the path from config
    calendar_manager = GoogleCalendarManager()
    
    # Example event data - using Lisbon timezone
    event = {
        'summary': 'Test Event',
        'description': 'This is a test event created via the API',
        'start': {
            'dateTime': '2025-04-07T21:00:00',
            'timeZone': 'Europe/Lisbon',
        },
        'end': {
            'dateTime': '2025-04-07T21:30:00',
            'timeZone': 'Europe/Lisbon',
        }
    }
    
    # Insert the event
    result = calendar_manager.insert_event(event)
    if result:
        print('Event was successfully created!')

if __name__ == '__main__':
    main()
