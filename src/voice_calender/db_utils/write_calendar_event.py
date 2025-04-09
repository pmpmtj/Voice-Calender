#!/usr/bin/env python3
"""
Calendar Event Writer

A simple script with functions to write calendar event data to the database.
"""

import json
import logging
from typing import Dict, Any, Optional, Union, List

from voice_calender.db_utils.db_manager import save_calendar_event, initialize_db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def write_event_to_db(event_data: Dict[str, Any]) -> Optional[int]:
    """
    Write a calendar event to the database
    
    Args:
        event_data: Dictionary containing event data in Google Calendar format
            {
              "summary": "Meeting title",
              "location": "Location information",
              "description": "Meeting details",
              "start": {
                "dateTime": "2025-04-15T10:00:00-07:00",
                "timeZone": "America/Los_Angeles"
              },
              "end": {
                "dateTime": "2025-04-15T11:00:00-07:00",
                "timeZone": "America/Los_Angeles"
              },
              "attendees": [
                {
                  "email": "person@example.com",
                  "displayName": "Person Name"
                }
              ],
              "recurrence": [
                "RRULE:FREQ=WEEKLY;BYDAY=MO"
              ],
              "reminders": {
                "useDefault": false,
                "overrides": [
                  {
                    "method": "popup",
                    "minutes": 10
                  }
                ]
              },
              "visibility": "private",
              "colorId": "7",
              "transparency": "opaque",
              "status": "confirmed"
            }
    
    Returns:
        int: ID of the inserted record or None if error
    """
    # Extract fields from the event data
    summary = event_data.get('summary')
    location = event_data.get('location')
    description = event_data.get('description')
    
    # Handle start and end times
    start_datetime = None
    start_timezone = None
    if 'start' in event_data:
        start_datetime = event_data['start'].get('dateTime')
        start_timezone = event_data['start'].get('timeZone')
    
    end_datetime = None
    end_timezone = None
    if 'end' in event_data:
        end_datetime = event_data['end'].get('dateTime')
        end_timezone = event_data['end'].get('timeZone')
    
    # Handle complex fields
    attendees = event_data.get('attendees')
    if attendees and isinstance(attendees, list):
        attendees = json.dumps(attendees)
        
    recurrence = event_data.get('recurrence')
    if recurrence and isinstance(recurrence, list):
        recurrence = json.dumps(recurrence)
        
    reminders = event_data.get('reminders')
    if reminders and isinstance(reminders, dict):
        reminders = json.dumps(reminders)
    
    # Other fields
    visibility = event_data.get('visibility')
    color_id = event_data.get('colorId')
    transparency = event_data.get('transparency')
    status = event_data.get('status')
    
    # Save the event to the database
    event_id = save_calendar_event(
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
    
    if event_id:
        logger.info(f"Successfully wrote event '{summary}' to database with ID: {event_id}")
    else:
        logger.error(f"Failed to write event '{summary}' to database")
    
    return event_id

def write_events_to_db(events_list: List[Dict[str, Any]]) -> List[int]:
    """
    Write multiple calendar events to the database
    
    Args:
        events_list: List of dictionaries containing event data
    
    Returns:
        List of event IDs that were successfully inserted
    """
    event_ids = []
    
    for event_data in events_list:
        event_id = write_event_to_db(event_data)
        if event_id:
            event_ids.append(event_id)
    
    logger.info(f"Successfully wrote {len(event_ids)} out of {len(events_list)} events to database")
    return event_ids

def main():
    """Example usage of the calendar event writer"""
    # Initialize database if needed
    initialize_db()
    
    # Example event data
    event_data = {
        "summary": "Team Meeting",
        "location": "Conference Room 1",
        "description": "Weekly team meeting to discuss progress",
        "start": {
            "dateTime": "2025-04-15T10:00:00-07:00",
            "timeZone": "America/Los_Angeles"
        },
        "end": {
            "dateTime": "2025-04-15T11:00:00-07:00",
            "timeZone": "America/Los_Angeles"
        },
        "attendees": [
            {
                "email": "team.member@example.com",
                "displayName": "Team Member"
            },
            {
                "email": "manager@example.com",
                "displayName": "Manager"
            }
        ],
        "recurrence": [
            "RRULE:FREQ=WEEKLY;BYDAY=TU"
        ],
        "reminders": {
            "useDefault": False,
            "overrides": [
                {
                    "method": "popup",
                    "minutes": 15
                }
            ]
        },
        "visibility": "private",
        "colorId": "5",
        "transparency": "opaque",
        "status": "confirmed"
    }
    
    # Write the event to the database
    event_id = write_event_to_db(event_data)
    print(f"Wrote event to database with ID: {event_id}")

if __name__ == "__main__":
    main() 