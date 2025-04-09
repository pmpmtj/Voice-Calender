#!/usr/bin/env python3
"""
Helper functions for saving calendar events with flexible data validation.

This module provides helper functions for validating and saving calendar events,
with special handling for incomplete or inconsistent data.
"""

import logging
import json
from datetime import datetime, timedelta
from voice_calender.db_utils.db_manager import save_calendar_event

logger = logging.getLogger(__name__)

def validate_and_complete_event(event_data):
    """
    Validate event data and attempt to complete missing fields
    
    Args:
        event_data (dict): The calendar event data to validate
        
    Returns:
        tuple: (bool, dict, str) - (is_valid, completed_data, error_message)
    """
    # Make a copy to avoid modifying the original
    data = event_data.copy() if event_data else {}
    
    # Check for minimum required data
    if not data:
        return False, None, "Event data is empty"
    
    # Check for summary
    if not data.get('summary'):
        # Try to generate a summary if possible
        if data.get('description'):
            # Use first line or first 30 chars of description
            summary_text = data['description'].strip().split('\n')[0][:30]
            data['summary'] = summary_text
            logger.info(f"Generated summary from description: {summary_text}")
        else:
            # Create a placeholder summary with timestamp
            ts = datetime.now().strftime("%Y-%m-%d %H:%M")
            data['summary'] = f"Calendar Event {ts}"
            logger.info(f"Created placeholder summary: {data['summary']}")
    
    # Process start date/time
    start_data = data.get('start', {})
    start_datetime = start_data.get('dateTime') or start_data.get('date')
    
    if not start_datetime:
        # No start time specified, use current time
        now = datetime.now()
        data['start'] = data.get('start', {})
        data['start']['dateTime'] = now.isoformat()
        logger.info(f"Using current time for missing start time: {data['start']['dateTime']}")
    
    # Process end date/time
    end_data = data.get('end', {})
    end_datetime = end_data.get('dateTime') or end_data.get('date')
    
    if not end_datetime:
        # No end time, create one based on start
        data['end'] = data.get('end', {})
        
        # Use start date/time as basis
        if 'dateTime' in data.get('start', {}):
            # Parse the start time and add 1 hour
            try:
                start_dt = data['start']['dateTime']
                
                # Handle different formats
                if 'T' in start_dt:
                    if '+' in start_dt:
                        dt_part, tz_part = start_dt.split('+', 1)
                        tz_info = '+' + tz_part
                    elif 'Z' in start_dt:
                        dt_part = start_dt.replace('Z', '')
                        tz_info = 'Z'
                    else:
                        dt_part = start_dt
                        tz_info = ''
                    
                    # Parse datetime
                    dt_obj = datetime.fromisoformat(dt_part.replace('Z', ''))
                    
                    # Add duration
                    end_dt = dt_obj + timedelta(hours=1)
                    
                    # Format back to ISO
                    data['end']['dateTime'] = end_dt.isoformat() + tz_info
                    logger.info(f"Created end time 1 hour after start: {data['end']['dateTime']}")
                else:
                    # Just copy start time to end time if format is unknown
                    data['end']['dateTime'] = data['start']['dateTime']
            except (ValueError, TypeError) as e:
                logger.warning(f"Error calculating end time: {e}")
                # Fallback: just use the same value
                data['end']['dateTime'] = data['start']['dateTime']
        elif 'date' in data.get('start', {}):
            # For all-day events, use the same date
            data['end']['date'] = data['start']['date']
    
    return True, data, ""

def save_event_flexible(event_data):
    """
    Save calendar event with flexible validation
    
    Args:
        event_data (dict): Calendar event data
        
    Returns:
        int or None: Database ID of the saved event, or None if save failed
    """
    try:
        # Validate and complete event data
        valid, completed_data, error = validate_and_complete_event(event_data)
        
        if not valid:
            logger.error(f"Invalid event data: {error}")
            return None
        
        # Extract fields needed for database
        summary = completed_data.get('summary', '')
        
        # Extract start date/time
        start_data = completed_data.get('start', {})
        start_datetime = start_data.get('dateTime') or start_data.get('date') or ''
        start_timezone = start_data.get('timeZone')
        
        # Extract end date/time
        end_data = completed_data.get('end', {})
        end_datetime = end_data.get('dateTime') or end_data.get('date') or ''
        end_timezone = end_data.get('timeZone')
        
        # Extract other optional fields
        location = completed_data.get('location')
        description = completed_data.get('description')
        attendees = completed_data.get('attendees')
        recurrence = completed_data.get('recurrence')
        reminders = completed_data.get('reminders')
        visibility = completed_data.get('visibility')
        color_id = completed_data.get('colorId')
        transparency = completed_data.get('transparency')
        status = completed_data.get('status')
        
        # Save to database
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
            logger.info(f"Successfully saved event to database with ID: {event_id}")
        else:
            logger.error("Failed to save event to database")
        
        return event_id
    except Exception as e:
        logger.error(f"Error in save_event_flexible: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None 