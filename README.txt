# Voice Calendar

A calendar application that uses voice input to create and manage calendar events.

## Features

- Create calendar events using voice commands
- Store events in a PostgreSQL database
- Retrieve upcoming events
- Search for events by date range

## Installation

1. Clone this repository
2. Create a virtual environment:
   ```
   python -m venv .venv
   .venv\Scripts\activate  # On Windows
   ```
3. Install the package in development mode:
   ```
   pip install -e .
   ```

## Usage

### Writing events to the database

```python
from voice_calender.db_utils.write_calendar_event import write_event_to_db

# Example event data
event_data = {
    "summary": "Team Meeting",
    "location": "Conference Room 1",
    "description": "Weekly team meeting to discuss progress",
    "start": {
        "dateTime": "2023-04-15T10:00:00-07:00",
        "timeZone": "America/Los_Angeles"
    },
    "end": {
        "dateTime": "2023-04-15T11:00:00-07:00",
        "timeZone": "America/Los_Angeles"
    },
    "attendees": [
        {
            "email": "team.member@example.com",
            "displayName": "Team Member"
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
    }
}

# Write to database
event_id = write_event_to_db(event_data)
```

### Retrieving events

```python
from voice_calender.db_utils.db_manager import get_upcoming_events, get_events_by_date_range

# Get upcoming events
upcoming = get_upcoming_events(5)  # Get next 5 upcoming events

# Get events within a date range
events = get_events_by_date_range("2023-04-01", "2023-04-30")
```

## Database Schema

The application uses a PostgreSQL database with the following schema:

```sql
CREATE TABLE calendar_events (
    id SERIAL PRIMARY KEY,
    summary TEXT,
    location TEXT,
    description TEXT,
    start_dateTime TEXT,
    start_timeZone TEXT,
    end_dateTime TEXT,
    end_timeZone TEXT,
    attendees TEXT,         -- JSON string containing array of attendees
    recurrence TEXT,        -- JSON string or comma-separated RRULEs
    reminders TEXT,         -- JSON string containing reminder config
    visibility TEXT,
    colorId TEXT,
    transparency TEXT,
    status TEXT
);
```

## License

MIT License 