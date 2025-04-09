# Database Utilities for Voice Calendar

## Overview

This module provides database management for the Voice Calendar application. It includes:

- Connection pooling
- Schema management
- CRUD operations for calendar events
- Date-range based queries
- Migration utilities

## Recent Enhancements

### Schema Improvements

The database schema now uses proper PostgreSQL types:

- `start_dateTime` and `end_dateTime` are now `TIMESTAMP WITH TIME ZONE` instead of `TEXT`
- Proper `NOT NULL` constraints on required fields
- `created_at` timestamp field added for record keeping

### Index Enhancements

Additional indexes have been added for better query performance:

- `idx_calendar_events_end_datetime` - Index on end_dateTime
- `idx_calendar_events_date_range` - Composite index on (start_dateTime, end_dateTime)

These indexes improve performance for:
- Date range queries
- Lookup by end dates
- Sorting operations

### Migration Utility

A migration script (`migrate_timestamp_schema.py`) has been provided to safely upgrade existing databases:

1. It checks if migration is needed
2. Creates temporary columns
3. Converts data from TEXT to TIMESTAMP
4. Adds constraints and indexes
5. Handles errors with transactions

To run the migration:

```bash
python -m voice_calender.db_utils.migrate_timestamp_schema
```

## Configuration

Database configuration is loaded from:
1. `db_utils_config.json` file
2. Environment variables
3. Default fallbacks

## Usage Examples

```python
# Initialize the database
from voice_calender.db_utils.db_manager import initialize_db
initialize_db()

# Store an event
from voice_calender.db_utils.db_manager import save_calendar_event
from datetime import datetime, timedelta

event_id = save_calendar_event(
    summary="Team Meeting",
    start_datetime=datetime.now(),
    end_datetime=datetime.now() + timedelta(hours=1),
    location="Conference Room"
)

# Retrieve events for date range
from voice_calender.db_utils.db_manager import get_events_by_date_range
events = get_events_by_date_range(
    start_date="2023-05-01",
    end_date="2023-05-31"
)
``` 