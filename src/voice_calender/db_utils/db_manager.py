import logging
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import json

from voice_calender.db_utils.db_config import get_db_url

# Ensure logging is configured
logger = logging.getLogger(__name__)

# Connection pool for reusing database connections
connection_pool = None

def initialize_db():
    """Initialize database and create necessary tables if they don't exist"""
    global connection_pool

    try:
        # Initialize connection pool
        db_url = get_db_url()
        connection_pool = pool.SimpleConnectionPool(1, 10, db_url)
        
        # Create tables
        create_tables()
        logger.info("Database initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        return False

def get_connection():
    """Get a connection from the pool"""
    global connection_pool
    
    if connection_pool is None:
        initialize_db()
    
    return connection_pool.getconn()

def return_connection(conn):
    """Return a connection to the pool"""
    global connection_pool
    
    if connection_pool is not None:
        connection_pool.putconn(conn)

def create_tables():
    """Create necessary tables if they don't exist"""
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # Create calendar_events table with relaxed constraints
        cur.execute("""
        CREATE TABLE IF NOT EXISTS calendar_events (
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
            status TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Create index on calendar_events.start_dateTime for faster date-based queries
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_calendar_events_start_datetime ON calendar_events(start_dateTime)
        """)
        
        # Add index on calendar_events.end_dateTime for faster range queries
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_calendar_events_end_datetime ON calendar_events(end_dateTime)
        """)
        
        # Create a composite index for date range queries
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_calendar_events_date_range ON calendar_events(start_dateTime, end_dateTime)
        """)
        
        conn.commit()
        logger.info("Database tables created successfully")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error creating tables: {str(e)}")
        raise
    finally:
        if conn:
            return_connection(conn)

def close_all_connections():
    """Close all database connections"""
    global connection_pool
    
    if connection_pool:
        connection_pool.closeall()
        connection_pool = None
        logger.info("All database connections closed")

def save_calendar_event(summary, start_datetime, end_datetime, location=None, description=None, 
                       start_timezone=None, end_timezone=None, attendees=None, recurrence=None, 
                       reminders=None, visibility=None, color_id=None, transparency=None, status=None):
    """
    Save a calendar event to the database
    
    Args:
        summary (str): Event summary/title
        start_datetime (str): Start date and time in ISO format
        end_datetime (str): End date and time in ISO format
        location (str, optional): Event location
        description (str, optional): Event description
        start_timezone (str, optional): Timezone for the start time
        end_timezone (str, optional): Timezone for the end time
        attendees (list, optional): List of attendees as dicts
        recurrence (list/str, optional): Recurrence rules 
        reminders (dict, optional): Reminder configuration
        visibility (str, optional): Event visibility
        color_id (str, optional): Color identifier
        transparency (str, optional): Whether event blocks time
        status (str, optional): Event status
        
    Returns:
        int: ID of the inserted record or None if error
    """
    conn = None
    event_id = None
    
    # Convert complex objects to JSON strings
    if attendees and isinstance(attendees, list):
        attendees = json.dumps(attendees)
        
    if recurrence and isinstance(recurrence, list):
        recurrence = json.dumps(recurrence)
        
    if reminders and isinstance(reminders, dict):
        reminders = json.dumps(reminders)
    
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # The database now expects timestamp values
        # If inputs are ISO strings, psycopg2 will handle the conversion
        # If they're already datetime objects, they'll work as is
        
        # Insert event
        cur.execute("""
        INSERT INTO calendar_events 
        (summary, location, description, start_dateTime, start_timeZone, 
        end_dateTime, end_timeZone, attendees, recurrence, reminders,
        visibility, colorId, transparency, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """, (summary, location, description, start_datetime, start_timezone, 
              end_datetime, end_timezone, attendees, recurrence, reminders,
              visibility, color_id, transparency, status))
        
        event_id = cur.fetchone()[0]
        
        conn.commit()
        logger.info(f"Saved calendar event with ID: {event_id}")
        return event_id
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error saving calendar event: {str(e)}")
        return None
    finally:
        if conn:
            return_connection(conn)

def get_events_by_date_range(start_date, end_date, limit=50):
    """
    Retrieve calendar events within a date range
    
    Args:
        start_date (str): Start date in ISO format
        end_date (str): End date in ISO format
        limit (int, optional): Maximum number of records to return
        
    Returns:
        list: List of calendar event records as dictionaries
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
        SELECT *
        FROM calendar_events
        WHERE start_dateTime >= %s AND start_dateTime <= %s
        ORDER BY start_dateTime ASC
        LIMIT %s
        """, (start_date, end_date, limit))
        
        results = cur.fetchall()
        return results
    except Exception as e:
        logger.error(f"Error retrieving calendar events by date range: {str(e)}")
        return []
    finally:
        if conn:
            return_connection(conn)

def get_upcoming_events(limit=10):
    """
    Retrieve upcoming calendar events
    
    Args:
        limit (int, optional): Maximum number of records to return
        
    Returns:
        list: List of calendar event records as dictionaries
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
        SELECT *
        FROM calendar_events
        WHERE start_dateTime >= CURRENT_TIMESTAMP
        ORDER BY start_dateTime ASC
        LIMIT %s
        """, (limit,))
        
        results = cur.fetchall()
        return results
    except Exception as e:
        logger.error(f"Error retrieving upcoming events: {str(e)}")
        return []
    finally:
        if conn:
            return_connection(conn)

def get_calendar_events_by_config_interval():
    """
    Retrieve calendar events from the database based on the date interval 
    and limit configured in db_utils_config.json
    
    Returns:
        list: List of calendar event records as dictionaries
    """
    conn = None
    try:
        # Load the configuration to get the date interval
        from pathlib import Path
        import json
        from datetime import datetime, timedelta
        
        # Get the config path
        db_utils_dir = Path(__file__).parent
        config_path = db_utils_dir / 'db_utils_config' / 'db_utils_config.json'
        
        # Load the config
        if not config_path.exists():
            logger.error(f"Config file not found at {config_path}")
            return []
            
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Get the date interval from config
        date_interval = config.get('calender_date_interval', [])
        
        # Default to today's date if interval is empty or invalid
        if not date_interval or len(date_interval) < 2:
            today = datetime.now().strftime("%Y-%m-%d")
            logger.info(f"Using default date (today): {today}")
            start_date = today
            end_date = today
        else:
            start_date = date_interval[0]
            end_date = date_interval[1]
        
        # Get the query limit from config (default to 100 if not specified)
        query_limit = config.get('query_limit', 100)
        
        # Ensure dates have the right format for comparison with TIMESTAMP fields
        # If only dates are specified (no time), append start/end of day time
        if isinstance(start_date, str) and len(start_date) == 10:  # YYYY-MM-DD format
            start_date = f"{start_date}T00:00:00"
        
        if isinstance(end_date, str) and len(end_date) == 10:  # YYYY-MM-DD format
            end_date = f"{end_date}T23:59:59"
        
        # Get the database connection
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query events within the date range
        # psycopg2 will handle the conversion of ISO strings to proper timestamp values
        cur.execute("""
        SELECT *
        FROM calendar_events
        WHERE start_dateTime >= %s AND start_dateTime <= %s
        ORDER BY start_dateTime ASC
        LIMIT %s
        """, (start_date, end_date, query_limit))
        
        results = cur.fetchall()
        
        logger.info(f"Retrieved {len(results)} calendar events between {start_date} and {end_date} (limit: {query_limit})")
        return results
        
    except Exception as e:
        logger.error(f"Error retrieving calendar events by config interval: {str(e)}")
        logger.error(f"Detail: {type(e).__name__}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return []
    finally:
        if conn:
            return_connection(conn)

def update_calendar_event(event_id, **kwargs):
    """
    Update a calendar event
    
    Args:
        event_id (int): ID of the event to update
        **kwargs: Fields to update
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    
    # Process complex objects
    if 'attendees' in kwargs and isinstance(kwargs['attendees'], list):
        kwargs['attendees'] = json.dumps(kwargs['attendees'])
        
    if 'recurrence' in kwargs and isinstance(kwargs['recurrence'], list):
        kwargs['recurrence'] = json.dumps(kwargs['recurrence'])
        
    if 'reminders' in kwargs and isinstance(kwargs['reminders'], dict):
        kwargs['reminders'] = json.dumps(kwargs['reminders'])
    
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # Build the update query dynamically based on provided fields
        fields = []
        values = []
        
        for key, value in kwargs.items():
            fields.append(f"{key} = %s")
            values.append(value)
            
        if not fields:
            logger.warning("No fields to update")
            return False
            
        values.append(event_id)  # For the WHERE clause
        
        query = f"""
        UPDATE calendar_events
        SET {", ".join(fields)}
        WHERE id = %s
        """
        
        cur.execute(query, values)
        
        rows_affected = cur.rowcount
        conn.commit()
        
        logger.info(f"Updated calendar event ID {event_id}, {rows_affected} rows affected")
        return rows_affected > 0
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error updating calendar event: {str(e)}")
        return False
    finally:
        if conn:
            return_connection(conn)

def delete_calendar_event(event_id):
    """
    Delete a calendar event
    
    Args:
        event_id (int): ID of the event to delete
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute("""
        DELETE FROM calendar_events
        WHERE id = %s
        """, (event_id,))
        
        rows_affected = cur.rowcount
        conn.commit()
        
        logger.info(f"Deleted calendar event ID {event_id}, {rows_affected} rows affected")
        return rows_affected > 0
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error deleting calendar event: {str(e)}")
        return False
    finally:
        if conn:
            return_connection(conn)
