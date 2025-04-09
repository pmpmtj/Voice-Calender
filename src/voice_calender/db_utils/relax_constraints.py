#!/usr/bin/env python3
"""
Database Constraint Relaxation Script

This script modifies an existing database to:
1. Remove NOT NULL constraints from start_dateTime and end_dateTime columns
2. Convert TIMESTAMP WITH TIME ZONE columns back to TEXT
3. Preserve all existing data

This script helps when dealing with inconsistent or partial calendar event data.
"""

import logging
import sys
import psycopg2
from pathlib import Path

# Set up basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import from voice_calender package
from voice_calender.db_utils.db_config import get_db_url

def relax_constraints():
    """Relax database constraints to accommodate flexible data formats"""
    conn = None
    try:
        # Get database connection
        db_url = get_db_url()
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        logger.info("Starting database constraint relaxation...")
        
        # 1. Check if the table exists
        cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'calendar_events'
        )
        """)
        
        table_exists = cur.fetchone()[0]
        if not table_exists:
            logger.info("Table 'calendar_events' doesn't exist. No changes needed.")
            return True
        
        # 2. Begin transaction
        logger.info("Beginning transaction...")
        
        # 3. Check column types and constraints
        cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'calendar_events'
        AND (column_name = 'start_dateTime' OR column_name = 'end_dateTime')
        """)
        
        columns = cur.fetchall()
        needs_relaxation = False
        
        for column_name, data_type, is_nullable in columns:
            if data_type.lower() != 'text' or is_nullable.lower() != 'yes':
                needs_relaxation = True
                logger.info(f"Column {column_name} has constraints that will be relaxed.")
        
        if not needs_relaxation:
            logger.info("Database already has relaxed constraints. No changes needed.")
            return True
        
        # 4. Create temporary columns
        logger.info("Creating temporary TEXT columns...")
        cur.execute("""
        ALTER TABLE calendar_events 
        ADD COLUMN start_dateTime_new TEXT,
        ADD COLUMN end_dateTime_new TEXT
        """)
        
        # 5. Copy data with conversion to TEXT
        logger.info("Converting data to TEXT format...")
        cur.execute("""
        UPDATE calendar_events
        SET 
            start_dateTime_new = start_dateTime::TEXT,
            end_dateTime_new = end_dateTime::TEXT
        """)
        
        # 6. Drop old columns and rename new ones
        logger.info("Replacing columns with relaxed TEXT columns...")
        cur.execute("""
        ALTER TABLE calendar_events 
        DROP COLUMN start_dateTime,
        DROP COLUMN end_dateTime,
        RENAME COLUMN start_dateTime_new TO start_dateTime,
        RENAME COLUMN end_dateTime_new TO end_dateTime
        """)
        
        # 7. Recreate indexes
        logger.info("Recreating indexes...")
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_calendar_events_start_datetime ON calendar_events(start_dateTime);
        CREATE INDEX IF NOT EXISTS idx_calendar_events_end_datetime ON calendar_events(end_dateTime);
        CREATE INDEX IF NOT EXISTS idx_calendar_events_date_range ON calendar_events(start_dateTime, end_dateTime);
        """)
        
        # 8. Commit the transaction
        conn.commit()
        logger.info("Constraint relaxation completed successfully!")
        return True
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Constraint relaxation failed: {str(e)}")
        logger.error(f"Detail: {type(e).__name__}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        if conn:
            conn.close()

def main():
    """Main function to run the constraint relaxation"""
    success = relax_constraints()
    if success:
        print("Database constraint relaxation completed successfully.")
        sys.exit(0)
    else:
        print("Database constraint relaxation failed. Check the logs for details.")
        sys.exit(1)

if __name__ == "__main__":
    main() 