#!/usr/bin/env python3
"""
Database Migration Script for Timestamp Types

This script updates the schema of an existing database to:
1. Convert TEXT date columns to TIMESTAMP WITH TIME ZONE
2. Add NOT NULL constraints to critical date fields
3. Add additional indexes for performance
4. Add created_at timestamp

Run this only if you have an existing database with the old schema.
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

def migrate_schema():
    """Migrate the database schema to use proper timestamp fields"""
    conn = None
    try:
        # Get database connection
        db_url = get_db_url()
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        logger.info("Starting database schema migration...")
        
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
            logger.info("Table 'calendar_events' doesn't exist. No migration needed.")
            return True
        
        # 2. Begin transaction
        logger.info("Beginning transaction...")
        
        # 3. Check column types
        cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'calendar_events'
        AND (column_name = 'start_dateTime' OR column_name = 'end_dateTime')
        """)
        
        columns = cur.fetchall()
        needs_migration = False
        
        for column_name, data_type in columns:
            if data_type.lower() == 'text':
                needs_migration = True
                logger.info(f"Column {column_name} is currently TEXT type. Will be migrated.")
        
        if not needs_migration:
            logger.info("Date columns are already proper timestamp types. Skipping column migration.")
        else:
            # 4. Create temporary columns
            logger.info("Creating temporary timestamp columns...")
            cur.execute("""
            ALTER TABLE calendar_events 
            ADD COLUMN start_dateTime_new TIMESTAMP WITH TIME ZONE,
            ADD COLUMN end_dateTime_new TIMESTAMP WITH TIME ZONE
            """)
            
            # 5. Copy data with conversion
            logger.info("Converting data from TEXT to TIMESTAMP WITH TIME ZONE...")
            cur.execute("""
            UPDATE calendar_events
            SET 
                start_dateTime_new = start_dateTime::TIMESTAMP WITH TIME ZONE,
                end_dateTime_new = end_dateTime::TIMESTAMP WITH TIME ZONE
            """)
            
            # 6. Drop old columns and rename new ones
            logger.info("Replacing old columns with new timestamp columns...")
            cur.execute("""
            ALTER TABLE calendar_events 
            DROP COLUMN start_dateTime,
            DROP COLUMN end_dateTime,
            RENAME COLUMN start_dateTime_new TO start_dateTime,
            RENAME COLUMN end_dateTime_new TO end_dateTime
            """)
            
            # 7. Add NOT NULL constraints
            logger.info("Adding NOT NULL constraints...")
            cur.execute("""
            ALTER TABLE calendar_events
            ALTER COLUMN start_dateTime SET NOT NULL,
            ALTER COLUMN end_dateTime SET NOT NULL
            """)
            
        # 8. Add created_at column if not exists
        cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.columns 
            WHERE table_name = 'calendar_events' 
            AND column_name = 'created_at'
        )
        """)
        
        created_at_exists = cur.fetchone()[0]
        if not created_at_exists:
            logger.info("Adding created_at column...")
            cur.execute("""
            ALTER TABLE calendar_events
            ADD COLUMN created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            """)
        
        # 9. Check and create indexes
        # For end_dateTime
        cur.execute("""
        SELECT EXISTS (
            SELECT FROM pg_indexes 
            WHERE tablename = 'calendar_events' 
            AND indexname = 'idx_calendar_events_end_datetime'
        )
        """)
        
        end_index_exists = cur.fetchone()[0]
        if not end_index_exists:
            logger.info("Creating index on end_dateTime...")
            cur.execute("""
            CREATE INDEX idx_calendar_events_end_datetime 
            ON calendar_events(end_dateTime)
            """)
        
        # For date range
        cur.execute("""
        SELECT EXISTS (
            SELECT FROM pg_indexes 
            WHERE tablename = 'calendar_events' 
            AND indexname = 'idx_calendar_events_date_range'
        )
        """)
        
        range_index_exists = cur.fetchone()[0]
        if not range_index_exists:
            logger.info("Creating composite index on date range...")
            cur.execute("""
            CREATE INDEX idx_calendar_events_date_range 
            ON calendar_events(start_dateTime, end_dateTime)
            """)
        
        # 10. Commit the transaction
        conn.commit()
        logger.info("Migration completed successfully!")
        return True
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Migration failed: {str(e)}")
        logger.error(f"Detail: {type(e).__name__}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        if conn:
            conn.close()

def main():
    """Main function to run the migration"""
    success = migrate_schema()
    if success:
        print("Database migration completed successfully.")
        sys.exit(0)
    else:
        print("Database migration failed. Check the logs for details.")
        sys.exit(1)

if __name__ == "__main__":
    main() 