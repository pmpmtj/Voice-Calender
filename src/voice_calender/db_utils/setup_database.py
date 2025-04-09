#!/usr/bin/env python3
"""
Database Setup Script

This script initializes the PostgreSQL database and creates the necessary tables
for storing calendar events.
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from dotenv import load_dotenv

# Import the db_manager from db_utils
from voice_calender.db_utils.db_manager import initialize_db

def main():
    """Main function to set up the database"""
    parser = argparse.ArgumentParser(description='Set up the PostgreSQL database for calendar events')
    args = parser.parse_args()
    
    # Logging is already configured by importing db_config (via db_manager)
    logger = logging.getLogger(__name__)
    
    # Load environment variables from .env file
    env_path = Path(__file__).parents[2] / '.env'
    load_dotenv(dotenv_path=env_path)
    
    # Check for PostgreSQL database connection
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        logger.warning("DATABASE_URL environment variable not found.")
        logger.warning("Make sure you have created a PostgreSQL database.")
        
        response = input("Do you want to continue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(1)
    
    # Initialize database
    logger.info("Initializing database...")
    success = initialize_db()
    
    if success:
        logger.info("Database setup completed successfully!")
        logger.info("The following tables have been created:")
        logger.info("  - calendar_events: Stores calendar event data")
    else:
        logger.error("Database setup failed.")
        sys.exit(1)

if __name__ == "__main__":
    main() 