#!/usr/bin/env python3
"""
Voice Calendar - Parse Entry for Calendar

This script takes transcribed voice entries and uses OpenAI Assistants API to parse
calendar event details, outputting structured JSON data that can be used to create
calendar events in the next step of the workflow.

The script also saves the parsed calendar events to a PostgreSQL database for backup
and easier retrieval.
"""

import json
import logging
import logging.handlers
import os
import sys
import yaml
import time
import glob
from datetime import datetime
from pathlib import Path
from openai import OpenAI

# Import database utilities
from voice_calender.db_utils.db_manager import (
    initialize_db, 
    save_calendar_event,
    close_all_connections
)

# Initialize paths - handling both frozen (PyInstaller) and regular Python execution
if getattr(sys, 'frozen', False):
    # Running as compiled executable
    SCRIPT_DIR = Path(sys._MEIPASS)
else:
    # Running as script
    SCRIPT_DIR = Path(__file__).parent.absolute()

# Project root for path calculations
PROJECT_ROOT = SCRIPT_DIR.parent

# Configuration paths
CONFIG_DIR = PROJECT_ROOT / "project_modules_configs" / "config_agent_parse_entry"
CONFIG_PATH = CONFIG_DIR / "agent_parse_entry_config.json"
OPENAI_CONFIG_PATH = CONFIG_DIR / "openai_config.json"
PROMPTS_PATH = CONFIG_DIR / "prompts.yaml"
LOG_DIR = SCRIPT_DIR / "logs"

# Create log directory if it doesn't exist
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Initialize logger
logger = logging.getLogger("parse_entry")

def load_config():
    """Load configuration from JSON file"""
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading configuration: {str(e)}")
        sys.exit(1)

def load_openai_config():
    """Load OpenAI configuration from JSON file"""
    try:
        with open(OPENAI_CONFIG_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading OpenAI configuration: {str(e)}")
        sys.exit(1)

def load_prompts():
    """Load prompt templates from YAML file"""
    try:
        with open(PROMPTS_PATH, 'r', encoding='utf-8') as f:
            prompts_data = yaml.safe_load(f)
            return prompts_data.get('prompts', {})
    except Exception as e:
        logger.error(f"Error loading prompt templates: {str(e)}")
        sys.exit(1)

def setup_logging(config):
    """Setup logging based on configuration"""
    log_config = config.get("logging", {})
    log_level = getattr(logging, log_config.get("log_level", "INFO"))
    
    logger.setLevel(log_level)
    
    # Set up console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Set up file handler with rotation
    log_file = log_config.get("parse_entry_log_file", "parse_entry.log")
    max_bytes = log_config.get("parse_entry_max_size_bytes", 1048576)  # 1MB default
    backup_count = log_config.get("parse_entry_backup_count", 3)
    
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_DIR / log_file,
        maxBytes=max_bytes,
        backupCount=backup_count
    )
    file_handler.setLevel(log_level)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # Set up OpenAI usage logger with rotation if configured
    openai_config = load_openai_config()
    if 'logging' in openai_config and 'openai_usage_log_file' in openai_config['logging']:
        openai_logger = logging.getLogger('openai_usage')
        openai_logger.setLevel(logging.INFO)
        
        # Clear any existing handlers for the openai logger
        if openai_logger.handlers:
            openai_logger.handlers.clear()
        
        # Prevent propagation to root logger to avoid duplicate entries
        openai_logger.propagate = False
        
        openai_log_config = openai_config['logging']
        openai_log_file = LOG_DIR / openai_log_config['openai_usage_log_file']
        openai_handler = logging.handlers.RotatingFileHandler(
            openai_log_file,
            maxBytes=openai_log_config.get('openai_usage_max_size_bytes', 1048576),  # Default 1MB
            backupCount=openai_log_config.get('openai_usage_backup_count', 3)        # Default 3 backups
        )
        
        # Simple formatter for OpenAI usage log (just the message)
        openai_formatter = logging.Formatter('%(message)s')
        openai_handler.setFormatter(openai_formatter)
        openai_logger.addHandler(openai_handler)
    
    logger.info("Logging configured successfully")

def get_transcription_files(transcription_dir):
    """
    Get transcription files from the specified directory
    
    Args:
        transcription_dir (str): Directory containing transcription files
        
    Returns:
        list: List of transcription file paths
    """
    try:
        transcription_path = Path(transcription_dir)
        if not transcription_path.exists():
            logger.error(f"Transcription directory does not exist: {transcription_dir}")
            return []
            
        # Look for text files in the directory
        # This assumes that transcription files have a .txt extension
        transcript_files = list(transcription_path.glob("*.txt"))
        
        if not transcript_files:
            logger.warning(f"No transcription files found in {transcription_dir}")
            return []
            
        logger.info(f"Found {len(transcript_files)} transcription files")
        return transcript_files
    except Exception as e:
        logger.error(f"Error getting transcription files: {str(e)}")
        return []

def load_transcription(file_path):
    """
    Load transcription from a file
    
    Args:
        file_path (str): Path to the transcription file
        
    Returns:
        str: Content of the transcription file
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return content
    except Exception as e:
        logger.error(f"Error loading transcription from {file_path}: {str(e)}")
        return None

def process_with_openai_assistant(entry_content, prompt_template, openai_config, prompts=None):
    """
    Process the entry content with OpenAI Assistants API to parse calendar events.
    
    Args:
        entry_content (str): The transcribed entry content
        prompt_template (str): The prompt template to use
        openai_config (dict): The OpenAI configuration
        prompts (dict): Dictionary of prompts loaded from YAML
        
    Returns:
        str: The JSON response from the assistant
    """
    # Format the prompt with the entry content
    prompt = prompt_template.format(
        entry_content=entry_content
    )
    
    # Set up the API client
    config = openai_config['openai_config']
    api_key = config['api_key'] or os.environ.get('OPENAI_API_KEY')
    
    if not api_key:
        logger.error("No OpenAI API key found. Set it in the config file or as an environment variable.")
        raise ValueError("No OpenAI API key found. Set it in the config file or as an environment variable.")
    
    client = OpenAI(api_key=api_key)
    
    try:
        # Check if we have a saved assistant_id in the config
        assistant_id = config.get('assistant_id', None)
        
        # Create a new assistant if we don't have one
        if not assistant_id:
            logger.info("Creating new OpenAI Assistant for parsing calendar entries")
            
            # Get assistant instructions from prompts config
            if not prompts:
                logger.error("Prompts dictionary is required for assistant creation")
                raise ValueError("Prompts dictionary is required for assistant creation")
                
            assistant_instructions = get_prompt_template(prompts, "assistant_instructions")
            
            # Get tools configuration from config
            tools = config.get('tools', [{"type": "file_search"}])
            logger.info(f"Creating assistant with tools: {tools}")
            
            assistant = client.beta.assistants.create(
                name="Calendar Entry Parser",
                instructions=assistant_instructions,
                tools=tools,
                model=config['model']
            )
            assistant_id = assistant.id
            
            # Add the assistant_id to the config for future use
            config['assistant_id'] = assistant_id
            with open(OPENAI_CONFIG_PATH, 'w', encoding='utf-8') as f:
                json.dump(openai_config, f, indent=2)
            
            logger.info(f"Assistant created with ID: {assistant_id}")
        else:
            # Verify assistant exists
            try:
                client.beta.assistants.retrieve(assistant_id)
                logger.info(f"Using existing Assistant with ID: {assistant_id}")
            except Exception as e:
                error_msg = str(e)
                if "No assistant found" in error_msg:
                    logger.error(f"Assistant ID {assistant_id} no longer exists on OpenAI server: {e}")
                    # Remove the invalid assistant_id from config
                    logger.info("Removing invalid assistant_id from config")
                    config.pop('assistant_id', None)
                    with open(OPENAI_CONFIG_PATH, 'w', encoding='utf-8') as f:
                        json.dump(openai_config, f, indent=2)
                    
                    # Restart the process (recursive call after fixing config)
                    logger.info("Restarting process with updated config")
                    return process_with_openai_assistant(entry_content, prompt_template, openai_config, prompts)
                else:
                    # For other errors, propagate them
                    raise
        
        # Check if we have a saved thread_id in the config
        thread_id = config.get('thread_id', None)
        
        # Check if thread needs to be rotated based on creation date
        thread_needs_rotation = False
        if thread_id:
            try:
                # Get thread creation time
                thread = client.beta.threads.retrieve(thread_id)
                thread_created_at = datetime.fromtimestamp(thread.created_at)
                days_since_creation = (datetime.now() - thread_created_at).days
                
                # Check if thread is older than retention period
                retention_days = config.get('thread_retention_days', 30)
                if days_since_creation > retention_days:
                    logger.info(f"Thread is {days_since_creation} days old (retention: {retention_days} days). Creating new thread.")
                    thread_needs_rotation = True
                else:
                    logger.info(f"Using existing thread (age: {days_since_creation} days, retention: {retention_days} days)")
            except Exception as e:
                error_msg = str(e)
                if "No thread found" in error_msg:
                    logger.error(f"Thread ID {thread_id} no longer exists on OpenAI server: {e}")
                    # Remove the invalid thread_id from config
                    logger.info("Removing invalid thread_id from config")
                    config.pop('thread_id', None)
                    if 'thread_created_at' in config:
                        config.pop('thread_created_at', None)
                    with open(OPENAI_CONFIG_PATH, 'w', encoding='utf-8') as f:
                        json.dump(openai_config, f, indent=2)
                    
                    # Continue with a new thread
                    thread_needs_rotation = True
                    logger.info("Will create a new thread")
                else:
                    logger.warning(f"Error checking thread age, will create new thread: {e}")
                    thread_needs_rotation = True
        
        # Create a new thread if needed
        if not thread_id or thread_needs_rotation:
            logger.info("Creating new thread for event parsing tasks")
            thread = client.beta.threads.create()
            thread_id = thread.id
            
            # Add the thread_id to the config for future use
            config['thread_id'] = thread_id
            # Store thread creation time
            config['thread_created_at'] = datetime.now().isoformat()
            with open(OPENAI_CONFIG_PATH, 'w', encoding='utf-8') as f:
                json.dump(openai_config, f, indent=2)
            
            logger.info(f"Thread created with ID: {thread_id}")
        else:
            logger.info(f"Using existing thread with ID: {thread_id}")
        
        # Add message to the thread with the entry content
        logger.info("Adding message with entry content to thread")
        client.beta.threads.messages.create(
            thread_id=thread_id,
            role="user",
            content=prompt
        )
        
        # Run the assistant on the thread
        logger.info("Running assistant to parse calendar entry")
        try:
            run = client.beta.threads.runs.create(
                thread_id=thread_id,
                assistant_id=assistant_id
            )
            
            # Poll for completion
            run_status = client.beta.threads.runs.retrieve(
                thread_id=thread_id,
                run_id=run.id
            )
            
            # Wait for run to complete
            logger.info("Waiting for assistant to complete processing")
            while run_status.status not in ["completed", "failed", "cancelled", "expired"]:
                logger.debug(f"Run status: {run_status.status}")
                time.sleep(1)
                run_status = client.beta.threads.runs.retrieve(
                    thread_id=thread_id,
                    run_id=run.id
                )
            
            if run_status.status != "completed":
                logger.error(f"Assistant run failed with status: {run_status.status}")
                raise ValueError(f"Assistant run failed with status: {run_status.status}")
            
            # Get the messages
            logger.info("Retrieving assistant's response")
            messages = client.beta.threads.messages.list(
                thread_id=thread_id
            )
            
            # Get the latest assistant response
            for message in messages.data:
                if message.role == "assistant":
                    # Extract the content from the message
                    content = message.content[0].text.value
                    
                    # Log usage statistics if available
                    if config['save_usage_stats'] and hasattr(run_status, 'usage'):
                        usage = run_status.usage
                        # Handle usage data correctly - usage is an object, not a dictionary
                        try:
                            usage_log = f"{datetime.now().isoformat()} | {config['model']} | " \
                                       f"Input: {usage.prompt_tokens if hasattr(usage, 'prompt_tokens') else 0} | " \
                                       f"Output: {usage.completion_tokens if hasattr(usage, 'completion_tokens') else 0} | " \
                                       f"Total: {usage.total_tokens if hasattr(usage, 'total_tokens') else 0}"
                            
                            openai_logger = logging.getLogger('openai_usage')
                            openai_logger.info(usage_log)
                        except Exception as e:
                            logger.warning(f"Error logging usage statistics: {e}")
                    
                    return content
            
            logger.error("No assistant response found in the thread")
            raise ValueError("No assistant response found in the thread")
            
        except Exception as e:
            error_msg = str(e)
            if "No assistant found" in error_msg:
                logger.error(f"Assistant ID {assistant_id} not found: {e}")
                # Remove the invalid assistant_id from config
                logger.info("Removing invalid assistant_id from config")
                config.pop('assistant_id', None)
                with open(OPENAI_CONFIG_PATH, 'w', encoding='utf-8') as f:
                    json.dump(openai_config, f, indent=2)
                
                # Restart the process (recursive call after fixing config)
                logger.info("Restarting process with updated config")
                return process_with_openai_assistant(entry_content, prompt_template, openai_config, prompts)
            else:
                logger.error(f"Error processing with OpenAI Assistant: {e}")
                raise ValueError(f"Error processing with OpenAI Assistant: {e}")
    
    except Exception as e:
        logger.error(f"Error processing with OpenAI Assistant: {e}")
        raise ValueError(f"Error processing with OpenAI Assistant: {e}")

def get_prompt_template(prompts, name):
    """
    Get a specific prompt template by name from the prompts dictionary.
    Raises an exception if the prompt is not found to ensure data precision.
    
    Args:
        prompts (dict): Dictionary of prompts loaded from YAML
        name (str): Name of the prompt template to retrieve
        
    Returns:
        str: The prompt template
        
    Raises:
        ValueError: If the prompt template is not found
    """
    if not prompts:
        logger.error(f"Prompts dictionary is empty or None")
        raise ValueError(f"Prompts dictionary is empty or None")
        
    prompt_data = prompts.get(name)
    if not prompt_data:
        logger.error(f"Prompt '{name}' not found in prompts configuration")
        raise ValueError(f"Prompt '{name}' not found in prompts configuration")
    
    template = prompt_data.get("template")
    if not template:
        logger.error(f"Template not found for prompt '{name}'")
        raise ValueError(f"Template not found for prompt '{name}'")
    
    return template

def save_json_output(json_data, output_dir):
    """
    Save JSON output to file
    
    Args:
        json_data (str): JSON data to save
        output_dir (str): Directory to save the JSON file
        
    Returns:
        tuple: (str, dict) Path to the saved file and the parsed JSON object
    """
    try:
        # Create output directory if it doesn't exist
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Generate a timestamp for the filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_path / f"calendar_event_{timestamp}.json"
        
        # Extract valid JSON from the response
        # The assistant might return formatted code blocks or extra text
        json_object = extract_json_from_text(json_data)
        
        if not json_object:
            logger.error("Failed to extract valid JSON from assistant response")
            return None, None
        
        # Write the JSON to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(json_object, f, indent=2)
            
        logger.info(f"Saved JSON output to {output_file}")
        return str(output_file), json_object
        
    except Exception as e:
        logger.error(f"Error saving JSON output: {str(e)}")
        return None, None

def extract_json_from_text(text):
    """
    Extract JSON object from text that might contain markdown formatting or extra text
    
    Args:
        text (str): Text that may contain JSON
        
    Returns:
        dict: Parsed JSON object, or None if parsing fails
    """
    try:
        # First try to parse the entire text as JSON
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # If that fails, look for JSON within code blocks
            pass
            
        # Look for JSON within markdown code blocks
        # Match content between ```json and ``` or between ``` and ```
        import re
        json_block_matches = re.findall(r'```(?:json)?\s*([\s\S]*?)\s*```', text)
        
        for match in json_block_matches:
            try:
                return json.loads(match)
            except json.JSONDecodeError:
                continue
                
        # If we still haven't found valid JSON, look for content between { and }
        # This is a more aggressive approach and might catch unintended content
        curly_brace_match = re.search(r'({[\s\S]*})', text)
        if curly_brace_match:
            try:
                return json.loads(curly_brace_match.group(1))
            except json.JSONDecodeError:
                pass
                
        # If all attempts fail, return None
        logger.error("Could not extract valid JSON from text")
        return None
    except Exception as e:
        logger.error(f"Error extracting JSON from text: {str(e)}")
        return None

def get_transcription_dir():
    """
    Get transcription directory from the transcribe_audio_for_calender config
    """
    try:
        # Load transcribe configuration
        transcribe_config_path = PROJECT_ROOT / "project_modules_configs" / "config_transcribe_raw_audio_for_calender" / "transcribe_for_calender_config.json"
        
        if not transcribe_config_path.exists():
            logger.error(f"Transcribe configuration file not found at {transcribe_config_path}")
            return None
            
        with open(transcribe_config_path, 'r', encoding='utf-8') as f:
            transcribe_config = json.load(f)
            
        # Get transcriptions directory
        transcriptions_dir = transcribe_config.get("transcriptions_dir")
        
        if not transcriptions_dir:
            logger.warning("Transcriptions directory not found in transcribe config")
            
        return transcriptions_dir
    except Exception as e:
        logger.error(f"Error getting transcriptions directory from config: {str(e)}")
        return None

def save_to_database(event_data):
    """
    Save calendar event data to the database
    
    Args:
        event_data (dict): Calendar event data parsed from the JSON
        
    Returns:
        int: Database record ID if successful, None otherwise
    """
    try:
        # Extract event fields from the event_data dictionary
        summary = event_data.get('summary')
        if not summary:
            logger.error("Event data missing required 'summary' field")
            return None
            
        # Extract start date/time information
        start_data = event_data.get('start', {})
        start_datetime = start_data.get('dateTime') or start_data.get('date')
        if not start_datetime:
            logger.error("Event data missing required start date/time information")
            return None
            
        # Extract timezone information from start data
        start_timezone = start_data.get('timeZone')
        
        # Extract end date/time information
        end_data = event_data.get('end', {})
        end_datetime = end_data.get('dateTime') or end_data.get('date')
        end_timezone = end_data.get('timeZone')
        
        # If end is not specified, use start date/time as fallback
        if not end_datetime:
            logger.warning("End date/time not specified, using start date/time as fallback")
            end_datetime = start_datetime
            end_timezone = start_timezone
            
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
            logger.info(f"Successfully saved calendar event to database with ID: {event_id}")
        else:
            logger.error("Failed to save calendar event to database")
            
        return event_id
    except Exception as e:
        logger.error(f"Error saving calendar event to database: {str(e)}")
        return None

def parse_calendar_entries():
    """
    Main function to parse calendar entries from transcriptions
    """
    config = load_config()
    setup_logging(config)
    
    logger.info("Starting parse_calendar_entries process")
    
    # Initialize the database
    try:
        db_initialized = initialize_db()
        if not db_initialized:
            logger.error("Failed to initialize database connection")
            # Continue with JSON file output even if database fails
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        # Continue with JSON file output even if database fails
    
    # Get output directory from config
    json_output_dir = config.get("paths", {}).get("json_output_directory")
    if not json_output_dir:
        logger.error("JSON output directory not specified in config")
        return False
    
    # Get transcription directory
    transcription_dir = get_transcription_dir()
    if not transcription_dir:
        logger.error("Could not determine transcription directory")
        return False
    
    # Get transcription files
    transcription_files = get_transcription_files(transcription_dir)
    if not transcription_files:
        logger.warning("No transcription files found to process")
        return False
    
    # Load OpenAI config and prompts
    openai_config = load_openai_config()
    prompts = load_prompts()
    
    # Get the parse entry prompt template
    try:
        parse_entry_template = get_prompt_template(prompts, "parse_entry_prompt")
    except ValueError as e:
        logger.error(f"Error getting parse entry prompt template: {str(e)}")
        return False
    
    # Process each transcription file
    success_count = 0
    db_success_count = 0
    
    for file_path in transcription_files:
        logger.info(f"Processing transcription file: {file_path}")
        
        # Load transcription content
        content = load_transcription(file_path)
        if not content:
            logger.warning(f"Failed to load content from {file_path}")
            continue
        
        # Skip empty files
        if not content.strip():
            logger.warning(f"File {file_path} is empty, skipping")
            continue
        
        try:
            # Process with OpenAI Assistant to parse calendar entry
            response = process_with_openai_assistant(content, parse_entry_template, openai_config, prompts)
            
            if not response:
                logger.warning(f"No response from assistant for {file_path}")
                continue
            
            # Save JSON output to file and get the parsed object
            output_file, json_object = save_json_output(response, json_output_dir)
            
            if output_file and json_object:
                logger.info(f"Successfully processed {file_path} and saved to {output_file}")
                success_count += 1
                
                # Save to database
                event_id = save_to_database(json_object)
                if event_id:
                    db_success_count += 1
                    logger.info(f"Event saved to database with ID: {event_id}")
                else:
                    logger.warning(f"Failed to save event to database for {file_path}")
            else:
                logger.warning(f"Failed to save output for {file_path}")
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
    
    # Clean up database connections
    try:
        close_all_connections()
    except Exception as e:
        logger.warning(f"Error closing database connections: {str(e)}")
    
    if success_count > 0:
        logger.info(f"Successfully processed {success_count} of {len(transcription_files)} transcription files")
        logger.info(f"Successfully saved {db_success_count} events to the database")
        return True
    else:
        logger.warning("No transcription files were successfully processed")
        return False

if __name__ == "__main__":
    parse_calendar_entries()
