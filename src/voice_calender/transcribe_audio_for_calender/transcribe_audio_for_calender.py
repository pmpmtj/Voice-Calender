#!/usr/bin/env python3
"""
OpenAI Whisper API Transcription for Voice Calendar

This script transcribes audio files using OpenAI's API:
- Supports multiple OpenAI speech-to-text models:
  - whisper-1
  - gpt-4o-transcribe
  - gpt-4o-mini-transcribe

It processes audio files in the downloads directory in chronological order,
supporting both individual files and batch processing based on the configuration.
The transcriptions will be used for calendar event creation.
"""

import os
import sys
import json
import logging
import time
from datetime import datetime
from pathlib import Path
import traceback
import subprocess
import logging.handlers
import re
from openai import OpenAI


# Initialize paths - handling both frozen (PyInstaller) and regular Python execution
if getattr(sys, 'frozen', False):
    # Running as compiled executable
    SCRIPT_DIR = Path(sys._MEIPASS)
else:
    # Running as script
    SCRIPT_DIR = Path(__file__).parent.absolute()

# Project root for path calculations
PROJECT_ROOT = SCRIPT_DIR.parent

# Define log directory
LOGS_DIR = SCRIPT_DIR / "logs"

# Make sure the log directory exists
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Load config function so we can get logging config
def load_config():
    """Load configuration from transcribe_for_calender_config.json."""
    try:
        # Use default config path
        config_path = PROJECT_ROOT / "project_modules_configs" / "config_transcribe_raw_audio_for_calender" / "transcribe_for_calender_config.json"
            
        if not config_path.exists():
            # Can't use logger here as it's not created yet
            print(f"Error: Configuration file not found at {config_path}")
            sys.exit(1)
            
        with open(config_path, 'r') as f:
            config = json.load(f)

        return config
    except Exception as e:
        # Can't use logger here as it's not created yet
        print(f"Error loading configuration: {str(e)}")
        traceback.print_exc()
        sys.exit(1)

# Load OpenAI transcription config
def load_openai_config():
    """Load OpenAI transcription configuration."""
    try:
        # Use default config path
        config_path = PROJECT_ROOT / "project_modules_configs" / "config_transcribe_raw_audio_for_calender" / "openai_transcribe_config.json"
            
        if not config_path.exists():
            # Can't use logger here as it's not created yet
            print(f"Error: OpenAI configuration file not found at {config_path}")
            sys.exit(1)
            
        with open(config_path, 'r') as f:
            config = json.load(f)

        return config
    except Exception as e:
        # Can't use logger here as it's not created yet
        print(f"Error loading OpenAI configuration: {str(e)}")
        traceback.print_exc()
        sys.exit(1)

# Load configuration
config = load_config()
openai_config = load_openai_config()

# Configure logging
logging_config = config.get("logging", {})
log_level = getattr(logging, logging_config.get("level", "INFO"))
log_format = logging_config.get("format", "%(asctime)s - %(levelname)s - %(message)s")
log_file = logging_config.get("log_file", "transcribe_audio_for_calender.log")
max_size = logging_config.get("max_size_bytes", 1048576)
backup_count = logging_config.get("backup_count", 3)

# Set up logger
logger = logging.getLogger("voice_calender.transcribe")
logger.setLevel(log_level)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(log_format))
logger.addHandler(console_handler)

# File handler with rotation
log_path = LOGS_DIR / log_file
file_handler = logging.handlers.RotatingFileHandler(
    log_path, maxBytes=max_size, backupCount=backup_count
)
file_handler.setFormatter(logging.Formatter(log_format))
logger.addHandler(file_handler)

# Log initial information
logger.info("Voice Calendar Transcription Service")
logger.info(f"Logging to {log_path}")


def get_openai_client():
    """Get the OpenAI client."""
    try:
        # Create the OpenAI client
        client = OpenAI()
        
        # Check if API key is set
        if not client.api_key:
            logger.error("OpenAI API key not found. Make sure it's set in the environment variables.")
            sys.exit(1)
            
        logger.info("OpenAI client initialized")
        return client
    except Exception as e:
        logger.error(f"Error creating OpenAI client: {str(e)}")
        traceback.print_exc()
        sys.exit(1)


def get_audio_extensions_from_gdrive_config():
    """Get audio file extensions from Google Drive config."""
    try:
        # Load Google Drive configuration
        gdrive_config_path = PROJECT_ROOT / "project_modules_configs" / "config_dwnload_files" / "dwnload_from_gdrive_conf.json"
        
        if not gdrive_config_path.exists():
            logger.error(f"Google Drive configuration file not found at {gdrive_config_path}")
            return None
            
        with open(gdrive_config_path, 'r') as f:
            gdrive_config = json.load(f)
            
        # Get audio file extensions
        audio_extensions = gdrive_config.get("audio_file_types", {}).get("include", [])
        
        if not audio_extensions:
            logger.warning("No audio file extensions found in Google Drive config")
            
        return audio_extensions
    except Exception as e:
        logger.error(f"Error getting audio extensions from Google Drive config: {str(e)}")
        return None


def get_downloads_dir_from_gdrive_config():
    """Get downloads directory from Google Drive config."""
    try:
        # Load Google Drive configuration
        gdrive_config_path = PROJECT_ROOT / "project_modules_configs" / "config_dwnload_files" / "dwnload_from_gdrive_conf.json"
        
        if not gdrive_config_path.exists():
            logger.error(f"Google Drive configuration file not found at {gdrive_config_path}")
            return None
            
        with open(gdrive_config_path, 'r') as f:
            gdrive_config = json.load(f)
            
        # Get downloads directory
        downloads_dir = gdrive_config.get("downloads_path", {}).get("downloads_dir")
        
        if not downloads_dir:
            logger.warning("Downloads directory not found in Google Drive config")
            
        return downloads_dir
    except Exception as e:
        logger.error(f"Error getting downloads directory from Google Drive config: {str(e)}")
        return None


def calculate_duration(file_path):
    """Calculate the duration of an audio file using ffprobe."""
    try:
        # Use ffprobe to get the duration
        result = subprocess.run(
            [
                "ffprobe", 
                "-v", "error", 
                "-show_entries", "format=duration", 
                "-of", "default=noprint_wrappers=1:nokey=1", 
                file_path
            ], 
            capture_output=True, 
            text=True
        )
        
        # Parse the output
        if result.returncode == 0 and result.stdout.strip():
            duration = float(result.stdout.strip())
            return duration
        else:
            logger.warning(f"Unable to determine audio duration: {result.stderr}")
            return None
    except Exception as e:
        logger.error(f"Error calculating audio duration: {str(e)}")
        return None


def get_transcription_model():
    """Get the appropriate transcription model based on configuration."""
    # Get available models from config
    models = openai_config.get("models", {})
    default_model = openai_config.get("default_model", "whisper-1")
    
    # Log available models
    logger.info(f"Configured transcription models:")
    for model_name, model_info in models.items():
        status = "enabled" if model_info.get("enabled", False) else "disabled"
        logger.info(f"  - {model_name}: {status} - {model_info.get('description', '')}")
    
    # Find enabled models
    enabled_models = [model_name for model_name, model_info in models.items() 
                     if model_info.get("enabled", False)]
    
    if not enabled_models:
        logger.warning(f"No enabled models found. Using default model: {default_model}")
        return default_model
    
    # If the default model is enabled, use it
    if default_model in enabled_models:
        logger.info(f"Using default model: {default_model}")
        return default_model
    
    # Otherwise use the first enabled model
    selected_model = enabled_models[0]
    logger.info(f"Default model not enabled. Using first enabled model: {selected_model}")
    return selected_model


def transcribe_audio_file(client, file_path):
    """Transcribe an audio file using OpenAI's API."""
    try:
        logger.info(f"Transcribing {file_path}")
        
        # Calculate estimated duration to log progress
        duration = calculate_duration(file_path)
        if duration:
            logger.info(f"Estimated duration: {duration:.2f} seconds")
            
            # Check file duration against maximum if configured
            max_duration = openai_config.get("cost_management", {}).get("max_audio_duration_seconds")
            if max_duration and duration > max_duration:
                logger.warning(f"Audio file duration ({duration}s) exceeds maximum allowed ({max_duration}s)")
                if openai_config.get("cost_management", {}).get("warn_on_large_files", True):
                    logger.warning(f"Processing the file anyway, but this may result in higher costs")
        
        # Get transcription model and settings
        model = get_transcription_model()
        model_info = openai_config.get("models", {}).get(model, {})
        settings = openai_config.get("settings", {})
        language = settings.get("language")
        response_format = settings.get("response_format", "json")
        
        # Get prompt: first try model-specific prompt, then fall back to general settings prompt
        prompt = model_info.get("prompt") if "prompt" in model_info else settings.get("prompt")
        
        # Log the model being used
        logger.info(f"Using transcription model: {model}")
        logger.info(f"Using prompt: {prompt[:50]}..." if prompt and len(prompt) > 50 else f"Using prompt: {prompt}")
        
        start_time = time.time()
        
        # Open the audio file
        with open(file_path, "rb") as audio_file:
            # Prepare API call parameters
            params = {
                "model": model,
                "file": audio_file
            }
            
            # Add optional parameters if provided
            # Only add language parameter if the model supports it or we don't know
            supports_language = model_info.get("supports_language_parameter", True)
            if language and supports_language:
                params["language"] = language
                logger.info(f"Using language parameter: {language}")
            elif language and not supports_language:
                logger.info(f"Model {model} auto-detects language, not using language parameter")
                
            if prompt:
                params["prompt"] = prompt
                
            if response_format:
                params["response_format"] = response_format
            
            # Call the OpenAI API
            response = client.audio.transcriptions.create(**params)
        
        end_time = time.time()
        transcription_time = end_time - start_time
        
        # Extract the transcription text
        if response_format == "json":
            # Parse the JSON if needed
            if hasattr(response, 'text'):
                transcription = response.text
            else:
                # Newer models might return a different structure
                transcription = json.dumps(response.model_dump(), indent=2)
        else:
            transcription = response.text
        
        if duration:
            logger.info(f"Transcription completed in {transcription_time:.2f} seconds")
            logger.info(f"Transcription speed: {duration/transcription_time:.2f}x real-time")
        
        if transcription:
            logger.info(f"Transcription successful: {len(transcription)} characters")
        else:
            logger.warning("Transcription returned empty result")
            
        return transcription
    except Exception as e:
        logger.error(f"Error transcribing audio file: {str(e)}")
        traceback.print_exc()
        return None


def get_audio_files(directory):
    """Get all audio files from the specified directory and sort them chronologically."""
    directory = Path(directory)
    
    if not directory.exists():
        logger.error(f"Directory {directory} does not exist")
        return []
        
    # Get audio extensions from Google Drive config
    audio_extensions = get_audio_extensions_from_gdrive_config()
    
    if not audio_extensions:
        logger.error("Could not load audio extensions from config")
        sys.exit(1)
    
    # Get all files with audio extensions
    audio_files = []
    for ext in audio_extensions:
        audio_files.extend(directory.glob(f"*{ext}"))
    
    if not audio_files:
        return []
    
    # Function to extract timestamp from filename if present
    def get_timestamp_from_filename(filepath):
        filename = filepath.name
        # Try to extract timestamp in format YYYYMMDD_HHMMSS from filename
        timestamp_match = re.search(r'(\d{8}_\d{6})', filename)
        if timestamp_match:
            try:
                # If timestamp found in filename, use it
                timestamp_str = timestamp_match.group(1)
                return datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
            except ValueError:
                pass
        
        # Fall back to file creation time if no timestamp in filename
        # or if timestamp couldn't be parsed
        return datetime.fromtimestamp(os.path.getctime(filepath))
    
    # Sort files by timestamp
    logger.info("Sorting audio files by creation time (chronological order)")
    sorted_files = sorted(audio_files, key=get_timestamp_from_filename)
    
    # Log the sorted files
    if sorted_files:
        logger.info("Files will be processed in the following order:")
        for i, file in enumerate(sorted_files, 1):
            timestamp = get_timestamp_from_filename(file)
            logger.info(f"{i}. {file.name} (Created: {timestamp.strftime('%Y-%m-%d %H:%M:%S')})")
    
    return sorted_files


def save_transcription(text, output_path, file_name):
    """Save the transcription to the output file."""
    if not text:
        logger.warning("No transcription text to save")
        return False
        
    try:
        output_path = Path(output_path)
        
        # Create output directory if it doesn't exist
        if not output_path.exists():
            logger.info(f"Creating output directory: {output_path}")
            output_path.mkdir(parents=True, exist_ok=True)
        
        # Use microsecond precision timestamp for unique filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        output_file = output_path / f"{timestamp}_{file_name}"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(text)
            
        logger.info(f"Transcription saved to {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving transcription: {str(e)}")
        traceback.print_exc()
        return False


def process_audio_files(client, audio_files, output_path, output_file):
    """Process all audio files and save their transcriptions."""
    if not audio_files:
        logger.warning("No audio files found")
        return False
        
    logger.info(f"Found {len(audio_files)} audio file(s) to process")
    
    all_transcriptions = []
    
    for file_path in audio_files:
        logger.info(f"Processing {file_path}")
        
        # Transcribe the audio file
        transcription = transcribe_audio_file(client, file_path)
        
        if transcription:
            # Add file name and timestamp to the transcription
            file_name = file_path.name
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            formatted_transcription = f"File: {file_name}\nTimestamp: {timestamp}\n\n{transcription}\n\n"
            
            all_transcriptions.append(formatted_transcription)
    
    # Combine all transcriptions and save them
    if all_transcriptions:
        combined_text = "\n".join(all_transcriptions)
        save_transcription(combined_text, output_path, output_file)
        return True
    
    return False


def run_transcribe():
    """Main function to run the transcription process."""
    try:
        # Configuration is already loaded at module level
        
        # Get downloads directory from Google Drive config
        gdrive_downloads_dir = get_downloads_dir_from_gdrive_config()
        
        if not gdrive_downloads_dir:
            logger.error("Downloads directory not found in Google Drive config")
            sys.exit(1)
            
        downloads_dir = Path(gdrive_downloads_dir)
        
        if not downloads_dir.exists():
            logger.error(f"Downloads directory {downloads_dir} does not exist")
            sys.exit(1)
        
        # Log the downloads directory being used
        logger.info(f"Using downloads directory: {downloads_dir}")
        
        # Get output file name
        if "output_file" not in config:
            logger.error("output_file not found in configuration")
            sys.exit(1)
        output_file = config["output_file"]
        
        # Get output directory from config
        if "transcriptions_dir" not in config:
            logger.error("transcriptions_dir not found in configuration")
            sys.exit(1)
        output_dir = Path(config["transcriptions_dir"])
        
        # Create transcriptions output directory if it doesn't exist
        if not output_dir.exists():
            logger.info(f"Creating output directory: {output_dir}")
            output_dir.mkdir(parents=True, exist_ok=True)
        
        # Log the output directory being used
        logger.info(f"Using output directory: {output_dir}")
        
        # Get OpenAI client
        client = get_openai_client()
        
        # Get audio files in chronological order
        audio_files = get_audio_files(downloads_dir)
        
        # Process audio files
        success = process_audio_files(client, audio_files, output_dir, output_file)
        
        if success:
            logger.info("Audio transcription completed successfully")
        else:
            logger.warning("No transcriptions were generated")
            
        return success
    except Exception as e:
        logger.error(f"Error in transcription process: {str(e)}")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    run_transcribe()
