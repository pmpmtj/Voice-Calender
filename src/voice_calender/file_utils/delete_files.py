#!/usr/bin/env python3
"""
File deletion utility module.

This module provides functionality to delete files from specified directories
based on file extensions and configuration.
"""

import os
import sys
import json
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Tuple, Set, Optional, Union
import glob

# Handle both frozen (PyInstaller) and regular Python execution
SCRIPT_DIR = Path(sys._MEIPASS) if getattr(sys, 'frozen', False) else Path(__file__).resolve().parent

# Project root is one level up from the file_utils directory
PROJECT_ROOT = SCRIPT_DIR.parent


def load_config(config_path: Union[str, Path]) -> Dict:
    """
    Load configuration from JSON file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Dictionary containing configuration settings
        
    Raises:
        FileNotFoundError: If the config file is not found
        json.JSONDecodeError: If the config file is not valid JSON
    """
    with open(config_path, 'r') as config_file:
        return json.load(config_file)


def load_gdrive_config() -> Dict:
    """
    Load the Google Drive configuration with file extensions.
    
    Returns:
        Complete Google Drive configuration dictionary
    """
    gdrive_config_path = PROJECT_ROOT / "project_modules_configs" / "config_dwnload_files" / "dwnload_from_gdrive_conf.json"
    
    if not gdrive_config_path.exists():
        raise FileNotFoundError(f"Google Drive config file not found at {gdrive_config_path}")
        
    with open(gdrive_config_path, 'r') as f:
        gdrive_config = json.load(f)
    
    return gdrive_config


def setup_logging(config: Dict) -> logging.Logger:
    """
    Configure logging based on settings in config.
    
    Args:
        config: Configuration dictionary containing logging settings
        
    Returns:
        Configured logger instance
    """
    log_config = config.get('logging', {})
    log_level = log_config.get('level', 'INFO')
    log_format = log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s')
    log_file = log_config.get('log_file', 'file_utils.log')
    max_bytes = log_config.get('max_size_bytes', 1048576)
    backup_count = log_config.get('backup_count', 3)
    
    # Create logs directory if it doesn't exist
    log_dir = SCRIPT_DIR / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    log_path = log_dir / log_file
    
    logger = logging.getLogger('file_deleter')
    logger.setLevel(getattr(logging, log_level))
    
    # Clear existing handlers to prevent duplicates
    logger.handlers = []
    
    # Create handler for logging to file with rotation
    file_handler = RotatingFileHandler(
        log_path, 
        maxBytes=max_bytes, 
        backupCount=backup_count
    )
    file_handler.setFormatter(logging.Formatter(log_format))
    
    # Create handler for logging to console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def get_supported_extensions(gdrive_config: Dict) -> List[str]:
    """
    Get a list of all supported file extensions from the Google Drive config.
    
    Args:
        gdrive_config: Google Drive configuration dictionary
        
    Returns:
        List of supported file extensions (with leading dot)
    """
    extensions = []
    
    if 'audio_file_types' in gdrive_config and 'include' in gdrive_config['audio_file_types']:
        extensions.extend(gdrive_config['audio_file_types']['include'])
    
    if 'image_file_types' in gdrive_config and 'include' in gdrive_config['image_file_types']:
        extensions.extend(gdrive_config['image_file_types']['include'])
    
    if 'video_file_types' in gdrive_config and 'include' in gdrive_config['video_file_types']:
        extensions.extend(gdrive_config['video_file_types']['include'])
    
    # Ensure all extensions include the dot prefix
    extensions = [ext if ext.startswith('.') else f'.{ext}' for ext in extensions]
    
    return extensions


def delete_files_in_directory(directory: Path, extensions: List[str], logger: logging.Logger) -> Tuple[int, int]:
    """
    Delete files with matching extensions in the specified directory.
    
    Args:
        directory: Path to the directory containing files to delete
        extensions: List of file extensions to match
        logger: Logger instance for logging
        
    Returns:
        Tuple of (files_deleted, files_failed)
    """
    files_deleted = 0
    files_failed = 0
    
    if not directory.exists():
        logger.warning(f"Directory does not exist: {directory}")
        return files_deleted, files_failed
    
    if not directory.is_dir():
        logger.warning(f"Path is not a directory: {directory}")
        return files_deleted, files_failed
    
    logger.info(f"Processing files in directory: {directory}")
    
    # Get all files in the directory
    all_files = [f for f in directory.iterdir() if f.is_file()]
    
    # If extensions is empty, use all files, otherwise filter by extension
    files_to_delete = all_files if not extensions else [f for f in all_files if f.suffix.lower() in extensions]
    
    if not files_to_delete:
        logger.info(f"No matching files found in {directory}")
        return files_deleted, files_failed
    
    logger.info(f"Found {len(files_to_delete)} files to delete in {directory}")
    
    for file_path in files_to_delete:
        try:
            file_path.unlink()
            logger.info(f"Deleted file: {file_path}")
            files_deleted += 1
        except Exception as e:
            logger.error(f"Error deleting file {file_path}: {str(e)}")
            files_failed += 1
    
    return files_deleted, files_failed


def delete_json_files(directory: Path, logger: logging.Logger) -> Tuple[int, int]:
    """
    Delete JSON files in the specified directory.
    
    Args:
        directory: Path to the directory containing JSON files to delete
        logger: Logger instance for logging
        
    Returns:
        Tuple of (files_deleted, files_failed)
    """
    return delete_files_in_directory(directory, ['.json'], logger)


def process_deletions(config: Dict, logger: logging.Logger) -> Tuple[int, int]:
    """
    Process file deletions according to the configuration.
    
    Args:
        config: Configuration dictionary for file paths and settings
        logger: Logger instance
        
    Returns:
        Tuple of (files_deleted, files_failed)
    """
    # Check if deletion is enabled in the config
    delete_enabled = config.get('processing', {}).get('delete_source_files', False)
    if not delete_enabled:
        logger.info("File deletion is disabled in configuration (delete_source_files=false). Exiting without deleting files.")
        return 0, 0
    
    # Load Google Drive config for file extensions
    try:
        gdrive_config = load_gdrive_config()
        extensions = get_supported_extensions(gdrive_config)
        logger.info(f"Using extensions for file matching: {extensions}")
    except Exception as e:
        logger.error(f"Error loading Google Drive config: {str(e)}")
        extensions = []  # If can't load extensions, will match all files
    
    total_deleted = 0
    total_failed = 0
    
    # Get directories from the file_utils config
    source_dirs = config.get('source_directories_files_to_delete', {})
    
    # Process audio files directory
    if 'audio_files_dir' in source_dirs:
        audio_dir = Path(source_dirs['audio_files_dir'])
        audio_extensions = [ext for ext in extensions if ext in gdrive_config.get('audio_file_types', {}).get('include', [])]
        logger.info(f"Processing audio files in {audio_dir}")
        audio_deleted, audio_failed = delete_files_in_directory(audio_dir, audio_extensions, logger)
        total_deleted += audio_deleted
        total_failed += audio_failed
        logger.info(f"Audio files processed: {audio_deleted} deleted, {audio_failed} failed")
    
    # Process JSON files directory
    if 'json_files_dir' in source_dirs:
        json_dir = Path(source_dirs['json_files_dir'])
        logger.info(f"Processing JSON files in {json_dir}")
        json_deleted, json_failed = delete_json_files(json_dir, logger)
        total_deleted += json_deleted
        total_failed += json_failed
        logger.info(f"JSON files processed: {json_deleted} deleted, {json_failed} failed")
    
    # Process video/transcription files directory
    if 'video_files_dir' in source_dirs:
        video_dir = Path(source_dirs['video_files_dir'])
        # Look for .txt files in the transcriptions directory
        logger.info(f"Processing transcription files in {video_dir}")
        video_deleted, video_failed = delete_files_in_directory(video_dir, ['.txt'], logger)
        total_deleted += video_deleted
        total_failed += video_failed
        logger.info(f"Transcription files processed: {video_deleted} deleted, {video_failed} failed")
    
    return total_deleted, total_failed


def main():
    """
    Main function to execute when the script is run directly.
    """
    # Determine the config file path relative to project root
    config_path = PROJECT_ROOT / 'project_modules_configs' / 'config_file_utils' / 'file_utils_config.json'
    
    try:
        # Load configuration
        config = load_config(config_path)
        
        # Setup logging
        logger = setup_logging(config)
        
        logger.info("Starting file deletion process")
        
        # Process file deletions
        files_deleted, files_failed = process_deletions(config, logger)
        
        logger.info(f"Completed file deletion: {files_deleted} files deleted, {files_failed} files failed")
        
        return 0  # Success
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1  # Failure


if __name__ == "__main__":
    sys.exit(main())
