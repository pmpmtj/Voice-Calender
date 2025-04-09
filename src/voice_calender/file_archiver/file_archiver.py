#!/usr/bin/env python3
"""
File archiver utility module.

This module provides functionality to archive files after processing,
with configurable destination directories and behaviors.
"""

import os
import sys
import json
import shutil
import logging
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple

# Configure a basic logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('file_archiver')

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
    with open(config_path, 'r', encoding='utf-8') as config_file:
        return json.load(config_file)

def find_config_file(possible_paths: List[Path]) -> Optional[Path]:
    """
    Find a configuration file from a list of possible paths.
    
    Args:
        possible_paths: List of paths to check
        
    Returns:
        Path to the first existing config file, or None if none found
    """
    for path in possible_paths:
        if path.exists():
            logger.info(f"Found config at: {path}")
            return path
    
    logger.error(f"Config file not found. Tried paths: {[str(p) for p in possible_paths]}")
    return None

def ensure_directory_exists(directory: Path) -> bool:
    """
    Ensure directory exists, creating it if necessary.
    
    Args:
        directory: Path to directory
        
    Returns:
        True if directory exists or was created, False on error
    """
    try:
        directory.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"Error creating directory {directory}: {e}")
        return False

def archive_file(file_path: Path, 
                archive_dir: Path, 
                rename_pattern: Optional[str] = None,
                delete_source: bool = False) -> bool:
    """
    Archive a file by moving or copying it to the archive directory.
    
    Args:
        file_path: Path to the file to archive
        archive_dir: Directory to archive the file to
        rename_pattern: Optional pattern for renaming the file
        delete_source: Whether to delete the source file after copying
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Ensure file exists
        if not file_path.exists():
            logger.error(f"File does not exist: {file_path}")
            return False
            
        # Ensure archive directory exists
        if not ensure_directory_exists(archive_dir):
            return False
        
        # Determine destination file name (with rename pattern if provided)
        if rename_pattern:
            # Get file metadata for pattern substitution
            file_stats = file_path.stat()
            file_info = {
                'name': file_path.stem,
                'ext': file_path.suffix,
                'size': file_stats.st_size,
                'ctime': file_stats.st_ctime,
                'mtime': file_stats.st_mtime
            }
            # TODO: Implement pattern substitution if needed
            dest_name = file_path.name
        else:
            dest_name = file_path.name
            
        # Create destination path
        dest_path = archive_dir / dest_name
        
        # Handle duplicate file names
        if dest_path.exists():
            base_name = dest_path.stem
            extension = dest_path.suffix
            counter = 1
            
            while dest_path.exists():
                new_name = f"{base_name}_{counter}{extension}"
                dest_path = archive_dir / new_name
                counter += 1
        
        # Perform the archive operation
        if delete_source:
            # Use rename/move which is typically more efficient
            shutil.move(str(file_path), str(dest_path))
            logger.info(f"Moved {file_path} to {dest_path}")
        else:
            # Copy the file, preserving metadata
            shutil.copy2(str(file_path), str(dest_path))
            logger.info(f"Copied {file_path} to {dest_path}")
            
        return True
    except Exception as e:
        logger.error(f"Error archiving {file_path}: {e}")
        return False

def batch_archive_files(files: List[Path], 
                       archive_dir: Path,
                       rename_pattern: Optional[str] = None,
                       delete_source: bool = False) -> Tuple[int, int]:
    """
    Archive multiple files to the archive directory.
    
    Args:
        files: List of file paths to archive
        archive_dir: Directory to archive the files to
        rename_pattern: Optional pattern for renaming the files
        delete_source: Whether to delete the source files after copying
        
    Returns:
        Tuple of (success_count, error_count)
    """
    success_count = 0
    error_count = 0
    
    # Ensure archive directory exists
    if not ensure_directory_exists(archive_dir):
        return 0, len(files)
    
    for file_path in files:
        if archive_file(file_path, archive_dir, rename_pattern, delete_source):
            success_count += 1
        else:
            error_count += 1
    
    logger.info(f"Archived {success_count} files, {error_count} failures")
    return success_count, error_count

def main():
    """
    Main function to run the file archiver from command line.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Archive files to a destination directory')
    parser.add_argument('--config', type=str, help='Path to configuration file')
    parser.add_argument('--source', type=str, help='Source file or directory')
    parser.add_argument('--dest', type=str, help='Destination directory')
    parser.add_argument('--delete-source', action='store_true', help='Delete source files after archiving')
    parser.add_argument('--pattern', type=str, help='Rename pattern for archived files')
    
    args = parser.parse_args()
    
    # Load configuration if provided
    config = None
    if args.config:
        try:
            config = load_config(args.config)
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return 1
    
    # Use config or command line arguments
    source_path = Path(args.source) if args.source else None
    if not source_path and config:
        source_path = Path(config.get('source_path', ''))
    
    dest_dir = Path(args.dest) if args.dest else None
    if not dest_dir and config:
        dest_dir = Path(config.get('archive_directory', ''))
    
    delete_source = args.delete_source
    if not delete_source and config:
        delete_source = config.get('delete_source_after_archive', False)
    
    rename_pattern = args.pattern
    if not rename_pattern and config:
        rename_pattern = config.get('rename_pattern')
    
    # Validate inputs
    if not source_path or not source_path.exists():
        logger.error(f"Source path does not exist: {source_path}")
        return 1
    
    if not dest_dir:
        logger.error("Destination directory not specified")
        return 1
    
    # Process files
    if source_path.is_file():
        # Single file
        success = archive_file(source_path, dest_dir, rename_pattern, delete_source)
        return 0 if success else 1
    elif source_path.is_dir():
        # Directory of files
        files = list(source_path.glob('*.*'))
        if not files:
            logger.info(f"No files found in {source_path}")
            return 0
            
        success_count, error_count = batch_archive_files(
            files, dest_dir, rename_pattern, delete_source
        )
        
        return 0 if error_count == 0 else 1
    else:
        logger.error(f"Invalid source path: {source_path}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 