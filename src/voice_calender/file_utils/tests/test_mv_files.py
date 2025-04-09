#!/usr/bin/env python3
"""
Test script for file mover utility.

This script demonstrates using the file mover utility to move files based on their extensions.
"""

import os
import sys
import json
import logging
from pathlib import Path

# Handle both frozen (PyInstaller) and regular Python execution
SCRIPT_DIR = Path(sys._MEIPASS) if getattr(sys, 'frozen', False) else Path(__file__).resolve().parent
parent_dir = SCRIPT_DIR.parent

# Add parent directory to path to allow imports when run directly
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Try package import first, then fallback to relative import if running as standalone script
try:
    from voice_diary.file_utils.mv_files import load_config, setup_logging, process_files, load_gdrive_config
except ImportError:
    # Fallback to direct import when running as a standalone script
    from voice_diary.file_utils.mv_files import load_config, setup_logging, process_files, load_gdrive_config


def create_test_files(source_dir: Path):
    """Create some test files in the source directory."""
    # Create source directory if it doesn't exist
    source_dir.mkdir(parents=True, exist_ok=True)
    
    # Create some test files of different types
    test_files = [
        # Audio files
        source_dir / "test_audio1.mp3",
        source_dir / "test_audio2.wav",
        # Image files
        source_dir / "test_image1.jpg",
        source_dir / "test_image2.png",
        # Video files
        source_dir / "test_video1.mp4",
        source_dir / "test_video2.avi",
        # Unknown extension
        source_dir / "test_unknown.xyz"
    ]
    
    # Create each file
    for file_path in test_files:
        if not file_path.exists():
            with open(file_path, 'w') as f:
                f.write(f"Test file content for {file_path.name}")
    
    return test_files


def print_extensions_source():
    """Print information about where the extensions are loaded from."""
    # Find the expected paths
    project_root = SCRIPT_DIR.parent.parent
    local_config_path = project_root / "project_modules_configs" / "config_file_utils" / "file_utils_config.json"
    gdrive_config_path = project_root / "project_modules_configs" / "config_dwnload_files" / "dwnload_from_gdrive_conf.json"
    
    print("\nExtension Sources:")
    print(f"Local config path: {local_config_path}")
    print(f"  Exists: {local_config_path.exists()}")
    
    print(f"Google Drive config path: {gdrive_config_path}")
    print(f"  Exists: {gdrive_config_path.exists()}")
    
    try:
        # Get the complete Google Drive config
        gdrive_config = load_gdrive_config()
        
        print("\nExtensions from Google Drive config:")
        print(f"  Audio: {gdrive_config['audio_file_types']['include'] if gdrive_config['audio_file_types']['include'] else 'None'}")
        print(f"  Image: {gdrive_config['image_file_types']['include'] if gdrive_config['image_file_types']['include'] else 'None'}")
        print(f"  Video: {gdrive_config['video_file_types']['include'] if gdrive_config['video_file_types']['include'] else 'None'}")
        
        # Get enabled flags from local config
        if local_config_path.exists():
            with open(local_config_path, 'r') as f:
                local_config = json.load(f)
                
            print("\nEnabled flags from local config:")
            print(f"  Audio: {local_config['audio_file_types']['enabled']}")
            print(f"  Image: {local_config['image_file_types']['enabled']}")
            print(f"  Video: {local_config['video_file_types']['enabled']}")
    except Exception as e:
        print(f"Error loading configs: {str(e)}")


def run_test():
    """Run a test of the file mover utility."""
    # Show extensions source information
    print_extensions_source()
    
    # Get the path to the configuration file
    config_path = SCRIPT_DIR.parent.parent / 'project_modules_configs' / 'config_file_utils' / 'file_utils_config.json'
    
    try:
        # Load configuration
        config = load_config(config_path)
        
        # Get source directory
        source_dir = Path(config['source_directory']['source_dir'])
        
        # Create test files
        print("\nCreating test files...")
        created_files = create_test_files(source_dir)
        print(f"Created {len(created_files)} test files in {source_dir}")
        
        # Setup logging
        logger = setup_logging(config)
        
        # Process files
        print("\nProcessing files...")
        files_processed, files_failed = process_files(config, logger)
        
        print(f"\nCompleted processing: {files_processed} files processed, {files_failed} files failed")
        
        return 0  # Success
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1  # Failure


# Allow running as a module or as a standalone script
if __name__ == "__main__":
    sys.exit(run_test()) 