# File Utils Module

A modular utility for moving files from a source directory to multiple target directories based on file extensions.

## Features

- Moves files to different directories based on their extensions (audio, image, or video)
- Fetches file extensions from the principal config file, with fallback to local config
- Configurable source and target directories through a JSON configuration file
- Handles duplicate filenames by adding a counter suffix
- Provides detailed logging
- Can optionally delete source files after moving

## Configuration

The module uses a local JSON configuration file located at `file_utils_config/file_utils_config.json` for directory paths and processing options. 

For file extensions, it first attempts to get them from the principal configuration file at:
`../dwnload_files/config_dwnload_files/config_dwnld_from_gdrive.json`

If the principal config file doesn't exist or cannot be loaded, it falls back to the extensions defined in the local config file.

The configuration includes:

- Source directory path
- Target directory paths for audio, image, and video files
- File extension definitions for each file type (as fallback)
- Processing options (create directories, delete source files)
- Logging configuration

## Extension Handling

The file extensions supported by the module are:

1. **Audio Files**: When enabled, moves files with extensions like `.mp3`, `.wav`, `.m4a`, etc.
2. **Image Files**: When enabled, moves files with extensions like `.jpg`, `.png`, `.gif`, etc.
3. **Video Files**: When enabled, moves files with extensions like `.mp4`, `.avi`, `.mov`, etc.

The actual list of extensions is fetched from the principal Google Drive config file to maintain consistency across the application.

## Usage

### As a Module

```python
from voice_diary.file_utils import load_config, process_files

# Load the configuration
config = load_config('path/to/file_utils_config.json')

# Setup logging (you can implement your own logging if preferred)
from voice_diary.file_utils.mv_files import setup_logging
logger = setup_logging(config)

# Process the files
files_processed, files_failed = process_files(config, logger)
print(f"Files processed: {files_processed}, Files failed: {files_failed}")
```

### As a Script

Run the script directly:

```bash
python -m voice_diary.file_utils.mv_files
```

## Testing

A test script is provided in the `tests` directory that demonstrates how to use the module. It creates test files in the source directory and processes them:

```bash
python -m voice_diary.file_utils.tests.test_mv_files
```

The test script also shows information about where file extensions are being loaded from.

## Directory Structure

```
file_utils/
├── __init__.py           # Package initialization
├── mv_files.py           # Main module functionality
├── file_utils_config/    # Configuration directory
│   └── file_utils_config.json  # Configuration file
├── logs/                 # Logs directory (created automatically)
│   └── file_utils.log    # Log file (created automatically)
├── source_files/         # Source directory for files to process
├── audio_files/          # Target directory for audio files
├── image_files/          # Target directory for image files
├── video_files/          # Target directory for video files
└── tests/                # Test scripts
    └── test_mv_files.py  # Sample test script
``` 