"""
Gmail API Email Sender

This script uses the Gmail API to send emails. It requires:
1. OAuth2 credentials (credentials_gmail.json)
2. Email configuration (email_config.json)

The script will:
1. Load email settings from email_config.json
2. Authenticate with Gmail API using Gmail-specific credentials
3. Send the configured email with optional attachments
"""

import os
import json
import base64
import re
import sys
import logging
from logging.handlers import RotatingFileHandler
import pickle
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

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

# Updated path to the configuration file
CONFIG_DIR = PROJECT_ROOT / "project_modules_configs" / "config_send_email"
CONFIG_FILE = CONFIG_DIR / "email_config.json"

# Define ensure_directory_exists before it's used
def ensure_directory_exists(directory_path, purpose="directory"):
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        directory_path: Path to the directory to ensure exists
        purpose: Description of the directory for logging messages
        
    Returns:
        True if directory exists or was created successfully
    """
    dir_path = Path(directory_path)
    if dir_path.exists():
        return True
        
    try:
        print(f"Creating {purpose}: {dir_path}")
        dir_path.mkdir(parents=True, exist_ok=True)
        
        # Use print initially, and if logger is available, use it too
        print(f"Created {purpose}: {dir_path}")
        
        # Try to use logger if it's been defined
        logger_obj = globals().get('logger')
        if logger_obj:
            logger_obj.info(f"Created {purpose}: {dir_path}")
            
        return True
    except Exception as e:
        error_msg = f"ERROR: Failed to create {purpose}: {dir_path}. Error: {str(e)}"
        print(error_msg)
        
        # Try to use logger if it's been defined
        logger_obj = globals().get('logger')
        if logger_obj:
            logger_obj.error(error_msg)
            
        print("This directory is required for the script to function. Exiting.")
        sys.exit(1)

# Load configuration
try:
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            CONFIG = json.load(f)
    else:
        print(f"ERROR: Config file not found at {CONFIG_FILE}")
        print("Please ensure a valid config file exists before running this script.")
        sys.exit(1)
except json.JSONDecodeError:
    print(f"ERROR: Invalid JSON in config file {CONFIG_FILE}")
    sys.exit(1)

# Configure logging
logging_config = CONFIG.get("logging", {})
log_level = getattr(logging, logging_config.get("level", "INFO"))
log_format = logging_config.get("format", "%(asctime)s - %(levelname)s - %(message)s")
log_file = logging_config.get("log_file", "send_email.log")
max_size = logging_config.get("max_size_bytes", 1048576)  # Default 1MB
backup_count = logging_config.get("backup_count", 3)

# Set up logger
logger = logging.getLogger("voice_calender.email")
logger.setLevel(log_level)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(log_format))
logger.addHandler(console_handler)

# File handler with rotation
log_path = LOGS_DIR / log_file
file_handler = RotatingFileHandler(
    log_path, maxBytes=max_size, backupCount=backup_count
)
file_handler.setFormatter(logging.Formatter(log_format))
logger.addHandler(file_handler)

# Log initial information
logger.info("Voice Calendar Email Service")
logger.info(f"Logging to {log_path}")

def validate_email_format(email):
    """Validate email address format using regex with additional domain duplication check"""
    # Basic RFC 5322 compliant email regex pattern
    pattern = r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$"
    
    if not re.match(pattern, email):
        return False
    
    # Additional checks for common mistakes
    
    # Check for domain duplication (.com.com, .org.org, etc.)
    domain_parts = email.split('@')[1].split('.')
    for i in range(len(domain_parts) - 1):
        if domain_parts[i] == domain_parts[i+1]:
            return False
            
    # Check for repeated TLDs (e.g., .com.com, .net.net)
    tld_parts = email.split('.')
    if len(tld_parts) >= 3:  # Has at least 2 dots
        if tld_parts[-1] == tld_parts[-2]:
            return False
    
    return True

def load_email_config():
    """Load email configuration from email_config.json file"""
    try:
        # Check if email sending is enabled
        if not CONFIG.get('send_email', False):
            logger.info("Email sending is disabled in config")
            return None, CONFIG
            
        email_config = CONFIG.get('email', {})
        
        # Validate email format if enabled
        if CONFIG.get('validate_email', False) and 'to' in email_config:
            recipient = email_config['to']
            if not validate_email_format(recipient):
                logger.error(f"Invalid email format: {recipient}")
                raise ValueError(f"Invalid email format: {recipient}")
                
        return email_config, CONFIG
    except Exception as e:
        logger.error(f"Error loading email config: {str(e)}")
        raise Exception(f"Error loading email config: {str(e)}")

# Get credentials paths from config or use defaults
def get_credentials_paths(config):
    """
    Get credentials file paths with mobile/portable support.
    
    Lookup order:
    1. Default location: SCRIPT_DIR/credentials/[filename from config]
    2. If path in config, interpret as either:
       a. Absolute path if it starts with drive letter
       b. Relative path from SCRIPT_DIR if not absolute
    
    Returns:
        Tuple of (credentials_file_path, token_file_path)
    """
    # Get filenames from config
    credentials_filename = config['auth'].get('credentials_file', 'credentials_gmail.json')
    token_filename = config['auth'].get('token_file', 'token_gmail.pickle')
    
    # Default credentials directory is under SCRIPT_DIR
    default_creds_dir = SCRIPT_DIR / "credentials"
    
    # First, check if credentials_path exists in config
    if config and 'credentials_path' in config:
        creds_path = config['credentials_path']
        
        # Determine if it's absolute or relative
        if os.path.isabs(creds_path):
            # It's an absolute path, use it directly
            full_creds_path = Path(creds_path)
            creds_dir = full_creds_path.parent
        else:
            # It's a relative path, make it relative to SCRIPT_DIR
            creds_dir = SCRIPT_DIR / Path(creds_path).parent
            full_creds_path = creds_dir / Path(creds_path).name
        
        # Create token path in same directory
        token_path = creds_dir / token_filename
        
        # Check if the file exists
        if full_creds_path.exists():
            logger.info(f"Using credentials from config path: {full_creds_path}")
            return full_creds_path, token_path
        else:
            logger.warning(f"Credentials file specified in config not found: {full_creds_path}")
    
    # If we get here, use the default location
    ensure_directory_exists(default_creds_dir, "credentials directory")
    default_creds_path = default_creds_dir / credentials_filename
    default_token_path = default_creds_dir / token_filename
    
    logger.info(f"Using default credentials location: {default_creds_path}")
    return default_creds_path, default_token_path

def check_credentials_file():
    """Check if credentials.json exists and provide help if not."""
    credentials_file, token_file = get_credentials_paths(CONFIG)
    
    if not credentials_file.exists():
        logger.error(f"'{credentials_file}' file not found!")
        
        # Get just the filename without the path
        credentials_filename = Path(credentials_file).name
        credentials_dir = Path(credentials_file).parent
        
        print("\nCredential file not found. Please do one of the following:")
        print(f"1. Create the directory {credentials_dir} if it doesn't exist")
        print(f"2. Place '{credentials_filename}' in: {credentials_dir}")
        print(f"3. Or update the 'credentials_path' in your config file ({CONFIG_FILE})")
        print("\nTo create your Gmail credentials file:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Create a project or select an existing one")
        print("3. Enable the Gmail API:")
        print("   - Navigate to 'APIs & Services' > 'Library'")
        print("   - Search for 'Gmail API' and enable it")
        print("4. Create OAuth credentials:")
        print("   - Go to 'APIs & Services' > 'Credentials'")
        print("   - Click 'Create Credentials' > 'OAuth client ID'")
        print("   - Select 'Desktop app' as application type")
        print(f"   - Download the JSON file and rename it to '{credentials_filename}'")
        print(f"   - Place it in: {credentials_dir}")
        print("\nThen run this script again.")
        return False
    return True

def authenticate_gmail():
    """Authenticate with Gmail API using OAuth."""
    try:
        credentials_file, token_file = get_credentials_paths(CONFIG)
        logger.info(f"Using credentials from: {credentials_file}")
        logger.info(f"Using token file: {token_file}")
        
        creds = None
        
        # The token file stores the user's access and refresh tokens
        if token_file.exists():
            logger.info(f"Found existing token file")
            with open(token_file, 'rb') as token:
                creds = pickle.load(token)
                
        # If no valid credentials are available, let the user log in
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logger.info("Token expired, refreshing...")
                creds.refresh(Request())
            else:
                if not check_credentials_file():
                    sys.exit(1)
                
                logger.info("Starting OAuth flow...")
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(credentials_file), CONFIG['api']['scopes'])
                creds = flow.run_local_server(port=0)
                
            # Save the credentials for the next run
            token_dir = token_file.parent
            ensure_directory_exists(token_dir, "token directory")
                
            with open(token_file, 'wb') as token:
                pickle.dump(creds, token)
        
        # Build the service with the credentials
        service = build('gmail', 'v1', credentials=creds)
        return service
    except Exception as e:
        logger.error(f"Authentication error: {str(e)}")
        raise

def create_message(sender, to, subject, message_text):
    """Create a message for an email."""
    message = MIMEText(message_text)
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')}

def create_message_with_attachment(sender, to, subject, message_text, attachment_path=None):
    """Create a message for an email with optional attachment."""
    message = MIMEMultipart()
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject

    # Add the message body
    msg = MIMEText(message_text)
    message.attach(msg)

    # Add attachment if provided
    if attachment_path and os.path.exists(attachment_path):
        try:
            with open(attachment_path, 'rb') as f:
                attachment = MIMEApplication(f.read())
                attachment.add_header(
                    'Content-Disposition', 
                    'attachment', 
                    filename=os.path.basename(attachment_path)
                )
                message.attach(attachment)
                logger.info(f"Attached file: {attachment_path}")
        except Exception as e:
            logger.error(f"Error attaching file {attachment_path}: {str(e)}")

    return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')}

def send_message(service, user_id, message):
    """Send an email message."""
    try:
        sent_message = service.users().messages().send(
            userId=user_id,
            body=message
        ).execute()
        logger.info(f'Message sent successfully, Message Id: {sent_message["id"]}')
        return sent_message
    except Exception as e:
        logger.error(f'Error sending message: {e}')
        return None

def main():
    """Main function to send email via Gmail API."""
    if not check_credentials_file():
        return
    
    try:
        # Load email configuration
        email_config, config = load_email_config()
        
        # Exit if email sending is disabled
        if not email_config:
            logger.info("Email sending is disabled in config. Exiting.")
            return
        
        # Authenticate with Gmail
        logger.info("Authenticating with Gmail...")
        service = authenticate_gmail()
        if not service:
            logger.error("Failed to authenticate with Gmail.")
            return

        # Get the authenticated user's email address
        user_profile = service.users().getProfile(userId='me').execute()
        sender = user_profile['emailAddress']
        logger.info(f"Authenticated as: {sender}")
        
        # Check if we need to send with attachment
        has_attachment = 'attachment' in config.get('email', {})
        attachment_path = config.get('email', {}).get('attachment')
        
        # Create the email
        if has_attachment and attachment_path:
            logger.info(f"Creating email with attachment: {attachment_path}")
            message = create_message_with_attachment(
                sender,
                email_config['to'],
                email_config['subject'],
                email_config['message'],
                attachment_path
            )
        else:
            logger.info("Creating plain email message")
            message = create_message(
                sender,
                email_config['to'],
                email_config['subject'],
                email_config['message']
            )
        
        # Send the email
        logger.info(f"Sending email to: {email_config['to']}")
        result = send_message(service, 'me', message)
        
        if result:
            logger.info("Email sent successfully!")
        else:
            logger.error("Failed to send email.")
            
    except Exception as e:
        logger.exception(f"An error occurred during the email sending process: {str(e)}")
        return 1
        
    return 0

if __name__ == "__main__":
    main() 