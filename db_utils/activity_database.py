import sqlite3
import os
import logging
import json

# Configure logging for database operations
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# --- Database Path Logic ---
DEV_DATA_DIRECTORY = "/home/mattw/Projects/discord_ticket_manager/data/"
PROD_DATA_DIRECTORY = "/home/container/data/"

# Determine the actual base directory to use
if os.path.exists(os.path.dirname(PROD_DATA_DIRECTORY)):
    ACTUAL_DATA_DIRECTORY = PROD_DATA_DIRECTORY
else:
    ACTUAL_DATA_DIRECTORY = DEV_DATA_DIRECTORY

# Ensure the directory exists
if not os.path.exists(ACTUAL_DATA_DIRECTORY):
    os.makedirs(ACTUAL_DATA_DIRECTORY, exist_ok=True)

DB_PATH = os.path.join(ACTUAL_DATA_DIRECTORY, "activity_check.db")
logging.info(f"activity_checker cog database will be at: {DB_PATH}")

def init_db():
    """Initialize the activity checker database and create tables if they don't exist."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Create the settings table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT DEFAULT 1,
                reminder_message TEXT DEFAULT 'Hey {user.mention}! This is an activity check. Please click the button below to confirm you are active. You have {timeout_minutes} minutes to respond.',
                afk_channel_id INTEGER,
                response_timeout_minutes INTEGER DEFAULT 1,
                excluded_channels TEXT DEFAULT '[]',
                is_enabled INTEGER DEFAULT 1,
                check_interval_minutes INTEGER DEFAULT 30,
                inactive_role_id INTEGER,
                inactive_role_duration_minutes INTEGER DEFAULT 0
            )
        ''')
        
        # Check if the table exists and has data
        cursor.execute('SELECT COUNT(*) FROM settings')
        count = cursor.fetchone()[0]
        
        # Insert default settings if table is empty
        if count == 0:
            cursor.execute('''
                INSERT INTO settings (id, reminder_message, afk_channel_id, response_timeout_minutes, excluded_channels, is_enabled, check_interval_minutes)
                VALUES (1, 'Hey {user.mention}! This is an activity check. Please click the button below to confirm you are active. You have {timeout_minutes} minutes to respond.', NULL, 1, '[]', 1, 30)
            ''')
            logging.info("Inserted default settings into activity checker database.")
        
        # Check for missing columns (for migration from older versions)
        cursor.execute("PRAGMA table_info(settings)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'is_enabled' not in columns:
            cursor.execute('ALTER TABLE settings ADD COLUMN is_enabled INTEGER DEFAULT 1')
            logging.info("Added is_enabled column to existing settings table.")
        
        if 'check_interval_minutes' not in columns:
            cursor.execute('ALTER TABLE settings ADD COLUMN check_interval_minutes INTEGER DEFAULT 30')
            logging.info("Added check_interval_minutes column to existing settings table.")
        
        if 'inactive_role_id' not in columns:
            cursor.execute('ALTER TABLE settings ADD COLUMN inactive_role_id INTEGER')
            logging.info("Added inactive_role_id column to existing settings table.")
        
        if 'inactive_role_duration_minutes' not in columns:
            cursor.execute('ALTER TABLE settings ADD COLUMN inactive_role_duration_minutes INTEGER DEFAULT 0')
            logging.info("Added inactive_role_duration_minutes column to existing settings table.")
        
        conn.commit()
        conn.close()
        logging.info("Activity checker database initialized successfully.")
        
    except sqlite3.Error as e:
        logging.error(f"Error initializing activity checker database: {e}")
        raise

def get_settings():
    """Retrieve the current settings from the database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM settings WHERE id = 1')
        row = cursor.fetchone()
        conn.close()
        
        if row:
            settings = {
                'id': row[0],
                'reminder_message': row[1],
                'afk_channel_id': row[2],
                'response_timeout_minutes': row[3],
                'excluded_channels': json.loads(row[4]) if row[4] else [],
                'is_enabled': bool(row[5]) if len(row) > 5 else True,
                'check_interval_minutes': row[6] if len(row) > 6 else 30,
                'inactive_role_id': row[7] if len(row) > 7 else None,
                'inactive_role_duration_minutes': row[8] if len(row) > 8 else 0
            }
            return settings
        else:
            logging.warning("No settings found in database, using defaults.")
            return {
                'id': 1,
                'reminder_message': 'Hey {user.mention}! This is an activity check. Please click the button below to confirm you are active. You have {timeout_minutes} minutes to respond.',
                'afk_channel_id': None,
                'response_timeout_minutes': 1,
                'excluded_channels': [],
                'is_enabled': True,
                'check_interval_minutes': 30,
                'inactive_role_id': None,
                'inactive_role_duration_minutes': 0
            }
            
    except sqlite3.Error as e:
        logging.error(f"Error retrieving settings from database: {e}")
        raise

def update_setting(key: str, value):
    """Update a specific setting in the database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Handle special serialization cases
        if key == 'excluded_channels':
            value = json.dumps(value)
        elif key == 'is_enabled':
            value = 1 if value else 0
        
        cursor.execute(f'UPDATE settings SET {key} = ? WHERE id = 1', (value,))
        conn.commit()
        conn.close()
        
        logging.info(f"Updated setting '{key}' to '{value}' in activity checker database.")
        
    except sqlite3.Error as e:
        logging.error(f"Error updating setting '{key}' in database: {e}")
        raise

def add_excluded_channel(channel_id: int):
    """Add a channel to the excluded channels list."""
    try:
        settings = get_settings()
        excluded_channels = settings['excluded_channels']
        
        if channel_id not in excluded_channels:
            excluded_channels.append(channel_id)
            update_setting('excluded_channels', excluded_channels)
            logging.info(f"Added channel {channel_id} to excluded channels list.")
        else:
            logging.info(f"Channel {channel_id} is already in excluded channels list.")
            
    except Exception as e:
        logging.error(f"Error adding excluded channel {channel_id}: {e}")
        raise

def remove_excluded_channel(channel_id: int):
    """Remove a channel from the excluded channels list."""
    try:
        settings = get_settings()
        excluded_channels = settings['excluded_channels']
        
        if channel_id in excluded_channels:
            excluded_channels.remove(channel_id)
            update_setting('excluded_channels', excluded_channels)
            logging.info(f"Removed channel {channel_id} from excluded channels list.")
        else:
            logging.info(f"Channel {channel_id} was not in excluded channels list.")
            
    except Exception as e:
        logging.error(f"Error removing excluded channel {channel_id}: {e}")
        raise