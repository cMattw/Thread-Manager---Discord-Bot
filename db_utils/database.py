import sqlite3
import logging
from typing import Optional, List, Tuple, Dict, Any, Set 
from datetime import datetime, timezone 
import os 

DEV_DATA_DIRECTORY = "/home/mattw/Projects/discord_ticket_manager/data/" # Your local development data directory
PROD_DATA_DIRECTORY = "/home/container/data/"    # Container data directory

# Determine the actual base directory to use
if os.path.exists("/home/container/"): # A common way to check if running in your prod container
    ACTUAL_DATA_DIRECTORY = PROD_DATA_DIRECTORY
    logging.info(f"Production environment detected. Using data directory: {ACTUAL_DATA_DIRECTORY}")
else:
    ACTUAL_DATA_DIRECTORY = DEV_DATA_DIRECTORY
    logging.info(f"Development environment detected. Using data directory: {ACTUAL_DATA_DIRECTORY}")

# Ensure the chosen ACTUAL_DATA_DIRECTORY exists
try:
    # Check if ACTUAL_DATA_DIRECTORY is not empty and then if it exists
    if ACTUAL_DATA_DIRECTORY and not os.path.exists(ACTUAL_DATA_DIRECTORY):
        os.makedirs(ACTUAL_DATA_DIRECTORY, exist_ok=True)
        logging.info(f"Successfully created data directory: {ACTUAL_DATA_DIRECTORY}")
except OSError as e:
    logging.error(f"CRITICAL: Could not create data directory {ACTUAL_DATA_DIRECTORY}: {e}")
    # You might want to raise the error or exit if the bot cannot function without this directory
    raise  # Or handle more gracefully if appropriate

# --- Now define your database file names using the ACTUAL_DATA_DIRECTORY ---
DATABASE_MAIN_NAME = os.path.join(ACTUAL_DATA_DIRECTORY, "ticket_bot_settings.db")

# If this same logic applies to your invites database, and it's a separate file:
INVITES_DATABASE_NAME = os.path.join(ACTUAL_DATA_DIRECTORY, "invites_tracker.db") # Example

# Log the final paths being used
logging.info(f"Main database file will be at: {DATABASE_MAIN_NAME}")
if 'INVITES_DATABASE_NAME' in locals(): # Check if defined
    logging.info(f"Invites database file will be at: {INVITES_DATABASE_NAME}")

def get_db_connection(): # Connects to the main database
    conn = sqlite3.connect(DATABASE_MAIN_NAME)
    conn.row_factory = sqlite3.Row 
    return conn

def initialize_database(): # Initializes tables in the main database
    conn = get_db_connection()
    cursor = conn.cursor()

    # 1. General bot settings
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            guild_id INTEGER PRIMARY KEY,
            scan_interval_minutes INTEGER DEFAULT 60,
            delete_delay_days INTEGER DEFAULT 7,
            log_channel_id INTEGER,           -- For TicketManagerCog & other general logs
            announcement_log_channel_id INTEGER -- For AnnouncementCog
        )
    ''')
    # Attempt to add announcement_log_channel_id if it doesn't exist
    try:
        cursor.execute("SELECT announcement_log_channel_id FROM settings LIMIT 1")
    except sqlite3.OperationalError:
        logging.info("Adding 'announcement_log_channel_id' column to settings table.")
        try:
            cursor.execute("ALTER TABLE settings ADD COLUMN announcement_log_channel_id INTEGER DEFAULT NULL")
        except sqlite3.OperationalError as e:
            logging.warning(f"Could not add 'announcement_log_channel_id' column: {e}")

    # 2. Monitored channels for thread manager
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS monitored_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL, channel_id INTEGER NOT NULL,
            UNIQUE (guild_id, channel_id),
            FOREIGN KEY (guild_id) REFERENCES settings(guild_id) ON DELETE CASCADE ON UPDATE CASCADE )
    ''')
    # 3. Exempted threads for thread manager
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS exempted_threads (
            thread_id INTEGER PRIMARY KEY, guild_id INTEGER NOT NULL, exempted_by_user_id INTEGER, exemption_timestamp TEXT )
    ''')
    # 4. Status Monitor Cog Settings (server_tag_role_id removed)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS status_monitor_settings (
            guild_id INTEGER PRIMARY KEY, vanity_phrase TEXT, vanity_role_id INTEGER,
            blacklist_role_id INTEGER, log_channel_id INTEGER,
            FOREIGN KEY (guild_id) REFERENCES settings(guild_id) ON DELETE CASCADE ON UPDATE CASCADE )
    ''')
    # 5. Blacklisted phrases for Status Monitor Cog
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS status_blacklist_phrases (
            id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL, phrase TEXT NOT NULL,
            UNIQUE (guild_id, phrase),
            FOREIGN KEY (guild_id) REFERENCES settings(guild_id) ON DELETE CASCADE ON UPDATE CASCADE )
    ''')
    # 6. Scheduled announcements for Announcement Cog
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scheduled_announcements (
            id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL, channel_id INTEGER, webhook_url TEXT, 
            message_content TEXT, unix_timestamp_to_send INTEGER NOT NULL, created_by_user_id INTEGER NOT NULL,
            creation_timestamp TEXT NOT NULL, sent_status INTEGER DEFAULT 0, attachment_urls TEXT,
            FOREIGN KEY (guild_id) REFERENCES settings(guild_id) ON DELETE CASCADE ON UPDATE CASCADE )
    ''')
    try: # Ensure webhook_url column exists
        cursor.execute("SELECT webhook_url FROM scheduled_announcements LIMIT 1")
    except sqlite3.OperationalError:
        cursor.execute("ALTER TABLE scheduled_announcements ADD COLUMN webhook_url TEXT DEFAULT NULL")
    
    # 7. Saved webhooks for Announcement Cog
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS saved_webhooks (
            id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
            name TEXT NOT NULL COLLATE NOCASE, url TEXT NOT NULL,       
            added_by_user_id INTEGER, added_timestamp TEXT,    
            UNIQUE (guild_id, name), 
            FOREIGN KEY (guild_id) REFERENCES settings(guild_id) ON DELETE CASCADE ON UPDATE CASCADE )
    ''')

    conn.commit() 
    conn.close()
    logging.info(f"Main Database '{DATABASE_MAIN_NAME}' initialized (all tables checked/created).")

# --- General Settings Functions (for settings table) ---
def get_guild_settings(guild_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM settings WHERE guild_id = ?", (guild_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    except sqlite3.Error as e: logging.error(f"DB Error getting guild_settings for {guild_id}: {e}"); return None
    finally: 
        if conn: conn.close()

def update_setting(guild_id: int, key: str, value: Any): 
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO settings (guild_id) VALUES (?)", (guild_id,))
        cursor.execute(f"UPDATE settings SET {key} = ? WHERE guild_id = ?", (value, guild_id))
        conn.commit()
        logging.info(f"Updated general setting '{key}' to '{value}' for guild_id {guild_id}")
    except sqlite3.Error as e: logging.error(f"DB Error updating setting '{key}' for {guild_id}: {e}")
    finally: 
        if conn: conn.close()

def get_all_guild_configs() -> List[Dict[str, Any]]: 
    conn = get_db_connection(); cursor = conn.cursor()
    configs = []
    try:
        cursor.execute("SELECT * FROM settings")
        configs = [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e: logging.error(f"DB Error getting all guild_configs: {e}")
    finally: 
        if conn: conn.close()
    return configs

# --- Monitored Channels Functions ---
def add_monitored_channel(guild_id: int, channel_id: int) -> bool:
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO settings (guild_id) VALUES (?)", (guild_id,))
        cursor.execute("INSERT INTO monitored_channels (guild_id, channel_id) VALUES (?, ?)", (guild_id, channel_id))
        conn.commit(); return True
    except sqlite3.IntegrityError: logging.warning(f"Channel {channel_id} already monitored for {guild_id}."); return False 
    except sqlite3.Error as e: logging.error(f"DB Error adding monitored_channel for {guild_id}: {e}"); return False
    finally: conn.close()

def remove_monitored_channel(guild_id: int, channel_id: int) -> bool:
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM monitored_channels WHERE guild_id = ? AND channel_id = ?", (guild_id, channel_id))
        conn.commit(); return cursor.rowcount > 0
    except sqlite3.Error as e: logging.error(f"DB Error removing monitored_channel for {guild_id}: {e}"); return False
    finally: conn.close()

def get_monitored_channels(guild_id: int) -> List[int]: # This was the function causing an AttributeError
    conn = get_db_connection(); cursor = conn.cursor()
    channels = []
    try:
        cursor.execute("SELECT channel_id FROM monitored_channels WHERE guild_id = ?", (guild_id,))
        channels = [row['channel_id'] for row in cursor.fetchall()]
    except sqlite3.Error as e: logging.error(f"DB Error getting monitored_channels for {guild_id}: {e}")
    finally: conn.close()
    return channels

# --- Exempted Threads Functions ---
def add_exempted_thread(guild_id: int, thread_id: int, user_id: int) -> bool:
    conn = get_db_connection(); cursor = conn.cursor()
    timestamp = datetime.now(timezone.utc).isoformat() 
    try:
        cursor.execute("INSERT OR IGNORE INTO settings (guild_id) VALUES (?)", (guild_id,)) 
        cursor.execute("INSERT OR REPLACE INTO exempted_threads (thread_id, guild_id, exempted_by_user_id, exemption_timestamp) VALUES (?, ?, ?, ?)",
                       (thread_id, guild_id, user_id, timestamp))
        conn.commit(); return True
    except sqlite3.Error as e: logging.error(f"DB Error exempting thread {thread_id}: {e}"); return False
    finally: conn.close()

def remove_exempted_thread(guild_id: int, thread_id: int) -> bool:
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM exempted_threads WHERE thread_id = ? AND guild_id = ?", (thread_id, guild_id))
        conn.commit(); return cursor.rowcount > 0
    except sqlite3.Error as e: logging.error(f"DB Error un-exempting thread {thread_id}: {e}"); return False
    finally: conn.close()

def is_thread_exempted(guild_id: int, thread_id: int) -> bool: 
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("SELECT 1 FROM exempted_threads WHERE thread_id = ? AND guild_id = ?", (thread_id, guild_id))
        return cursor.fetchone() is not None
    except sqlite3.Error as e: logging.error(f"DB Error checking exemption for thread {thread_id}: {e}"); return False 
    finally: conn.close()

def get_exempted_thread_ids_for_guild(guild_id: int) -> Set[int]:
    conn = get_db_connection(); cursor = conn.cursor()
    exempted_ids: Set[int] = set()
    try:
        cursor.execute("SELECT thread_id FROM exempted_threads WHERE guild_id = ?", (guild_id,))
        for row in cursor.fetchall(): exempted_ids.add(row['thread_id'])
    except sqlite3.Error as e: logging.error(f"DB Error fetching exempted_thread_ids for {guild_id}: {e}")
    finally: conn.close()
    return exempted_ids

# --- Status Monitor Cog Settings Functions ---
def get_status_monitor_settings(guild_id: int) -> Optional[Dict[str, Any]]: # This was missing or causing error
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM status_monitor_settings WHERE guild_id = ?", (guild_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    except sqlite3.Error as e: logging.error(f"DB Error getting status_monitor_settings for {guild_id}: {e}"); return None
    finally: 
        if conn: conn.close()

def update_status_monitor_setting(guild_id: int, key: str, value: Any):
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO settings (guild_id) VALUES (?)", (guild_id,)) 
        cursor.execute("INSERT OR IGNORE INTO status_monitor_settings (guild_id) VALUES (?)", (guild_id,))
        cursor.execute(f"UPDATE status_monitor_settings SET {key} = ? WHERE guild_id = ?", (value, guild_id))
        conn.commit()
        logging.info(f"Updated status_monitor_setting '{key}' for {guild_id}")
    except sqlite3.Error as e: logging.error(f"DB Error updating status_monitor_setting '{key}' for {guild_id}: {e}")
    finally: 
        if conn: conn.close()

def add_blacklist_phrase(guild_id: int, phrase: str) -> bool:
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO settings (guild_id) VALUES (?)", (guild_id,)) 
        cursor.execute("INSERT INTO status_blacklist_phrases (guild_id, phrase) VALUES (?, ?)", (guild_id, phrase.lower()))
        conn.commit(); return True
    except sqlite3.IntegrityError: logging.warning(f"Blacklist phrase '{phrase}' already exists for {guild_id}."); return False
    except sqlite3.Error as e: logging.error(f"DB Error adding blacklist_phrase for {guild_id}: {e}"); return False
    finally: conn.close()

def remove_blacklist_phrase(guild_id: int, phrase: str) -> bool:
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM status_blacklist_phrases WHERE guild_id = ? AND phrase = ?", (guild_id, phrase.lower()))
        conn.commit(); return cursor.rowcount > 0
    except sqlite3.Error as e: logging.error(f"DB Error removing blacklist_phrase for {guild_id}: {e}"); return False
    finally: conn.close()

def get_blacklist_phrases(guild_id: int) -> List[str]:
    conn = get_db_connection(); cursor = conn.cursor()
    phrases = []
    try:
        cursor.execute("SELECT phrase FROM status_blacklist_phrases WHERE guild_id = ?", (guild_id,))
        phrases = [row['phrase'] for row in cursor.fetchall()]
    except sqlite3.Error as e: logging.error(f"DB Error getting blacklist_phrases for {guild_id}: {e}")
    finally: conn.close()
    return phrases

# --- Scheduled Announcements Functions ---
def add_scheduled_announcement(guild_id: int, message_content: str, 
                               unix_timestamp_to_send: int, created_by_user_id: int, 
                               channel_id: Optional[int] = None, 
                               webhook_url: Optional[str] = None, 
                               attachment_urls_json: Optional[str] = None) -> Optional[int]:
    conn = get_db_connection(); cursor = conn.cursor()
    creation_ts_str = datetime.now(timezone.utc).isoformat()
    try:
        cursor.execute("INSERT OR IGNORE INTO settings (guild_id) VALUES (?)", (guild_id,))
        cursor.execute("""
            INSERT INTO scheduled_announcements 
            (guild_id, channel_id, webhook_url, message_content, unix_timestamp_to_send, 
             created_by_user_id, creation_timestamp, attachment_urls)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (guild_id, channel_id, webhook_url, message_content, unix_timestamp_to_send, 
              created_by_user_id, creation_ts_str, attachment_urls_json))
        conn.commit(); announcement_id = cursor.lastrowid; return announcement_id
    except sqlite3.Error as e: logging.error(f"DB Error adding scheduled_announcement for {guild_id}: {e}"); return None
    finally: conn.close()

def get_pending_announcements_due(guild_id: int, current_unix_timestamp: int) -> List[Dict[str, Any]]:
    conn = get_db_connection(); cursor = conn.cursor()
    announcements = []
    try:
        cursor.execute("SELECT * FROM scheduled_announcements WHERE guild_id = ? AND sent_status = 0 AND unix_timestamp_to_send <= ? ORDER BY unix_timestamp_to_send ASC", (guild_id, current_unix_timestamp))
        announcements = [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e: logging.error(f"DB Error getting pending_announcements_due for {guild_id}: {e}")
    finally: conn.close()
    return announcements

def update_announcement_status(announcement_id: int, new_status: int):
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("UPDATE scheduled_announcements SET sent_status = ? WHERE id = ?", (new_status, announcement_id))
        conn.commit()
    except sqlite3.Error as e: logging.error(f"DB Error updating announcement_status for ID {announcement_id}: {e}")
    finally: conn.close()

def get_all_guild_announcements(guild_id: int, pending_only: bool = False) -> List[Dict[str, Any]]:
    conn = get_db_connection(); cursor = conn.cursor()
    announcements = []; query = "SELECT * FROM scheduled_announcements WHERE guild_id = ?"
    params: Tuple[Any, ...] = (guild_id,)
    if pending_only: query += " AND sent_status = 0"
    query += " ORDER BY unix_timestamp_to_send ASC"
    try:
        cursor.execute(query, params)
        announcements = [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e: logging.error(f"DB Error getting all_guild_announcements for {guild_id}: {e}")
    finally: conn.close()
    return announcements

def delete_pending_announcement(announcement_id: int, guild_id: int) -> bool:
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM scheduled_announcements WHERE id = ? AND guild_id = ? AND sent_status = 0", (announcement_id, guild_id))
        conn.commit(); return cursor.rowcount > 0
    except sqlite3.Error as e: logging.error(f"DB Error deleting pending_announcement ID {announcement_id}: {e}"); return False
    finally: conn.close()

# --- Saved Webhooks Functions ---
def add_saved_webhook(guild_id: int, name: str, url: str, user_id: int) -> bool:
    conn = get_db_connection(); cursor = conn.cursor()
    timestamp = datetime.now(timezone.utc).isoformat()
    try:
        cursor.execute("INSERT OR IGNORE INTO settings (guild_id) VALUES (?)", (guild_id,))
        cursor.execute("INSERT INTO saved_webhooks (guild_id, name, url, added_by_user_id, added_timestamp) VALUES (?, ?, ?, ?, ?)",
                       (guild_id, name.lower(), url, user_id, timestamp))
        conn.commit(); return True
    except sqlite3.IntegrityError: logging.warning(f"Webhook name '{name.lower()}' already exists for {guild_id}."); return False
    except sqlite3.Error as e: logging.error(f"DB Error adding saved_webhook '{name.lower()}': {e}"); return False
    finally: conn.close()

def remove_saved_webhook(guild_id: int, name: str) -> bool:
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM saved_webhooks WHERE guild_id = ? AND name = ?", (guild_id, name.lower()))
        conn.commit(); return cursor.rowcount > 0
    except sqlite3.Error as e: logging.error(f"DB Error removing saved_webhook '{name.lower()}': {e}"); return False
    finally: conn.close()

def get_saved_webhook_by_name(guild_id: int, name: str) -> Optional[Dict[str, Any]]:
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM saved_webhooks WHERE guild_id = ? AND name = ?", (guild_id, name.lower()))
        row = cursor.fetchone(); return dict(row) if row else None
    except sqlite3.Error as e: logging.error(f"DB Error getting saved_webhook_by_name '{name.lower()}': {e}"); return None
    finally: conn.close()

def get_all_saved_webhooks(guild_id: int) -> List[Dict[str, Any]]:
    conn = get_db_connection(); cursor = conn.cursor()
    webhooks = []
    try:
        cursor.execute("SELECT id, name, url FROM saved_webhooks WHERE guild_id = ? ORDER BY name ASC", (guild_id,))
        webhooks = [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e: logging.error(f"DB Error getting all_saved_webhooks for {guild_id}: {e}")
    finally: conn.close()
    return webhooks


if __name__ == '__main__':
    print("Running MAIN database module directly for initialization/testing...")
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - [%(module)s.%(funcName)s:%(lineno)d] %(message)s')
    initialize_database() # Initializes tables in ticket_bot_settings.db
    
    # If you also have invites_database.py and want to test its initialization
    # you would run that file directly, or call its init function here if merged.
    # Since we decided to keep invites_database.py separate, we don't call its init here.
    print("\nMAIN Database module direct execution finished.")