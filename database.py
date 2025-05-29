import sqlite3
import logging
from typing import Optional, List, Tuple, Dict, Any, Set
from datetime import datetime, timezone

DATABASE_NAME = '/home/container/data/ticket_bot_settings.db'

def get_db_connection():
    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = sqlite3.Row 
    return conn

def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()

    # 1. General bot settings
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            guild_id INTEGER PRIMARY KEY,
            scan_interval_minutes INTEGER DEFAULT 60,
            delete_delay_days INTEGER DEFAULT 7,
            log_channel_id INTEGER,           -- For TicketManagerCog (main/default)
            announcement_log_channel_id INTEGER -- For AnnouncementCog
        )
    ''')

    # Attempt to add announcement_log_channel_id if it doesn't exist (for existing dbs)
    try:
        cursor.execute("SELECT announcement_log_channel_id FROM settings LIMIT 1")
    except sqlite3.OperationalError:
        logging.info("Adding 'announcement_log_channel_id' column to settings table.")
        try:
            cursor.execute("ALTER TABLE settings ADD COLUMN announcement_log_channel_id INTEGER DEFAULT NULL")
            conn.commit() # Commit alter table immediately
        except sqlite3.OperationalError as e:
            logging.warning(f"Could not add 'announcement_log_channel_id' column (may already exist or other issue): {e}")

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
    # 4. Status Monitor Cog Settings
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
    # 6. Scheduled announcements for Announcement Cog - Modified for webhook_url
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scheduled_announcements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            channel_id INTEGER, -- Now NULLABLE, one of channel_id or webhook_url should be set
            webhook_url TEXT,   -- NEW, NULLABLE
            message_content TEXT,
            unix_timestamp_to_send INTEGER NOT NULL,
            created_by_user_id INTEGER NOT NULL,
            creation_timestamp TEXT NOT NULL, 
            sent_status INTEGER DEFAULT 0, 
            attachment_urls TEXT, 
            FOREIGN KEY (guild_id) REFERENCES settings(guild_id) 
                ON DELETE CASCADE ON UPDATE CASCADE
        )
    ''')
    # Attempt to add webhook_url column to scheduled_announcements if it doesn't exist
    try:
        cursor.execute("SELECT webhook_url FROM scheduled_announcements LIMIT 1")
    except sqlite3.OperationalError:
        logging.info("Adding 'webhook_url' column to scheduled_announcements table.")
        try:
            cursor.execute("ALTER TABLE scheduled_announcements ADD COLUMN webhook_url TEXT DEFAULT NULL")
            # Note: Making channel_id nullable if it was previously NOT NULL requires more complex migration
            # if data integrity (existing rows having NULL channel_id) is a concern.
            # For new setups or if user accepts potential data loss/reconfiguration, this is simpler.
            # The CREATE TABLE above defines it as nullable from the start for new DBs.
            conn.commit()
        except sqlite3.OperationalError as e:
             logging.warning(f"Could not add 'webhook_url' column to scheduled_announcements (may already exist or other issue): {e}")


    # 7. Saved webhooks for Announcement Cog
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS saved_webhooks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            name TEXT NOT NULL COLLATE NOCASE, -- Store name case-insensitively for lookups
            url TEXT NOT NULL,       
            added_by_user_id INTEGER,
            added_timestamp TEXT,    
            UNIQUE (guild_id, name), 
            FOREIGN KEY (guild_id) REFERENCES settings(guild_id) 
                ON DELETE CASCADE ON UPDATE CASCADE
        )
    ''')

    conn.commit() 
    conn.close()
    logging.info("Database initialized (all tables checked/created, including saved_webhooks and schema updates).")

# --- General Settings Functions ---
def get_guild_settings(guild_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection(); cursor = conn.cursor()
    cursor.execute("SELECT * FROM settings WHERE guild_id = ?", (guild_id,))
    row = cursor.fetchone(); conn.close()
    return dict(row) if row else None

def update_setting(guild_id: int, key: str, value: Any): 
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO settings (guild_id) VALUES (?)", (guild_id,))
        cursor.execute(f"UPDATE settings SET {key} = ? WHERE guild_id = ?", (value, guild_id))
        conn.commit()
        logging.info(f"Updated general setting '{key}' to '{value}' for guild_id {guild_id}")
    except sqlite3.OperationalError as e:
        logging.error(f"SQLite error in update_setting for key '{key}' in guild {guild_id}: {e}. Ensure column exists.")
    except sqlite3.Error as e:
        logging.error(f"General SQLite error in update_setting for guild {guild_id}, key {key}: {e}")
    finally: 
        if conn: conn.close()

def get_all_guild_configs() -> List[Dict[str, Any]]: 
    conn = get_db_connection(); cursor = conn.cursor()
    cursor.execute("SELECT * FROM settings")
    configs = [dict(row) for row in cursor.fetchall()]; conn.close()
    return configs

# --- Monitored Channels Functions ---
def add_monitored_channel(guild_id: int, channel_id: int) -> bool:
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO settings (guild_id) VALUES (?)", (guild_id,))
        cursor.execute("INSERT INTO monitored_channels (guild_id, channel_id) VALUES (?, ?)", (guild_id, channel_id))
        conn.commit(); return True
    except sqlite3.IntegrityError: logging.warning(f"Channel {channel_id} already monitored for {guild_id}."); return False 
    finally: conn.close()

def remove_monitored_channel(guild_id: int, channel_id: int) -> bool:
    conn = get_db_connection(); cursor = conn.cursor()
    cursor.execute("DELETE FROM monitored_channels WHERE guild_id = ? AND channel_id = ?", (guild_id, channel_id))
    conn.commit(); affected = cursor.rowcount > 0; conn.close()
    return affected

def get_monitored_channels(guild_id: int) -> List[int]:
    conn = get_db_connection(); cursor = conn.cursor()
    cursor.execute("SELECT channel_id FROM monitored_channels WHERE guild_id = ?", (guild_id,))
    channels = [row['channel_id'] for row in cursor.fetchall()]; conn.close()
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
    except sqlite3.Error as e: logging.error(f"SQLite error exempting thread {thread_id}: {e}"); return False
    finally: conn.close()

def remove_exempted_thread(guild_id: int, thread_id: int) -> bool:
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM exempted_threads WHERE thread_id = ? AND guild_id = ?", (thread_id, guild_id))
        conn.commit(); return cursor.rowcount > 0
    except sqlite3.Error as e: logging.error(f"SQLite error un-exempting thread {thread_id}: {e}"); return False
    finally: conn.close()

def is_thread_exempted(guild_id: int, thread_id: int) -> bool: 
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("SELECT 1 FROM exempted_threads WHERE thread_id = ? AND guild_id = ?", (thread_id, guild_id))
        return cursor.fetchone() is not None
    except sqlite3.Error as e: logging.error(f"SQLite error checking exemption for thread {thread_id}: {e}"); return False 
    finally: conn.close()

def get_exempted_thread_ids_for_guild(guild_id: int) -> Set[int]:
    conn = get_db_connection(); cursor = conn.cursor()
    exempted_ids: Set[int] = set()
    try:
        cursor.execute("SELECT thread_id FROM exempted_threads WHERE guild_id = ?", (guild_id,))
        for row in cursor.fetchall(): exempted_ids.add(row['thread_id'])
    except sqlite3.Error as e: logging.error(f"SQLite error fetching exempted threads for guild {guild_id}: {e}")
    finally: conn.close()
    return exempted_ids

# --- Status Monitor Cog Settings Functions ---
def get_status_monitor_settings(guild_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection(); cursor = conn.cursor()
    cursor.execute("SELECT * FROM status_monitor_settings WHERE guild_id = ?", (guild_id,))
    row = cursor.fetchone(); conn.close()
    return dict(row) if row else None

def update_status_monitor_setting(guild_id: int, key: str, value: Any):
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO settings (guild_id) VALUES (?)", (guild_id,)) 
        cursor.execute("INSERT OR IGNORE INTO status_monitor_settings (guild_id) VALUES (?)", (guild_id,))
        cursor.execute(f"UPDATE status_monitor_settings SET {key} = ? WHERE guild_id = ?", (value, guild_id))
        conn.commit()
    except sqlite3.Error as e: logging.error(f"SQLite error updating status_monitor_setting '{key}' for guild {guild_id}: {e}")
    finally: conn.close()

def add_blacklist_phrase(guild_id: int, phrase: str) -> bool:
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO settings (guild_id) VALUES (?)", (guild_id,)) 
        cursor.execute("INSERT INTO status_blacklist_phrases (guild_id, phrase) VALUES (?, ?)", (guild_id, phrase.lower()))
        conn.commit(); return True
    except sqlite3.IntegrityError: logging.warning(f"Blacklist phrase '{phrase}' already exists for guild {guild_id}."); return False
    except sqlite3.Error as e: logging.error(f"SQLite error adding blacklist phrase for guild {guild_id}: {e}"); return False
    finally: conn.close()

def remove_blacklist_phrase(guild_id: int, phrase: str) -> bool:
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM status_blacklist_phrases WHERE guild_id = ? AND phrase = ?", (guild_id, phrase.lower()))
        conn.commit(); affected = cursor.rowcount > 0
        return affected
    except sqlite3.Error as e: logging.error(f"SQLite error removing blacklist phrase for guild {guild_id}: {e}"); return False
    finally: conn.close()

def get_blacklist_phrases(guild_id: int) -> List[str]:
    conn = get_db_connection(); cursor = conn.cursor()
    phrases = []
    try:
        cursor.execute("SELECT phrase FROM status_blacklist_phrases WHERE guild_id = ?", (guild_id,))
        phrases = [row['phrase'] for row in cursor.fetchall()]
    except sqlite3.Error as e: logging.error(f"SQLite error fetching blacklist phrases for guild {guild_id}: {e}")
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
        conn.commit(); announcement_id = cursor.lastrowid
        return announcement_id
    except sqlite3.Error as e: logging.error(f"SQLite error adding scheduled announcement for guild {guild_id}: {e}"); return None
    finally: conn.close()

def get_pending_announcements_due(guild_id: int, current_unix_timestamp: int) -> List[Dict[str, Any]]:
    conn = get_db_connection(); cursor = conn.cursor()
    announcements = []
    try:
        cursor.execute("""
            SELECT * FROM scheduled_announcements 
            WHERE guild_id = ? AND sent_status = 0 AND unix_timestamp_to_send <= ?
            ORDER BY unix_timestamp_to_send ASC
        """, (guild_id, current_unix_timestamp))
        announcements = [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e: logging.error(f"SQLite error fetching pending due announcements for guild {guild_id}: {e}")
    finally: conn.close()
    return announcements

def update_announcement_status(announcement_id: int, new_status: int):
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("UPDATE scheduled_announcements SET sent_status = ? WHERE id = ?", (new_status, announcement_id))
        conn.commit()
    except sqlite3.Error as e: logging.error(f"SQLite error updating announcement ID {announcement_id} status: {e}")
    finally: conn.close()

def get_all_guild_announcements(guild_id: int, pending_only: bool = False) -> List[Dict[str, Any]]:
    conn = get_db_connection(); cursor = conn.cursor()
    announcements = []
    query = "SELECT * FROM scheduled_announcements WHERE guild_id = ?"
    params: Tuple[Any, ...] = (guild_id,)
    if pending_only: query += " AND sent_status = 0"
    query += " ORDER BY unix_timestamp_to_send ASC"
    try:
        cursor.execute(query, params)
        announcements = [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e: logging.error(f"SQLite error fetching announcements for guild {guild_id}: {e}")
    finally: conn.close()
    return announcements

def delete_pending_announcement(announcement_id: int, guild_id: int) -> bool:
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM scheduled_announcements WHERE id = ? AND guild_id = ? AND sent_status = 0", 
                       (announcement_id, guild_id))
        conn.commit(); affected = cursor.rowcount > 0
        return affected
    except sqlite3.Error as e: logging.error(f"SQLite error deleting announcement ID {announcement_id} for guild {guild_id}: {e}"); return False
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
    except sqlite3.IntegrityError: logging.warning(f"Webhook name '{name.lower()}' already exists for guild {guild_id}."); return False
    except sqlite3.Error as e: logging.error(f"SQLite error saving webhook '{name.lower()}': {e}"); return False
    finally: conn.close()

def remove_saved_webhook(guild_id: int, name: str) -> bool:
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM saved_webhooks WHERE guild_id = ? AND name = ?", (guild_id, name.lower()))
        conn.commit(); return cursor.rowcount > 0
    except sqlite3.Error as e: logging.error(f"SQLite error removing webhook '{name.lower()}': {e}"); return False
    finally: conn.close()

def get_saved_webhook_by_name(guild_id: int, name: str) -> Optional[Dict[str, Any]]:
    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM saved_webhooks WHERE guild_id = ? AND name = ?", (guild_id, name.lower()))
        row = cursor.fetchone(); return dict(row) if row else None
    except sqlite3.Error as e: logging.error(f"SQLite error fetching webhook '{name.lower()}': {e}"); return None
    finally: conn.close()

def get_all_saved_webhooks(guild_id: int) -> List[Dict[str, Any]]:
    conn = get_db_connection(); cursor = conn.cursor()
    webhooks = []
    try:
        cursor.execute("SELECT id, name, url FROM saved_webhooks WHERE guild_id = ? ORDER BY name ASC", (guild_id,))
        webhooks = [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e: logging.error(f"SQLite error fetching all saved webhooks for guild {guild_id}: {e}")
    finally: conn.close()
    return webhooks


if __name__ == '__main__':
    print("Running database module directly for initialization/testing...")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - [%(module)s.%(funcName)s] %(message)s')
    initialize_database()
    print("\nDatabase module direct execution finished.")