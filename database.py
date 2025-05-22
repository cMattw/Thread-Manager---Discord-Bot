import sqlite3
import logging
from typing import Optional, List, Tuple, Dict, Any
from datetime import datetime, timezone

DATABASE_NAME = 'ticket_bot_settings.db'

def get_db_connection():
    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()

    # General settings table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            guild_id INTEGER PRIMARY KEY,
            scan_interval_minutes INTEGER DEFAULT 60,
            delete_delay_days INTEGER DEFAULT 7,
            log_channel_id INTEGER
        )
    ''')

    # Monitored channels table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS monitored_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            UNIQUE (guild_id, channel_id),
            FOREIGN KEY (guild_id) REFERENCES settings(guild_id)
                ON DELETE CASCADE
                ON UPDATE CASCADE
        )
    ''')

    # *** NEW TABLE for exempted threads ***
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS exempted_threads (
            thread_id INTEGER PRIMARY KEY,
            guild_id INTEGER NOT NULL,
            exempted_by_user_id INTEGER,
            exemption_timestamp TEXT  -- Store as ISO8601 string
        )
    ''')

    conn.commit()
    conn.close()
    logging.info("Database initialized successfully (schema includes 'exempted_threads').")

# --- Settings Functions (remain the same) ---
def get_guild_settings(guild_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM settings WHERE guild_id = ?", (guild_id,))
    settings_row = cursor.fetchone()
    conn.close()
    return dict(settings_row) if settings_row else None

def update_setting(guild_id: int, key: str, value: Any):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO settings (guild_id) VALUES (?)", (guild_id,))
        cursor.execute(f"UPDATE settings SET {key} = ? WHERE guild_id = ?", (value, guild_id))
        conn.commit()
        logging.info(f"Updated setting '{key}' to '{value}' for guild_id {guild_id}")
    except sqlite3.Error as e:
        logging.error(f"SQLite error in update_setting for guild {guild_id}, key {key}: {e}")
    finally:
        conn.close()

def get_all_guild_configs() -> List[Dict[str, Any]]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM settings")
    configs = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return configs

# --- Monitored Channels Functions (remain the same) ---
def add_monitored_channel(guild_id: int, channel_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO settings (guild_id) VALUES (?)", (guild_id,))
        cursor.execute("INSERT INTO monitored_channels (guild_id, channel_id) VALUES (?, ?)",
                       (guild_id, channel_id))
        conn.commit()
        logging.info(f"Added monitored channel {channel_id} for guild {guild_id}.")
        return True
    except sqlite3.IntegrityError:
        logging.warning(f"Channel {channel_id} already monitored for guild {guild_id}.")
        return False
    except sqlite3.Error as e:
        logging.error(f"SQLite error in add_monitored_channel for guild {guild_id}, channel {channel_id}: {e}")
        return False
    finally:
        conn.close()

def remove_monitored_channel(guild_id: int, channel_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM monitored_channels WHERE guild_id = ? AND channel_id = ?",
                       (guild_id, channel_id))
        conn.commit()
        affected_rows = cursor.rowcount
        if affected_rows > 0:
            logging.info(f"Removed monitored channel {channel_id} for guild {guild_id}.")
            return True
        else:
            logging.info(f"No monitored channel {channel_id} found for guild {guild_id} to remove.")
            return False
    except sqlite3.Error as e:
        logging.error(f"SQLite error in remove_monitored_channel for guild {guild_id}, channel {channel_id}: {e}")
        return False
    finally:
        conn.close()

def get_monitored_channels(guild_id: int) -> List[int]:
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT channel_id FROM monitored_channels WHERE guild_id = ?", (guild_id,))
        channels = [row['channel_id'] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"SQLite error in get_monitored_channels for guild {guild_id}: {e}")
        channels = []
    finally:
        conn.close()
    return channels

# --- *** NEW Exempted Threads Functions *** ---
def add_exempted_thread(guild_id: int, thread_id: int, user_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    timestamp = datetime.now(timezone.utc).isoformat()
    try:
        cursor.execute(
            "INSERT OR REPLACE INTO exempted_threads (thread_id, guild_id, exempted_by_user_id, exemption_timestamp) VALUES (?, ?, ?, ?)",
            (thread_id, guild_id, user_id, timestamp)
        )
        conn.commit()
        logging.info(f"Thread {thread_id} in guild {guild_id} exempted by user {user_id}.")
        return True
    except sqlite3.Error as e:
        logging.error(f"SQLite error exempting thread {thread_id} in guild {guild_id}: {e}")
        return False
    finally:
        conn.close()

def remove_exempted_thread(guild_id: int, thread_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM exempted_threads WHERE thread_id = ? AND guild_id = ?", (thread_id, guild_id))
        conn.commit()
        affected_rows = cursor.rowcount
        if affected_rows > 0:
            logging.info(f"Thread {thread_id} in guild {guild_id} un-exempted.")
            return True
        else:
            logging.info(f"No exemption found for thread {thread_id} in guild {guild_id} to remove.")
            return False
    except sqlite3.Error as e:
        logging.error(f"SQLite error un-exempting thread {thread_id} in guild {guild_id}: {e}")
        return False
    finally:
        conn.close()

def is_thread_exempted(thread_id: int) -> bool: # Simplified to just check thread_id for now
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT 1 FROM exempted_threads WHERE thread_id = ?", (thread_id,))
        return cursor.fetchone() is not None
    except sqlite3.Error as e:
        logging.error(f"SQLite error checking exemption for thread {thread_id}: {e}")
        return False # Fail safe: assume not exempted on error
    finally:
        conn.close()

if __name__ == '__main__':
    print("Running database module directly for initialization/testing...")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
    initialize_database()
    print("\nDatabase module direct execution finished.")