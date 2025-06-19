import sqlite3
import os
import logging
from typing import Optional, List, Dict, Any, Tuple

# --- Database Path Logic (adopted from existing project files) ---
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

DB_PATH = os.path.join(ACTUAL_DATA_DIRECTORY, "trades.db")
logging.info(f"Trade Manager database will be at: {DB_PATH}")

def get_db_connection() -> sqlite3.Connection:
    """Establishes and returns a SQLite database connection for the trade manager."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def initialize_database(guild_id: int):
    """Initializes the database and creates tables if they don't exist for a specific guild."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # --- `cog_config` table ---
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cog_config (
                config_id TEXT PRIMARY KEY,
                forum_channel_id TEXT,
                deletion_delay_hours INTEGER DEFAULT 24
            )
        """)

        # --- `managed_threads` table ---
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS managed_threads (
                thread_id TEXT PRIMARY KEY,
                op_id TEXT NOT NULL,
                guild_id TEXT NOT NULL,
                creation_timestamp INTEGER NOT NULL,
                last_reminder_message_id TEXT,
                last_reminder_sent_timestamp INTEGER,
                is_complete INTEGER DEFAULT 0,
                deletion_timestamp INTEGER 
            )
        """)
        
        # Ensure the server_id for this guild exists in the config table.
        cursor.execute("INSERT OR IGNORE INTO cog_config (config_id) VALUES (?)", (str(guild_id),))
        conn.commit()
        logging.info("Trade Manager database initialized/verified.")

# --- Config Functions ---

def get_config(guild_id: int) -> Optional[Dict[str, Any]]:
    """Retrieves the configuration for a specific guild."""
    with get_db_connection() as conn:
        row = conn.cursor().execute("SELECT * FROM cog_config WHERE config_id = ?", (str(guild_id),)).fetchone()
        return dict(row) if row else None

def update_config(guild_id: int, settings: Dict[str, Any]) -> bool:
    """Updates one or more configuration settings for a guild."""
    if not settings:
        return False

    set_clauses = [f"{key} = ?" for key in settings.keys()]
    params = list(settings.values())
    params.append(str(guild_id))
    query = f"UPDATE cog_config SET {', '.join(set_clauses)} WHERE config_id = ?"
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, tuple(params))
        conn.commit()
        return cursor.rowcount > 0

# --- Managed Thread Functions ---

def add_managed_thread(thread_id: int, op_id: int, guild_id: int, creation_timestamp: int):
    """Adds a new trade thread to be managed."""
    with get_db_connection() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO managed_threads 
            (thread_id, op_id, guild_id, creation_timestamp) 
            VALUES (?, ?, ?, ?)
        """, (str(thread_id), str(op_id), str(guild_id), creation_timestamp))
        conn.commit()

def get_managed_thread(thread_id: int) -> Optional[Dict[str, Any]]:
    """Gets data for a single managed thread."""
    with get_db_connection() as conn:
        row = conn.cursor().execute("SELECT * FROM managed_threads WHERE thread_id = ?", (str(thread_id),)).fetchone()
        return dict(row) if row else None

def get_all_active_threads() -> List[Dict[str, Any]]:
    """Gets all threads that are not yet marked as complete."""
    with get_db_connection() as conn:
        rows = conn.cursor().execute("SELECT * FROM managed_threads WHERE is_complete = 0").fetchall()
        return [dict(row) for row in rows]

def get_threads_for_deletion(current_timestamp: int) -> List[Dict[str, Any]]:
    """Gets all threads whose deletion_timestamp is in the past."""
    with get_db_connection() as conn:
        rows = conn.cursor().execute(
            "SELECT * FROM managed_threads WHERE deletion_timestamp IS NOT NULL AND deletion_timestamp <= ?",
            (current_timestamp,)
        ).fetchall()
        return [dict(row) for row in rows]

def get_user_active_trades(user_id: int, guild_id: int) -> List[Dict[str, Any]]:
    """Gets all active trade threads for a specific user."""
    with get_db_connection() as conn:
        rows = conn.cursor().execute(
            "SELECT * FROM managed_threads WHERE op_id = ? AND guild_id = ? AND is_complete = 0",
            (str(user_id), str(guild_id))
        ).fetchall()
        return [dict(row) for row in rows]

def update_thread_reminder_info(thread_id: int, message_id: int, timestamp: int):
    """Updates a thread's record with the latest reminder message ID and timestamp."""
    with get_db_connection() as conn:
        conn.execute("""
            UPDATE managed_threads 
            SET last_reminder_message_id = ?, last_reminder_sent_timestamp = ? 
            WHERE thread_id = ?
        """, (str(message_id), timestamp, str(thread_id)))
        conn.commit()

def clear_thread_reminder_info(thread_id: int):
    """Clears reminder info, typically when a user clicks 'Keep Open'."""
    with get_db_connection() as conn:
        conn.execute("""
            UPDATE managed_threads
            SET last_reminder_message_id = NULL, last_reminder_sent_timestamp = NULL
            WHERE thread_id = ?
        """, (str(thread_id),))
        conn.commit()
        
def mark_thread_as_complete(thread_id: int, deletion_timestamp: int):
    """Marks a thread as complete and sets its deletion timestamp."""
    with get_db_connection() as conn:
        conn.execute("""
            UPDATE managed_threads 
            SET is_complete = 1, deletion_timestamp = ? 
            WHERE thread_id = ?
        """, (deletion_timestamp, str(thread_id)))
        conn.commit()

def set_thread_deletion_time(thread_id: int, deletion_timestamp: int):
    """Sets or updates the deletion timestamp for a thread, also marks as complete."""
    with get_db_connection() as conn:
        conn.execute("""
            UPDATE managed_threads 
            SET is_complete = 1, deletion_timestamp = ? 
            WHERE thread_id = ?
        """, (deletion_timestamp, str(thread_id)))
        conn.commit()
        
def remove_thread(thread_id: int):
    """Removes a thread record from the database entirely."""
    with get_db_connection() as conn:
        conn.execute("DELETE FROM managed_threads WHERE thread_id = ?", (str(thread_id),))
        conn.commit()