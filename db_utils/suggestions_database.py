import sqlite3
import os
import logging
from typing import Optional, Dict, Any, Tuple

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

DB_PATH = os.path.join(ACTUAL_DATA_DIRECTORY, "suggestions_cog.db")
logging.info(f"Suggestions cog database will be at: {DB_PATH}")

def get_db_connection():
    """Establishes and returns a SQLite database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def initialize_database(guild_id: int):
    """Initializes the database and creates/alters tables. Safe to call multiple times."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Main config table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS config (
                server_id TEXT PRIMARY KEY,
                forum_channel_id TEXT,
                title_min_length INTEGER DEFAULT 10,
                title_max_length INTEGER DEFAULT 45,
                description_min_length INTEGER DEFAULT 50,
                description_max_length INTEGER DEFAULT 4000,
                pre_modal_message TEXT
            )
        """)
        
        # New table to track individual suggestions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS suggestions (
                thread_id TEXT PRIMARY KEY,
                suggester_id TEXT NOT NULL,
                guild_id TEXT NOT NULL
            )
        """)

        # Add new columns for status management to the config table
        new_columns = {
            "planned_tag_id": "TEXT",
            "implemented_tag_id": "TEXT",
            "denied_tag_id": "TEXT",
            "planned_message": "TEXT DEFAULT 'This suggestion has been marked as **Planned**.'",
            "implemented_message": "TEXT DEFAULT 'This suggestion has been **Implemented**.'",
            "denied_message": "TEXT DEFAULT 'This suggestion has been **Denied**.'",
        }
        
        for col_name, col_type in new_columns.items():
            try:
                cursor.execute(f"ALTER TABLE config ADD COLUMN {col_name} {col_type}")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e):
                    raise

        # Ensure the server_id for this guild exists in the config table.
        cursor.execute("INSERT OR IGNORE INTO config (server_id) VALUES (?)", (str(guild_id),))
        conn.commit()

def get_config(guild_id: int) -> Optional[Dict[str, Any]]:
    """Retrieves the configuration for a specific guild."""
    initialize_database(guild_id)
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM config WHERE server_id = ?", (str(guild_id),))
        row = cursor.fetchone()
        return dict(row) if row else None

def update_config(guild_id: int, settings: Dict[str, Any]) -> bool:
    """Updates one or more configuration settings for a guild."""
    initialize_database(guild_id)
    if not settings:
        return False

    set_clauses = [f"{key} = ?" for key in settings.keys()]
    params = list(settings.values())
    params.append(str(guild_id))

    query = f"UPDATE config SET {', '.join(set_clauses)} WHERE server_id = ?"

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, tuple(params))
        conn.commit()
        return cursor.rowcount > 0

# --- New functions for suggestion tracking ---

def add_suggestion(thread_id: int, suggester_id: int, guild_id: int):
    """Adds a new suggestion to the tracking database."""
    with get_db_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO suggestions (thread_id, suggester_id, guild_id) VALUES (?, ?, ?)",
            (str(thread_id), str(suggester_id), str(guild_id))
        )
        conn.commit()

def get_suggestion_suggester(thread_id: int) -> Optional[int]:
    """Gets the original suggester's ID for a given suggestion thread."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT suggester_id FROM suggestions WHERE thread_id = ?", (str(thread_id),))
        row = cursor.fetchone()
        return int(row['suggester_id']) if row else None