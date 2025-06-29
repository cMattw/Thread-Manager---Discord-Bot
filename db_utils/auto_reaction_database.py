import sqlite3
import os
import logging
import json
from typing import Optional, Dict, Any, List, Tuple

# --- Database Path Logic (following the pattern from suggestions_database.py) ---
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

DB_PATH = os.path.join(ACTUAL_DATA_DIRECTORY, "auto_reaction_cog.db")
logging.info(f"Auto-reaction cog database will be at: {DB_PATH}")

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
                enabled BOOLEAN DEFAULT FALSE,
                reaction_mode TEXT DEFAULT 'all'
            )
        """)
        
        # Table for reaction sets
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reaction_sets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                server_id TEXT NOT NULL,
                name TEXT NOT NULL,
                reactions TEXT NOT NULL,
                UNIQUE(server_id, name)
            )
        """)
        
        # Table for target channels (specific channels to react in)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS target_channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                server_id TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                channel_type TEXT NOT NULL DEFAULT 'channel',
                UNIQUE(server_id, channel_id)
            )
        """)
        
        # Table for channel exceptions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS channel_exceptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                server_id TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                UNIQUE(server_id, channel_id)
            )
        """)

        # Ensure the server_id for this guild exists in the config table
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

# --- Reaction Set Management ---

def add_reaction_set(guild_id: int, name: str, reactions: List[str]) -> bool:
    """Adds a new reaction set for a guild."""
    reactions_json = json.dumps(reactions)
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO reaction_sets (server_id, name, reactions) VALUES (?, ?, ?)",
                (str(guild_id), name, reactions_json)
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Reaction set with this name already exists
            return False

def remove_reaction_set(guild_id: int, name: str) -> bool:
    """Removes a reaction set for a guild."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM reaction_sets WHERE server_id = ? AND name = ?",
            (str(guild_id), name)
        )
        conn.commit()
        return cursor.rowcount > 0

def get_reaction_sets(guild_id: int) -> Dict[str, List[str]]:
    """Gets all reaction sets for a guild."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name, reactions FROM reaction_sets WHERE server_id = ?",
            (str(guild_id),)
        )
        rows = cursor.fetchall()
        
        result = {}
        for row in rows:
            name = row['name']
            reactions = json.loads(row['reactions'])
            result[name] = reactions
        
        return result

def update_reaction_set(guild_id: int, name: str, reactions: List[str]) -> bool:
    """Updates an existing reaction set."""
    reactions_json = json.dumps(reactions)
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE reaction_sets SET reactions = ? WHERE server_id = ? AND name = ?",
            (reactions_json, str(guild_id), name)
        )
        conn.commit()
        return cursor.rowcount > 0

# --- Target Channel Management ---

def add_target_channel(guild_id: int, channel_id: int, channel_type: str = "channel") -> bool:
    """Adds a channel to the targets list."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO target_channels (server_id, channel_id, channel_type) VALUES (?, ?, ?)",
                (str(guild_id), str(channel_id), channel_type)
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Channel already in targets
            return False

def remove_target_channel(guild_id: int, channel_id: int) -> bool:
    """Removes a channel from the targets list."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM target_channels WHERE server_id = ? AND channel_id = ?",
            (str(guild_id), str(channel_id))
        )
        conn.commit()
        return cursor.rowcount > 0

def get_target_channels(guild_id: int) -> List[Tuple[int, str]]:
    """Gets all target channels for a guild, returning (channel_id, channel_type) tuples."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT channel_id, channel_type FROM target_channels WHERE server_id = ?",
            (str(guild_id),)
        )
        rows = cursor.fetchall()
        return [(int(row['channel_id']), row['channel_type']) for row in rows]

def is_target_channel(guild_id: int, channel_id: int) -> bool:
    """Checks if a channel is in the targets list."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM target_channels WHERE server_id = ? AND channel_id = ?",
            (str(guild_id), str(channel_id))
        )
        return cursor.fetchone() is not None

# --- Channel Exception Management ---

def add_channel_exception(guild_id: int, channel_id: int) -> bool:
    """Adds a channel to the exceptions list."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO channel_exceptions (server_id, channel_id) VALUES (?, ?)",
                (str(guild_id), str(channel_id))
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Channel already in exceptions
            return False

def remove_channel_exception(guild_id: int, channel_id: int) -> bool:
    """Removes a channel from the exceptions list."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM channel_exceptions WHERE server_id = ? AND channel_id = ?",
            (str(guild_id), str(channel_id))
        )
        conn.commit()
        return cursor.rowcount > 0

def get_channel_exceptions(guild_id: int) -> List[int]:
    """Gets all channel exceptions for a guild."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT channel_id FROM channel_exceptions WHERE server_id = ?",
            (str(guild_id),)
        )
        rows = cursor.fetchall()
        return [int(row['channel_id']) for row in rows]

def is_channel_exception(guild_id: int, channel_id: int) -> bool:
    """Checks if a channel is in the exceptions list."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM channel_exceptions WHERE server_id = ? AND channel_id = ?",
            (str(guild_id), str(channel_id))
        )
        return cursor.fetchone() is not None