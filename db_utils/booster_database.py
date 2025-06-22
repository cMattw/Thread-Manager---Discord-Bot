import sqlite3
import os
import logging
from typing import Optional, List, Dict, Any

# --- Database Path Logic ---
DEV_DATA_DIRECTORY = "/home/mattw/Projects/discord_ticket_manager/data/"
PROD_DATA_DIRECTORY = "/home/container/data/"

if os.path.exists(os.path.dirname(PROD_DATA_DIRECTORY)):
    ACTUAL_DATA_DIRECTORY = PROD_DATA_DIRECTORY
else:
    ACTUAL_DATA_DIRECTORY = DEV_DATA_DIRECTORY

if not os.path.exists(ACTUAL_DATA_DIRECTORY):
    os.makedirs(ACTUAL_DATA_DIRECTORY, exist_ok=True)

DB_PATH = os.path.join(ACTUAL_DATA_DIRECTORY, "booster_tracker.db")
logging.info(f"Booster Tracker database will be at: {DB_PATH}")

def get_db_connection() -> sqlite3.Connection:
    """Establishes and returns a SQLite database connection for the booster tracker."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def initialize_database():
    """Initializes the database and creates/upgrades tables."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS boosters (
                user_id TEXT PRIMARY KEY,
                guild_id TEXT NOT NULL,
                is_currently_boosting INTEGER DEFAULT 0,
                current_boost_start_timestamp INTEGER,
                last_anniversary_notified INTEGER DEFAULT 0,
                total_boost_count INTEGER DEFAULT 0,
                total_duration_days INTEGER DEFAULT 0,
                first_boost_timestamp INTEGER
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS boost_history (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                guild_id TEXT NOT NULL,
                boost_start_timestamp INTEGER NOT NULL,
                boost_end_timestamp INTEGER
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cog_config (
                config_id TEXT PRIMARY KEY, 
                announcement_channel_id TEXT, 
                welcome_message_template TEXT, 
                anniversary_message_template TEXT,
                announcement_webhook_url TEXT
            )
        """)
        # Add booster_announcement_webhook_url if missing
        try:
            cursor.execute("ALTER TABLE cog_config ADD COLUMN booster_announcement_webhook_url TEXT")
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e):
                raise
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reward_roles (
                duration_months INTEGER PRIMARY KEY, 
                role_id TEXT NOT NULL
            )
        """)
        try:
            cursor.execute("ALTER TABLE boosters ADD COLUMN claimed_keys INTEGER DEFAULT 0")
        except Exception as e:
            if "duplicate column name" not in str(e):
                raise
        conn.commit()
        logging.info("Booster tracker database initialized/verified.")

def get_booster(user_id: str) -> Optional[Dict[str, Any]]:
    """Retrieves a specific booster's data."""
    with get_db_connection() as conn:
        row = conn.cursor().execute("SELECT * FROM boosters WHERE user_id = ?", (user_id,)).fetchone()
        return dict(row) if row else None

def start_new_boost(user_id: str, guild_id: str, start_timestamp: int):
    """Logs the start of a new boost streak without incrementing the count."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO boosters (user_id, guild_id) VALUES (?, ?)", (user_id, guild_id))

        # --- MODIFIED: Removed 'total_boost_count' increment to prevent double-counting ---
        # The on_message listener in the cog now handles the increment.
        cursor.execute("""
            UPDATE boosters
            SET is_currently_boosting = 1,
                current_boost_start_timestamp = ?,
                last_anniversary_notified = 0
            WHERE user_id = ?
        """, (start_timestamp, user_id))

        # Set the first_boost_timestamp only if it's not already set
        cursor.execute("""
            UPDATE boosters
            SET first_boost_timestamp = ?
            WHERE user_id = ? AND first_boost_timestamp IS NULL
        """, (start_timestamp, user_id))

        cursor.execute("INSERT INTO boost_history (user_id, guild_id, boost_start_timestamp) VALUES (?, ?, ?)", (user_id, guild_id, start_timestamp))
        conn.commit()

def end_boost(user_id: str, end_timestamp: int):
    """Logs the end of a boost and calculates cumulative duration."""
    with get_db_connection() as conn:
        history_cursor = conn.cursor()
        history_cursor.execute("SELECT event_id, boost_start_timestamp FROM boost_history WHERE user_id = ? AND boost_end_timestamp IS NULL ORDER BY boost_start_timestamp DESC LIMIT 1", (user_id,))
        active_boost = history_cursor.fetchone()
        duration_days = 0
        if active_boost:
            duration_seconds = end_timestamp - active_boost['boost_start_timestamp']
            duration_days = duration_seconds // (24 * 3600)
            conn.cursor().execute("UPDATE boost_history SET boost_end_timestamp = ? WHERE event_id = ?", (end_timestamp, active_boost['event_id']))
        
        conn.cursor().execute("""
            UPDATE boosters
            SET is_currently_boosting = 0,
                current_boost_start_timestamp = NULL,
                total_duration_days = total_duration_days + ?
            WHERE user_id = ?
        """, (duration_days, user_id))
        conn.commit()

def increment_boost_count(user_id: str, amount: int = 1):
    """Increments the total boost count for a user."""
    with get_db_connection() as conn:
        # --- ADDED: Ensure user exists before trying to increment ---
        conn.cursor().execute("INSERT OR IGNORE INTO boosters (user_id, guild_id) VALUES (?, 'default')", (user_id,))
        conn.cursor().execute("UPDATE boosters SET total_boost_count = total_boost_count + ? WHERE user_id = ?", (amount, user_id))
        conn.commit()
        
def get_booster_history(user_id: str) -> List[Dict[str, Any]]:
    """Retrieves the full boost history for a user."""
    with get_db_connection() as conn:
        rows = conn.cursor().execute("SELECT * FROM boost_history WHERE user_id = ? ORDER BY boost_start_timestamp DESC", (user_id,)).fetchall()
        return [dict(row) for row in rows]

def get_all_boosters_for_leaderboard() -> List[Dict[str, Any]]:
    """Retrieves all booster data for leaderboard generation."""
    with get_db_connection() as conn:
        rows = conn.cursor().execute("SELECT * FROM boosters").fetchall()
        return [dict(row) for row in rows]

def get_config(guild_id: str) -> Optional[Dict[str, Any]]:
    """Retrieves the configuration for a specific guild."""
    with get_db_connection() as conn:
        row = conn.cursor().execute("SELECT * FROM cog_config WHERE config_id = ?", (guild_id,)).fetchone()
        return dict(row) if row else {} # Return empty dict if no config

def update_config(guild_id: str, settings: Dict[str, Any]):
    """Updates one or more configuration settings for a guild."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO cog_config (config_id) VALUES (?)", (guild_id,))
        for key, value in settings.items():
            # This dynamic update remains the same and will handle the new webhook key perfectly.
            cursor.execute(f"UPDATE cog_config SET {key} = ? WHERE config_id = ?", (value, guild_id))
        conn.commit()

def add_reward_role(duration_months: int, role_id: str):
    """Adds a new role reward."""
    with get_db_connection() as conn:
        conn.cursor().execute("INSERT OR REPLACE INTO reward_roles (duration_months, role_id) VALUES (?, ?)", (duration_months, role_id))
        conn.commit()

def remove_reward_role(role_id: str):
    """Removes a role reward."""
    with get_db_connection() as conn:
        conn.cursor().execute("DELETE FROM reward_roles WHERE role_id = ?", (role_id,))
        conn.commit()

def get_all_reward_roles() -> List[Dict[str, Any]]:
    """Retrieves all configured role rewards."""
    with get_db_connection() as conn:
        rows = conn.cursor().execute("SELECT * FROM reward_roles ORDER BY duration_months ASC").fetchall()
        return [dict(row) for row in rows]

def update_anniversary_notified(user_id: str, month_milestone: int):
    """Updates the last notified anniversary for a booster."""
    with get_db_connection() as conn:
        conn.cursor().execute("UPDATE boosters SET last_anniversary_notified = ? WHERE user_id = ?", (month_milestone, user_id))
        conn.commit()

def add_claimed_keys(user_id: str, amount: int):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE boosters SET claimed_keys = COALESCE(claimed_keys, 0) + ? WHERE user_id = ?",
            (amount, user_id)
        )
        conn.commit()

def get_claimed_keys(user_id: str) -> int:
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT claimed_keys FROM boosters WHERE user_id = ?", (user_id,)
        )
        row = cursor.fetchone()
        return row[0] if row and row[0] is not None else 0