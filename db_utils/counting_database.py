import sqlite3
import logging
from typing import Optional
import os

DEV_DATA_DIRECTORY = "/home/mattw/Projects/discord_ticket_manager/data/"
PROD_DATA_DIRECTORY = "/home/container/data/"

if os.path.exists("/home/container/"):
    ACTUAL_DATA_DIRECTORY = PROD_DATA_DIRECTORY
    logging.info(f"Production environment detected. Using data directory: {ACTUAL_DATA_DIRECTORY}")
else:
    ACTUAL_DATA_DIRECTORY = DEV_DATA_DIRECTORY
    logging.info(f"Development environment detected. Using data directory: {ACTUAL_DATA_DIRECTORY}")

if ACTUAL_DATA_DIRECTORY and not os.path.exists(ACTUAL_DATA_DIRECTORY):
    os.makedirs(ACTUAL_DATA_DIRECTORY, exist_ok=True)
    logging.info(f"Successfully created data directory: {ACTUAL_DATA_DIRECTORY}")

DATABASE_NAME = os.path.join(ACTUAL_DATA_DIRECTORY, "ticket_bot_settings.db")
logging.info(f"Counting database file will be at: {DATABASE_NAME}")

def get_db_connection():
    """Get database connection."""
    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def get_counting_channel(guild_id: int) -> Optional[int]:
    """Get the counting channel ID for a guild."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT counting_channel_id FROM settings WHERE guild_id = ?", (guild_id,))
        row = cursor.fetchone()
        return row['counting_channel_id'] if row else None
    except sqlite3.Error as e:
        logging.error(f"DB Error getting counting_channel for {guild_id}: {e}")
        return None
    finally:
        conn.close()

def set_counting_channel(guild_id: int, channel_id: int) -> bool:
    """Set the counting channel ID for a guild."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO settings (guild_id) VALUES (?)", (guild_id,))
        cursor.execute("UPDATE settings SET counting_channel_id = ? WHERE guild_id = ?", (channel_id, guild_id))
        conn.commit()
        logging.info(f"Set counting channel to {channel_id} for guild {guild_id}")
        return True
    except sqlite3.Error as e:
        logging.error(f"DB Error setting counting_channel for {guild_id}: {e}")
        return False
    finally:
        conn.close()

def add_exempted_role(guild_id: int, role_id: int) -> bool:
    """Add an exempted role for the counting channel."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO settings (guild_id) VALUES (?)", (guild_id,))
        cursor.execute("INSERT INTO counting_exempted_roles (guild_id, role_id) VALUES (?, ?)", (guild_id, role_id))
        conn.commit()
        logging.info(f"Added exempted role {role_id} for guild {guild_id}")
        return True
    except sqlite3.IntegrityError:
        logging.warning(f"Role {role_id} already exempted for guild {guild_id}")
        return False
    except sqlite3.Error as e:
        logging.error(f"DB Error adding exempted_role for {guild_id}: {e}")
        return False
    finally:
        conn.close()

def remove_exempted_role(guild_id: int, role_id: int) -> bool:
    """Remove an exempted role for the counting channel."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM counting_exempted_roles WHERE guild_id = ? AND role_id = ?", (guild_id, role_id))
        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"DB Error removing exempted_role for {guild_id}: {e}")
        return False
    finally:
        conn.close()

def get_exempted_roles(guild_id: int) -> list:
    """Get all exempted roles for a guild."""
    conn = get_db_connection()
    cursor = conn.cursor()
    roles = []
    try:
        cursor.execute("SELECT role_id FROM counting_exempted_roles WHERE guild_id = ?", (guild_id,))
        roles = [row['role_id'] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"DB Error getting exempted_roles for {guild_id}: {e}")
    finally:
        conn.close()
    return roles
