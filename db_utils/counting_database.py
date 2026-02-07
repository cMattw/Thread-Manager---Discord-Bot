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
