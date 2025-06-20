import sqlite3
import os
import logging
from typing import Optional, List, Dict, Any

# --- Database Path Logic ---
DEV_DATA_DIRECTORY = "/home/mattw/Projects/discord_ticket_manager/data/"
PROD_DATA_DIRECTORY = "/home/container/data/"

# Determine the actual base directory to use
if os.path.exists(os.path.dirname(PROD_DATA_DIRECTORY)):
    ACTUAL_DATA_DIRECTORY = PROD_DATA_DIRECTORY
else:
    ACTUAL_DATA_DIRECTORY = DEV_DATA_DIRECTORY

# Ensure the directory exists
os.makedirs(ACTUAL_DATA_DIRECTORY, exist_ok=True)

DB_PATH = os.path.join(ACTUAL_DATA_DIRECTORY, "recruitment.db")
logging.info(f"Recruitment Manager database will be at: {DB_PATH}")

def get_db_connection() -> sqlite3.Connection:
    """Establishes and returns a SQLite database connection for the recruitment manager."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def initialize_database(guild_id: int):
    """Initializes the database and creates tables if they don't exist."""
    logging.info("Attempting to initialize recruitment database...")
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            logging.info("Executing CREATE TABLE IF NOT EXISTS for cog_config.")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cog_config (
                    config_id TEXT PRIMARY KEY,
                    forum_channel_id TEXT,
                    open_tag_id TEXT,
                    closed_tag_id TEXT,
                    asset_channel_id TEXT 
                )
            """)
            logging.info("Executing CREATE TABLE IF NOT EXISTS for managed_threads.")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS managed_threads (
                    thread_id TEXT PRIMARY KEY,
                    op_id TEXT NOT NULL,
                    main_post_message_id TEXT NOT NULL,
                    manager_panel_message_id TEXT,
                    creation_timestamp INTEGER NOT NULL,
                    last_reminder_sent_timestamp INTEGER,
                    is_closed INTEGER DEFAULT 0,
                    starter_message_id TEXT
                )
            """)
            logging.info("Executing CREATE TABLE IF NOT EXISTS for scheduled_deletions.")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scheduled_deletions (
                    message_id TEXT PRIMARY KEY, channel_id TEXT NOT NULL, deletion_timestamp INTEGER NOT NULL
                )
            """)
            
            logging.info(f"Executing INSERT OR IGNORE for cog_config with guild_id: {guild_id}.")
            cursor.execute("INSERT OR IGNORE INTO cog_config (config_id) VALUES (?)", (str(guild_id),))
            conn.commit()
            logging.info("Recruitment Manager database tables created/verified successfully.")
    except Exception as e:
        logging.error(f"CRITICAL ERROR during database initialization: {e}", exc_info=True)

    migrate_add_starter_message_id()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS applicants (
            application_id INTEGER PRIMARY KEY AUTOINCREMENT,
            thread_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            UNIQUE(thread_id, user_id)
        )
    """)
    
    conn.commit()
    conn.close()

def get_config(guild_id: int) -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        row = conn.cursor().execute("SELECT * FROM cog_config WHERE config_id = ?", (str(guild_id),)).fetchone()
        return dict(row) if row else None

def update_config(guild_id: int, settings: Dict[str, Any]):
    set_clauses = [f"{key} = ?" for key in settings.keys()]
    params = list(settings.values())
    params.append(str(guild_id))
    query = f"UPDATE cog_config SET {', '.join(set_clauses)} WHERE config_id = ?"
    with get_db_connection() as conn:
        conn.cursor().execute(query, tuple(params))
        conn.commit()
# (The rest of the database functions are unchanged)
def add_managed_thread(thread_id: int, op_id: int, main_post_id: int, panel_id: int, creation_ts: int, starter_message_id: int):
    with get_db_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO managed_threads (thread_id, op_id, main_post_message_id, manager_panel_message_id, creation_timestamp, starter_message_id) VALUES (?, ?, ?, ?, ?, ?)",
            (str(thread_id), str(op_id), str(main_post_id), str(panel_id), creation_ts, str(starter_message_id))
        )
        conn.commit()

def get_managed_thread(thread_id: int) -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        row = conn.cursor().execute("SELECT * FROM managed_threads WHERE thread_id = ?", (str(thread_id),)).fetchone()
        return dict(row) if row else None

def get_user_threads(op_id: int) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        rows = conn.cursor().execute("SELECT * FROM managed_threads WHERE op_id = ?", (str(op_id),)).fetchall()
        return [dict(row) for row in rows]

def get_all_open_threads() -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        rows = conn.cursor().execute("SELECT * FROM managed_threads WHERE is_closed = 0").fetchall()
        return [dict(row) for row in rows]

def update_thread_panel_id(thread_id: int, panel_id: int):
    with get_db_connection() as conn:
        conn.execute("UPDATE managed_threads SET manager_panel_message_id = ? WHERE thread_id = ?", (str(panel_id), str(thread_id)))
        conn.commit()

def update_thread_status(thread_id: int, is_closed: bool):
    with get_db_connection() as conn:
        conn.execute("UPDATE managed_threads SET is_closed = ? WHERE thread_id = ?", (1 if is_closed else 0, str(thread_id)))
        conn.commit()

def update_reminder_timestamp(thread_id: int, timestamp: Optional[int]):
    with get_db_connection() as conn:
        conn.execute("UPDATE managed_threads SET last_reminder_sent_timestamp = ? WHERE thread_id = ?", (timestamp, str(thread_id)))
        conn.commit()

def add_scheduled_deletion(message_id: int, channel_id: int, deletion_timestamp: int):
    with get_db_connection() as conn:
        conn.execute("INSERT OR REPLACE INTO scheduled_deletions (message_id, channel_id, deletion_timestamp) VALUES (?, ?, ?)", (str(message_id), str(channel_id), deletion_timestamp))
        conn.commit()

def get_due_deletions(current_timestamp: int) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        rows = conn.cursor().execute("SELECT * FROM scheduled_deletions WHERE deletion_timestamp <= ?", (current_timestamp,)).fetchall()
        return [dict(row) for row in rows]

def remove_scheduled_deletion(message_id: int):
    with get_db_connection() as conn:
        conn.execute("DELETE FROM scheduled_deletions WHERE message_id = ?", (str(message_id),))
        conn.commit()

def update_main_post_id(thread_id: int, new_message_id: int):
    """Updates the main_post_message_id for a managed thread."""
    with get_db_connection() as conn:
        conn.execute(
            "UPDATE managed_threads SET main_post_message_id = ? WHERE thread_id = ?",
            (str(new_message_id), str(thread_id))
        )
        conn.commit()

def add_applicant(thread_id: int, user_id: int):
    """Adds a new applicant for a thread with 'pending' status."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO applicants (thread_id, user_id, status) VALUES (?, ?, ?)",
        (thread_id, user_id, 'pending')
    )
    conn.commit()
    conn.close()

def get_applicant_status(thread_id: int, user_id: int) -> Optional[str]:
    """Gets the application status for a user on a specific thread."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT status FROM applicants WHERE thread_id = ? AND user_id = ?",
        (thread_id, user_id)
    )
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None

def update_applicant_status(thread_id: int, user_id: int, status: str):
    """Updates an applicant's status to 'accepted' or 'denied'."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE applicants SET status = ? WHERE thread_id = ? AND user_id = ?",
        (status, thread_id, user_id)
    )
    conn.commit()
    conn.close()

def migrate_add_starter_message_id():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("ALTER TABLE managed_threads ADD COLUMN starter_message_id TEXT")
            conn.commit()
        except sqlite3.OperationalError:
            # Column already exists
            pass

def delete_managed_thread(thread_id: int):
    with get_db_connection() as conn:
        conn.execute("DELETE FROM managed_threads WHERE thread_id = ?", (str(thread_id),))
        conn.commit()