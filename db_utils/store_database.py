import sqlite3
import os
import logging
import json
from typing import Optional, List, Dict, Any

# --- Logger Setup ---
# Get a specific logger for this module
logger = logging.getLogger('nextcord.store_database')
# Add a handler if one doesn't exist to ensure output
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


# --- Database Path Logic ---
DEV_DATA_DIRECTORY = "/home/mattw/Projects/discord_ticket_manager/data/"
PROD_DATA_DIRECTORY = "/home/container/data/"

if os.path.exists(os.path.dirname(PROD_DATA_DIRECTORY)):
    ACTUAL_DATA_DIRECTORY = PROD_DATA_DIRECTORY
else:
    ACTUAL_DATA_DIRECTORY = DEV_DATA_DIRECTORY

os.makedirs(ACTUAL_DATA_DIRECTORY, exist_ok=True)
DB_PATH = os.path.join(ACTUAL_DATA_DIRECTORY, "store_data.db")
logger.info(f"Database path configured to: {DB_PATH}")


def get_db_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)

def initialize_database():
    """Initializes the database and creates tables if they don't exist."""
    logger.info("initialize_database() called. Attempting to create tables...")
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
                    username_at_time TEXT NOT NULL, transaction_type TEXT NOT NULL, item_description TEXT NOT NULL,
                    quantity INTEGER, notes TEXT, timestamp INTEGER NOT NULL, admin_id INTEGER NOT NULL, ingame_name TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS store_items (
                    item_id INTEGER PRIMARY KEY AUTOINCREMENT, category TEXT NOT NULL, item_name TEXT NOT NULL UNIQUE,
                    associated_role_id INTEGER, is_subscription INTEGER DEFAULT 0
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scheduled_role_removals (
                    schedule_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, role_id INTEGER NOT NULL,
                    removal_timestamp INTEGER NOT NULL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cog_config (
                    config_id INTEGER PRIMARY KEY, subscriber_list_channel_id INTEGER, subscriber_list_webhook_url TEXT,
                    dm_receipts_enabled INTEGER DEFAULT 1, embed_configs_json TEXT, webhook_message_ids_json TEXT
                )
            """)
            cursor.execute("INSERT OR IGNORE INTO cog_config (config_id) VALUES (1)")
        try:
            cursor.execute("ALTER TABLE cog_config ADD COLUMN subscriber_list_footer_text TEXT")
            logger.info("Added 'subscriber_list_footer_text' column to cog_config table.")
        except sqlite3.OperationalError as e:
            # This is expected to fail if the column already exists. We can ignore that specific error.
            if "duplicate column name" not in str(e):
                logger.error("An unexpected DB error occurred when adding new column.", exc_info=True)
                raise e
            conn.commit()
            logger.info("Database tables for Store Manager created/verified successfully.")
    except Exception as e:
        logger.error(f"CRITICAL: An exception occurred during database initialization.", exc_info=True)

# ... (The rest of the functions in this file remain the same) ...
# --- Transaction Functions ---

def add_transaction(
    guild_id: int, user_id: int, username_at_time: str, trans_type: str, item: str,
    admin_id: int, quantity: Optional[int], notes: Optional[str], ign: Optional[str],
    timestamp: int, duration_months: Optional[int] = None, duration_days: Optional[int] = None,
    expires_at: Optional[int] = None, is_permanent: int = 0, expired: int = 0
) -> int:
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO transactions (
                guild_id, user_id, username_at_time, transaction_type, item_description,
                quantity, notes, timestamp, admin_id, ingame_name,
                duration_months, duration_days, expires_at, is_permanent, expired
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            guild_id, user_id, username_at_time, trans_type, item, quantity, notes, timestamp, admin_id, ign,
            duration_months, duration_days, expires_at, is_permanent, expired
        ))
        conn.commit()
        return cursor.lastrowid

def get_user_transactions(user_id: int) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM transactions WHERE user_id = ? ORDER BY timestamp DESC", (user_id,))
        return [dict(row) for row in cursor.fetchall()]

def get_transaction(transaction_id: int) -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM transactions WHERE transaction_id = ?", (transaction_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

def update_transaction(transaction_id: int, updates: Dict[str, Any]) -> bool:
    with get_db_connection() as conn:
        set_clauses = [f"{key} = ?" for key in updates.keys()]
        query = f"UPDATE transactions SET {', '.join(set_clauses)} WHERE transaction_id = ?"
        params = list(updates.values()) + [transaction_id]
        cursor = conn.execute(query, tuple(params))
        conn.commit()
        return cursor.rowcount > 0

def remove_transaction(transaction_id: int) -> bool:
    with get_db_connection() as conn:
        cursor = conn.execute("DELETE FROM transactions WHERE transaction_id = ?", (transaction_id,))
        conn.commit()
        return cursor.rowcount > 0

def get_transaction_by_user_and_item(user_id: int, item_name: str) -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT * FROM transactions WHERE user_id = ? AND item_description = ? ORDER BY timestamp DESC LIMIT 1",
            (user_id, item_name)
        )
        row = cursor.fetchone()
        return dict(row) if row else None
    
def user_has_purchase_record(user_id: int, item_name: str) -> bool:
    """Checks if a user has at least one 'Purchase' transaction for a specific item."""
    with get_db_connection() as conn:
        cursor = conn.execute(
            "SELECT 1 FROM transactions WHERE user_id = ? AND item_description = ? AND transaction_type = 'Purchase' LIMIT 1",
            (user_id, item_name)
        )
        return cursor.fetchone() is not None

# --- Store Item Functions ---

def add_store_item(category: str, item_name: str) -> bool:
    with get_db_connection() as conn:
        try:
            conn.execute("INSERT INTO store_items (category, item_name) VALUES (?, ?)", (category, item_name))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

def remove_store_item(item_name: str) -> bool:
    with get_db_connection() as conn:
        cursor = conn.execute("DELETE FROM store_items WHERE item_name = ?", (item_name,))
        conn.commit()
        return cursor.rowcount > 0

def get_all_store_items() -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM store_items ORDER BY category, item_name")
        return [dict(row) for row in cursor.fetchall()]

def get_item_by_name(item_name: str) -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM store_items WHERE item_name = ?", (item_name,))
        row = cursor.fetchone()
        return dict(row) if row else None

def update_store_item(item_name: str, updates: Dict[str, Any]) -> bool:
    with get_db_connection() as conn:
        set_clauses = [f"{key} = ?" for key in updates.keys()]
        query = f"UPDATE store_items SET {', '.join(set_clauses)} WHERE item_name = ?"
        params = list(updates.values()) + [item_name]
        cursor = conn.execute(query, tuple(params))
        conn.commit()
        return cursor.rowcount > 0

# --- Subscription Role Removal Functions ---

def schedule_role_removal(user_id: int, role_id: int, removal_timestamp: int):
    with get_db_connection() as conn:
        conn.execute("INSERT INTO scheduled_role_removals (user_id, role_id, removal_timestamp) VALUES (?, ?, ?)", (user_id, role_id, removal_timestamp))
        conn.commit()

def get_due_role_removals(current_timestamp: int) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM scheduled_role_removals WHERE removal_timestamp <= ?", (current_timestamp,))
        return [dict(row) for row in cursor.fetchall()]
        
def get_all_scheduled_removals() -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM scheduled_role_removals")
        return [dict(row) for row in cursor.fetchall()]

def delete_scheduled_removal(schedule_id: int):
    with get_db_connection() as conn:
        conn.execute("DELETE FROM scheduled_role_removals WHERE schedule_id = ?", (schedule_id,))
        conn.commit()
        
def get_user_subscription(user_id: int, role_id: int) -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM scheduled_role_removals WHERE user_id = ? AND role_id = ?", (user_id, role_id))
        row = cursor.fetchone()
        return dict(row) if row else None

def update_user_subscription(schedule_id: int, new_timestamp: int):
    with get_db_connection() as conn:
        conn.execute("UPDATE scheduled_role_removals SET removal_timestamp = ? WHERE schedule_id = ?", (new_timestamp, schedule_id))
        conn.commit()

# --- Cog Config Functions ---

def get_config() -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM cog_config WHERE config_id = 1")
        row = cursor.fetchone()
        if row:
            config = dict(row)
            config['embed_configs_json'] = json.loads(config['embed_configs_json']) if config.get('embed_configs_json') else {}
            config['webhook_message_ids_json'] = json.loads(config['webhook_message_ids_json']) if config.get('webhook_message_ids_json') else {}
            return config
        return None

def update_config(updates: Dict[str, Any]) -> bool:
    with get_db_connection() as conn:
        if 'embed_configs_json' in updates: updates['embed_configs_json'] = json.dumps(updates['embed_configs_json'])
        if 'webhook_message_ids_json' in updates: updates['webhook_message_ids_json'] = json.dumps(updates['webhook_message_ids_json'])
        set_clauses = [f"{key} = ?" for key in updates.keys()]
        query = f"UPDATE cog_config SET {', '.join(set_clauses)} WHERE config_id = 1"
        params = list(updates.values())
        cursor = conn.execute(query, tuple(params))
        conn.commit()
        return cursor.rowcount > 0

def update_transaction_for_expiry(user_id: int, item_name: str):
    with get_db_connection() as conn:
        conn.execute(
            "UPDATE transactions SET expired = 1 WHERE user_id = ? AND item_description = ? AND expired = 0",
            (user_id, item_name)
        )
        conn.commit()