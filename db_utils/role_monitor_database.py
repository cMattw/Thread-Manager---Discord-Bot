import sqlite3
import os
from typing import Optional, Dict, Any, List, Tuple, Union

DB_PATH = "/home/mattw/Projects/discord_ticket_manager/data/role_monitor.db"
PROD_DB_PATH = "/home/container/data/role_monitor.db"

# Determine which DB path to use
DB_PATH = PROD_DB_PATH if os.path.exists("/home/container/") else DB_PATH
# Ensure the directory exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def get_connection() -> sqlite3.Connection:
    """Establishes and returns a SQLite database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Access columns by name
    return conn

def init_db():
    """Initializes the database and creates tables if they don't exist."""
    with get_connection() as conn:
        cursor = conn.cursor()
        # Cog Config Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cog_config (
                server_id TEXT PRIMARY KEY,
                webhook_url TEXT
            )
        """)
        # Watched Roles Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS watched_roles (
                server_id TEXT NOT NULL,
                role_id TEXT NOT NULL,
                is_enabled BOOLEAN DEFAULT TRUE,
                gain_custom_title TEXT,
                gain_custom_description TEXT,
                gain_custom_content TEXT,
                loss_custom_title TEXT,
                loss_custom_description TEXT,
                loss_custom_content TEXT,
                PRIMARY KEY (server_id, role_id)
            )
        """)
        # Active Role Messages Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS active_role_messages (
                server_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                role_id TEXT NOT NULL,
                webhook_message_id TEXT NOT NULL,
                message_state TEXT NOT NULL, -- "gained" or "lost"
                PRIMARY KEY (server_id, user_id, role_id)
            )
        """)
        conn.commit()

# --- Cog Config Functions ---
def set_webhook_url(server_id: str, url: Optional[str]):
    with get_connection() as conn:
        conn.execute(
            "INSERT INTO cog_config (server_id, webhook_url) VALUES (?, ?) "
            "ON CONFLICT(server_id) DO UPDATE SET webhook_url = excluded.webhook_url",
            (server_id, url)
        )
        conn.commit()

def get_webhook_url(server_id: str) -> Optional[str]:
    with get_connection() as conn:
        cursor = conn.execute("SELECT webhook_url FROM cog_config WHERE server_id = ?", (server_id,))
        row = cursor.fetchone()
        return row['webhook_url'] if row else None

# --- Watched Roles Functions ---
def add_watched_role(server_id: str, role_id: str):
    with get_connection() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO watched_roles (server_id, role_id, is_enabled) VALUES (?, ?, TRUE)",
            (server_id, role_id)
        )
        conn.commit()

def remove_watched_role(server_id: str, role_id: str):
    with get_connection() as conn:
        conn.execute("DELETE FROM watched_roles WHERE server_id = ? AND role_id = ?", (server_id, role_id))
        conn.commit()

def toggle_watched_role_enabled(server_id: str, role_id: str) -> Optional[bool]:
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT is_enabled FROM watched_roles WHERE server_id = ? AND role_id = ?", (server_id, role_id))
        row = cursor.fetchone()
        if row:
            new_status = not row['is_enabled']
            cursor.execute(
                "UPDATE watched_roles SET is_enabled = ? WHERE server_id = ? AND role_id = ?",
                (new_status, server_id, role_id)
            )
            conn.commit()
            return new_status
        return None

def get_watched_role(server_id: str, role_id: str) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        cursor = conn.execute("SELECT * FROM watched_roles WHERE server_id = ? AND role_id = ?", (server_id, role_id))
        row = cursor.fetchone()
        return dict(row) if row else None

def get_all_watched_roles(server_id: str) -> List[Dict[str, Any]]:
    with get_connection() as conn:
        cursor = conn.execute("SELECT * FROM watched_roles WHERE server_id = ?", (server_id,))
        return [dict(row) for row in cursor.fetchall()]

def update_role_template(server_id: str, role_id: str, event_type: str,
                         title: Optional[str] = None,
                         description: Optional[str] = None,
                         content: Optional[str] = None):
    with get_connection() as conn:
        updates: Dict[str, Optional[str]] = {}
        if title is not None:
            updates[f"{event_type}_custom_title"] = title
        if description is not None:
            updates[f"{event_type}_custom_description"] = description
        if content is not None:
            updates[f"{event_type}_custom_content"] = content

        if not updates:
            return # No changes to apply

        set_clauses = [f"{key} = ?" for key in updates.keys()]
        params = list(updates.values())
        params.extend([server_id, role_id])

        query = f"UPDATE watched_roles SET {', '.join(set_clauses)} WHERE server_id = ? AND role_id = ?"
        conn.execute(query, tuple(params))
        conn.commit()

def clear_role_template_part(server_id: str, role_id: str, event_type: str, part: str):
    with get_connection() as conn:
        fields_to_null: List[str] = []
        if part == "title":
            fields_to_null.append(f"{event_type}_custom_title")
        elif part == "description":
            fields_to_null.append(f"{event_type}_custom_description")
        elif part == "content":
            fields_to_null.append(f"{event_type}_custom_content")
        elif part == "all_embed_parts":
            fields_to_null.append(f"{event_type}_custom_title")
            fields_to_null.append(f"{event_type}_custom_description")
        elif part == "all":
            fields_to_null.append(f"{event_type}_custom_title")
            fields_to_null.append(f"{event_type}_custom_description")
            fields_to_null.append(f"{event_type}_custom_content")

        if not fields_to_null:
            return

        set_clauses = [f"{field} = NULL" for field in fields_to_null]
        query = f"UPDATE watched_roles SET {', '.join(set_clauses)} WHERE server_id = ? AND role_id = ?"
        conn.execute(query, (server_id, role_id))
        conn.commit()

# --- Active Role Messages Functions ---
def get_active_message(server_id: str, user_id: str, role_id: str) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT webhook_message_id, message_state FROM active_role_messages "
            "WHERE server_id = ? AND user_id = ? AND role_id = ?",
            (server_id, user_id, role_id)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

def update_active_message(server_id: str, user_id: str, role_id: str, webhook_message_id: str, message_state: str):
    with get_connection() as conn:
        conn.execute(
            "INSERT INTO active_role_messages (server_id, user_id, role_id, webhook_message_id, message_state) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(server_id, user_id, role_id) DO UPDATE SET "
            "webhook_message_id = excluded.webhook_message_id, message_state = excluded.message_state",
            (server_id, user_id, role_id, webhook_message_id, message_state)
        )
        conn.commit()

def delete_active_message(server_id: str, user_id: str, role_id: str):
     with get_connection() as conn:
        conn.execute(
            "DELETE FROM active_role_messages WHERE server_id = ? AND user_id = ? AND role_id = ?",
            (server_id, user_id, role_id)
        )
        conn.commit()

def delete_all_active_messages_for_role(server_id: str, role_id: str) -> List[str]:
    """Deletes all active messages for a role and returns their webhook_message_ids."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT webhook_message_id FROM active_role_messages WHERE server_id = ? AND role_id = ?",
            (server_id, role_id)
        )
        message_ids = [row['webhook_message_id'] for row in cursor.fetchall()]
        
        cursor.execute(
            "DELETE FROM active_role_messages WHERE server_id = ? AND role_id = ?",
            (server_id, role_id)
        )
        conn.commit()
        return message_ids