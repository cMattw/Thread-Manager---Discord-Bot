import sqlite3
import logging
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timezone
import os

DB_DIRECTORY = "/home/container/data"
# DB_DIRECTORY = "/home/mattw/Projects/discord_ticket_manager/data"
INVITES_DATABASE_NAME = os.path.join(DB_DIRECTORY, "invites_cog.db")

def get_invites_db_connection():
    try:
        invites_db_dir = os.path.dirname(INVITES_DATABASE_NAME)
        if invites_db_dir and not os.path.exists(invites_db_dir):
             os.makedirs(invites_db_dir, exist_ok=True)
    except OSError as e:
        logging.error(f"Could not ensure invites database directory {os.path.dirname(INVITES_DATABASE_NAME)} exists: {e}")
    conn = sqlite3.connect(INVITES_DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def initialize_database(guild_id_to_ensure: Optional[int] = None):
    conn = get_invites_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS invited_members (
            invited_user_id INTEGER NOT NULL,
            guild_id INTEGER NOT NULL,
            inviter_user_id INTEGER NOT NULL,
            invite_code TEXT, 
            join_timestamp INTEGER NOT NULL,
            is_currently_valid INTEGER DEFAULT 0, -- 0 for false, 1 for true
            PRIMARY KEY (invited_user_id, guild_id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS inviter_stats (
            inviter_user_id INTEGER NOT NULL,
            guild_id INTEGER NOT NULL,
            total_raw_invites INTEGER DEFAULT 0,    -- All members they invited still in server
            total_valid_invites INTEGER DEFAULT 0,  -- Those who also meet role criteria
            PRIMARY KEY (inviter_user_id, guild_id) 
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS invite_cog_config ( 
            guild_id INTEGER PRIMARY KEY,
            log_channel_id INTEGER,
            leaderboard_webhook_url TEXT,
            leaderboard_message_id INTEGER,
            leaderboard_channel_id INTEGER,
            required_role_id INTEGER DEFAULT NULL -- For the "Katipunero" role
        )
    ''')
    # Attempt to add required_role_id if table exists but column doesn't
    try:
        cursor.execute("SELECT required_role_id FROM invite_cog_config LIMIT 1")
    except sqlite3.OperationalError:
        logging.info("Adding 'required_role_id' to invite_cog_config table.")
        try:
            cursor.execute("ALTER TABLE invite_cog_config ADD COLUMN required_role_id INTEGER DEFAULT NULL")
        except sqlite3.OperationalError as e_alter: # Should not happen if OperationalError was on SELECT
             logging.warning(f"Could not add 'required_role_id' to invite_cog_config: {e_alter}")


    cursor.execute('''
        CREATE TABLE IF NOT EXISTS invite_role_rewards ( 
            reward_id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            invite_threshold INTEGER NOT NULL,
            role_id INTEGER NOT NULL,
            UNIQUE (guild_id, invite_threshold),
            UNIQUE (guild_id, role_id) 
        )
    ''')
    conn.commit()
    if guild_id_to_ensure:
        cursor.execute("INSERT OR IGNORE INTO invite_cog_config (guild_id) VALUES (?)", (guild_id_to_ensure,))
        conn.commit()
    conn.close()
    logging.info(f"Invites Cog: Database '{INVITES_DATABASE_NAME}' initialized/checked successfully.")

# --- Config Functions ---
def get_cog_config(guild_id: int) -> Optional[Dict[str, Any]]:
    conn = get_invites_db_connection(); cursor = conn.cursor()
    cursor.execute("SELECT * FROM invite_cog_config WHERE guild_id = ?", (guild_id,))
    row = cursor.fetchone(); conn.close()
    return dict(row) if row else None

def update_cog_config(guild_id: int, key: str, value: Any):
    conn = get_invites_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO invite_cog_config (guild_id) VALUES (?)", (guild_id,))
        cursor.execute(f"UPDATE invite_cog_config SET {key} = ? WHERE guild_id = ?", (value, guild_id))
        conn.commit()
    except sqlite3.Error as e: logging.error(f"Invites DB Error updating invite_cog_config: {e}")
    finally: conn.close()

# --- Invite Processing Functions ---
def record_join(guild_id: int, invited_user_id: int, inviter_user_id: int, invite_code: Optional[str], is_initially_valid: bool):
    conn = get_invites_db_connection(); cursor = conn.cursor()
    join_ts = int(datetime.now(timezone.utc).timestamp())
    is_valid_int = 1 if is_initially_valid else 0
    try:
        cursor.execute("INSERT OR REPLACE INTO invited_members (invited_user_id, guild_id, inviter_user_id, invite_code, join_timestamp, is_currently_valid) VALUES (?, ?, ?, ?, ?, ?)",
                       (invited_user_id, guild_id, inviter_user_id, invite_code, join_ts, is_valid_int))
        
        cursor.execute("INSERT INTO inviter_stats (inviter_user_id, guild_id, total_raw_invites, total_valid_invites) VALUES (?, ?, 1, ?) ON CONFLICT(inviter_user_id, guild_id) DO UPDATE SET total_raw_invites = total_raw_invites + 1, total_valid_invites = total_valid_invites + excluded.total_valid_invites",
                       (inviter_user_id, guild_id, is_valid_int)) # excluded.total_valid_invites will be 'is_valid_int'
        conn.commit()
    finally: conn.close()

def get_invited_member_details(guild_id: int, invited_user_id: int) -> Optional[Dict[str, Any]]:
    conn = get_invites_db_connection(); cursor = conn.cursor()
    cursor.execute("SELECT inviter_user_id, is_currently_valid FROM invited_members WHERE invited_user_id = ? AND guild_id = ?", (invited_user_id, guild_id))
    row = cursor.fetchone(); conn.close()
    return dict(row) if row else None

def update_invited_member_validity(guild_id: int, invited_user_id: int, inviter_user_id: int, is_now_valid: bool):
    conn = get_invites_db_connection(); cursor = conn.cursor()
    is_valid_int = 1 if is_now_valid else 0
    change_in_valid_count = 1 if is_now_valid else -1
    try:
        # First update the invited_members table
        cursor.execute("UPDATE invited_members SET is_currently_valid = ? WHERE invited_user_id = ? AND guild_id = ?", 
                       (is_valid_int, invited_user_id, guild_id))
        
        # Then update the inviter_stats for total_valid_invites
        # This ensures total_valid_invites doesn't go below zero
        cursor.execute("UPDATE inviter_stats SET total_valid_invites = MAX(0, total_valid_invites + ?) WHERE inviter_user_id = ? AND guild_id = ?",
                       (change_in_valid_count, inviter_user_id, guild_id))
        conn.commit()
    finally: conn.close()

def record_leave(guild_id: int, leaving_user_id: int) -> Optional[Tuple[int, bool]]: # Returns (inviter_id, was_valid)
    conn = get_invites_db_connection(); cursor = conn.cursor()
    inviter_id = None
    was_valid = False
    try:
        cursor.execute("SELECT inviter_user_id, is_currently_valid FROM invited_members WHERE invited_user_id = ? AND guild_id = ?", (leaving_user_id, guild_id))
        row = cursor.fetchone()
        if row:
            inviter_id = row['inviter_user_id']
            was_valid = bool(row['is_currently_valid'])
            
            cursor.execute("UPDATE inviter_stats SET total_raw_invites = MAX(0, total_raw_invites - 1) WHERE inviter_user_id = ? AND guild_id = ?", (inviter_id, guild_id))
            if was_valid:
                cursor.execute("UPDATE inviter_stats SET total_valid_invites = MAX(0, total_valid_invites - 1) WHERE inviter_user_id = ? AND guild_id = ?", (inviter_id, guild_id))
            
            cursor.execute("DELETE FROM invited_members WHERE invited_user_id = ? AND guild_id = ?", (leaving_user_id, guild_id))
            conn.commit()
    finally: conn.close()
    return (inviter_id, was_valid) if inviter_id is not None else None


def get_inviter_stats(guild_id: int, inviter_user_id: int) -> Dict[str, int]:
    conn = get_invites_db_connection(); cursor = conn.cursor()
    stats = {'total_raw_invites': 0, 'total_valid_invites': 0}
    try:
        cursor.execute("SELECT total_raw_invites, total_valid_invites FROM inviter_stats WHERE inviter_user_id = ? AND guild_id = ?", (inviter_user_id, guild_id))
        row = cursor.fetchone()
        if row: stats = dict(row)
    finally: conn.close()
    return stats

def compensate_invites(guild_id: int, user_id: int, amount: int, action: str) -> bool:
    conn = get_invites_db_connection(); cursor = conn.cursor()
    # Compensation affects both raw and valid invites to maintain consistency
    try:
        if action == "add":
            cursor.execute("""
                INSERT INTO inviter_stats (inviter_user_id, guild_id, total_raw_invites, total_valid_invites) VALUES (?, ?, ?, ?)
                ON CONFLICT(inviter_user_id, guild_id) DO UPDATE SET 
                total_raw_invites = total_raw_invites + excluded.total_raw_invites,
                total_valid_invites = total_valid_invites + excluded.total_valid_invites
            """, (user_id, guild_id, amount, amount))
        elif action == "remove":
            cursor.execute("""
                INSERT INTO inviter_stats (inviter_user_id, guild_id, total_raw_invites, total_valid_invites) VALUES (?, ?, 0, 0)
                ON CONFLICT(inviter_user_id, guild_id) DO UPDATE SET 
                total_raw_invites = MAX(0, total_raw_invites - ?),
                total_valid_invites = MAX(0, total_valid_invites - ?)
            """, (user_id, guild_id, amount, amount))
        else: return False
        conn.commit(); return True
    except sqlite3.Error as e: logging.error(f"Invites DB Error compensating: {e}"); return False
    finally: conn.close()

def get_leaderboard(guild_id: int, limit: int = 10) -> List[Dict[str, Any]]: # Fetches both counts
    conn = get_invites_db_connection(); cursor = conn.cursor()
    leaders = []
    try:
        # Order by valid invites, then raw invites as a tie-breaker
        cursor.execute("""
            SELECT inviter_user_id, total_valid_invites, total_raw_invites FROM inviter_stats 
            WHERE guild_id = ? AND (total_valid_invites > 0 OR total_raw_invites > 0)
            ORDER BY total_valid_invites DESC, total_raw_invites DESC LIMIT ?
        """, (guild_id, limit))
        leaders = [dict(row) for row in cursor.fetchall()]
    finally: conn.close()
    return leaders

# Role Reward Functions (add_role_reward, remove_role_reward, get_all_role_rewards)
# remain structurally the same, but they operate on the 'invite_role_rewards' table
# and use get_invites_db_connection().

def add_role_reward(guild_id: int, invite_threshold: int, role_id: int) -> bool:
    conn = get_invites_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO invite_cog_config (guild_id) VALUES (?)", (guild_id,))
        cursor.execute("INSERT INTO invite_role_rewards (guild_id, invite_threshold, role_id) VALUES (?, ?, ?)",
                       (guild_id, invite_threshold, role_id))
        conn.commit(); return True
    except sqlite3.IntegrityError: logging.warning(f"Invite role reward for threshold {invite_threshold} or role {role_id} already exists in guild {guild_id}."); return False
    except sqlite3.Error as e: logging.error(f"Invites DB Error adding invite role reward: {e}"); return False
    finally: conn.close()

def remove_role_reward(guild_id: int, role_id: int) -> bool:
    conn = get_invites_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM invite_role_rewards WHERE guild_id = ? AND role_id = ?", (guild_id, role_id))
        conn.commit(); return cursor.rowcount > 0
    except sqlite3.Error as e: logging.error(f"Invites DB Error removing invite role reward for role {role_id}: {e}"); return False
    finally: conn.close()

def get_all_role_rewards(guild_id: int) -> List[Dict[str, Any]]:
    conn = get_invites_db_connection(); cursor = conn.cursor()
    rewards = []
    try:
        cursor.execute("SELECT reward_id, invite_threshold, role_id FROM invite_role_rewards WHERE guild_id = ? ORDER BY invite_threshold ASC", 
                       (guild_id,))
        rewards = [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e: logging.error(f"Invites DB Error fetching invite role rewards for guild {guild_id}: {e}")
    finally: conn.close()
    return rewards


if __name__ == '__main__':
    print("Running INVITES database module directly for initialization/testing...")
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - [%(module)s.%(funcName)s:%(lineno)d] %(message)s')
    # Example: Ensure the main data directory exists if running this standalone for testing
    if not os.path.exists(DB_DIRECTORY): os.makedirs(DB_DIRECTORY, exist_ok=True)
    initialize_database(guild_id_to_ensure=1) # Pass a dummy guild_id for testing
    print("\nINVITES Database module direct execution finished.")