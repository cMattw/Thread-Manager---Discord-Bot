import sqlite3
import logging
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timezone
import os

DB_DIRECTORY = "/home/mattw/Projects/discord_ticket_manager/data/"
PROD_DB_PATH = "/home/container/data/"

INVITES_DATABASE_NAME = os.path.join(DB_DIRECTORY, "invites_cog.db")

# Determine which DB path to use
DB_DIRECTORY = PROD_DB_PATH if os.path.exists("/home/container/") else DB_DIRECTORY
# Ensure the directory exists
os.makedirs(os.path.dirname(DB_DIRECTORY), exist_ok=True)

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
            join_timestamp INTEGER NOT NULL, -- Stored as Unix timestamp
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
    # Ensure guild exists in config
    cursor.execute("INSERT OR IGNORE INTO invite_cog_config (guild_id) VALUES (?)", (guild_id,))
    # Validate key against table columns to prevent SQL injection if key comes from unsafe source
    # For this context, assuming key is hardcoded in the cog and safe.
    try:
        cursor.execute(f"UPDATE invite_cog_config SET {key} = ? WHERE guild_id = ?", (value, guild_id))
        conn.commit()
    except sqlite3.Error as e: logging.error(f"Invites DB Error updating invite_cog_config for key {key}: {e}")
    finally: conn.close()

# --- Invite Processing Functions ---
def record_join(guild_id: int, invited_user_id: int, inviter_user_id: int, invite_code: Optional[str], is_initially_valid: bool):
    conn = get_invites_db_connection(); cursor = conn.cursor()
    join_ts = int(datetime.now(timezone.utc).timestamp())
    is_valid_int = 1 if is_initially_valid else 0
    try:
        # Record the invited member
        cursor.execute("""
            INSERT OR REPLACE INTO invited_members 
            (invited_user_id, guild_id, inviter_user_id, invite_code, join_timestamp, is_currently_valid) 
            VALUES (?, ?, ?, ?, ?, ?)
            """, (invited_user_id, guild_id, inviter_user_id, invite_code, join_ts, is_valid_int))
        
        # Update inviter's stats
        # The excluded.total_valid_invites in the ON CONFLICT clause refers to the 'is_valid_int' from the new insert.
        cursor.execute("""
            INSERT INTO inviter_stats (inviter_user_id, guild_id, total_raw_invites, total_valid_invites) 
            VALUES (?, ?, 1, ?) 
            ON CONFLICT(inviter_user_id, guild_id) 
            DO UPDATE SET 
                total_raw_invites = total_raw_invites + 1, 
                total_valid_invites = total_valid_invites + excluded.total_valid_invites
            """, (inviter_user_id, guild_id, is_valid_int))
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Invites DB Error in record_join: {e}")
    finally: conn.close()

def get_invited_member_details(guild_id: int, invited_user_id: int) -> Optional[Dict[str, Any]]:
    """ Fetches details for a specific invited member, including inviter, code, join time, and validity. """
    conn = get_invites_db_connection(); cursor = conn.cursor()
    cursor.execute("""
        SELECT inviter_user_id, invite_code, join_timestamp, is_currently_valid 
        FROM invited_members 
        WHERE invited_user_id = ? AND guild_id = ?
        """, (invited_user_id, guild_id))
    row = cursor.fetchone(); conn.close()
    if row:
        return {
            "inviter_user_id": row["inviter_user_id"],
            "invite_code": row["invite_code"],
            "joined_at": datetime.fromtimestamp(row["join_timestamp"], tz=timezone.utc) if row["join_timestamp"] else None,
            "is_currently_valid": bool(row["is_currently_valid"])
        }
    return None

def update_invited_member_validity(guild_id: int, invited_user_id: int, inviter_user_id: int, is_now_valid: bool):
    conn = get_invites_db_connection(); cursor = conn.cursor()
    is_valid_int = 1 if is_now_valid else 0
    
    try:
        # Get current validity to determine change
        cursor.execute("SELECT is_currently_valid FROM invited_members WHERE invited_user_id = ? AND guild_id = ?",
                       (invited_user_id, guild_id))
        current_state_row = cursor.fetchone()

        if current_state_row is None:
            logging.warning(f"No record found for member {invited_user_id} in guild {guild_id} to update validity.")
            return

        currently_is_valid = bool(current_state_row['is_currently_valid'])
        change_in_valid_count = 0
        if is_now_valid and not currently_is_valid:
            change_in_valid_count = 1
        elif not is_now_valid and currently_is_valid:
            change_in_valid_count = -1

        if change_in_valid_count != 0: # Only update if there's an actual change
            cursor.execute("UPDATE invited_members SET is_currently_valid = ? WHERE invited_user_id = ? AND guild_id = ?",
                           (is_valid_int, invited_user_id, guild_id))
            
            cursor.execute("""
                UPDATE inviter_stats 
                SET total_valid_invites = MAX(0, total_valid_invites + ?) 
                WHERE inviter_user_id = ? AND guild_id = ?
                """, (change_in_valid_count, inviter_user_id, guild_id))
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Invites DB Error in update_invited_member_validity: {e}")
    finally: conn.close()

def record_leave(guild_id: int, leaving_user_id: int) -> Optional[Tuple[int, bool]]:
    conn = get_invites_db_connection(); cursor = conn.cursor()
    inviter_id = None
    was_valid_at_leave = False # Renamed for clarity
    try:
        cursor.execute("SELECT inviter_user_id, is_currently_valid FROM invited_members WHERE invited_user_id = ? AND guild_id = ?", (leaving_user_id, guild_id))
        row = cursor.fetchone()
        if row:
            inviter_id = row['inviter_user_id']
            was_valid_at_leave = bool(row['is_currently_valid'])
            
            # Decrement inviter_stats
            if inviter_id: # Ensure inviter_id is not None before trying to update stats
                cursor.execute("UPDATE inviter_stats SET total_raw_invites = MAX(0, total_raw_invites - 1) WHERE inviter_user_id = ? AND guild_id = ?", (inviter_id, guild_id))
                if was_valid_at_leave:
                    cursor.execute("UPDATE inviter_stats SET total_valid_invites = MAX(0, total_valid_invites - 1) WHERE inviter_user_id = ? AND guild_id = ?", (inviter_id, guild_id))
            
            # Remove the member's record
            cursor.execute("DELETE FROM invited_members WHERE invited_user_id = ? AND guild_id = ?", (leaving_user_id, guild_id))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Invites DB Error in record_leave: {e}")
    finally: conn.close()
    
    return (inviter_id, was_valid_at_leave) if inviter_id is not None else None


def get_inviter_stats(guild_id: int, inviter_user_id: int) -> Dict[str, int]:
    conn = get_invites_db_connection(); cursor = conn.cursor()
    stats = {'total_raw_invites': 0, 'total_valid_invites': 0} # Default if no record
    try:
        cursor.execute("SELECT total_raw_invites, total_valid_invites FROM inviter_stats WHERE inviter_user_id = ? AND guild_id = ?", (inviter_user_id, guild_id))
        row = cursor.fetchone()
        if row: stats = dict(row)
    except sqlite3.Error as e:
        logging.error(f"Invites DB Error in get_inviter_stats: {e}")
    finally: conn.close()
    return stats

# --- New function for /invited command ---
def get_active_invitees(guild_id: int, inviter_user_id: int) -> List[Dict[str, Any]]:
    """ Retrieves a list of members invited by a specific user who are still in the server. """
    conn = get_invites_db_connection(); cursor = conn.cursor()
    invitees_list = []
    try:
        # Since record_leave deletes from invited_members, any member in this table is considered active.
        cursor.execute("""
            SELECT invited_user_id, invite_code, join_timestamp
            FROM invited_members
            WHERE guild_id = ? AND inviter_user_id = ?
            ORDER BY join_timestamp DESC
            """, (guild_id, inviter_user_id))
        
        for row in cursor.fetchall():
            invitees_list.append({
                "member_id": row["invited_user_id"], # Renamed for clarity in the cog
                "used_invite_code": row["invite_code"],
                "joined_at": datetime.fromtimestamp(row["join_timestamp"], tz=timezone.utc) if row["join_timestamp"] else None
            })
    except sqlite3.Error as e:
        logging.error(f"Invites DB Error in get_active_invitees: {e}")
    finally:
        conn.close()
    return invitees_list

def compensate_invites(guild_id: int, user_id: int, amount: int, action: str) -> bool:
    conn = get_invites_db_connection(); cursor = conn.cursor()
    success = False
    try:
        current_stats = get_inviter_stats(guild_id, user_id) # Get current stats to ensure we don't go below zero inappropriately
        
        raw_change = amount if action == "add" else -amount
        valid_change = amount if action == "add" else -amount # Assuming compensation affects both equally

        new_raw = current_stats['total_raw_invites'] + raw_change
        new_valid = current_stats['total_valid_invites'] + valid_change

        # Ensure counts don't drop below zero due to compensation
        final_raw = max(0, new_raw)
        final_valid = max(0, new_valid)
        
        # Update or insert into inviter_stats
        cursor.execute("""
            INSERT INTO inviter_stats (inviter_user_id, guild_id, total_raw_invites, total_valid_invites) 
            VALUES (?, ?, ?, ?)
            ON CONFLICT(inviter_user_id, guild_id) DO UPDATE SET 
                total_raw_invites = ?,
                total_valid_invites = ?
            """, (user_id, guild_id, final_raw, final_valid, final_raw, final_valid))
        conn.commit()
        success = True
    except sqlite3.Error as e: 
        logging.error(f"Invites DB Error compensating invites for user {user_id}: {e}")
    finally: 
        conn.close()
    return success

def get_leaderboard(guild_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    conn = get_invites_db_connection(); cursor = conn.cursor()
    leaders = []
    try:
        cursor.execute("""
            SELECT inviter_user_id, total_valid_invites, total_raw_invites FROM inviter_stats
            WHERE guild_id = ? AND (total_valid_invites > 0 OR total_raw_invites > 0)
            ORDER BY total_valid_invites DESC, total_raw_invites DESC LIMIT ?
        """, (guild_id, limit))
        leaders = [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Invites DB Error fetching leaderboard for guild {guild_id}: {e}")
    finally: conn.close()
    return leaders

# --- Role Reward Functions ---
def add_role_reward(guild_id: int, invite_threshold: int, role_id: int) -> bool:
    conn = get_invites_db_connection(); cursor = conn.cursor()
    success = False
    try:
        # Ensure guild exists in config as a prerequisite (optional, depends on desired strictness)
        # cursor.execute("INSERT OR IGNORE INTO invite_cog_config (guild_id) VALUES (?)", (guild_id,))
        cursor.execute("INSERT INTO invite_role_rewards (guild_id, invite_threshold, role_id) VALUES (?, ?, ?)",
                       (guild_id, invite_threshold, role_id))
        conn.commit()
        success = True
    except sqlite3.IntegrityError: 
        logging.warning(f"Invite role reward for threshold {invite_threshold} or role {role_id} already exists in guild {guild_id}.")
    except sqlite3.Error as e: 
        logging.error(f"Invites DB Error adding invite role reward: {e}")
    finally: 
        conn.close()
    return success

def remove_role_reward(guild_id: int, role_id: int) -> bool:
    conn = get_invites_db_connection(); cursor = conn.cursor()
    success = False
    try:
        cursor.execute("DELETE FROM invite_role_rewards WHERE guild_id = ? AND role_id = ?", (guild_id, role_id))
        conn.commit()
        success = cursor.rowcount > 0
    except sqlite3.Error as e: 
        logging.error(f"Invites DB Error removing invite role reward for role {role_id} in guild {guild_id}: {e}")
    finally: 
        conn.close()
    return success

def get_all_role_rewards(guild_id: int) -> List[Dict[str, Any]]:
    conn = get_invites_db_connection(); cursor = conn.cursor()
    rewards_list = []
    try:
        cursor.execute("SELECT reward_id, invite_threshold, role_id FROM invite_role_rewards WHERE guild_id = ? ORDER BY invite_threshold ASC",
                       (guild_id,))
        rewards_list = [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e: 
        logging.error(f"Invites DB Error fetching all role rewards for guild {guild_id}: {e}")
    finally: 
        conn.close()
    return rewards_list


if __name__ == '__main__':
    print("Running INVITES database module directly for initialization/testing...")
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - [%(module)s.%(funcName)s:%(lineno)d] %(message)s')
    # Example: Ensure the main data directory exists if running this standalone for testing
    if not os.path.exists(DB_DIRECTORY): os.makedirs(DB_DIRECTORY, exist_ok=True)
    initialize_database(guild_id_to_ensure=1) # Pass a dummy guild_id for testing
    
    # Example usage (optional - for direct testing of new/modified functions):
    # test_guild_id = 1
    # test_inviter_id = 100
    # test_invited_id = 200

    # print(f"\n--- Testing record_join for inviter {test_inviter_id}, invited {test_invited_id} ---")
    # record_join(test_guild_id, test_invited_id, test_inviter_id, "testcode123", True)
    # record_join(test_guild_id, test_invited_id + 1, test_inviter_id, "testcode456", False)


    # print(f"\n--- Testing get_active_invitees for inviter {test_inviter_id} ---")
    # active_invitees = get_active_invitees(test_guild_id, test_inviter_id)
    # if active_invitees:
    #     for invitee in active_invitees:
    #         print(f"  Invited Member ID: {invitee['member_id']}, Code: {invitee['used_invite_code']}, Joined: {invitee['joined_at']}")
    # else:
    #     print(f"  No active invitees found for inviter {test_inviter_id}.")

    # print(f"\n--- Testing get_invited_member_details for member {test_invited_id} ---")
    # details = get_invited_member_details(test_guild_id, test_invited_id)
    # if details:
    #     print(f"  Invited by: {details.get('inviter_user_id')}, Code: {details.get('invite_code')}, Joined: {details.get('joined_at')}, Valid: {details.get('is_currently_valid')}")
    # else:
    #     print(f"  No details found for invited member {test_invited_id}.")
        
    # print(f"\n--- Testing record_leave for invited {test_invited_id} ---")
    # leave_info = record_leave(test_guild_id, test_invited_id)
    # if leave_info:
    #     print(f"  Left member {test_invited_id} was invited by {leave_info[0]}, was valid: {leave_info[1]}")
    #     active_invitees_after_leave = get_active_invitees(test_guild_id, test_inviter_id)
    #     print(f"  Active invitees for {test_inviter_id} after leave: {len(active_invitees_after_leave)}")


    print("\nINVITES Database module direct execution finished.")