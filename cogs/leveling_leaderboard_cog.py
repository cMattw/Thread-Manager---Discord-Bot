import nextcord
from nextcord.ext import commands, tasks
from nextcord import (
    Interaction, SlashOption, Embed, Color, TextChannel, Role, Member,
    Forbidden, HTTPException, Message, Intents, RawMessageUpdateEvent
)
import asyncio
import sqlite3
import time
import os
import logging
import re  # For parsing messages
from typing import Optional, List, Dict, Tuple, Any
import aiohttp  # For webhooks

# --- Configuration ---
# !!! SET THESE VALUES FOR YOUR BOT !!!
TARGET_GUILD_ID = 992662612401725502  # YOUR SERVER ID
DEV_DB_PATH = "/home/mattw/Projects/discord_ticket_manager/data/levelboard.db"  # Example: "data/levelboard.db"
PROD_DB_PATH = "/home/container/data/levelboard.db" # Example: "/home/container/data/levelboard.db"
# !!! END OF USER CONFIGURATION !!!

# Determine which DB path to use
db_dir = os.path.dirname(PROD_DB_PATH if os.path.exists(os.path.dirname(PROD_DB_PATH)) and os.path.isdir(os.path.dirname(PROD_DB_PATH)) else DEV_DB_PATH)
if db_dir and not os.path.exists(db_dir): # Ensure directory exists if it's not current dir
    try:
        os.makedirs(db_dir, exist_ok=True)
        print(f"INFO: Created directory for database: {db_dir}")
    except Exception as e:
        print(f"ERROR: Could not create directory {db_dir}: {e}")
DB_PATH = os.path.join(db_dir, os.path.basename(PROD_DB_PATH if os.path.exists(db_dir) else DEV_DB_PATH))
if db_dir == "": # If db_dir is empty, it means current directory
    DB_PATH = os.path.basename(PROD_DB_PATH if os.path.exists(PROD_DB_PATH) else DEV_DB_PATH)


# --- Logging Setup ---
logger = logging.getLogger('nextcord.leveling_cog_v4') # Incremented version for logger

# --- Regex Patterns ---
FULL_LEADERBOARD_ENTRY_PATTERN = re.compile(
    r"^<:\w+:\d+>\s+"
    r"\*\*(?:\d+)(?:st|nd|rd|th)\*\*\s+"
    r"<@!?(\d+)>\s*"
    r"<:\w+:\d+>\s+level\s+"
    r"\*\*(\d+)\*\*\s*"
    r"`\(([\d,]+)\s+xp/\s*([\d,]+)\s+xp\)`\s*$"
)
LEVELUP_LEVEL_PATTERN = re.compile(r"Current Level:\s*(\d+)")
LEVELUP_XP_PATTERN = re.compile(
    r"Current XP:\s*([\d,]+)\s*/\s*([\d,]+)"
)

# --- Database Helper Class ---
class LevelingDatabase:
    def __init__(self, db_path):
        self.db_path = db_path
        logger.info(f"Database will be initialized at: {os.path.abspath(self.db_path)}")
        self.create_tables()

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def create_tables(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cog_settings (
                    config_id TEXT PRIMARY KEY, webhook_url TEXT,
                    update_interval_minutes INTEGER DEFAULT 10, last_webhook_message_id TEXT,
                    top1_role_id TEXT, previous_top1_discord_id TEXT,
                    rank_emoji_up TEXT DEFAULT '▲', rank_emoji_down TEXT DEFAULT '▼',
                    rank_emoji_new TEXT DEFAULT '✦', rank_emoji_same TEXT DEFAULT '►',
                    error_notification_channel_id TEXT, updates_enabled INTEGER DEFAULT 1,
                    last_data_update_timestamp INTEGER, source_bot_id TEXT,
                    levelup_channel_id TEXT, leaderboard_channel_id TEXT 
                )
            """)
            cursor.execute(f"INSERT OR IGNORE INTO cog_settings (config_id) VALUES (?)", (str(TARGET_GUILD_ID),))
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS leaderboard_data_cache (
                    discord_user_id TEXT PRIMARY KEY, display_name TEXT,
                    current_level INTEGER, xp_in_current_level INTEGER,
                    xp_needed_for_current_level INTEGER, last_update_timestamp INTEGER
                )
            """)
            conn.commit()

    def get_setting(self, key: str, default: Any = None) -> Any:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT {key} FROM cog_settings WHERE config_id = ?", (str(TARGET_GUILD_ID),))
            row = cursor.fetchone()
            return row[0] if row and row[0] is not None else default

    def update_setting(self, key: str, value: Any):
        with self._get_connection() as conn:
            conn.execute(f"UPDATE cog_settings SET {key} = ? WHERE config_id = ?", (value, str(TARGET_GUILD_ID)))
            conn.commit()

    def get_all_settings(self) -> Dict[str, Any]:
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM cog_settings WHERE config_id = ?", (str(TARGET_GUILD_ID),))
            row = cursor.fetchone()
            return dict(row) if row else {}
            
    def update_user_from_full_leaderboard(self, user_id: str, name: str, level: int, xp_in_level: int, xp_needed: int):
        timestamp = int(time.time())
        with self._get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO leaderboard_data_cache 
                (discord_user_id, display_name, current_level, xp_in_current_level, xp_needed_for_current_level, last_update_timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (user_id, name, level, xp_in_level, xp_needed, timestamp))
            conn.commit()
        self.update_setting('last_data_update_timestamp', timestamp) # Update timestamp after successful commit
        logger.info(f"DB: Updated user {name} (ID: {user_id}) from full LB: L{level} {xp_in_level}/{xp_needed} XP")

    def update_user_from_levelup(self, user_id: str, name: str, new_level: int, xp_span_for_new_level: int):
        timestamp = int(time.time())
        with self._get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO leaderboard_data_cache
                (discord_user_id, display_name, current_level, xp_in_current_level, xp_needed_for_current_level, last_update_timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (user_id, name, new_level, 0, xp_span_for_new_level, timestamp))
            conn.commit()
        self.update_setting('last_data_update_timestamp', timestamp) # Update timestamp after successful commit
        logger.info(f"DB: Updated user {name} (ID: {user_id}) from levelup: New L{new_level}, XP span {xp_span_for_new_level}")

    def get_all_leaderboard_users(self) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM leaderboard_data_cache") # Select all columns for sorting flexibility
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

# --- Cog Class ---
class LevelingLeaderboardCog(commands.Cog, name="LevelingLeaderboard"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.target_guild_id_int = TARGET_GUILD_ID
        self.db = LevelingDatabase(DB_PATH)
        
        self.leaderboard_data: List[Dict[str, Any]] = [] 
        self.previous_leaderboard_data: List[Dict[str, Any]] = []
        self._update_lock = asyncio.Lock() # Lock for update cycle

        current_interval = self.db.get_setting('update_interval_minutes', 10)
        self.update_leaderboard_task.change_interval(minutes=current_interval)
        if self.db.get_setting('updates_enabled', 1) == 1:
            self.update_leaderboard_task.start()
        logger.info(f"LevelingLeaderboardCog (V4 - Bot Interaction Mode) initialized. DB: {DB_PATH}")

    def cog_unload(self):
        self.update_leaderboard_task.cancel()
        logger.info("LevelingLeaderboardCog unloaded.")

    async def _get_guild(self) -> Optional[nextcord.Guild]:
        guild = self.bot.get_guild(self.target_guild_id_int)
        if not guild: logger.error(f"Target guild {self.target_guild_id_int} not found.")
        return guild

    async def _resolve_user_details(self, guild: nextcord.Guild, user_id_str: Optional[str], username_text: Optional[str]) -> Tuple[Optional[str], str]:
        member: Optional[Member] = None; final_user_id: Optional[str] = user_id_str
        display_name_to_use: str = username_text or (f"User_{user_id_str}" if user_id_str else "Member Left")
        
        if user_id_str:
            try: member = await guild.fetch_member(int(user_id_str))
            except ValueError: 
                 if username_text: member = guild.get_member_named(username_text)
            except (nextcord.NotFound, Forbidden): pass # Logged in calling functions if needed
        
        if not member and username_text: 
            member_by_name = guild.get_member_named(username_text)
            if member_by_name: member = member_by_name; final_user_id = str(member.id) if not final_user_id else final_user_id

        if member: display_name_to_use = member.display_name; final_user_id = str(member.id)
        
        return final_user_id, display_name_to_use

    async def _process_source_bot_message(self, message: Message):
        guild = message.guild
        if not guild: return
        processed_data_in_this_call = False # Flag if any DB update happened

        if not message.embeds:
            logger.debug(f"Processor: Message {message.id} has no embeds. Cannot process for LB/LU data.")
            return

        embed = message.embeds[0]
        logger.debug(f"Processor: Evaluating message {message.id} with embed. Title: '{embed.title}', Author: '{embed.author.name if embed.author else 'N/A'}'")

        # Full Leaderboard Parsing
        is_lb_title = embed.title and "leaderboard" in embed.title.lower()
        is_lb_author = embed.author and embed.author.name and "katipunan smp" in embed.author.name.lower()
        is_leaderboard_embed = is_lb_title or is_lb_author
        lb_channel_id_str = self.db.get_setting('leaderboard_channel_id')
        in_lb_channel = False
        if lb_channel_id_str:
            try: in_lb_channel = (message.channel.id == int(lb_channel_id_str))
            except ValueError: logger.warning(f"Invalid leaderboard_channel_id in DB: {lb_channel_id_str}")
        
        process_full_lb = is_leaderboard_embed and embed.description and (not lb_channel_id_str or in_lb_channel)

        if process_full_lb:
            logger.info(f"Processor: Attempting to parse embed from message {message.id} as FULL LEADERBOARD.")
            lines = embed.description.split('\n'); parsed_count = 0
            for i, line in enumerate(lines):
                match = FULL_LEADERBOARD_ENTRY_PATTERN.match(line.strip())
                if match:
                    uid_mention, lvl_str, xp_in_lvl_str, xp_needed_str = match.groups() # Adjusted for new regex
                    try:
                        lvl, xp_in, xp_need = int(lvl_str), int(xp_in_lvl_str.replace(',', '')), int(xp_needed_str.replace(',', ''))
                        real_uid, real_dname = await self._resolve_user_details(guild, uid_mention, None)
                        if real_uid: self.db.update_user_from_full_leaderboard(real_uid, real_dname, lvl, xp_in, xp_need); parsed_count += 1; processed_data_in_this_call = True
                        else: logger.warning(f"Processor: Could not resolve user from full LB line: {line.strip()}")
                    except ValueError as ve: logger.warning(f"Processor: ValueError parsing numbers in full LB line '{line.strip()}': {ve}")
            if parsed_count > 0: logger.info(f"Processor: Updated {parsed_count} users from full LB (msg {message.id}).")
            else: logger.warning(f"Processor: Identified msg {message.id} as full LB, but no entries parsed. Desc: ```{embed.description}```")
            if processed_data_in_this_call: await self._execute_leaderboard_update_cycle(); return # Update and exit

        # Level-Up Parsing (only if not processed as full LB)
        if embed.author and embed.author.name: # Level-up embeds usually have an author
            lu_channel_id_str = self.db.get_setting('levelup_channel_id')
            in_lu_channel = False
            if lu_channel_id_str:
                try: in_lu_channel = (message.channel.id == int(lu_channel_id_str))
                except ValueError: logger.warning(f"Invalid levelup_channel_id in DB: {lu_channel_id_str}")

            process_level_up = not lu_channel_id_str or in_lu_channel
            if process_level_up:
                is_lu_title = embed.title and "leveled up!" in embed.title.lower()
                has_lu_desc_info = False
                if embed.description:
                    has_lu_desc_info = "Current Level:" in embed.description and "Current XP:" in embed.description
                    # logger.debug(f"  Level-up desc content for msg {message.id}: ```{embed.description}```")

                if is_lu_title and has_lu_desc_info:
                    logger.info(f"Processor: Attempting to parse embed from message {message.id} as LEVEL-UP.")
                    final_uid, final_dname = None, "Member Left"
                    title_username = embed.title.split(',')[0].strip() if embed.title else None

                    if message.mentions:
                        pinged = message.mentions[0]
                        if title_username and (pinged.name.lower() == title_username.lower() or pinged.display_name.lower() == title_username.lower() or (pinged.global_name and pinged.global_name.lower() == title_username.lower())):
                            final_uid, final_dname = str(pinged.id), pinged.display_name
                            logger.info(f"  LU User CONFIRMED from mentions: {final_dname} (ID: {final_uid})")
                    
                    if not final_uid and title_username: # Fallback to title name if no conclusive ping
                        temp_id, temp_name = await self._resolve_user_details(guild, None, title_username)
                        if temp_id: final_uid, final_dname = temp_id, temp_name
                        logger.info(f"  LU User resolved from title: {final_dname} (ID: {final_uid})")
                    
                    if not final_uid: logger.warning(f"Processor: LU: Could not ID user for msg {message.id}. Title:'{embed.title}'"); return

                    newLvl, xpSpan = None, None
                    if embed.description:
                        for line in embed.description.split('\n'):
                            lvl_match = LEVELUP_LEVEL_PATTERN.search(line)
                            if lvl_match: newLvl = int(lvl_match.group(1))
                            xp_match = LEVELUP_XP_PATTERN.search(line)
                            if xp_match: xpSpan = int(xp_match.group(2).replace(',', '')) # Group 2 is B value
                    
                    if newLvl is not None and xpSpan is not None:
                        logger.debug(f"    LU Parsed for msg {message.id}: User {final_dname}, NewLvl={newLvl}, XPSpan={xpSpan}")
                        self.db.update_user_from_levelup(final_uid, final_dname, newLvl, xpSpan)
                        await self._execute_leaderboard_update_cycle() # Trigger instant update
                    else: logger.warning(f"  Could not parse Lvl/XP from LU desc for {final_dname} (msg {message.id}). Lvl:{newLvl}, XPSpan:{xpSpan}. Desc: ```{embed.description}```")

    @commands.Cog.listener()
    async def on_message(self, message: Message):
        if message.guild is None or message.guild.id != self.target_guild_id_int or message.author.bot is False:
            return
        source_bot_id_str = self.db.get_setting('source_bot_id')
        if not source_bot_id_str: return
        try:
            if message.author.id != int(source_bot_id_str): return
        except ValueError: logger.error(f"Invalid source_bot_id: {source_bot_id_str}"); return

        logger.info(f"on_message: New message {message.id} from source bot {message.author.id} in chan {message.channel.id}. Forwarding to processor.")
        await self._process_source_bot_message(message)

    @commands.Cog.listener()
    async def on_raw_message_edit(self, payload: RawMessageUpdateEvent):
        if payload.guild_id is None or payload.guild_id != self.target_guild_id_int: return
        source_bot_id_str = self.db.get_setting('source_bot_id')
        if not source_bot_id_str: return
        if 'embeds' not in payload.data and 'content' not in payload.data : return # Only care if embeds/content changed
            
        channel = self.bot.get_channel(payload.channel_id)
        if not isinstance(channel, TextChannel): return

        try: message = await channel.fetch_message(payload.message_id)
        except (nextcord.NotFound, nextcord.Forbidden, Exception) as e: logger.warning(f"on_raw_edit: Fetch fail for {payload.message_id}: {e}"); return
            
        if message.author.id == int(source_bot_id_str):
            logger.info(f"on_raw_message_edit: EDITED message {message.id} from source bot. Forwarding to processor.")
            await self._process_source_bot_message(message)

    async def _execute_leaderboard_update_cycle(self):
        async with self._update_lock:
            logger.info("Executing leaderboard update cycle...")
            guild = await self._get_guild()
            if not guild: logger.error("Update Cycle: Guild not found."); return

            all_users_data = self.db.get_all_leaderboard_users()
            if not all_users_data: logger.info("Update Cycle: No data in local cache."); self.leaderboard_data = [];
            else:
                logger.debug(f"Update Cycle: Processing {len(all_users_data)} users from DB.")
                sorted_users = sorted(all_users_data, key=lambda u: (u.get('current_level', 0), u.get('xp_in_current_level', 0)), reverse=True)
                processed_lb = []
                for i, ud in enumerate(sorted_users[:10]):
                    rank, uid, db_name = i + 1, ud.get('discord_user_id'), ud.get('display_name', 'Unknown')
                    emb_disp = db_name; txt_name = db_name
                    if uid:
                        member = guild.get_member(int(uid))
                        if member: emb_disp, txt_name = member.mention, member.display_name
                        else: emb_disp = "Member Left" # As requested
                    processed_lb.append({
                        "rank": rank, "discord_user_id": uid, "display_name_text": txt_name, 
                        "display_name_mention": emb_disp, "level": ud.get('current_level', 0),
                        "xp_display": f"{ud.get('xp_in_current_level', 0)}/{ud.get('xp_needed_for_current_level', '?')} XP"
                    })
                self.previous_leaderboard_data = list(self.leaderboard_data); self.leaderboard_data = processed_lb
            
            embed = await self._create_leaderboard_embed()
            await self._post_or_edit_webhook(embed)
            await self._update_top1_role()
            logger.info("Leaderboard update cycle completed.")

    @tasks.loop(minutes=10)
    async def update_leaderboard_task(self):
        logger.info("Scheduled leaderboard update task triggered.")
        await self._execute_leaderboard_update_cycle()

    @update_leaderboard_task.before_loop
    async def before_update_leaderboard_task(self):
        await self.bot.wait_until_ready()
        logger.info("Leaderboard task before_loop: Bot ready.")

    async def _create_leaderboard_embed(self) -> Embed:
        # (Implementation from previous - uses self.leaderboard_data, formats XP in code block)
        guild = await self._get_guild()
        embed_color = guild.me.color if guild and guild.me else Color.blue()
        title = "Level Leaderboard"; embed = Embed(title=title, color=embed_color); description_lines = []
        if not self.leaderboard_data: description_lines.append("Leaderboard data is being collected...")
        else:
            for ud in self.leaderboard_data:
                id_for_rank = ud.get('discord_user_id') or ud.get('display_name_text')
                emoji = await self._get_rank_change_emoji(ud['rank'], id_for_rank, bool(ud.get('discord_user_id')))
                description_lines.append(f"{ud['rank']}. {emoji} {ud['display_name_mention']} - Level {ud['level']} `({ud['xp_display']})`")
        
        last_ts = self.db.get_setting('last_data_update_timestamp')
        description_lines.append(f"\nData last processed: {f'<t:{last_ts}:R>' if last_ts else 'Never'}")
        next_ts = int(time.time()) 
        if self.update_leaderboard_task.is_running() and self.update_leaderboard_task.next_iteration:
            next_ts = int(self.update_leaderboard_task.next_iteration.timestamp())
        description_lines.append(f"Next update: <t:{next_ts}:R>")
        embed.description = "\n".join(description_lines)
        embed.set_footer(text="Leveling Data Provided by Atom"); embed.timestamp = nextcord.utils.utcnow()
        return embed

    async def _get_rank_change_emoji(self, current_rank: int, user_identifier: str, is_id: bool) -> str:
        # (Implementation from previous - compares with self.previous_leaderboard_data)
        settings = self.db.get_all_settings()
        default_same = settings.get('rank_emoji_same', '►') if settings.get('rank_emoji_same') else ''
        if not self.previous_leaderboard_data: return settings.get('rank_emoji_new', '✦')
        for prev_user in self.previous_leaderboard_data:
            prev_key = prev_user.get("discord_user_id") if prev_user.get("discord_user_id") else prev_user.get("display_name_text")
            current_key = user_identifier # This is already the ID or name
            if prev_key == current_key:
                prev_rank = prev_user["rank"]
                if current_rank < prev_rank: return settings.get('rank_emoji_up', '▲')
                elif current_rank > prev_rank: return settings.get('rank_emoji_down', '▼')
                else: return default_same
        return settings.get('rank_emoji_new', '✦')

    async def _post_or_edit_webhook(self, embed: Embed):
        # (Implementation from previous - uses aiohttp session)
        webhook_url = self.db.get_setting('webhook_url')
        if not webhook_url: logger.warning("Webhook URL not set."); return
        msg_id_str = self.db.get_setting('last_webhook_message_id')
        msg_id = int(msg_id_str) if msg_id_str else None
        session: Optional[aiohttp.ClientSession] = None
        try:
            session = aiohttp.ClientSession()
            webhook = nextcord.Webhook.from_url(webhook_url, session=session)
            if msg_id: await webhook.edit_message(msg_id, embed=embed)
            else: sent_msg = await webhook.send(embed=embed, wait=True); self.db.update_setting('last_webhook_message_id', str(sent_msg.id))
        except (nextcord.NotFound, nextcord.HTTPException, aiohttp.ClientResponseError) as e:
            logger.warning(f"Webhook op failed for msg {msg_id} (Error: {type(e).__name__}: {e}). Trying new send.")
            new_session: Optional[aiohttp.ClientSession] = None
            try:
                if session and not session.closed: await session.close() # Close original session if still open
                new_session = aiohttp.ClientSession()
                webhook_retry = nextcord.Webhook.from_url(webhook_url, session=new_session)
                sent_msg = await webhook_retry.send(embed=embed, wait=True)
                self.db.update_setting('last_webhook_message_id', str(sent_msg.id))
                logger.info(f"Sent new webhook msg after failure. ID: {sent_msg.id}")
            except Exception as ex_send: logger.error(f"Webhook retry failed: {ex_send}", exc_info=True); self.db.update_setting('last_webhook_message_id', None)
            finally:
                if new_session and not new_session.closed: await new_session.close()
        except Exception as e: logger.error(f"Unexpected webhook error: {e}", exc_info=True); self.db.update_setting('last_webhook_message_id', None)
        finally:
            if session and not session.closed: await session.close()
            
    async def _update_top1_role(self):
        # (Implementation from previous full code - uses self.leaderboard_data)
        guild = await self._get_guild()
        if not guild : return 
        if not self.leaderboard_data or self.leaderboard_data[0].get("rank") != 1:
            # Logic to remove role if no data or no rank 1 (as before)
            prev_top1_id_str = self.db.get_setting('previous_top1_discord_id')
            role_id_str_remove = self.db.get_setting('top1_role_id')
            if prev_top1_id_str and role_id_str_remove:
                try:
                    role_obj = guild.get_role(int(role_id_str_remove))
                    member_obj = guild.get_member(int(prev_top1_id_str))
                    if member_obj and role_obj and role_obj in member_obj.roles:
                        await member_obj.remove_roles(role_obj, reason="No longer Top 1 / data unavailable")
                        logger.info(f"Removed Top 1 role (no data) from {member_obj.display_name}")
                except Exception as e: logger.error(f"Error removing prev Top 1 role (no data): {e}")
                finally: self.db.update_setting('previous_top1_discord_id', None)
            return

        top1_role_id_str = self.db.get_setting('top1_role_id')
        if not top1_role_id_str: return
        
        try: top1_role = guild.get_role(int(top1_role_id_str))
        except ValueError: logger.error(f"Invalid Top1 role ID: {top1_role_id_str}."); self.db.update_setting('top1_role_id', None); return
        if not top1_role: logger.warning(f"Top1 role ID {top1_role_id_str} not found."); self.db.update_setting('top1_role_id', None); return

        current_top1 = self.leaderboard_data[0]
        current_top1_id = current_top1.get('discord_user_id')
        prev_top1_id = self.db.get_setting('previous_top1_discord_id')
        
        if prev_top1_id and prev_top1_id != current_top1_id:
            try:
                prev_member = guild.get_member(int(prev_top1_id))
                if prev_member and top1_role in prev_member.roles: await prev_member.remove_roles(top1_role, reason="No longer Top 1"); logger.info(f"Removed Top1 role from {prev_member.display_name}")
            except Exception as e: logger.error(f"Failed removing Top1 role from previous: {e}")
        
        if current_top1_id:
            try:
                curr_member = await guild.fetch_member(int(current_top1_id)) 
                if curr_member and top1_role not in curr_member.roles:
                    if guild.me.top_role > top1_role and guild.me.guild_permissions.manage_roles:
                        await curr_member.add_roles(top1_role, reason="Achieved Top 1"); logger.info(f"Assigned Top1 role to {curr_member.display_name}")
                    else: logger.warning(f"Cannot manage Top1 role '{top1_role.name}'. Check hierarchy/perms.")
                self.db.update_setting('previous_top1_discord_id', current_top1_id)
            except (ValueError, nextcord.NotFound): logger.warning(f"Current Top1 user (ID: {current_top1_id}) not found.")
            except Exception as e: logger.error(f"Failed assigning Top1 role: {e}")
        elif prev_top1_id : self.db.update_setting('previous_top1_discord_id', None)

    # --- All Slash Commands from previous full code ---
    @nextcord.slash_command(name="levelboard", description="Manage the leveling leaderboard cog.", guild_ids=[TARGET_GUILD_ID])
    async def levelboard_group(self, interaction: Interaction): pass
    # (set_webhook_url, set_interval, set_top1_role, set_rank_emojis, set_error_channel)
    # (force_update, toggle_updates)
    # (set_source_bot, set_levelup_channel, set_leaderboard_channel)
    # (status)
    # These command definitions were provided in the previous "full code" response and should be copied here.
    # For brevity in this response, I'm not re-listing all of them, but they are essential.
    # I will include the status command as it was updated.
    @levelboard_group.subcommand(name="set_webhook_url", description="Sets the Discord webhook URL for leaderboard posts.")
    @commands.has_permissions(manage_guild=True)
    async def set_webhook_url(self, interaction: Interaction, url: str = SlashOption(description="The full Discord webhook URL", required=True)):
        if not (url.startswith("https://discord.com/api/webhooks/") or url.startswith("https://ptb.discord.com/api/webhooks/") or url.startswith("https://canary.discord.com/api/webhooks/")):
            await interaction.response.send_message("Invalid webhook URL format.", ephemeral=True); return
        session = None
        try:
            session = aiohttp.ClientSession()
            webhook = nextcord.Webhook.from_url(url, session=session)
            await webhook.send("Webhook test from LevelingLeaderboardCog!", username=f"{self.bot.user.name} Webhook Test", avatar_url=self.bot.user.avatar.url if self.bot.user.avatar else None)
            self.db.update_setting('webhook_url', url); self.db.update_setting('last_webhook_message_id', None) 
            await interaction.response.send_message(f"Webhook URL set and tested successfully!", ephemeral=True)
        except Exception as e:
            logger.error(f"Webhook test failed for URL {url}: {e}", exc_info=True)
            self.db.update_setting('webhook_url', url); self.db.update_setting('last_webhook_message_id', None)
            await interaction.response.send_message(f"Webhook URL format okay but test failed: `{e}`. URL saved.", ephemeral=True)
        finally:
            if session: await session.close()

    @levelboard_group.subcommand(name="set_interval", description="Sets the leaderboard update interval.")
    @commands.has_permissions(manage_guild=True)
    async def set_interval(self, interaction: Interaction, minutes: int = SlashOption(description="Update interval in minutes (min 5, max 60)", min_value=5, max_value=60, required=True)):
        self.db.update_setting('update_interval_minutes', minutes)
        if self.update_leaderboard_task.is_running():
             self.update_leaderboard_task.change_interval(minutes=minutes)
        await interaction.response.send_message(f"Leaderboard update interval set to {minutes} minutes.", ephemeral=True)

    @levelboard_group.subcommand(name="set_top1_role", description="Sets the role for the Top 1 user on the leaderboard.")
    @commands.has_permissions(manage_guild=True)
    async def set_top1_role(self, interaction: Interaction, role: Optional[Role] = SlashOption(description="The role to assign. Clears if not provided.", required=False)):
        if not interaction.guild: return await interaction.response.send_message("Command must be used in a server.", ephemeral=True)
        bot_member = interaction.guild.me
        if role:
            if role.position >= bot_member.top_role.position and bot_member.id != interaction.guild.owner_id:
                await interaction.response.send_message(f"I can't manage '{role.name}' (higher/equal to my role).", ephemeral=True); return
            if not bot_member.guild_permissions.manage_roles:
                 await interaction.response.send_message("I lack 'Manage Roles' permission.", ephemeral=True); return
            self.db.update_setting('top1_role_id', str(role.id))
            await interaction.response.send_message(f"Top 1 role set to {role.mention}. Applying on next update.", ephemeral=True)
            # Consider triggering an immediate update cycle if data is available
            if self.leaderboard_data : await self._execute_leaderboard_update_cycle() 
            else: await self._update_top1_role() # Or just try to apply role with current data
        else:
            self.db.update_setting('top1_role_id', None)
            old_top1_id = self.db.get_setting('previous_top1_discord_id')
            if old_top1_id: self.db.update_setting('previous_top1_discord_id', None)
            await interaction.response.send_message("Top 1 role cleared.", ephemeral=True)
            # Attempt to remove from old top 1 if role is cleared
            if old_top1_id : await self._execute_leaderboard_update_cycle()

    @levelboard_group.subcommand(name="set_rank_emojis", description="Configures custom emojis for rank changes.")
    @commands.has_permissions(manage_guild=True)
    async def set_rank_emojis(self, interaction: Interaction,
                              up_emoji: Optional[str] = SlashOption(name="up", description="Emoji for rank up.", required=False),
                              down_emoji: Optional[str] = SlashOption(name="down", description="Emoji for rank down.", required=False),
                              new_emoji: Optional[str] = SlashOption(name="new", description="Emoji for new entry.", required=False),
                              same_emoji: Optional[str] = SlashOption(name="no_change", description="Emoji for same rank. Blank for no emoji.", required=False)):
        changes = []
        if up_emoji is not None: self.db.update_setting('rank_emoji_up', up_emoji); changes.append(f"Up: `{up_emoji}`")
        if down_emoji is not None: self.db.update_setting('rank_emoji_down', down_emoji); changes.append(f"Down: `{down_emoji}`")
        if new_emoji is not None: self.db.update_setting('rank_emoji_new', new_emoji); changes.append(f"New: `{new_emoji}`")
        if same_emoji is not None: self.db.update_setting('rank_emoji_same', same_emoji if same_emoji else None); changes.append(f"No Change: `{same_emoji if same_emoji else '(None)'}`")
        if not changes: await interaction.response.send_message("No emojis provided to update.", ephemeral=True)
        else: await interaction.response.send_message("Rank emojis updated:\n" + "\n".join(changes), ephemeral=True)

    @levelboard_group.subcommand(name="set_error_channel", description="Sets channel for error notifications from this cog.")
    @commands.has_permissions(manage_guild=True)
    async def set_error_channel(self, interaction: Interaction, channel: Optional[TextChannel] = SlashOption(description="Text channel for errors. Clears if not provided.", required=False)):
        if channel:
            if not channel.permissions_for(interaction.guild.me).send_messages:
                await interaction.response.send_message(f"I can't send messages in {channel.mention}.", ephemeral=True); return
            self.db.update_setting('error_notification_channel_id', str(channel.id))
            await interaction.response.send_message(f"Error notification channel set to {channel.mention}.", ephemeral=True)
        else:
            self.db.update_setting('error_notification_channel_id', None)
            await interaction.response.send_message("Error notification channel cleared.", ephemeral=True)
            
    @levelboard_group.subcommand(name="force_update", description="Forces an immediate leaderboard update and post.")
    @commands.has_permissions(manage_guild=True)
    async def force_update(self, interaction: Interaction):
        if not self.update_leaderboard_task.is_running() and self.db.get_setting('updates_enabled', 1) == 0 :
             await interaction.response.send_message("Updates are disabled. Enable with `/levelboard toggle_updates` first.", ephemeral=True); return
        await interaction.response.defer(ephemeral=True)
        logger.info(f"Force update triggered by {interaction.user} (ID: {interaction.user.id})")
        try: 
            await self._execute_leaderboard_update_cycle() # Call the main cycle directly
            await interaction.followup.send("Leaderboard update manually triggered and has run.", ephemeral=True)
        except Exception as e: 
            logger.error(f"Error during force_update: {e}", exc_info=True)
            await interaction.followup.send(f"An error occurred during forced update: {e}", ephemeral=True)

    @levelboard_group.subcommand(name="toggle_updates", description="Enables or disables automatic leaderboard updates.")
    @commands.has_permissions(manage_guild=True)
    async def toggle_updates(self, interaction: Interaction):
        if self.update_leaderboard_task.is_running():
            self.update_leaderboard_task.stop(); self.db.update_setting('updates_enabled', 0)
            await interaction.response.send_message("Automatic leaderboard updates DISABLED.", ephemeral=True)
        else:
            current_interval = self.db.get_setting('update_interval_minutes', 10)
            self.update_leaderboard_task.change_interval(minutes=current_interval)
            self.update_leaderboard_task.start(); self.db.update_setting('updates_enabled', 1)
            await interaction.response.send_message("Automatic leaderboard updates ENABLED.", ephemeral=True)

    @levelboard_group.subcommand(name="set_source_bot", description="Sets the User ID of the source leveling bot.")
    @commands.has_permissions(manage_guild=True)
    async def set_source_bot(self, interaction: Interaction, bot_id: str = SlashOption(description="User ID of the leveling.gg bot", required=True)):
        try: int(bot_id); self.db.update_setting('source_bot_id', bot_id)
        except ValueError: await interaction.response.send_message("Invalid Bot ID. Must be a number.", ephemeral=True); return
        await interaction.response.send_message(f"Source bot ID set to `{bot_id}`.", ephemeral=True)

    @levelboard_group.subcommand(name="set_levelup_channel", description="Sets the channel for level-up messages.")
    @commands.has_permissions(manage_guild=True)
    async def set_levelup_channel(self, interaction: Interaction, channel: Optional[TextChannel] = SlashOption(description="Channel for level-up messages. None to clear.", required=False)):
        if channel: self.db.update_setting('levelup_channel_id', str(channel.id)); await interaction.response.send_message(f"Level-up channel set to {channel.mention}.", ephemeral=True)
        else: self.db.update_setting('levelup_channel_id', None); await interaction.response.send_message("Level-up channel cleared.", ephemeral=True)

    @levelboard_group.subcommand(name="set_leaderboard_channel", description="Sets a primary channel for /leaderboard outputs.")
    @commands.has_permissions(manage_guild=True)
    async def set_leaderboard_channel(self, interaction: Interaction, channel: Optional[TextChannel] = SlashOption(description="Primary channel for /leaderboard. None to clear.", required=False)):
        if channel: self.db.update_setting('leaderboard_channel_id', str(channel.id)); await interaction.response.send_message(f"Primary LB channel set to {channel.mention}.", ephemeral=True)
        else: self.db.update_setting('leaderboard_channel_id', None); await interaction.response.send_message("Primary LB channel cleared.", ephemeral=True)
        
    @levelboard_group.subcommand(name="status", description="Shows current leaderboard cog configuration and status.")
    @commands.has_permissions(manage_guild=True)
    async def status(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        settings = self.db.get_all_settings()
        guild = await self._get_guild() 

        embed = Embed(title="Levelboard Cog Status (V4 - Bot Interaction)", color=Color.og_blurple())
        # ... (rest of status command from previous full code, it was fairly complete) ...
        embed.add_field(name="Target Guild ID", value=f"`{TARGET_GUILD_ID}`", inline=False)
        embed.add_field(name="Webhook URL", value=f"`{settings.get('webhook_url', 'Not Set')}`", inline=False)
        embed.add_field(name="Update Interval", value=f"{settings.get('update_interval_minutes', 10)} minutes", inline=True)
        embed.add_field(name="Automatic Updates", value="Running" if self.update_leaderboard_task.is_running() else "Stopped", inline=True)
        
        top1_role_str = "Not Set"
        if settings.get('top1_role_id') and guild:
            try: role = guild.get_role(int(settings['top1_role_id'])); top1_role_str = role.mention if role else f"ID {settings['top1_role_id']} (Not Found)"
            except: top1_role_str = f"ID {settings['top1_role_id']} (Invalid)"
        elif settings.get('top1_role_id'): top1_role_str = f"ID {settings['top1_role_id']} (Guild N/A)"
        embed.add_field(name="Top 1 Role", value=top1_role_str, inline=False)

        emoji_str = f"Up: `{settings.get('rank_emoji_up', '▲')}`, Down: `{settings.get('rank_emoji_down', '▼')}`, New: `{settings.get('rank_emoji_new', '✦')}`, Same: `{settings.get('rank_emoji_same', '►') or '(None)'}`"
        embed.add_field(name="Rank Emojis", value=emoji_str, inline=False)

        err_ch_str = "Not Set"
        if settings.get('error_notification_channel_id') and guild:
            try: channel = guild.get_channel(int(settings['error_notification_channel_id'])); err_ch_str = channel.mention if channel else f"ID {settings['error_notification_channel_id']} (Not Found)"
            except: err_ch_str = f"ID {settings['error_notification_channel_id']} (Invalid)"
        elif settings.get('error_notification_channel_id'): err_ch_str = f"ID {settings['error_notification_channel_id']} (Guild N/A)"
        embed.add_field(name="Error Channel", value=err_ch_str, inline=False)

        embed.add_field(name="Source Bot ID", value=f"`{settings.get('source_bot_id', 'Not Set')}`", inline=True)
        
        lvl_ch_str = "Not Set"
        if settings.get('levelup_channel_id') and guild:
            try: channel = guild.get_channel(int(settings['levelup_channel_id'])); lvl_ch_str = channel.mention if channel else f"ID {settings['levelup_channel_id']} (Not Found)"
            except: lvl_ch_str = f"ID {settings['levelup_channel_id']} (Invalid)"
        elif settings.get('levelup_channel_id'): lvl_ch_str = f"ID {settings['levelup_channel_id']} (Guild N/A)"
        embed.add_field(name="Level-Up Channel", value=lvl_ch_str, inline=True)

        lb_ch_str = "Not Set (Monitors all if LB embed)"
        if settings.get('leaderboard_channel_id') and guild:
            try: channel = guild.get_channel(int(settings['leaderboard_channel_id'])); lb_ch_str = channel.mention if channel else f"ID {settings['leaderboard_channel_id']} (Not Found)"
            except: lb_ch_str = f"ID {settings['leaderboard_channel_id']} (Invalid)"
        elif settings.get('leaderboard_channel_id'): lb_ch_str = f"ID {settings['leaderboard_channel_id']} (Guild N/A)"
        embed.add_field(name="Primary LB Channel", value=lb_ch_str, inline=True)

        last_update_ts = settings.get('last_data_update_timestamp')
        embed.add_field(name="Last Data Processed", value=f"<t:{last_update_ts}:R>" if last_update_ts else "Never", inline=True)
        
        next_run = "N/A (Task Stopped)"
        if self.update_leaderboard_task.is_running() and self.update_leaderboard_task.next_iteration:
            next_run_ts = int(self.update_leaderboard_task.next_iteration.timestamp())
            next_run = f"<t:{next_run_ts}:R>"
        embed.add_field(name="Next Scheduled Post", value=next_run, inline=True)
        
        embed.add_field(name="Last Webhook Msg ID", value=f"`{settings.get('last_webhook_message_id', 'None')}`", inline=False)
        
        users_in_cache_count = "N/A"
        try: 
            with self.db._get_connection() as conn_temp:
                cursor_temp = conn_temp.cursor()
                cursor_temp.execute("SELECT COUNT(*) FROM leaderboard_data_cache")
                count_row = cursor_temp.fetchone()
                if count_row: users_in_cache_count = str(count_row[0])
        except Exception as e_db: logger.error(f"Failed to get user count for status: {e_db}")
        embed.add_field(name="Users in Local Cache", value=users_in_cache_count, inline=True)
        await interaction.followup.send(embed=embed)

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f'{self.__class__.__name__} cog is ready (V4 - Bot Interaction Mode).')
        # Standard on_ready task check (same as before)
        if self.db.get_setting('updates_enabled', 1) == 1:
            if not self.update_leaderboard_task.is_running():
                logger.info("Restarting update_leaderboard_task (on_ready).")
                current_interval = self.db.get_setting('update_interval_minutes', 10)
                self.update_leaderboard_task.change_interval(minutes=current_interval)
                self.update_leaderboard_task.start()
        else:
            if self.update_leaderboard_task.is_running():
                logger.info("Stopping update_leaderboard_task (on_ready) as it was disabled.")
                self.update_leaderboard_task.stop()

def setup(bot: commands.Bot):
    log_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    if not logger.handlers: 
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(log_formatter)
        logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO) # KEEP DEBUG FOR TROUBLESHOOTING

    # Ensure bot has necessary intents (This should be set when bot is created)
    # e.g. intents = nextcord.Intents.default(); intents.messages = True; intents.guilds = True; intents.message_content = True
    # if not bot.intents.messages:
    #     logger.critical("CRITICAL: 'messages' INTENT IS NOT ENABLED FOR THE BOT!")
    # if not bot.intents.message_content: # Needed for message.mentions if ping is in content
    #     logger.warning("WARNING: 'message_content' INTENT IS NOT ENABLED. User ID from pings in level-up messages might fail.")
            
    bot.add_cog(LevelingLeaderboardCog(bot))
    logger.info("LevelingLeaderboardCog (V4 - Bot Interaction Mode) added to bot.")