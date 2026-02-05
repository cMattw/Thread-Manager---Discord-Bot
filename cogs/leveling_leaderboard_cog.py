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
import re
from typing import Optional, List, Dict, Tuple, Any
import aiohttp

# --- Configuration ---
# !!! SET THESE VALUES FOR YOUR BOT !!!
TARGET_GUILD_ID = 992662612401725502  # YOUR SERVER ID
DEV_DB_PATH = "/home/mattw/Projects/discord_ticket_manager/data/levelboard.db"
PROD_DB_PATH = "/home/container/data/levelboard.db"
# !!! END OF USER CONFIGURATION !!!

# Determine which DB path to use
db_dir = os.path.dirname(PROD_DB_PATH if os.path.exists(os.path.dirname(PROD_DB_PATH)) and os.path.isdir(os.path.dirname(PROD_DB_PATH)) else DEV_DB_PATH)
if db_dir and not os.path.exists(db_dir):
    try:
        os.makedirs(db_dir, exist_ok=True)
        print(f"INFO: Created directory for database: {db_dir}")
    except Exception as e:
        print(f"ERROR: Could not create directory {db_dir}: {e}")
DB_PATH = os.path.join(db_dir, os.path.basename(PROD_DB_PATH if os.path.exists(db_dir) else DEV_DB_PATH))
if db_dir == "":
    DB_PATH = os.path.basename(PROD_DB_PATH if os.path.exists(PROD_DB_PATH) else DEV_DB_PATH)


# --- Logging Setup ---
logger = logging.getLogger('nextcord.leveling_cog_v5_v2fix') 

# --- Regex Patterns (UPDATED FOR COMPONENTS V2) ---
# New Format Match: "#1 — [Lvl. 14] username"
# Handles standard dash (-), em-dash (—), or en-dash (–)
FULL_LEADERBOARD_ENTRY_PATTERN = re.compile(
    r"^#(\d+)\s*[—\-\u2013\u2014]\s*\[Lvl\.\s*(\d+)\]\s+(.+?)\s*$", 
    re.MULTILINE
)

LEVELUP_LEVEL_PATTERN = re.compile(r"Current Level:\s*(\d+)")
LEVELUP_XP_PATTERN = re.compile(r"Current XP:\s*([\d,]+)\s*/\s*([\d,]+)")

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
        self.update_setting('last_data_update_timestamp', timestamp)
        logger.info(f"DB: Updated user {name} (ID: {user_id}) from full LB: L{level}")

    def update_user_from_levelup(self, user_id: str, name: str, new_level: int, xp_span_for_new_level: int):
        timestamp = int(time.time())
        with self._get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO leaderboard_data_cache
                (discord_user_id, display_name, current_level, xp_in_current_level, xp_needed_for_current_level, last_update_timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (user_id, name, new_level, 0, xp_span_for_new_level, timestamp))
            conn.commit()
        self.update_setting('last_data_update_timestamp', timestamp)
        logger.info(f"DB: Updated user {name} (ID: {user_id}) from levelup: New L{new_level}")

    def get_all_leaderboard_users(self) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM leaderboard_data_cache")
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
        self._update_lock = asyncio.Lock()

        current_interval = self.db.get_setting('update_interval_minutes', 10)
        self.update_leaderboard_task.change_interval(minutes=current_interval)
        if self.db.get_setting('updates_enabled', 1) == 1:
            self.update_leaderboard_task.start()
        logger.info(f"LevelingLeaderboardCog (V5 - V2 Format Fix) initialized. DB: {DB_PATH}")

    def cog_unload(self):
        self.update_leaderboard_task.cancel()
        logger.info("LevelingLeaderboardCog unloaded.")

    async def _get_guild(self) -> Optional[nextcord.Guild]:
        guild = self.bot.get_guild(self.target_guild_id_int)
        if not guild: logger.error(f"Target guild {self.target_guild_id_int} not found.")
        return guild

    async def _resolve_user_details(self, guild: nextcord.Guild, user_id_str: Optional[str], username_text: Optional[str]) -> Tuple[Optional[str], str]:
        # Logic to match ID first, then fallback to Name matching (essential for V2 leaderboard)
        member: Optional[Member] = None; final_user_id: Optional[str] = user_id_str
        display_name_to_use: str = username_text or (f"User_{user_id_str}" if user_id_str else "Member Left")
        
        if user_id_str:
            try: member = await guild.fetch_member(int(user_id_str))
            except ValueError: 
                 if username_text: member = guild.get_member_named(username_text)
            except (nextcord.NotFound, Forbidden): pass
        
        # If no ID provided or fetch failed, try resolving by username (V2 Fallback)
        if not member and username_text: 
            member_by_name = guild.get_member_named(username_text)
            # Try basic iteration if exact match fails (case-insensitive fallback)
            if not member_by_name:
                for m in guild.members:
                    if m.name.lower() == username_text.lower() or m.display_name.lower() == username_text.lower():
                        member_by_name = m
                        break
            
            if member_by_name: 
                member = member_by_name
                final_user_id = str(member.id)

        if member: 
            display_name_to_use = member.display_name
            final_user_id = str(member.id)
        
        return final_user_id, display_name_to_use

    async def _process_source_bot_message(self, message: Message):
        guild = message.guild
        if not guild: return
        processed_data_in_this_call = False

        if not message.embeds:
            return

        embed = message.embeds[0]
        
        # 1. Check Title/Author (Legacy check)
        is_lb_title = embed.title and "leaderboard" in embed.title.lower()
        is_lb_author = embed.author and embed.author.name and "leaderboard" in embed.author.name.lower()
        
        # 2. Check Content (V2 Fix: Look for "#1 — [Lvl. X]" pattern directly)
        # We check if there is at least one valid rank entry in the description
        has_lb_content_pattern = False
        if embed.description:
            # We look for at least 1 match to identify it as a leaderboard
            if len(FULL_LEADERBOARD_ENTRY_PATTERN.findall(embed.description)) >= 1:
                has_lb_content_pattern = True

        # 3. Check Channel Restrictions
        lb_channel_id_str = self.db.get_setting('leaderboard_channel_id')
        is_allowed_channel = True
        if lb_channel_id_str:
            try: 
                is_allowed_channel = (message.channel.id == int(lb_channel_id_str))
            except ValueError: 
                pass
        
        # DECISION: Process if it looks like a leaderboard AND is in a valid channel
        process_full_lb = (is_lb_title or is_lb_author or has_lb_content_pattern) and is_allowed_channel

        if process_full_lb:
            logger.info(f"Processor: Parsing msg {message.id} as FULL LEADERBOARD (V2).")
            lines = embed.description.split('\n')
            parsed_count = 0
            
            for line in lines:
                line = line.strip()
                match = FULL_LEADERBOARD_ENTRY_PATTERN.match(line)
                if match:
                    # Format: #1 — [Lvl. 14] username
                    rank_str, lvl_str, username = match.groups()
                    try:
                        rank = int(rank_str)
                        lvl = int(lvl_str)
                        
                        # Resolve User by NAME (since ID is gone in V2)
                        real_uid, real_dname = await self._resolve_user_details(guild, None, username)
                        
                        if real_uid:
                            # HACK: V2 format does not have explicit XP.
                            # We generate a "Sort Weight" so the DB maintains the correct rank order.
                            # Weight = 10,000 - Rank. (Rank 1 = 9999, Rank 2 = 9998)
                            fake_xp_current = 10000 - rank
                            fake_xp_needed = 10000 
                            
                            self.db.update_user_from_full_leaderboard(real_uid, real_dname, lvl, fake_xp_current, fake_xp_needed)
                            parsed_count += 1
                            processed_data_in_this_call = True
                        else:
                            # Optional: Log only if you want to debug missing users
                            # logger.warning(f"Processor: Could not find user '{username}' in guild.")
                            pass
                    except ValueError as ve:
                        logger.warning(f"Processor: Error parsing numbers in line '{line}': {ve}")
            
            if parsed_count > 0:
                logger.info(f"Processor: Updated {parsed_count} users from V2 LB.")
            else:
                logger.warning(f"Processor: Detected LB message but found 0 matches. Regex might need tweaking.")
            
            if processed_data_in_this_call: 
                await self._execute_leaderboard_update_cycle()
                return

        # --- LEVEL-UP PARSING (Legacy Fallback) ---
        if embed.author and embed.author.name:
            lu_channel_id_str = self.db.get_setting('levelup_channel_id')
            in_lu_channel = False
            if lu_channel_id_str:
                try: in_lu_channel = (message.channel.id == int(lu_channel_id_str))
                except ValueError: pass

            if in_lu_channel:
                is_lu_title = embed.title and "leveled up!" in embed.title.lower()
                has_lu_desc_info = False
                if embed.description:
                    has_lu_desc_info = "Current Level:" in embed.description and "Current XP:" in embed.description

                if is_lu_title and has_lu_desc_info:
                    logger.info(f"Processor: Parsing embed from message {message.id} as LEVEL-UP.")
                    final_uid, final_dname = None, "Member Left"
                    title_username = embed.title.split(',')[0].strip() if embed.title else None

                    if message.mentions:
                        pinged = message.mentions[0]
                        if title_username and (pinged.name.lower() in title_username.lower() or pinged.display_name.lower() in title_username.lower()):
                            final_uid, final_dname = str(pinged.id), pinged.display_name
                    
                    if not final_uid and title_username:
                        temp_id, temp_name = await self._resolve_user_details(guild, None, title_username)
                        if temp_id: final_uid, final_dname = temp_id, temp_name
                    
                    if not final_uid: return

                    newLvl, xpSpan = None, None
                    if embed.description:
                        for line in embed.description.split('\n'):
                            lvl_match = LEVELUP_LEVEL_PATTERN.search(line)
                            if lvl_match: newLvl = int(lvl_match.group(1))
                            xp_match = LEVELUP_XP_PATTERN.search(line)
                            if xp_match: xpSpan = int(xp_match.group(2).replace(',', ''))
                    
                    if newLvl is not None and xpSpan is not None:
                        self.db.update_user_from_levelup(final_uid, final_dname, newLvl, xpSpan)
                        await self._execute_leaderboard_update_cycle()

    @commands.Cog.listener()
    async def on_message(self, message: Message):
        if message.guild is None or message.guild.id != self.target_guild_id_int or message.author.bot is False:
            return
        source_bot_id_str = self.db.get_setting('source_bot_id')
        if not source_bot_id_str: return
        try:
            if message.author.id != int(source_bot_id_str): return
        except ValueError: return

        await self._process_source_bot_message(message)

    @commands.Cog.listener()
    async def on_raw_message_edit(self, payload: RawMessageUpdateEvent):
        if payload.guild_id is None or payload.guild_id != self.target_guild_id_int: return
        source_bot_id_str = self.db.get_setting('source_bot_id')
        if not source_bot_id_str: return
        
        channel = self.bot.get_channel(payload.channel_id)
        if not isinstance(channel, TextChannel): return

        try: message = await channel.fetch_message(payload.message_id)
        except: return
            
        if message.author.id == int(source_bot_id_str):
            await self._process_source_bot_message(message)

    async def _execute_leaderboard_update_cycle(self):
        async with self._update_lock:
            guild = await self._get_guild()
            if not guild: return

            all_users_data = self.db.get_all_leaderboard_users()
            if not all_users_data: self.leaderboard_data = []
            else:
                # Sort by Level desc, then XP desc (XP is now a fake weight to preserve Rank)
                sorted_users = sorted(all_users_data, key=lambda u: (u.get('current_level', 0), u.get('xp_in_current_level', 0)), reverse=True)
                processed_lb = []
                for i, ud in enumerate(sorted_users[:10]):
                    rank, uid, db_name = i + 1, ud.get('discord_user_id'), ud.get('display_name', 'Unknown')
                    emb_disp = db_name; txt_name = db_name
                    if uid:
                        member = guild.get_member(int(uid))
                        if member: emb_disp, txt_name = member.mention, member.display_name
                        else: emb_disp = "Member Left"
                    
                    # Special display for V2: XP is hidden/unknown, so we don't show specific numbers
                    # Checks if we have "perfect" XP (10000) which indicates it's the fake weight
                    xp_display_str = "XP Hidden"
                    if ud.get('xp_needed_for_current_level') == 10000:
                         xp_display_str = "XP Hidden"
                    else:
                         # Legacy display for old data or Level-ups that still have XP
                         xp_display_str = f"{ud.get('xp_in_current_level', 0)}/{ud.get('xp_needed_for_current_level', '?')} XP"

                    processed_lb.append({
                        "rank": rank, "discord_user_id": uid, "display_name_text": txt_name, 
                        "display_name_mention": emb_disp, "level": ud.get('current_level', 0),
                        "xp_display": xp_display_str
                    })
                self.previous_leaderboard_data = list(self.leaderboard_data); self.leaderboard_data = processed_lb
            
            embed = await self._create_leaderboard_embed()
            await self._post_or_edit_webhook(embed)
            await self._update_top1_role()

    @tasks.loop(minutes=10)
    async def update_leaderboard_task(self):
        await self._execute_leaderboard_update_cycle()

    @update_leaderboard_task.before_loop
    async def before_update_leaderboard_task(self):
        await self.bot.wait_until_ready()

    async def _create_leaderboard_embed(self) -> Embed:
        guild = await self._get_guild()
        embed_color = guild.me.color if guild and guild.me else Color.blue()
        title = "Level Leaderboard"; embed = Embed(title=title, color=embed_color); description_lines = []
        if not self.leaderboard_data: description_lines.append("Leaderboard data is being collected...")
        else:
            for ud in self.leaderboard_data:
                id_for_rank = ud.get('discord_user_id') or ud.get('display_name_text')
                emoji = await self._get_rank_change_emoji(ud['rank'], id_for_rank, bool(ud.get('discord_user_id')))
                # Show Level, but conditionally show XP if it's not the hidden type
                xp_part = f"`({ud['xp_display']})`" if ud['xp_display'] != "XP Hidden" else ""
                description_lines.append(f"{ud['rank']}. {emoji} {ud['display_name_mention']} - Level {ud['level']} {xp_part}")
        
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
        settings = self.db.get_all_settings()
        default_same = settings.get('rank_emoji_same', '►') if settings.get('rank_emoji_same') else ''
        if not self.previous_leaderboard_data: return settings.get('rank_emoji_new', '✦')
        for prev_user in self.previous_leaderboard_data:
            prev_key = prev_user.get("discord_user_id") if prev_user.get("discord_user_id") else prev_user.get("display_name_text")
            current_key = user_identifier
            if prev_key == current_key:
                prev_rank = prev_user["rank"]
                if current_rank < prev_rank: return settings.get('rank_emoji_up', '▲')
                elif current_rank > prev_rank: return settings.get('rank_emoji_down', '▼')
                else: return default_same
        return settings.get('rank_emoji_new', '✦')

    async def _post_or_edit_webhook(self, embed: Embed):
        webhook_url = self.db.get_setting('webhook_url')
        if not webhook_url: return
        msg_id_str = self.db.get_setting('last_webhook_message_id')
        msg_id = int(msg_id_str) if msg_id_str else None
        session: Optional[aiohttp.ClientSession] = None
        try:
            session = aiohttp.ClientSession()
            webhook = nextcord.Webhook.from_url(webhook_url, session=session)
            if msg_id: await webhook.edit_message(msg_id, embed=embed)
            else: sent_msg = await webhook.send(embed=embed, wait=True); self.db.update_setting('last_webhook_message_id', str(sent_msg.id))
        except (nextcord.NotFound, nextcord.HTTPException, aiohttp.ClientResponseError):
            new_session: Optional[aiohttp.ClientSession] = None
            try:
                if session and not session.closed: await session.close()
                new_session = aiohttp.ClientSession()
                webhook_retry = nextcord.Webhook.from_url(webhook_url, session=new_session)
                sent_msg = await webhook_retry.send(embed=embed, wait=True)
                self.db.update_setting('last_webhook_message_id', str(sent_msg.id))
            except Exception as ex_send: logger.error(f"Webhook retry failed: {ex_send}"); self.db.update_setting('last_webhook_message_id', None)
            finally:
                if new_session and not new_session.closed: await new_session.close()
        except Exception as e: logger.error(f"Unexpected webhook error: {e}"); self.db.update_setting('last_webhook_message_id', None)
        finally:
            if session and not session.closed: await session.close()
            
    async def _update_top1_role(self):
        guild = await self._get_guild()
        if not guild : return 
        if not self.leaderboard_data or self.leaderboard_data[0].get("rank") != 1:
            prev_top1_id_str = self.db.get_setting('previous_top1_discord_id')
            role_id_str_remove = self.db.get_setting('top1_role_id')
            if prev_top1_id_str and role_id_str_remove:
                try:
                    role_obj = guild.get_role(int(role_id_str_remove))
                    member_obj = guild.get_member(int(prev_top1_id_str))
                    if member_obj and role_obj and role_obj in member_obj.roles:
                        await member_obj.remove_roles(role_obj, reason="No longer Top 1")
                except Exception: pass
                finally: self.db.update_setting('previous_top1_discord_id', None)
            return

        top1_role_id_str = self.db.get_setting('top1_role_id')
        if not top1_role_id_str: return
        
        try: top1_role = guild.get_role(int(top1_role_id_str))
        except ValueError: self.db.update_setting('top1_role_id', None); return
        if not top1_role: self.db.update_setting('top1_role_id', None); return

        current_top1 = self.leaderboard_data[0]
        current_top1_id = current_top1.get('discord_user_id')
        prev_top1_id = self.db.get_setting('previous_top1_discord_id')
        
        if prev_top1_id and prev_top1_id != current_top1_id:
            try:
                prev_member = guild.get_member(int(prev_top1_id))
                if prev_member and top1_role in prev_member.roles: await prev_member.remove_roles(top1_role, reason="No longer Top 1")
            except Exception: pass
        
        if current_top1_id:
            try:
                curr_member = await guild.fetch_member(int(current_top1_id)) 
                if curr_member and top1_role not in curr_member.roles:
                    if guild.me.top_role > top1_role and guild.me.guild_permissions.manage_roles:
                        await curr_member.add_roles(top1_role, reason="Achieved Top 1")
                self.db.update_setting('previous_top1_discord_id', current_top1_id)
            except (ValueError, nextcord.NotFound): pass
            except Exception: pass
        elif prev_top1_id : self.db.update_setting('previous_top1_discord_id', None)

    @nextcord.slash_command(name="levelboard", description="Manage the leveling leaderboard cog.", guild_ids=[TARGET_GUILD_ID])
    async def levelboard_group(self, interaction: Interaction): pass

    @levelboard_group.subcommand(name="set_webhook_url", description="Sets the Discord webhook URL for leaderboard posts.")
    @commands.has_permissions(manage_guild=True)
    async def set_webhook_url(self, interaction: Interaction, url: str = SlashOption(description="The full Discord webhook URL", required=True)):
        if not (url.startswith("https://discord.com/api/webhooks/") or url.startswith("https://ptb.discord.com/api/webhooks/") or url.startswith("https://canary.discord.com/api/webhooks/")):
            await interaction.response.send_message("Invalid webhook URL format.", ephemeral=True); return
        self.db.update_setting('webhook_url', url); self.db.update_setting('last_webhook_message_id', None)
        await interaction.response.send_message(f"Webhook URL set!", ephemeral=True)

    @levelboard_group.subcommand(name="set_interval", description="Sets the leaderboard update interval.")
    @commands.has_permissions(manage_guild=True)
    async def set_interval(self, interaction: Interaction, minutes: int = SlashOption(description="Update interval in minutes (min 5, max 60)", min_value=5, max_value=60, required=True)):
        self.db.update_setting('update_interval_minutes', minutes)
        if self.update_leaderboard_task.is_running(): self.update_leaderboard_task.change_interval(minutes=minutes)
        await interaction.response.send_message(f"Leaderboard update interval set to {minutes} minutes.", ephemeral=True)

    @levelboard_group.subcommand(name="set_top1_role", description="Sets the role for the Top 1 user on the leaderboard.")
    @commands.has_permissions(manage_guild=True)
    async def set_top1_role(self, interaction: Interaction, role: Optional[Role] = SlashOption(description="The role to assign. Clears if not provided.", required=False)):
        if role:
            self.db.update_setting('top1_role_id', str(role.id))
            await interaction.response.send_message(f"Top 1 role set to {role.mention}.", ephemeral=True)
            if self.leaderboard_data : await self._execute_leaderboard_update_cycle() 
        else:
            self.db.update_setting('top1_role_id', None)
            await interaction.response.send_message("Top 1 role cleared.", ephemeral=True)

    @levelboard_group.subcommand(name="set_rank_emojis", description="Configures custom emojis for rank changes.")
    @commands.has_permissions(manage_guild=True)
    async def set_rank_emojis(self, interaction: Interaction,
                              up_emoji: Optional[str] = SlashOption(name="up", required=False),
                              down_emoji: Optional[str] = SlashOption(name="down", required=False),
                              new_emoji: Optional[str] = SlashOption(name="new", required=False),
                              same_emoji: Optional[str] = SlashOption(name="no_change", required=False)):
        if up_emoji: self.db.update_setting('rank_emoji_up', up_emoji)
        if down_emoji: self.db.update_setting('rank_emoji_down', down_emoji)
        if new_emoji: self.db.update_setting('rank_emoji_new', new_emoji)
        if same_emoji: self.db.update_setting('rank_emoji_same', same_emoji)
        await interaction.response.send_message("Rank emojis updated.", ephemeral=True)

    @levelboard_group.subcommand(name="set_error_channel", description="Sets channel for error notifications from this cog.")
    @commands.has_permissions(manage_guild=True)
    async def set_error_channel(self, interaction: Interaction, channel: Optional[TextChannel] = SlashOption(description="Text channel for errors. Clears if not provided.", required=False)):
        if channel: self.db.update_setting('error_notification_channel_id', str(channel.id)); await interaction.response.send_message(f"Error channel set to {channel.mention}.", ephemeral=True)
        else: self.db.update_setting('error_notification_channel_id', None); await interaction.response.send_message("Error channel cleared.", ephemeral=True)
            
    @levelboard_group.subcommand(name="force_update", description="Forces an immediate leaderboard update and post.")
    @commands.has_permissions(manage_guild=True)
    async def force_update(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        try: 
            await self._execute_leaderboard_update_cycle()
            await interaction.followup.send("Leaderboard update run.", ephemeral=True)
        except Exception as e: await interaction.followup.send(f"Error: {e}", ephemeral=True)

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
        except ValueError: await interaction.response.send_message("Invalid Bot ID.", ephemeral=True); return
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
        embed = Embed(title="Levelboard Cog Status (V5 - V2 Format Fix)", color=Color.blurple())
        embed.add_field(name="Target Guild ID", value=f"`{TARGET_GUILD_ID}`", inline=False)
        embed.add_field(name="Webhook URL", value=f"`{settings.get('webhook_url', 'Not Set')}`", inline=False)
        embed.add_field(name="Update Interval", value=f"{settings.get('update_interval_minutes', 10)} minutes", inline=True)
        embed.add_field(name="Automatic Updates", value="Running" if self.update_leaderboard_task.is_running() else "Stopped", inline=True)
        await interaction.followup.send(embed=embed)

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f'{self.__class__.__name__} cog is ready (V5 - V2 Format Fix).')
        if self.db.get_setting('updates_enabled', 1) == 1:
            if not self.update_leaderboard_task.is_running():
                current_interval = self.db.get_setting('update_interval_minutes', 10)
                self.update_leaderboard_task.change_interval(minutes=current_interval)
                self.update_leaderboard_task.start()

def setup(bot: commands.Bot):
    log_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    if not logger.handlers: 
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(log_formatter)
        logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)
    bot.add_cog(LevelingLeaderboardCog(bot))
    logger.info("LevelingLeaderboardCog (V5) added to bot.")