import nextcord
from nextcord.ext import commands, tasks, application_checks
from nextcord import Interaction, SlashOption, Thread, TextChannel, ForumChannel, Color, ButtonStyle
from nextcord.ui import View, Button
from db_utils import database
import logging
from datetime import datetime, timedelta, timezone
import pytz
from typing import Optional, List, Dict, Union, Set 
import re 

# Constants
CLOSED_PHRASE = "This ticket has been closed"
DEFAULT_SCAN_INTERVAL_MINUTES = 60
DEFAULT_DELETE_DELAY_DAYS = 7 
MANILA_TZ = pytz.timezone("Asia/Manila")
INACTIVE_DAYS_THRESHOLD = 3
INACTIVE_CHECK_INTERVAL_HOURS = 1

class InactiveTicketView(View):
    def __init__(self):
        super().__init__(timeout=None)  # Persistent view

    @nextcord.ui.button(label="Notify Support", style=ButtonStyle.green, emoji="🔔", custom_id="inactive_notify_support")
    async def notify_support(self, button: Button, interaction: Interaction):
        # Get the thread and settings from the context
        thread = interaction.channel
        if not isinstance(thread, nextcord.Thread):
            await interaction.response.send_message("This button can only be used in a ticket thread.", ephemeral=True)
            return

        # Fetch settings from your database
        settings = database.get_inactive_ticket_settings(thread.parent_id)
        if not settings:
            await interaction.response.send_message("No inactive notification settings found for this channel.", ephemeral=True)
            return

        # Determine the ticket owner (repeat your logic here)
        thread_owner = None
        async for message in thread.history(limit=10, oldest_first=True):
            if message.mentions:
                thread_owner = message.mentions[0]
                break
        if not thread_owner:
            # Fallback: extract from thread name, etc.
            pass  # (repeat your fallback logic here)

        if interaction.user.id != (thread_owner.id if thread_owner else None):
            await interaction.response.send_message("Only the ticket owner can use this button.", ephemeral=True)
            return

        staff_mentions = []
        for role_id in settings.get('staff_roles', []):
            role = interaction.guild.get_role(role_id)
            if role:
                staff_mentions.append(role.mention)
        if not staff_mentions:
            await interaction.response.send_message("No valid staff roles configured for this channel.", ephemeral=True)
            return

        staff_ping = " ".join(staff_mentions)
        custom_message = settings.get('notification_message', "has notified you about this inactive ticket.")
        await interaction.response.send_message(
            f"{staff_ping}, {interaction.user.mention} {custom_message}"
        )
        try:
            await interaction.message.delete()
        except Exception:
            pass

class TicketManagerCog(commands.Cog, name="Ticket Lifecycle Manager"):
    def __init__(self, bot: commands.Bot): 
        self.bot = bot
        self.check_archived_threads_task.start()
        self.check_inactive_tickets_task.start()

    def cog_unload(self):
        self.check_archived_threads_task.cancel()
        self.check_inactive_tickets_task.cancel()

    async def _log_action(self, guild_id: int, action_title: str, 
                          thread_obj: Optional[nextcord.Thread] = None, 
                          details: Optional[str] = None, 
                          error_details_text: Optional[str] = None,
                          color: nextcord.Color = nextcord.Color.orange()):
        settings = database.get_guild_settings(guild_id) 
        log_channel_id = None
        if settings:
            log_channel_id = settings.get('log_channel_id')

        log_channel = None 
        if log_channel_id:
            log_channel = self.bot.get_channel(log_channel_id)
            if not log_channel:
                logging.warning(f"TicketManagerCog: Main log channel ID {log_channel_id} configured but channel not found for guild {guild_id}. Action: {action_title}")
        
        if not log_channel: # Fallback to console if channel not found or not configured
            log_msg_console = f"TicketManagerCog (Guild {guild_id}): Action: {action_title}"
            if thread_obj: log_msg_console += f" | Thread: {thread_obj.name} ({thread_obj.id})"
            if details: log_msg_console += f" | Details: {details}"
            if error_details_text: log_msg_console += f" | Error: {error_details_text}"
            logging.info(log_msg_console)
            return

        try:
            embed = nextcord.Embed(title=f"Ticket Manager: {action_title}", color=color)
            timestamp = datetime.now(MANILA_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')
            embed.add_field(name="Timestamp (GMT+8)", value=timestamp, inline=False)

            if thread_obj:
                embed.add_field(name="Thread", value=f"{thread_obj.name} (ID: `{thread_obj.id}`)", inline=False)
                if thread_obj.parent:
                     embed.add_field(name="In Channel", value=f"{thread_obj.parent.mention}", inline=True)
            if details:
                embed.add_field(name="Details", value=details[:1020], inline=False) 
            if error_details_text:
                embed.add_field(name="Error Info", value=error_details_text[:1020], inline=False)
                embed.color = nextcord.Color.red() 
            
            await log_channel.send(embed=embed)
        except nextcord.Forbidden:
            logging.warning(f"TicketManagerCog: Missing permissions to send log to channel {log_channel.id} in guild {guild_id}")
        except Exception as e:
            logging.error(f"TicketManagerCog: Error sending log: {e}", exc_info=True)

    async def _get_channels_to_scan(self, guild: nextcord.Guild) -> List[Union[TextChannel, ForumChannel]]:
        monitored_channel_ids = database.get_monitored_channels(guild.id)
        channels_to_scan: List[Union[TextChannel, ForumChannel]] = []

        if not monitored_channel_ids: # Check if the list is empty or None
            # If no channels are explicitly monitored, return an empty list.
            # The main task loop will then correctly skip scanning.
            logging.info(f"No monitored channels are configured for guild '{guild.name}'. Thread scanning will be skipped.")
            return channels_to_scan # Returns an empty list

        # If monitored_channel_ids is not empty, proceed to populate the list:
        for chan_id in monitored_channel_ids:
            channel = guild.get_channel(chan_id)
            if channel and isinstance(channel, (TextChannel, ForumChannel)):
                channels_to_scan.append(channel)
            else:
                logging.warning(f"Configured monitored channel ID {chan_id} not found or is not a Text/Forum channel in guild '{guild.name}'.")
        
        # If monitored_channel_ids were provided but all were invalid,
        # channels_to_scan will be empty, and the scan will be skipped, which is correct.
        return channels_to_scan

    async def process_archived_thread(self, thread: nextcord.Thread, guild_id: int, 
                                      delete_delay_config_days: int, guild_settings: dict, 
                                      exempted_ids_set: Set[int], 
                                      is_dry_run: bool = False, 
                                      check_closed_phrase_only: bool = False) -> Optional[Dict]:
        if thread.id in exempted_ids_set:
            if not is_dry_run: 
                if thread.archived:
                    logging.info(f"Exempted thread '{thread.name}' ({thread.id}) is currently archived. Attempting to unarchive to keep active.")
                    try: 
                        await thread.edit(archived=False) 
                        await self._log_action(guild_id, "Exempted Thread Auto-Unarchived", thread_obj=thread, details="Kept active by bot due to exemption setting.", color=Color.teal())
                    except nextcord.Forbidden: 
                        logging.warning(f"Missing 'Manage Threads' permission to unarchive exempted thread '{thread.name}' ({thread.id}).")
                        await self._log_action(guild_id, "Exempted Thread Unarchive FAILED", thread_obj=thread, details="Missing Manage Threads permission.", error_details_text="Forbidden", color=Color.red())
                    except nextcord.HTTPException as e: 
                        logging.error(f"HTTP error unarchiving exempted thread '{thread.name}' ({thread.id}): {e}")
                        await self._log_action(guild_id, "Exempted Thread Unarchive FAILED", thread_obj=thread, details="Discord API error.", error_details_text=str(e), color=Color.red())
                else: 
                    logging.debug(f"Exempted thread '{thread.name}' ({thread.id}) is unarchived. No further auto-management.")
                return None 
            elif is_dry_run and check_closed_phrase_only:
                return {
                    "id": thread.id, "name": thread.name, 
                    "status": f"Exempted (Currently {'Archived' if thread.archived else 'Unarchived'})",
                    "parent_name": thread.parent.name if thread.parent else "Unknown",
                    "parent_id": thread.parent_id
                }
            return None 

        if not thread.archived: return None
        if not is_dry_run: logging.debug(f"Processing non-exempted archived thread: {thread.name} ({thread.id}) for guild {guild_id}")

        message_containing_phrase = None; timestamp_of_phrase = None; found_closed_phrase_in_message_or_embed = False
        try:
            history_limit = 20 if (is_dry_run and check_closed_phrase_only) else 100
            async for msg_obj in thread.history(limit=history_limit, oldest_first=False):
                if msg_obj.content and CLOSED_PHRASE.lower() in msg_obj.content.lower():
                    message_containing_phrase = msg_obj; timestamp_of_phrase = msg_obj.created_at; found_closed_phrase_in_message_or_embed = True
                    if not is_dry_run: logging.debug(f"Found '{CLOSED_PHRASE}' in thread {thread.name} (message content) by {msg_obj.author.name} at {timestamp_of_phrase}")
                    break 
                
                # Use the new comprehensive method instead
                if not found_closed_phrase_in_message_or_embed and await self._has_closed_phrase(msg_obj):
                    message_containing_phrase = msg_obj; timestamp_of_phrase = msg_obj.created_at; found_closed_phrase_in_message_or_embed = True
                    if not is_dry_run: logging.debug(f"Found '{CLOSED_PHRASE}' in thread {thread.name} (embed or content) by {msg_obj.author.name} at {timestamp_of_phrase}")
                    break
        except nextcord.HTTPException as e:
            logging.error(f"Error fetching history for thread {thread.name} ({thread.id}): {e}")
            if not is_dry_run and guild_settings.get('log_channel_id'): await self._log_action(guild_id, "Thread Processing Error", thread_obj=thread, details="Failed to fetch message history.", error_details_text=str(e), color=Color.red())
            if is_dry_run: return {"id": thread.id, "name": thread.name, "error": "Failed to fetch history"}
            return None
        
        # Always return status and deletion info for closed threads, not just in dry run
        if found_closed_phrase_in_message_or_embed and timestamp_of_phrase:
            if timestamp_of_phrase.tzinfo is None:
                timestamp_of_phrase = timestamp_of_phrase.replace(tzinfo=timezone.utc)
            delete_after_timestamp = timestamp_of_phrase + timedelta(days=delete_delay_config_days)
            # Persist status and deletion info to the database
            database.set_thread_data(thread.id, "status", "Archived (Closed)")
            database.set_thread_data(thread.id, "closed_at", timestamp_of_phrase.isoformat())
            database.set_thread_data(thread.id, "delete_due_at", delete_after_timestamp.isoformat())
            # Always return info for closed threads, so they can be scheduled for deletion
            if is_dry_run:
                return {
                    "id": thread.id,
                    "name": thread.name,
                    "parent_name": thread.parent.name if thread.parent else "Unknown",
                    "parent_id": thread.parent_id,
                    "status": "Archived (Closed)",
                    "closed_at": timestamp_of_phrase,
                    "delete_due_at": delete_after_timestamp,
                    "channel_id": thread.parent_id,
                    "channel_name": thread.parent.name if thread.parent else "Unknown"
                }
            if datetime.now(timezone.utc) > delete_after_timestamp:
                try:
                    logging.info(f"Deleting thread {thread.name} ({thread.id}) as it was closed and {delete_delay_config_days} day(s) delay period passed.")
                    await thread.delete()
                    if guild_settings.get('log_channel_id'):
                        await self._log_action(guild_id, "Thread Deleted", thread_obj=thread, details=f"Ticket closed on {timestamp_of_phrase.astimezone(MANILA_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')} and {delete_delay_config_days} day(s) deletion delay passed.", color=Color.dark_red())
                except nextcord.Forbidden:
                    logging.warning(f"Missing permissions to delete thread {thread.name} ({thread.id}).")
                    if guild_settings.get('log_channel_id'):
                        await self._log_action(guild_id, "Thread Deletion FAILED", thread_obj=thread, details="Missing Manage Threads permission.", error_details_text="Forbidden", color=Color.red())
                except nextcord.HTTPException as e:
                    logging.error(f"HTTP error deleting thread {thread.name} ({thread.id}): {e}")
                    if guild_settings.get('log_channel_id'):
                        await self._log_action(guild_id, "Thread Deletion FAILED", thread_obj=thread, details="Discord API error.", error_details_text=str(e), color=Color.red())
        elif is_dry_run and check_closed_phrase_only:
            # In dry run, if not closed, mark as inactive
            return {
                "id": thread.id,
                "name": thread.name,
                "parent_name": thread.parent.name if thread.parent else "Unknown",
                "parent_id": thread.parent_id,
                "status": "Archived (Inactive)"
            }
        elif not is_dry_run:
            try:
                logging.info(f"Unarchiving non-exempted thread {thread.name} ({thread.id}) due to inactivity (no closure message found).")
                await thread.edit(archived=False)
                if guild_settings.get('log_channel_id'):
                    await self._log_action(guild_id, "Thread Auto-Unarchived", thread_obj=thread, details="Thread auto-archived by Discord, unarchiving to keep active (non-exempted).", color=Color.gold())
            except nextcord.Forbidden:
                logging.warning(f"Missing permissions to unarchive non-exempted thread {thread.name} ({thread.id}).")
                if guild_settings.get('log_channel_id'):
                    await self._log_action(guild_id, "Thread Unarchive FAILED", thread_obj=thread, details="Missing Manage Threads permission for non-exempted thread.", error_details_text="Forbidden", color=Color.red())
            except nextcord.HTTPException as e:
                logging.error(f"HTTP error unarchiving non-exempted thread {thread.name} ({thread.id}): {e}")
                if guild_settings.get('log_channel_id'):
                    await self._log_action(guild_id, "Thread Unarchive FAILED", thread_obj=thread, details="Discord API error for non-exempted thread.", error_details_text=str(e), color=Color.red())
        return None

    @tasks.loop(minutes=DEFAULT_SCAN_INTERVAL_MINUTES)
    async def check_archived_threads_task(self):
        await self.bot.wait_until_ready()
        if not self.bot.target_guild_id: 
            logging.error("Target guild ID not set. Halting check_archived_threads_task.")
            self.check_archived_threads_task.stop() 
            return
        
        logging.info(f"Starting periodic check for archived threads on target guild {getattr(self.bot, 'target_guild_name', self.bot.target_guild_id)}...")
        
        guild = self.bot.get_guild(self.bot.target_guild_id)
        if not guild:
            logging.error(f"Target guild {self.bot.target_guild_id} not found by bot. Skipping scan.")
            return

        current_guild_settings = database.get_guild_settings(guild.id)
        if not current_guild_settings:
            logging.info(f"No settings for target guild {guild.name}. Using defaults for scan. Please configure via /config.")
            current_guild_settings = {} 

        new_interval = current_guild_settings.get('scan_interval_minutes', DEFAULT_SCAN_INTERVAL_MINUTES)
        if self.check_archived_threads_task.minutes != new_interval:
            try:
                self.check_archived_threads_task.change_interval(minutes=new_interval)
                logging.info(f"Scan interval for target guild updated to {new_interval} minutes from DB.")
            except Exception as e:
                logging.error(f"Failed to change interval: {e}. Keeping {self.check_archived_threads_task.minutes} min.")

        exempted_thread_ids = database.get_exempted_thread_ids_for_guild(guild.id)
        logging.debug(f"[MAIN_TASK_SCAN] Guild '{guild.name}': Found {len(exempted_thread_ids)} exempted thread(s).")
        delete_delay_val_days = current_guild_settings.get('delete_delay_days', DEFAULT_DELETE_DELAY_DAYS)
        channels_to_scan = await self._get_channels_to_scan(guild)
        
        logging.info(f"[MAIN_TASK_SCAN] Guild '{guild.name}': Channels to scan: {[f'{ch.name} ({type(ch).__name__})' for ch in channels_to_scan]}")
        if not channels_to_scan:
            logging.info(f"No channels to scan in target guild {guild.name}.")
            if current_guild_settings.get('log_channel_id'):
                 await self._log_action(guild.id, "Scan Info", details="No channels configured or accessible for scanning.")
            logging.info(f"Finished scanning target guild: {guild.name} ({guild.id}).")
            logging.info("Finished periodic check for archived threads (single server mode).")
            return
        
        logging.info(f"Scanning target guild: {guild.name} ({guild.id}) in {len(channels_to_scan)} container(s) with delete delay of {delete_delay_val_days} days.")
        for channel_obj in channels_to_scan:
            logging.info(f"[MAIN_TASK_SCAN] Guild '{guild.name}': Now scanning {type(channel_obj).__name__} '{channel_obj.name}' (ID: {channel_obj.id})")
            threads_found_in_this_container_count = 0
            try:
                processed_thread_ids_in_task = set()
                iterators_to_check_task = []
                if isinstance(channel_obj, TextChannel):
                    iterators_to_check_task.append(channel_obj.archived_threads(private=False, limit=None))
                    iterators_to_check_task.append(channel_obj.archived_threads(private=True, joined=True, limit=None))
                elif isinstance(channel_obj, ForumChannel): 
                    iterators_to_check_task.append(channel_obj.archived_threads(limit=None))
                
                for iterator in iterators_to_check_task:
                    if iterator is None: continue
                    async for thread_item in iterator:
                        threads_found_in_this_container_count +=1
                        if thread_item.id not in processed_thread_ids_in_task:
                            await self.process_archived_thread(thread_item, guild.id, delete_delay_val_days, current_guild_settings, exempted_thread_ids, is_dry_run=False)
                            processed_thread_ids_in_task.add(thread_item.id)
                if threads_found_in_this_container_count == 0:
                    logging.info(f"[MAIN_TASK_SCAN] Guild '{guild.name}', Container '{channel_obj.name}': No archived threads yielded by iterators.")
            except nextcord.Forbidden: 
                logging.warning(f"Missing permissions to fetch archived threads in {type(channel_obj).__name__} {channel_obj.name} ({channel_obj.id}) in guild {guild.name}.")
                if current_guild_settings.get('log_channel_id'): await self._log_action(guild.id, "Scan Error", details=f"Missing permissions for {channel_obj.mention}.", error_details_text="Forbidden to fetch archived threads.")
            except Exception as e: 
                logging.error(f"Error processing {type(channel_obj).__name__} {channel_obj.name} ({channel_obj.id}) in task: {e}", exc_info=True)
                if current_guild_settings.get('log_channel_id'): await self._log_action(guild.id, "Scan Error", details=f"Error during scan of {channel_obj.mention}.", error_details_text=str(e))
        logging.info(f"Finished scanning target guild: {guild.name} ({guild.id}).")
        logging.info("Finished periodic check for archived threads (single server mode).")

    @check_archived_threads_task.before_loop
    async def before_check_archived_threads_task(self):
        logging.info("TicketManagerCog: Waiting for bot to be fully ready before starting check_archived_threads_task loop...")
        await self.bot.wait_until_ready()
        # The task's main loop will perform the critical check for self.bot.target_guild_id.
        # database.initialize_database() is already called in main.py's on_ready.
        logging.info("TicketManagerCog: Bot is ready. Task loop for check_archived_threads_task will now begin its iterations.")

    async def _get_inactive_ticket_settings(self, channel_id: int) -> Optional[Dict]:
        """Get inactive ticket notification settings for a specific channel"""
        return database.get_inactive_ticket_settings(channel_id)

    async def _check_thread_inactivity(self, thread: Thread, guild_id: int) -> bool:
        """Check if a thread has been inactive for the threshold period"""
        try:
            # Get inactive ticket settings to determine staff roles
            settings = await self._get_inactive_ticket_settings(thread.parent_id)
            staff_role_ids = settings.get('staff_roles', []) if settings else []
            
            # Get the last message from support staff in the thread
            last_support_message_time = None
            async for message in thread.history(limit=50):  # Check more messages to find support activity
                # Skip messages from the ticket owner
                thread_owner_id = await self._get_thread_owner_id(thread)
                if message.author.id == thread_owner_id:
                    continue
                
                # Check if message author has any of the staff roles
                if hasattr(message.author, 'roles'):
                    author_role_ids = [role.id for role in message.author.roles]
                    if any(role_id in staff_role_ids for role_id in author_role_ids):
                        last_support_message_time = message.created_at
                        break
                
                # Also consider bot messages as support activity (for ticket bots)
                if message.author.bot:
                    last_support_message_time = message.created_at
                    break
            
            # If no support message found, use thread creation time
            if not last_support_message_time:
                last_support_message_time = thread.created_at
            
            if last_support_message_time.tzinfo is None:
                last_support_message_time = last_support_message_time.replace(tzinfo=timezone.utc)
            
            time_since_last_support = datetime.now(timezone.utc) - last_support_message_time
            return time_since_last_support.days >= INACTIVE_DAYS_THRESHOLD
            
        except Exception as e:
            logging.error(f"Error checking thread inactivity for {thread.name}: {e}")
            return False

    async def _get_thread_owner_id(self, thread: Thread) -> Optional[int]:
        """Get the thread owner's user ID"""
        try:
            async for message in thread.history(limit=10, oldest_first=True):
                if message.mentions:
                    return message.mentions[0].id
                # Also check message content for user ID patterns if mentions are empty
                if message.content:
                    import re
                    user_mention_pattern = r'<@!?(\d+)>'
                    match = re.search(user_mention_pattern, message.content)
                    if match:
                        return int(match.group(1))
            
            # Try to extract username from thread name
            import re
            thread_name = thread.name
            match = re.search(r'-([A-Za-z0-9_]+)$', thread_name)
            if match:
                possible_username = match.group(1)
                for member in thread.guild.members:
                    if member.name.lower() == possible_username.lower() or (member.nick and member.nick.lower() == possible_username.lower()):
                        return member.id
            
            return None
        except Exception as e:
            logging.error(f"Error getting thread owner ID for {thread.name}: {e}")
            return None

    async def _send_inactive_notification(self, thread: Thread, guild_id: int):
        """Send inactive ticket notification to thread"""
        try:
            # Get inactive ticket settings for this channel
            settings = await self._get_inactive_ticket_settings(thread.parent_id)
            if not settings or not settings.get('enabled', False):
                return
            
            # Check if we already sent a notification recently
            notification_key = f"inactive_notif_{thread.id}"
            last_notification = database.get_thread_data(thread.id, notification_key)
            if last_notification:
                last_notif_time = datetime.fromisoformat(last_notification)
                if (datetime.now(timezone.utc) - last_notif_time).days < 1:
                    return  # Already notified within last day
            
            # Get thread owner
            thread_owner_id = await self._get_thread_owner_id(thread)
            if not thread_owner_id:
                logging.warning(f"Could not determine ticket owner for thread {thread.name} ({thread.id})")
                return
            
            thread_owner = thread.guild.get_member(thread_owner_id)
            if not thread_owner:
                logging.warning(f"Thread owner {thread_owner_id} not found in guild for thread {thread.name}")
                return
            
            # Delete previous inactive notification if it exists
            try:
                async for message in thread.history(limit=20):
                    if (message.author == self.bot.user and 
                        message.embeds and 
                        len(message.embeds) > 0 and
                        message.embeds[0].title == "Inactive Ticket Notification"):
                        await message.delete()
                        break
            except Exception as e:
                logging.debug(f"Could not delete previous inactive notification: {e}")
            
            # Create embed
            embed = nextcord.Embed(
                title="Inactive Ticket Notification",
                description=f"{thread_owner.mention}, this ticket has been inactive for {INACTIVE_DAYS_THRESHOLD} days and support has not yet responded. Would you like to notify them?",
                color=nextcord.Color.orange()
            )
            embed.set_footer(text="Click the button below to ping support staff")
            
            # Create view with notification button
            view = InactiveTicketView()
            
            # Send the notification
            await thread.send(content=thread_owner.mention, embed=embed, view=view)
            
            # Record that we sent a notification
            database.set_thread_data(thread.id, notification_key, datetime.now(timezone.utc).isoformat())
            
            # Log the action
            await self._log_action(
                guild_id, 
                "Inactive Ticket Notification Sent", 
                thread_obj=thread,
                details=f"Notified {thread_owner.mention} about {INACTIVE_DAYS_THRESHOLD}-day inactivity",
                color=Color.orange()
            )
            
        except Exception as e:
            logging.error(f"Error sending inactive notification for thread {thread.name}: {e}")
            
    @tasks.loop(hours=INACTIVE_CHECK_INTERVAL_HOURS)
    async def check_inactive_tickets_task(self):
        """Check for inactive open tickets and send notifications"""
        await self.bot.wait_until_ready()
        
        if not self.bot.target_guild_id:
            return
        
        guild = self.bot.get_guild(self.bot.target_guild_id)
        if not guild:
            return
        
        logging.info(f"Checking for inactive tickets in guild: {guild.name}")
        
        channels_to_scan = await self._get_channels_to_scan(guild)
        
        for channel_obj in channels_to_scan:
            try:
                # Check active (non-archived) threads only
                active_threads = []
                
                if isinstance(channel_obj, TextChannel):
                    active_threads.extend(channel_obj.threads)
                elif isinstance(channel_obj, ForumChannel):
                    active_threads.extend(channel_obj.threads)
                
                for thread in active_threads:
                    if thread.archived:
                        continue
                    
                    # Skip if thread has closed phrase
                    has_closed_phrase = False
                    try:
                        async for msg in thread.history(limit=20):
                            if CLOSED_PHRASE.lower() in msg.content.lower():
                                has_closed_phrase = True
                                break
                            # Also check embeds
                            for embed in msg.embeds:
                                embed_texts = [embed.title, embed.description] + [f.value for f in embed.fields]
                                if any(CLOSED_PHRASE.lower() in str(text).lower() for text in embed_texts if text):
                                    has_closed_phrase = True
                                    break
                            if has_closed_phrase:
                                break
                    except:
                        continue
                    
                    if has_closed_phrase:
                        continue
                    
                    # Check if thread is inactive
                    if await self._check_thread_inactivity(thread, guild.id):
                        await self._send_inactive_notification(thread, guild.id)
                        
            except Exception as e:
                logging.error(f"Error checking inactive tickets in {channel_obj.name}: {e}")

    async def cog_check(self, interaction: Interaction) -> bool:
        if not self.bot.target_guild_id:
            if not interaction.response.is_done(): 
                try: await interaction.response.defer(ephemeral=True)
                except nextcord.NotFound: pass
            await interaction.followup.send("Bot is not yet ready or target server not identified.", ephemeral=True); return False
        if interaction.guild is None or interaction.guild.id != self.bot.target_guild_id:
            if not interaction.response.is_done(): 
                try: await interaction.response.defer(ephemeral=True)
                except nextcord.NotFound: pass
            target_guild_name = getattr(self.bot, 'target_guild_name', 'the configured server')
            await interaction.followup.send(f"This bot is configured for a specific server. Please use commands in '{target_guild_name}'.", ephemeral=True); return False
        return True

    @nextcord.slash_command(name="configure_inactive_notifications", description="Configure inactive ticket notifications for a channel")
    @application_checks.has_permissions(manage_guild=True)
    async def configure_inactive_notifications(
        self, 
        interaction: Interaction,
        channel: nextcord.abc.GuildChannel = SlashOption(description="Channel to configure"),
        enabled: bool = SlashOption(description="Enable inactive notifications"),
        staff_roles: str = SlashOption(description="Comma-separated role IDs to ping", required=False),
        notification_message: str = SlashOption(description="Custom message when notifying staff", required=False)
    ):
        await interaction.response.defer(ephemeral=True)
        
        if not isinstance(channel, (TextChannel, ForumChannel)):
            await interaction.followup.send("Channel must be a text channel or forum channel.", ephemeral=True)
            return
        
        # Parse staff roles
        staff_role_ids = []
        if staff_roles:
            try:
                staff_role_ids = [int(role_id.strip()) for role_id in staff_roles.split(',')]
                # Validate roles exist
                valid_roles = []
                for role_id in staff_role_ids:
                    role = interaction.guild.get_role(role_id)
                    if role:
                        valid_roles.append(role_id)
                staff_role_ids = valid_roles
            except ValueError:
                await interaction.followup.send("Invalid role IDs provided. Use comma-separated numbers.", ephemeral=True)
                return
        
        # Save settings
        settings = {
            'enabled': enabled,
            'staff_roles': staff_role_ids,
            'notification_message': notification_message or "has notified you about this inactive ticket."
        }
        
        database.set_inactive_ticket_settings(channel.id, settings)
        
        role_mentions = ", ".join([f"<@&{role_id}>" for role_id in staff_role_ids]) if staff_role_ids else "None"
        
        embed = nextcord.Embed(
            title="Inactive Ticket Notifications Configured",
            color=nextcord.Color.green() if enabled else nextcord.Color.red()
        )
        embed.add_field(name="Channel", value=channel.mention, inline=False)
        embed.add_field(name="Enabled", value="✅ Yes" if enabled else "❌ No", inline=True)
        embed.add_field(name="Staff Roles", value=role_mentions, inline=False)
        embed.add_field(name="Notification Message", value=settings['notification_message'], inline=False)
        
        await interaction.followup.send(embed=embed, ephemeral=True)

    @nextcord.slash_command(name="view_inactive_settings", description="View inactive ticket notification settings")
    @application_checks.has_permissions(manage_guild=True)
    async def view_inactive_settings(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        
        channels_to_scan = await self._get_channels_to_scan(interaction.guild)
        
        embed = nextcord.Embed(
            title="Inactive Ticket Notification Settings",
            description="Current configuration for monitored channels",
            color=nextcord.Color.blue()
        )
        
        for channel in channels_to_scan:
            settings = await self._get_inactive_ticket_settings(channel.id)
            
            if settings and settings.get('enabled'):
                staff_roles = settings.get('staff_roles', [])
                role_mentions = ", ".join([f"<@&{role_id}>" for role_id in staff_roles]) if staff_roles else "None"
                
                field_value = f"**Enabled:** ✅ Yes\n**Staff Roles:** {role_mentions}\n**Message:** {settings.get('notification_message', 'Default')}"
            else:
                field_value = "**Enabled:** ❌ No"
            
            embed.add_field(
                name=f"#{channel.name}",
                value=field_value,
                inline=False
            )
        
        if not embed.fields:
            embed.description = "No monitored channels configured or no settings found."
        
        await interaction.followup.send(embed=embed, ephemeral=True)

    @nextcord.slash_command(name="view_pending_deletions", description="Lists threads marked closed and scheduled for deletion.")
    @application_checks.has_permissions(manage_guild=True)
    async def view_pending_deletions(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        guild = self.bot.get_guild(self.bot.target_guild_id)
        if not guild:
            await interaction.followup.send("Target server not found by bot.", ephemeral=True)
            return
        current_guild_settings = database.get_guild_settings(self.bot.target_guild_id)
        if not current_guild_settings:
            await interaction.followup.send("Settings not configured.", ephemeral=True)
            return
        delete_delay_val_days = current_guild_settings.get('delete_delay_days', DEFAULT_DELETE_DELAY_DAYS)
        exempted_thread_ids = database.get_exempted_thread_ids_for_guild(self.bot.target_guild_id)
        channels_to_scan = await self._get_channels_to_scan(guild)
        if not channels_to_scan:
            await interaction.followup.send("No channels to scan.", ephemeral=True)
            return

        closed_threads: List[Dict] = []
        errors_encountered: List[str] = []
        processed_thread_ids_for_view = set()
        now = datetime.now(timezone.utc)
        for channel_obj in channels_to_scan:
            try:
                iterators_to_check = []
                if isinstance(channel_obj, TextChannel):
                    iterators_to_check.append(channel_obj.archived_threads(private=False, limit=None))
                    iterators_to_check.append(channel_obj.archived_threads(private=True, joined=True, limit=None))
                elif isinstance(channel_obj, ForumChannel):
                    iterators_to_check.append(channel_obj.archived_threads(limit=None))
                for iterator in filter(None, iterators_to_check):
                    async for thread_item in iterator:
                        if thread_item.id in processed_thread_ids_for_view:
                            continue
                        result = await self.process_archived_thread(
                            thread_item,
                            self.bot.target_guild_id,
                            delete_delay_val_days,
                            current_guild_settings,
                            exempted_thread_ids,
                            is_dry_run=True,
                            check_closed_phrase_only=False
                        )
                        if result and "status" in result and result["status"] == "Archived (Closed)":
                            closed_threads.append(result)
                        elif result and "error" in result:
                            errors_encountered.append(f"T#{thread_item.id}: {result['error']}")
                        processed_thread_ids_for_view.add(thread_item.id)
            except nextcord.Forbidden:
                errors_encountered.append(f"NoPerms: {channel_obj.mention}")
            except Exception as e:
                errors_encountered.append(f"ErrScan: {channel_obj.mention}: {str(e)[:50]}")
                logging.error(f"[VIEW_PENDING] Error scanning {type(channel_obj).__name__} {channel_obj.name}: {e}", exc_info=True)

        if not closed_threads and not errors_encountered:
            await interaction.followup.send("No closed threads are currently scheduled for deletion (or they are exempted).", ephemeral=True)
            return

        embed = nextcord.Embed(
            title="Threads Scheduled for Deletion",
            description=f"All non-exempted threads marked closed and scheduled for deletion after {delete_delay_val_days}-day delay.",
            color=nextcord.Color.orange()
        )

        if closed_threads:
            overdue_lines = ""
            pending_lines = ""
            for thread_info in closed_threads:
                closed_ts = int(thread_info['closed_at'].timestamp())
                due_ts = int(thread_info['delete_due_at'].timestamp())
                line = f"- **{thread_info.get('name', 'N/A')}** (ID: `{thread_info.get('id', 'N/A')}`)\n  In: <#{thread_info.get('channel_id', 'N/A')}> | Closed: <t:{closed_ts}:R> | Due: <t:{due_ts}:R>"
                if now > thread_info['delete_due_at']:
                    overdue_lines += line + "\n"
                else:
                    pending_lines += line + "\n"
            if overdue_lines:
                embed.add_field(name="Overdue for Deletion", value=overdue_lines[:1020], inline=False)
            if pending_lines:
                embed.add_field(name="Pending Deletion", value=pending_lines[:1020], inline=False)
        else:
            embed.add_field(name="Pending Threads", value="None meeting criteria (or all eligible are exempted).", inline=False)

        if errors_encountered:
            error_output = "\n".join(errors_encountered)
            embed.add_field(name="⚠️ Errors During Scan", value=f"```{error_output[:1020]}```", inline=False)
            embed.color = nextcord.Color.red()
        embed.set_footer(text="This is a preview. Deletion by periodic scan.")
        try:
            await interaction.followup.send(embed=embed, ephemeral=True)
        except nextcord.HTTPException as e:
            await interaction.followup.send(f"Found {len(closed_threads)} threads, but list too long. Errors: {len(errors_encountered)}", ephemeral=True)
            logging.error(f"Error sending /view_pending_deletions embed: {e}")

    def _humanize_timedelta(self, delta: timedelta) -> str:
        if delta.total_seconds() <= 0: return "now or overdue"
        days = delta.days; hours, remainder = divmod(delta.seconds, 3600); minutes, seconds = divmod(remainder, 60)
        parts = []
        if days > 0: parts.append(f"{days}d")
        if hours > 0: parts.append(f"{hours}h")
        if minutes > 0: parts.append(f"{minutes}m")
        if not parts: return f"{seconds}s" if seconds > 0 else "imminently"
        return ", ".join(parts)

    @nextcord.slash_command(name="view_scanned_threads", description="Shows all detected archived threads in monitored channels.")
    @application_checks.has_permissions(manage_guild=True)
    async def view_scanned_threads(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        guild = self.bot.get_guild(self.bot.target_guild_id)
        if not guild:
            await interaction.followup.send("Target server not found by bot.", ephemeral=True)
            return

        current_guild_settings = database.get_guild_settings(self.bot.target_guild_id)
        if not current_guild_settings: # Technically, only delete_delay_days is strictly needed by process_archived_thread for this command
            await interaction.followup.send("Settings not fully configured (e.g., delete delay missing).", ephemeral=True)
            return
        
        delete_delay_val_days = current_guild_settings.get('delete_delay_days', DEFAULT_DELETE_DELAY_DAYS)
        exempted_thread_ids = database.get_exempted_thread_ids_for_guild(self.bot.target_guild_id)
        channels_to_scan = await self._get_channels_to_scan(guild)

        if not channels_to_scan:
            await interaction.followup.send("No channels to scan (either not configured or none accessible).", ephemeral=True)
            return

        logging.info(f"[VIEW_SCANNED] User {interaction.user} (ID: {interaction.user.id}) initiated scan in guild '{guild.name}' (ID: {guild.id})")
        logging.info(f"[VIEW_SCANNED] Channels to scan in '{guild.name}': {[f'{ch.name} ({type(ch).__name__})' for ch in channels_to_scan]}")
        
        detected_threads_info: List[Dict] = []
        errors_encountered: List[str] = []
        scanned_thread_ids_this_command = set() 

        for channel_obj in channels_to_scan:
            logging.info(f"[VIEW_SCANNED] Scanning {type(channel_obj).__name__}: '{channel_obj.name}' (ID: {channel_obj.id})")
            threads_found_and_processed_in_channel = 0
            try:
                iterators_to_check = []
                if isinstance(channel_obj, TextChannel):
                    iterators_to_check.append({"type": "public", "iter": channel_obj.archived_threads(private=False, limit=None)})
                    iterators_to_check.append({"type": "private", "iter": channel_obj.archived_threads(private=True, joined=True, limit=None)})
                elif isinstance(channel_obj, ForumChannel):
                    iterators_to_check.append({"type": "forum_archived", "iter": channel_obj.archived_threads(limit=None)})

                for iter_info in iterators_to_check:
                    if iter_info["iter"] is None: continue
                    async for thread_item in iter_info["iter"]:
                        logging.debug(f"[VIEW_SCANNED] Found {iter_info['type']} thread: '{thread_item.name}' (ID: {thread_item.id}, Archived: {thread_item.archived}) in '{channel_obj.name}'")
                        if thread_item.id in scanned_thread_ids_this_command: 
                            logging.debug(f"[VIEW_SCANNED] Thread '{thread_item.name}' ID {thread_item.id} already listed, skipping.")
                            continue
                        # Ensure guild_id is correctly passed (it's self.bot.target_guild_id or interaction.guild.id)
                        # Try to load persisted status info first
                        # Always scan and update the database for every thread
                        result = await self.process_archived_thread(
                            thread_item, 
                            guild.id, # Use guild.id from the fetched guild object
                            delete_delay_val_days, 
                            current_guild_settings, 
                            exempted_thread_ids, 
                            is_dry_run=True, 
                            check_closed_phrase_only=True
                        )
                        if result:
                            detected_threads_info.append(result)
                            threads_found_and_processed_in_channel += 1
                        scanned_thread_ids_this_command.add(thread_item.id)
                
                if threads_found_and_processed_in_channel == 0:
                    logging.info(f"[VIEW_SCANNED] No new archived threads were processed from {type(channel_obj).__name__} '{channel_obj.name}' by the iterators in this command scan.")
            except nextcord.Forbidden: 
                logging.warning(f"[VIEW_SCANNED] Missing permissions for {type(channel_obj).__name__} '{channel_obj.name}'.")
                errors_encountered.append(f"Missing permissions for {channel_obj.mention}")
            except Exception as e:
                logging.error(f"[VIEW_SCANNED] Error during scan of {type(channel_obj).__name__} '{channel_obj.name}': {e}", exc_info=True)
                errors_encountered.append(f"Error scanning {channel_obj.mention}: {str(e)[:50]}")
        
        logging.info(f"[VIEW_SCANNED] Finished iteration for guild '{guild.name}'. Total threads added to info list for embed: {len(detected_threads_info)}. Total errors encountered: {len(errors_encountered)}.")

        embeds_to_send = []
        current_page_num = 1
        # Max 3-4 "content" fields per embed to leave space for title, description, footer, and potential error field
        # Discord's limit for field value characters is 1024. Total embed chars ~6000.
        MAX_CONTENT_FIELDS_PER_EMBED = 3 
        MAX_CHARS_PER_FIELD_VALUE = 1024 

        if not detected_threads_info and not errors_encountered: 
            await interaction.followup.send("No archived threads were found in the monitored/accessible channels.", ephemeral=True)
            return

        if detected_threads_info:
            current_embed = nextcord.Embed(title=f"Detected Archived Threads (Page {current_page_num})", description="Listing all archived threads found and their status.", color=nextcord.Color.blue())
            current_field_text = ""
            content_field_count_in_current_embed = 0
            now_utc = datetime.now(timezone.utc)

            for i, thread_info in enumerate(detected_threads_info):
                status = thread_info.get('status', 'Archived (Unknown)')
                line = f"- **{thread_info.get('name', 'N/A')}** (ID: `{thread_info.get('id', 'N/A')}`)\n  In: <#{thread_info.get('parent_id', 'N/A')}> | Status: `{status}`"
                if status == "Archived (Closed)" and 'delete_due_at' in thread_info and 'closed_at' in thread_info:
                    delete_due_at_dt = thread_info['delete_due_at'] 
                    closed_at_dt = thread_info['closed_at'] 
                    line += f"\n  Closed: <t:{int(closed_at_dt.timestamp())}:R>"
                    if delete_due_at_dt <= now_utc: line += " | Deletion: **Overdue / Pending**"
                    else: line += f" | Deletes in: **{self._humanize_timedelta(delete_due_at_dt - now_utc)}** (<t:{int(delete_due_at_dt.timestamp())}:R>)"
                line += "\n" 
                if thread_info.get('error'): line += f"  *Error processing this thread: {thread_info['error']}*\n"

                if len(current_field_text) + len(line) > MAX_CHARS_PER_FIELD_VALUE and current_field_text:
                    current_embed.add_field(name=f"Threads (Batch {content_field_count_in_current_embed + 1})", value=current_field_text, inline=False)
                    content_field_count_in_current_embed += 1
                    current_field_text = "" 

                    if content_field_count_in_current_embed >= MAX_CONTENT_FIELDS_PER_EMBED:
                        current_embed.set_footer(text=f"Page {current_page_num} • {len(detected_threads_info)} total threads processed.")
                        embeds_to_send.append(current_embed)
                        current_page_num += 1
                        current_embed = nextcord.Embed(title=f"Detected Archived Threads (Page {current_page_num})", description="Listing all archived threads found and their status.", color=nextcord.Color.blue())
                        content_field_count_in_current_embed = 0
                
                current_field_text += line

            if current_field_text: # Add any remaining text
                current_embed.add_field(name=f"Threads (Batch {content_field_count_in_current_embed + 1})", value=current_field_text, inline=False)
            
            if current_embed.fields: # Ensure the last embed is added if it has content
                current_embed.set_footer(text=f"Page {current_page_num} • {len(detected_threads_info)} total threads processed.")
                embeds_to_send.append(current_embed)
        
        if errors_encountered:
            error_summary = "\n".join(errors_encountered)
            error_field_content = f"```{error_summary[:MAX_CHARS_PER_FIELD_VALUE - 10]}```" # -10 for markdown ```
            if len(error_summary) > MAX_CHARS_PER_FIELD_VALUE - 10:
                error_field_content += "\n*(Further errors truncated. Check console logs.)*"

            if embeds_to_send and len(embeds_to_send[-1].fields) < (MAX_CONTENT_FIELDS_PER_EMBED + 1) and \
               (len(embeds_to_send[-1]) + len(error_field_content) < 5800): # Try to add to last page
                embeds_to_send[-1].add_field(name="⚠️ Scan Issues Encountered", value=error_field_content, inline=False)
                if not detected_threads_info: embeds_to_send[-1].color = nextcord.Color.red()
            else: # Create a new embed for errors
                error_embed = nextcord.Embed(title="⚠️ Scan Issues Encountered", color=nextcord.Color.red())
                error_embed.add_field(name="Details", value=error_field_content, inline=False)
                error_embed.set_footer(text="Some channels or threads might not have been scanned correctly.")
                embeds_to_send.append(error_embed)

        if not embeds_to_send:
            # This case should ideally be covered if errors_encountered leads to an error_embed
            # or if detected_threads_info is empty and handled at the start.
            # But as a fallback:
            await interaction.followup.send("No information to display or an issue occurred generating the report.", ephemeral=True)
            return

        # Send all prepared embeds
        for embed_to_send in embeds_to_send:
            if not embed_to_send.fields and not (embed_to_send.description and detected_threads_info): # Skip completely empty embeds unless it's the initial "None found"
                 if not (embed_to_send.description and not detected_threads_info and not errors_encountered): # Allow "None found" embed
                    continue
            try:
                await interaction.followup.send(embed=embed_to_send, ephemeral=True)
            except nextcord.HTTPException as e:
                logging.error(f"[VIEW_SCANNED] HTTPException while sending paginated embed: {e}")
                # If one fails, send a generic error for the rest
                await interaction.followup.send(f"An error occurred while sending part of the thread list. Some information might be missing. (Total items: {len(detected_threads_info)}, Errors: {len(errors_encountered)})", ephemeral=True)
                break 
            except Exception as e:
                logging.error(f"[VIEW_SCANNED] Exception while sending paginated embed: {e}", exc_info=True)
                await interaction.followup.send(f"A critical error occurred while displaying thread list. Check logs.", ephemeral=True)
                break
    
    async def _has_closed_phrase(self, message: nextcord.Message) -> bool:
        phrase_lower = CLOSED_PHRASE.lower()

        # 1. Check Content
        if message.content and phrase_lower in message.content.lower():
            return True

        # 2. Check Embeds (Explicitly check visible fields)
        for embed in message.embeds:
            # Check standard fields
            if embed.title and phrase_lower in embed.title.lower(): return True
            if embed.description and phrase_lower in embed.description.lower(): return True
            if embed.footer and embed.footer.text and phrase_lower in embed.footer.text.lower(): return True
            if embed.author and embed.author.name and phrase_lower in embed.author.name.lower(): return True
            
            # Check fields
            for field in embed.fields:
                if (field.name and phrase_lower in field.name.lower()) or \
                   (field.value and phrase_lower in field.value.lower()):
                    return True

        # 3. Check Components (Deep Search)
        # This handles Button Labels, Select Menu Placeholders, and custom "V2" Component text.
        
        components_data = []
        
        # Priority 1: content within 'raw_data' (contains the exact API response including new/unsupported types)
        if hasattr(message, 'raw_data') and 'components' in message.raw_data:
            components_data = message.raw_data['components']
        # Priority 2: Hydrated components from nextcord (if raw_data isn't available)
        elif message.components:
            try:
                components_data = [c.to_dict() for c in message.components]
            except Exception:
                # Fallback if to_dict fails on weird components
                pass

        if not components_data:
            return False

        # Recursive helper to find string in any JSON structure (list/dict)
        def contains_phrase_recursive(data):
            if isinstance(data, str):
                return phrase_lower in data.lower()
            elif isinstance(data, list):
                return any(contains_phrase_recursive(item) for item in data)
            elif isinstance(data, dict):
                # We iterate over values. We can ignore keys (like 'custom_id') usually, 
                # but searching all values is safest for "V2" types.
                return any(contains_phrase_recursive(value) for value in data.values())
            return False

        return contains_phrase_recursive(components_data)
    
def setup(bot: commands.Bot):
    bot.add_cog(TicketManagerCog(bot))