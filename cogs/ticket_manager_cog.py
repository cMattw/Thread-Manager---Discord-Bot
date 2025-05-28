import nextcord
from nextcord.ext import commands, tasks, application_checks
from nextcord import Interaction, SlashOption, Thread, TextChannel, ForumChannel, Color 
import database 
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

class TicketManagerCog(commands.Cog, name="Ticket Lifecycle Manager"):
    def __init__(self, bot: commands.Bot): 
        self.bot = bot
        self.check_archived_threads_task.start()

    def cog_unload(self):
        self.check_archived_threads_task.cancel()

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
        if monitored_channel_ids:
            for chan_id in monitored_channel_ids:
                channel = guild.get_channel(chan_id)
                if channel and isinstance(channel, (TextChannel, ForumChannel)):
                    channels_to_scan.append(channel)
                else:
                    logging.warning(f"Monitored channel ID {chan_id} not found or not a Text/Forum channel in guild {guild.name}.")
        else: 
            for tc in guild.text_channels: channels_to_scan.append(tc)
            for fc in guild.forum_channels: channels_to_scan.append(fc)
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
                if not found_closed_phrase_in_message_or_embed and msg_obj.embeds:
                    for embed_obj in msg_obj.embeds:
                        texts_to_check = [embed_obj.title, embed_obj.description, embed_obj.footer.text if embed_obj.footer else None, embed_obj.author.name if embed_obj.author else None]
                        for field in embed_obj.fields: texts_to_check.extend([field.name, field.value])
                        for text_content in filter(None, texts_to_check):
                            if CLOSED_PHRASE.lower() in text_content.lower():
                                message_containing_phrase = msg_obj; timestamp_of_phrase = msg_obj.created_at; found_closed_phrase_in_message_or_embed = True
                                if not is_dry_run: logging.debug(f"Found '{CLOSED_PHRASE}' in thread {thread.name} (embed content) by {msg_obj.author.name} at {timestamp_of_phrase}")
                                break 
                        if found_closed_phrase_in_message_or_embed: break 
                    if found_closed_phrase_in_message_or_embed: break
        except nextcord.HTTPException as e:
            logging.error(f"Error fetching history for thread {thread.name} ({thread.id}): {e}")
            if not is_dry_run and guild_settings.get('log_channel_id'): await self._log_action(guild_id, "Thread Processing Error", thread_obj=thread, details="Failed to fetch message history.", error_details_text=str(e), color=Color.red())
            if is_dry_run: return {"id": thread.id, "name": thread.name, "error": "Failed to fetch history"}
            return None
        
        if is_dry_run and check_closed_phrase_only: 
            base_info = {"id": thread.id, "name": thread.name, "parent_name": thread.parent.name if thread.parent else "Unknown", "parent_id": thread.parent_id}
            if found_closed_phrase_in_message_or_embed and timestamp_of_phrase:
                base_info["status"] = "Archived (Closed)"; base_info["closed_at"] = timestamp_of_phrase
                if timestamp_of_phrase.tzinfo is None: timestamp_of_phrase = timestamp_of_phrase.replace(tzinfo=timezone.utc)
                base_info["delete_due_at"] = timestamp_of_phrase + timedelta(days=delete_delay_config_days)
            else: base_info["status"] = "Archived (Inactive)"
            return base_info

        if found_closed_phrase_in_message_or_embed and timestamp_of_phrase:
            if timestamp_of_phrase.tzinfo is None: timestamp_of_phrase = timestamp_of_phrase.replace(tzinfo=timezone.utc)
            delete_after_timestamp = timestamp_of_phrase + timedelta(days=delete_delay_config_days)
            if datetime.now(timezone.utc) > delete_after_timestamp: 
                if is_dry_run: return {"name": thread.name, "id": thread.id, "closed_at": timestamp_of_phrase, "delete_due_at": delete_after_timestamp, "channel_id": thread.parent_id, "channel_name": thread.parent.name if thread.parent else "Unknown"}
                try: 
                    logging.info(f"Deleting thread {thread.name} ({thread.id}) as it was closed and {delete_delay_config_days} day(s) delay period passed.")
                    await thread.delete() 
                    if guild_settings.get('log_channel_id'): await self._log_action(guild_id, "Thread Deleted", thread_obj=thread, details=f"Ticket closed on {timestamp_of_phrase.astimezone(MANILA_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')} and {delete_delay_config_days} day(s) deletion delay passed.", color=Color.dark_red())
                except nextcord.Forbidden: 
                    logging.warning(f"Missing permissions to delete thread {thread.name} ({thread.id}).")
                    if guild_settings.get('log_channel_id'): await self._log_action(guild_id, "Thread Deletion FAILED", thread_obj=thread, details="Missing Manage Threads permission.", error_details_text="Forbidden", color=Color.red())
                except nextcord.HTTPException as e: 
                    logging.error(f"HTTP error deleting thread {thread.name} ({thread.id}): {e}")
                    if guild_settings.get('log_channel_id'): await self._log_action(guild_id, "Thread Deletion FAILED", thread_obj=thread, details="Discord API error.", error_details_text=str(e), color=Color.red())
            elif not is_dry_run: logging.info(f"Thread {thread.name} ({thread.id}) is closed but {delete_delay_config_days} day(s) delay has not passed. Phrase found at {timestamp_of_phrase}. Will be deleted after {delete_after_timestamp}.")
        elif not is_dry_run: 
            try:
                logging.info(f"Unarchiving non-exempted thread {thread.name} ({thread.id}) due to inactivity (no closure message found).")
                await thread.edit(archived=False) 
                if guild_settings.get('log_channel_id'): await self._log_action(guild_id, "Thread Auto-Unarchived", thread_obj=thread, details="Thread auto-archived by Discord, unarchiving to keep active (non-exempted).", color=Color.gold())
            except nextcord.Forbidden: 
                logging.warning(f"Missing permissions to unarchive non-exempted thread {thread.name} ({thread.id}).")
                if guild_settings.get('log_channel_id'): await self._log_action(guild_id, "Thread Unarchive FAILED", thread_obj=thread, details="Missing Manage Threads permission for non-exempted thread.", error_details_text="Forbidden", color=Color.red())
            except nextcord.HTTPException as e: 
                logging.error(f"HTTP error unarchiving non-exempted thread {thread.name} ({thread.id}): {e}")
                if guild_settings.get('log_channel_id'): await self._log_action(guild_id, "Thread Unarchive FAILED", thread_obj=thread, details="Discord API error for non-exempted thread.", error_details_text=str(e), color=Color.red())
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
        logging.info("Waiting for bot to be ready before starting thread check task...")
        await self.bot.wait_until_ready()
        if not hasattr(self.bot, 'target_guild_id') or not self.bot.target_guild_id:
            logging.error("Target guild ID not available in before_loop for TicketManager. Task may not run or will stop.")
        database.initialize_database() 
        logging.info("TicketManagerCog task ready.")

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

    @nextcord.slash_command(name="view_pending_deletions", description="Lists threads marked closed and past their deletion delay.")
    @application_checks.has_permissions(manage_guild=True)
    async def view_pending_deletions(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True); guild = self.bot.get_guild(self.bot.target_guild_id)
        if not guild: await interaction.followup.send("Target server not found by bot.", ephemeral=True); return
        current_guild_settings = database.get_guild_settings(self.bot.target_guild_id)
        if not current_guild_settings: await interaction.followup.send("Settings not configured.", ephemeral=True); return
        delete_delay_val_days = current_guild_settings.get('delete_delay_days', DEFAULT_DELETE_DELAY_DAYS)
        exempted_thread_ids = database.get_exempted_thread_ids_for_guild(self.bot.target_guild_id)
        channels_to_scan = await self._get_channels_to_scan(guild)
        if not channels_to_scan: await interaction.followup.send("No channels to scan.", ephemeral=True); return

        pending_deletion_threads: List[Dict] = []; errors_encountered: List[str] = []; processed_thread_ids_for_view = set()
        for channel_obj in channels_to_scan:
            try:
                iterators_to_check = []
                if isinstance(channel_obj, TextChannel): 
                    iterators_to_check.append(channel_obj.archived_threads(private=False, limit=None))
                    iterators_to_check.append(channel_obj.archived_threads(private=True, joined=True, limit=None))
                elif isinstance(channel_obj, ForumChannel): iterators_to_check.append(channel_obj.archived_threads(limit=None))
                for iterator in filter(None, iterators_to_check):
                    async for thread_item in iterator:
                        if thread_item.id in processed_thread_ids_for_view: continue
                        result = await self.process_archived_thread(thread_item, self.bot.target_guild_id, delete_delay_val_days, current_guild_settings, exempted_thread_ids, is_dry_run=True, check_closed_phrase_only=False)
                        if result and "error" not in result: pending_deletion_threads.append(result)
                        elif result and "error" in result: errors_encountered.append(f"T#{thread_item.id}: {result['error']}")
                        processed_thread_ids_for_view.add(thread_item.id)
            except nextcord.Forbidden: errors_encountered.append(f"NoPerms: {channel_obj.mention}")
            except Exception as e: errors_encountered.append(f"ErrScan: {channel_obj.mention}: {str(e)[:50]}"); logging.error(f"[VIEW_PENDING] Error scanning {type(channel_obj).__name__} {channel_obj.name}: {e}", exc_info=True)
        
        if not pending_deletion_threads and not errors_encountered:
            await interaction.followup.send("No threads are currently scheduled for deletion (or they are exempted).", ephemeral=True); return
        embed = nextcord.Embed(title="Threads Scheduled for Deletion", description=f"Non-exempted threads marked closed & past {delete_delay_val_days}-day delay.", color=nextcord.Color.orange())
        if pending_deletion_threads:
            output_str = ""; field_count = 0
            for i, thread_info in enumerate(pending_deletion_threads):
                closed_ts = int(thread_info['closed_at'].timestamp()); due_ts = int(thread_info['delete_due_at'].timestamp())
                line = f"- **{thread_info.get('name', 'N/A')}** (ID: `{thread_info.get('id', 'N/A')}`)\n  In: <#{thread_info.get('channel_id', 'N/A')}> | Closed: <t:{closed_ts}:R> | Due: <t:{due_ts}:R>\n"
                if len(output_str) + len(line) > 1020 and i > 0: field_count += 1; embed.add_field(name=f"Pending (Part {field_count})", value=output_str, inline=False); output_str = ""
                output_str += line
            if output_str: field_count += 1; embed.add_field(name=f"Pending (Part {field_count})", value=output_str, inline=False)
        else: embed.add_field(name="Pending Threads", value="None meeting criteria (or all eligible are exempted).", inline=False)
        if errors_encountered:
            error_output = "\n".join(errors_encountered); 
            embed.add_field(name="⚠️ Errors During Scan", value=f"```{error_output[:1020]}```", inline=False); embed.color = nextcord.Color.red()
        embed.set_footer(text="This is a preview. Deletion by periodic scan.")
        try:
            if not embed.fields and (not pending_deletion_threads and not errors_encountered): embed.description = "No threads found and no scan issues."
            elif not embed.fields and not pending_deletion_threads and errors_encountered: embed.description = "No threads found. See scan issues below."
            elif not embed.fields and pending_deletion_threads: embed.add_field(name="Pending Threads", value="Error formatting threads.", inline=False)
            if len(embed) > 5900: await interaction.followup.send(f"Found {len(pending_deletion_threads)} threads. List too long. Errors: {len(errors_encountered)}", ephemeral=True)
            else: await interaction.followup.send(embed=embed, ephemeral=True)
        except nextcord.HTTPException as e: await interaction.followup.send(f"Found {len(pending_deletion_threads)} threads, but list too long. Errors: {len(errors_encountered)}", ephemeral=True); logging.error(f"Error sending /view_pending_deletions embed: {e}")

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
        await interaction.response.defer(ephemeral=True); guild = self.bot.get_guild(self.bot.target_guild_id)
        if not guild: await interaction.followup.send("Target server not found by bot.", ephemeral=True); return
        current_guild_settings = database.get_guild_settings(self.bot.target_guild_id)
        if not current_guild_settings: await interaction.followup.send("Settings not configured (delete delay missing).", ephemeral=True); return
        delete_delay_val_days = current_guild_settings.get('delete_delay_days', DEFAULT_DELETE_DELAY_DAYS)
        exempted_thread_ids = database.get_exempted_thread_ids_for_guild(self.bot.target_guild_id)
        channels_to_scan = await self._get_channels_to_scan(guild)
        if not channels_to_scan: await interaction.followup.send("No channels to scan.", ephemeral=True); return

        logging.info(f"[VIEW_SCANNED] User {interaction.user} (ID: {interaction.user.id}) initiated scan in guild '{guild.name}' (ID: {guild.id})")
        logging.info(f"[VIEW_SCANNED] Channels to scan in '{guild.name}': {[f'{ch.name} ({type(ch).__name__})' for ch in channels_to_scan]}")
        detected_threads_info: List[Dict] = []; errors_encountered: List[str] = []; scanned_thread_ids_this_command = set() 

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
                        logging.info(f"[VIEW_SCANNED] Found {iter_info['type']} thread: '{thread_item.name}' (ID: {thread_item.id}, Archived: {thread_item.archived}) in '{channel_obj.name}'")
                        if thread_item.id in scanned_thread_ids_this_command: 
                            logging.debug(f"[VIEW_SCANNED] Thread '{thread_item.name}' ID {thread_item.id} already listed, skipping.")
                            continue
                        result = await self.process_archived_thread(thread_item, self.bot.target_guild_id, delete_delay_val_days, current_guild_settings, exempted_thread_ids, is_dry_run=True, check_closed_phrase_only=True)
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
        if not detected_threads_info and not errors_encountered: 
            await interaction.followup.send("No archived threads were found in the monitored/accessible channels.", ephemeral=True); return
        embed = nextcord.Embed(title="Detected Archived Threads", description="Listing all archived threads found and their status.", color=nextcord.Color.blue())
        if detected_threads_info:
            output_str = ""; field_count = 0; now_utc = datetime.now(timezone.utc)
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
                if thread_info.get('error'): line += f"  *Error: {thread_info['error']}*\n"
                if len(output_str) + len(line) > 1020 and i > 0:
                    field_count += 1; embed.add_field(name=f"Detected Threads (Part {field_count})", value=output_str, inline=False); output_str = ""
                output_str += line
            if output_str: field_count += 1; embed.add_field(name=f"Detected Threads (Part {field_count})", value=output_str, inline=False)
        else: embed.add_field(name="Detected Threads", value="None found.", inline=False)
        if errors_encountered:
            error_output = "\n".join(errors_encountered); 
            embed.add_field(name="⚠️ Scan Issues", value=f"```{error_output[:1020]}```", inline=False); 
            if not detected_threads_info : embed.color = nextcord.Color.red()
        embed.set_footer(text=f"Found {len(detected_threads_info)} archived threads entries. This is a snapshot.")
        try:
            if not embed.fields: 
                 if not detected_threads_info and not errors_encountered: embed.description = "No archived threads found and no scan issues."
                 elif not detected_threads_info and errors_encountered: embed.description = "No threads found. See scan issues below."
            if len(embed) > 5900: await interaction.followup.send(f"Found {len(detected_threads_info)} threads. List too long. Errors: {len(errors_encountered)}", ephemeral=True)
            else: await interaction.followup.send(embed=embed, ephemeral=True)
        except nextcord.HTTPException as e: await interaction.followup.send(f"Found {len(detected_threads_info)} threads, but an error occurred displaying them. Errors: {len(errors_encountered)}", ephemeral=True); logging.error(f"Error sending /view_scanned_threads embed: {e}")

def setup(bot: commands.Bot):
    bot.add_cog(TicketManagerCog(bot))