import nextcord
from nextcord.ext import commands, tasks, application_checks
from nextcord import Interaction, SlashOption, Thread, TextChannel, ForumChannel # Added TextChannel, ForumChannel
import database 
import logging
from datetime import datetime, timedelta, timezone
import pytz
from typing import Optional, List, Dict, Union # Added Union
import re 

# Constants
CLOSED_PHRASE = "This ticket has been closed"
DEFAULT_SCAN_INTERVAL_MINUTES = 60
DEFAULT_DELETE_DELAY_DAYS = 7 
MANILA_TZ = pytz.timezone("Asia/Manila")

class TicketManagerCog(commands.Cog):
    def __init__(self, bot: commands.Bot): 
        self.bot = bot
        self.check_archived_threads_task.start()

    def cog_unload(self):
        self.check_archived_threads_task.cancel()

    async def _log_action(self, guild_id: int, action: str, thread: Optional[nextcord.Thread] = None, reason: Optional[str] = None, error_details: Optional[str] = None):
        settings = database.get_guild_settings(guild_id)
        log_channel_id = None
        if settings:
            log_channel_id = settings.get('log_channel_id')

        console_log_needed = True
        log_channel = None 
        if log_channel_id:
            log_channel = self.bot.get_channel(log_channel_id)
            if log_channel:
                console_log_needed = False 
            else:
                logging.warning(f"Log channel {log_channel_id} not found for guild {guild_id}. Action: {action}, Thread: {thread.name if thread else 'N/A'}")
        else: 
            if not settings:
                 logging.info(f"Guild {guild_id} settings not found, cannot determine log channel.")
        
        if console_log_needed:
            log_msg_console = f"GUILD_ID: {guild_id} | Action: {action}"
            if thread: log_msg_console += f" | Thread: {thread.name} ({thread.id})"
            if reason: log_msg_console += f" | Reason: {reason}"
            if error_details: log_msg_console += f" | Error: {error_details}"
            logging.info(log_msg_console)
            if not log_channel: 
                return

        timestamp = datetime.now(MANILA_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')
        embed = nextcord.Embed(title="Bot Action Log", color=nextcord.Color.orange())
        embed.add_field(name="Timestamp (GMT+8)", value=timestamp, inline=False)
        embed.add_field(name="Action", value=action, inline=False)

        if thread:
            embed.add_field(name="Thread Name", value=thread.name, inline=True)
            embed.add_field(name="Thread ID", value=str(thread.id), inline=True)
        
        if reason:
            embed.add_field(name="Details", value=reason, inline=False)
        
        if error_details:
            embed.color = nextcord.Color.red()
            embed.add_field(name="Error", value=error_details, inline=False)
        
        try:
            if log_channel: 
                 await log_channel.send(embed=embed)
        except nextcord.HTTPException as e:
            logging.error(f"Failed to send log message to channel {log_channel_id} for guild {guild_id}: {e}")

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
            for tc in guild.text_channels:
                channels_to_scan.append(tc)
            for fc in guild.forum_channels:
                channels_to_scan.append(fc)
        return channels_to_scan

    @tasks.loop(minutes=DEFAULT_SCAN_INTERVAL_MINUTES)
    async def check_archived_threads_task(self):
        await self.bot.wait_until_ready()
        logging.info("Starting periodic check for archived threads...")
        
        all_guild_configs = database.get_all_guild_configs()
        if not all_guild_configs:
            logging.info("No guild configurations found in DB. Defaulting task interval. Scan skipped.")
            current_loop_interval = self.check_archived_threads_task.minutes
            if current_loop_interval != DEFAULT_SCAN_INTERVAL_MINUTES:
                try:
                    self.check_archived_threads_task.change_interval(minutes=DEFAULT_SCAN_INTERVAL_MINUTES)
                    logging.info(f"Task interval reset to default {DEFAULT_SCAN_INTERVAL_MINUTES} minutes.")
                except Exception as e:
                    logging.error(f"Failed to change interval to default: {e}. Keeping {current_loop_interval} min.")
            return

        first_config = all_guild_configs[0]
        new_interval = first_config.get('scan_interval_minutes', DEFAULT_SCAN_INTERVAL_MINUTES)
        current_loop_interval = self.check_archived_threads_task.minutes
        if current_loop_interval != new_interval:
            try:
                self.check_archived_threads_task.change_interval(minutes=new_interval)
                logging.info(f"Scan interval updated to {new_interval} minutes from DB.")
            except Exception as e:
                logging.error(f"Failed to change interval to {new_interval}: {e}. Keeping {current_loop_interval} min.")

        for guild_config_from_db in all_guild_configs:
            guild_id = guild_config_from_db['guild_id']
            guild = self.bot.get_guild(guild_id)
            if not guild:
                logging.warning(f"Guild {guild_id} not found during scan. Skipping.")
                continue

            current_guild_settings = database.get_guild_settings(guild.id)
            if not current_guild_settings:
                logging.info(f"No settings for guild {guild.name} ({guild.id}). Skipping.")
                continue

            delete_delay_val_days = current_guild_settings.get('delete_delay_days', DEFAULT_DELETE_DELAY_DAYS)
            channels_to_scan = await self._get_channels_to_scan(guild)
            
            logging.info(f"[MAIN_TASK_SCAN] Guild '{guild.name}': Channels to scan: {[f'{ch.name} ({type(ch).__name__})' for ch in channels_to_scan]}")

            if not channels_to_scan:
                logging.info(f"No channels to scan in guild {guild.name}.")
                if current_guild_settings.get('log_channel_id'):
                     await self._log_action(guild.id, "Scan Info", reason="No channels configured or accessible for scanning.")
                continue
            
            logging.info(f"Scanning guild: {guild.name} ({guild.id}) in {len(channels_to_scan)} container(s) with delete delay of {delete_delay_val_days} days.")

            for channel_obj in channels_to_scan:
                logging.info(f"[MAIN_TASK_SCAN] Guild '{guild.name}': Now scanning {type(channel_obj).__name__} '{channel_obj.name}' (ID: {channel_obj.id})")
                threads_found_in_this_container_count = 0
                try:
                    processed_thread_ids = set()
                    
                    threads_iterator = None
                    private_threads_iterator = None # Only for TextChannel

                    if isinstance(channel_obj, TextChannel):
                        threads_iterator = channel_obj.archived_threads(private=False, limit=None)
                        private_threads_iterator = channel_obj.archived_threads(private=True, joined=True, limit=None)
                    elif isinstance(channel_obj, ForumChannel):
                        threads_iterator = channel_obj.archived_threads(limit=None)
                    
                    if threads_iterator:
                        async for thread_item in threads_iterator:
                            threads_found_in_this_container_count += 1
                            logging.info(f"[MAIN_TASK_SCAN] Guild '{guild.name}', Container '{channel_obj.name}': Found archived thread '{thread_item.name}' (ID: {thread_item.id}, Archived: {thread_item.archived})")
                            if thread_item.id not in processed_thread_ids:
                                await self.process_archived_thread(thread_item, guild.id, delete_delay_val_days, current_guild_settings, is_dry_run=False)
                                processed_thread_ids.add(thread_item.id)
                    
                    if private_threads_iterator: # Only for TextChannels
                         async for thread_item in private_threads_iterator: 
                            threads_found_in_this_container_count +=1
                            logging.info(f"[MAIN_TASK_SCAN] Guild '{guild.name}', Container '{channel_obj.name}': Found PRIVATE archived thread '{thread_item.name}' (ID: {thread_item.id}, Archived: {thread_item.archived})")
                            if thread_item.id not in processed_thread_ids:
                                await self.process_archived_thread(thread_item, guild.id, delete_delay_val_days, current_guild_settings, is_dry_run=False)
                                processed_thread_ids.add(thread_item.id)
                    
                    if threads_found_in_this_container_count == 0:
                        logging.info(f"[MAIN_TASK_SCAN] Guild '{guild.name}', Container '{channel_obj.name}': No archived threads yielded by iterators.")

                except nextcord.Forbidden:
                    logging.warning(f"Missing permissions to fetch archived threads in {type(channel_obj).__name__} {channel_obj.name} ({channel_obj.id}) in guild {guild.name}.")
                    if current_guild_settings.get('log_channel_id'):
                        await self._log_action(guild.id, "Scan Error", reason=f"Missing permissions for {channel_obj.mention}.", error_details="Forbidden to fetch archived threads.")
                except Exception as e:
                    logging.error(f"Error processing {type(channel_obj).__name__} {channel_obj.name} ({channel_obj.id}) in task: {e}", exc_info=True)
                    if current_guild_settings.get('log_channel_id'):
                        await self._log_action(guild.id, "Scan Error", reason=f"Error during scan of {channel_obj.mention}.", error_details=str(e))
            logging.info(f"Finished scanning guild: {guild.name} ({guild.id}).")
        logging.info("Finished periodic check for archived threads.")
        
    async def process_archived_thread(self, thread: nextcord.Thread, guild_id: int, delete_delay_config_days: int, guild_settings: dict, is_dry_run: bool = False, check_closed_phrase_only: bool = False) -> Optional[Dict]:
        if database.is_thread_exempted(thread.id):
            if not is_dry_run: 
                if thread.archived:
                    logging.info(f"Exempted thread '{thread.name}' ({thread.id}) is currently archived. Attempting to unarchive to keep active.")
                    try:
                        await thread.edit(archived=False) 
                        await self._log_action(guild_id, "Exempted Thread Auto-Unarchived", thread, "Kept active by bot due to exemption setting.")
                    except nextcord.Forbidden:
                        logging.warning(f"Missing 'Manage Threads' permission to unarchive exempted thread '{thread.name}' ({thread.id}).")
                        await self._log_action(guild_id, "Exempted Thread Unarchive Failed", thread, "Missing Manage Threads permission for auto-unarchive.", error_details="Forbidden")
                    except nextcord.HTTPException as e:
                        logging.error(f"HTTP error unarchiving exempted thread '{thread.name}' ({thread.id}): {e}")
                        await self._log_action(guild_id, "Exempted Thread Unarchive Failed", thread, "Discord API error during auto-unarchive.", error_details=str(e))
                else: 
                    logging.debug(f"Exempted thread '{thread.name}' ({thread.id}) is unarchived as expected. No further auto-management.")
                return None 
            
            elif is_dry_run and check_closed_phrase_only:
                return {
                    "id": thread.id, "name": thread.name, 
                    "status": f"Exempted (Currently {'Archived' if thread.archived else 'Unarchived'})",
                    "parent_name": thread.parent.name if thread.parent else "Unknown",
                    "parent_id": thread.parent_id
                }
            return None 

        if not thread.archived: 
            return None

        if not is_dry_run:
            logging.debug(f"Processing non-exempted archived thread: {thread.name} ({thread.id}) for guild {guild_id}")

        message_containing_phrase = None
        timestamp_of_phrase = None
        found_closed_phrase_in_message_or_embed = False

        try:
            history_limit = 20 if (is_dry_run and check_closed_phrase_only) else 100
            async for msg_obj in thread.history(limit=history_limit, oldest_first=False):
                if msg_obj.content and CLOSED_PHRASE.lower() in msg_obj.content.lower():
                    message_containing_phrase = msg_obj
                    timestamp_of_phrase = msg_obj.created_at
                    found_closed_phrase_in_message_or_embed = True
                    if not is_dry_run:
                        logging.debug(f"Found '{CLOSED_PHRASE}' in thread {thread.name} (message content) by {msg_obj.author.name} at {timestamp_of_phrase}")
                    break 

                if not found_closed_phrase_in_message_or_embed and msg_obj.embeds:
                    for embed_obj in msg_obj.embeds:
                        texts_to_check = []
                        if embed_obj.title: texts_to_check.append(embed_obj.title)
                        if embed_obj.description: texts_to_check.append(embed_obj.description)
                        if embed_obj.footer and embed_obj.footer.text: texts_to_check.append(embed_obj.footer.text)
                        if embed_obj.author and embed_obj.author.name: texts_to_check.append(embed_obj.author.name)
                        for field in embed_obj.fields:
                            if field.name: texts_to_check.append(field.name)
                            if field.value: texts_to_check.append(field.value)

                        for text_content in texts_to_check:
                            if CLOSED_PHRASE.lower() in text_content.lower():
                                message_containing_phrase = msg_obj
                                timestamp_of_phrase = msg_obj.created_at
                                found_closed_phrase_in_message_or_embed = True
                                if not is_dry_run:
                                    logging.debug(f"Found '{CLOSED_PHRASE}' in thread {thread.name} (embed content) by {msg_obj.author.name} at {timestamp_of_phrase}")
                                break 
                        if found_closed_phrase_in_message_or_embed: break 
                    if found_closed_phrase_in_message_or_embed: break
                        
        except nextcord.HTTPException as e:
            logging.error(f"Error fetching history for thread {thread.name} ({thread.id}): {e}")
            if not is_dry_run and guild_settings.get('log_channel_id'):
                await self._log_action(guild_id, "Thread Processing Error", thread=thread, reason="Failed to fetch message history.", error_details=str(e))
            if is_dry_run: return {"id": thread.id, "name": thread.name, "error": "Failed to fetch history"}
            return None
        
        if is_dry_run and check_closed_phrase_only: 
            base_info = {
                "id": thread.id, "name": thread.name,
                "parent_name": thread.parent.name if thread.parent else "Unknown",
                "parent_id": thread.parent_id
            }
            if found_closed_phrase_in_message_or_embed and timestamp_of_phrase:
                base_info["status"] = "Archived (Closed)"
                base_info["closed_at"] = timestamp_of_phrase
                if timestamp_of_phrase.tzinfo is None: 
                     timestamp_of_phrase = timestamp_of_phrase.replace(tzinfo=timezone.utc)
                base_info["delete_due_at"] = timestamp_of_phrase + timedelta(days=delete_delay_config_days)
            else:
                base_info["status"] = "Archived (Inactive)"
            return base_info

        if found_closed_phrase_in_message_or_embed and timestamp_of_phrase:
            if timestamp_of_phrase.tzinfo is None:
                timestamp_of_phrase = timestamp_of_phrase.replace(tzinfo=timezone.utc)
            
            delete_after_timestamp = timestamp_of_phrase + timedelta(days=delete_delay_config_days)

            if datetime.now(timezone.utc) > delete_after_timestamp: 
                if is_dry_run: 
                    return {
                        "name": thread.name, "id": thread.id,
                        "closed_at": timestamp_of_phrase,
                        "delete_due_at": delete_after_timestamp,
                        "channel_id": thread.parent_id,
                        "channel_name": thread.parent.name if thread.parent else "Unknown"
                    }
                try:
                    logging.info(f"Deleting thread {thread.name} ({thread.id}) as it was closed and {delete_delay_config_days} day(s) delay period passed.")
                    await thread.delete() 
                    if guild_settings.get('log_channel_id'):
                        closed_time_local = timestamp_of_phrase.astimezone(MANILA_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')
                        await self._log_action(guild_id, "Deleted Thread", thread=thread, reason=f"Ticket closed on {closed_time_local} and {delete_delay_config_days} day(s) deletion delay passed.")
                except nextcord.Forbidden:
                    logging.warning(f"Missing permissions to delete thread {thread.name} ({thread.id}).")
                    if guild_settings.get('log_channel_id'):
                        await self._log_action(guild_id, "Deletion Failed", thread=thread, reason="Missing permissions (Manage Threads).")
                except nextcord.HTTPException as e:
                    logging.error(f"HTTP error deleting thread {thread.name} ({thread.id}): {e}")
                    if guild_settings.get('log_channel_id'):
                        await self._log_action(guild_id, "Deletion Failed", thread=thread, reason="Discord API error.", error_details=str(e))
            else: 
                if not is_dry_run:
                    logging.info(f"Thread {thread.name} ({thread.id}) is closed but {delete_delay_config_days} day(s) delay has not passed. Phrase found at {timestamp_of_phrase}. Will be deleted after {delete_after_timestamp}.")
        
        elif not is_dry_run: 
            try:
                logging.info(f"Unarchiving non-exempted thread {thread.name} ({thread.id}) due to inactivity (no closure message found).")
                await thread.edit(archived=False) 
                if guild_settings.get('log_channel_id'):
                    await self._log_action(guild_id, "Unarchived Thread", thread=thread, reason="Thread auto-archived by Discord, unarchiving to keep active (non-exempted).")
            except nextcord.Forbidden:
                 logging.warning(f"Missing permissions to unarchive non-exempted thread {thread.name} ({thread.id}).")
                 if guild_settings.get('log_channel_id'):
                    await self._log_action(guild_id, "Unarchive Failed", thread=thread, reason="Missing permissions (Manage Threads) for non-exempted thread.")
            except nextcord.HTTPException as e:
                logging.error(f"HTTP error unarchiving non-exempted thread {thread.name} ({thread.id}): {e}")
                if guild_settings.get('log_channel_id'):
                    await self._log_action(guild_id, "Unarchive Failed", thread=thread, reason="Discord API error for non-exempted thread.", error_details=str(e))
        
        return None

    @check_archived_threads_task.before_loop
    async def before_check_archived_threads_task(self):
        logging.info("Waiting for bot to be ready before starting thread check task...")
        await self.bot.wait_until_ready()
        database.initialize_database() 
        logging.info("TicketManagerCog task ready.")

    @nextcord.slash_command(name="view_pending_deletions", description="Lists threads marked closed and past their deletion delay.")
    @application_checks.has_permissions(manage_guild=True)
    async def view_pending_deletions(self, interaction: nextcord.Interaction):
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        if not guild: await interaction.followup.send("Server context not found.", ephemeral=True); return
        current_guild_settings = database.get_guild_settings(guild.id)
        if not current_guild_settings: await interaction.followup.send("Settings not configured.", ephemeral=True); return
        delete_delay_val_days = current_guild_settings.get('delete_delay_days', DEFAULT_DELETE_DELAY_DAYS)
        channels_to_scan = await self._get_channels_to_scan(guild)
        if not channels_to_scan: await interaction.followup.send("No channels to scan.", ephemeral=True); return

        pending_deletion_threads: List[Dict] = []
        errors_encountered: List[str] = []
        processed_thread_ids_for_view = set()

        for channel_obj in channels_to_scan:
            try:
                threads_iterator = None
                private_threads_iterator = None
                if isinstance(channel_obj, TextChannel):
                    threads_iterator = channel_obj.archived_threads(private=False, limit=None)
                    private_threads_iterator = channel_obj.archived_threads(private=True, joined=True, limit=None)
                elif isinstance(channel_obj, ForumChannel):
                    threads_iterator = channel_obj.archived_threads(limit=None)
                
                if threads_iterator:
                    async for thread_item in threads_iterator:
                        if thread_item.id in processed_thread_ids_for_view: continue
                        result = await self.process_archived_thread(thread_item, guild.id, delete_delay_val_days, current_guild_settings, is_dry_run=True, check_closed_phrase_only=False)
                        if result and "error" not in result: pending_deletion_threads.append(result)
                        elif result and "error" in result: errors_encountered.append(f"T#{thread_item.id}: {result['error']}")
                        processed_thread_ids_for_view.add(thread_item.id)
                
                if private_threads_iterator:
                    async for thread_item in private_threads_iterator:
                        if thread_item.id in processed_thread_ids_for_view: continue
                        result = await self.process_archived_thread(thread_item, guild.id, delete_delay_val_days, current_guild_settings, is_dry_run=True, check_closed_phrase_only=False)
                        if result and "error" not in result: pending_deletion_threads.append(result)
                        elif result and "error" in result: errors_encountered.append(f"T#{thread_item.id}: {result['error']}")
                        processed_thread_ids_for_view.add(thread_item.id)
            except nextcord.Forbidden: errors_encountered.append(f"NoPerms: {channel_obj.mention}")
            except Exception as e: errors_encountered.append(f"ErrScan: {channel_obj.mention}: {str(e)[:50]}"); logging.error(f"[VIEW_PENDING] Error scanning {type(channel_obj).__name__} {channel_obj.name}: {e}", exc_info=True)
        
        if not pending_deletion_threads and not errors_encountered:
            await interaction.followup.send("No threads are currently scheduled for deletion (or they are exempted).", ephemeral=True)
            return
        embed = nextcord.Embed(
            title="Threads Scheduled for Deletion",
            description=f"The following non-exempted threads are marked closed and have passed the {delete_delay_val_days}-day deletion delay.",
            color=nextcord.Color.orange()
        )
        if pending_deletion_threads:
            output_str = ""
            field_count = 0
            for i, thread_info in enumerate(pending_deletion_threads):
                closed_ts = int(thread_info['closed_at'].timestamp())
                due_ts = int(thread_info['delete_due_at'].timestamp())
                line = f"- **{thread_info.get('name', 'Unknown Name')}** (ID: `{thread_info.get('id', 'N/A')}`)\n  In: <#{thread_info.get('channel_id', 'N/A')}> | Closed: <t:{closed_ts}:R> | Due: <t:{due_ts}:R>\n"
                if len(output_str) + len(line) > 1020 and i > 0: 
                    field_count += 1
                    embed.add_field(name=f"Pending (Part {field_count})", value=output_str, inline=False)
                    output_str = ""
                output_str += line
            if output_str:
                 field_count += 1
                 embed.add_field(name=f"Pending (Part {field_count})", value=output_str, inline=False)
        else:
            embed.add_field(name="Pending Threads", value="None meeting criteria (or all eligible are exempted).", inline=False)
        if errors_encountered:
            error_output = "\n".join(errors_encountered)
            if len(error_output) > 1020: error_output = error_output[:1020] + "..."
            embed.add_field(name="⚠️ Errors During Scan", value=f"```{error_output}```", inline=False)
            embed.color = nextcord.Color.red()
        embed.set_footer(text="This is a preview. Actual deletion occurs during the bot's periodic scan.")
        try:
            if not embed.fields:
                 if not pending_deletion_threads and not errors_encountered: 
                      embed.description = "No threads found and no scan issues."
                 elif not pending_deletion_threads and errors_encountered:
                      embed.description = "No threads found. See scan issues below."
            if len(embed) > 6000:
                await interaction.followup.send(f"Found {len(pending_deletion_threads)} threads pending deletion. The list is too long to display. Errors: {len(errors_encountered)}", ephemeral=True)
            else:
                await interaction.followup.send(embed=embed, ephemeral=True)
        except nextcord.HTTPException as e:
             await interaction.followup.send(f"Found {len(pending_deletion_threads)} threads pending deletion, but the list is too long to display. Errors: {len(errors_encountered)}", ephemeral=True)

    def _humanize_timedelta(self, delta: timedelta) -> str:
        if delta.total_seconds() <= 0:
            return "now or overdue"
        days = delta.days
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        parts = []
        if days > 0: parts.append(f"{days}d")
        if hours > 0: parts.append(f"{hours}h")
        if minutes > 0: parts.append(f"{minutes}m")
        if not parts: 
            if seconds > 0 : return f"{seconds}s"
            return "imminently"
        return ", ".join(parts)

    @nextcord.slash_command(name="view_scanned_threads", description="Shows all detected archived threads in monitored channels.")
    @application_checks.has_permissions(manage_guild=True)
    async def view_scanned_threads(self, interaction: nextcord.Interaction):
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        if not guild: await interaction.followup.send("Server context not found.", ephemeral=True); return
        current_guild_settings = database.get_guild_settings(guild.id)
        if not current_guild_settings: await interaction.followup.send("Settings not configured (delete delay missing).", ephemeral=True); return
        delete_delay_val_days = current_guild_settings.get('delete_delay_days', DEFAULT_DELETE_DELAY_DAYS)
        channels_to_scan = await self._get_channels_to_scan(guild)
        if not channels_to_scan: await interaction.followup.send("No channels to scan.", ephemeral=True); return

        logging.info(f"[VIEW_SCANNED] Channels to scan in '{guild.name}': {[f'{ch.name} ({type(ch).__name__})' for ch in channels_to_scan]}")
        detected_threads_info: List[Dict] = []
        errors_encountered: List[str] = []
        scanned_thread_ids = set() 

        for channel_obj in channels_to_scan:
            logging.info(f"[VIEW_SCANNED] Scanning {type(channel_obj).__name__}: '{channel_obj.name}' (ID: {channel_obj.id})")
            found_in_container_count = 0
            try:
                threads_iterator = None
                private_threads_iterator = None
                if isinstance(channel_obj, TextChannel):
                    threads_iterator = channel_obj.archived_threads(private=False, limit=None)
                    private_threads_iterator = channel_obj.archived_threads(private=True, joined=True, limit=None)
                elif isinstance(channel_obj, ForumChannel):
                    threads_iterator = channel_obj.archived_threads(limit=None)

                if threads_iterator:
                    async for thread_item in threads_iterator:
                        found_in_container_count +=1
                        logging.info(f"[VIEW_SCANNED] Found archived thread: '{thread_item.name}' (ID: {thread_item.id}) in '{channel_obj.name}'")
                        if thread_item.id in scanned_thread_ids: continue
                        result = await self.process_archived_thread(thread_item, guild.id, delete_delay_val_days, current_guild_settings, is_dry_run=True, check_closed_phrase_only=True)
                        if result: detected_threads_info.append(result)
                        scanned_thread_ids.add(thread_item.id)
                
                if private_threads_iterator:
                    async for thread_item in private_threads_iterator:
                        found_in_container_count +=1
                        logging.info(f"[VIEW_SCANNED] Found PRIVATE archived thread: '{thread_item.name}' (ID: {thread_item.id}) in '{channel_obj.name}'")
                        if thread_item.id in scanned_thread_ids: continue
                        result = await self.process_archived_thread(thread_item, guild.id, delete_delay_val_days, current_guild_settings, is_dry_run=True, check_closed_phrase_only=True)
                        if result: detected_threads_info.append(result)
                        scanned_thread_ids.add(thread_item.id)
                
                if found_in_container_count == 0:
                    logging.info(f"[VIEW_SCANNED] No archived threads yielded by iterators in {type(channel_obj).__name__} '{channel_obj.name}'.")
            except nextcord.Forbidden: 
                logging.warning(f"[VIEW_SCANNED] Missing permissions for {type(channel_obj).__name__} '{channel_obj.name}'.")
                errors_encountered.append(f"Missing permissions for {channel_obj.mention}")
            except Exception as e:
                logging.error(f"[VIEW_SCANNED] Error during scan of {type(channel_obj).__name__} '{channel_obj.name}': {e}", exc_info=True)
                errors_encountered.append(f"Error scanning {channel_obj.mention}: {str(e)[:50]}")
        
        logging.info(f"[VIEW_SCANNED] Finished iteration for guild '{guild.name}'. Total threads added: {len(detected_threads_info)}. Errors: {len(errors_encountered)}.")
        if not detected_threads_info and not errors_encountered:
            await interaction.followup.send("No archived threads found.", ephemeral=True)
            return
        embed = nextcord.Embed(title="Detected Archived Threads", description="Listing all archived threads found and their status.", color=nextcord.Color.blue())
        if detected_threads_info:
            output_str = ""
            field_count = 0
            now_utc = datetime.now(timezone.utc)
            for i, thread_info in enumerate(detected_threads_info):
                status = thread_info.get('status', 'Archived (Unknown)')
                line = f"- **{thread_info.get('name', 'Unknown Name')}** (ID: `{thread_info.get('id', 'N/A')}`)\n  In: <#{thread_info.get('parent_id', 'N/A')}> | Status: `{status}`"
                if status == "Archived (Closed)" and 'delete_due_at' in thread_info and 'closed_at' in thread_info:
                    delete_due_at_dt = thread_info['delete_due_at'] 
                    closed_at_dt = thread_info['closed_at'] 
                    line += f"\n  Closed: <t:{int(closed_at_dt.timestamp())}:R>"
                    if delete_due_at_dt <= now_utc: line += " | Deletion: **Overdue / Pending**"
                    else: line += f" | Deletes in: **{self._humanize_timedelta(delete_due_at_dt - now_utc)}** (<t:{int(delete_due_at_dt.timestamp())}:R>)"
                line += "\n" 
                if thread_info.get('error'): line += f"  *Error: {thread_info['error']}*\n"
                if len(output_str) + len(line) > 1020 and i > 0:
                    field_count += 1
                    embed.add_field(name=f"Detected Threads (Part {field_count})", value=output_str, inline=False)
                    output_str = ""
                output_str += line
            if output_str:
                 field_count += 1
                 embed.add_field(name=f"Detected Threads (Part {field_count})", value=output_str, inline=False)
        else:
            embed.add_field(name="Detected Threads", value="None found.", inline=False)
        if errors_encountered:
            error_output = "\n".join(errors_encountered)
            if len(error_output) > 1020: error_output = error_output[:1020] + "..."
            embed.add_field(name="⚠️ Scan Issues", value=f"```{error_output}```", inline=False)
            if not detected_threads_info : embed.color = nextcord.Color.red()
        embed.set_footer(text=f"Found {len(detected_threads_info)} archived threads entries. This is a snapshot.")
        try:
            if not embed.fields: 
                 if not detected_threads_info and not errors_encountered: embed.description = "No archived threads found and no scan issues."
                 elif not detected_threads_info and errors_encountered: embed.description = "No threads found. See scan issues below."
            if len(embed) > 5900: 
                 await interaction.followup.send(f"Found {len(detected_threads_info)} archived threads. List is too long. Errors: {len(errors_encountered)}", ephemeral=True)
            else:
                await interaction.followup.send(embed=embed, ephemeral=True)
        except nextcord.HTTPException as e:
             await interaction.followup.send(f"Found {len(detected_threads_info)} archived threads, but an error occurred displaying them. Errors: {len(errors_encountered)}", ephemeral=True)

    @nextcord.slash_command(name="ticket", description="Ticket management utilities.")
    async def ticket_group(self, interaction: Interaction):
        pass 

    async def _resolve_thread_from_target(self, guild: nextcord.Guild, target_str: str) -> Optional[nextcord.Thread]:
        thread_id = None
        link_match = re.search(r'discord(?:app)?.com/channels/\d+/(?:\d+/)?(\d+)', target_str) # Handles both guild/channel/thread and guild/thread
        
        if link_match:
            try:
                thread_id = int(link_match.group(1))
            except ValueError:
                pass
        else: 
            try:
                thread_id = int(target_str)
            except ValueError:
                return None

        if thread_id:
            try:
                # Use self.bot.fetch_channel as it's more general for fetching any channel type by ID
                channel_or_thread = await self.bot.fetch_channel(thread_id) 
                if isinstance(channel_or_thread, nextcord.Thread):
                    if channel_or_thread.guild.id == guild.id: # Ensure thread is in the correct guild
                        return channel_or_thread
                    else:
                        logging.warning(f"Resolved thread {thread_id} does not belong to guild {guild.id}")
                        return None
                else:
                    logging.warning(f"Fetched channel {thread_id} is a {type(channel_or_thread).__name__}, not a Thread object.")
                    return None 
            except nextcord.NotFound:
                logging.debug(f"Thread with ID {thread_id} not found.")
                return None
            except nextcord.Forbidden:
                logging.warning(f"Bot lacks permissions to fetch channel/thread {thread_id}.")
                return None
            except Exception as e:
                logging.error(f"Error fetching channel/thread {thread_id}: {e}", exc_info=True)
                return None
        return None

    @ticket_group.subcommand(name="keep_active", description="Exempt a thread from auto-management and unarchives it.")
    @application_checks.has_permissions(manage_threads=True) 
    async def ticket_keep_active(self, interaction: Interaction, 
                                 thread_target: str = SlashOption(description="The ID or link of the thread to keep active", required=True)):
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild 
        if not guild: 
            await interaction.followup.send("This command must be used in a server.", ephemeral=True)
            return

        target_thread = await self._resolve_thread_from_target(guild, thread_target)

        if not target_thread:
            await interaction.followup.send(f"Could not find a valid thread in this server with the provided ID or link: `{thread_target}`.", ephemeral=True)
            logging.warning(f"[KEEP_ACTIVE] User {interaction.user} failed to find thread: {thread_target} in guild {guild.id}")
            return

        logging.info(f"[KEEP_ACTIVE] User {interaction.user} targeting thread '{target_thread.name}' (ID: {target_thread.id}). Current archived status: {target_thread.archived}")
        is_already_exempted = database.is_thread_exempted(target_thread.id)
        feedback_message = ""

        if is_already_exempted:
            feedback_message = f"Thread <#{target_thread.id}> is already exempted from auto-management."
            logging.info(f"[KEEP_ACTIVE] Thread {target_thread.id} is already exempted.")
            if target_thread.archived:
                logging.info(f"[KEEP_ACTIVE] Thread {target_thread.id} (already exempted) is archived. Attempting to unarchive.")
                try:
                    await target_thread.edit(archived=False) 
                    feedback_message += " It has now been unarchived."
                    logging.info(f"[KEEP_ACTIVE] Successfully unarchived already-exempted thread {target_thread.id}.")
                    await self._log_action(guild.id, "Exempted Thread Unarchived", target_thread, f"Force unarchived by {interaction.user.name} (was already exempted)")
                except nextcord.Forbidden:
                    feedback_message += " I tried to unarchive it but **lack `Manage Threads` permission**."
                    logging.warning(f"[KEEP_ACTIVE] Failed to unarchive already-exempted thread {target_thread.id} due to missing 'Manage Threads' permission.")
                except Exception as e:
                    feedback_message += f" An error occurred while trying to unarchive it: {type(e).__name__}."
                    logging.error(f"[KEEP_ACTIVE] Error unarchiving already-exempted thread {target_thread.id}: {e}", exc_info=True)
            else:
                logging.info(f"[KEEP_ACTIVE] Thread {target_thread.id} (already exempted) is not archived. No unarchive action needed.")
            await interaction.followup.send(feedback_message, ephemeral=True, suppress_embeds=True)
            return

        if database.add_exempted_thread(guild.id, target_thread.id, interaction.user.id):
            feedback_message = f"Thread <#{target_thread.id}> is now exempted from auto-management by {interaction.user.mention}."
            action_taken_for_log = "Thread Exempted"
            logging.info(f"[KEEP_ACTIVE] Successfully exempted thread {target_thread.id}.")
            if target_thread.archived:
                logging.info(f"[KEEP_ACTIVE] Newly exempted thread {target_thread.id} is archived. Attempting to unarchive.")
                try:
                    await target_thread.edit(archived=False) 
                    feedback_message += " It has also been unarchived."
                    action_taken_for_log = "Thread Exempted & Unarchived"
                    logging.info(f"[KEEP_ACTIVE] Successfully unarchived newly exempted thread {target_thread.id}.")
                except nextcord.Forbidden:
                    feedback_message += " I tried to unarchive it but **lack `Manage Threads` permission**."
                    action_taken_for_log = "Thread Exempted (Unarchive Failed - No Perms)"
                    logging.warning(f"[KEEP_ACTIVE] Failed to unarchive newly exempted thread {target_thread.id} due to missing 'Manage Threads' permission.")
                except Exception as e:
                    feedback_message += f" An error occurred while trying to unarchive it: {type(e).__name__}."
                    action_taken_for_log = f"Thread Exempted (Unarchive Error: {type(e).__name__})"
                    logging.error(f"[KEEP_ACTIVE] Error unarchiving newly exempted thread {target_thread.id}: {e}", exc_info=True)
            else:
                logging.info(f"[KEEP_ACTIVE] Newly exempted thread {target_thread.id} is not archived. No unarchive action needed.")
            await self._log_action(guild.id, action_taken_for_log, target_thread, f"Action by {interaction.user.name}")
            await interaction.followup.send(feedback_message, ephemeral=True, suppress_embeds=True)
        else:
            feedback_message = f"Failed to save exemption for thread <#{target_thread.id}> in the database. Please check bot console logs."
            logging.error(f"[KEEP_ACTIVE] Database call to add_exempted_thread failed for thread {target_thread.id}.")
            await interaction.followup.send(feedback_message, ephemeral=True)

    @ticket_group.subcommand(name="allow_automation", description="Removes a thread's exemption, allowing bot auto-management.")
    @application_checks.has_permissions(manage_threads=True) 
    async def ticket_allow_automation(self, interaction: Interaction,
                                      thread_target: str = SlashOption(description="The ID or link of the thread to manage automatically", required=True)):
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        if not guild:
            await interaction.followup.send("This command must be used in a server.", ephemeral=True)
            return

        target_thread = await self._resolve_thread_from_target(guild, thread_target)

        if not target_thread:
            await interaction.followup.send(f"Could not find a valid thread in this server with the provided ID or link: `{thread_target}`.", ephemeral=True)
            return

        if not database.is_thread_exempted(target_thread.id):
            await interaction.followup.send(f"Thread <#{target_thread.id}> was not exempted from auto-management.", ephemeral=True)
            return

        if database.remove_exempted_thread(guild.id, target_thread.id): 
            await interaction.followup.send(f"Thread <#{target_thread.id}> will now be auto-managed by the bot again.", ephemeral=True)
            await self._log_action(guild.id, "Thread Exemption Removed", target_thread, f"Exemption removed by {interaction.user.name}")
        else:
            await interaction.followup.send(f"Failed to remove exemption for thread <#{target_thread.id}>. Please check bot logs.", ephemeral=True)

def setup(bot):
    bot.add_cog(TicketManagerCog(bot))