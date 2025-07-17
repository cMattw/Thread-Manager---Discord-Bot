import nextcord
from nextcord.ext import commands, tasks, application_checks
from nextcord import (
    Interaction, SlashOption, Embed, Color, ForumChannel, Thread, Message,
    ui, ButtonStyle, PartialMessage
)
import logging
from typing import Optional, List, Dict
from datetime import datetime, timedelta, timezone
import asyncio

# Import the database utility
from db_utils import trade_database as db

# --- Logger ---
logger = logging.getLogger('nextcord.trade_manager_cog')

# --- Helper Functions ---
def get_unix_time(offset_seconds: int = 0) -> int:
    """Returns the current Unix timestamp, with an optional offset in seconds."""
    return int((datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)).timestamp())

# --- UI Views ---

class ControlPanelView(ui.View):
    def __init__(self, cog_instance):
        super().__init__(timeout=None)
        self.cog = cog_instance

    @ui.button(label="Mark as Complete", style=ButtonStyle.green, custom_id="trade_mark_complete")
    async def mark_complete_button(self, button: ui.Button, interaction: Interaction):
        thread = interaction.channel
        if not isinstance(thread, Thread): return

        trade_data = db.get_managed_thread(thread.id)
        if not trade_data or str(interaction.user.id) != trade_data.get('op_id'):
            await interaction.response.send_message("Only the author of the trade post can mark it as complete.", ephemeral=True)
            return
        
        await interaction.response.defer()
        await self.cog.execute_completion(thread, interaction.user)

class ReminderView(ui.View):
    def __init__(self, cog_instance):
        super().__init__(timeout=43200)
        self.cog = cog_instance

    @ui.button(label="Yes, It's Complete", style=ButtonStyle.green, custom_id="trade_reminder_complete")
    async def complete_button(self, button: ui.Button, interaction: Interaction):
        thread = interaction.channel
        trade_data = db.get_managed_thread(thread.id)
        if not trade_data or str(interaction.user.id) != trade_data.get('op_id'):
            await interaction.response.send_message("Only the trade author can respond.", ephemeral=True)
            return

        await interaction.response.defer()
        await self.cog.execute_completion(thread, interaction.user)
        try:
            await interaction.message.delete()
        except nextcord.NotFound:
            pass

    @ui.button(label="No, Keep Open", style=ButtonStyle.grey, custom_id="trade_reminder_keep_open")
    async def keep_open_button(self, button: ui.Button, interaction: Interaction):
        thread = interaction.channel
        trade_data = db.get_managed_thread(thread.id)
        if not trade_data or str(interaction.user.id) != trade_data.get('op_id'):
            await interaction.response.send_message("Only the trade author can respond.", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)

        # Clear ALL reminder info to reset the inactivity timer completely
        db.clear_thread_reminder_info(thread.id)
        try:
            await interaction.message.delete()
        except nextcord.NotFound:
            pass
            
        await interaction.followup.send("Thanks for the update! I've reset the inactivity timer.", ephemeral=True)
                
    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        pass

# --- Cog Definition ---

class TradeManagerCog(commands.Cog, name="Trade Manager"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config: Optional[Dict] = None
        self._cog_loaded = False
        self._target_guild_id = bot.target_guild_id
        # Schedule the async startup logic
        bot.loop.create_task(self._startup())

    async def _startup(self):
        await self.bot.wait_until_ready()
        logger.info("TradeManagerCog: Running async startup.")
        db.initialize_database(self._target_guild_id)
        self.config = db.get_config(self._target_guild_id)
        logger.info(f"Loaded config at startup: {self.config}")
        forum_id = self.config.get('forum_channel_id') if self.config else None
        if forum_id and forum_id != "None":
            logger.info(f"Trade Manager configured for forum channel ID: {forum_id}")
            self._cog_loaded = True
            if not self.daily_reminder_task.is_running():
                self.daily_reminder_task.start()
                logger.info("Started daily_reminder_task")
            if not self.expiration_and_deletion_task.is_running():
                self.expiration_and_deletion_task.start()
                logger.info("Started expiration_and_deletion_task")
            logger.info("Trade Manager automated tasks have started.")
        else:
            logger.warning("Trade Manager not configured. Use `/trade_config set_channel` to begin.")

        self.bot.add_view(ControlPanelView(self))
        self.bot.add_view(ReminderView(self))

    async def cog_load(self):
        await self.bot.wait_until_ready()
        logger.info("TradeManagerCog: Cog is loading.")
        db.initialize_database(self._target_guild_id)
        self.config = db.get_config(self._target_guild_id)
        logger.info(f"Loaded config at startup: {self.config}")
        forum_id = self.config.get('forum_channel_id') if self.config else None
        if forum_id and forum_id != "None":
            logger.info(f"Trade Manager configured for forum channel ID: {forum_id}")
            self._cog_loaded = True
            if not self.daily_reminder_task.is_running():
                self.daily_reminder_task.start()
                logger.info("Started daily_reminder_task")
            if not self.expiration_and_deletion_task.is_running():
                self.expiration_and_deletion_task.start()
                logger.info("Started expiration_and_deletion_task")
            logger.info("Trade Manager automated tasks have started.")
        else:
            logger.warning("Trade Manager not configured. Use `/trade_config set_channel` to begin.")
        
        self.bot.add_view(ControlPanelView(self))
        self.bot.add_view(ReminderView(self))

    def cog_unload(self):
        self.daily_reminder_task.cancel()
        self.expiration_and_deletion_task.cancel()
        logger.info("TradeManagerCog: Unloaded and tasks cancelled.")
        
    async def cog_check(self, interaction: Interaction) -> bool:
        if not self._cog_loaded and not interaction.application_command.name.startswith("trade_config"):
            await interaction.response.send_message("The Trade Manager cog is not configured. An admin must set the trades channel first.", ephemeral=True)
            return False
        return True
    
    def refresh_config(self):
        """Always fetch the latest config from the database."""
        db.initialize_database(self._target_guild_id)
        self.config = db.get_config(self._target_guild_id)
        return self.config
    
    @commands.Cog.listener()
    async def on_thread_create(self, thread: Thread):

        if thread.guild.id != self._target_guild_id:
            return

        config = db.get_config(self._target_guild_id)
        
        if not config or not config.get('forum_channel_id'):
            return

        forum_channel_id = config.get('forum_channel_id')
        if thread.parent_id != int(forum_channel_id):
            return

        logger.info(f"New trade post detected in configured forum: '{thread.name}' (ID: {thread.id}) by {thread.owner.display_name}")

        author_has_message = False
        async for msg in thread.history(limit=5, oldest_first=True):
            if msg.author.id == thread.owner_id:
                author_has_message = True
                break
        if not author_has_message:
            logger.info(f"Not sending control panel to thread {thread.id} yet: waiting for author to send a message.")
            return
        
        creation_unix = int(thread.created_at.timestamp())
        db.add_managed_thread(thread.id, thread.owner_id, thread.guild.id, creation_unix)

        tag_names = [tag.name for tag in thread.applied_tags]
        tag_display = f"`{', '.join(tag_names)}`" if tag_names else "None"
        
        embed = Embed(
            title="Trade Management Panel",
            description=f"Welcome, {thread.owner.mention}! Your trade post is now active.\n\n"
                        f"The tags you selected are: {tag_display}.\n\n"
                        f"Use the button below when your trade is complete. I will send a reminder here every 24 hours.",
            color=Color.blue()
        )
        embed.set_footer(text="This panel helps keep the trade channel clean.")

        try:
            view = ControlPanelView(self)
            await thread.send(embed=embed, view=view)
        except Exception as e:
            logger.error(f"Failed to send control panel to thread {thread.id}: {e}", exc_info=True)

    async def execute_completion(self, thread: Thread, user: nextcord.User):
        logger.info(f"Executing completion for thread {thread.id} initiated by {user.display_name}")

        if self.config is None:
            logger.warning("Trade Manager config was not loaded. Attempting to load it now.")
            db.initialize_database(self._target_guild_id)
            self.config = db.get_config(self._target_guild_id)
            if self.config is None:
                logger.error(f"Could not load config for guild {self._target_guild_id}. Cannot execute completion.")
                await thread.send("⚠️ **Critical Error:** Could not load bot configuration. Please ask an admin to reconfigure the trade manager.")
                return
        
        try:
            await thread.edit(locked=True)
        except nextcord.Forbidden:
            await thread.send("⚠️ **Error:** I don't have permission to lock this thread. Please contact an admin.")
            logger.warning(f"Could not lock thread {thread.id} due to missing permissions.")
            return

        delay_hours = self.config.get('deletion_delay_hours', 24)
        deletion_unix = get_unix_time(offset_seconds=delay_hours * 3600)
        db.mark_thread_as_complete(thread.id, deletion_unix)
        
        try:
            async for message in thread.history(limit=10, oldest_first=True):
                if message.author.id == self.bot.user.id and message.embeds:
                    new_embed = message.embeds[0]
                    new_embed.description = f"**Trade Complete!** This post has been locked by {user.mention}.\n\nIt will be automatically deleted <t:{deletion_unix}:R>."
                    new_embed.color = Color.green()
                    
                    disabled_view = ui.View()
                    for item in message.components:
                        if isinstance(item, nextcord.ActionRow):
                             for child in item.children:
                                 if isinstance(child, ui.Button):
                                     child.disabled = True
                                     disabled_view.add_item(child)

                    await message.edit(embed=new_embed, view=disabled_view)
                    break
        except Exception as e:
            logger.error(f"Failed to edit control panel message in thread {thread.id}: {e}", exc_info=True)
            await thread.send(f"**Trade Complete!** This post is now locked and will be automatically deleted <t:{deletion_unix}:R>.")

    @tasks.loop(minutes=1)
    async def expiration_and_deletion_task(self):
        self.refresh_config()
        forum_id = self.config.get('forum_channel_id') if self.config else None
        if not forum_id or forum_id == "None":
            logger.warning("Config missing or forum_channel_id not set. Skipping expiration_and_deletion_task.")
            return
        """Enhanced task with better logging and error handling"""
        logger.info("=== STARTING expiration_and_deletion_task ===")
        
        try:
            now_unix = get_unix_time()
            logger.info(f"Current Unix timestamp: {now_unix}")
            
            guild = self.bot.get_guild(self._target_guild_id)
            if not guild: 
                logger.error(f"Could not find target guild {self._target_guild_id} for expiration task")
                return

            logger.info(f"Processing tasks for guild: {guild.name} (ID: {guild.id})")

            # FIRST: Handle deletion of completed trades (HIGH PRIORITY)
            logger.info("--- CHECKING FOR THREADS TO DELETE ---")
            threads_to_delete = db.get_threads_for_deletion(now_unix)
            logger.info(f"Found {len(threads_to_delete)} threads scheduled for deletion")
            
            if threads_to_delete:
                for trade in threads_to_delete:
                    thread_id = trade['thread_id']
                    deletion_ts = trade.get('deletion_timestamp')
                    logger.info(f"Processing thread {thread_id} for deletion (scheduled for {deletion_ts}, current: {now_unix})")
                    
                    thread = self.bot.get_channel(int(thread_id))
                    if thread:
                        logger.info(f"Found thread '{thread.name}' (ID: {thread.id}), attempting deletion...")
                        try:
                            await thread.delete()
                            logger.info(f"✅ Successfully deleted thread {thread.id}")
                        except nextcord.Forbidden as e:
                            logger.error(f"❌ Missing permissions to delete thread {thread.id}: {e}")
                            logger.error("Bot needs 'Manage Threads' permission in the forum channel!")
                        except nextcord.NotFound as e:
                            logger.warning(f"⚠️ Thread {thread.id} was already deleted or not found: {e}")
                        except Exception as e:
                            logger.error(f"❌ Failed to delete thread {thread.id}: {e}", exc_info=True)
                    else:
                        logger.warning(f"⚠️ Thread {thread_id} not found in cache, removing from database")
                    
                    # Always remove from database whether deletion succeeded or not
                    try:
                        db.remove_thread(thread_id)
                        logger.info(f"Removed thread {thread_id} from database")
                    except Exception as e:
                        logger.error(f"Failed to remove thread {thread_id} from database: {e}")
                    
                    await asyncio.sleep(1)  # Rate limiting
            else:
                logger.info("No threads scheduled for deletion at this time")

            # SECOND: Handle active thread management (inactivity and expiration)
            logger.info("--- CHECKING ACTIVE THREADS FOR INACTIVITY/EXPIRATION ---")
            active_threads = db.get_all_active_threads()
            logger.info(f"Found {len(active_threads)} active threads to check")
            
            processed_count = 0
            for trade in active_threads:
                thread_id = trade['thread_id']
                logger.debug(f"Checking active thread {thread_id}")
                
                thread = self.bot.get_channel(int(thread_id))
                if not isinstance(thread, Thread):
                    logger.warning(f"Thread {thread_id} not found or not a Thread, removing from database")
                    try:
                        db.remove_thread(thread_id)
                    except Exception as e:
                        logger.error(f"Failed to remove invalid thread {thread_id}: {e}")
                    continue
                    
                if thread.archived:
                    logger.debug(f"Skipping archived thread {thread.id}")
                    continue
                    
                if thread.locked:
                    logger.debug(f"Skipping locked thread {thread.id}")
                    continue
                
                op = guild.get_member(int(trade['op_id']))
                if not op:
                    logger.warning(f"Original poster {trade['op_id']} not found for thread {thread.id}")
                    continue
                
                # Check for inactivity after reminder (12 hours)
                last_sent_ts = trade.get('last_reminder_sent_timestamp')
                last_msg_id = trade.get('last_reminder_message_id')
                # Only close if both conditions are met: reminder was sent AND it's been 12+ hours AND message still exists
                if last_sent_ts and last_msg_id and (now_unix - last_sent_ts) > (12 * 3600):
                    logger.info(f"🔒 Thread {thread.id} inactive for >12h after reminder. Closing.")
                    try:
                        await thread.edit(locked=True)
                        deletion_ts = get_unix_time(offset_seconds=3600)  # Delete in 1 hour
                        await thread.send(f"{op.mention}, this post is being closed due to inactivity after a reminder. It will be automatically deleted <t:{deletion_ts}:R>.")
                        db.set_thread_deletion_time(thread.id, deletion_ts)
                        logger.info(f"Thread {thread.id} marked for deletion at {deletion_ts}")
                        processed_count += 1
                    except Exception as e:
                        logger.error(f"Failed to close inactive thread {thread.id}: {e}", exc_info=True)
                    continue

                # Check for 7-day lifetime limit
                creation_ts = trade['creation_timestamp']
                age_seconds = now_unix - creation_ts
                age_days = age_seconds / (24 * 3600)
                if age_seconds >= (7 * 24 * 3600):
                    logger.info(f"⏰ Thread {thread.id} reached 7-day lifetime limit (age: {age_days:.1f} days). Closing.")
                    try:
                        await thread.edit(locked=True)
                        deletion_ts = get_unix_time(offset_seconds=3600)  # Delete in 1 hour
                        await thread.send(f"{op.mention}, your trade post has been active for one week and will be automatically closed to keep the forum clean. It will be deleted <t:{deletion_ts}:R>. Please create a new post if still needed.")
                        db.set_thread_deletion_time(thread.id, deletion_ts)
                        logger.info(f"Thread {thread.id} marked for deletion at {deletion_ts}")
                        processed_count += 1
                    except Exception as e:
                        logger.error(f"Failed to close expired thread {thread.id}: {e}", exc_info=True)
                    continue

            logger.info(f"Processed {processed_count} threads for closure/expiration")
            logger.info("=== COMPLETED expiration_and_deletion_task ===")
            
        except Exception as e:
            logger.error(f"Critical error in expiration_and_deletion_task: {e}", exc_info=True)

    @tasks.loop(hours=24)
    async def daily_reminder_task(self):
        self.refresh_config()
        forum_id = self.config.get('forum_channel_id') if self.config else None
        if not forum_id or forum_id == "None":
            logger.warning("Config missing or forum_channel_id not set. Skipping expiration_and_deletion_task.")
            return
        logger.info("=== STARTING daily_reminder_task ===")
        
        try:
            # Ensure config is loaded
            if self.config is None:
                logger.warning("Config not loaded in daily_reminder_task, attempting to load...")
                db.initialize_database(self._target_guild_id)
                self.config = db.get_config(self._target_guild_id)
                if self.config is None:
                    logger.error("Failed to load config in daily_reminder_task")
                    return
            
            logger.info("Running daily reminder check for active trades.")
            active_threads = db.get_all_active_threads()
            logger.info(f"Found {len(active_threads)} active threads to check for reminders")
            
            guild = self.bot.get_guild(self._target_guild_id)
            if not guild: 
                logger.error(f"Could not find guild {self._target_guild_id}")
                return

            reminder_count = 0
            for trade in active_threads:
                last_sent_ts = trade.get('last_reminder_sent_timestamp')
                if last_sent_ts and (get_unix_time() - last_sent_ts) < (24 * 3600):
                    logger.debug(f"Thread {trade['thread_id']} reminder sent recently, skipping")
                    continue

                thread = self.bot.get_channel(int(trade['thread_id']))
                if not isinstance(thread, Thread) or thread.archived or thread.locked: 
                    logger.debug(f"Thread {trade['thread_id']} is not available for reminders")
                    continue

                op = guild.get_member(int(trade['op_id']))
                if not op: 
                    logger.warning(f"Could not find OP {trade['op_id']} for thread {trade['thread_id']}")
                    continue

                last_msg_id = trade.get('last_reminder_message_id')
                if last_msg_id:
                    try:
                        old_msg = thread.get_partial_message(int(last_msg_id))
                        await old_msg.delete()
                        logger.debug(f"Deleted old reminder message {last_msg_id}")
                    except nextcord.NotFound: 
                        pass
                    except Exception as e: 
                        logger.warning(f"Could not delete old reminder message {last_msg_id}: {e}")

                try:
                    # Calculate when the thread would be deleted if inactive after reminder (12 hours + 1 hour grace)
                    deletion_unix = get_unix_time(offset_seconds=(12) * 3600)
                    view = ReminderView(self)
                    reminder_msg = await thread.send(
                        content=(
                            f"{op.mention}, is this trade still active? Please mark it as complete if it's done.\n"
                            f"-# If there is no response, this post will be automatically **locked <t:{deletion_unix}:R>.**"
                        ),
                        view=view
                    )
                    db.update_thread_reminder_info(thread.id, reminder_msg.id, get_unix_time())
                    logger.info(f"Sent reminder to thread {thread.id}")
                    reminder_count += 1
                except Exception as e:
                    logger.error(f"Failed to send daily reminder to thread {thread.id}: {e}", exc_info=True)
                
                await asyncio.sleep(2)

            logger.info(f"Sent {reminder_count} reminders")
            logger.info("=== COMPLETED daily_reminder_task ===")
            
        except Exception as e:
            logger.error(f"Critical error in daily_reminder_task: {e}", exc_info=True)

    @daily_reminder_task.before_loop
    @expiration_and_deletion_task.before_loop
    async def before_tasks(self):
        await self.bot.wait_until_ready()

    @commands.Cog.listener()
    async def on_message(self, message: nextcord.Message):
        # Only care about forum threads in the target guild
        if not isinstance(message.channel, Thread):
            return
        thread = message.channel
        if thread.guild.id != self._target_guild_id:
            return

        config = db.get_config(self._target_guild_id)
        if not config or not config.get('forum_channel_id'):
            return
        forum_channel_id = config.get('forum_channel_id')
        if thread.parent_id != int(forum_channel_id):
            return

        # Only act if the message is from the thread owner
        if message.author.id != thread.owner_id:
            return

        # Only send if the bot hasn't already sent the control panel
        already_sent = False
        async for msg in thread.history(limit=10, oldest_first=True):
            if msg.author.id == self.bot.user.id and msg.embeds:
                already_sent = True
                break
        if already_sent:
            return

        # Send the control panel
        tag_names = [tag.name for tag in thread.applied_tags]
        tag_display = f"`{', '.join(tag_names)}`" if tag_names else "None"
        embed = Embed(
            title="Trade Management Panel",
            description=f"Welcome, {thread.owner.mention}! Your trade post is now active.\n\n"
                        f"The tags you selected are: {tag_display}.\n\n"
                        f"Use the button below when your trade is complete. I will send a reminder here every 24 hours.",
            color=Color.blue()
        )
        embed.set_footer(text="This panel helps keep the trade channel clean.")

        try:
            view = ControlPanelView(self)
            await thread.send(embed=embed, view=view)
            logger.info(f"Sent control panel to thread {thread.id} after first author message.")
        except Exception as e:
            logger.error(f"Failed to send control panel to thread {thread.id} after first author message: {e}", exc_info=True)

    @nextcord.slash_command(name="trades", description="Manage your active trade posts.")
    async def trades_group(self, interaction: Interaction): pass

    @trades_group.subcommand(name="list", description="Shows a list of your active trade posts.")
    async def list_my_trades(self, interaction: Interaction):
        active_trades = db.get_user_active_trades(interaction.user.id, self._target_guild_id)
        if not active_trades:
            await interaction.response.send_message("You have no active trade posts.", ephemeral=True)
            return
            
        embed = Embed(title="Your Active Trades", color=Color.blurple())
        lines = []
        for trade in active_trades:
            thread = self.bot.get_channel(int(trade['thread_id']))
            title = thread.name if thread else f"Unknown Thread (ID: {trade['thread_id']})"
            expiry_ts = trade['creation_timestamp'] + (7 * 24 * 3600)
            lines.append(f"• <#{trade['thread_id']}> - Expires <t:{expiry_ts}:R>")

        embed.description = "\n".join(lines)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @trades_group.subcommand(name="close", description="Close one of your active trade posts.")
    async def close_my_trade(self, interaction: Interaction,
                             post: str = SlashOption(description="The trade post to close.", required=True, autocomplete=True)):
        
        try:
            thread_id = int(post)
        except ValueError:
            await interaction.response.send_message("Invalid selection. Please choose a post from the list.", ephemeral=True)
            return

        thread_to_close = self.bot.get_channel(thread_id)
        if not isinstance(thread_to_close, Thread):
            await interaction.response.send_message("Could not find the specified trade post.", ephemeral=True); return

        trade_data = db.get_managed_thread(thread_id)
        if not trade_data or str(interaction.user.id) != trade_data.get('op_id'):
            await interaction.response.send_message("This is not your trade post, or it is not managed.", ephemeral=True); return

        await interaction.response.defer(ephemeral=True)
        await self.execute_completion(thread_to_close, interaction.user)
        await interaction.followup.send(f"Trade post <#{thread_to_close.id}> has been marked as complete.", ephemeral=True)

    @close_my_trade.on_autocomplete("post")
    async def autocomplete_user_trades(self, interaction: Interaction, current_input: str):
        active_trades = db.get_user_active_trades(interaction.user.id, self._target_guild_id)
        choices = {}
        for trade in active_trades:
            thread = self.bot.get_channel(int(trade['thread_id']))
            if thread:
                title = thread.name
                if current_input.lower() in title.lower():
                    choices[title[:100]] = str(thread.id)
        await interaction.response.send_autocomplete(choices)

    @nextcord.slash_command(name="trade_config", description="Configure the Trade Manager cog.")
    @application_checks.has_permissions(manage_guild=True)
    async def trade_config_group(self, interaction: Interaction): pass

    @trade_config_group.subcommand(name="set_channel", description="Sets the trades forum channel for the bot to manage.")
    async def set_channel(self, interaction: Interaction, channel: ForumChannel = SlashOption(description="The forum channel for trades.", required=True)):
        db.initialize_database(self._target_guild_id)
        db.update_config(self._target_guild_id, {"forum_channel_id": str(channel.id)})
        self.config = db.get_config(self._target_guild_id)
        self._cog_loaded = True
        if not self.daily_reminder_task.is_running():
            self.daily_reminder_task.start()
        if not self.expiration_and_deletion_task.is_running():
            self.expiration_and_deletion_task.start()
        await interaction.response.send_message(f"✅ Trade Manager will now manage new posts in {channel.mention}.", ephemeral=True)

    @trade_config_group.subcommand(name="set_delete_delay", description="Sets the delay (hours) for deleting a user-completed post.")
    async def set_delete_delay(self, interaction: Interaction, hours: float = SlashOption(description="Hours to wait before deleting a completed trade (e.g., 24).", min_value=0, required=True)):
        db.initialize_database(self._target_guild_id)
        db.update_config(self._target_guild_id, {"deletion_delay_hours": hours})
        self.config = db.get_config(self._target_guild_id)
        await interaction.response.send_message(f"✅ Deletion delay for user-completed trades set to {hours} hour(s).", ephemeral=True)

    @trade_config_group.subcommand(name="complete", description="Allows an admin to manually mark any trade post as complete.")
    async def admin_complete_trade(self, interaction: Interaction, thread_id: str = SlashOption(description="The ID of the trade thread to complete.", required=True)):
        db.initialize_database(self._target_guild_id)
        try:
            thread_id_int = int(thread_id)
        except ValueError:
            await interaction.response.send_message("Invalid Thread ID format.", ephemeral=True)
            return
            
        thread = self.bot.get_channel(thread_id_int)
        if not isinstance(thread, Thread) or not db.get_managed_thread(thread.id):
            await interaction.response.send_message("This is not a valid or managed trade thread.", ephemeral=True)
            return
            
        await interaction.response.defer(ephemeral=True)
        await self.execute_completion(thread, interaction.user)
        await interaction.followup.send(f"Trade <#{thread.id}> has been administratively marked as complete.", ephemeral=True)

    @trade_config_group.subcommand(name="show", description="Displays the current Trade Manager configuration.")
    async def show_config(self, interaction: Interaction):
        db.initialize_database(self._target_guild_id)
        self.config = db.get_config(self._target_guild_id)

        forum_id = self.config.get('forum_channel_id') if self.config else None
        channel = self.bot.get_channel(int(forum_id)) if forum_id else None
        delay = self.config.get('deletion_delay_hours', 24) if self.config else 24

        embed = Embed(title="Trade Manager Configuration", color=Color.dark_blue())
        embed.add_field(name="Trades Forum Channel", value=channel.mention if channel else "Not Set", inline=False)
        embed.add_field(name="Deletion Delay (User-Completed)", value=f"{delay} hours", inline=False)
        
        await interaction.response.send_message(embed=embed, ephemeral=True)

def setup(bot: commands.Bot):
    bot.add_cog(TradeManagerCog(bot))