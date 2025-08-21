import nextcord
from nextcord.ext import commands, tasks
from nextcord import SlashOption
import nextcord.ui
import asyncio
import logging
import datetime

from db_utils.activity_database import (
    init_db, get_settings, update_setting, 
    add_excluded_channel, remove_excluded_channel
)

# Configure logging
logger = logging.getLogger("activity_checker_cog")
if not logger.handlers:
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
logger.setLevel(logging.INFO)  # Or DEBUG for more detail

class ActivityCheckButton(nextcord.ui.View):
    def __init__(self, user_id: int, message_id: int, parent_cog):
        super().__init__()
        self.user_id = user_id
        self.message_id = message_id
        self.parent_cog = parent_cog
        # Set timeout based on response timeout + buffer
        self.timeout = parent_cog.settings['response_timeout_minutes'] * 60 + 10

    @nextcord.ui.button(label="I'm Active!", style=nextcord.ButtonStyle.green)
    async def activity_button(self, button: nextcord.ui.Button, interaction: nextcord.Interaction):
        # Check if the correct user is clicking the button
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This button is not for you.", ephemeral=True)
            return

        # Send ephemeral thank you message
        await interaction.response.send_message("Thanks for confirming your activity!", ephemeral=True)
        
        # Mark user as active in the parent cog
        await self.parent_cog.mark_user_active(self.user_id, self.message_id, interaction.guild.id)
        
        # Delete the reminder message
        try:
            await interaction.message.delete()
        except nextcord.NotFound:
            logger.info(f"Reminder message {self.message_id} already deleted.")
        except nextcord.Forbidden:
            logger.warning(f"Missing permissions to delete message {self.message_id}.")
        
        # Stop the view
        self.stop()

class ActivityChecker(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # Initialize database
        init_db()
        # Load settings
        self.settings = get_settings()
        # Track user voice activity: {guild_id: {user_id: {'join_time': datetime, 'last_check_time': datetime, 'vc_id': int}}}
        self.user_voice_tracking = {}
        # Track pending checks: {guild_id: {user_id: {'message_id': int, 'message_channel_id': int, 'original_vc_id': int, 'check_start_time': datetime, 'move_task': asyncio.Task}}}
        self.pending_checks = {}
        # Start background tasks
        self.voice_state_monitor.start()
        self.activity_check_loop.start()
        self.cleanup_pending_checks_loop.start()

        logger.info("ActivityChecker cog initialized.")

    def cog_unload(self):
        """Clean up when cog is unloaded."""
        self.voice_state_monitor.cancel()
        self.activity_check_loop.cancel()
        self.cleanup_pending_checks_loop.cancel()
        
        # Cancel all pending move tasks
        for guild_id, guild_checks in self.pending_checks.items():
            for user_id, check_data in guild_checks.items():
                if 'move_task' in check_data and check_data['move_task']:
                    check_data['move_task'].cancel()

    @commands.Cog.listener()
    async def on_voice_state_update(self, member, before, after):
        """Track when users join/leave voice channels."""
        if member.bot:
            return
        
        guild_id = member.guild.id
        user_id = member.id
        current_time = datetime.datetime.now()
        
        # Initialize guild tracking if not exists
        if guild_id not in self.user_voice_tracking:
            self.user_voice_tracking[guild_id] = {}
        
        # User joined a voice channel
        if before.channel is None and after.channel is not None:
            self.user_voice_tracking[guild_id][user_id] = {
                'join_time': current_time,
                'last_check_time': current_time,
                'vc_id': after.channel.id
            }
            logger.info(f"Tracking voice join: {member.display_name} joined {after.channel.name}")
        
        # User left a voice channel
        elif before.channel is not None and after.channel is None:
            if user_id in self.user_voice_tracking[guild_id]:
                del self.user_voice_tracking[guild_id][user_id]
                logger.info(f"Stopped tracking: {member.display_name} left voice")
            
            # Cancel any pending checks for this user
            if guild_id in self.pending_checks and user_id in self.pending_checks[guild_id]:
                move_task = self.pending_checks[guild_id][user_id].get('move_task')
                if move_task:
                    move_task.cancel()
                del self.pending_checks[guild_id][user_id]
        
        # User switched voice channels
        elif before.channel != after.channel and after.channel is not None:
            if user_id in self.user_voice_tracking[guild_id]:
                # Update the channel but keep the join time
                self.user_voice_tracking[guild_id][user_id]['vc_id'] = after.channel.id
                logger.info(f"Updated tracking: {member.display_name} moved to {after.channel.name}")

    @tasks.loop(minutes=1)  # Check every 1 minute for users who need reminders
    async def activity_check_loop(self):
        """Check for users who need activity reminders based on their individual join times."""
        self.settings = get_settings()
        if not self.settings.get('is_enabled', True):
            logger.info("Activity checking is disabled in settings.")
            return

        current_time = datetime.datetime.now()
        check_interval_seconds = self.settings.get('check_interval_minutes', 1) * 60 

        logger.info("Running activity_check_loop...")

        for guild_id, users in self.user_voice_tracking.items():
            guild = self.bot.get_guild(guild_id)
            if not guild:
                logger.warning(f"Guild {guild_id} not found.")
                continue

            if guild_id not in self.pending_checks:
                self.pending_checks[guild_id] = {}

            for user_id, tracking_data in list(users.items()):
                logger.info(f"Checking user {user_id} in guild {guild_id}...")

                if user_id in self.pending_checks[guild_id]:
                    logger.info(f"User {user_id} already has a pending check.")
                    continue

                time_since_last_check = (current_time - tracking_data['last_check_time']).total_seconds()
                logger.info(f"User {user_id} time since last check: {time_since_last_check}s (interval: {check_interval_seconds}s)")

                if time_since_last_check < check_interval_seconds:
                    continue

                member = guild.get_member(user_id)
                if not member or not member.voice:
                    logger.info(f"User {user_id} not found or not in voice. Removing from tracking.")
                    del users[user_id]
                    continue

                voice_channel = member.voice.channel

                # Skip if in AFK channel
                afk_channel_id = self.settings.get('afk_channel_id')
                if afk_channel_id and voice_channel.id == afk_channel_id:
                    logger.info(f"Skipping reminder for {member.display_name} in AFK channel.")
                    tracking_data['last_check_time'] = current_time
                    continue

                if voice_channel.id in self.settings['excluded_channels']:
                    logger.info(f"Voice channel {voice_channel.id} is excluded.")
                    tracking_data['last_check_time'] = current_time
                    continue

                if not self._is_user_eligible_for_check(member):
                    logger.info(f"User {user_id} is not eligible for check.")
                    tracking_data['last_check_time'] = current_time
                    continue

                eligible_users = self._get_eligible_users_in_channel(voice_channel)
                if len(eligible_users) < 2:
                    logger.info(f"Not enough eligible users in channel {voice_channel.id}.")
                    tracking_data['last_check_time'] = current_time
                    continue

                # If you reach here, a reminder should be sent
                logger.info(f"Sending reminder to {member.display_name} in {voice_channel.name}.")

                # Calculate time until next reminder
                next_reminder_minutes = self.settings.get('check_interval_minutes', 30)
                
                # Format the reminder message
                formatted_message = self.settings['reminder_message'].format(
                    user=member,
                    timeout_minutes=self.settings['response_timeout_minutes'],
                    reminder_frequency=self.settings.get('check_interval_minutes', 30)
                )
                
                # Create button view
                view = ActivityCheckButton(user_id, 0, self)  # message_id will be updated
                
                # Send reminder message to voice channel's text chat
                try:
                    message = await voice_channel.send(formatted_message, view=view)
                    view.message_id = message.id  # Update the message ID in the view
                    
                    # Store check data
                    self.pending_checks[guild_id][user_id] = {
                        'message_id': message.id,
                        'message_channel_id': message.channel.id,
                        'original_vc_id': voice_channel.id,
                        'check_start_time': current_time,
                        'move_task': None
                    }
                    
                    # Schedule the move task
                    move_task = asyncio.create_task(
                        self._schedule_move_if_inactive(
                            guild_id, user_id, message.id, 
                            message.channel.id, voice_channel.id
                        )
                    )
                    self.pending_checks[guild_id][user_id]['move_task'] = move_task
                    
                    # Update last check time
                    tracking_data['last_check_time'] = current_time
                    
                    logger.info(f"Sent activity check to {member.display_name} in {voice_channel.name}")
                    
                except nextcord.HTTPException as e:
                    logger.error(f"Failed to send reminder to {voice_channel.name} ({voice_channel.id}): {e}")
                    continue

    @activity_check_loop.before_loop
    async def before_activity_check_loop(self):
        """Wait for bot to be ready before starting the loop."""
        await self.bot.wait_until_ready()

    @tasks.loop(minutes=1.0)  # Monitor voice states every 1 minute1
    async def voice_state_monitor(self):
        """Monitor current voice states to catch any missed join/leave events."""
        current_time = datetime.datetime.now()
        
        for guild in self.bot.guilds:
            guild_id = guild.id
            
            # Initialize guild tracking if not exists
            if guild_id not in self.user_voice_tracking:
                self.user_voice_tracking[guild_id] = {}
            
            # Get all current voice members
            current_voice_members = set()
            for voice_channel in guild.voice_channels:
                for member in voice_channel.members:
                    if not member.bot:
                        current_voice_members.add((member.id, voice_channel.id))
            
            # Get tracked members
            tracked_members = set((user_id, data['vc_id']) for user_id, data in self.user_voice_tracking[guild_id].items())
            
            # Find members who joined but aren't tracked
            new_members = current_voice_members - tracked_members
            for user_id, channel_id in new_members:
                self.user_voice_tracking[guild_id][user_id] = {
                    'join_time': current_time,
                    'last_check_time': current_time,
                    'vc_id': channel_id
                }
                member = guild.get_member(user_id)
                channel = guild.get_channel(channel_id)
                logger.info(f"Caught missed join: {member.display_name if member else user_id} in {channel.name if channel else channel_id}")
            
            # Find members who left but are still tracked
            left_members = tracked_members - current_voice_members
            for user_id, _ in left_members:
                if user_id in self.user_voice_tracking[guild_id]:
                    del self.user_voice_tracking[guild_id][user_id]
                    member = guild.get_member(user_id)
                    logger.info(f"Caught missed leave: {member.display_name if member else user_id}")
                
                # Cancel any pending checks
                if guild_id in self.pending_checks and user_id in self.pending_checks[guild_id]:
                    move_task = self.pending_checks[guild_id][user_id].get('move_task')
                    if move_task:
                        move_task.cancel()
                    del self.pending_checks[guild_id][user_id]

    @voice_state_monitor.before_loop
    async def before_voice_state_monitor(self):
        """Wait for bot to be ready before starting the monitor."""
        await self.bot.wait_until_ready()

    @tasks.loop(minutes=10)
    async def cleanup_pending_checks_loop(self):
        """Clean up old pending checks that might be left over."""
        current_time = datetime.datetime.now()
        cleanup_threshold = 3 * self.settings['response_timeout_minutes'] * 60  # 3x timeout in seconds
        
        for guild_id in list(self.pending_checks.keys()):
            for user_id in list(self.pending_checks[guild_id].keys()):
                check_data = self.pending_checks[guild_id][user_id]
                time_diff = (current_time - check_data['check_start_time']).total_seconds()
                
                if time_diff > cleanup_threshold:
                    # Cancel move task if it exists
                    if check_data.get('move_task'):
                        check_data['move_task'].cancel()
                    
                    # Remove from pending checks
                    del self.pending_checks[guild_id][user_id]
                    logger.info(f"Cleaned up old pending check for user {user_id} in guild {guild_id}")

    def _is_human_member(self, member: nextcord.Member) -> bool:
        """Check if a member is human (not a bot)."""
        return not member.bot

    def _is_user_eligible_for_check(self, member: nextcord.Member) -> bool:
        """Check if a user is eligible for activity checking."""
        if not self._is_human_member(member):
            return False
        
        if not member.voice:
            return False
        
        # Check if user is muted or deafened
        if member.voice.self_mute or member.voice.mute:
            return False
        if member.voice.self_deaf or member.voice.deaf:
            return False
        
        # Skip users who are sharing screen or video (considered active)
        if member.voice.self_video or member.voice.self_stream:
            return False
        
        return True

    def _get_eligible_users_in_channel(self, channel: nextcord.VoiceChannel) -> list[nextcord.Member]:
        """Get eligible users in a voice channel for activity checking."""
        eligible_users = []
        
        # Filter channel members - count all unmuted/undeafened users for the 2-user minimum
        for member in channel.members:
            if not self._is_human_member(member):
                continue
            if not member.voice:
                continue
            # Only check mute/deaf status for counting toward minimum
            if member.voice.self_mute or member.voice.mute:
                continue
            if member.voice.self_deaf or member.voice.deaf:
                continue
            
            eligible_users.append(member)
        
        return eligible_users

    async def _schedule_move_if_inactive(self, guild_id: int, user_id: int, 
                                       reminder_message_id: int, reminder_channel_id: int, 
                                       original_vc_id: int):
        """Schedule moving a user if they don't respond to activity check."""
        # Wait for the timeout period
        await asyncio.sleep(self.settings['response_timeout_minutes'] * 60)
        
        # Get the reminder channel and message
        reminder_channel = self.bot.get_channel(reminder_channel_id)
        message_to_delete = None
        
        if reminder_channel:
            try:
                message_to_delete = await reminder_channel.fetch_message(reminder_message_id)
            except nextcord.NotFound:
                logger.info(f"Reminder message {reminder_message_id} for user {user_id} already deleted (timeout).")
            except nextcord.Forbidden:
                logger.warning(f"Missing permissions to fetch/delete message {reminder_message_id} in {reminder_channel.name}.")
        
        # Check if user is still in pending checks (meaning they didn't click the button)
        if guild_id in self.pending_checks and user_id in self.pending_checks[guild_id]:
            guild = self.bot.get_guild(guild_id)
            if not guild:
                logger.error(f"Guild {guild_id} not found.")
                return
            
            member = guild.get_member(user_id)
            if not member or not member.voice:
                logger.info(f"Member {user_id} not found or not in voice channel.")
                # Clean up pending check and tracking
                if guild_id in self.pending_checks and user_id in self.pending_checks[guild_id]:
                    del self.pending_checks[guild_id][user_id]
                if guild_id in self.user_voice_tracking and user_id in self.user_voice_tracking[guild_id]:
                    del self.user_voice_tracking[guild_id][user_id]
                return
            
            # Get AFK channel
            afk_channel_id = self.settings['afk_channel_id']
            if not afk_channel_id:
                logger.info("No AFK channel configured, skipping move.")
                # Clean up pending check
                if guild_id in self.pending_checks and user_id in self.pending_checks[guild_id]:
                    del self.pending_checks[guild_id][user_id]
                return
            
            afk_channel = self.bot.get_channel(afk_channel_id)
            if not afk_channel or not isinstance(afk_channel, nextcord.VoiceChannel):
                logger.error(f"AFK channel {afk_channel_id} not found or not a voice channel.")
                # Clean up pending check
                if guild_id in self.pending_checks and user_id in self.pending_checks[guild_id]:
                    del self.pending_checks[guild_id][user_id]
                return
            
            # Move user to AFK channel
            try:
                await member.move_to(afk_channel)

                # Assign inactive role
                await self._assign_inactive_role(member)
                
                # Get original voice channel for reference
                original_vc = self.bot.get_channel(original_vc_id)
                
                # Send message to AFK channel
                await afk_channel.send(
                    f"{member.mention}, you have been moved here due to inactivity in "
                    f"{original_vc.mention if original_vc else 'an unknown channel'}."
                )
                
                logger.info(f"Moved {member.display_name} to AFK channel {afk_channel.name} due to inactivity.")
                
                # Remove from voice tracking since they were moved
                if guild_id in self.user_voice_tracking and user_id in self.user_voice_tracking[guild_id]:
                    del self.user_voice_tracking[guild_id][user_id]
                
            except nextcord.Forbidden:
                logger.warning(f"Missing permissions to move member {member.display_name} to {afk_channel.name}.")
            except nextcord.HTTPException as e:
                logger.error(f"Error moving {member.display_name}: {e}")
        
        # Delete reminder message (cleanup)
        if message_to_delete:
            try:
                await message_to_delete.delete()
            except nextcord.Forbidden:
                logger.warning(f"Missing permissions to delete message {reminder_message_id} (fallback).")
            except nextcord.NotFound:
                logger.info(f"Reminder message {reminder_message_id} already deleted (fallback).")
        
        # Remove from pending checks
        if guild_id in self.pending_checks and user_id in self.pending_checks[guild_id]:
            del self.pending_checks[guild_id][user_id]

    async def mark_user_active(self, user_id: int, message_id: int, guild_id: int):
        """Mark a user as active when they click the button."""
        current_time = datetime.datetime.now()
        
        if guild_id in self.pending_checks and user_id in self.pending_checks[guild_id]:
            # Cancel the move task
            move_task = self.pending_checks[guild_id][user_id].get('move_task')
            if move_task:
                move_task.cancel()
            
            # Remove from pending checks
            del self.pending_checks[guild_id][user_id]
            logger.info(f"User {user_id} confirmed activity.")
        
        # Update their last check time in voice tracking
        if guild_id in self.user_voice_tracking and user_id in self.user_voice_tracking[guild_id]:
            self.user_voice_tracking[guild_id][user_id]['last_check_time'] = current_time

    async def _assign_inactive_role(self, member: nextcord.Member):
        """Assign the inactive role to a member and schedule its removal if configured."""
        inactive_role_id = self.settings.get('inactive_role_id')
        if not inactive_role_id:
            return
        
        inactive_role = member.guild.get_role(inactive_role_id)
        if not inactive_role:
            logger.warning(f"Inactive role {inactive_role_id} not found in guild {member.guild.id}")
            return
        
        try:
            await member.add_roles(inactive_role, reason="Failed activity check")
            logger.info(f"Added inactive role to {member.display_name}")
            
            # Schedule role removal if duration is set
            duration_minutes = self.settings.get('inactive_role_duration_minutes', 0)
            if duration_minutes > 0:
                asyncio.create_task(self._remove_inactive_role_after_delay(member, inactive_role, duration_minutes))
        
        except nextcord.Forbidden:
            logger.warning(f"Missing permissions to assign inactive role to {member.display_name}")
        except nextcord.HTTPException as e:
            logger.error(f"Error assigning inactive role to {member.display_name}: {e}")

    async def _remove_inactive_role_after_delay(self, member: nextcord.Member, role: nextcord.Role, minutes: int):
        """Remove the inactive role after a specified delay."""
        await asyncio.sleep(minutes * 60)
        
        try:
            if role in member.roles:
                await member.remove_roles(role, reason="Inactive role duration expired")
                logger.info(f"Removed inactive role from {member.display_name} after {minutes} minutes")
        except nextcord.Forbidden:
            logger.warning(f"Missing permissions to remove inactive role from {member.display_name}")
        except nextcord.HTTPException as e:
            logger.error(f"Error removing inactive role from {member.display_name}: {e}")
            
    # Slash command group
    @nextcord.slash_command(name="activity", description="Voice activity checker commands")
    async def activity_group(self, interaction: nextcord.Interaction):
        pass

    @activity_group.subcommand(name="toggle", description="Toggle the voice activity check on or off.")
    async def toggle(self, interaction: nextcord.Interaction):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You need 'Manage Server' permissions to use this command.", ephemeral=True)
            return
        
        current_state = self.settings.get('is_enabled', True)
        new_state = not current_state
        
        # Update settings
        self.settings['is_enabled'] = new_state
        update_setting('is_enabled', new_state)
        
        await interaction.response.send_message(
            f"Activity check is now: **{'Enabled' if new_state else 'Disabled'}**",
            ephemeral=True
        )
        logger.info(f"Activity check toggled to: {new_state}")

    @activity_group.subcommand(name="set_reminder_message", description="Set the activity checker reminder message.")
    async def set_reminder_message(self, interaction: nextcord.Interaction, 
                                 message: str = SlashOption(name="message", 
                                                          description="The new reminder message. Use {user.mention}, {timeout_minutes} and {reminder_frequency}.")):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You need 'Manage Server' permissions to use this command.", ephemeral=True)
            return
        
        self.settings['reminder_message'] = message
        update_setting('reminder_message', message)
        
        await interaction.response.send_message(f"Reminder message updated to: {message}", ephemeral=True)

    @activity_group.subcommand(name="set_afk_channel", description="Set the channel to move inactive users to.")
    async def set_afk_channel(self, interaction: nextcord.Interaction,
                            channel: nextcord.VoiceChannel = SlashOption(name="channel", 
                                                                       description="The voice channel to move users to.", 
                                                                       required=False)):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You need 'Manage Server' permissions to use this command.", ephemeral=True)
            return
        
        if channel:
            self.settings['afk_channel_id'] = channel.id
            update_setting('afk_channel_id', channel.id)
            await interaction.response.send_message(f"AFK channel set to: {channel.mention}", ephemeral=True)
        else:
            self.settings['afk_channel_id'] = None
            update_setting('afk_channel_id', None)
            await interaction.response.send_message("AFK channel disabled.", ephemeral=True)

    @activity_group.subcommand(name="set_response_timeout", description="Set the time in minutes users have to respond.")
    async def set_response_timeout(self, interaction: nextcord.Interaction,
                                 minutes: int = SlashOption(name="minutes", 
                                                          description="Number of minutes (minimum 1).", 
                                                          min_value=1)):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You need 'Manage Server' permissions to use this command.", ephemeral=True)
            return
        
        self.settings['response_timeout_minutes'] = minutes
        update_setting('response_timeout_minutes', minutes)
        
        await interaction.response.send_message(f"Response timeout set to: {minutes} minutes", ephemeral=True)

    @activity_group.subcommand(name="exclude_channel", description="Manage channels excluded from activity checks.")
    async def exclude_channel_group(self, interaction: nextcord.Interaction):
        pass

    @exclude_channel_group.subcommand(name="add", description="Add a channel to the exclusion list.")
    async def exclude_add(self, interaction: nextcord.Interaction,
                         channel: nextcord.VoiceChannel = SlashOption(name="channel", 
                                                                     description="The voice channel to exclude.")):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You need 'Manage Server' permissions to use this command.", ephemeral=True)
            return
        
        add_excluded_channel(channel.id)
        self.settings = get_settings()  # Reload settings
        
        await interaction.response.send_message(f"Added {channel.mention} to excluded channels.", ephemeral=True)

    @exclude_channel_group.subcommand(name="remove", description="Remove a channel from the exclusion list.")
    async def exclude_remove(self, interaction: nextcord.Interaction,
                           channel: nextcord.VoiceChannel = SlashOption(name="channel", 
                                                                       description="The voice channel to include again.")):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You need 'Manage Server' permissions to use this command.", ephemeral=True)
            return
        
        remove_excluded_channel(channel.id)
        self.settings = get_settings()  # Reload settings
        
        await interaction.response.send_message(f"Removed {channel.mention} from excluded channels.", ephemeral=True)

    @activity_group.subcommand(name="show_settings", description="Display current activity checker settings.")
    async def show_settings(self, interaction: nextcord.Interaction):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You need 'Manage Server' permissions to use this command.", ephemeral=True)
            return
        
        # Reload settings to ensure they're current
        self.settings = get_settings()
        
        embed = nextcord.Embed(title="Activity Checker Settings", color=0x00ff00)
        
        embed.add_field(name="Enabled", value="✅ Yes" if self.settings['is_enabled'] else "❌ No", inline=True)
        embed.add_field(name="Response Timeout", value=f"{self.settings['response_timeout_minutes']} minutes", inline=True)
        
        # AFK Channel
        afk_channel_id = self.settings['afk_channel_id']
        if afk_channel_id:
            afk_channel = self.bot.get_channel(afk_channel_id)
            afk_channel_text = afk_channel.mention if afk_channel else f"Unknown Channel ({afk_channel_id})"
        else:
            afk_channel_text = "Not set"
        embed.add_field(name="AFK Channel", value=afk_channel_text, inline=True)

        # Inactive Role
        inactive_role_id = self.settings.get('inactive_role_id')
        if inactive_role_id:
            inactive_role = interaction.guild.get_role(inactive_role_id)
            inactive_role_text = inactive_role.mention if inactive_role else f"Unknown Role ({inactive_role_id})"
        else:
            inactive_role_text = "Not set"
        embed.add_field(name="Inactive Role", value=inactive_role_text, inline=True)
        
        # Role Duration
        duration_minutes = self.settings.get('inactive_role_duration_minutes', 0)
        duration_text = f"{duration_minutes} minutes" if duration_minutes > 0 else "Permanent"
        embed.add_field(name="Role Duration", value=duration_text, inline=True)
        
        # Excluded Channels
        excluded_channels = self.settings['excluded_channels']
        if excluded_channels:
            excluded_text = "\n".join([
                f"<#{channel_id}>" for channel_id in excluded_channels[:10]  # Limit to 10 for display
            ])
            if len(excluded_channels) > 10:
                excluded_text += f"\n... and {len(excluded_channels) - 10} more"
        else:
            excluded_text = "None"
        embed.add_field(name="Excluded Channels", value=excluded_text, inline=False)
        
        # Reminder Message
        embed.add_field(name="Reminder Message", value=self.settings['reminder_message'][:1000], inline=False)
        
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @activity_group.subcommand(name="force_check", description="Manually trigger an immediate activity check.")
    async def force_check(self, interaction: nextcord.Interaction):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You need 'Manage Server' permissions to use this command.", ephemeral=True)
            return
        
        await interaction.response.send_message("Triggering immediate activity check...", ephemeral=True)
        
        # Manually trigger the check loop, bypassing reminder frequency
        try:
            self.settings = get_settings()
            current_time = datetime.datetime.now()
            logger.info("Running force_check: bypassing reminder frequency.")

            for guild_id, users in self.user_voice_tracking.items():
                guild = self.bot.get_guild(guild_id)
                if not guild:
                    logger.warning(f"Guild {guild_id} not found.")
                    continue

                if guild_id not in self.pending_checks:
                    self.pending_checks[guild_id] = {}

                for user_id, tracking_data in list(users.items()):
                    logger.info(f"[force_check] Checking user {user_id} in guild {guild_id}...")

                    if user_id in self.pending_checks[guild_id]:
                        logger.info(f"[force_check] User {user_id} already has a pending check.")
                        continue

                    member = guild.get_member(user_id)
                    if not member or not member.voice:
                        logger.info(f"[force_check] User {user_id} not found or not in voice. Removing from tracking.")
                        del users[user_id]
                        continue

                    voice_channel = member.voice.channel

                    afk_channel_id = self.settings.get('afk_channel_id')
                    if afk_channel_id and voice_channel.id == afk_channel_id:
                        logger.info(f"[force_check] Skipping reminder for {member.display_name} in AFK channel.")
                        tracking_data['last_check_time'] = current_time
                        continue

                    if voice_channel.id in self.settings['excluded_channels']:
                        logger.info(f"[force_check] Voice channel {voice_channel.id} is excluded.")
                        tracking_data['last_check_time'] = current_time
                        continue

                    if not self._is_user_eligible_for_check(member):
                        logger.info(f"[force_check] User {user_id} is not eligible for check.")
                        tracking_data['last_check_time'] = current_time
                        continue

                    eligible_users = self._get_eligible_users_in_channel(voice_channel)
                    if len(eligible_users) < 2:
                        logger.info(f"[force_check] Not enough eligible users in channel {voice_channel.id}.")
                        tracking_data['last_check_time'] = current_time
                        continue

                    # If you reach here, a reminder should be sent
                    logger.info(f"[force_check] Sending reminder to {member.display_name} in {voice_channel.name}.")

                    next_reminder_minutes = self.settings.get('check_interval_minutes', 30)
                    formatted_message = self.settings['reminder_message'].format(
                        user=member,
                        timeout_minutes=self.settings['response_timeout_minutes'],
                        reminder_frequency=next_reminder_minutes
                    )

                    view = ActivityCheckButton(user_id, 0, self)  # message_id will be updated

                    try:
                        message = await voice_channel.send(formatted_message, view=view)
                        view.message_id = message.id

                        self.pending_checks[guild_id][user_id] = {
                            'message_id': message.id,
                            'message_channel_id': message.channel.id,
                            'original_vc_id': voice_channel.id,
                            'check_start_time': current_time,
                            'move_task': None
                        }

                        move_task = asyncio.create_task(
                            self._schedule_move_if_inactive(
                                guild_id, user_id, message.id,
                                message.channel.id, voice_channel.id
                            )
                        )
                        self.pending_checks[guild_id][user_id]['move_task'] = move_task

                        # Update last check time
                        tracking_data['last_check_time'] = current_time

                        logger.info(f"[force_check] Sent activity check to {member.display_name} in {voice_channel.name}")

                    except nextcord.HTTPException as e:
                        logger.error(f"[force_check] Failed to send reminder to {voice_channel.name} ({voice_channel.id}): {e}")
                        continue

            await interaction.followup.send("Activity check completed!", ephemeral=True)
        except Exception as e:
            logger.error(f"Error during manual activity check: {e}")
            await interaction.followup.send(f"Error during activity check: {str(e)}", ephemeral=True)

    @activity_group.subcommand(name="set_inactive_role", description="Set the role to assign to users who fail activity checks.")
    async def set_inactive_role(self, interaction: nextcord.Interaction,
                            role: nextcord.Role = SlashOption(name="role", 
                                                            description="The role to assign to inactive users.", 
                                                            required=False)):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You need 'Manage Server' permissions to use this command.", ephemeral=True)
            return
        
        if role:
            self.settings['inactive_role_id'] = role.id
            update_setting('inactive_role_id', role.id)
            await interaction.response.send_message(f"Inactive role set to: {role.mention}", ephemeral=True)
        else:
            self.settings['inactive_role_id'] = None
            update_setting('inactive_role_id', None)
            await interaction.response.send_message("Inactive role disabled.", ephemeral=True)

    @activity_group.subcommand(name="set_role_duration", description="Set how long the inactive role stays on users (in minutes).")
    async def set_role_duration(self, interaction: nextcord.Interaction,
                            minutes: int = SlashOption(name="minutes", 
                                                        description="Number of minutes (0 for permanent until manual removal).", 
                                                        min_value=0)):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You need 'Manage Server' permissions to use this command.", ephemeral=True)
            return
        
        self.settings['inactive_role_duration_minutes'] = minutes
        update_setting('inactive_role_duration_minutes', minutes)
        
        duration_text = f"{minutes} minutes" if minutes > 0 else "permanent (until manual removal)"
        await interaction.response.send_message(f"Inactive role duration set to: {duration_text}", ephemeral=True)
        
def setup(bot):
    """Add the ActivityChecker cog to the bot."""
    bot.add_cog(ActivityChecker(bot))