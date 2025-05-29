import nextcord
from nextcord.ext import commands, application_checks
from nextcord import Interaction, SlashOption, ChannelType, TextChannel, ForumChannel
from db_utils import database # <<< CORRECTED IMPORT
import logging
import sqlite3 

# Define constants used in this cog for display defaults if settings are not yet in DB
DEFAULT_DELETE_DELAY_DAYS_FOR_DISPLAY = 7
DEFAULT_SCAN_INTERVAL_MINUTES_FOR_DISPLAY = 60 
MAX_DELETE_DELAY_DAYS = 30 # Example maximum
MIN_DELETE_DELAY_DAYS = 0  # Allow 0 for near-immediate deletion

class ConfigCog(commands.Cog, name="Bot Configuration"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_check(self, interaction: Interaction) -> bool:
        # This check applies to all commands in this cog for single-server operation
        if not self.bot.target_guild_id: 
            if not interaction.response.is_done():
                try: await interaction.response.defer(ephemeral=True)
                except nextcord.NotFound: pass # Interaction might have already expired if bot was slow
            await interaction.followup.send("Bot is not yet ready or target server not identified. Please wait a moment and try again.", ephemeral=True)
            return False
        if interaction.guild is None or interaction.guild.id != self.bot.target_guild_id:
            if not interaction.response.is_done():
                try: await interaction.response.defer(ephemeral=True)
                except nextcord.NotFound: pass
            target_guild_name = getattr(self.bot, 'target_guild_name', 'the configured server')
            await interaction.followup.send(f"This bot is configured for a specific server. Please use commands in '{target_guild_name}'.", ephemeral=True)
            return False
        return True

    @nextcord.slash_command(name="config", description="Configure general bot settings.")
    async def config_group(self, interaction: Interaction):
        pass 

    @config_group.subcommand(name="set_scan_interval", description="Sets how often Ticket Manager checks archived threads.")
    @application_checks.has_permissions(manage_guild=True)
    async def set_scan_interval(self, interaction: Interaction, minutes: int = SlashOption(description="Interval in minutes", required=True)):
        await interaction.response.defer(ephemeral=True)
        if minutes <= 0:
            await interaction.followup.send("Scan interval must be a positive number of minutes.", ephemeral=True)
            return
        database.update_setting(self.bot.target_guild_id, 'scan_interval_minutes', minutes)
        await interaction.followup.send(f"Ticket Manager scan interval set to {minutes} minutes.", ephemeral=True)
        logging.info(f"Scan interval set to {minutes} for target guild {self.bot.target_guild_id} by {interaction.user.name}")

    @config_group.subcommand(name="set_delete_delay", description=f"Sets days after ticket closure for deletion ({MIN_DELETE_DELAY_DAYS}-{MAX_DELETE_DELAY_DAYS} days).")
    @application_checks.has_permissions(manage_guild=True)
    async def set_delete_delay(self, interaction: Interaction, days: int = SlashOption(description=f"Delay in days (Min: {MIN_DELETE_DELAY_DAYS}, Max: {MAX_DELETE_DELAY_DAYS})", required=True)):
        await interaction.response.defer(ephemeral=True)
        if not (MIN_DELETE_DELAY_DAYS <= days <= MAX_DELETE_DELAY_DAYS):
            await interaction.followup.send(
                f"Delete delay for tickets must be between {MIN_DELETE_DELAY_DAYS} and {MAX_DELETE_DELAY_DAYS} days.",
                ephemeral=True
            )
            return
        database.update_setting(self.bot.target_guild_id, 'delete_delay_days', days)
        await interaction.followup.send(f"Ticket delete delay set to {days} day(s).", ephemeral=True)
        logging.info(f"Delete delay set to {days} for target guild {self.bot.target_guild_id} by {interaction.user.name}")

    @config_group.subcommand(name="set_main_log_channel", description="Designates the main log channel (e.g., for Ticket Manager actions).")
    @application_checks.has_permissions(manage_guild=True)
    async def set_main_log_channel(self, interaction: Interaction, channel: TextChannel = SlashOption(description="The text channel for main bot logs", required=True)):
        await interaction.response.defer(ephemeral=True)
        database.update_setting(self.bot.target_guild_id, 'log_channel_id', channel.id)
        await interaction.followup.send(f"Main bot log channel set to: {channel.mention}.", ephemeral=True)
        logging.info(f"Main log channel set to {channel.id} for target guild {self.bot.target_guild_id} by {interaction.user.name}")

    @config_group.subcommand(name="set_announcement_log_channel", description="Designates a specific log channel for the Announcement cog.")
    @application_checks.has_permissions(manage_guild=True)
    async def set_announcement_log_channel(self, interaction: Interaction, channel: TextChannel = SlashOption(description="The text channel for announcement cog logs", required=True)):
        await interaction.response.defer(ephemeral=True)
        database.update_setting(self.bot.target_guild_id, 'announcement_log_channel_id', channel.id)
        await interaction.followup.send(f"Announcement cog log channel set to: {channel.mention}.", ephemeral=True)
        logging.info(f"Announcement log channel set to {channel.id} for target guild {self.bot.target_guild_id} by {interaction.user.name}")
        
        announcement_cog = self.bot.get_cog("Announcements") 
        if announcement_cog and hasattr(announcement_cog, '_load_config'):
             if callable(getattr(announcement_cog, '_load_config', None)):
                await announcement_cog._load_config(self.bot.target_guild_id)

    @config_group.subcommand(name="add_monitored_channel", description="Adds a text/forum channel for Ticket Manager to scan threads in.")
    @application_checks.has_permissions(manage_guild=True)
    async def add_monitored_channel(self, 
                                    interaction: Interaction, 
                                    channel_to_monitor: nextcord.abc.GuildChannel = SlashOption(
                                        description="The text or forum channel to monitor",
                                        channel_types=[ChannelType.text, ChannelType.forum], 
                                        required=True
                                    )):
        await interaction.response.defer(ephemeral=True)
        
        if not isinstance(channel_to_monitor, (TextChannel, ForumChannel)):
             await interaction.followup.send(f"'{channel_to_monitor.name}' is not a valid text or forum channel for monitoring threads.", ephemeral=True)
             return
            
        if database.add_monitored_channel(self.bot.target_guild_id, channel_to_monitor.id):
            await interaction.followup.send(f"Channel {channel_to_monitor.mention} (`{channel_to_monitor.name}`) will now be monitored by the Ticket Manager.", ephemeral=True, suppress_embeds=True)
            logging.info(f"Added monitored channel {channel_to_monitor.id} ('{channel_to_monitor.name}') type: {type(channel_to_monitor).__name__} for guild {self.bot.target_guild_id} by {interaction.user.name}")
        else:
            await interaction.followup.send(f"Channel {channel_to_monitor.mention} (`{channel_to_monitor.name}`) is already being monitored by the Ticket Manager.", ephemeral=True, suppress_embeds=True)

    @config_group.subcommand(name="remove_monitored_channel", description="Removes a channel from Ticket Manager's thread scanning list.")
    @application_checks.has_permissions(manage_guild=True)
    async def remove_monitored_channel(self, interaction: Interaction, channel_id_to_remove: str = SlashOption(description="The ID of the channel to remove from monitoring", required=True)):
        await interaction.response.defer(ephemeral=True)
        try:
            chan_id = int(channel_id_to_remove)
            channel_obj = self.bot.get_channel(chan_id) 
            channel_name_mention = channel_obj.mention if channel_obj else f"ID `{chan_id}`"
            channel_name_log = channel_obj.name if channel_obj else f"ID {chan_id}"

            if database.remove_monitored_channel(self.bot.target_guild_id, chan_id):
                await interaction.followup.send(f"Channel {channel_name_mention} will no longer be monitored by the Ticket Manager.", ephemeral=True, suppress_embeds=True)
                logging.info(f"Removed monitored channel {chan_id} ('{channel_name_log}') for guild {self.bot.target_guild_id} by {interaction.user.name}")
            else:
                await interaction.followup.send(f"Channel {channel_name_mention} was not found in the Ticket Manager's monitored list.", ephemeral=True, suppress_embeds=True)
        except ValueError:
            await interaction.followup.send(f"'{channel_id_to_remove}' is not a valid channel ID format.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"An error occurred: {e}", ephemeral=True)
            logging.error(f"Error removing monitored channel: {e}", exc_info=True)

    @config_group.subcommand(name="view_settings", description="Displays current general bot configurations.")
    @application_checks.has_permissions(manage_guild=True)
    async def view_settings(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True) 
        
        target_gid = self.bot.target_guild_id
        
        settings = database.get_guild_settings(target_gid)
        monitored_channel_ids = database.get_monitored_channels(target_gid)
        embed = nextcord.Embed(title=f"General Bot Configuration for {interaction.guild.name}", color=nextcord.Color.blue())

        if not settings and not monitored_channel_ids:
             embed.description = "No general settings configured yet. Using defaults where applicable.\nTicket Manager will scan all accessible text and forum channels."
             await interaction.followup.send(embed=embed, ephemeral=True)
             return
        
        settings = settings if settings is not None else {} 

        embed.add_field(name="Scan Interval (Ticket Manager)", value=f"{settings.get('scan_interval_minutes', DEFAULT_SCAN_INTERVAL_MINUTES_FOR_DISPLAY)} minutes", inline=False)
        embed.add_field(name="Delete Delay (Ticket Manager)", value=f"{settings.get('delete_delay_days', DEFAULT_DELETE_DELAY_DAYS_FOR_DISPLAY)} day(s)", inline=False)
        
        main_log_channel_obj = interaction.guild.get_channel(settings.get('log_channel_id')) if settings.get('log_channel_id') else None
        embed.add_field(name="Main Log Channel (e.g., Ticket Manager)", value=main_log_channel_obj.mention if main_log_channel_obj else "Not Set", inline=False)
        
        announcement_log_obj = interaction.guild.get_channel(settings.get('announcement_log_channel_id')) if settings.get('announcement_log_channel_id') else None
        embed.add_field(name="Announcement Log Channel", value=announcement_log_obj.mention if announcement_log_obj else "Not Set", inline=False)
        
        if monitored_channel_ids:
            channel_mentions = []
            for chan_id in monitored_channel_ids:
                chan_obj = interaction.guild.get_channel(chan_id) 
                channel_mentions.append(f"{chan_obj.mention} (`{chan_obj.name}`)" if chan_obj else f"Unknown Channel (ID: {chan_id})")
            embed.add_field(name="Monitored Channels (Ticket Manager)", value="\n".join(channel_mentions) if channel_mentions else "None Set", inline=False)
        else:
            embed.add_field(name="Monitored Channels (Ticket Manager)", value="None (Scanning all text and forum channels)", inline=False)
        
        await interaction.followup.send(embed=embed, ephemeral=True)

    # Error Handler - Decorators on separate lines, correct signature
    @set_scan_interval.error
    @set_delete_delay.error
    @set_main_log_channel.error 
    @set_announcement_log_channel.error
    @add_monitored_channel.error 
    @remove_monitored_channel.error 
    @view_settings.error
    async def config_command_error(self, interaction: Interaction, error): 
        send_method = interaction.followup.send
        if not interaction.response.is_done():
            try: 
                await interaction.response.defer(ephemeral=True)
            except nextcord.NotFound: 
                logging.warning(f"Interaction expired before error handler could defer for user {interaction.user.id}. Error: {error}")
                return 
        
        if isinstance(error, application_checks.ApplicationMissingPermissions): 
            await send_method("You lack `Manage Guild` permission to use this command.", ephemeral=True)
        else:
            original_error = getattr(error, 'original', error) 
            if isinstance(original_error, sqlite3.OperationalError) and "no such column" in str(original_error).lower():
                await send_method("Database schema error. The bot admin may need to delete the `.db` file and reconfigure settings after restarting the bot.", ephemeral=True)
                logging.error(f"Database schema error: {original_error}", exc_info=True)
            elif isinstance(error, nextcord.errors.NotFound) and error.code == 10062: 
                 logging.warning(f"Caught 'Unknown Interaction' in config_command_error for user {interaction.user.id}. Original error: {error}")
            else:
                await send_method(f"An unexpected error occurred in a config command: {type(error).__name__}", ephemeral=True)
                logging.error(f"Error in config command for user {interaction.user.id}: {error}", exc_info=True)

def setup(bot: commands.Bot):
    bot.add_cog(ConfigCog(bot))