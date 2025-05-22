import nextcord
from nextcord.ext import commands, application_checks
from nextcord import Interaction, SlashOption, ChannelType, TextChannel, ForumChannel # Added ForumChannel
import database # Ensure this is found (root directory)
import logging
import sqlite3 # For the error handler

DEFAULT_DELETE_DELAY_DAYS_FOR_DISPLAY = 7
MAX_DELETE_DELAY_DAYS = 30 
MIN_DELETE_DELAY_DAYS = 0 

class ConfigCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @nextcord.slash_command(name="config", description="Configure bot settings.")
    async def config_group(self, interaction: Interaction):
        pass # Base command for subcommands

    @config_group.subcommand(name="set_scan_interval", description="Sets how often the bot checks archived threads.")
    @application_checks.has_permissions(manage_guild=True)
    async def set_scan_interval(self, interaction: Interaction, minutes: int = SlashOption(description="Interval in minutes", required=True)):
        await interaction.response.defer(ephemeral=True)
        if minutes <= 0:
            await interaction.followup.send("Scan interval must be a positive number of minutes.", ephemeral=True)
            return
        database.update_setting(interaction.guild.id, 'scan_interval_minutes', minutes)
        await interaction.followup.send(f"Scan interval set to {minutes} minutes.", ephemeral=True)
        logging.info(f"Scan interval set to {minutes} for guild {interaction.guild.id} by {interaction.user.name}")

    @config_group.subcommand(name="set_delete_delay", description=f"Sets how long after a ticket is closed it should be deleted ({MIN_DELETE_DELAY_DAYS}-{MAX_DELETE_DELAY_DAYS} days).")
    @application_checks.has_permissions(manage_guild=True)
    async def set_delete_delay(self, interaction: Interaction, days: int = SlashOption(description=f"Delay in days (Min: {MIN_DELETE_DELAY_DAYS}, Max: {MAX_DELETE_DELAY_DAYS})", required=True)):
        await interaction.response.defer(ephemeral=True)
        if not (MIN_DELETE_DELAY_DAYS <= days <= MAX_DELETE_DELAY_DAYS):
            await interaction.followup.send(
                f"Delete delay must be between {MIN_DELETE_DELAY_DAYS} and {MAX_DELETE_DELAY_DAYS} days.",
                ephemeral=True
            )
            return
        database.update_setting(interaction.guild.id, 'delete_delay_days', days)
        await interaction.followup.send(f"Delete delay set to {days} day(s).", ephemeral=True)
        logging.info(f"Delete delay set to {days} days for guild {interaction.guild.id} by {interaction.user.name}")

    @config_group.subcommand(name="set_log_channel", description="Designates a channel for bot action logs.")
    @application_checks.has_permissions(manage_guild=True)
    async def set_log_channel(self, interaction: Interaction, channel: nextcord.abc.GuildChannel = SlashOption(channel_types=[ChannelType.text], description="The channel to send logs to", required=True)):
        await interaction.response.defer(ephemeral=True)
        database.update_setting(interaction.guild.id, 'log_channel_id', channel.id)
        await interaction.followup.send(f"Log channel set to {channel.mention}.", ephemeral=True)
        logging.info(f"Log channel set to {channel.id} for guild {interaction.guild.id} by {interaction.user.name}")

    @config_group.subcommand(name="add_monitored_channel", description="Adds a text or forum channel for the bot to scan threads within.")
    @application_checks.has_permissions(manage_guild=True)
    async def add_monitored_channel(self, 
                                    interaction: Interaction, 
                                    channel_to_monitor: nextcord.abc.GuildChannel = SlashOption(
                                        description="The text or forum channel to monitor",
                                        channel_types=[ChannelType.text, ChannelType.forum], # Accepts Text or Forum
                                        required=True
                                    )):
        await interaction.response.defer(ephemeral=True)
        
        if not isinstance(channel_to_monitor, (TextChannel, ForumChannel)):
             await interaction.followup.send(f"'{channel_to_monitor.name}' is not a valid text or forum channel.", ephemeral=True)
             return
            
        if database.add_monitored_channel(interaction.guild.id, channel_to_monitor.id):
            await interaction.followup.send(f"Channel {channel_to_monitor.mention} (`{channel_to_monitor.name}`) will now be monitored.", ephemeral=True, suppress_embeds=True)
            logging.info(f"Added monitored channel {channel_to_monitor.id} ('{channel_to_monitor.name}') type: {type(channel_to_monitor).__name__} for guild {interaction.guild.id} by {interaction.user.name}")
        else:
            await interaction.followup.send(f"Channel {channel_to_monitor.mention} (`{channel_to_monitor.name}`) is already being monitored.", ephemeral=True, suppress_embeds=True)

    @config_group.subcommand(name="remove_monitored_channel", description="Removes a text or forum channel from monitoring.")
    @application_checks.has_permissions(manage_guild=True)
    async def remove_monitored_channel(self, interaction: Interaction, channel_id_to_remove: str = SlashOption(description="The ID of the channel to remove", required=True)):
        await interaction.response.defer(ephemeral=True)
        try:
            chan_id = int(channel_id_to_remove)
            channel_obj = self.bot.get_channel(chan_id) # Try to get object for name
            channel_name_mention = channel_obj.mention if channel_obj else f"ID `{chan_id}`"
            channel_name_log = channel_obj.name if channel_obj else f"ID {chan_id}"


            if database.remove_monitored_channel(interaction.guild.id, chan_id):
                await interaction.followup.send(f"Channel {channel_name_mention} will no longer be monitored.", ephemeral=True, suppress_embeds=True)
                logging.info(f"Removed monitored channel {chan_id} ('{channel_name_log}') for guild {interaction.guild.id} by {interaction.user.name}")
            else:
                await interaction.followup.send(f"Channel {channel_name_mention} was not found in the monitored list.", ephemeral=True, suppress_embeds=True)
        except ValueError:
            await interaction.followup.send(f"'{channel_id_to_remove}' is not a valid channel ID format.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"An error occurred: {e}", ephemeral=True)
            logging.error(f"Error removing monitored channel: {e}", exc_info=True)

    @config_group.subcommand(name="view_settings", description="Displays current bot configurations for this server.")
    @application_checks.has_permissions(manage_guild=True)
    async def view_settings(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True) 
        
        settings = database.get_guild_settings(interaction.guild.id)
        monitored_channel_ids = database.get_monitored_channels(interaction.guild.id)

        embed = nextcord.Embed(title=f"Bot Configuration for {interaction.guild.name}", color=nextcord.Color.blue())

        if not settings and not monitored_channel_ids:
             embed.description = "No settings configured for this server yet. Use `/config set_...` commands.\nCurrently monitoring all accessible text and forum channels for threads."
             await interaction.followup.send(embed=embed, ephemeral=True)
             return
        
        settings = settings or {} 

        embed.add_field(name="Scan Interval", value=f"{settings.get('scan_interval_minutes', 'Not Set (Defaults to 60)')} minutes", inline=False)
        
        delete_days_val = settings.get('delete_delay_days', DEFAULT_DELETE_DELAY_DAYS_FOR_DISPLAY)
        embed.add_field(name="Delete Delay", value=f"{delete_days_val} day(s)", inline=False)
        
        log_channel_id_val = settings.get('log_channel_id')
        log_channel_obj = interaction.guild.get_channel(log_channel_id_val) if log_channel_id_val else None
        embed.add_field(name="Log Channel", value=log_channel_obj.mention if log_channel_obj else "Not Set", inline=False)

        if monitored_channel_ids:
            channel_mentions = []
            for chan_id in monitored_channel_ids:
                chan = self.bot.get_channel(chan_id)
                channel_mentions.append(f"{chan.mention} (`{chan.name}`)" if chan else f"Unknown Channel (ID: {chan_id})")
            embed.add_field(name="Monitored Channels", value="\n".join(channel_mentions) if channel_mentions else "None Set", inline=False)
        else:
            embed.add_field(name="Monitored Channels", value="None (Scanning all accessible text and forum channels)", inline=False)
        
        await interaction.followup.send(embed=embed, ephemeral=True)

    @set_scan_interval.error
    @set_delete_delay.error
    @set_log_channel.error
    @add_monitored_channel.error 
    @remove_monitored_channel.error 
    @view_settings.error
    async def config_command_error(self, interaction: Interaction, error):
        if not interaction.response.is_done():
            try:
                await interaction.response.defer(ephemeral=True)
                send_method = interaction.followup.send
            except nextcord.NotFound: # Interaction already expired
                logging.warning(f"Interaction expired before error handler could defer for user {interaction.user.id}. Error: {error}")
                return 
        else:
            send_method = interaction.followup.send

        if isinstance(error, application_checks.ApplicationMissingPermissions):
            await send_method("You do not have the `Manage Guild` permission to use this command.", ephemeral=True)
        else:
            original_error = getattr(error, 'original', error) 
            if isinstance(original_error, sqlite3.OperationalError) and "no such column" in str(original_error).lower():
                await send_method(
                    "Error: Database schema mismatch. Admin may need to delete the `.db` file and reconfigure.",
                    ephemeral=True
                )
                logging.error(f"Database schema error: {original_error}", exc_info=True)
            elif isinstance(error, nextcord.errors.NotFound) and error.code == 10062:
                 logging.warning(f"Caught 'Unknown Interaction' error for user {interaction.user.id} in config_command_error. Error: {error}")
            else:
                await send_method(f"An unexpected error occurred: {error}", ephemeral=True)
                logging.error(f"Error in config command for user {interaction.user.id}: {error}", exc_info=True)

def setup(bot):
    bot.add_cog(ConfigCog(bot))