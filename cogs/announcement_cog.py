import nextcord
from nextcord.ext import commands, tasks, application_checks
from nextcord import (
    Interaction, SlashOption, TextChannel, Attachment, 
    Embed, Color, Webhook)
import database
import logging
from datetime import datetime, timezone, timedelta
import json 
from typing import Optional, List, Dict, Union # Added Union for type hint clarity
import pytz 
import asyncio 

MANILA_TZ = pytz.timezone("Asia/Manila")

class AnnouncementCog(commands.Cog, name="Announcements"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.announcement_log_channel_obj: Optional[TextChannel] = None
        # Ensure task is started after bot is ready and target_guild_id is known
        # self.check_scheduled_announcements.start() # Will be started in on_ready or cog_load

    async def cog_load(self):
        """Called when cog is loaded/reloaded."""
        # Wait for bot to be ready to ensure target_guild_id is available
        await self.bot.wait_until_ready() 
        if hasattr(self.bot, 'target_guild_id') and self.bot.target_guild_id:
            await self._load_config(self.bot.target_guild_id)
            if not self.check_scheduled_announcements.is_running():
                self.check_scheduled_announcements.start()
        else:
            logging.warning("AnnouncementCog: Target guild ID not available on bot object at cog_load. Task not started.")

    async def _load_config(self, guild_id: int):
        """Loads configuration for the AnnouncementCog, specifically its log channel."""
        if not guild_id: 
            logging.error("AnnouncementCog: _load_config called with no guild_id.")
            return
        
        guild_settings = database.get_guild_settings(guild_id) 
        log_channel_id = None
        if guild_settings:
            log_channel_id = guild_settings.get('announcement_log_channel_id') 
        
        if log_channel_id:
            self.announcement_log_channel_obj = self.bot.get_channel(log_channel_id)
            if self.announcement_log_channel_obj:
                logging.info(f"AnnouncementCog: Log channel set to '{self.announcement_log_channel_obj.name}' for guild {guild_id}.")
            else:
                logging.warning(f"AnnouncementCog: Announcement log channel ID {log_channel_id} configured but channel not found.")
                self.announcement_log_channel_obj = None 
        else:
            self.announcement_log_channel_obj = None
            logging.info(f"AnnouncementCog: No specific log channel configured for announcements in guild {guild_id}. Logs will go to console.")
        logging.info(f"AnnouncementCog: Configuration loaded for guild {guild_id}.")


    def cog_unload(self):
        self.check_scheduled_announcements.cancel()

    async def _log_announcement_action(self, guild_id: int, title: str, description: str, color: Color = Color.blue()):
        if not self.announcement_log_channel_obj and hasattr(self.bot, 'target_guild_id') and self.bot.target_guild_id == guild_id:
            await self._load_config(guild_id) # Attempt to reload log channel if not set

        if self.announcement_log_channel_obj:
            embed = Embed(title=f"Announcement System: {title}", description=description, color=color)
            timestamp_str = datetime.now(MANILA_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')
            embed.set_footer(text=f"Timestamp (GMT+8): {timestamp_str}")
            try:
                await self.announcement_log_channel_obj.send(embed=embed)
            except nextcord.Forbidden:
                logging.warning(f"AnnouncementCog: Missing perms to send log to its dedicated channel {self.announcement_log_channel_obj.id}")
            except Exception as e:
                logging.error(f"AnnouncementCog: Error sending log to its dedicated channel: {e}", exc_info=True)
        else:
            logging.info(f"AnnouncementCog (Guild {guild_id}): {title} - {description}")

    async def _send_announcement_internal(self, guild_id: int, 
                                          message_content: Optional[str], 
                                          announcement_id: Optional[int] = None,
                                          target_channel_id: Optional[int] = None,
                                          target_webhook_url: Optional[str] = None,
                                          attachment_urls_json: Optional[str] = None,
                                          files_for_now: Optional[List[nextcord.File]] = None):
        guild = self.bot.get_guild(guild_id)
        if not guild:
            logging.error(f"Cannot send announcement {announcement_id or 'NOW'}: Guild {guild_id} not found.")
            if announcement_id: database.update_announcement_status(announcement_id, 2); return

        content_to_send = message_content if message_content else ""
        embeds_to_send = []
        
        urls_from_json = []
        if attachment_urls_json:
            try:
                urls_from_json = json.loads(attachment_urls_json)
                if not isinstance(urls_from_json, list): urls_from_json = [urls_from_json]
                image_extensions = ('.png', '.jpg', '.jpeg', '.gif', '.webp')
                temp_content_urls = []
                for url_str in urls_from_json:
                    if isinstance(url_str, str) and any(url_str.lower().endswith(ext) for ext in image_extensions):
                        if len(embeds_to_send) < 10: 
                            embed = Embed(color=Color.blue()); embed.set_image(url=url_str)
                            embeds_to_send.append(embed)
                        else: temp_content_urls.append(url_str)
                    elif isinstance(url_str, str): temp_content_urls.append(url_str)
                if temp_content_urls: content_to_send = (content_to_send + "\n" + "\n".join(temp_content_urls)).strip()
            except Exception as e: 
                logging.error(f"Error processing attachment_urls for announcement {announcement_id}: {e}")
                content_to_send = (content_to_send + "\n(Error with attachment URLs)").strip()

        if not content_to_send and not embeds_to_send and not (files_for_now or []): # Ensure files_for_now is treated as empty list if None
            logging.warning(f"Announcement {announcement_id or 'NOW'} has no content/embeds/files. Marking as error.")
            if announcement_id: database.update_announcement_status(announcement_id, 2)
            return

        sent_target_description = ""
        try:
            final_files = files_for_now or []
            if target_webhook_url:
                webhook = Webhook.from_url(target_webhook_url, session=self.bot.http._session) 
                await webhook.send(content=content_to_send if content_to_send else None, 
                                   embeds=embeds_to_send or None, 
                                   files=final_files)
                sent_target_description = f"Webhook: `{target_webhook_url[:30]}...`"
            elif target_channel_id:
                channel = guild.get_channel(target_channel_id)
                if not isinstance(channel, TextChannel):
                    logging.error(f"Cannot send to {target_channel_id}: Not a text channel."); raise ValueError("Invalid channel type")
                await channel.send(content=content_to_send if content_to_send else None, 
                                   embeds=embeds_to_send or None, 
                                   files=final_files)
                sent_target_description = f"Channel: {channel.mention}"
            else:
                logging.error(f"No target (channel or webhook) for announcement {announcement_id or 'NOW'}")
                if announcement_id: database.update_announcement_status(announcement_id, 2)
                return

            logging.info(f"Sent announcement {announcement_id or 'NOW'} via {sent_target_description} in guild {guild.id}")
            if announcement_id: database.update_announcement_status(announcement_id, 1)
            await self._log_announcement_action(guild_id, "Announcement Sent", f"To: {sent_target_description}\nID: {announcement_id or 'Immediate'}\nContent: {message_content[:100] if message_content else 'N/A'}{'...' if message_content and len(message_content) > 100 else ''}\nAttachments: {len(final_files) + len(urls_from_json)}", Color.green())

        except Exception as e:
            logging.error(f"Error sending announcement {announcement_id or 'NOW'} to {sent_target_description or 'Unknown Target'}: {e}", exc_info=True)
            if announcement_id: database.update_announcement_status(announcement_id, 2)
            await self._log_announcement_action(guild_id, "Announcement FAILED", f"To: {sent_target_description or 'Unknown Target'} (ID: {announcement_id or 'NOW'})\nReason: {str(e)[:500]}", Color.red())

    @tasks.loop(seconds=45) 
    async def check_scheduled_announcements(self):
        if not self.bot.is_ready() or not hasattr(self.bot, 'target_guild_id') or not self.bot.target_guild_id: 
            return 
        
        guild_id = self.bot.target_guild_id
        now_unix = int(datetime.now(timezone.utc).timestamp())
        due_announcements = database.get_pending_announcements_due(guild_id, now_unix)
        
        if due_announcements: logging.info(f"Found {len(due_announcements)} due announcement(s) for guild {guild_id}.")
        
        for ann in due_announcements:
            logging.info(f"Processing due announcement ID: {ann['id']}")
            await self._send_announcement_internal(
                guild_id=ann['guild_id'],
                message_content=ann['message_content'],
                announcement_id=ann['id'],
                target_channel_id=ann['channel_id'],
                target_webhook_url=ann['webhook_url'],
                attachment_urls_json=ann['attachment_urls']
            )
            await asyncio.sleep(2) 

    @check_scheduled_announcements.before_loop
    async def before_checking_announcements(self):
        await self.bot.wait_until_ready()
        if hasattr(self.bot, 'target_guild_id') and self.bot.target_guild_id:
            await self._load_config(self.bot.target_guild_id)
        logging.info("AnnouncementCog: Scheduled announcements task ready.")

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

    # --- Helper for Webhook Name Autocomplete ---

    async def webhook_name_autocomplete(self, interaction: Interaction, current_input: str) -> List[Dict[str, str]]: # Return type changed
        if not interaction.guild_id or (hasattr(self.bot, 'target_guild_id') and self.bot.target_guild_id and interaction.guild_id != self.bot.target_guild_id):
            return []

        saved_webhooks = database.get_all_saved_webhooks(interaction.guild_id)
        choices = []
        current_input_lower = current_input.lower() if current_input else ""
        for wh in saved_webhooks:
            wh_name_str = str(wh['name']) 
            if not current_input_lower or current_input_lower in wh_name_str.lower(): 
                # Return a dictionary as expected by send_autocomplete
                choices.append({"name": wh_name_str, "value": wh_name_str}) 
            if len(choices) >= 25: 
                break
        return choices

    @nextcord.slash_command(name="announce", description="Announcement commands.")
    @application_checks.has_permissions(manage_messages=True) 
    async def announce_group(self, interaction: Interaction): pass

    @announce_group.subcommand(name="schedule", description="Schedule a new announcement to a channel or saved webhook.")
    async def schedule_announcement(
        self, interaction: Interaction,
        message: str = SlashOption(description="The announcement message content.", required=True),
        unix_timestamp: int = SlashOption(description="UNIX timestamp for when to send.", required=True),
        channel: Optional[TextChannel] = SlashOption(description="Channel to send to (if not using webhook).", required=False),
        webhook_name: Optional[str] = SlashOption(description="Name of a SAVED webhook to use (if not using channel).", required=False, autocomplete=True),
        image_url_1: Optional[str] = SlashOption(description="Optional: URL of a first image to include.", required=False),
        image_url_2: Optional[str] = SlashOption(description="Optional: URL of a second image to include.", required=False)
    ):
        await interaction.response.defer(ephemeral=True)
        actual_webhook_url = None
        if webhook_name:
            webhook_data = database.get_saved_webhook_by_name(interaction.guild.id, webhook_name)
            if not webhook_data: await interaction.followup.send(f"❌ Saved webhook named `{webhook_name}` not found.", ephemeral=True); return
            actual_webhook_url = webhook_data['url']
        
        if not channel and not actual_webhook_url: await interaction.followup.send("❌ Must provide either a channel or a saved webhook name.", ephemeral=True); return
        if channel and actual_webhook_url: logging.warning("Both channel and webhook for schedule; webhook will be used."); channel = None 

        now_utc = datetime.now(timezone.utc); scheduled_dt = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)
        if scheduled_dt <= now_utc: await interaction.followup.send("Scheduled time must be in the future.", ephemeral=True); return
        if len(message) > 1950 and not (image_url_1 or image_url_2): await interaction.followup.send("Message content too long.", ephemeral=True); return

        attachment_urls = [url for url in [image_url_1, image_url_2] if url]
        attachment_urls_json = json.dumps(attachment_urls) if attachment_urls else None
        announcement_id = database.add_scheduled_announcement(interaction.guild.id, message, unix_timestamp, interaction.user.id, channel.id if channel else None, actual_webhook_url, attachment_urls_json)
        if announcement_id:
            target_desc = f"to {channel.mention}" if channel else f"via webhook '{webhook_name}'"
            scheduled_time_discord_format = f"<t:{unix_timestamp}:F>"
            await interaction.followup.send(f"✅ Announcement (ID: `{announcement_id}`) scheduled {target_desc} at {scheduled_time_discord_format}.", ephemeral=True)
            await self._log_announcement_action(interaction.guild.id, "Announcement Scheduled", f"ID: {announcement_id}\nTarget: {target_desc}\nTime: {scheduled_time_discord_format}\nBy: {interaction.user.mention}\nContent: {message[:100]}{'...' if len(message)>100 else ''}\nImage URLs: {len(attachment_urls)}")
        else: await interaction.followup.send("❌ Failed to schedule. Check logs.", ephemeral=True)

    @schedule_announcement.on_autocomplete("webhook_name") 
    async def schedule_webhook_name_autocomplete(self, interaction: Interaction, webhook_name_input: str):
        await interaction.response.send_autocomplete(await self.webhook_name_autocomplete(interaction, webhook_name_input))

    @announce_group.subcommand(name="now", description="Send an announcement immediately to a channel or saved webhook.")
    async def announce_now(
        self, interaction: Interaction,
        message: str = SlashOption(description="The announcement message content.", required=True),
        channel: Optional[TextChannel] = SlashOption(description="Channel to send to (if not using webhook).", required=False),
        webhook_name: Optional[str] = SlashOption(description="Name of a SAVED webhook to use (if not using channel).", required=False, autocomplete=True ),
        attachment_1: Optional[Attachment] = SlashOption(description="Optional: First attachment.", required=False),
        attachment_2: Optional[Attachment] = SlashOption(description="Optional: Second attachment.", required=False)
    ):
        await interaction.response.defer(ephemeral=True) 
        actual_webhook_url = None
        if webhook_name:
            webhook_data = database.get_saved_webhook_by_name(interaction.guild.id, webhook_name)
            if not webhook_data: await interaction.followup.send(f"❌ Saved webhook named `{webhook_name}` not found.", ephemeral=True); return
            actual_webhook_url = webhook_data['url']

        if not channel and not actual_webhook_url: await interaction.followup.send("❌ Must provide either a channel or a saved webhook name.", ephemeral=True); return
        if channel and actual_webhook_url: logging.warning("Both channel and webhook for announce now; webhook will be used."); channel = None

        files_to_send = []
        if attachment_1: files_to_send.append(await attachment_1.to_file())
        if attachment_2: files_to_send.append(await attachment_2.to_file())
        
        await self._send_announcement_internal(interaction.guild.id, message, target_channel_id=channel.id if channel else None, target_webhook_url=actual_webhook_url, files_for_now=files_to_send)
        target_name_log = f"webhook '{webhook_name}'" if actual_webhook_url else f"channel {channel.mention if channel else 'N/A'}"
        await interaction.followup.send(f"📣 Announcement attempt to {target_name_log} processed. Check target or logs.", ephemeral=True)

    @announce_now.on_autocomplete("webhook_name") 
    async def now_webhook_name_autocomplete(self, interaction: Interaction, webhook_name_input: str):
        await interaction.response.send_autocomplete(await self.webhook_name_autocomplete(interaction, webhook_name_input))

    @announce_group.subcommand(name="list", description="List scheduled announcements.")
    async def list_announcements(self, interaction: Interaction, pending_only: bool = SlashOption(description="Show only pending announcements? (Default: True)", default=True, required=False)):
        await interaction.response.defer(ephemeral=True)
        announcements = database.get_all_guild_announcements(interaction.guild.id, pending_only=pending_only)
        if not announcements: await interaction.followup.send(f"No {'pending' if pending_only else 'any'} announcements found.", ephemeral=True); return
        embed = Embed(title=f"{'Pending' if pending_only else 'All'} Scheduled Announcements", color=Color.blue())
        description_parts = []
        for ann in announcements:
            target_desc = ""
            if ann['webhook_url']: 
                # Try to find the name for the URL (reverse lookup, can be slow if many webhooks)
                # For simplicity now, just indicate it's a webhook.
                # A better DB design might store saved_webhook_id in scheduled_announcements.
                target_desc = f"Webhook (URL used)" 
            elif ann['channel_id']: 
                chan_obj = self.bot.get_channel(ann['channel_id']); target_desc = chan_obj.mention if chan_obj else f"Channel ID {ann['channel_id']}"
            else: target_desc = "Unknown Target"
            time_str = f"<t:{ann['unix_timestamp_to_send']}:F>"; status_map = {0: "Pending ⏳", 1: "Sent ✅", 2: "Error ❌"}
            status_str = status_map.get(ann['sent_status'], "Unknown"); num_attachments = 0
            if ann['attachment_urls']:
                try: num_attachments = len(json.loads(ann['attachment_urls']))
                except: pass
            entry = (f"**ID: {ann['id']}** | {status_str}\n  Target: {target_desc}\n  Scheduled: {time_str}\n"
                     f"  Content: \"{ann['message_content'][:50]}{'...' if ann['message_content'] and len(ann['message_content']) > 50 else ''}\"\n  Image URLs: {num_attachments}")
            description_parts.append(entry)
        full_description = "\n\n".join(description_parts)
        if len(full_description) > 3800: full_description = full_description[:3800] + "\n... (list truncated)"
        embed.description = full_description if full_description else "No announcements."
        await interaction.followup.send(embed=embed, ephemeral=True)

    @announce_group.subcommand(name="cancel", description="Cancel a PENDING scheduled announcement.")
    async def cancel_announcement(self, interaction: Interaction, announcement_id: int = SlashOption(description="The ID of the pending announcement to cancel.", required=True)):
        await interaction.response.defer(ephemeral=True)
        if database.delete_pending_announcement(interaction.guild.id, announcement_id):
            await interaction.followup.send(f"✅ Pending announcement ID `{announcement_id}` has been cancelled.", ephemeral=True)
            await self._log_announcement_action(interaction.guild.id, "Announcement Cancelled", f"ID: {announcement_id}\nBy: {interaction.user.mention}", Color.gold())
        else: await interaction.followup.send(f"❌ Could not cancel ID `{announcement_id}`. (Not pending or not found).", ephemeral=True)

    @nextcord.slash_command(name="webhook", description="Manage saved webhooks for announcements.")
    @application_checks.has_permissions(manage_guild=True) 
    async def webhook_group(self, interaction: Interaction): pass

    @webhook_group.subcommand(name="add", description="Save a new webhook URL with a name.")
    async def webhook_add(self, interaction: Interaction,
                          name: str = SlashOption(description="A short, unique name for this webhook (e.g., 'general-updates').", required=True),
                          url: str = SlashOption(description="The full Discord webhook URL.", required=True)):
        await interaction.response.defer(ephemeral=True)
        if not url.startswith("https://discord.com/api/webhooks/"): await interaction.followup.send("❌ Invalid Discord webhook URL format.", ephemeral=True); return
        clean_name = name.strip().lower()
        if not clean_name: await interaction.followup.send("❌ Webhook name cannot be empty.", ephemeral=True); return
        if database.add_saved_webhook(interaction.guild.id, clean_name, url, interaction.user.id):
            await interaction.followup.send(f"✅ Webhook `{clean_name}` saved.", ephemeral=True)
            await self._log_announcement_action(interaction.guild.id, "Webhook Added", f"Name: `{clean_name}`\nAdded by: {interaction.user.mention}", Color.dark_green())
        else: await interaction.followup.send(f"❌ Failed to save webhook `{clean_name}`. (Already exists or DB error).", ephemeral=True)

    @webhook_group.subcommand(name="remove", description="Delete a saved webhook.")
    async def webhook_remove(self, interaction: Interaction,
                             name: str = SlashOption(description="Name of the saved webhook to remove.", required=True, autocomplete=True)):
        await interaction.response.defer(ephemeral=True)
        clean_name = name.strip().lower()
        webhook_to_delete = database.get_saved_webhook_by_name(interaction.guild.id, clean_name)
        if not webhook_to_delete: await interaction.followup.send(f"❌ Saved webhook named `{clean_name}` not found.", ephemeral=True); return
        if database.remove_saved_webhook(interaction.guild.id, clean_name):
            await interaction.followup.send(f"✅ Saved webhook `{clean_name}` removed.", ephemeral=True)
            await self._log_announcement_action(interaction.guild.id, "Webhook Removed", f"Name: `{clean_name}`\nRemoved by: {interaction.user.mention}", Color.dark_orange())
        else: await interaction.followup.send(f"❌ Failed to remove webhook `{clean_name}`.", ephemeral=True)
            
    @webhook_remove.on_autocomplete("name")
    async def webhook_remove_name_autocomplete(self, interaction: Interaction, name_input: str):
        await interaction.response.send_autocomplete(await self.webhook_name_autocomplete(interaction, name_input))

    @webhook_group.subcommand(name="list", description="List all saved webhooks.")
    async def webhook_list(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        webhooks = database.get_all_saved_webhooks(interaction.guild.id)
        if not webhooks: await interaction.followup.send("No webhooks are currently saved for this server.", ephemeral=True); return
        embed = Embed(title=f"Saved Webhooks for {interaction.guild.name}", color=Color.purple())
        description = ""
        for wh in webhooks:
            url_display = wh['url'][:35] + "..." + wh['url'][-5:] if len(wh['url']) > 40 else wh['url']
            description += f"- **Name:** `{wh['name']}`\n  URL: `{url_display}`\n"
            if len(description) > 3800: description += "\n... (list truncated)"; break
        embed.description = description
        await interaction.followup.send(embed=embed, ephemeral=True)

def setup(bot: commands.Bot):
    bot.add_cog(AnnouncementCog(bot))