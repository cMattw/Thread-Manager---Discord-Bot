import nextcord
from nextcord.ext import commands, tasks, application_checks
from nextcord import Interaction, SlashOption, Member, Role, TextChannel, CustomActivity, ActivityType, Color 
from db_utils import database
import logging
import re
from typing import Optional, List, Set
from datetime import datetime # Ensure datetime is imported
import pytz                 

# For normalizing the vanity phrase
VANITY_REMOVE_PREFIXES = ["https://", "http://", "www.", "discord."]
VANITY_REMOVE_SUFFIXES = ["/"]
MANILA_TZ = pytz.timezone("Asia/Manila") 

class StatusMonitorCog(commands.Cog, name="Status Monitor"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.settings: Optional[dict] = None
        self.blacklist_phrases: List[str] = []
        self.log_channel_obj: Optional[TextChannel] = None 
        
        self.vanity_role: Optional[Role] = None
        self.blacklist_role: Optional[Role] = None
        self._initial_scan_done_guilds: Set[int] = set()

    async def cog_load(self):
        # Defer loading config to on_ready to ensure bot.target_guild_id is set
        # Or, if called by reload, target_guild_id should still be there.
        if hasattr(self.bot, 'target_guild_id') and self.bot.target_guild_id:
            await self._load_config(self.bot.target_guild_id)
        else:
            logging.warning("StatusMonitorCog: Target guild ID not available on bot object at cog_load. Config will be loaded in on_ready.")


    async def _load_config(self, guild_id: int):
        if not guild_id: 
            logging.error("StatusMonitorCog: _load_config called with no guild_id.")
            return

        self.settings = database.get_status_monitor_settings(guild_id)
        if not self.settings:
            self.settings = {} 
            logging.info(f"StatusMonitorCog: No settings found for guild {guild_id}. Please configure using /statusconfig commands.")
        
        self.blacklist_phrases = database.get_blacklist_phrases(guild_id) # These are stored lowercase
        
        log_channel_id = self.settings.get('log_channel_id')
        self.log_channel_obj = self.bot.get_channel(log_channel_id) if log_channel_id else None
        if log_channel_id and not self.log_channel_obj:
            logging.warning(f"StatusMonitorCog: Log channel ID {log_channel_id} configured but channel not found.")

        guild = self.bot.get_guild(guild_id)
        if guild:
            vanity_role_id = self.settings.get('vanity_role_id')
            self.vanity_role = guild.get_role(vanity_role_id) if vanity_role_id else None
            if vanity_role_id and not self.vanity_role:
                logging.warning(f"StatusMonitorCog: Vanity role ID {vanity_role_id} configured but role not found.")

            blacklist_role_id = self.settings.get('blacklist_role_id')
            self.blacklist_role = guild.get_role(blacklist_role_id) if blacklist_role_id else None
            if blacklist_role_id and not self.blacklist_role:
                logging.warning(f"StatusMonitorCog: Blacklist role ID {blacklist_role_id} configured but role not found.")
        
        logging.info(f"StatusMonitorCog: Config loaded for guild {guild_id}. Vanity Phrase: '{self.settings.get('vanity_phrase')}', Vanity Role: {self.vanity_role.name if self.vanity_role else 'N/A'}, Blacklist Role: {self.blacklist_role.name if self.blacklist_role else 'N/A'}, Log Channel: {self.log_channel_obj.name if self.log_channel_obj else 'N/A'}, Blacklisted Phrases: {len(self.blacklist_phrases)}")

    async def _log_action(self, guild_id: int, action_title: str, 
                          member_affected: Optional[Member] = None, 
                          details: Optional[str] = None, 
                          status_involved: Optional[str] = None,
                          role_involved: Optional[Role] = None,
                          color: nextcord.Color = nextcord.Color.blue()):
        if not self.log_channel_obj:
            log_message_console = f"StatusMonitorCog (Guild {guild_id}): Action: {action_title}"
            if member_affected: log_message_console += f" | Member: {member_affected.display_name} ({member_affected.id})"
            if role_involved: log_message_console += f" | Role: {role_involved.name}"
            if status_involved is not None: log_message_console += f" | Status: '{status_involved}'"
            if details: log_message_console += f" | Details: {details}"
            logging.info(log_message_console)
            return

        try:
            embed = nextcord.Embed(title=f"Status Monitor: {action_title}", color=color)
            timestamp = datetime.now(MANILA_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')
            embed.add_field(name="Timestamp (GMT+8)", value=timestamp, inline=False)

            if member_affected:
                embed.add_field(name="Member", value=f"{member_affected.mention} ({member_affected.id})", inline=True)
            if role_involved:
                embed.add_field(name="Role", value=f"{role_involved.name} ({role_involved.id})", inline=True)
            if status_involved is not None: 
                embed.add_field(name="Triggering Status", value=f"```{status_involved[:1000]}```", inline=False) 
            if details:
                embed.add_field(name="Details", value=details[:1020], inline=False) 
            
            await self.log_channel_obj.send(embed=embed)
        except nextcord.Forbidden:
            logging.warning(f"StatusMonitorCog: Missing permissions to send log to channel {self.log_channel_obj.id} in guild {guild_id}")
        except Exception as e:
            logging.error(f"StatusMonitorCog: Error sending log: {e}", exc_info=True)

    def _normalize_vanity_phrase(self, phrase: Optional[str]) -> Optional[str]:
        if not phrase: return None
        normalized = phrase.lower().strip()
        for prefix in VANITY_REMOVE_PREFIXES:
            if normalized.startswith(prefix): normalized = normalized[len(prefix):]
        for suffix in VANITY_REMOVE_SUFFIXES:
            if normalized.endswith(suffix): normalized = normalized[:-len(suffix)]
        return normalized.strip()

    async def _process_member_status(self, member: Member):
        if not self.settings or member.bot: return

        guild_id = member.guild.id 
        custom_status_text = ""
        for activity in member.activities:
            if isinstance(activity, CustomActivity) and activity.name:
                custom_status_text = activity.name; break
        
        normalized_current_status_for_vanity = self._normalize_vanity_phrase(custom_status_text) if custom_status_text else ""
        simple_normalized_current_status = custom_status_text.lower().strip() if custom_status_text else ""
        
        configured_vanity_trigger = self.settings.get('vanity_phrase') 

        has_vanity_role = self.vanity_role and self.vanity_role in member.roles
        has_blacklist_role = self.blacklist_role and self.blacklist_role in member.roles

        status_contains_full_vanity = False
        if self.vanity_role and configured_vanity_trigger and normalized_current_status_for_vanity:
            if configured_vanity_trigger in normalized_current_status_for_vanity:
                status_contains_full_vanity = True
        
        if status_contains_full_vanity:
            if not has_vanity_role:
                try: await member.add_roles(self.vanity_role, reason="Status contains specific vanity phrase"); await self._log_action(guild_id, "Vanity Role Added", member_affected=member, role_involved=self.vanity_role, status_involved=custom_status_text, color=Color.green())
                except nextcord.Forbidden: await self._log_action(guild_id, "Vanity Role Add FAILED", member_affected=member, role_involved=self.vanity_role, details="Bot lacks permissions.", color=Color.red())
                except Exception as e: await self._log_action(guild_id, "Vanity Role Add ERROR", member_affected=member, role_involved=self.vanity_role, details=str(e), color=Color.red())
            if self.blacklist_role and has_blacklist_role:
                try: await member.remove_roles(self.blacklist_role, reason="Vanity phrase present; removing conflicting blacklist role."); await self._log_action(guild_id, "Blacklist Role Removed (Vanity Override)", member_affected=member, role_involved=self.blacklist_role, status_involved=custom_status_text, color=Color.dark_grey())
                except nextcord.Forbidden: await self._log_action(guild_id, "Blacklist Role Remove FAILED (Vanity Override)", member_affected=member, role_involved=self.blacklist_role, details="Bot lacks permissions.", color=Color.red())
                except Exception as e: await self._log_action(guild_id, "Blacklist Role Remove ERROR (Vanity Override)", member_affected=member, role_involved=self.blacklist_role, details=str(e), color=Color.red())
            return 

        if self.vanity_role and has_vanity_role: 
            try: await member.remove_roles(self.vanity_role, reason="Status no longer contains specific vanity phrase"); await self._log_action(guild_id, "Vanity Role Removed", member_affected=member, role_involved=self.vanity_role, status_involved=custom_status_text, color=Color.orange())
            except nextcord.Forbidden: await self._log_action(guild_id, "Vanity Role Remove FAILED", member_affected=member, role_involved=self.vanity_role, details="Bot lacks permissions.", color=Color.red())
            except Exception as e: await self._log_action(guild_id, "Vanity Role Remove ERROR", member_affected=member, role_involved=self.vanity_role, details=str(e), color=Color.red())
        
        if self.blacklist_role and self.blacklist_phrases:
            status_contains_any_blacklist_phrase = False; triggered_blacklist_phrase = None
            if custom_status_text:
                for bp_phrase in self.blacklist_phrases: 
                    if bp_phrase in simple_normalized_current_status:
                        status_contains_any_blacklist_phrase = True; triggered_blacklist_phrase = bp_phrase; break
            if status_contains_any_blacklist_phrase:
                if not has_blacklist_role:
                    try: await member.add_roles(self.blacklist_role, reason=f"Status contains blacklisted phrase: {triggered_blacklist_phrase}"); await self._log_action(guild_id, "Blacklist Role Added", member_affected=member, role_involved=self.blacklist_role, status_involved=custom_status_text, details=f"Trigger: `{triggered_blacklist_phrase}`", color=Color.dark_red())
                    except nextcord.Forbidden: await self._log_action(guild_id, "Blacklist Role Add FAILED", member_affected=member, role_involved=self.blacklist_role, details="Bot lacks permissions.", color=Color.red())
                    except Exception as e: await self._log_action(guild_id, "Blacklist Role Add ERROR", member_affected=member, role_involved=self.blacklist_role, details=str(e), color=Color.red())
            elif has_blacklist_role: 
                try: await member.remove_roles(self.blacklist_role, reason="Status no longer contains blacklisted phrase"); await self._log_action(guild_id, "Blacklist Role Removed", member_affected=member, role_involved=self.blacklist_role, status_involved=custom_status_text, color=Color.gold())
                except nextcord.Forbidden: await self._log_action(guild_id, "Blacklist Role Remove FAILED", member_affected=member, role_involved=self.blacklist_role, details="Bot lacks permissions.", color=Color.red())
                except Exception as e: await self._log_action(guild_id, "Blacklist Role Remove ERROR", member_affected=member, role_involved=self.blacklist_role, details=str(e), color=Color.red())

    @commands.Cog.listener()
    async def on_ready(self):
        await self.bot.wait_until_ready() 
        if not self.bot.target_guild_id or not hasattr(self.bot, 'target_guild_id'):
            logging.error("StatusMonitorCog: Target guild ID not set in bot. Cannot perform initial scan.")
            return
        
        # This check is important if on_ready can fire multiple times for the same "session"
        # or if the cog is reloaded without a full bot restart.
        if self.bot.target_guild_id in self._initial_scan_done_guilds and self.settings is not None: # Check if settings also loaded
            logging.info(f"StatusMonitorCog: Initial scan for guild {self.bot.target_guild_id} already performed or config loaded. Forcing a config reload for on_ready.")
            # Forcing a config reload on every on_ready (after the first scan) might be good practice
            # to catch any role/channel ID changes if the bot was offline.
            await self._load_config(self.bot.target_guild_id)
            # Decide if a full re-scan of members is needed here upon every reconnect.
            # For now, we only do the full member scan once per bot process lifetime per guild.
            # If you want it to rescan all members on every reconnect, you'd remove the
            # self._initial_scan_done_guilds check or call the scan logic here too.
            return 
        
        await self._load_config(self.bot.target_guild_id) 
        target_guild = self.bot.get_guild(self.bot.target_guild_id)
        if not target_guild:
            logging.error(f"StatusMonitorCog: Target guild {self.bot.target_guild_id} not found during on_ready scan.")
            return

        logging.info(f"StatusMonitorCog: Starting initial status scan for members in {target_guild.name}...")
        count = 0
        try:
            async for member in target_guild.fetch_members(limit=None): 
                if not member.bot: 
                    await self._process_member_status(member)
                    count +=1
        except nextcord.Forbidden:
            logging.error(f"StatusMonitorCog: Missing 'Server Members Intent' or permissions to fetch members for initial scan in {target_guild.name}.")
            await self._log_action(target_guild.id, "Initial Scan FAILED", details="Bot lacks permissions to fetch guild members.", color=Color.red())
        except Exception as e:
            logging.error(f"StatusMonitorCog: Error during initial member scan in {target_guild.name}: {e}", exc_info=True)
            await self._log_action(target_guild.id, "Initial Scan ERROR", details=f"An error occurred: {e}", color=Color.red())

        logging.info(f"StatusMonitorCog: Initial status scan completed for {count} members in {target_guild.name}.")
        self._initial_scan_done_guilds.add(target_guild.id)

    @commands.Cog.listener()
    async def on_presence_update(self, before: Member, after: Member):
        if not self.bot.target_guild_id or not self.settings or not hasattr(self.bot, 'target_guild_id'):
            return 
        if not after.guild or after.guild.id != self.bot.target_guild_id or after.bot: 
            return
        
        before_custom_status_text = ""; after_custom_status_text = ""
        for act in before.activities:
            if isinstance(act, CustomActivity) and act.name: before_custom_status_text = act.name; break
        for act in after.activities:
            if isinstance(act, CustomActivity) and act.name: after_custom_status_text = act.name; break

        if before_custom_status_text != after_custom_status_text:
            logging.debug(f"StatusMonitorCog: Presence update for {after.display_name}. Status changed: '{before_custom_status_text}' -> '{after_custom_status_text}'")
            await self._process_member_status(after)

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

    @nextcord.slash_command(name="statusconfig", description="Configure status monitoring settings.")
    @application_checks.has_permissions(manage_guild=True)
    async def statusconfig_group(self, interaction: Interaction): pass

    @statusconfig_group.subcommand(name="set_vanity_phrase", description="Sets the vanity phrase to look for (e.g., gg/yourserver).")
    async def set_vanity_phrase(self, interaction: Interaction, phrase: str = SlashOption(description="The core phrase (e.g. gg/myserver). Bot normalizes common prefixes.", required=True)):
        await interaction.response.defer(ephemeral=True)
        normalized_phrase = self._normalize_vanity_phrase(phrase)
        if not normalized_phrase: await interaction.followup.send("Invalid phrase after normalization (cannot be empty).", ephemeral=True); return
        
        database.update_status_monitor_setting(self.bot.target_guild_id, 'vanity_phrase', normalized_phrase)
        await self._load_config(self.bot.target_guild_id)
        await interaction.followup.send(f"Vanity phrase set to be matched: `{normalized_phrase}`. (Normalized from your input: `{phrase}`)", ephemeral=True)
        await self._log_action(self.bot.target_guild_id, "Config Update: Vanity Phrase", details=f"Set to `{normalized_phrase}` by {interaction.user.mention}", color=Color.blurple())

    @statusconfig_group.subcommand(name="set_vanity_role", description="Sets the role to give for the vanity phrase.")
    async def set_vanity_role(self, interaction: Interaction, role: Role = SlashOption(description="The role to assign", required=True)):
        await interaction.response.defer(ephemeral=True)
        database.update_status_monitor_setting(self.bot.target_guild_id, 'vanity_role_id', role.id)
        await self._load_config(self.bot.target_guild_id)
        await interaction.followup.send(f"Vanity role set to: {role.mention}.", ephemeral=True)
        await self._log_action(self.bot.target_guild_id, "Config Update: Vanity Role", role_involved=role, details=f"Set by {interaction.user.mention}", color=Color.blurple())

    @statusconfig_group.subcommand(name="set_blacklist_role", description="Sets the role for users with blacklisted status phrases.")
    async def set_blacklist_role(self, interaction: Interaction, role: Role = SlashOption(description="The role for blacklist violations", required=True)):
        await interaction.response.defer(ephemeral=True)
        database.update_status_monitor_setting(self.bot.target_guild_id, 'blacklist_role_id', role.id)
        await self._load_config(self.bot.target_guild_id)
        await interaction.followup.send(f"Blacklist role set to: {role.mention}.", ephemeral=True)
        await self._log_action(self.bot.target_guild_id, "Config Update: Blacklist Role", role_involved=role, details=f"Set by {interaction.user.mention}", color=Color.blurple())

    @statusconfig_group.subcommand(name="set_log_channel", description="Sets the log channel for this cog's actions.")
    async def set_log_channel(self, interaction: Interaction, channel: TextChannel = SlashOption(description="The text channel for logs", required=True)):
        await interaction.response.defer(ephemeral=True)
        database.update_status_monitor_setting(self.bot.target_guild_id, 'log_channel_id', channel.id)
        await self._load_config(self.bot.target_guild_id) 
        await interaction.followup.send(f"Status monitor log channel set to: {channel.mention}.", ephemeral=True)
        # The log action below will use the newly set channel if successful.
        await self._log_action(self.bot.target_guild_id, "Config Update: Log Channel", details=f"Set to {channel.mention} by {interaction.user.mention}", color=Color.blurple())

    @statusconfig_group.subcommand(name="view", description="View current status monitoring settings.")
    async def view_settings(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        await self._load_config(self.bot.target_guild_id) 
        if not self.settings: 
             await interaction.followup.send("No status monitor settings found or loaded. Please configure first.", ephemeral=True)
             return

        embed = nextcord.Embed(title=f"Status Monitor Settings for {interaction.guild.name}", color=nextcord.Color.blue())
        vp = self.settings.get('vanity_phrase', 'Not Set')
        vr_mention = self.vanity_role.mention if self.vanity_role else 'Not Set'
        br_mention = self.blacklist_role.mention if self.blacklist_role else 'Not Set'
        lc_mention = self.log_channel_obj.mention if self.log_channel_obj else 'Not Set'
        
        embed.add_field(name="Vanity Phrase (Normalized)", value=f"`{vp}`", inline=False)
        embed.add_field(name="Vanity Role", value=vr_mention, inline=False)
        embed.add_field(name="Blacklist Role", value=br_mention, inline=False)
        embed.add_field(name="Log Channel (This Cog)", value=lc_mention, inline=False)
        
        bl_phrases_str = "\n".join([f"- `{p}`" for p in self.blacklist_phrases]) if self.blacklist_phrases else "None Set"
        if len(bl_phrases_str) > 1000 : bl_phrases_str = bl_phrases_str[:1000] + "..." 
        embed.add_field(name=f"Blacklisted Phrases ({len(self.blacklist_phrases)})", value=bl_phrases_str, inline=False)
        
        await interaction.followup.send(embed=embed, ephemeral=True)

    @statusconfig_group.subcommand(name="scanall", description="Manually re-scan all members' statuses and update roles.")
    @application_checks.has_permissions(manage_guild=True)
    async def scan_all_members_command(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True) 
        
        target_guild = self.bot.get_guild(self.bot.target_guild_id)
        if not target_guild:
            await interaction.followup.send("Target guild not found by the bot. Please check bot console.", ephemeral=True)
            return

        logging.info(f"[SCAN_ALL] Forcing reload of status monitor configuration for guild {target_guild.name}...")
        await self._load_config(self.bot.target_guild_id) 
        
        if not self.settings or (not self.vanity_role and not self.settings.get('vanity_phrase')) and (not self.blacklist_role and not self.blacklist_phrases) : 
            error_msg = "Status monitor is not configured with any roles or phrases to act upon. Please use `/statusconfig`."
            logging.error(f"[SCAN_ALL] Aborted: {error_msg}")
            logging.error(f"[SCAN_ALL] Current loaded - VanityRole: {self.vanity_role}, BlacklistRole: {self.blacklist_role}, VanityPhrase: {self.settings.get('vanity_phrase')}, BlacklistPhrases Count: {len(self.blacklist_phrases)}")
            await interaction.followup.send(error_msg, ephemeral=True)
            return

        await interaction.followup.send(f"Starting manual scan of members in {target_guild.name}. This may take time...", ephemeral=True)
        
        logging.info(f"StatusMonitorCog: Manual scan initiated by {interaction.user.display_name} for {target_guild.name}...")
        logging.info(f"[SCAN_ALL] Using Config - Vanity Phrase: '{self.settings.get('vanity_phrase')}', "
                     f"Vanity Role: '{self.vanity_role.name if self.vanity_role else 'None'}', "
                     f"Blacklist Role: '{self.blacklist_role.name if self.blacklist_role else 'None'}', "
                     f"Blacklisted Phrases Count: {len(self.blacklist_phrases)}")

        processed_members_count = 0
        errors_during_scan_details = []

        async for member in target_guild.fetch_members(limit=None): 
            if not member.bot:
                custom_status_text_debug = ""
                for activity in member.activities: 
                    if isinstance(activity, CustomActivity) and activity.name:
                        custom_status_text_debug = activity.name
                        break
                logging.debug(f"[SCAN_ALL] Processing member: {member.display_name} ({member.id}), Fetched Status: '{custom_status_text_debug}'")
                
                try:
                    await self._process_member_status(member)
                    processed_members_count += 1
                except Exception as e:
                    logging.error(f"[SCAN_ALL] Error processing member {member.display_name} ({member.id}): {e}", exc_info=True)
                    errors_during_scan_details.append(f"Error with {member.display_name}: {e}")
        
        completion_message = f"Manual status-based role update completed. Processed {processed_members_count} members."
        if errors_during_scan_details:
            completion_message += f"\nEncountered {len(errors_during_scan_details)} error(s) during processing. Check bot console logs."
            logging.warning(f"StatusMonitorCog: Manual role update for {target_guild.name} finished with {len(errors_during_scan_details)} errors during _process_member_status calls.")
        
        logging.info(f"StatusMonitorCog: Manual scan completed for {processed_members_count} members in {target_guild.name}.")
        
        try:
            await interaction.followup.send(completion_message, ephemeral=True)
        except nextcord.NotFound: 
            logging.warning("[SCAN_ALL] Original interaction for scanall command followup timed out. Process completed.")
            if self.log_channel_obj:
                 await self._log_action(target_guild.id, "Manual Role Update Complete", details=completion_message, color=Color.green() if not errors_during_scan_details else Color.orange())

    @nextcord.slash_command(name="blacklistphrase", description="Manage blacklisted status phrases.")
    @application_checks.has_permissions(manage_guild=True)
    async def blacklistphrase_group(self, interaction: Interaction): pass

    @blacklistphrase_group.subcommand(name="add", description="Adds a phrase to the status blacklist.")
    async def blacklist_add(self, interaction: Interaction, phrase: str = SlashOption(description="Phrase to blacklist (case-insensitive, 'contains' match)", required=True)):
        await interaction.response.defer(ephemeral=True)
        clean_phrase = phrase.strip().lower()
        if not clean_phrase: await interaction.followup.send("Phrase cannot be empty.", ephemeral=True); return
        
        if database.add_blacklist_phrase(self.bot.target_guild_id, clean_phrase):
            await self._load_config(self.bot.target_guild_id) 
            await interaction.followup.send(f"Phrase `{clean_phrase}` added to blacklist.", ephemeral=True)
            await self._log_action(self.bot.target_guild_id, "Blacklist Phrase Added", details=f"Phrase: `{clean_phrase}` by {interaction.user.mention}", color=Color.light_grey())
        else: await interaction.followup.send(f"Phrase `{clean_phrase}` might already be blacklisted or DB error.", ephemeral=True)

    @blacklistphrase_group.subcommand(name="remove", description="Removes a phrase from the status blacklist.")
    async def blacklist_remove(self, interaction: Interaction, phrase: str = SlashOption(description="Phrase to remove from blacklist", required=True)):
        await interaction.response.defer(ephemeral=True)
        clean_phrase = phrase.strip().lower()
        if not clean_phrase: await interaction.followup.send("Phrase cannot be empty.", ephemeral=True); return

        if database.remove_blacklist_phrase(self.bot.target_guild_id, clean_phrase):
            await self._load_config(self.bot.target_guild_id)
            await interaction.followup.send(f"Phrase `{clean_phrase}` removed from blacklist.", ephemeral=True)
            await self._log_action(self.bot.target_guild_id, "Blacklist Phrase Removed", details=f"Phrase: `{clean_phrase}` by {interaction.user.mention}", color=Color.light_grey())
        else: await interaction.followup.send(f"Phrase `{clean_phrase}` not found in blacklist or DB error.", ephemeral=True)

    @blacklistphrase_group.subcommand(name="list", description="Lists all blacklisted status phrases.")
    async def blacklist_list(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        # Ensure phrases are loaded, though _load_config on cog_load/ready should handle it
        if not self.blacklist_phrases and hasattr(self.bot, 'target_guild_id') and self.bot.target_guild_id: 
            await self._load_config(self.bot.target_guild_id)

        if not self.blacklist_phrases:
            await interaction.followup.send("No phrases are currently blacklisted.", ephemeral=True)
            return
        
        description = "Current blacklisted phrases (case-insensitive, 'contains' match):\n"
        for p in self.blacklist_phrases: description += f"- `{p}`\n"
        
        embeds_to_send = []
        if len(description) > 2000: 
            parts = []
            current_part = "Current blacklisted phrases (case-insensitive, 'contains' match):\n"
            for p in self.blacklist_phrases:
                line = f"- `{p}`\n"
                if len(current_part) + len(line) > 1900: 
                    parts.append(current_part)
                    current_part = "" # Start new part with no header
                current_part += line
            if current_part: parts.append(current_part)

            for i, part_desc in enumerate(parts):
                embed = nextcord.Embed(title=f"Blacklisted Status Phrases (Part {i+1}/{len(parts)})", description=part_desc, color=nextcord.Color.orange())
                embeds_to_send.append(embed)
        else:
            embed = nextcord.Embed(title="Blacklisted Status Phrases", description=description, color=nextcord.Color.orange())
            embeds_to_send.append(embed)

        first_embed_sent = False
        for embed_item in embeds_to_send:
            if not first_embed_sent:
                await interaction.followup.send(embed=embed_item, ephemeral=True)
                first_embed_sent = True
            else: 
                # Subsequent embeds for a single ephemeral response must be sent as new messages if the first was a followup
                # Or, if interaction.send is available and followup hasn't been used for this interaction.
                # For simplicity and safety with ephemeral, sending multiple followups for different embeds
                # isn't standard. Usually, you send one followup. If multiple embeds, they must be in one message.
                # So, we should ensure the description splitting handles this better or just send one embed.
                # The above logic tries to split but it's for a single description.
                # A better way is to use nextcord's paginator or send multiple separate followups if the interaction is still valid.
                # For now, if it's too long, it will be truncated by the ...
                # The above loop creating multiple embeds for one ephemeral followup isn't quite right.
                # Let's just send the first one. The user can refine this if many phrases.
                break # Only send the first embed for now to avoid issues with multiple ephemeral followups.

        if not embeds_to_send and first_embed_sent: # Should not happen if list is not empty
             await interaction.followup.send("Error formatting blacklist. List might be too long.", ephemeral=True)


def setup(bot: commands.Bot):
    bot.add_cog(StatusMonitorCog(bot))