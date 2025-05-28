import nextcord
from nextcord.ext import commands, application_checks
from nextcord import Interaction, SlashOption, Thread 
import database 
import logging
import re 
from typing import Optional 

class ThreadExemptionCog(commands.Cog, name="Thread Exemptions"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_check(self, interaction: Interaction) -> bool:
        if not self.bot.target_guild_id:
            if not interaction.response.is_done():
                try: await interaction.response.defer(ephemeral=True)
                except nextcord.NotFound: pass 
            await interaction.followup.send("Bot is not yet ready or target server not identified.", ephemeral=True)
            return False
        if interaction.guild is None or interaction.guild.id != self.bot.target_guild_id:
            if not interaction.response.is_done():
                try: await interaction.response.defer(ephemeral=True)
                except nextcord.NotFound: pass
            target_guild_name = getattr(self.bot, 'target_guild_name', 'the configured server')
            await interaction.followup.send(f"This bot is configured for a specific server. Please use commands in '{target_guild_name}'.", ephemeral=True)
            return False
        return True

    async def _resolve_thread_from_target(self, guild: nextcord.Guild, target_str: str) -> Optional[nextcord.Thread]:
        thread_id = None
        link_match = re.search(r'discord(?:app)?.com/channels/\d+/(?:\d+/)?(\d+)', target_str) 
        
        if link_match:
            try: thread_id = int(link_match.group(1))
            except ValueError: pass
        else: 
            try: thread_id = int(target_str)
            except ValueError: logging.debug(f"Could not parse '{target_str}' as a thread ID or link."); return None 

        if thread_id:
            try:
                channel_or_thread = await self.bot.fetch_channel(thread_id) 
                if isinstance(channel_or_thread, nextcord.Thread):
                    if channel_or_thread.guild.id == guild.id: return channel_or_thread
                    else: logging.warning(f"Resolved thread {thread_id} does not belong to guild {guild.id}"); return None
                else: logging.warning(f"Fetched channel {thread_id} is not a Thread object."); return None 
            except nextcord.NotFound: logging.debug(f"Thread with ID {thread_id} not found via fetch_channel."); return None
            except nextcord.Forbidden: logging.warning(f"Bot lacks permissions to fetch channel/thread {thread_id}."); return None
            except Exception as e: logging.error(f"Error fetching channel/thread {thread_id}: {e}", exc_info=True); return None
        return None

    # *** MODIFIED HERE: Renamed command group from "ticket" to "thread" ***
    @nextcord.slash_command(name="thread", description="Thread exemption & utility commands.")
    async def thread_group(self, interaction: Interaction): # Renamed method for clarity
        pass 

    # Subcommands now belong to "thread_group"
    @thread_group.subcommand(name="keep_active", description="Exempt a thread from auto-management and unarchives it.")
    @application_checks.has_permissions(manage_threads=True) 
    async def thread_keep_active(self, interaction: Interaction, # Renamed method for clarity
                                 thread_target: str = SlashOption(description="The ID or link of the thread to keep active", required=True)):
        await interaction.response.defer(ephemeral=True)
        
        target_thread = await self._resolve_thread_from_target(interaction.guild, thread_target)

        if not target_thread:
            await interaction.followup.send(f"Could not find a valid thread in this server with the provided ID or link: `{thread_target}`.", ephemeral=True)
            logging.warning(f"[KEEP_ACTIVE] User {interaction.user} failed to find thread: {thread_target} in guild {interaction.guild.id}")
            return

        logging.info(f"[KEEP_ACTIVE] User {interaction.user} targeting thread '{target_thread.name}' (ID: {target_thread.id}). Current archived status: {target_thread.archived}")
        is_already_exempted = database.is_thread_exempted(interaction.guild.id, target_thread.id)
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

        if database.add_exempted_thread(interaction.guild.id, target_thread.id, interaction.user.id):
            feedback_message = f"Thread <#{target_thread.id}> is now exempted from auto-management by {interaction.user.mention}."
            logging.info(f"[KEEP_ACTIVE] Successfully exempted thread {target_thread.id}.")
            if target_thread.archived:
                logging.info(f"[KEEP_ACTIVE] Newly exempted thread {target_thread.id} is archived. Attempting to unarchive.")
                try:
                    await target_thread.edit(archived=False) 
                    feedback_message += " It has also been unarchived."
                    logging.info(f"[KEEP_ACTIVE] Successfully unarchived newly exempted thread {target_thread.id}.")
                except nextcord.Forbidden:
                    feedback_message += " I tried to unarchive it but **lack `Manage Threads` permission**."
                    logging.warning(f"[KEEP_ACTIVE] Failed to unarchive newly exempted thread {target_thread.id} due to missing 'Manage Threads' permission.")
                except Exception as e:
                    feedback_message += f" An error occurred while trying to unarchive it: {type(e).__name__}."
                    logging.error(f"[KEEP_ACTIVE] Error unarchiving newly exempted thread {target_thread.id}: {e}", exc_info=True)
            else:
                logging.info(f"[KEEP_ACTIVE] Newly exempted thread {target_thread.id} is not archived. No unarchive action needed.")
            await interaction.followup.send(feedback_message, ephemeral=True, suppress_embeds=True)
        else:
            feedback_message = f"Failed to save exemption for thread <#{target_thread.id}> in the database. Please check bot console logs."
            logging.error(f"[KEEP_ACTIVE] Database call to add_exempted_thread failed for thread {target_thread.id}.")
            await interaction.followup.send(feedback_message, ephemeral=True)

    @thread_group.subcommand(name="allow_automation", description="Removes a thread's exemption, allowing bot auto-management.")
    @application_checks.has_permissions(manage_threads=True) 
    async def thread_allow_automation(self, interaction: Interaction, # Renamed method for clarity
                                      thread_target: str = SlashOption(description="The ID or link of the thread to manage automatically", required=True)):
        await interaction.response.defer(ephemeral=True)
        
        target_thread = await self._resolve_thread_from_target(interaction.guild, thread_target)

        if not target_thread:
            await interaction.followup.send(f"Could not find a valid thread in this server with the provided ID or link: `{thread_target}`.", ephemeral=True)
            return

        if not database.is_thread_exempted(interaction.guild.id, target_thread.id):
            await interaction.followup.send(f"Thread <#{target_thread.id}> was not exempted from auto-management.", ephemeral=True)
            return

        if database.remove_exempted_thread(interaction.guild.id, target_thread.id):
            await interaction.followup.send(f"Thread <#{target_thread.id}> will now be auto-managed by the bot again.", ephemeral=True)
            logging.info(f"Exemption removed for thread {target_thread.id} by {interaction.user.name}")
        else:
            await interaction.followup.send(f"Failed to remove exemption for thread <#{target_thread.id}>. Please check bot logs.", ephemeral=True)

def setup(bot: commands.Bot):
    bot.add_cog(ThreadExemptionCog(bot))