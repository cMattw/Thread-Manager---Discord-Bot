import nextcord
from nextcord.ext import commands, tasks, application_checks
from nextcord import (
    Interaction, SlashOption, Member, Role, TextChannel, 
    Embed, Color, Invite, Webhook, User, AuditLogAction, Intents 
)
from db_utils import invites_database as idb 
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Set
import asyncio 
import aiohttp 
# import pytz # Not strictly needed if all embed timestamps are UTC or relative

# MANILA_TZ = pytz.timezone("Asia/Manila") # Can be used for footer if desired

class InviteTrackerCog(commands.Cog, name="Invite Tracker"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.invite_cache: Dict[int, Dict[str, Invite]] = {} 
        self.log_channel_obj: Optional[TextChannel] = None
        self.leaderboard_webhook_url: Optional[str] = None
        self.leaderboard_message_id: Optional[int] = None
        self.leaderboard_channel_id: Optional[int] = None 
        self.required_role_obj: Optional[Role] = None
        self.target_guild_id: Optional[int] = None 
        self._cog_config_loaded_for_guild: Optional[int] = None
        self._initial_scan_done_guilds: Set[int] = set()

    async def cog_load(self):
        await self.bot.wait_until_ready()
        if hasattr(self.bot, 'target_guild_id') and self.bot.target_guild_id:
            self.target_guild_id = self.bot.target_guild_id
            idb.initialize_database(self.target_guild_id) 
            await self._load_config() 
            await self._cache_invites(self.target_guild_id) 
            if not self.update_leaderboard_task.is_running() and self.leaderboard_webhook_url: # Only start if configured
                self.update_leaderboard_task.start()
            logging.info("InviteTrackerCog loaded and configured for target guild.")
        else:
            logging.error("InviteTrackerCog: Target guild ID not set on bot. Cog will not function correctly.")
            if self.update_leaderboard_task.is_running(): self.update_leaderboard_task.cancel()

    async def _load_config(self):
        if not self.target_guild_id: 
            logging.warning("InviteTrackerCog: Cannot load config, target_guild_id not set.")
            return
        
        config = idb.get_cog_config(self.target_guild_id)
        if config:
            log_ch_id = config.get('log_channel_id')
            self.log_channel_obj = self.bot.get_channel(log_ch_id) if log_ch_id else None
            self.leaderboard_webhook_url = config.get('leaderboard_webhook_url')
            self.leaderboard_message_id = config.get('leaderboard_message_id')
            self.leaderboard_channel_id = config.get('leaderboard_channel_id')
            required_role_id = config.get('required_role_id')
            guild = self.bot.get_guild(self.target_guild_id)
            if guild and required_role_id:
                self.required_role_obj = guild.get_role(required_role_id)
                if not self.required_role_obj: logging.warning(f"InviteTrackerCog: Required role ID {required_role_id} not found.")
            else: self.required_role_obj = None
            self._cog_config_loaded_for_guild = self.target_guild_id
            logging.info(f"InviteTrackerCog: Config loaded. Log: {self.log_channel_obj.name if self.log_channel_obj else 'N/A'}, LB Webhook: {bool(self.leaderboard_webhook_url)}, Req. Role: {self.required_role_obj.name if self.required_role_obj else 'N/A'}")
        else:
            self.log_channel_obj = None; self.leaderboard_webhook_url = None; self.leaderboard_message_id = None; self.leaderboard_channel_id = None; self.required_role_obj = None
            self._cog_config_loaded_for_guild = self.target_guild_id
            logging.info(f"InviteTrackerCog: No specific config for guild {self.target_guild_id}. Please use /inviteset.")

    def cog_unload(self):
        self.update_leaderboard_task.cancel()
        logging.info("InviteTrackerCog: Tasks cancelled.")

    async def _log_invite_action(self, title: str, color: Color = Color.blue(), **kwargs):
        """Logs invite-related actions to the configured log channel using embeds."""
        if self.target_guild_id and self._cog_config_loaded_for_guild != self.target_guild_id:
            await self._load_config()

        if not self.log_channel_obj:
            # Fallback to console logging if no log channel is set for this cog
            log_parts = [f"InviteTrackerCog (Guild {self.target_guild_id}): {title}"]
            for key, value in kwargs.items():
                if value is not None:
                    log_parts.append(f"{key.replace('_', ' ').title()}: {value}")
            logging.info(" | ".join(log_parts))
            return

        embed = Embed(title=f"Invite Tracker: {title}", color=color)
        embed.timestamp = datetime.now(timezone.utc) # Use UTC for embed timestamp

        for key, value in kwargs.items():
            if value is not None:
                name = key.replace("_", " ").title()
                value_str = str(value)
                # Try to make inline if value is short enough
                is_inline = len(value_str) < 40 
                if len(embed.fields) >= 24: # Max 25 fields, leave one for potential error/summary
                    embed.add_field(name="More Info...", value="Too many details to display.", inline=False)
                    break
                embed.add_field(name=name, value=value_str[:1020], inline=is_inline) # Limit field value length

        try:
            await self.log_channel_obj.send(embed=embed)
        except nextcord.Forbidden:
            logging.warning(f"InviteTrackerCog: Missing permissions to send embed log to {self.log_channel_obj.mention}")
        except Exception as e:
            logging.error(f"InviteTrackerCog: Error sending embed log: {e}", exc_info=True)

    async def _cache_invites(self, guild_id: int):
        # (Same as before)
        guild = self.bot.get_guild(guild_id)
        if not guild: logging.error(f"InviteTrackerCog: Guild {guild_id} not found for caching."); return
        try:
            self.invite_cache[guild.id] = {invite.code: invite for invite in await guild.invites()}
            logging.info(f"InviteTrackerCog: Cached {len(self.invite_cache[guild.id])} invites for {guild.name}")
        except nextcord.Forbidden: logging.error(f"InviteTrackerCog: Missing 'Manage Server' for guild {guild.name} to cache invites.")
        except Exception as e: logging.error(f"InviteTrackerCog: Error caching invites for {guild.name}: {e}", exc_info=True)


    @commands.Cog.listener()
    async def on_ready(self): 
        # (Same as before, ensures _load_config_and_cache and task start)
        await self.bot.wait_until_ready()
        if hasattr(self.bot, 'target_guild_id') and self.bot.target_guild_id:
            self.target_guild_id = self.bot.target_guild_id 
            if self._cog_config_loaded_for_guild != self.target_guild_id:
                idb.initialize_database(self.target_guild_id)
                await self._load_config_and_cache() # Renamed this method
            
            if self.target_guild_id not in self._initial_scan_done_guilds:
                logging.info(f"InviteTrackerCog: Performing initial member scan for role rewards in {self.bot.target_guild_name}...")
                target_guild = self.bot.get_guild(self.target_guild_id)
                if target_guild:
                    member_count = 0
                    try:
                        async for member in target_guild.fetch_members(limit=None): 
                            if not member.bot: await self._check_and_apply_role_rewards(member); member_count +=1
                        logging.info(f"InviteTrackerCog: Initial role reward check done for {member_count} members.")
                    except nextcord.Forbidden: logging.error(f"InviteTrackerCog: Missing 'Server Members Intent' or permissions to fetch members for initial scan in {target_guild.name}.")
                    except Exception as e: logging.error(f"InviteTrackerCog: Error during initial member scan for role rewards: {e}", exc_info=True)
                self._initial_scan_done_guilds.add(self.target_guild_id)

            if not self.update_leaderboard_task.is_running() and self.leaderboard_webhook_url: 
                 self.update_leaderboard_task.start()
        logging.info("InviteTrackerCog is ready.")
        
    async def _load_config_and_cache(self): # Renamed this method
        """Loads configuration and initial invite cache for the target guild."""
        await self._load_config() # Calls the original _load_config
        if self.target_guild_id: # Ensure target_guild_id is set before caching
            await self._cache_invites(self.target_guild_id)


    @commands.Cog.listener()
    async def on_member_join(self, member: Member):
        if not self.target_guild_id or member.guild.id != self.target_guild_id or member.bot: return
        if self._cog_config_loaded_for_guild != member.guild.id: await self._load_config_and_cache()
        
        await asyncio.sleep(3) 
        guild = member.guild
        new_invites_map: Dict[str, Invite] = {invite.code: invite for invite in await guild.invites()}
        old_guild_invites = self.invite_cache.get(guild.id, {})
        found_inviter: Optional[User] = None; used_invite_code: Optional[str] = None

        for code, new_invite_obj in new_invites_map.items():
            old_invite_obj = old_guild_invites.get(code)
            current_uses = new_invite_obj.uses or 0
            previous_uses = old_invite_obj.uses if old_invite_obj and old_invite_obj.uses is not None else 0
            if current_uses > previous_uses:
                found_inviter = new_invite_obj.inviter; used_invite_code = new_invite_obj.code; break
            elif not old_invite_obj and current_uses == 1: 
                found_inviter = new_invite_obj.inviter; used_invite_code = new_invite_obj.code; break
        
        self.invite_cache[guild.id] = new_invites_map

        if found_inviter and used_invite_code:
            is_initially_valid = bool(self.required_role_obj and self.required_role_obj in member.roles)
            idb.record_join(guild.id, member.id, found_inviter.id, used_invite_code, is_initially_valid)
            stats = idb.get_inviter_stats(guild.id, found_inviter.id)
            
            await self._log_invite_action(
                title="Member Joined via Invite",
                color=Color.green(),
                joined_member=member.mention,
                invite_used=f"`discord.gg/{used_invite_code}`",
                inviter=found_inviter.mention,
                inviter_stats=f"{stats['total_valid_invites']} Valid ({stats['total_raw_invites']} Raw)"
            )
            inviter_member_obj = guild.get_member(found_inviter.id)
            if inviter_member_obj: await self._check_and_apply_role_rewards(inviter_member_obj)
        else: 
            vanity_log = ""
            if guild.vanity_url_code: vanity_log = f"(Possible vanity: `discord.gg/{guild.vanity_url_code}`)"
            await self._log_invite_action(
                title="Member Joined",
                color=Color.light_grey(),
                joined_member=member.mention,
                details=f"Invite source unclear. {vanity_log}"
            )

    @commands.Cog.listener()
    async def on_member_remove(self, member: Member):
        if not self.target_guild_id or member.guild.id != self.target_guild_id or member.bot: return
        if self._cog_config_loaded_for_guild != member.guild.id: await self._load_config_and_cache()

        leave_data = idb.record_leave(member.guild.id, member.id) 
        if leave_data:
            inviter_id, was_valid = leave_data
            inviter_member = member.guild.get_member(inviter_id) 
            
            log_fields = {
                "leaving_member": f"{member.display_name} ({member.id})",
                "original_inviter": inviter_member.mention if inviter_member else f"User ID `{inviter_id}`",
                "invite_was_counted_as_valid": "Yes" if was_valid else "No"
            }
            if inviter_member: 
                new_stats = idb.get_inviter_stats(member.guild.id, inviter_id)
                log_fields["inviter_new_stats"] = f"{new_stats['total_valid_invites']} Valid ({new_stats['total_raw_invites']} Raw)"
                await self._check_and_apply_role_rewards(inviter_member)
            
            await self._log_invite_action(
                title="Invited Member Left",
                color=Color.orange(),
                **log_fields
            )
            logging.info(f"Member {member.display_name} left, inviter {inviter_id}'s stats updated (was valid: {was_valid}).")


    @commands.Cog.listener()
    async def on_member_update(self, before: Member, after: Member):
        if not self.target_guild_id or after.guild.id != self.target_guild_id or after.bot: return
        if not self.required_role_obj: return # No required role set
        if self._cog_config_loaded_for_guild != after.guild.id: await self._load_config_and_cache()

        before_has_role = self.required_role_obj in before.roles
        after_has_role = self.required_role_obj in after.roles

        if before_has_role != after_has_role:
            logging.debug(f"Required role status changed for {after.display_name} ({after.id}).")
            invited_details = idb.get_invited_member_details(after.guild.id, after.id)
            if invited_details and invited_details['inviter_user_id']:
                inviter_id = invited_details['inviter_user_id']
                was_previously_valid = bool(invited_details['is_currently_valid'])
                if was_previously_valid != after_has_role:
                    idb.update_invited_member_validity(after.guild.id, after.id, inviter_id, after_has_role)
                    action_taken = "gained" if after_has_role else "lost"
                    color = Color.blue() if after_has_role else Color.dark_orange()
                    await self._log_invite_action(
                        title=f"Invite Validity Changed (Role {action_taken.capitalize()})",
                        color=color,
                        member_actioned=after.mention,
                        role_involved=self.required_role_obj,
                        details=f"Inviter {inviter_id}'s valid invite count has been updated."
                    )
                    inviter_member_obj = after.guild.get_member(inviter_id)
                    if inviter_member_obj: await self._check_and_apply_role_rewards(inviter_member_obj)

    async def _check_and_apply_role_rewards(self, member: Member): 
        # (Same as before, using idb.)
        if not self.target_guild_id or not member or member.bot: return
        if self._cog_config_loaded_for_guild != member.guild.id: await self._load_config_and_cache()
        stats = idb.get_inviter_stats(member.guild.id, member.id); current_valid_invites = stats['total_valid_invites']
        all_rewards = sorted(idb.get_all_role_rewards(member.guild.id), key=lambda x: x['invite_threshold'], reverse=True)
        member_roles_ids = {role.id for role in member.roles}; highest_eligible_reward_role_id: Optional[int] = None
        for reward_setting in all_rewards:
            if current_valid_invites >= reward_setting['invite_threshold']: highest_eligible_reward_role_id = reward_setting['role_id']; break 
        roles_to_add_objs: List[Role] = []; roles_to_remove_objs: List[Role] = []
        for reward_setting in all_rewards:
            reward_role_obj = member.guild.get_role(reward_setting['role_id'])
            if not reward_role_obj: continue
            has_this_reward_role = reward_role_obj.id in member_roles_ids
            if reward_setting['role_id'] == highest_eligible_reward_role_id: 
                if not has_this_reward_role: roles_to_add_objs.append(reward_role_obj)
            elif has_this_reward_role: roles_to_remove_objs.append(reward_role_obj)
        if roles_to_add_objs:
            try:
                await member.add_roles(*roles_to_add_objs, reason="Invite role reward earned (valid invites).")
                for r in roles_to_add_objs: await self._log_invite_action(title="Role Reward Added", color=Color.dark_teal(), member_affected=member, role_involved=r, details=f"Reached {current_valid_invites} valid invites.")
            except Exception as e: await self._log_invite_action(title="Role Reward Add FAILED", color=Color.red(), member_affected=member, details=f"Error: {e}")
        if roles_to_remove_objs:
            try:
                await member.remove_roles(*roles_to_remove_objs, reason="Invite count/validity changed.")
                for r in roles_to_remove_objs: await self._log_invite_action(title="Role Reward Removed", color=Color.dark_gold(), member_affected=member, role_involved=r, details=f"Valid invites: {current_valid_invites}.")
            except Exception as e: await self._log_invite_action(title="Role Reward Remove FAILED", color=Color.red(), member_affected=member, details=f"Error: {e}")


    @tasks.loop(minutes=10)
    async def update_leaderboard_task(self):
        # (Updated to move "Next update" to description and remove bot username/avatar from send)
        if not self.bot.is_ready() or not self.target_guild_id or not self.leaderboard_webhook_url:
            if not self.leaderboard_webhook_url and hasattr(self.bot, 'target_guild_id') and self.bot.target_guild_id:
                 logging.debug("Leaderboard update skipped: Webhook URL not configured.")
            return
        guild = self.bot.get_guild(self.target_guild_id)
        if not guild: logging.error(f"Leaderboard update: Target guild {self.target_guild_id} not found."); return

        top_inviters = idb.get_leaderboard(guild.id, limit=10)
        embed = Embed(title="Invitation Leaderboard", color=Color.gold())
        description_lines = []
        if top_inviters:
            for i, inviter_data in enumerate(top_inviters):
                user = guild.get_member(inviter_data['inviter_user_id']) 
                user_mention = user.mention if user else f"User ID `{inviter_data['inviter_user_id']}`"
                description_lines.append(f"{i+1}. {user_mention} - **{inviter_data['total_valid_invites']}** Valid ({inviter_data['total_raw_invites']} Total)")
        else: description_lines.append("No one has any valid invites yet!")
        
        next_update_timestamp = int((datetime.now(timezone.utc) + timedelta(minutes=10)).timestamp())
        description_lines.append(f"\n\nNext update: <t:{next_update_timestamp}:R>") # Moved to description
        embed.description = "\n".join(description_lines)
        embed.timestamp = datetime.now(timezone.utc) # Actual update time in footer

        try:
            async with aiohttp.ClientSession() as http_session:
                current_webhook = Webhook.from_url(self.leaderboard_webhook_url, session=http_session)
                if self.leaderboard_message_id:
                    try: await current_webhook.edit_message(self.leaderboard_message_id, embed=embed) 
                    except nextcord.NotFound: 
                        message = await current_webhook.send(embed=embed, wait=True) # No username/avatar
                        self.leaderboard_message_id = message.id
                        idb.update_cog_config(self.target_guild_id, 'leaderboard_message_id', message.id)
                        idb.update_cog_config(self.target_guild_id, 'leaderboard_channel_id', message.channel.id)
                    except Exception as e: 
                        logging.error(f"Error editing leaderboard. Sending new. Error: {e}", exc_info=False)
                        message = await current_webhook.send(embed=embed, wait=True) # No username/avatar
                        self.leaderboard_message_id = message.id
                        idb.update_cog_config(self.target_guild_id, 'leaderboard_message_id', message.id)
                        idb.update_cog_config(self.target_guild_id, 'leaderboard_channel_id', message.channel.id)
                else: 
                    message = await current_webhook.send(embed=embed, wait=True) # No username/avatar
                    self.leaderboard_message_id = message.id
                    idb.update_cog_config(self.target_guild_id, 'leaderboard_message_id', message.id)
                    idb.update_cog_config(self.target_guild_id, 'leaderboard_channel_id', message.channel.id)
        except Exception as e: logging.error(f"Leaderboard update: Error with webhook: {e}", exc_info=True)

    @update_leaderboard_task.before_loop
    async def before_leaderboard_update(self):
        await self.bot.wait_until_ready()
        if hasattr(self.bot, 'target_guild_id') and self.bot.target_guild_id:
             if not self.target_guild_id: self.target_guild_id = self.bot.target_guild_id 
             await self._load_config_and_cache() # Use combined method
        logging.info("InviteTrackerCog: Leaderboard update task ready.")

    # --- Slash Commands ---
    # cog_check remains the same
    async def cog_check(self, interaction: Interaction) -> bool:
        if not self.bot.target_guild_id:
            if not interaction.response.is_done(): await interaction.response.defer(ephemeral=True)
            await interaction.followup.send("Bot not ready/target server not identified.", ephemeral=True); return False
        if interaction.guild is None or interaction.guild.id != self.bot.target_guild_id:
            if not interaction.response.is_done(): await interaction.response.defer(ephemeral=True)
            target_guild_name = getattr(self.bot, 'target_guild_name', 'the configured server')
            await interaction.followup.send(f"Use commands in '{target_guild_name}'.", ephemeral=True); return False
        return True

    # Merged /invites command
    @nextcord.slash_command(name="invites", description="Check your own or another user's invite count.")
    async def invites_command(self, interaction: Interaction, 
                              user: Optional[Member] = SlashOption( description="User to check (defaults to yourself). Admin to check others.", required=False)):
        await interaction.response.defer() # Public by default

        target_user = user or interaction.user
        is_self_check = (target_user == interaction.user)

        if not is_self_check and not interaction.permissions.manage_guild:
            await interaction.followup.send("You need 'Manage Server' permission to view other users' invite counts.", ephemeral=True); return

        if target_user.bot:
            msg = "Bots don't have invites!" if is_self_check else f"{target_user.mention} is a bot and cannot have invites."
            await interaction.followup.send(msg, ephemeral=True, allowed_mentions=nextcord.AllowedMentions.none()); return

        stats = idb.get_inviter_stats(interaction.guild.id, target_user.id)
        valid_invites = stats.get('total_valid_invites', 0)
        raw_invites = stats.get('total_raw_invites', 0)

        embed = Embed(color=Color.blue())
        embed.title = "Your Invite Statistics" if is_self_check else f"Invite Statistics for {target_user.display_name}"
        embed.set_thumbnail(url=target_user.display_avatar.url)
        embed.add_field(name="User", value=target_user.mention, inline=False)
        embed.add_field(name="Valid Invites", value=f"**{valid_invites}** (Verified Members)", inline=False) # User's wording
        embed.add_field(name="Total Invites", value=f"**{raw_invites}** (Invited members who are still in server)", inline=False) # User's wording
        
        if self.required_role_obj:
            embed.set_footer(text="A 'valid' invite means the invited member has successfully verified in the server.") # User's wording
        else:
            embed.set_footer(text="Note: 'Valid invites' count relies on the 'required role' being set via /inviteset.") # User's wording
        
        await interaction.followup.send(embed=embed, allowed_mentions=nextcord.AllowedMentions(users=[interaction.user]) if is_self_check else nextcord.AllowedMentions.none())


    @nextcord.slash_command(name="inviteset", description="Configure invite tracker settings (Admin).")
    @application_checks.has_permissions(manage_guild=True)
    async def inviteset_group(self, interaction: Interaction): pass

    @inviteset_group.subcommand(name="log_channel", description="Sets the channel for invite join/leave logs.")
    async def set_invite_log_channel(self, interaction: Interaction, channel: TextChannel = SlashOption(required=True)):
        await interaction.response.defer(ephemeral=True)
        idb.update_cog_config(interaction.guild.id, 'log_channel_id', channel.id)
        await self._load_config_and_cache() 
        await interaction.followup.send(f"Invite log channel set to {channel.mention}.", ephemeral=True)

    @inviteset_group.subcommand(name="leaderboard_webhook", description="Sets webhook URL for invite leaderboard.")
    async def set_leaderboard_webhook(self, interaction: Interaction, webhook_url: str = SlashOption(description="The full Discord webhook URL", required=True)):
        await interaction.response.defer(ephemeral=True)
        if not webhook_url.startswith("https://discord.com/api/webhooks/"): await interaction.followup.send("Invalid webhook URL format.", ephemeral=True); return
        try:
            async with aiohttp.ClientSession() as session:
                temp_webhook = Webhook.from_url(webhook_url, session=session)
                # Send initial message WITHOUT bot's username/avatar
                test_message = await temp_webhook.send("Invitation Leaderboard\nInitializing... the first update will appear shortly.", wait=True) 
            idb.update_cog_config(interaction.guild.id, 'leaderboard_webhook_url', webhook_url)
            idb.update_cog_config(interaction.guild.id, 'leaderboard_message_id', test_message.id)
            idb.update_cog_config(interaction.guild.id, 'leaderboard_channel_id', test_message.channel.id)
            await self._load_config_and_cache() 
            await interaction.followup.send(f"Leaderboard webhook set & initialized in <#{test_message.channel.id}>.", ephemeral=True)
            if self.update_leaderboard_task.is_running(): self.update_leaderboard_task.restart()
            else: self.update_leaderboard_task.start()
        except Exception as e: await interaction.followup.send(f"Error with webhook: {e}", ephemeral=True); logging.error("Webhook setup error", exc_info=True)

    @inviteset_group.subcommand(name="set_required_role", description="Role an invited member needs for the invite to be 'valid'.")
    async def set_required_role_for_valid_invite(self, interaction: Interaction, role: Optional[Role] = SlashOption(description="The role required. Select None/empty to clear.", required=False)):
        await interaction.response.defer(ephemeral=True)
        role_id_to_set = role.id if role else None
        role_name_to_set = role.name if role else "None (any invite is valid if member stays)"
        idb.update_cog_config(interaction.guild.id, 'required_role_id', role_id_to_set)
        await self._load_config_and_cache() 
        await interaction.followup.send(f"Required role for invites to be valid set to: {role_name_to_set}.", ephemeral=True)
        await self._log_invite_action(title="Invite Config Updated", description=f"Required role for valid invites set to '{role_name_to_set}' by {interaction.user.mention}.", color=Color.blurple())


    @nextcord.slash_command(name="compensate_invites", description="Manually adjust a user's invite counts (Admin).")
    @application_checks.has_permissions(manage_guild=True)
    async def compensate_invites_cmd(self, interaction: Interaction, user: Member = SlashOption(required=True), action: str = SlashOption(choices={"add": "add", "remove": "remove"}, required=True), amount: int = SlashOption(min_value=1, required=True), reason: Optional[str] = SlashOption(required=False)):
        await interaction.response.defer(ephemeral=True)
        if user.bot: await interaction.followup.send("Cannot compensate for a bot.", ephemeral=True); return
        if idb.compensate_invites(interaction.guild.id, user.id, amount, action):
            stats = idb.get_inviter_stats(interaction.guild.id, user.id)
            await self._check_and_apply_role_rewards(user)
            reason_text = f"\nReason: {reason}" if reason else ""
            log_desc = f"Invites for {user.mention} adjusted by {interaction.user.mention}: **{action}ed {amount}** (affects raw & valid).\nNew Valid: {stats['total_valid_invites']}\nNew Raw: {stats['total_raw_invites']}{reason_text}"
            await self._log_invite_action(title="Invites Compensated", description=log_desc, color=Color.purple())
            await interaction.followup.send(f"Successfully {action}ed {amount} invites for {user.mention}. New valid: {stats['total_valid_invites']}, new raw: {stats['total_raw_invites']}.", ephemeral=True)
        else: await interaction.followup.send(f"Failed to compensate invites for {user.mention}.", ephemeral=True)

    @nextcord.slash_command(name="invitereward", description="Configure rewards based on VALID invites (Admin).")
    @application_checks.has_permissions(manage_guild=True)
    async def invitereward_group(self, interaction: Interaction): pass

    @invitereward_group.subcommand(name="add", description="Add a role reward for a VALID invite threshold.")
    async def invitereward_add(self, interaction: Interaction, invite_threshold: int = SlashOption(min_value=1, required=True), role: Role = SlashOption(required=True)):
        await interaction.response.defer(ephemeral=True)
        if role.is_default() or role.is_bot_managed() or role.is_premium_subscriber() or role.is_integration():
            await interaction.followup.send("Cannot use this type of role as a reward.", ephemeral=True); return
        bot_member = interaction.guild.me
        if bot_member.top_role <= role: await interaction.followup.send(f"I cannot manage {role.mention} (my role is lower/equal).", ephemeral=True); return
        if idb.add_role_reward(interaction.guild.id, invite_threshold, role.id):
            await interaction.followup.send(f"Role {role.mention} set for {invite_threshold} valid invites.", ephemeral=True)
            await self._log_invite_action(title="Invite Reward Added", description=f"Role: {role.mention}\nThreshold: {invite_threshold} valid invites\nSet by: {interaction.user.mention}", color=Color.teal())
        else: await interaction.followup.send(f"Failed to add (threshold or role might already exist).", ephemeral=True)

    @invitereward_group.subcommand(name="remove", description="Remove a role reward configuration.")
    async def invitereward_remove(self, interaction: Interaction, role: Role = SlashOption(required=True)):
        await interaction.response.defer(ephemeral=True)
        if idb.remove_role_reward(interaction.guild.id, role.id):
            await interaction.followup.send(f"Role {role.mention} removed from invite rewards.", ephemeral=True)
            await self._log_invite_action(title="Invite Reward Removed", description=f"Role: {role.mention}\nRemoved by: {interaction.user.mention}", color=Color.dark_gold())
        else: await interaction.followup.send(f"Role {role.mention} not found in reward configs.", ephemeral=True)

    @invitereward_group.subcommand(name="list", description="List current invite role rewards (based on valid invites).")
    async def invitereward_list(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        rewards = idb.get_all_role_rewards(interaction.guild.id)
        if not rewards: await interaction.followup.send("No role rewards configured.", ephemeral=True); return
        embed = Embed(title="Invite Role Rewards (based on Valid Invites)", color=Color.purple())
        description = ""
        for reward_data in rewards: 
            role = interaction.guild.get_role(reward_data['role_id'])
            role_mention = role.mention if role else f"ID {reward_data['role_id']} (Not Found?)"
            description += f"- **{reward_data['invite_threshold']} Valid Invites** -> {role_mention}\n"
        embed.description = description if description else "No rewards set."
        await interaction.followup.send(embed=embed, ephemeral=True)


def setup(bot: commands.Bot):
    global aiohttp 
    try: import aiohttp
    except ImportError: logging.error("InviteTrackerCog: aiohttp library not installed, needed for webhooks.")
    bot.add_cog(InviteTrackerCog(bot))