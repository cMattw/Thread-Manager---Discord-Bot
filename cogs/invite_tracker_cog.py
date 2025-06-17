import nextcord
from nextcord.ext import commands, tasks, application_checks
from nextcord import (
    Interaction, SlashOption, Member, Role, TextChannel,
    Embed, Color, Invite, Webhook, User, ui # Added ui for View/Button
)
# Assuming invites_database.py is in db_utils folder
from db_utils import invites_database as idb
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Set
import asyncio
import aiohttp # For creating webhook sessions
import pytz # For MANILA_TZ if used for specific display

# For plain text logs, UTC is fine. If specific TZ needed for display:
MANILA_TZ = pytz.timezone("Asia/Manila")

# --- Pagination View for /invited command ---
ITEMS_PER_PAGE_INVITED = 10 # Max members to show per page in /invited

class InvitedListView(ui.View):
    def __init__(self, interaction: Interaction, data: List[Dict[str, Any]], target_user: Member, title_prefix: str):
        super().__init__(timeout=180)  # 3 minutes
        self.interaction = interaction
        self.data = data
        self.target_user = target_user
        self.title_prefix = title_prefix
        self.current_page = 0
        self.total_pages = (len(self.data) - 1) // ITEMS_PER_PAGE_INVITED + 1
        self.update_buttons()

    def format_page_description(self) -> str:
        start_index = self.current_page * ITEMS_PER_PAGE_INVITED
        end_index = start_index + ITEMS_PER_PAGE_INVITED
        page_data = self.data[start_index:end_index]

        if not page_data:
            return "No members found on this page."

        description_lines = []
        for invitee_data in page_data:
            member_obj = self.interaction.guild.get_member(invitee_data['member_id'])
            if member_obj:
                joined_at_obj = invitee_data.get('joined_at')
                joined_at_ts = int(joined_at_obj.timestamp()) if isinstance(joined_at_obj, datetime) else None
                joined_at_str = f"<t:{joined_at_ts}:R>" if joined_at_ts else "Unknown join time"
                invite_code_str = f" (Code: `{invitee_data.get('used_invite_code', 'N/A')}`)"
                description_lines.append(f"- {member_obj.mention} {member_obj.display_name}{invite_code_str} - Joined: {joined_at_str}")
            else:
                 description_lines.append(f"- User ID `{invitee_data['member_id']}` (Not found in server cache)")
        return "\n".join(description_lines)

    def get_embed(self) -> Embed:
        embed = Embed(color=Color.green())
        embed.title = f"{self.title_prefix} (Page {self.current_page + 1}/{self.total_pages})"
        if self.target_user.display_avatar:
            embed.set_thumbnail(url=self.target_user.display_avatar.url)
        
        embed.description = self.format_page_description()
        
        embed.set_footer(text=f"Inviter: {self.target_user.display_name} ({self.target_user.id})")
        embed.timestamp = datetime.now(timezone.utc)
        return embed

    def update_buttons(self):
        # Previous Button
        prev_button = next((item for item in self.children if isinstance(item, ui.Button) and item.custom_id == "prev_page"), None)
        if prev_button: prev_button.disabled = self.current_page == 0
        
        # Next Button
        next_button = next((item for item in self.children if isinstance(item, ui.Button) and item.custom_id == "next_page"), None)
        if next_button: next_button.disabled = self.current_page >= self.total_pages - 1

    async def show_current_page(self):
        self.update_buttons()
        await self.interaction.edit_original_message(embed=self.get_embed(), view=self)

    @ui.button(label="Previous", style=nextcord.ButtonStyle.grey, custom_id="prev_page")
    async def previous_page_button(self, button: ui.Button, interaction: Interaction):
        if interaction.user.id != self.interaction.user.id:
            await interaction.response.send_message("You cannot control this pagination.", ephemeral=True)
            return
        if self.current_page > 0:
            self.current_page -= 1
            await interaction.response.defer() # Acknowledge click
            await self.show_current_page()
        else: # Should be disabled, but as a fallback
            await interaction.response.defer()


    @ui.button(label="Next", style=nextcord.ButtonStyle.grey, custom_id="next_page")
    async def next_page_button(self, button: ui.Button, interaction: Interaction):
        if interaction.user.id != self.interaction.user.id:
            await interaction.response.send_message("You cannot control this pagination.", ephemeral=True)
            return
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            await interaction.response.defer() # Acknowledge click
            await self.show_current_page()
        else: # Should be disabled
            await interaction.response.defer()

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        try:
            await self.interaction.edit_original_message(view=self)
        except nextcord.NotFound:
            pass # Message might have been deleted


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
            await self._load_config_and_cache()
            if not self.update_leaderboard_task.is_running() and self.leaderboard_webhook_url:
                self.update_leaderboard_task.start()
            logging.info("InviteTrackerCog loaded and configured for target guild.")
        else:
            logging.error("InviteTrackerCog: Target guild ID not set on bot. Cog will not function correctly.")
            if self.update_leaderboard_task.is_running(): self.update_leaderboard_task.cancel()

    async def _load_config_and_cache(self):
        if not self.target_guild_id:
            logging.warning("InviteTrackerCog: Cannot load config/cache, target_guild_id not set.")
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

        if self.target_guild_id:
            await self._cache_invites(self.target_guild_id)

    def cog_unload(self):
        self.update_leaderboard_task.cancel()
        logging.info("InviteTrackerCog: Tasks cancelled.")

    async def _log_invite_action(self, title: str, color: Color = Color.blue(), **fields_data):
        if self.target_guild_id and self._cog_config_loaded_for_guild != self.target_guild_id:
            await self._load_config_and_cache()

        if not self.log_channel_obj:
            log_parts = [f"InviteTrackerCog (Guild {self.target_guild_id}, No LogCh): {title}"]
            for key, value in fields_data.items():
                if value is not None: log_parts.append(f"{key.replace('_', ' ').title()}: {value}")
            logging.info(" | ".join(log_parts))
            return

        embed = Embed(title=f"Invite Tracker: {title}", color=color)
        embed.timestamp = datetime.now(timezone.utc)

        field_count = 0
        for key, value in fields_data.items():
            if value is not None:
                if field_count >= 24:
                    embed.add_field(name="More Info...", value="Too many details for one embed.", inline=False); break
                name = key.replace("_", " ").title(); val_str = str(value)
                is_inline = (isinstance(value, (Member, User, Role, TextChannel)) or \
                            (isinstance(value, str) and len(val_str) < 40 and '\n' not in val_str)) \
                            and key not in ["details", "reason", "inviter_stats", "content", "leaving_member"]
                if len(val_str) > 1020: val_str = val_str[:1020] + "..."
                if len(name) > 250: name = name[:250] + "..."
                if name == "Invite Used" and "`" in val_str: pass
                elif isinstance(value, (Member, User, Role, TextChannel)) and hasattr(value, 'mention'): val_str = value.mention
                embed.add_field(name=name, value=val_str, inline=is_inline); field_count += 1
        try:
            await self.log_channel_obj.send(embed=embed)
        except Exception as e: logging.error(f"InviteTrackerCog: Error sending embed log: {e}", exc_info=True)

    async def _cache_invites(self, guild_id: int):
        guild = self.bot.get_guild(guild_id)
        if not guild: logging.error(f"InviteTrackerCog: Guild {guild_id} not found for caching."); return
        try:
            self.invite_cache[guild.id] = {invite.code: invite for invite in await guild.invites()}
            logging.info(f"InviteTrackerCog: Cached {len(self.invite_cache[guild.id])} invites for {guild.name}")
        except nextcord.Forbidden: logging.error(f"InviteTrackerCog: Missing 'Manage Server' for guild {guild.name} to cache invites.")
        except Exception as e: logging.error(f"InviteTrackerCog: Error caching invites for {guild.name}: {e}", exc_info=True)

    @commands.Cog.listener()
    async def on_ready(self):
        await self.bot.wait_until_ready()
        if hasattr(self.bot, 'target_guild_id') and self.bot.target_guild_id:
            self.target_guild_id = self.bot.target_guild_id
            if self._cog_config_loaded_for_guild != self.target_guild_id:
                idb.initialize_database(self.target_guild_id)
                await self._load_config_and_cache()

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

    @commands.Cog.listener()
    async def on_member_join(self, member: Member):
        if not self.target_guild_id or member.guild.id != self.target_guild_id or member.bot:
            return

        if self._cog_config_loaded_for_guild != member.guild.id:
            await self._load_config_and_cache()

        guild = member.guild
        old_invites_map_for_comparison = self.invite_cache.get(guild.id, {}).copy()

        await asyncio.sleep(3.5)

        new_invites_map: Dict[str, Invite] = {}
        try:
            current_invites_list = await guild.invites()
            new_invites_map = {invite.code: invite for invite in current_invites_list}
        except nextcord.Forbidden:
            await self._log_invite_action(
                title="Member Joined - Invite Check Failed", color=Color.red(),
                Joined_Member=member, Details="Bot lacks 'Manage Server' permission to determine the invite source accurately."
            )
            return
        except Exception as e:
            await self._log_invite_action(
                title="Member Joined - Invite Check Error", color=Color.red(),
                Joined_Member=member, Details=f"An error occurred fetching current invites: {str(e)[:150]}"
            )
            return

        found_inviter: Optional[User] = None
        used_invite_code: Optional[str] = None

        for code, new_invite_obj in new_invites_map.items():
            old_invite_obj = old_invites_map_for_comparison.get(code)
            current_uses = new_invite_obj.uses or 0
            previous_uses = old_invite_obj.uses if old_invite_obj and old_invite_obj.uses is not None else 0

            if current_uses > previous_uses:
                if new_invite_obj.inviter:
                    found_inviter = new_invite_obj.inviter
                    used_invite_code = new_invite_obj.code
                    logging.info(f"InviteTracker: Standard invite {used_invite_code} by {found_inviter} detected for {member.display_name} (Uses: {previous_uses} -> {current_uses}).")
                else:
                    logging.warning(f"InviteTracker: Invite code {new_invite_obj.code} usage increased ({previous_uses} -> {current_uses}), but inviter object is None.")
                break
            elif not old_invite_obj and current_uses >= 1 and new_invite_obj.inviter:
                found_inviter = new_invite_obj.inviter
                used_invite_code = new_invite_obj.code
                logging.info(f"InviteTracker: New standard invite {used_invite_code} (not in old cache) by {found_inviter} detected for {member.display_name} (Uses: {current_uses}).")
                break

        self.invite_cache[guild.id] = new_invites_map

        if found_inviter and used_invite_code:
            is_initially_valid = bool(self.required_role_obj and self.required_role_obj in member.roles)
            idb.record_join(guild.id, member.id, found_inviter.id, used_invite_code, is_initially_valid)
            stats = idb.get_inviter_stats(guild.id, found_inviter.id)
            await self._log_invite_action(
                title="Member Joined (Standard Invite)", color=Color.green(),
                Joined_Member=member, Invite_Used=f"`discord.gg/{used_invite_code}`",
                Inviter=found_inviter, Inviter_Stats=f"{stats['total_valid_invites']} Valid ({stats['total_raw_invites']} Total)"
            )
            inviter_member_obj = guild.get_member(found_inviter.id)
            if inviter_member_obj:
                await self._check_and_apply_role_rewards(inviter_member_obj)
        else:
            vanity_invite_used_and_logged = False
            try:
                vanity_invite_obj = await guild.vanity_invite() # Corrected method
                if vanity_invite_obj:
                    await self._log_invite_action(
                        title="Member Joined (Vanity Invite)", color=Color.blue(),
                        Joined_Member=member, Details=f"Joined using the server vanity invite"
                    )
                    vanity_invite_used_and_logged = True
            except nextcord.Forbidden:
                await self._log_invite_action(
                    title="Member Joined (Vanity Check Failed)", color=Color.light_grey(),
                    Joined_Member=member, Details="Could not check for vanity invite due to missing 'Manage Server' permission."
                )
                vanity_invite_used_and_logged = True
            except nextcord.NotFound:
                 logging.debug(f"No vanity invite set for guild {guild.name} or bot cannot find it.")
            except Exception as e_vanity:
                logging.warning(f"Error trying to fetch/process vanity invite for guild {guild.id}: {e_vanity}")

            if not vanity_invite_used_and_logged:
                await self._log_invite_action(
                    title="Member Joined (Unknown Source)", color=Color.orange(),
                    Joined_Member=member, Details="Invite source could not be determined through standard invite tracking or vanity check."
                )

    @commands.Cog.listener()
    async def on_member_remove(self, member: Member):
        if not self.target_guild_id or member.guild.id != self.target_guild_id or member.bot: return
        if self._cog_config_loaded_for_guild != member.guild.id: await self._load_config_and_cache()
        leave_data = idb.record_leave(member.guild.id, member.id)
        if leave_data:
            inviter_id, was_valid = leave_data; inviter_member = member.guild.get_member(inviter_id)
            log_fields = {"leaving_member": f"{member.display_name} ({member.id})",
                          "original_inviter": inviter_member.mention if inviter_member else f"User ID `{inviter_id}`",
                          "invite_was_valid": "Yes" if was_valid else "No"}
            if inviter_member:
                await self._check_and_apply_role_rewards(inviter_member)
                new_stats = idb.get_inviter_stats(member.guild.id, inviter_id)
                log_fields["inviter_new_stats"] = f"{new_stats['total_valid_invites']} Valid ({new_stats['total_raw_invites']} Total)"
            await self._log_invite_action(title="Invited Member Left", color=Color.orange(), **log_fields)

    @commands.Cog.listener()
    async def on_member_update(self, before: Member, after: Member):
        if not self.target_guild_id or after.guild.id != self.target_guild_id or after.bot: return
        if not self.required_role_obj: return
        if self._cog_config_loaded_for_guild != after.guild.id: await self._load_config_and_cache()
        before_has_role = self.required_role_obj in before.roles; after_has_role = self.required_role_obj in after.roles
        if before_has_role != after_has_role:
            invited_details = idb.get_invited_member_details(after.guild.id, after.id)
            if invited_details and invited_details.get('inviter_user_id'): # Ensure inviter_user_id exists
                inviter_id = invited_details['inviter_user_id']; was_previously_valid = bool(invited_details['is_currently_valid'])
                if was_previously_valid != after_has_role:
                    idb.update_invited_member_validity(after.guild.id, after.id, inviter_id, after_has_role)
                    action_taken = "gained" if after_has_role else "lost"; color = Color.blue() if after_has_role else Color.dark_orange()
                    inviter_member_obj = after.guild.get_member(inviter_id)
                    details_str = f"Inviter: {inviter_member_obj.mention if inviter_member_obj else f'ID {inviter_id}'}"
                    if inviter_member_obj:
                        await self._check_and_apply_role_rewards(inviter_member_obj)
                        current_stats = idb.get_inviter_stats(after.guild.id, inviter_id)
                        details_str += f"\nInviter's new stats: {current_stats['total_valid_invites']} Valid ({current_stats['total_raw_invites']} Total)"
                    await self._log_invite_action(title=f"Invite Validity Changed (Role {action_taken.capitalize()})", color=color, member_actioned=after, role_involved=self.required_role_obj, details=details_str)

    async def _check_and_apply_role_rewards(self, member: Member):
        if not self.target_guild_id or not member or member.bot: return
        if self._cog_config_loaded_for_guild != member.guild.id: await self._load_config_and_cache()
        stats = idb.get_inviter_stats(member.guild.id, member.id); current_valid_invites = stats['total_valid_invites']
        all_rewards = sorted(idb.get_all_role_rewards(member.guild.id), key=lambda x: x['invite_threshold'], reverse=True)
        member_roles_ids = {role.id for role in member.roles}; highest_eligible_reward_role_id: Optional[int] = None
        for r_setting in all_rewards:
            if current_valid_invites >= r_setting['invite_threshold']: highest_eligible_reward_role_id = r_setting['role_id']; break
        roles_to_add: List[Role] = []; roles_to_remove: List[Role] = []
        for r_setting in all_rewards:
            reward_role = member.guild.get_role(r_setting['role_id'])
            if not reward_role: continue
            has_this = reward_role.id in member_roles_ids
            if r_setting['role_id'] == highest_eligible_reward_role_id:
                if not has_this: roles_to_add.append(reward_role)
            elif has_this: roles_to_remove.append(reward_role)
        if roles_to_add:
            try:
                await member.add_roles(*roles_to_add, reason="Invite role reward earned.")
                for r_obj in roles_to_add: await self._log_invite_action(title="Role Reward Added", color=Color.dark_teal(), member_affected=member, role_involved=r_obj, details=f"Reached {current_valid_invites} valid invites.")
            except Exception as e: await self._log_invite_action(title="Role Reward Add FAILED", color=Color.red(), member_affected=member, details=f"Error: {str(e)[:200]}")
        if roles_to_remove:
            try:
                await member.remove_roles(*roles_to_remove, reason="Invite count/validity changed.")
                for r_obj in roles_to_remove: await self._log_invite_action(title="Role Reward Removed", color=Color.dark_gold(), member_affected=member, role_involved=r_obj, details=f"Valid invites: {current_valid_invites}.")
            except Exception as e: await self._log_invite_action(title="Role Reward Remove FAILED", color=Color.red(), member_affected=member, details=f"Error: {str(e)[:200]}")

    @tasks.loop(minutes=10)
    async def update_leaderboard_task(self, *args, **kwargs):
        if not self.bot.is_ready() or not self.target_guild_id or not self.leaderboard_webhook_url:
            if not self.leaderboard_webhook_url and hasattr(self.bot, 'target_guild_id') and self.bot.target_guild_id: logging.debug("Leaderboard update skipped: Webhook URL not configured.")
            return
        guild = self.bot.get_guild(self.target_guild_id)
        if not guild: logging.error(f"Leaderboard update: Target guild {self.target_guild_id} not found."); return
        top_inviters = idb.get_leaderboard(guild.id, limit=10)
        embed = Embed(title="Invitation Leaderboard", color=Color.gold())
        description_lines = []
        if top_inviters:
            for i, inviter_data in enumerate(top_inviters):
                user = guild.get_member(inviter_data['inviter_user_id']); user_mention = user.mention if user else f"User ID `{inviter_data['inviter_user_id']}`"
                description_lines.append(f"{i+1}. {user_mention} - **{inviter_data['total_valid_invites']}** Invites")
        else: description_lines.append("No one has any valid invites yet!")
        next_update_interval = self.update_leaderboard_task.minutes if self.update_leaderboard_task.minutes else 10
        next_update_timestamp = int((datetime.now(timezone.utc) + timedelta(minutes=next_update_interval)).timestamp())
        description_lines.append(f"\n\nNext update: <t:{next_update_timestamp}:R>")
        embed.description = "\n".join(description_lines); embed.timestamp = datetime.now(timezone.utc)
        try:
            async with aiohttp.ClientSession() as http_session:
                current_webhook = Webhook.from_url(self.leaderboard_webhook_url, session=http_session)
                if self.leaderboard_message_id:
                    try: await current_webhook.edit_message(self.leaderboard_message_id, embed=embed)
                    except nextcord.NotFound:
                        message = await current_webhook.send(embed=embed, wait=True)
                        self.leaderboard_message_id = message.id
                        idb.update_cog_config(self.target_guild_id, 'leaderboard_message_id', message.id)
                        idb.update_cog_config(self.target_guild_id, 'leaderboard_channel_id', message.channel.id) # Store channel ID for future ref
                    except Exception as e_edit:
                        logging.error(f"Error editing leaderboard: {e_edit}. Sending new.", exc_info=False)
                        message = await current_webhook.send(embed=embed, wait=True)
                        self.leaderboard_message_id = message.id
                        idb.update_cog_config(self.target_guild_id, 'leaderboard_message_id', message.id)
                        idb.update_cog_config(self.target_guild_id, 'leaderboard_channel_id', message.channel.id)
                else:
                    message = await current_webhook.send(embed=embed, wait=True)
                    self.leaderboard_message_id = message.id
                    idb.update_cog_config(self.target_guild_id, 'leaderboard_message_id', message.id)
                    idb.update_cog_config(self.target_guild_id, 'leaderboard_channel_id', message.channel.id)
        except Exception as e: logging.error(f"Leaderboard update: Error with webhook: {e}", exc_info=True)

    @update_leaderboard_task.before_loop
    async def before_leaderboard_update(self):
        await self.bot.wait_until_ready()
        if hasattr(self.bot, 'target_guild_id') and self.bot.target_guild_id:
             if not self.target_guild_id: self.target_guild_id = self.bot.target_guild_id
             await self._load_config_and_cache()
        logging.info("InviteTrackerCog: Leaderboard update task ready.")

    async def cog_check(self, interaction: Interaction) -> bool:
        if not self.bot.target_guild_id:
            if not interaction.response.is_done(): await interaction.response.defer(ephemeral=True)
            await interaction.followup.send("Bot not ready/target server not identified.", ephemeral=True); return False
        if interaction.guild is None or interaction.guild.id != self.bot.target_guild_id:
            if not interaction.response.is_done(): await interaction.response.defer(ephemeral=True)
            target_guild_name = getattr(self.bot, 'target_guild_name', 'the configured server')
            await interaction.followup.send(f"Use commands in '{target_guild_name}'.", ephemeral=True); return False
        return True

    @nextcord.slash_command(name="invites", description="Check invite counts.")
    async def invites_command(self, interaction: Interaction, user: Optional[Member] = SlashOption( description="User to check (defaults to yourself). Admin to check others.", required=False)):
        await interaction.response.defer()
        target_user = user or interaction.user; is_self_check = (target_user == interaction.user)
        if not is_self_check and not interaction.permissions.manage_guild:
            await interaction.followup.send("You need 'Manage Server' permission to view others' invites.", ephemeral=True); return
        if target_user.bot:
            msg = "Bots don't have invites!" if is_self_check else f"{target_user.mention} is a bot."
            await interaction.followup.send(msg, ephemeral=True, allowed_mentions=nextcord.AllowedMentions.none()); return
        stats = idb.get_inviter_stats(interaction.guild.id, target_user.id)
        valid_invites = stats.get('total_valid_invites', 0); raw_invites = stats.get('total_raw_invites', 0)
        embed = Embed(color=Color.blue())
        embed.title = "Your Invite Statistics" if is_self_check else f"Invite Statistics for {target_user.display_name}"
        if target_user.display_avatar: embed.set_thumbnail(url=target_user.display_avatar.url)
        embed.add_field(name="User", value=target_user.mention, inline=False)
        embed.add_field(name="Total Invites", value=f"**{valid_invites}** (Invited members who are still in server)", inline=False)
        if self.required_role_obj: embed.set_footer(text=f"A 'valid' invite means the invited member is still on the server.")
        else: embed.set_footer(text="Note: 'Valid invites' count relies on the 'required role' being set.")
        await interaction.followup.send(embed=embed, allowed_mentions=nextcord.AllowedMentions(users=[interaction.user]) if is_self_check else nextcord.AllowedMentions.none())

    # --- NEW /invited COMMAND WITH PAGINATION ---
    @nextcord.slash_command(name="invited", description="Shows members invited by a user who are still in the server.")
    async def invited_command(self,
                              interaction: Interaction,
                              user: Optional[Member] = SlashOption(
                                  description="User whose invited members to list (defaults to yourself). Admin to check others.",
                                  required=False
                              )):
        await interaction.response.defer() # Defer immediately

        target_user = user or interaction.user
        is_self_check = (target_user == interaction.user)

        if not is_self_check and not interaction.permissions.manage_guild:
            await interaction.followup.send("You need 'Manage Server' permission to view another user's invited members.", ephemeral=True)
            return

        if target_user.bot:
            msg = "Bots don't invite users!" if is_self_check else f"{target_user.mention} is a bot and cannot invite users."
            await interaction.followup.send(msg, ephemeral=True, allowed_mentions=nextcord.AllowedMentions.none())
            return

        active_invitees_data = idb.get_active_invitees(interaction.guild.id, target_user.id)

        title_prefix = "Members You Invited" if is_self_check else f"Members Invited by {target_user.display_name}"
        full_title = f"{title_prefix}"

        if not active_invitees_data:
            embed = Embed(
                title=full_title,
                description="No members found that were invited by this user and are still in the server.",
                color=Color.blue() # Or Color.orange()
            )
            if target_user.display_avatar:
                embed.set_thumbnail(url=target_user.display_avatar.url)
            embed.set_footer(text=f"Inviter: {target_user.display_name}")
            embed.timestamp = datetime.now(timezone.utc)
            await interaction.followup.send(embed=embed, allowed_mentions=nextcord.AllowedMentions.none())
            return

        # Create and send the paginated view
        view = InvitedListView(interaction, active_invitees_data, target_user, title_prefix)
        initial_embed = view.get_embed() # Get embed for the first page
        await interaction.followup.send(embed=initial_embed, view=view)


    # --- NEW /inviter COMMAND ---
    @nextcord.slash_command(name="inviter", description="Shows who invited a specific member to the server.")
    async def inviter_command(self,
                              interaction: Interaction,
                              member: Optional[Member] = SlashOption(
                                  name="user", # Keep slash option name as "user" for consistency
                                  description="Member whose inviter to check (defaults to yourself). Admin to check others'.",
                                  required=False
                              )):
        await interaction.response.defer() # Defer immediately

        target_member = member or interaction.user
        is_self_check = (target_member == interaction.user)

        if not is_self_check and not interaction.permissions.manage_guild:
            await interaction.followup.send("You need 'Manage Server' permission to check who invited another member.", ephemeral=True)
            return

        invite_details = idb.get_invited_member_details(interaction.guild.id, target_member.id)

        embed = Embed(color=Color.blue())
        title_prefix = "How You Were Invited" if is_self_check else f"How {target_member.display_name} Was Invited"
        embed.title = title_prefix
        if target_member.display_avatar:
            embed.set_thumbnail(url=target_member.display_avatar.url)

        if invite_details and invite_details.get('inviter_user_id'):
            inviter_id = invite_details['inviter_user_id']
            inviter_user_obj = interaction.guild.get_member(inviter_id)
            if not inviter_user_obj:
                 try: inviter_user_obj = await self.bot.fetch_user(inviter_id)
                 except nextcord.NotFound: inviter_user_obj = None

            inviter_mention = "Unknown User"
            if inviter_user_obj:
                inviter_mention = f"{inviter_user_obj.mention} ({inviter_user_obj.display_name})"
            elif inviter_id:
                inviter_mention = f"User ID `{inviter_id}` (User not found or left)"

            used_code = invite_details.get('invite_code', 'N/A')
            joined_at_obj = invite_details.get('joined_at')
            joined_at_ts = int(joined_at_obj.timestamp()) if isinstance(joined_at_obj, datetime) else None
            joined_at_str = f"<t:{joined_at_ts}:f>" if joined_at_ts else "Unknown"

            embed.add_field(name="Invited By", value=inviter_mention, inline=False)
            embed.add_field(name="Invite Code Used", value=f"`{used_code}`", inline=True)
            embed.add_field(name="Joined At", value=joined_at_str, inline=True)

            if self.required_role_obj: # Check if required role is configured for the server
                is_valid_str = "Yes" if invite_details.get('is_currently_valid') else "No"
                embed.add_field(name=f"Invite Counts as Valid?", value=is_valid_str, inline=True)

        elif invite_details and invite_details.get('used_invite_code') and not invite_details.get('inviter_user_id'):
            embed.description = f"{target_member.mention} joined using invite code `{invite_details['used_invite_code']}`."
            embed.add_field(name="Inviter", value="System or Vanity URL (no specific user inviter recorded).", inline=False)
        else:
            embed.description = f"Could not determine who invited {target_member.mention}."
            embed.add_field(name="Details", value="They might have joined before invite tracking was active, via a method not tracked, or their invite data is unavailable.", inline=False)

        embed.set_footer(text=f"Queried Member: {target_member.display_name}")
        embed.timestamp = datetime.now(timezone.utc)

        await interaction.followup.send(embed=embed, allowed_mentions=nextcord.AllowedMentions.none())


    @nextcord.slash_command(name="inviteset", description="Configure invite tracker settings (Admin).")
    @application_checks.has_permissions(manage_guild=True)
    async def inviteset_group(self, interaction: Interaction): pass

    @inviteset_group.subcommand(name="log_channel", description="Sets the channel for invite join/leave logs.")
    async def set_invite_log_channel(self, interaction: Interaction, channel: TextChannel = SlashOption(required=True)):
        await interaction.response.defer(ephemeral=True); idb.update_cog_config(interaction.guild.id, 'log_channel_id', channel.id)
        await self._load_config_and_cache(); await interaction.followup.send(f"Invite log channel set to {channel.mention}.", ephemeral=True)
        await self._log_invite_action(title="Invite Log Channel Set", color=Color.blurple(), details=f"Set to {channel.mention} by {interaction.user.mention}")

    @inviteset_group.subcommand(name="leaderboard_webhook", description="Sets webhook URL for invite leaderboard.")
    async def set_leaderboard_webhook(self, interaction: Interaction, webhook_url: str = SlashOption(description="The full Discord webhook URL", required=True)):
        await interaction.response.defer(ephemeral=True)
        if not webhook_url.startswith("https://discord.com/api/webhooks/"): await interaction.followup.send("Invalid webhook URL format.", ephemeral=True); return
        try:
            async with aiohttp.ClientSession() as session:
                temp_webhook = Webhook.from_url(webhook_url, session=session)
                initial_embed = Embed(title="Invitation Leaderboard", description="Initializing...", color=Color.blurple())
                initial_embed.set_footer(text="Awaiting first update cycle."); initial_embed.timestamp = datetime.now(timezone.utc)
                test_message = await temp_webhook.send(embed=initial_embed, wait=True)
            idb.update_cog_config(interaction.guild.id, 'leaderboard_webhook_url', webhook_url)
            idb.update_cog_config(interaction.guild.id, 'leaderboard_message_id', test_message.id)
            idb.update_cog_config(interaction.guild.id, 'leaderboard_channel_id', test_message.channel.id)
            await self._load_config_and_cache()
            await interaction.followup.send(f"Leaderboard webhook set & initialized in <#{test_message.channel.id}>.", ephemeral=True)
            await self._log_invite_action(title="Leaderboard Webhook Set", color=Color.blurple(), details=f"URL: `{webhook_url[:40]}...`\nInitial Msg ID: {test_message.id}\nBy: {interaction.user.mention}")
            if self.update_leaderboard_task.is_running(): self.update_leaderboard_task.restart()
            elif self.leaderboard_webhook_url: self.update_leaderboard_task.start()
        except Exception as e: await interaction.followup.send(f"Error with webhook: {e}", ephemeral=True); logging.error("Webhook setup error", exc_info=True)

    @inviteset_group.subcommand(name="set_required_role", description="Role an invited member needs for the invite to be 'valid'.")
    async def set_required_role_for_valid_invite(self, interaction: Interaction, role: Optional[Role] = SlashOption(description="The role required. Select None/empty to clear.", required=False)):
        await interaction.response.defer(ephemeral=True)
        role_id_to_set = role.id if role else None; role_name_to_set = role.name if role else "None"
        idb.update_cog_config(interaction.guild.id, 'required_role_id', role_id_to_set)
        await self._load_config_and_cache()
        await interaction.followup.send(f"Required role for invites to be valid set to: {role_name_to_set if role else 'None (any invite is valid if member stays)'}.", ephemeral=True)
        await self._log_invite_action(title="Invite Config Updated", color=Color.blurple(), details=f"Required role for valid invites set to '{role_name_to_set}' by {interaction.user.mention}.")

    @nextcord.slash_command(name="compensate_invites", description="Manually adjust a user's invite counts (Admin).")
    @application_checks.has_permissions(manage_guild=True)
    async def compensate_invites_cmd(self, interaction: Interaction, user: Member = SlashOption(required=True), action: str = SlashOption(choices={"add": "add", "remove": "remove"}, required=True), amount: int = SlashOption(min_value=1, required=True), reason: Optional[str] = SlashOption(required=False)):
        await interaction.response.defer(ephemeral=True)
        if user.bot: await interaction.followup.send("Cannot compensate for a bot.", ephemeral=True); return
        if idb.compensate_invites(interaction.guild.id, user.id, amount, action):
            stats = idb.get_inviter_stats(interaction.guild.id, user.id)
            await self._check_and_apply_role_rewards(user)
            reason_text = f"\nReason: {reason}" if reason else ""
            # The description for _log_invite_action should be a string.
            desc_for_log = f"Invites for {user.mention} adjusted by {interaction.user.mention}. Action: {action.capitalize()} {amount}. New Valid: {stats['total_valid_invites']}, New Raw: {stats['total_raw_invites']}{reason_text}"
            await self._log_invite_action(title="Invites Compensated", color=Color.purple(), Details=desc_for_log) # Using Details to match _log_invite_action behavior
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
            await self._log_invite_action(title="Invite Reward Added", color=Color.teal(), role_involved=role, details=f"Threshold: {invite_threshold} valid invites\nSet by: {interaction.user.mention}")
        else: await interaction.followup.send(f"Failed to add (threshold or role might already exist).", ephemeral=True)

    @invitereward_group.subcommand(name="remove", description="Remove a role reward configuration.")
    async def invitereward_remove(self, interaction: Interaction, role: Role = SlashOption(required=True)):
        await interaction.response.defer(ephemeral=True)
        if idb.remove_role_reward(interaction.guild.id, role.id):
            await interaction.followup.send(f"Role {role.mention} removed from invite rewards.", ephemeral=True)
            await self._log_invite_action(title="Invite Reward Removed", color=Color.dark_gold(), role_involved=role, details=f"Removed by: {interaction.user.mention}")
        else: await interaction.followup.send(f"Role {role.mention} not found in reward configs.", ephemeral=True)

    @invitereward_group.subcommand(name="list", description="List current invite role rewards (based on valid invites).")
    async def invitereward_list(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        rewards = idb.get_all_role_rewards(interaction.guild.id)
        if not rewards: await interaction.followup.send("No role rewards configured.", ephemeral=True); return
        embed = Embed(title="Invite Role Rewards (based on Valid Invites)", color=Color.purple())
        description = ""
        for reward_data in rewards:
            role_obj = interaction.guild.get_role(reward_data['role_id']) # Renamed from role to role_obj
            role_mention = role_obj.mention if role_obj else f"ID {reward_data['role_id']} (Not Found?)"
            description += f"- **{reward_data['invite_threshold']} Valid Invites** -> {role_mention}\n"
        embed.description = description if description else "No rewards set."
        await interaction.followup.send(embed=embed, ephemeral=True)

def setup(bot: commands.Bot):
    global aiohttp # Ensure aiohttp is accessible
    try:
        import aiohttp # Check if aiohttp is available
    except ImportError:
        logging.error("InviteTrackerCog: aiohttp library not installed, needed for webhooks. Leaderboard may not function.")
        # You might choose to not load the cog or disable leaderboard if aiohttp is missing
    bot.add_cog(InviteTrackerCog(bot))