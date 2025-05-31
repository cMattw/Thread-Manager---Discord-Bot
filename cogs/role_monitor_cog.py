import nextcord
from nextcord.ext import commands
from nextcord import Interaction, SlashOption, Permissions, Member, Role, Embed, Color, Webhook, WebhookMessage
from nextcord.ext import application_checks
import logging
import aiohttp
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Union

# Import the database module
from db_utils import role_monitor_database as db

# Configure logging
logger = logging.getLogger('nextcord.role_monitor')

# Static Image URL for all embeds from this cog
STATIC_EMBED_IMAGE_URL = "https://media.discordapp.net/attachments/1134400036697022494/1318828843842142258/Untitled91_20241218143030.png?ex=67763393&is=6774e213&hm=7c12453788265df2c9c3881a8220939468adc75fa5854e8973dfed8f9049ed21&"

# Helper classes for placeholder formatting
class _UserPlaceholderWrapper:
    def __init__(self, member: Member):
        self.mention: str = member.mention
        self.name: str = str(member)
        self.id: str = str(member.id)
        self.raw_name: str = member.name
        self.discriminator: str = member.discriminator
        self.display_name: str = member.display_name
        self.global_name: Optional[str] = member.global_name

class _RolePlaceholderWrapper:
    def __init__(self, role: Role):
        self.name: str = role.name
        self.id: str = str(role.id)
        self.mention: str = role.mention
        self.color: Color = role.color

class RoleMonitorCog(commands.Cog, name="Role Watcher"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.session: Optional[aiohttp.ClientSession] = None
        db.init_db()
        logger.info("RoleMonitorCog loaded and database initialized.")
        self.recently_processed_events = {}
        self.DEBOUNCE_SECONDS = 5

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            logger.info("aiohttp.ClientSession is None or closed. Creating new session.")
            try:
                self.session = aiohttp.ClientSession(loop=self.bot.loop)
                logger.info("aiohttp.ClientSession created successfully.")
            except Exception as e:
                logger.error(f"Failed to create aiohttp.ClientSession: {e}", exc_info=True)
                raise
        return self.session

    async def cog_load(self):
        logger.info("RoleMonitorCog: cog_load called. Initializing aiohttp session.")
        try:
            await self._get_session()
        except Exception as e:
            logger.error(f"RoleMonitorCog: Failed to initialize session during cog_load: {e}")

    async def cog_unload(self):
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("aiohttp.ClientSession closed for RoleMonitorCog.")
        self.session = None
        self.recently_processed_events.clear()

    def _resolve_placeholders(self, template_str: str, user_obj: Member, role_obj: Role) -> str:
        if template_str is None: return ""
        try:
            return template_str.format(
                user=_UserPlaceholderWrapper(user_obj),
                role=_RolePlaceholderWrapper(role_obj)
            )
        except KeyError as e:
            logger.error(f"Invalid placeholder in template: '{template_str}'. Missing key: {e}")
            return template_str 
        except Exception as e:
            logger.error(f"Error resolving placeholders for template '{template_str}': {e}", exc_info=True)
            return template_str

    async def _send_webhook_message(self, webhook_url: str, content: Optional[str] = None, embed: Optional[Embed] = None) -> Optional[WebhookMessage]:
        try:
            session = await self._get_session()
        except Exception:
            logger.error("Failed to obtain aiohttp session for sending webhook message.")
            return None
        if not webhook_url:
            logger.error("Webhook URL is not configured for _send_webhook_message.")
            return None
        try:
            webhook = Webhook.from_url(webhook_url, session=session)
            message = await webhook.send(content=content if content else None, embed=embed, wait=True)
            return message
        except nextcord.HTTPException as e:
            logger.error(f"HTTPException (Status: {e.status}) while sending webhook message: {e.text}", exc_info=True)
        except aiohttp.ClientError as e:
            logger.error(f"aiohttp.ClientError while sending webhook message: {e}", exc_info=True)
        except ValueError as e:
            logger.error(f"Invalid webhook URL format for sending: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"An unexpected error occurred while sending webhook message via URL {webhook_url[:50]}...: {e}", exc_info=True)
        return None

    async def _delete_webhook_message(self, webhook_url: str, message_id: Union[str, int]):
        try:
            session = await self._get_session()
        except Exception:
            logger.error("Failed to obtain aiohttp session for deleting webhook message.")
            return False
        if not webhook_url:
            logger.error("Webhook URL is not configured for _delete_webhook_message.")
            return False
        logger.debug(f"Attempting to delete webhook message ID: {message_id} using URL: {webhook_url[:50]}...")
        try:
            webhook = Webhook.from_url(webhook_url, session=session)
            await webhook.delete_message(int(message_id))
            logger.info(f"Successfully deleted webhook message ID: {message_id}")
            return True
        except nextcord.NotFound:
            logger.warning(f"Webhook message ID {message_id} not found (already deleted?).")
        except nextcord.Forbidden:
            logger.error(f"Forbidden (403) to delete webhook message ID {message_id}. Check webhook permissions or if it's the correct webhook.")
        except nextcord.HTTPException as e:
            logger.error(f"HTTPException (Status: {e.status}) while deleting webhook message ID {message_id}: {e.text}", exc_info=True)
        except aiohttp.ClientError as e:
            logger.error(f"aiohttp.ClientError while deleting webhook message: {e}", exc_info=True)
        except ValueError as e:
            logger.error(f"Invalid webhook URL format for deleting: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"An unexpected error occurred while deleting webhook message ID {message_id}: {e}", exc_info=True)
        return False

    @commands.Cog.listener()
    async def on_member_update(self, before: Member, after: Member):
        if before.guild.id != after.guild.id: return
        if before.roles == after.roles: return

        guild_id = str(after.guild.id)
        
        now_for_cleanup = datetime.now(timezone.utc).timestamp()
        keys_to_delete_from_cache = [
            key for key, ts in list(self.recently_processed_events.items())
            if (now_for_cleanup - ts) > (self.DEBOUNCE_SECONDS * 12)
        ]
        for key in keys_to_delete_from_cache:
            self.recently_processed_events.pop(key, None)

        webhook_url = db.get_webhook_url(guild_id)
        if not webhook_url:
            return

        before_role_ids = {role.id for role in before.roles}
        after_role_ids = {role.id for role in after.roles}
        added_role_ids = after_role_ids - before_role_ids
        removed_role_ids = before_role_ids - after_role_ids
        default_content_placeholder = "{user.mention}"

        # Handle Gained Roles
        for role_id in added_role_ids:
            role = after.guild.get_role(role_id)
            if not role: continue

            event_key = (str(after.id), str(role.id), "added")
            current_time = datetime.now(timezone.utc).timestamp()
            last_processed_time = self.recently_processed_events.get(event_key)

            if last_processed_time and (current_time - last_processed_time) < self.DEBOUNCE_SECONDS:
                logger.info(f"Debouncing GAINED Event for User {after.id}, Role {role.id}.")
                continue 

            watched_role_data = db.get_watched_role(guild_id, str(role.id))
            if watched_role_data and watched_role_data['is_enabled']:
                logger.info(f"Event: User {after.name} GAINED role {role.name}.")
                event_type = "gain"
                self.recently_processed_events[event_key] = current_time
                
                active_msg = db.get_active_message(guild_id, str(after.id), str(role.id))
                if active_msg:
                    db_message_state_raw = active_msg.get('message_state')
                    db_webhook_id = active_msg.get('webhook_message_id')
                    logger.info(f"  GAINED Event - Previous DB state for user {after.id}, role {role.id}: {repr(db_message_state_raw)}, msg_id: '{db_webhook_id}'.")

                    processed_db_state_for_lost_check = None
                    if isinstance(db_message_state_raw, str):
                        processed_db_state_for_lost_check = db_message_state_raw.strip().lower()
                    
                    target_state_lost = 'lost'
                    is_prev_state_lost = (processed_db_state_for_lost_check == target_state_lost)
                    has_message_id = bool(db_webhook_id) 
                    
                    if is_prev_state_lost and has_message_id:
                        logger.info(f"    Attempting to delete previous 'lost' message (ID: {db_webhook_id}).")
                        deleted = await self._delete_webhook_message(webhook_url, db_webhook_id)
                        if deleted: logger.info(f"      Successfully deleted previous 'lost' message.")
                        else: logger.warning(f"      FAILED to delete previous 'lost' message.")
                    elif active_msg:
                        logger.info(f"    No deletion needed for GAIN event (prev_state_is_lost: {is_prev_state_lost}, has_msg_id: {has_message_id}).")
                
                custom_content_template = watched_role_data.get('gain_custom_content')
                text_content = self._resolve_placeholders(custom_content_template or default_content_placeholder, after, role)
                
                db_gain_title = watched_role_data.get('gain_custom_title')
                title_for_embed_gain: Optional[str]
                if db_gain_title == "":
                    title_for_embed_gain = None 
                elif db_gain_title is not None:
                    title_for_embed_gain = self._resolve_placeholders(db_gain_title, after, role)
                else:
                    title_for_embed_gain = "Role Acquired"
                
                description_template_gain = watched_role_data.get('gain_custom_description') or "{user.mention} has acquired the {role.name}"
                embed_title = title_for_embed_gain
                embed_description = self._resolve_placeholders(description_template_gain, after, role)

                embed = Embed(title=embed_title, description=embed_description, color=role.color if role.color != Color.default() else Color.blue(), timestamp=datetime.now(timezone.utc))
                embed.set_thumbnail(url=after.display_avatar.url)
                embed.set_image(url=STATIC_EMBED_IMAGE_URL) # Added static image
                embed.set_footer(text="Role Monitor") # Updated footer text
                
                logger.info(f"  Sending new '{event_type}' message for {after.name}, role {role.name}.")
                sent_message = await self._send_webhook_message(webhook_url, content=text_content, embed=embed)
                if sent_message:
                    logger.info(f"    New 'gain' message sent (ID: {sent_message.id}). Updating DB.")
                    db.update_active_message(guild_id, str(after.id), str(role.id), str(sent_message.id), event_type)
                else: logger.error(f"    FAILED to send new '{event_type}' message.")


        # Handle Lost Roles
        for role_id in removed_role_ids:
            role = before.guild.get_role(role_id) 
            if not role: role = after.guild.get_role(role_id) 
            if not role: continue

            event_key = (str(after.id), str(role.id), "removed")
            current_time = datetime.now(timezone.utc).timestamp()
            last_processed_time = self.recently_processed_events.get(event_key)

            if last_processed_time and (current_time - last_processed_time) < self.DEBOUNCE_SECONDS:
                logger.info(f"Debouncing LOST Event for User {after.id}, Role {role.id}.")
                continue

            watched_role_data = db.get_watched_role(guild_id, str(role.id)) 
            if watched_role_data and watched_role_data['is_enabled']:
                logger.info(f"Event: User {after.name} LOST role {role.name}.")
                event_type = "loss"
                self.recently_processed_events[event_key] = current_time

                active_msg = db.get_active_message(guild_id, str(after.id), str(role.id))
                if active_msg:
                    db_message_state_raw = active_msg.get('message_state')
                    db_webhook_id = active_msg.get('webhook_message_id')
                    logger.info(f"  LOST Event - Previous DB state for user {after.id}, role {role.id}: {repr(db_message_state_raw)}, msg_id: '{db_webhook_id}'.")

                    processed_db_state_for_gain_check = None
                    if isinstance(db_message_state_raw, str):
                        processed_db_state_for_gain_check = db_message_state_raw.strip().lower()
                    
                    target_state_gain = 'gain'
                    is_prev_state_gain = (processed_db_state_for_gain_check == target_state_gain)
                    has_message_id = bool(db_webhook_id)
                    
                    if is_prev_state_gain and has_message_id:
                        logger.info(f"    Attempting to delete previous 'gain' message (ID: {db_webhook_id}).")
                        deleted = await self._delete_webhook_message(webhook_url, db_webhook_id)
                        if deleted: logger.info(f"      Successfully deleted previous 'gain' message.")
                        else: logger.warning(f"      FAILED to delete previous 'gain' message.")
                    elif active_msg:
                        logger.info(f"    No deletion needed for LOST event (prev_state_is_gain: {is_prev_state_gain}, has_msg_id: {has_message_id}).")
                
                custom_content_template = watched_role_data.get('loss_custom_content')
                text_content = self._resolve_placeholders(custom_content_template or default_content_placeholder, after, role)

                db_loss_title = watched_role_data.get('loss_custom_title')
                title_for_embed_loss: Optional[str]
                if db_loss_title == "": 
                    title_for_embed_loss = None
                elif db_loss_title is not None:
                    title_for_embed_loss = self._resolve_placeholders(db_loss_title, after, role)
                else:
                    title_for_embed_loss = "Role Lost"

                description_template_loss = watched_role_data.get('loss_custom_description') or "{user.mention} no longer has the {role.name}"
                embed_title = title_for_embed_loss
                embed_description = self._resolve_placeholders(description_template_loss, after, role)

                embed = Embed(title=embed_title, description=embed_description, color=role.color if role.color != Color.default() else Color.greyple(), timestamp=datetime.now(timezone.utc))
                embed.set_thumbnail(url=after.display_avatar.url)
                embed.set_image(url=STATIC_EMBED_IMAGE_URL) # Added static image
                embed.set_footer(text="Role Monitor") # Updated footer text

                logger.info(f"  Sending new '{event_type}' message for {after.name}, role {role.name}.")
                sent_message = await self._send_webhook_message(webhook_url, content=text_content, embed=embed)
                if sent_message:
                    logger.info(f"    New 'loss' message sent (ID: {sent_message.id}). Updating DB.")
                    db.update_active_message(guild_id, str(after.id), str(role.id), str(sent_message.id), event_type)
                else: logger.error(f"    FAILED to send new '{event_type}' message.")

    # --- Slash Commands ---
    @nextcord.slash_command(name="rolewatch", description="Manage role monitoring settings.", default_member_permissions=Permissions(administrator=True))
    async def rolewatch(self, interaction: Interaction):
        if interaction.application_command.qualified_name == "rolewatch" and not interaction.data.get("options"):
             await interaction.response.send_message("Please use a subcommand.", ephemeral=True)

    @rolewatch.subcommand(name="set_webhook", description="Sets webhook URL.")
    async def set_webhook_sub(self, interaction: Interaction, url: str = SlashOption(description="Webhook URL", required=True)):
        guild_id = str(interaction.guild.id)
        if not (url.startswith("https://discord.com/api/webhooks/") or url.startswith("https://ptb.discord.com/api/webhooks/") or url.startswith("https://canary.discord.com/api/webhooks/")):
            await interaction.response.send_message("⚠️ Invalid webhook URL format.", ephemeral=True)
            return
        try:
            session = await self._get_session()
            webhook_to_test = Webhook.from_url(url, session=session)
            await webhook_to_test.fetch() 
        except Exception as e:
             logger.warning(f"Webhook validation failed for URL {url}: {e}")
             await interaction.response.send_message(f"⚠️ Webhook URL invalid/inaccessible. Error: `{e}`", ephemeral=True)
             return
        db.set_webhook_url(guild_id, url)
        await interaction.response.send_message(f"✅ Webhook URL set.", ephemeral=True)

    @rolewatch.subcommand(name="add_role", description="Adds a role to monitor.")
    async def add_role_sub(self, interaction: Interaction, role: Role = SlashOption(description="Role to monitor", required=True)):
        guild_id = str(interaction.guild.id)
        db.add_watched_role(guild_id, str(role.id))
        await interaction.response.send_message(f"✅ Role **{role.name}** will be monitored.", ephemeral=True)

    @rolewatch.subcommand(name="remove_role", description="Removes a role from monitoring.")
    async def remove_role_sub(self, interaction: Interaction, role: Role = SlashOption(description="Role to stop monitoring", required=True)):
        guild_id = str(interaction.guild.id)
        role_id_str = str(role.id)
        webhook_url = db.get_webhook_url(guild_id)
        messages_to_delete_ids = db.delete_all_active_messages_for_role(guild_id, role_id_str)
        deleted_count = 0
        should_defer = bool(webhook_url and messages_to_delete_ids)
        if should_defer: 
            try:
                await interaction.response.defer(ephemeral=True)
            except nextcord.errors.InteractionResponded:
                logger.warning("Interaction already responded in remove_role_sub, cannot defer again.")

        if webhook_url and messages_to_delete_ids:
            for msg_id in messages_to_delete_ids:
                if await self._delete_webhook_message(webhook_url, msg_id): deleted_count += 1
        
        db.remove_watched_role(guild_id, role_id_str)
        
        response_message = f"✅ Role **{role.name}** is no longer monitored."
        if messages_to_delete_ids:
            response_message += f" Found {len(messages_to_delete_ids)} message record(s) to delete; {deleted_count} successfully deleted via webhook."
        
        if interaction.response.is_done():
            await interaction.followup.send(response_message, ephemeral=True)
        else:
            await interaction.response.send_message(response_message, ephemeral=True)

    @rolewatch.subcommand(name="toggle_role", description="Toggles monitoring for a role.")
    async def toggle_role_sub(self, interaction: Interaction, role: Role = SlashOption(description="Role to toggle", required=True)):
        guild_id = str(interaction.guild.id)
        new_status = db.toggle_watched_role_enabled(guild_id, str(role.id))
        if new_status is None: await interaction.response.send_message(f"⚠️ Role **{role.name}** not monitored.", ephemeral=True)
        else:
            status_text = "ENABLED" if new_status else "DISABLED"
            await interaction.response.send_message(f"✅ Monitoring for **{role.name}** is now **{status_text}**.", ephemeral=True)

    @rolewatch.subcommand(name="list_roles", description="Lists monitored roles.")
    async def list_roles_sub(self, interaction: Interaction):
        guild_id = str(interaction.guild.id)
        watched_roles_data = db.get_all_watched_roles(guild_id)
        if not watched_roles_data:
            await interaction.response.send_message("ℹ️ No roles monitored.", ephemeral=True)
            return
        embed = Embed(title="Monitored Roles", color=Color.blue(), timestamp=datetime.now(timezone.utc))
        desc_lines = []
        for rd in watched_roles_data:
            role_obj = interaction.guild.get_role(int(rd['role_id']))
            r_name = role_obj.name if role_obj else f"Unknown (ID: {rd['role_id']})"
            status = "✅ Enabled" if rd['is_enabled'] else "❌ Disabled"
            cust_fields = ['gain_custom_title', 'gain_custom_description', 'gain_custom_content', 'loss_custom_title', 'loss_custom_description', 'loss_custom_content']
            has_cust = any(rd.get(f) for f in cust_fields)
            c_text = " (Custom Tmpl)" if has_cust else ""
            desc_lines.append(f"**{r_name}**: {status}{c_text}")
        embed.description = "\n".join(desc_lines) if desc_lines else "No roles."
        embed.set_footer(text="Role Monitor") # Updated footer
        # Timestamp is handled by the Embed object itself
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @rolewatch.subcommand(name="set_template", description="Sets custom templates.")
    async def set_template_sub(self, interaction: Interaction, role: Role = SlashOption(description="Role", required=True), event_type: str = SlashOption(description="Event type", choices={"gain": "gain", "loss": "loss"}, required=True), title: Optional[str] = SlashOption(description="Embed title (empty string for no title)", required=False), description: Optional[str] = SlashOption(description="Embed description", required=False), content: Optional[str] = SlashOption(description="Text content", required=False)):
        guild_id = str(interaction.guild.id)
        role_id_str = str(role.id)
        if not db.get_watched_role(guild_id, role_id_str):
            await interaction.response.send_message(f"⚠️ Role **{role.name}** not monitored.", ephemeral=True)
            return
        
        if title is None and description is None and content is None:
            await interaction.response.send_message(f"ℹ️ No template parts provided to set/change.", ephemeral=True)
            return

        db.update_role_template(guild_id, role_id_str, event_type, title, description, content)
        await interaction.response.send_message(f"✅ Templates for **{role.name}** ({event_type}) updated.", ephemeral=True)
        logger.info(f"Templates for {role.name} ({event_type}) updated for {guild_id} by {interaction.user}.")

    @rolewatch.subcommand(name="clear_template", description="Clears custom templates.")
    async def clear_template_sub(self, interaction: Interaction, role: Role = SlashOption(description="Role", required=True), event_type: str = SlashOption(description="Event type", choices={"gain": "gain", "loss": "loss"}, required=True), part: str = SlashOption(description="Template part", choices={"Embed Title": "title", "Embed Description": "description", "Text Content": "content", "All Embed Parts": "all_embed_parts", "All": "all"}, required=True)):
        guild_id = str(interaction.guild.id)
        role_id_str = str(role.id)
        if not db.get_watched_role(guild_id, role_id_str):
            await interaction.response.send_message(f"⚠️ Role **{role.name}** not monitored.", ephemeral=True)
            return
        db.clear_role_template_part(guild_id, role_id_str, event_type, part)
        await interaction.response.send_message(f"✅ Template part(s) `({part})` for **{role.name}** ({event_type}) cleared.", ephemeral=True)
        logger.info(f"Templates part {part} for {role.name} ({event_type}) cleared for {guild_id} by {interaction.user}.")

def setup(bot: commands.Bot):
    bot.add_cog(RoleMonitorCog(bot))
    logger.info("RoleMonitorCog has been added to the bot.")