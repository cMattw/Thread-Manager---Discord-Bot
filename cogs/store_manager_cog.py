# cogs/store_manager_cog.py
import nextcord
from nextcord.ext import commands, tasks, application_checks
from nextcord import (
    Interaction, SlashOption, Embed, Color, Member, Role, TextChannel,
    ui, ButtonStyle, TextInputStyle, Forbidden, Webhook
)
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone, timedelta
import asyncio
import aiohttp
from dateutil.relativedelta import relativedelta
import math

from db_utils import store_database as db

logger = logging.getLogger('nextcord.store_manager_cog')

# ... (All helper functions and UI classes like TransactionHistoryView, SubscriptionModal, etc., remain unchanged) ...
def get_unix_time() -> int:
    return int(datetime.now(timezone.utc).timestamp())

class TransactionHistoryView(ui.View):
    """A view for paginating through a user's transaction history."""
    def __init__(self, interaction: Interaction, transactions: List[Dict[str, Any]], user: Member):
        # Add the required super().__init__() call
        super().__init__(timeout=180)
        self.interaction = interaction
        self.transactions = transactions
        self.user = user
        self.current_page = 1
        self.per_page = 5
        self.total_pages = math.ceil(len(self.transactions) / self.per_page)

    async def get_page_embed(self) -> Embed:
        """Generates the embed for the current page, now with expiry info."""
        embed = Embed(
            title=f"Transaction History for {self.user.display_name}",
            color=Color.blue()
        )
        
        start_index = (self.current_page - 1) * self.per_page
        end_index = start_index + self.per_page
        
        all_sub_items = {item['item_name']: item for item in db.get_all_store_items() if item.get('is_subscription')}
        all_scheduled = db.get_all_scheduled_removals()
        user_active_subs = {sub['role_id']: sub for sub in all_scheduled if sub['user_id'] == self.user.id}
        
        description = []
        for trans in self.transactions[start_index:end_index]:
            ts = f"<t:{trans['timestamp']}:f>"
            entry = (f"**ID: `{trans['transaction_id']}` | {trans['transaction_type']} | {ts}**\n"
                     f"> Item: `{trans['item_description']}` | Qty: `{trans.get('quantity', 'N/A')}`\n"
                     f"> Notes: *{trans.get('notes', 'None')}*")
            
            item_name = trans['item_description']
            if item_name in all_sub_items:
                role_id = all_sub_items[item_name].get('associated_role_id')
                if role_id and role_id in user_active_subs:
                    expiry_timestamp = user_active_subs[role_id]['removal_timestamp']
                    if expiry_timestamp < get_unix_time():
                        entry += f"\n> **Status:** `Expired` (was <t:{expiry_timestamp}:f>)"
                    else:
                        entry += f"\n> **Expires:** <t:{expiry_timestamp}:f>"
                else:
                    # Check the transaction itself for permanent/expired status
                    if trans.get('is_permanent', 0):
                        entry += "\n> **Status:** `Permanent`"
                    elif trans.get('expired', 0):
                        entry += "\n> **Status:** `Expired`"
                    else:
                        # Fallback: treat as expired for legacy data
                        entry += "\n> **Status:** `Expired`"
            description.append(entry)

        embed.description = "\n\n".join(description) if description else "No transactions on this page."
        embed.set_footer(text=f"Page {self.current_page} of {self.total_pages}")

        self.prev_button.disabled = self.current_page == 1
        self.next_button.disabled = self.current_page == self.total_pages
        
        return embed

    @ui.button(label="Previous", style=ButtonStyle.primary, emoji="⬅️")
    async def prev_button(self, button: ui.Button, interaction: Interaction):
        if self.current_page > 1:
            self.current_page -= 1
            embed = await self.get_page_embed()
            await interaction.response.edit_message(embed=embed, view=self)

    @ui.button(label="Next", style=ButtonStyle.primary, emoji="➡️")
    async def next_button(self, button: ui.Button, interaction: Interaction):
        if self.current_page < self.total_pages:
            self.current_page += 1
            embed = await self.get_page_embed()
            await interaction.response.edit_message(embed=embed, view=self)

class SubscriptionModal(ui.Modal):
    def __init__(self, parent_interaction: Interaction, item_name: str, role_to_assign: Role, parent_cog: 'StoreManagerCog'):
        super().__init__(f"Subscription: {item_name}", timeout=600)
        self.parent_interaction = parent_interaction
        self.item_name = item_name
        self.role_to_assign = role_to_assign
        self.parent_cog = parent_cog

        self.ign = ui.TextInput(label="Minecraft In-Game Name (IGN)", style=TextInputStyle.short, placeholder="Enter the user's Minecraft username", required=True, max_length=50)
        self.add_item(self.ign)
        self.months = ui.TextInput(label="Subscription Duration (Months)", style=TextInputStyle.short, placeholder="e.g., 3, or -1 for Permanent. Default: 0", required=False, max_length=4)
        self.add_item(self.months)
        self.days = ui.TextInput(label="Additional Subscription Duration (Days)", style=TextInputStyle.short, placeholder="e.g., 15. Default: 0", required=False, max_length=4)
        self.add_item(self.days)
        
    async def callback(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)

        try:
            months_val = 0
            if self.months.value and self.months.value.strip(): months_val = int(self.months.value)
            days_val = 0
            if self.days.value and self.days.value.strip(): days_val = int(self.days.value)
        except ValueError:
            await interaction.followup.send("Invalid number format for months or days.", ephemeral=True)
            return

        is_permanent = (months_val == -1)
        target_user_id = self.parent_interaction.data['options'][0]['options'][0]['value']
        target_user = await self.parent_interaction.guild.fetch_member(int(target_user_id))
        
        removal_timestamp = None
        if not is_permanent:
            now = datetime.now(timezone.utc)
            future_date = now + relativedelta(months=months_val) + timedelta(days=days_val)
            removal_timestamp = int(future_date.timestamp())

        try:
            existing_sub = db.get_user_subscription(target_user.id, self.role_to_assign.id)
            if self.role_to_assign not in target_user.roles:
                 await target_user.add_roles(self.role_to_assign, reason=f"Store purchase: {self.item_name}")

            # --- Updated DM Logic ---
            dm_embed = None
            if is_permanent:
                if existing_sub: db.delete_scheduled_removal(existing_sub['schedule_id'])
                await interaction.followup.send(f"✅ Role {self.role_to_assign.mention} has been **permanently** assigned to {target_user.mention}.", ephemeral=True)
                dm_embed = Embed(title="Subscription Activated", description=f"Your **{self.item_name}** rank is now permanently active!", color=Color.gold())
            else:
                if existing_sub:
                    current_removal_dt = datetime.fromtimestamp(existing_sub['removal_timestamp'], tz=timezone.utc)
                    new_removal_dt = current_removal_dt + relativedelta(months=months_val) + timedelta(days=days_val)
                    new_timestamp = int(new_removal_dt.timestamp())
                    db.update_user_subscription(existing_sub['schedule_id'], new_timestamp)
                    await interaction.followup.send(f"✅ Extended {target_user.mention}'s subscription for {self.role_to_assign.mention}. New expiry: <t:{new_timestamp}:F>.", ephemeral=True)
                    dm_embed = Embed(title="Subscription Extended", description=f"Your **{self.item_name}** subscription has been extended!", color=Color.green())
                    dm_embed.add_field(name="New Expiration Date", value=f"<t:{new_timestamp}:F>")
                else:
                    db.schedule_role_removal(target_user.id, self.role_to_assign.id, removal_timestamp)
                    await interaction.followup.send(f"✅ Role {self.role_to_assign.mention} assigned to {target_user.mention}. Expires: <t:{removal_timestamp}:F>.", ephemeral=True)
                    dm_embed = Embed(title="Subscription Activated", description=f"You have received the **{self.item_name}** subscription!", color=Color.green())
                    dm_embed.add_field(name="Expires", value=f"<t:{removal_timestamp}:F>")
            
            # Send the specific DM
            if dm_embed:
                dm_embed.set_footer(text=f"Thank you for your support!")
                await self.parent_cog._send_dm(target_user, dm_embed)

            cmd_options = {opt['name']: opt['value'] for opt in self.parent_interaction.data['options'][0]['options']}
            db.add_transaction(
                guild_id=self.parent_interaction.guild.id,
                user_id=target_user.id,
                username_at_time=str(target_user),
                trans_type=cmd_options['type'],
                item=self.item_name,
                admin_id=interaction.user.id,
                quantity=cmd_options.get('quantity'),
                notes=cmd_options.get('notes'),
                ign=self.ign.value,
                timestamp=get_unix_time(),
                is_permanent=1
            )

            try:
                logger.info("Triggering subscriber list update after new subscription.")
                await self.parent_cog.update_subscriber_list_task.coro(self.parent_cog)
            except Exception as e:
                logger.error(f"Failed to auto-trigger subscriber list update: {e}", exc_info=True)
        
        except Forbidden:
            await interaction.followup.send("❌ **Error:** I don't have permission to assign that role...", ephemeral=True)
        except Exception as e:
            logger.error(f"Error in subscription modal callback: {e}", exc_info=True)
            await interaction.followup.send(f"An unexpected error occurred: {e}", ephemeral=True)

class EditTransactionModal(ui.Modal):
    def __init__(self, transaction_data: Dict[str, Any], is_subscription: bool, subscription_details: Optional[Dict[str, Any]]):
        super().__init__("Edit Transaction", timeout=600)
        self.transaction_data = transaction_data
        self.is_subscription = is_subscription
        self.subscription_details = subscription_details

        # Always-present fields
        self.item_desc = ui.TextInput(label="Item Description", default_value=transaction_data.get('item_description', ''), max_length=200)
        self.add_item(self.item_desc)

        self.ign = ui.TextInput(label="In-Game Name", default_value=transaction_data.get('ingame_name', ''), required=False, max_length=50)
        self.add_item(self.ign)

        # Always show duration fields for subscriptions
        if self.is_subscription:
            self.days_edit = ui.TextInput(
                label="Add or Remove Days",
                style=TextInputStyle.short,
                placeholder="e.g., 15 to add, -7 to remove",
                required=False, max_length=6
            )
            self.add_item(self.days_edit)

            self.timestamp_edit = ui.TextInput(
                label="Set Expiry (Unix Timestamp)",
                style=TextInputStyle.short,
                placeholder="e.g., 1750000000 (leave blank to ignore)",
                required=False, max_length=15
            )
            self.add_item(self.timestamp_edit)
            self.quantity = None
        else:
            self.quantity = ui.TextInput(label="Quantity", default_value=str(transaction_data.get('quantity', '')), required=False, max_length=10)
            self.add_item(self.quantity)
            self.days_edit = None
            self.timestamp_edit = None

        # Always add notes as the last field (will be 5th field)
        self.notes = ui.TextInput(label="Notes", style=TextInputStyle.paragraph, default_value=transaction_data.get('notes', ''), required=False, max_length=1000)
        self.add_item(self.notes)

    async def callback(self, interaction: Interaction):
        qty_val = None
        if self.quantity:
            try:
                qty_val = int(self.quantity.value) if self.quantity.value and self.quantity.value.strip() else None
            except ValueError:
                await interaction.response.send_message("Quantity must be a valid number.", ephemeral=True)
                return

        updates = {
            "item_description": self.item_desc.value,
            "ingame_name": self.ign.value,
            "notes": self.notes.value
        }
        if not self.is_subscription:
            updates["quantity"] = qty_val
        
        db.update_transaction(self.transaction_data['transaction_id'], updates)
        response_messages = [f"✅ Transaction ID `{self.transaction_data['transaction_id']}` has been updated."]

        # --- Duration Editing Logic ---
        if self.is_subscription and self.days_edit and self.timestamp_edit:
            try:
                days_val = int(self.days_edit.value) if self.days_edit.value and self.days_edit.value.strip() else 0
                timestamp_val = int(self.timestamp_edit.value) if self.timestamp_edit.value and self.timestamp_edit.value.strip() else None

                # Use current time as base if no active scheduled removal
                if self.subscription_details and self.subscription_details.get('removal_timestamp'):
                    current_timestamp = self.subscription_details['removal_timestamp']
                else:
                    current_timestamp = get_unix_time()

                new_timestamp = current_timestamp

                if timestamp_val:
                    new_timestamp = timestamp_val
                    response_messages.append(f"✅ Subscription expiry set to <t:{new_timestamp}:F> (unix: `{new_timestamp}`)")
                elif days_val != 0:
                    current_dt = datetime.fromtimestamp(current_timestamp, tz=timezone.utc)
                    new_dt = current_dt + timedelta(days=days_val)
                    new_timestamp = int(new_dt.timestamp())
                    response_messages.append(f"✅ Subscription duration updated. New expiry: <t:{new_timestamp}:F>")

                if new_timestamp != current_timestamp:
                    user_id = self.transaction_data['user_id']
                    item_details = db.get_item_by_name(self.transaction_data['item_description'])
                    role_id = item_details.get('associated_role_id') if item_details else None
                    if self.subscription_details and self.subscription_details.get('schedule_id'):
                        db.update_user_subscription(self.subscription_details['schedule_id'], new_timestamp)
                    elif role_id:
                        db.schedule_role_removal(user_id, role_id, new_timestamp)
                        response_messages.append("✅ Scheduled a new expiration for this subscription.")

                if new_timestamp < get_unix_time() and role_id:
                    # Remove the role immediately if the expiry is in the past
                    guild = interaction.guild or interaction.client.get_guild(interaction.guild_id)
                    if guild:
                        member = guild.get_member(user_id)
                        role = guild.get_role(role_id)
                        if member and role and role in member.roles:
                            try:
                                await member.remove_roles(role, reason="Subscription expired (manual edit)")
                                db.delete_scheduled_removal(db.get_user_subscription(user_id, role_id)['schedule_id'])
                                db.update_transaction_for_expiry(user_id=user_id, item_name=item_details['item_name'])
                                response_messages.append("✅ Role removed immediately due to past expiry.")
                            except Exception as e:
                                logger.error(f"Failed to remove expired role during edit: {e}", exc_info=True)
        
            except ValueError:
                response_messages.append("⚠️ Could not update duration: Invalid number format for days or timestamp.")
            except Exception as e:
                logger.error(f"Error updating subscription duration during edit: {e}", exc_info=True)
                response_messages.append("⚠️ An unexpected error occurred while updating the subscription duration.")        
        await interaction.response.send_message("\n".join(response_messages), ephemeral=True)
                
class StoreManagerCog(commands.Cog, name="Store Manager"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        logger.info("StoreManagerCog __init__: Initializing database...")
        db.initialize_database()
        
        self.config = db.get_config()
        if not self.config:
            logger.warning("No config found in database. Use admin commands to set up.")
            self.config = {}
            
        self._cog_loaded = False
        self._target_guild_id = bot.target_guild_id
        self.session: Optional[aiohttp.ClientSession] = None

    @commands.Cog.listener()
    async def on_ready(self):       
        if self.session is None or self.session.closed:
            logger.info("StoreManagerCog on_ready: Creating aiohttp.ClientSession.")
            self.session = aiohttp.ClientSession()

        if not self.check_role_expirations.is_running():
            self.check_role_expirations.start()
        if not self.update_subscriber_list_task.is_running():
            self.update_subscriber_list_task.start()
        if not self.verify_subscription_roles_task.is_running():
            self.verify_subscription_roles_task.start()
        if not self.audit_subscription_roles_task.is_running():
            self.audit_subscription_roles_task.start()
        
        self._cog_loaded = True
        logger.info("Store Manager Cog is ready and tasks are running.")

    def cog_unload(self):
        self.check_role_expirations.cancel()
        self.update_subscriber_list_task.cancel()
        self.audit_subscription_roles_task.cancel()
        self.verify_subscription_roles_task.cancel()
        if self.session and not self.session.closed:
            # When the cog unloads, close the session
            asyncio.create_task(self.session.close())
        logger.info("StoreManagerCog Unloaded and tasks cancelled.")

    async def cog_check(self, interaction: Interaction) -> bool:
        if not self._cog_loaded:
            await interaction.response.send_message("The Store Manager cog is not fully ready yet. Please try again in a moment.", ephemeral=True)
            return False
        return True

    async def _send_dm(self, member: nextcord.Member, embed: nextcord.Embed):
        """A helper function to send DMs and handle errors."""
        if not self.config.get('dm_receipts_enabled', True):
            return True
        try:
            await member.send(embed=embed)
            return True
        except nextcord.Forbidden:
            logger.warning(f"Could not send DM to {member.display_name} (DMs closed or bot blocked).")
            return False
        except Exception as e:
            logger.error(f"Failed to send DM to {member.display_name}", exc_info=True)
            return False

    async def item_autocomplete(self, interaction: Interaction, current_input: str) -> List[str]:
        items = db.get_all_store_items()
        return [item['item_name'] for item in items if current_input.lower() in item['item_name'].lower()][:25]
        
    async def subscription_item_autocomplete(self, interaction: Interaction, current_input: str) -> List[str]:
        items = db.get_all_store_items()
        return [item['item_name'] for item in items if item['is_subscription'] and current_input.lower() in item['item_name'].lower()][:25]

    @tasks.loop(minutes=5)
    async def check_role_expirations(self):
        now_ts = get_unix_time()
        due_removals = db.get_due_role_removals(now_ts)
        guild = self.bot.get_guild(self._target_guild_id)
        if not guild: return

        for removal in due_removals:
            try:
                member = await guild.fetch_member(removal['user_id'])
                role = guild.get_role(removal['role_id'])
                if member and role and role in member.roles:
                    await member.remove_roles(role, reason="Subscription expired")
                    logger.info(f"Removed expired role '{role.name}' from {member.display_name}")
                    
                    # --- Send DM on Expiration ---
                    dm_embed = Embed(
                        title="Subscription Expired",
                        description=f"Your **{role.name}** subscription has expired.\nThank you for your support!",
                        color=Color.orange()
                    )
                    await self._send_dm(member, dm_embed)

                    db.update_transaction_for_expiry(user_id=removal['user_id'], item_name=role.name)

            except Forbidden:
                logger.error(f"Failed to remove role ID {removal['role_id']}... Missing permissions.")
            except Exception as e:
                logger.error(f"Error processing role removal for schedule ID {removal['schedule_id']}: {e}")
            finally:
                db.delete_scheduled_removal(removal['schedule_id'])
            await asyncio.sleep(1)

    @tasks.loop(minutes=15)
    async def update_subscriber_list_task(self):
        if not self.session or self.session.closed:
            logger.warning("Subscriber list update skipped: aiohttp session is not ready.")
            return

        self.config = db.get_config()
        if not self.config or not self.config.get('subscriber_list_webhook_url'):
            return
            
        guild = self.bot.get_guild(self._target_guild_id)
        if not guild:
            return
            
        webhook_url = self.config.get('subscriber_list_webhook_url')
        webhook_message_ids = self.config.get('webhook_message_ids_json', {})
        embed_configs = self.config.get('embed_configs_json', {})
        
        subscription_items = [item for item in db.get_all_store_items() if item['is_subscription']]
        num_items = len(subscription_items)

        try:
            webhook = Webhook.from_url(webhook_url, session=self.session)
        except ValueError:
            logger.error("Invalid webhook URL for subscriber list.")
            return

        for i, item in enumerate(subscription_items):
            role_id = item.get('associated_role_id')
            if not role_id:
                continue
            
            role = guild.get_role(role_id)
            if not role:
                continue

            all_expiring_subs = {sub['user_id']: sub for sub in db.get_all_scheduled_removals() if sub['role_id'] == role_id}
            
            permanent_subscribers = []
            expiring_subscribers = []

            for member in role.members:
                if member.id in all_expiring_subs:
                    expiring_subscribers.append((member, all_expiring_subs[member.id]['removal_timestamp']))
                else:
                    permanent_subscribers.append(member)
            
            expiring_subscribers.sort(key=lambda x: x[1])
            
            default_template = (
                "- **{ingame.name}** | {user.mention}\n"
                "  - Expires: <t:{timestamp.code}:R>"
            )
            embed_config = embed_configs.get(str(role.id), {})
            desc_template = embed_config.get('description', default_template)

            description_lines = []
            
            for member in permanent_subscribers:
                sub_info = db.get_transaction_by_user_and_item(member.id, item['item_name'])
                ign = sub_info.get('ingame_name', "N/A") if sub_info else "N/A"
                line = (
                    f"- **{ign}** | {member.mention}\n"
                    f"  - Valid Permanently"
                )
                description_lines.append(line)
                
            if permanent_subscribers and expiring_subscribers:
                pass 

            for member, timestamp in expiring_subscribers:
                if timestamp < get_unix_time():
                    continue  # Skip expired subscriptions
                sub_info = db.get_transaction_by_user_and_item(member.id, item['item_name'])
                ign = sub_info.get('ingame_name', "N/A") if sub_info else "N/A"
                line = desc_template.replace('{user.mention}', member.mention)
                line = line.replace('{user.name}', member.display_name)
                line = line.replace('{ingame.name}', ign)
                line = line.replace('{timestamp.code}', str(timestamp))
                description_lines.append(line)

            if i == num_items - 1:
                footer_text = self.config.get('subscriber_list_footer_text')
                if footer_text:
                    if description_lines:
                        description_lines.append("") 
                    description_lines.append(footer_text)
            
            embed = Embed(
                title=f"**{role.name} Subscribers**",
                description="\n\n".join(description_lines) if description_lines else "No subscribers.",
                color=role.color or Color.blue()
            )
            if tn_url := embed_config.get('thumbnail_url'):
                embed.set_thumbnail(url=tn_url)
            if img_url := embed_config.get('image_url'):
                embed.set_image(url=img_url)

            message_id = webhook_message_ids.get(str(role.id))
            try:
                if message_id:
                    await webhook.edit_message(message_id, embed=embed)
                else:
                    new_msg = await webhook.send(embed=embed, wait=True)
                    webhook_message_ids[str(role.id)] = new_msg.id
                    db.update_config({'webhook_message_ids_json': webhook_message_ids})
            except (Forbidden, nextcord.NotFound):
                logger.error(f"Webhook/message permissions error for subscriber list '{role.name}'. It might have been deleted.")
                if str(role.id) in webhook_message_ids:
                    del webhook_message_ids[str(role.id)]
                    db.update_config({'webhook_message_ids_json': webhook_message_ids})
            except Exception as e:
                logger.error(f"Failed to update subscriber list for role '{role.name}': {e}", exc_info=True)
            
            await asyncio.sleep(2)

    @tasks.loop(minutes=15)
    async def verify_subscription_roles_task(self):
        """Periodically checks if users with active subscriptions have the correct role."""
        logger.info("Starting periodic verification of subscription roles.")
        guild = self.bot.get_guild(self._target_guild_id)
        if not guild:
            logger.error("Role verification task: Guild not found.")
            return

        all_active_subscriptions = db.get_all_scheduled_removals()
        if not all_active_subscriptions:
            logger.info("Role verification task: No active subscriptions to verify.")
            return
            
        for sub in all_active_subscriptions:
            user_id = sub['user_id']
            role_id = sub['role_id']

            # Only re-apply if the scheduled removal is still in the DB and not expired
            if sub['removal_timestamp'] < get_unix_time():
                continue  # Skip expired subscriptions

            member = guild.get_member(user_id)
            role = guild.get_role(role_id)

            if not role:
                logger.info(f"Role {role_id} for an active sub no longer exists. Removing subscription record for user {user_id}.")
                db.delete_scheduled_removal(sub['schedule_id'])
                await asyncio.sleep(0.5)
                continue

            if member and role not in member.roles:
                logger.warning(f"User {member.display_name} is missing active subscription role '{role.name}'. Re-applying now.")
                try:
                    await member.add_roles(role, reason="Store Manager: Re-applying active subscription role.")
                except nextcord.Forbidden:
                    logger.error(f"Failed to re-apply role '{role.name}' to {member.display_name}. Check bot permissions and hierarchy.")
                except Exception as e:
                    logger.error(f"An unexpected error occurred while re-applying role '{role.name}' to {member.display_name}: {e}")

            await asyncio.sleep(1)
        logger.info("Finished periodic verification of subscription roles.")

    @tasks.loop(hours=1)
    async def audit_subscription_roles_task(self):
        logger.info("Starting hourly audit of assigned subscription roles.")
        guild = self.bot.get_guild(self._target_guild_id)
        if not guild: return

        all_sub_items = [item for item in db.get_all_store_items() if item.get('is_subscription')]
        if not all_sub_items: return

        for item in all_sub_items:
            role_id = item.get('associated_role_id')
            if not role_id: continue
            role = guild.get_role(role_id)
            if not role: continue

            for member in role.members:
                active_sub = db.get_user_subscription(member.id, role.id)
                if active_sub: continue
                has_permanent_purchase = db.user_has_purchase_record(member.id, item['item_name'])
                if has_permanent_purchase: continue
                
                logger.warning(f"Role audit: Removing role '{role.name}' from {member.display_name} - no valid subscription record found.")
                try:
                    await member.remove_roles(role, reason="Store Manager: Role audit - No valid subscription record.")
                    # --- Send DM on Audit Removal ---
                    dm_embed = Embed(
                        title="Subscription Removed",
                        description=f"Your **{role.name}** rank has been removed by an automated audit because no valid subscription record was found.",
                        color=Color.dark_grey()
                    )
                    await self._send_dm(member, dm_embed)
                except nextcord.Forbidden:
                    logger.error(f"Role audit: FAILED to remove role '{role.name}' from {member.display_name}. Check permissions/hierarchy.")
                except Exception as e:
                    logger.error(f"Role audit: An unexpected error occurred removing role from {member.display_name}: {e}")
                
                await asyncio.sleep(2)
        
        logger.info("Finished hourly role audit.")

    @check_role_expirations.before_loop
    @update_subscriber_list_task.before_loop
    async def before_tasks(self):
        await self.bot.wait_until_ready()

    @nextcord.slash_command(name="store", description="Commands for managing store transactions.")
    async def store_group(self, interaction: Interaction): pass

    @store_group.subcommand(name="add", description="Log a new purchase or donation.")
    # @application_checks.has_permissions(manage_guild=True)
    async def store_add(self, interaction: Interaction, user: Member, type: str = SlashOption(choices=["Purchase", "Donation"]), item: str = SlashOption(autocomplete=True), quantity: Optional[int] = None, notes: Optional[str] = None):
        item_data = db.get_item_by_name(item)
        if item_data and item_data.get('is_subscription'):
            role_id = item_data.get('associated_role_id')
            if not role_id:
                await interaction.response.send_message(f"❌ Item '{item}' is a subscription but has no linked role.", ephemeral=True)
                return
            role_to_assign = interaction.guild.get_role(role_id)
            if not role_to_assign:
                await interaction.response.send_message(f"❌ Could not find the role linked to '{item}'.", ephemeral=True)
                return
            await interaction.response.send_modal(SubscriptionModal(interaction, item, role_to_assign, self))
        else:
            await interaction.response.defer(ephemeral=True)
            transaction_id = db.add_transaction(guild_id=interaction.guild.id, user_id=user.id, username_at_time=str(user), trans_type=type, item=item, admin_id=interaction.user.id, quantity=quantity, notes=notes, ign=None, timestamp=get_unix_time())
            receipt_sent = False
            if self.config.get('dm_receipts_enabled', True):
                try:
                    embed = Embed(title="Transaction Receipt", description=f"Thank you for your {type.lower()}!", color=Color.green())
                    embed.add_field(name="Item", value=item, inline=True)
                    if quantity: embed.add_field(name="Quantity", value=quantity, inline=True)
                    if notes: embed.add_field(name="Notes", value=notes, inline=False)
                    embed.set_footer(text=f"Transaction ID: {transaction_id}")
                    await user.send(embed=embed)
                    receipt_sent = True
                except Forbidden:
                    logger.warning(f"Could not send DM receipt to {user.display_name} (DMs closed).")
            response_msg = f"✅ Logged {type} for {user.mention}: **{item}** (ID: `{transaction_id}`)."
            if not receipt_sent and self.config.get('dm_receipts_enabled'):
                response_msg += "\n*Note: Could not send a DM receipt to the user (their DMs are likely closed).*"
            await interaction.followup.send(response_msg, ephemeral=False)
            
    @store_add.on_autocomplete("item")
    async def store_add_autocomplete(self, interaction: Interaction, item_input: str):
        choices = await self.item_autocomplete(interaction, item_input)
        await interaction.response.send_autocomplete(choices)

    @store_group.subcommand(name="list", description="View a user's transaction history with pagination.")
    # @application_checks.has_permissions(manage_guild=True)
    async def store_list(self, interaction: Interaction, user: Member):
        await interaction.response.defer(ephemeral=False)
        transactions = db.get_user_transactions(user.id)
        if not transactions:
            await interaction.followup.send(f"{user.mention} has no transaction history.", ephemeral=True)
            return
        view = TransactionHistoryView(interaction, transactions, user)
        embed = await view.get_page_embed()
        await interaction.followup.send(embed=embed, view=view, ephemeral=False)

    @store_group.subcommand(name="edit", description="Edit a past transaction entry, including subscription duration.")
    @application_checks.has_permissions(manage_guild=True)
    async def store_edit(self, interaction: Interaction, transaction_id: int):
        transaction = db.get_transaction(transaction_id)
        if not transaction:
            await interaction.response.send_message(f"Transaction ID `{transaction_id}` not found.", ephemeral=True)
            return
            
        # Check if this transaction is for a subscription to show the correct modal
        item_details = db.get_item_by_name(transaction['item_description'])
        is_subscription = False
        subscription_details = None

        if item_details and item_details.get('is_subscription'):
            is_subscription = True
            role_id = item_details.get('associated_role_id')
            user_id = transaction.get('user_id')
            if role_id and user_id:
                # This will be None for permanent subs, which is what we want.
                # The modal will not show duration fields for permanent subs.
                subscription_details = db.get_user_subscription(user_id, role_id)

        await interaction.response.send_modal(
            EditTransactionModal(
                transaction_data=transaction, 
                is_subscription=is_subscription, 
                subscription_details=subscription_details
            )
        )

    @store_group.subcommand(name="remove", description="Delete a transaction entry.")
    @application_checks.has_permissions(manage_guild=True)
    async def store_remove(self, interaction: Interaction, transaction_id: int):
        if db.remove_transaction(transaction_id):
            await interaction.response.send_message(f"✅ Transaction ID `{transaction_id}` has been deleted.", ephemeral=True)
        else:
            await interaction.response.send_message(f"❌ Transaction ID `{transaction_id}` not found.", ephemeral=True)
            
    @nextcord.slash_command(name="store_admin", description="Admin commands for the store.")
    @application_checks.has_permissions(administrator=True)
    async def store_admin_group(self, interaction: Interaction): pass
    
    @store_admin_group.subcommand(name="items_add", description="Add a new item to the autocomplete list.")
    async def items_add(self, interaction: Interaction, category: str, item_name: str):
        if db.add_store_item(category, item_name):
            await interaction.response.send_message(f"✅ Item `{item_name}` added to category `{category}`.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ That item name already exists.", ephemeral=True)

    @store_admin_group.subcommand(name="items_remove", description="Remove an item from the autocomplete list.")
    async def items_remove(self, interaction: Interaction, item_name: str = SlashOption(autocomplete=True)):
        if db.remove_store_item(item_name):
            await interaction.response.send_message(f"✅ Item `{item_name}` removed.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Item not found.", ephemeral=True)
    
    @items_remove.on_autocomplete("item_name")
    async def items_remove_autocomplete(self, i: Interaction, val: str):
        await i.response.send_autocomplete(await self.item_autocomplete(i, val))

    @store_admin_group.subcommand(name="items_list", description="List all configurable store items.")
    async def items_list(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        items = db.get_all_store_items()
        if not items:
            await interaction.followup.send("No items configured.", ephemeral=True)
            return
        embed = Embed(title="Configured Store Items", color=Color.purple())
        by_category = {}
        for item in items:
            cat = item['category']
            if cat not in by_category: by_category[cat] = []
            by_category[cat].append(item)
        for category, cat_items in by_category.items():
            value = "\n".join([f"- `{i['item_name']}`" for i in cat_items])
            embed.add_field(name=category, value=value, inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @store_admin_group.subcommand(name="toggle_subscription", description="Toggle if an item is a subscription rank.")
    async def toggle_subscription(self, interaction: Interaction, item_name: str = SlashOption(autocomplete=True)):
        item = db.get_item_by_name(item_name)
        if not item:
            await interaction.response.send_message("❌ Item not found.", ephemeral=True)
            return
        new_status = not item['is_subscription']
        db.update_store_item(item_name, {'is_subscription': new_status})
        status_text = "ENABLED" if new_status else "DISABLED"
        await interaction.response.send_message(f"✅ Subscription status for `{item_name}` is now **{status_text}**.", ephemeral=True)

    @toggle_subscription.on_autocomplete("item_name")
    async def toggle_sub_autocomplete(self, i: Interaction, val: str):
        await i.response.send_autocomplete(await self.item_autocomplete(i, val))
        
    @store_admin_group.subcommand(name="link_item_to_role", description="Link a subscription item to a Discord role.")
    async def link_item_to_role(self, interaction: Interaction, item_name: str = SlashOption(autocomplete=True), role: Role = SlashOption()):
        item = db.get_item_by_name(item_name)
        if not item or not item['is_subscription']:
            await interaction.response.send_message("❌ Item not found or is not a subscription.", ephemeral=True)
            return
        db.update_store_item(item_name, {'associated_role_id': role.id})
        await interaction.response.send_message(f"✅ Item `{item_name}` is now linked to the role {role.mention}.", ephemeral=True)

    @link_item_to_role.on_autocomplete("item_name")
    async def link_item_autocomplete(self, i: Interaction, val: str):
        await i.response.send_autocomplete(await self.subscription_item_autocomplete(i, val))
        
    @store_admin_group.subcommand(name="config_subscriber_list", description="Set up the channel and webhook for the live subscriber list.")
    async def config_subscriber_list(self, interaction: Interaction, channel: TextChannel, webhook_url: str):
        if not webhook_url.startswith("https://discord.com/api/webhooks/"):
            await interaction.response.send_message("Invalid Webhook URL format.", ephemeral=True)
            return
        updates = {'subscriber_list_channel_id': channel.id, 'subscriber_list_webhook_url': webhook_url}
        db.update_config(updates)
        self.config = db.get_config()
        await interaction.response.send_message(f"✅ Subscriber list configured for {channel.mention}.", ephemeral=True)

    @store_admin_group.subcommand(name="config_subscriber_embed", description="Customize the embed for a specific subscription rank.")
    async def config_subscriber_embed(
        self, interaction: Interaction,
        rank_item: str = SlashOption(autocomplete=True),
        description_template: Optional[str] = SlashOption(description="The template, or 'reset' to clear.", required=False),
        thumbnail_url: Optional[str] = SlashOption(description="The thumbnail URL, or 'reset' to clear.", required=False),
        image_url: Optional[str] = SlashOption(description="The image URL, or 'reset' to clear.", required=False)
    ):
        item = db.get_item_by_name(rank_item)
        if not item or not item['associated_role_id']:
            await interaction.response.send_message("Invalid item or item not linked to a role.", ephemeral=True)
            return
            
        role_id = str(item['associated_role_id'])
        # Make a copy of the configs to modify
        current_embed_configs = self.config.get('embed_configs_json', {}).copy()
        
        if role_id not in current_embed_configs:
            current_embed_configs[role_id] = {}

        response_messages = []

        # Handle Description
        if description_template is not None:
            if description_template.lower() in ['reset', 'default', 'none', 'clear']:
                if 'description' in current_embed_configs[role_id]:
                    del current_embed_configs[role_id]['description']
                    response_messages.append("✅ Description reset to default.")
            else:
                current_embed_configs[role_id]['description'] = description_template
                response_messages.append("✅ Description template updated.")

        # Handle Thumbnail
        if thumbnail_url is not None:
            if thumbnail_url.lower() in ['reset', 'default', 'none', 'clear']:
                if 'thumbnail_url' in current_embed_configs[role_id]:
                    del current_embed_configs[role_id]['thumbnail_url']
                    response_messages.append("✅ Thumbnail URL cleared.")
            else:
                current_embed_configs[role_id]['thumbnail_url'] = thumbnail_url
                response_messages.append("✅ Thumbnail URL updated.")

        # Handle Image
        if image_url is not None:
            if image_url.lower() in ['reset', 'default', 'none', 'clear']:
                if 'image_url' in current_embed_configs[role_id]:
                    del current_embed_configs[role_id]['image_url']
                    response_messages.append("✅ Image URL cleared.")
            else:
                current_embed_configs[role_id]['image_url'] = image_url
                response_messages.append("✅ Image URL updated.")

        # Clean up empty role config to keep database tidy
        if not current_embed_configs[role_id]:
            del current_embed_configs[role_id]
            
        db.update_config({'embed_configs_json': current_embed_configs})
        self.config = db.get_config() # Reload config
        
        if not response_messages:
            await interaction.response.send_message("No changes were specified.", ephemeral=True)
        else:
            await interaction.response.send_message("\n".join(response_messages), ephemeral=True)


    @config_subscriber_embed.on_autocomplete("rank_item")
    async def config_embed_autocomplete(self, i: Interaction, val: str):
        await i.response.send_autocomplete(await self.subscription_item_autocomplete(i, val))

    @store_admin_group.subcommand(name="set_subscriber_footer", description="Set or clear a footer message on the subscriber list.")
    async def set_subscriber_footer(
        self,
        interaction: Interaction,
        text: Optional[str] = SlashOption(description="The message to display. Omit this option to clear the message.", required=False)
    ):
        await interaction.response.defer(ephemeral=True)

        db.update_config({'subscriber_list_footer_text': text})
        self.config = db.get_config() # Reload the cog's config with the new value

        if text:
            await interaction.followup.send(f"✅ Subscriber list footer message has been set.", ephemeral=True)
        else:
            await interaction.followup.send(f"✅ Subscriber list footer message has been cleared.", ephemeral=True)
        try:
            await self.update_subscriber_list_task.coro(self)
        except Exception as e:
            logger.error(f"Failed to auto-trigger list update after setting footer: {e}")

    @store_admin_group.subcommand(name="toggle_receipts", description="Toggle automated DM receipts on or off.")
    async def toggle_receipts(self, interaction: Interaction):
        new_status = not self.config.get('dm_receipts_enabled', True)
        db.update_config({'dm_receipts_enabled': new_status})
        self.config = db.get_config()
        status_text = "ENABLED" if new_status else "DISABLED"
        await interaction.response.send_message(f"✅ Automated DM receipts are now **{status_text}**.", ephemeral=True)

    @store_admin_group.subcommand(name="force_subscriber_update", description="Forces an immediate update of the subscriber list embeds.")
    async def force_subscriber_update(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"Force subscriber list update triggered by {interaction.user}.")
        try:
            await self.update_subscriber_list_task.coro(self) 
            await interaction.followup.send("✅ Manually triggered the subscriber list update. Please check the channel.", ephemeral=True)
        except Exception as e:
            logger.error(f"Error during forced subscriber update: {e}", exc_info=True)
            await interaction.followup.send(f"An error occurred during the update. Please check the console logs.", ephemeral=True)

    @store_admin_group.subcommand(name="remove_subscription", description="Manually remove a subscription role and cancel its expiry task.")
    async def remove_subscription(
        self,
        interaction: Interaction,
        user: Member = SlashOption(description="The user to remove the subscription from.", required=True),
        rank_item: str = SlashOption(description="The subscription rank to remove.", required=True, autocomplete=True)
    ):
        await interaction.response.defer(ephemeral=True)
        item_details = db.get_item_by_name(rank_item)
        if not item_details or not item_details.get('is_subscription') or not item_details.get('associated_role_id'):
            await interaction.followup.send(f"❌ '{rank_item}' is not a valid subscription item with a linked role.", ephemeral=True)
            return

        role = interaction.guild.get_role(item_details['associated_role_id'])
        if not role:
            await interaction.followup.send(f"❌ Could not find the role for '{rank_item}'.", ephemeral=True)
            return

        response_messages = []
        if role in user.roles:
            try:
                await user.remove_roles(role, reason=f"Manual removal by admin {interaction.user}")
                response_messages.append(f"✅ Role {role.mention} has been removed from {user.mention}.")
                # --- Send DM on Manual Removal ---
                dm_embed = Embed(
                    title="Subscription Removed",
                    description=f"An administrator has manually removed your **{role.name}** rank.",
                    color=Color.dark_red()
                )
                await self._send_dm(user, dm_embed)
            except nextcord.Forbidden:
                await interaction.followup.send(f"❌ I do not have permission to remove the role {role.mention}.", ephemeral=True)
                return
            except Exception as e:
                logger.error(f"Failed to remove role during manual sub removal: {e}", exc_info=True)
                await interaction.followup.send(f"❌ An unexpected error occurred while trying to remove the role.", ephemeral=True)
                return
        else:
            response_messages.append(f"ℹ️ User {user.mention} did not have the {role.mention} role.")

        subscription_details = db.get_user_subscription(user.id, role.id)
        if subscription_details:
            db.delete_scheduled_removal(subscription_details['schedule_id'])
            response_messages.append("✅ The scheduled expiration task has been cancelled.")
        else:
            response_messages.append("ℹ️ No scheduled expiration task was found (it may have been permanent or already removed).")

        await interaction.followup.send("\n".join(response_messages), ephemeral=True)
        pass

    @remove_subscription.on_autocomplete("rank_item")
    async def remove_sub_autocomplete(self, i: nextcord.Interaction, val: str):
        await i.response.send_autocomplete(await self.subscription_item_autocomplete(i, val))

    @store_admin_group.subcommand(name="recover_expirations", description="Automatically recover missing expiration records for donator roles.")
    @application_checks.has_permissions(administrator=True)
    async def recover_expirations(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        store_items = db.get_all_store_items()
        sub_items = {item['item_name']: item for item in store_items if item.get('is_subscription') and item.get('associated_role_id')}
        transactions = []
        for item_name in sub_items:
            transactions += db.get_transactions_by_item(item_name)
        recovered = 0
        already_present = 0
        skipped = 0
        now = get_unix_time()
        guild = interaction.guild
        for trans in transactions:
            item = sub_items.get(trans['item_description'])
            if not item:
                skipped += 1
                continue
            role_id = item['associated_role_id']
            user_id = trans['user_id']
            is_perm = trans.get('is_permanent', 0)
            expired = trans.get('expired', 0)
            if is_perm or expired:
                skipped += 1
                continue
            # Estimate expiration: If original record missing, set to purchase timestamp + 30 days (or configurable)
            duration_days = 30
            if 'duration_days' in trans:
                duration_days = trans['duration_days']
            elif 'months' in trans and trans['months']:
                duration_days = int(trans['months']) * 30
            expiry_ts = trans['timestamp'] + duration_days * 24 * 3600
            # If the scheduled removal already exists, skip
            existing = db.get_user_subscription(user_id, role_id)
            if existing:
                already_present += 1
                continue
            if expiry_ts < now:
                skipped += 1
                continue
            db.schedule_role_removal(user_id, role_id, expiry_ts)
            recovered += 1
            # Optional: re-assign role if missing
            member = guild.get_member(user_id)
            role = guild.get_role(role_id)
            if member and role and role not in member.roles:
                try:
                    await member.add_roles(role, reason="Recovered subscription role")
                except Exception:
                    pass
        await interaction.followup.send(f"Recovered {recovered} expiration records. {already_present} already present. {skipped} skipped (permanent, expired, or missing info).", ephemeral=True)

def setup(bot: commands.Bot):
    global logger
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    
    bot.add_cog(StoreManagerCog(bot))
    logger.info("StoreManagerCog has been loaded.")