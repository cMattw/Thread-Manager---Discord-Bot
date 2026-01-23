import nextcord
from nextcord.ext import commands, tasks, application_checks
from nextcord import Interaction, SlashOption, Embed, Color, Member, Role, TextChannel, Webhook
import logging
from datetime import datetime, timezone
import aiohttp

from db_utils import booster_database as db

logger = logging.getLogger('nextcord.boost_tracker_cog')
if not logger.handlers:
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
logger.setLevel(logging.INFO)  # Or DEBUG for more verbosity

NITRO_PINK = Color(0xf47fff)

def format_duration(total_days: int) -> str:
    """Formats a duration in days into a more readable string like '1 year, 2 months'."""
    if total_days is None or total_days < 0:
        return "N/A"
    if total_days == 0:
        return "0 days"

    years, remaining_days = divmod(total_days, 365)
    months, days = divmod(remaining_days, 30)

    parts = []
    if years > 0:
        parts.append(f"{years} year{'s' if years != 1 else ''}")
    if months > 0:
        parts.append(f"{months} month{'s' if months != 1 else ''}")
    
    if years == 0 and days > 0:
        parts.append(f"{days} day{'s' if days != 1 else ''}")
    
    if not parts:
        return "Less than a day" if total_days > 0 else "0 days"
        
    return ", ".join(parts)


class BoostTrackerCog(commands.Cog, name="Boost Tracker"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.target_guild_id = bot.target_guild_id
        db.initialize_database()
        self.check_boosters_task.start()
        self.sync_boosters_task.start()
        self.initial_scan_done = False

    def cog_unload(self):
        self.check_boosters_task.cancel()

    # --- EVENT LISTENERS ---

    @commands.Cog.listener()
    async def on_ready(self):
        if self.initial_scan_done: return
        await self.bot.wait_until_ready()
        guild = self.bot.get_guild(self.target_guild_id)
        if not guild:
            logger.error(f"Initial booster scan: Target guild {self.target_guild_id} not found.")
            return
        
        logger.info("Performing initial scan for existing boosters...")
        async for member in guild.fetch_members(limit=None):
            if member.premium_since is not None:
                booster_data = db.get_booster(str(member.id))
                if not booster_data or not booster_data.get('is_currently_boosting'):
                    # IMPORTANT: Ensure your start_new_boost function does NOT increment the boost count.
                    db.start_new_boost(str(member.id), str(guild.id), int(member.premium_since.timestamp()))
        self.initial_scan_done = True
        logger.info("Initial booster scan complete. Running first monthly count update.")
        await self.check_boosters_task.coro(self)

    @commands.Cog.listener()
    async def on_member_update(self, before: Member, after: Member):
        if before.premium_since == after.premium_since or after.guild.id != self.target_guild_id: return
        now_ts = int(datetime.now(timezone.utc).timestamp())
        if before.premium_since is None and after.premium_since is not None:
            # IMPORTANT: Ensure this function does NOT increment the boost count itself.
            db.start_new_boost(str(after.id), str(after.guild.id), int(after.premium_since.timestamp()))
        elif before.premium_since is not None and after.premium_since is None:
            db.end_boost(str(after.id), now_ts)
            
    # ADDED: Listener for boost messages to accurately count boosts
    @commands.Cog.listener()
    async def on_message(self, message: nextcord.Message):
        """Listen for official boost messages to increment the count and send announcements."""
        if not message.guild or message.guild.id != self.target_guild_id:
            return
        
        # Check if the message is a server boost message
        if message.type == nextcord.MessageType.premium_guild_subscription:
            booster = message.author
            if not isinstance(booster, Member): return

            logger.info(f"Detected boost message from {booster.name}. Incrementing count.")
            # Increment the boost count by 1 for this event
            db.increment_boost_count(str(booster.id), 1)

            # --- Send Welcome Message for New Boost ---
            config = db.get_config(str(message.guild.id))
            webhook_url = config.get("booster_announcement_webhook_url")
            template = config.get("welcome_message_template", "Thank you {mention} for boosting {server}! 🚀")
            rate = config.get("keys_per_month", 1) # Default to 1 if not set
            db.add_claimed_keys(str(message.author.id), rate)

            content = template.format(
                mention=booster.mention,
                user=booster.name,
                server=message.guild.name
            )
            
            if webhook_url:
                logger.info(f"Attempting to send welcome message via webhook: {webhook_url}")
                async with aiohttp.ClientSession() as session:
                    try:
                        webhook = Webhook.from_url(webhook_url, session=session)
                        await webhook.send(content)
                        logger.info("Welcome message sent via webhook.")
                    except Exception as e:
                        logger.error(f"Failed to send welcome webhook: {e}")
            else:
                channel_id = config.get("announcement_channel_id")
                if channel_id and (channel := self.bot.get_channel(int(channel_id))):
                    await channel.send(content)
                    logger.info("Welcome message sent via fallback channel.")

    # --- TASKS ---

    @tasks.loop(hours=1)
    async def sync_boosters_task(self):
        await self.bot.wait_until_ready()
        guild = self.bot.get_guild(self.target_guild_id)
        if not guild:
            logger.error("Guild not found for booster sync.")
            return

        booster_role = guild.premium_subscriber_role
        if not booster_role:
            logger.error("Server Booster role not found for booster sync.")
            return

        boosters_in_guild = set(member.id for member in booster_role.members)
        db_boosters = set(int(b['user_id']) for b in db.get_all_boosters_for_leaderboard() if b.get('is_currently_boosting'))

        # Mark as not boosting in DB if not in role
        for user_id in db_boosters - boosters_in_guild:
            db.end_boost(str(user_id), int(datetime.now(timezone.utc).timestamp()))
            logger.info(f"Marked user {user_id} as not boosting (sync task).")

        # Mark as boosting in DB if in role but not in DB
        for user_id in boosters_in_guild - db_boosters:
            member = guild.get_member(user_id)
            if member and member.premium_since:
                db.start_new_boost(str(user_id), str(guild.id), int(member.premium_since.timestamp()))
                logger.info(f"Marked user {user_id} as boosting (sync task).")

        logger.info(
            f"Booster sync complete. "
            f"Marked {len(db_boosters - boosters_in_guild)} as not boosting, "
            f"{len(boosters_in_guild - db_boosters)} as boosting."
        )
    
    @tasks.loop(hours=1)
    async def check_boosters_task(self):
        await self.bot.wait_until_ready()
        guild = self.bot.get_guild(self.target_guild_id)
        if not guild: return

        logger.info("Running daily check for monthly booster count updates...")
        active_boosters = [b for b in db.get_all_boosters_for_leaderboard() if b.get('is_currently_boosting')]
        now = datetime.now(timezone.utc)
        config = db.get_config(str(guild.id))

        # Get the role ID and month threshold from config
        reward_roles = db.get_all_reward_roles()  # List of dicts: {'duration_months': int, 'role_id': str}
        if not reward_roles:
            logger.info("No reward roles configured.")
            return

        for booster_data in active_boosters:
            user_id = str(booster_data['user_id'])
            start_ts = booster_data.get('current_boost_start_timestamp')
            if not start_ts:
                continue

            boost_start = datetime.fromtimestamp(start_ts, tz=timezone.utc)
            months_boosted = (now.year - boost_start.year) * 12 + (now.month - boost_start.month)
            if now.day < boost_start.day:
                months_boosted -= 1

            # Skip if less than 1 month
            if months_boosted < 1:
                continue

            member = guild.get_member(int(user_id))
            if not member:
                try:
                    member = await guild.fetch_member(int(user_id))
                except Exception:
                    logger.warning(f"Could not fetch member {user_id}")
                    continue

            # Check if we need to send anniversary message
            last_notified = booster_data.get('last_anniversary_notified', 0)
            if months_boosted > last_notified:
                rate = config.get("keys_per_month", 1)
                db.add_claimed_keys(user_id, rate)
                db.increment_boost_count(user_id, 1)
                # Send anniversary message
                template = config.get("anniversary_message_template", "{mention} has been boosting for {months} {month_label}!")
                month_label = "month" if months_boosted == 1 else "months"
                content = template.format(
                    mention=member.mention,
                    user=member.name,
                    server=guild.name,
                    months=months_boosted,
                    month_label=month_label
                )
                
                webhook_url = config.get("booster_announcement_webhook_url")
                if webhook_url:
                    logger.info(f"Attempting to send anniversary message via webhook for {member.name}")
                    async with aiohttp.ClientSession() as session:
                        try:
                            webhook = Webhook.from_url(webhook_url, session=session)
                            await webhook.send(content)
                            logger.info("Anniversary message sent via webhook.")
                        except Exception as e:
                            logger.error(f"Failed to send anniversary webhook: {e}")
                else:
                    channel_id = config.get("announcement_channel_id")
                    if channel_id and (channel := self.bot.get_channel(int(channel_id))):
                        await channel.send(content)
                        logger.info("Anniversary message sent via fallback channel.")
                
                # Update the last notified milestone
                db.update_anniversary_notified(user_id, months_boosted)

            # Assign roles
            for reward in reward_roles:
                milestone = reward['duration_months']
                role = guild.get_role(int(reward['role_id']))
                if not role:
                    logger.warning(f"Role ID {reward['role_id']} not found in guild.")
                    continue
                
                if months_boosted >= milestone and role not in member.roles:
                    try:
                        await member.add_roles(role, reason=f"Reached {milestone} months of boosting.")
                        logger.info(f"Gave {role.name} to {member.display_name} for {months_boosted} months of boosting.")
                    except Exception as e:
                        logger.error(f"Failed to assign role to {member.display_name}: {e}")
                        
    # --- COMMANDS ---

    @nextcord.slash_command(name="boost", description="User booster info and leaderboard.")
    async def boost_group(self, interaction: Interaction):
        pass

    @boost_group.subcommand(name="list", description="Displays the booster leaderboard.")
    async def list_boosters(self, interaction: Interaction,
        sort_by: str = SlashOption(
            name="sort_by",
            description="Choose how to rank the boosters.",
            choices={
                "Current Streak": "streak", 
                "Total Boost Count": "count", 
                "Total Duration": "duration"
            },
            default="count"
        )):
        all_boosters_data = db.get_all_boosters_for_leaderboard()
        if not all_boosters_data:
            return await interaction.send("There are no boosters to display.", ephemeral=True)
        now = datetime.now(timezone.utc)
        def get_true_total_duration(b):
            days = b.get('total_duration_days', 0)
            if b.get('is_currently_boosting') and b.get('current_boost_start_timestamp'):
                start = datetime.fromtimestamp(b.get('current_boost_start_timestamp'), tz=timezone.utc)
                days += (now - start).days
            return days
        if sort_by == "streak":
            active_boosters = [b for b in all_boosters_data if b.get('is_currently_boosting')]
            sorted_boosters = sorted(active_boosters, key=lambda b: b.get('current_boost_start_timestamp') or 0)
            title = "Booster Leaderboard (Current Streak)"
        elif sort_by == "count":
            sorted_boosters = sorted(all_boosters_data, key=lambda b: b.get('total_boost_count', 0), reverse=True)
            title = "Booster Leaderboard (Total Boost Count)"
        else: # duration
            sorted_boosters = sorted(all_boosters_data, key=get_true_total_duration, reverse=True)
            title = "Booster Leaderboard (Total Duration)"
        embed = Embed(title=title, color=NITRO_PINK, timestamp=now)
        description = ""
        for i, booster_data in enumerate(sorted_boosters[:20], 1):
            user = self.bot.get_user(int(booster_data['user_id']))
            if not user: continue
            display_str = ""
            if sort_by == 'streak':
                start_ts = booster_data.get('current_boost_start_timestamp')
                if start_ts: display_str = f"Boosting since: <t:{start_ts}:D>"
                else: display_str = "Streak: `N/A`"
            elif sort_by == 'count':
                display_str = f"Boost Count: `{booster_data.get('total_boost_count', 0)}`"
            else: # duration
                total_days = get_true_total_duration(booster_data)
                display_str = f"Total duration: `{format_duration(total_days)}`"
            line = f"**{i}.** {user.mention} - {display_str}\n"
            if not booster_data.get('is_currently_boosting'):
                line = f"~~{line.strip()}~~\n"
            description += line
        if not description: description = "No boosters to display for this category."
        embed.description = description
        await interaction.send(embed=embed)

    @boost_group.subcommand(name="status", description="View the boost status of a specific user.")
    async def history(self, interaction: Interaction, user: Member = SlashOption(description="The user to check.")):
        booster_stats = db.get_booster(str(user.id))
        if not booster_stats:
            return await interaction.send(f"{user.display_name} has no boosting history.", ephemeral=False)
        embed = Embed(title=f"{user.display_name}'s Boost Status", color=NITRO_PINK)
        embed.set_thumbnail(url=user.display_avatar.url)
        total_boosts = booster_stats.get('total_boost_count', 0)
        first_boost_ts = booster_stats.get('first_boost_timestamp')
        now = datetime.now(timezone.utc)
        total_days = booster_stats.get('total_duration_days', 0)
        if booster_stats.get('is_currently_boosting') and booster_stats.get('current_boost_start_timestamp'):
            current_start = datetime.fromtimestamp(booster_stats.get('current_boost_start_timestamp'), tz=timezone.utc)
            total_days += (now - current_start).days

        boost_status = "**Active**" if booster_stats.get('is_currently_boosting') else "**Inactive**"
        claimed_keys = booster_stats.get('claimed_keys', 0)
        available_keys = max(0, total_boosts * 2 - claimed_keys)

        desc_parts = [
            f"**Status:** {boost_status}",
            f"**Total Boost Count:** `{total_boosts}`",
            f"**Total Time Boosted:** `{format_duration(total_days)}`"
        ]
        if first_boost_ts:
            desc_parts.append(f"**First Boosted On:** <t:{first_boost_ts}:D>")
        embed.description = "\n".join(desc_parts)

        # Add the Rewards field
        embed.add_field(
            name="Rewards",
            value=f"**Available:** `{available_keys} Keys`\n**Claimed:** `{claimed_keys} Keys`",
            inline=False
        )

        await interaction.send(embed=embed, ephemeral=False)

    @nextcord.slash_command(name="booster", description="Commands for managing server boosters.")
    async def booster_group(self, interaction: Interaction):
        pass
        
    @booster_group.subcommand(name="reward", description="Add or deduct claimed reward keys to a booster.")
    @application_checks.has_permissions(manage_guild=True)
    async def reward(self, interaction: Interaction, user: Member = SlashOption(description="The user to add/deduct keys for."), amount: int = SlashOption(description="Number of keys to add (use negative to deduct).")):
        booster_stats = db.get_booster(str(user.id))
        if not booster_stats:
            return await interaction.send(f"{user.display_name} has no boosting history.", ephemeral=True)

        total_boosts = booster_stats.get('total_boost_count', 0)
        claimed_keys = booster_stats.get('claimed_keys', 0)
        available_keys = max(0, total_boosts * 2 - claimed_keys)

        if amount == 0:
            return await interaction.send("Amount must not be zero.", ephemeral=True)
        if amount > 0:
            if amount > available_keys:
                return await interaction.send(
                    f"Cannot add {amount} keys. Only {available_keys} available for {user.display_name}.", ephemeral=True
                )
            db.add_claimed_keys(str(user.id), amount)
            await interaction.send(f"Added {amount} key(s) to {user.display_name}.", ephemeral=True)
        else:  # amount < 0
            if abs(amount) > claimed_keys:
                return await interaction.send(
                    f"Cannot deduct {abs(amount)} keys. {user.display_name} only has {claimed_keys} claimed.", ephemeral=True
                )
            db.add_claimed_keys(str(user.id), amount)  # amount is negative
            await interaction.send(f"Deducted {abs(amount)} key(s) from {user.display_name}.", ephemeral=True)

    @booster_group.subcommand(name="test_anniversary", description="Manually trigger anniversary check for a user (Testing).")
    @application_checks.has_permissions(manage_guild=True)
    async def test_anniversary(self, interaction: Interaction, user: Member = SlashOption(description="User to check.")):
        await interaction.response.defer(ephemeral=True)
        
        # 1. Fetch booster data from DB
        booster_data = db.get_booster(str(user.id))
        if not booster_data or not booster_data.get('is_currently_boosting'):
            return await interaction.send(f"{user.display_name} is not currently recorded as a booster.", ephemeral=True)

        # 2. Calculate months boosted based on the stored timestamp
        start_ts = booster_data.get('current_boost_start_timestamp')
        if not start_ts:
            return await interaction.send("No boost start timestamp found in database.", ephemeral=True)

        now = datetime.now(timezone.utc)
        boost_start = datetime.fromtimestamp(start_ts, tz=timezone.utc)
        
        # Calculate month difference
        months_boosted = (now.year - boost_start.year) * 12 + (now.month - boost_start.month)
        if now.day < boost_start.day:
            months_boosted -= 1

        if months_boosted < 1:
            return await interaction.send(f"{user.display_name} has only been boosting for less than a month ({months_boosted} months).", ephemeral=True)

        # 3. Retrieve configuration
        config = db.get_config(str(interaction.guild.id))
        template = config.get("anniversary_message_template", "{mention} has been boosting for {months} {month_label}!")
        month_label = "month" if months_boosted == 1 else "months"
        
        content = template.format(
            mention=user.mention,
            user=user.name,
            server=interaction.guild.name,
            months=months_boosted,
            month_label=month_label
        )

        # 4. Attempt to send via Webhook or Channel
        webhook_url = config.get("booster_announcement_webhook_url")
        success = False
        
        if webhook_url:
            async with aiohttp.ClientSession() as session:
                try:
                    webhook = Webhook.from_url(webhook_url, session=session)
                    await webhook.send(content)
                    success = True
                except Exception as e:
                    logger.error(f"Test Anniversary: Webhook failed: {e}")
        else:
            channel_id = config.get("announcement_channel_id")
            if channel_id and (channel := self.bot.get_channel(int(channel_id))):
                await channel.send(content)
                success = True

        if success:
            # We do NOT update the DB here so you can test it multiple times
            await interaction.send(f"Success! Sent {months_boosted}-month anniversary message for {user.display_name}.", ephemeral=True)
        else:
            await interaction.send("Failed to send message. Check if a channel or webhook is configured.", ephemeral=True)

    @booster_group.subcommand(name="sync_counts", description="Syncs boost counts based on months boosted.")
    @application_checks.has_permissions(manage_guild=True)
    async def sync_counts(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        active_boosters = [b for b in db.get_all_boosters_for_leaderboard() if b.get('is_currently_boosting')]
        updated_count = 0
        now = datetime.now(timezone.utc)

        for booster in active_boosters:
            start_ts = booster.get('current_boost_start_timestamp')
            if not start_ts: continue

            boost_start = datetime.fromtimestamp(start_ts, tz=timezone.utc)
            months_boosted = (now.year - boost_start.year) * 12 + (now.month - boost_start.month)
            if now.day < boost_start.day: months_boosted -= 1

            # Only update if the calculated months are higher than recorded count
            if months_boosted > booster.get('total_boost_count', 0):
                # Calculate how many counts they are missing
                diff = months_boosted - booster.get('total_boost_count', 0)
                db.increment_boost_count(booster['user_id'], diff)
                updated_count += 1

        await interaction.send(f"Synced counts for {updated_count} boosters based on their duration.", ephemeral=True)
        
    # --- CONFIG GROUP ---
    
    @booster_group.subcommand(name="config", description="Configuration commands for the booster tracker.")
    async def config_group(self, interaction: Interaction):
        pass

    @config_group.subcommand(name="set_key_rate", description="Set how many keys a user gets per month of boosting.")
    @application_checks.has_permissions(manage_guild=True)
    async def set_key_rate(self, interaction: Interaction, amount: int = SlashOption(description="Amount of keys per month", min_value=0)):
        # Update the config in the DB
        with db.get_db_connection() as conn:
            conn.cursor().execute(
                "UPDATE cog_config SET keys_per_month = ? WHERE guild_id = ?",
                (amount, str(interaction.guild.id))
            )
            conn.commit()
    
        await interaction.send(f"✅ Key exchange rate updated. Boosters will now receive **{amount}** keys per month.", ephemeral=True)

    @config_group.subcommand(name="channel", description="Sets the channel for all boost-related announcements.")
    @application_checks.has_permissions(manage_guild=True)
    async def set_channel(self, interaction: Interaction, channel: TextChannel):
        db.update_config(str(interaction.guild.id), {'announcement_channel_id': str(channel.id)})
        await interaction.send(f"Booster announcement channel set to {channel.mention}.", ephemeral=True)

    # ADDED: Command to set the webhook URL
    @config_group.subcommand(name="webhook", description="Sets the webhook URL for announcements (overrides channel).")
    @application_checks.has_permissions(manage_guild=True)
    async def set_webhook(self, interaction: Interaction, url: str):
        if not url.startswith("https://discord.com/api/webhooks/"):
            return await interaction.send("This does not look like a valid Discord webhook URL.", ephemeral=True)
        db.update_config(str(interaction.guild.id), {'booster_announcement_webhook_url': url})
        await interaction.send(f"Booster announcement webhook has been set.", ephemeral=True)

    @config_group.subcommand(name="message", description="Sets the custom message for new boosters or the monthly anniversary.")
    @application_checks.has_permissions(manage_guild=True)
    async def set_message(self, interaction: Interaction,
                          msg_type: str = SlashOption(name="type", choices=["welcome", "anniversary"]),
                          template: str = SlashOption(name="template")):
        # You can use placeholders: {mention}, {user}, {server}, and {months} for anniversary messages
        db.update_config(str(interaction.guild.id), {f'{msg_type}_message_template': template})
        await interaction.send(f"Booster {msg_type} message updated.", ephemeral=True)

    @config_group.subcommand(name="add_reward_role", description="Adds a new role reward for a duration milestone.")
    @application_checks.has_permissions(manage_guild=True)
    async def add_reward(self, interaction: Interaction, months: int, role: Role):
        # ... (This command remains unchanged) ...
        db.add_reward_role(months, str(role.id))
        await interaction.send(f"Role {role.mention} will be given for {months} months of continuous boosting.", ephemeral=True)

    @config_group.subcommand(name="remove_reward_role", description="Removes a role reward.")
    @application_checks.has_permissions(manage_guild=True)
    async def remove_reward(self, interaction: Interaction, role: Role):
        # ... (This command remains unchanged) ...
        db.remove_reward_role(str(role.id))
        await interaction.send(f"Role reward for {role.mention} has been removed.", ephemeral=True)

    # ADDED: Command to view all current configurations
    @config_group.subcommand(name="view", description="Displays the current configuration for the booster tracker.")
    @application_checks.has_permissions(manage_guild=True)
    async def view_config(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        config = db.get_config(str(interaction.guild.id))

        webhook_url = config.get("booster_announcement_webhook_url")
        webhook_status = "Set" if webhook_url else "Not Set"
        
        channel_id = config.get("announcement_channel_id")
        channel_status = f"<#{channel_id}>" if channel_id else "Not Set"
        
        welcome_msg = config.get("welcome_message_template", "Not Set")
        anniv_msg = config.get("anniversary_message_template", "Not Set")

        embed = Embed(title="Booster Tracker Configuration", color=NITRO_PINK, timestamp=datetime.now(timezone.utc))
        embed.add_field(name="Webhook URL", value=webhook_status, inline=False)
        embed.add_field(name="Announcement Channel (Fallback)", value=channel_status, inline=False)
        embed.add_field(name="Welcome Message", value=f"```{welcome_msg}```", inline=False)
        embed.add_field(name="Anniversary Message", value=f"```{anniv_msg}```", inline=False)
        
        await interaction.send(embed=embed)

    @nextcord.slash_command(name="test_boost_task", description="Manually run the booster check task (admin only, full process).")
    @application_checks.has_permissions(manage_guild=True)
    async def test_boost_task(self, interaction: Interaction):
        await self.bot.wait_until_ready()
        guild = self.bot.get_guild(self.target_guild_id)
        if not guild:
            await interaction.send("Target guild not found.", ephemeral=True)
            return

        now = datetime.now(timezone.utc)
        config = db.get_config(str(guild.id))
        reward_roles = db.get_all_reward_roles()
        if not reward_roles:
            await interaction.send("No reward roles configured.", ephemeral=True)
            return

        active_boosters = [b for b in db.get_all_boosters_for_leaderboard() if b.get('is_currently_boosting')]
        sent_announcements = 0
        assigned_roles = 0

        for booster_data in active_boosters:
            user_id = str(booster_data['user_id'])
            start_ts = booster_data.get('current_boost_start_timestamp')
            if not start_ts:
                continue

            boost_start = datetime.fromtimestamp(start_ts, tz=timezone.utc)
            months_boosted = (now.year - boost_start.year) * 12 + (now.month - boost_start.month)
            if now.day < boost_start.day:
                months_boosted -= 1

            member = guild.get_member(int(user_id))
            if not member:
                continue

            for reward in reward_roles:
                milestone = reward['duration_months']
                role = guild.get_role(int(reward['role_id']))
                if not role:
                    continue

                # Assign role if milestone reached and not already assigned
                if months_boosted >= milestone and role not in member.roles:
                    try:
                        await member.add_roles(role, reason=f"Reached {milestone} months of boosting.")
                        assigned_roles += 1
                    except Exception as e:
                        logger.error(f"Failed to assign role to {member.display_name}: {e}")

                # Send anniversary message if just hit the milestone this month
                # (You may want to track last notified milestone in your DB for production)
                if months_boosted >= milestone:
                    template = config.get("anniversary_message_template", "{mention} has been boosting for {months} {month_label}!")
                    month_label = "month" if months_boosted == 1 else "months"
                    content = template.format(
                        mention=member.mention,
                        user=member.name,
                        server=guild.name,
                        months=months_boosted,
                        month_label=month_label
                    )
                    webhook_url = config.get("booster_announcement_webhook_url")
                    if webhook_url:
                        logger.info(f"Attempting to send anniversary message via webhook: {webhook_url}")
                        async with aiohttp.ClientSession() as session:
                            try:
                                webhook = Webhook.from_url(webhook_url, session=session)
                                await webhook.send(content)
                                sent_announcements += 1
                                logger.info("Anniversary message sent via webhook.")
                            except Exception as e:
                                logger.error(f"Failed to send anniversary webhook: {e}")

def setup(bot):
    bot.add_cog(BoostTrackerCog(bot))