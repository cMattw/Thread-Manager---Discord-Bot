import nextcord
from nextcord.ext import commands, tasks, application_checks
from nextcord import Interaction, SlashOption, Embed, Color, Member, Role, TextChannel
import logging
from datetime import datetime, timezone

from db_utils import booster_database as db

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
        self.initial_scan_done = False

    def cog_unload(self):
        self.check_boosters_task.cancel()

    @commands.Cog.listener()
    async def on_ready(self):
        if self.initial_scan_done: return
        await self.bot.wait_until_ready()
        guild = self.bot.get_guild(self.target_guild_id)
        if not guild:
            logging.error(f"Initial booster scan: Target guild {self.target_guild_id} not found.")
            return
        
        logging.info("Performing initial scan for existing boosters...")
        async for member in guild.fetch_members(limit=None):
            if member.premium_since is not None:
                booster_data = db.get_booster(str(member.id))
                if not booster_data or not booster_data.get('is_currently_boosting'):
                    db.start_new_boost(str(member.id), str(guild.id), int(member.premium_since.timestamp()))
        self.initial_scan_done = True
        logging.info("Initial booster scan complete. Running first monthly count update.")
        await self.check_boosters_task.coro(self)

    @commands.Cog.listener()
    async def on_member_update(self, before: Member, after: Member):
        if before.premium_since == after.premium_since or after.guild.id != self.target_guild_id: return
        now_ts = int(datetime.now(timezone.utc).timestamp())
        if before.premium_since is None and after.premium_since is not None:
            db.start_new_boost(str(after.id), str(after.guild.id), int(after.premium_since.timestamp()))
        elif before.premium_since is not None and after.premium_since is None:
            db.end_boost(str(after.id), now_ts)

    @tasks.loop(hours=24)
    async def check_boosters_task(self):
        await self.bot.wait_until_ready()
        guild = self.bot.get_guild(self.target_guild_id)
        if not guild: return

        logging.info("Running daily check for monthly booster count updates...")
        active_boosters = [b for b in db.get_all_boosters_for_leaderboard() if b.get('is_currently_boosting')]
        now = datetime.now(timezone.utc)

        for booster_data in active_boosters:
            user_id = str(booster_data['user_id'])
            start_ts = booster_data.get('current_boost_start_timestamp')
            if not start_ts: continue

            start_time = datetime.fromtimestamp(start_ts, tz=timezone.utc)
            months_in_streak = int((now - start_time).days / 30.44)
            last_notified_month = booster_data.get('last_anniversary_notified', 0)

            if months_in_streak > last_notified_month:
                new_months_passed = months_in_streak - last_notified_month
                if new_months_passed > 0:
                    logging.info(f"User {user_id} crossed {new_months_passed} new month thresholds. Incrementing boost count.")
                    db.increment_boost_count(user_id, new_months_passed)
                
                db.update_anniversary_notified(user_id, months_in_streak)

    @nextcord.slash_command(name="booster", description="Commands for managing server boosters.")
    async def booster_group(self, interaction: Interaction):
        pass

    @booster_group.subcommand(name="list", description="Displays the booster leaderboard.")
    async def list_boosters(self, interaction: Interaction,
        sort_by: str = SlashOption(
            name="sort_by",
            description="Choose how to rank the boosters.",
            choices={
                "Current Streak": "streak", 
                "Total Boost Count": "count", 
                "Total Duration": "duration"
            },
            default="count" # Default to the new hybrid count
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
                if start_ts:
                    display_str = f"Boosting since: <t:{start_ts}:D>"
                else:
                    display_str = "Streak: `N/A`"
            elif sort_by == 'count':
                display_str = f"Boost Count: `{booster_data.get('total_boost_count', 0)}`"
            else: # duration
                total_days = get_true_total_duration(booster_data)
                display_str = f"Total duration: `{format_duration(total_days)}`"
            
            description += f"**{i}.** {user.mention} - {display_str}\n"

        if not description:
            description = "No boosters to display for this category."
        embed.description = description
        await interaction.send(embed=embed)

    @booster_group.subcommand(name="history", description="View the boost history of a specific user.")
    @application_checks.has_permissions(manage_guild=True)
    async def history(self, interaction: Interaction, user: Member = SlashOption(description="The user to check.")):
        booster_stats = db.get_booster(str(user.id))
        if not booster_stats:
            return await interaction.send(f"{user.display_name} has no boosting history.", ephemeral=False)

        embed = Embed(title=f"Boost History for {user.display_name}", color=NITRO_PINK)
        embed.set_thumbnail(url=user.display_avatar.url)

        total_boosts = booster_stats.get('total_boost_count', 0)
        first_boost_ts = booster_stats.get('first_boost_timestamp')

        now = datetime.now(timezone.utc)
        total_days = booster_stats.get('total_duration_days', 0)
        if booster_stats.get('is_currently_boosting') and booster_stats.get('current_boost_start_timestamp'):
            current_start = datetime.fromtimestamp(booster_stats.get('current_boost_start_timestamp'), tz=timezone.utc)
            total_days += (now - current_start).days
        
        desc_parts = [
            f"**Total Boost Count:** `{total_boosts}`",
            f"**Total Time Boosted:** `{format_duration(total_days)}`"
        ]
        if first_boost_ts:
            desc_parts.append(f"**First Boosted On:** <t:{first_boost_ts}:D>")
        
        embed.description = "\n".join(desc_parts)
        await interaction.send(embed=embed, ephemeral=False)

    @booster_group.subcommand(name="config", description="Configuration commands for the booster tracker.")
    @application_checks.has_permissions(manage_guild=True)
    async def config_group(self, interaction: Interaction):
        pass

    @config_group.subcommand(name="channel", description="Sets the channel for all boost-related announcements.")
    async def set_channel(self, interaction: Interaction, channel: TextChannel):
        db.update_config(str(interaction.guild_id), {'announcement_channel_id': str(channel.id)})
        await interaction.send(f"Booster announcement channel set to {channel.mention}.", ephemeral=True)

    @config_group.subcommand(name="message", description="Sets the custom message for new boosters or the monthly anniversary.")
    async def set_message(self, interaction: Interaction,
                          msg_type: str = SlashOption(name="type", choices=["welcome", "anniversary"]),
                          template: str = SlashOption(name="template")):
        db.update_config(str(interaction.guild_id), {f'{msg_type}_message_template': template})
        await interaction.send(f"Booster {msg_type} message updated.", ephemeral=True)

    @config_group.subcommand(name="add_reward_role", description="Adds a new role reward for a duration milestone.")
    async def add_reward(self, interaction: Interaction, months: int, role: Role):
        db.add_reward_role(months, str(role.id))
        await interaction.send(f"Role {role.mention} will be given for {months} months of continuous boosting.", ephemeral=True)

    @config_group.subcommand(name="remove_reward_role", description="Removes a role reward.")
    async def remove_reward(self, interaction: Interaction, role: Role):
        db.remove_reward_role(str(role.id))
        await interaction.send(f"Role reward for {role.mention} has been removed.", ephemeral=True)

def setup(bot: commands.Bot):
    bot.add_cog(BoostTrackerCog(bot))