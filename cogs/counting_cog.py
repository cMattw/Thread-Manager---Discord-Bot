import nextcord
from nextcord.ext import commands
import logging
import re
from db_utils.counting_database import (
    get_counting_channel,
    set_counting_channel,
    add_exempted_role,
    remove_exempted_role,
    get_exempted_roles,
)


class CountingCog(commands.Cog):
    """Cog for managing a counting channel."""

    def __init__(self, bot):
        self.bot = bot

    @nextcord.slash_command(
        name="counting",
        description="Manage the counting channel"
    )
    async def counting(self, interaction: nextcord.Interaction):
        """Base command for counting channel management."""
        pass

    @counting.subcommand(
        name="set_channel",
        description="Set the counting channel"
    )
    async def set_counting_channel(
        self,
        interaction: nextcord.Interaction,
        channel: nextcord.abc.GuildChannel
    ):
        """Set which channel is used for counting."""
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("❌ You need 'Manage Server' permission to use this command.", ephemeral=True)
            return

        if not isinstance(channel, nextcord.TextChannel):
            await interaction.response.send_message("❌ The channel must be a text channel.", ephemeral=True)
            return

        success = set_counting_channel(interaction.guild_id, channel.id)

        if success:
            await interaction.response.send_message(f"✅ Counting channel set to {channel.mention}")
        else:
            await interaction.response.send_message("❌ Failed to set counting channel.", ephemeral=True)

    @counting.subcommand(
        name="add_exempted_role",
        description="Add a role that is exempt from counting channel rules"
    )
    async def add_exempted_role_cmd(
        self,
        interaction: nextcord.Interaction,
        role: nextcord.Role
    ):
        """Add a role that will be exempt from message deletion."""
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("❌ You need 'Manage Server' permission to use this command.", ephemeral=True)
            return

        success = add_exempted_role(interaction.guild_id, role.id)

        if success:
            await interaction.response.send_message(f"✅ Role {role.mention} is now exempt from counting rules")
        else:
            await interaction.response.send_message(f"⚠️ Role {role.mention} was already exempt.", ephemeral=True)

    @counting.subcommand(
        name="remove_exempted_role",
        description="Remove a role from the exemption list"
    )
    async def remove_exempted_role_cmd(
        self,
        interaction: nextcord.Interaction,
        role: nextcord.Role
    ):
        """Remove a role from the exemption list."""
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("❌ You need 'Manage Server' permission to use this command.", ephemeral=True)
            return

        success = remove_exempted_role(interaction.guild_id, role.id)

        if success:
            await interaction.response.send_message(f"✅ Role {role.mention} is no longer exempt")
        else:
            await interaction.response.send_message(f"❌ Role {role.mention} was not in the exemption list.", ephemeral=True)

    @commands.Cog.listener()
    async def on_message(self, message: nextcord.Message):
        """Delete messages with non-numerical characters in the counting channel."""
        # Ignore bot messages
        if message.author.bot:
            return

        # Ignore messages without a guild
        if not message.guild:
            return

        # Get the counting channel for this guild
        counting_channel_id = get_counting_channel(message.guild.id)

        # If no counting channel is set, do nothing
        if not counting_channel_id:
            return

        # If this message is not in the counting channel, do nothing
        if message.channel.id != counting_channel_id:
            return

        # Check if the user has an exempted role
        exempted_roles = get_exempted_roles(message.guild.id)
        user_role_ids = [role.id for role in message.author.roles]

        if any(role_id in exempted_roles for role_id in user_role_ids):
            return

        # Check if the message contains only numerical characters (ignore whitespace)
        if not re.match(r'^\d+$', message.content.strip()):
            try:
                await message.delete()
            except nextcord.HTTPException as e:
                logging.warning(f"Failed to delete message {message.id}: {e}")


def setup(bot):
    bot.add_cog(CountingCog(bot))