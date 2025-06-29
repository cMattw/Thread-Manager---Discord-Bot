import nextcord
from nextcord.ext import commands
from nextcord import SlashOption
import logging
from typing import List, Optional, Dict, Any
from db_utils.auto_reaction_database import (
    initialize_database, get_config, update_config, 
    add_reaction_set, remove_reaction_set, get_reaction_sets, 
    add_target_channel, remove_target_channel, get_target_channels,
    add_channel_exception, remove_channel_exception, get_channel_exceptions
)

class AutoReactionCog(commands.Cog):
    """Cog for automatically adding reactions to messages in configured channels."""
    
    def __init__(self, bot):
        self.bot = bot
        
    async def cog_application_command_before_invoke(self, interaction: nextcord.Interaction):
        """Ensure database is initialized before any command."""
        initialize_database(interaction.guild_id)

    @nextcord.slash_command(
        name="autoreact",
        description="Manage automatic reactions for messages"
    )
    async def autoreact(self, interaction: nextcord.Interaction):
        """Base command for auto-reaction management."""
        pass

    @autoreact.subcommand(
        name="toggle",
        description="Enable or disable auto-reactions for this server"
    )
    async def toggle_autoreact(
        self,
        interaction: nextcord.Interaction,
        enabled: bool = SlashOption(description="Enable or disable auto-reactions")
    ):
        """Toggle auto-reactions on or off."""
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("❌ You need 'Manage Server' permission to use this command.", ephemeral=True)
            return

        success = update_config(interaction.guild_id, {"enabled": enabled})
        
        if success:
            status = "enabled" if enabled else "disabled"
            await interaction.response.send_message(f"✅ Auto-reactions have been **{status}** for this server.")
        else:
            await interaction.response.send_message("❌ Failed to update auto-reaction settings.", ephemeral=True)

    @autoreact.subcommand(
        name="add_reactions",
        description="Add a set of reactions to be automatically applied"
    )
    async def add_reactions(
        self,
        interaction: nextcord.Interaction,
        name: str = SlashOption(description="Name for this reaction set"),
        reactions: str = SlashOption(description="Reactions separated by spaces (e.g., 👍 👎 ❤️)")
    ):
        """Add a new reaction set."""
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("❌ You need 'Manage Server' permission to use this command.", ephemeral=True)
            return

        # Parse reactions
        reaction_list = reactions.split()
        if not reaction_list:
            await interaction.response.send_message("❌ Please provide at least one reaction.", ephemeral=True)
            return

        # Validate reactions
        valid_reactions = []
        for reaction in reaction_list:
            # Check if it's a custom emoji (format: <:name:id> or <a:name:id>)
            if reaction.startswith('<') and reaction.endswith('>'):
                valid_reactions.append(reaction)
            # Check if it's a unicode emoji
            else:
                try:
                    # Try to add it as a reaction to validate
                    valid_reactions.append(reaction)
                except:
                    await interaction.response.send_message(f"❌ Invalid reaction: {reaction}", ephemeral=True)
                    return

        success = add_reaction_set(interaction.guild_id, name, valid_reactions)
        
        if success:
            reactions_str = " ".join(valid_reactions)
            await interaction.response.send_message(f"✅ Added reaction set **{name}**: {reactions_str}")
        else:
            await interaction.response.send_message("❌ Failed to add reaction set. A set with this name might already exist.", ephemeral=True)

    @autoreact.subcommand(
        name="remove_reactions",
        description="Remove a reaction set"
    )
    async def remove_reactions(
        self,
        interaction: nextcord.Interaction,
        name: str = SlashOption(description="Name of the reaction set to remove")
    ):
        """Remove a reaction set."""
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("❌ You need 'Manage Server' permission to use this command.", ephemeral=True)
            return

        success = remove_reaction_set(interaction.guild_id, name)
        
        if success:
            await interaction.response.send_message(f"✅ Removed reaction set **{name}**.")
        else:
            await interaction.response.send_message(f"❌ Reaction set **{name}** not found.", ephemeral=True)

    @autoreact.subcommand(
        name="list_reactions",
        description="List all configured reaction sets"
    )
    async def list_reactions(self, interaction: nextcord.Interaction):
        """List all reaction sets."""
        reaction_sets = get_reaction_sets(interaction.guild_id)
        
        if not reaction_sets:
            await interaction.response.send_message("📝 No reaction sets configured.")
            return

        embed = nextcord.Embed(
            title="🎭 Auto-Reaction Sets",
            color=0x00ff00
        )
        
        for name, reactions in reaction_sets.items():
            reactions_str = " ".join(reactions)
            embed.add_field(
                name=name,
                value=reactions_str,
                inline=False
            )

        await interaction.response.send_message(embed=embed)

    @autoreact.subcommand(
        name="set_mode",
        description="Set reaction mode (all messages, forum posts only, threads only, etc.)"
    )
    async def set_mode(
        self,
        interaction: nextcord.Interaction,
        mode: str = SlashOption(
            description="Reaction mode",
            choices=[
                "all", "forum_posts", "threads", "regular_channels", 
                "forum_and_threads", "exclude_threads"
            ]
        )
    ):
        """Set the reaction mode."""
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("❌ You need 'Manage Server' permission to use this command.", ephemeral=True)
            return

        mode_descriptions = {
            "all": "All messages in all channels",
            "forum_posts": "Only initial forum posts",
            "threads": "Only messages in threads",
            "regular_channels": "Only regular text channels (no threads/forums)",
            "forum_and_threads": "Forum posts and thread messages",
            "exclude_threads": "All channels except threads"
        }

        success = update_config(interaction.guild_id, {"reaction_mode": mode})
        
        if success:
            description = mode_descriptions.get(mode, mode)
            await interaction.response.send_message(f"✅ Reaction mode set to: **{description}**")
        else:
            await interaction.response.send_message("❌ Failed to update reaction mode.", ephemeral=True)

    @autoreact.subcommand(
        name="add_target",
        description="Add a specific channel, forum post, or thread to receive auto-reactions"
    )
    async def add_target(
        self,
        interaction: nextcord.Interaction,
        channel: nextcord.abc.GuildChannel = SlashOption(description="Channel, forum post, or thread to target")
    ):
        """Add a specific target for auto-reactions."""
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("❌ You need 'Manage Server' permission to use this command.", ephemeral=True)
            return

        # Determine the type of channel
        channel_type = "channel"
        if isinstance(channel, nextcord.Thread):
            if isinstance(channel.parent, nextcord.ForumChannel):
                channel_type = "forum_post"
            else:
                channel_type = "thread"
        elif isinstance(channel, nextcord.ForumChannel):
            channel_type = "forum_channel"

        success = add_target_channel(interaction.guild_id, channel.id, channel_type)
        
        if success:
            await interaction.response.send_message(f"✅ Added {channel.mention} as an auto-reaction target ({channel_type}).")
        else:
            await interaction.response.send_message(f"❌ {channel.mention} is already in the targets list.", ephemeral=True)

    @autoreact.subcommand(
        name="remove_target",
        description="Remove a specific target from auto-reactions"
    )
    async def remove_target(
        self,
        interaction: nextcord.Interaction,
        channel: nextcord.abc.GuildChannel = SlashOption(description="Channel, forum post, or thread to remove from targets")
    ):
        """Remove a target from auto-reactions."""
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("❌ You need 'Manage Server' permission to use this command.", ephemeral=True)
            return

        success = remove_target_channel(interaction.guild_id, channel.id)
        
        if success:
            await interaction.response.send_message(f"✅ Removed {channel.mention} from auto-reaction targets.")
        else:
            await interaction.response.send_message(f"❌ {channel.mention} was not in the targets list.", ephemeral=True)

    @autoreact.subcommand(
        name="list_targets",
        description="List all specific channels/threads targeted for auto-reactions"
    )
    async def list_targets(self, interaction: nextcord.Interaction):
        """List all targeted channels."""
        targets = get_target_channels(interaction.guild_id)
        
        if not targets:
            await interaction.response.send_message("📝 No specific targets configured. Using global mode settings.")
            return

        embed = nextcord.Embed(
            title="🎯 Auto-Reaction Targets",
            color=0x00ff00
        )
        
        # Group targets by type
        grouped_targets = {}
        for channel_id, channel_type in targets:
            if channel_type not in grouped_targets:
                grouped_targets[channel_type] = []
            
            channel = interaction.guild.get_channel(channel_id)
            if channel:
                grouped_targets[channel_type].append(channel.mention)
            else:
                grouped_targets[channel_type].append(f"Unknown Channel ({channel_id})")

        type_names = {
            "channel": "📝 Text Channels",
            "thread": "🧵 Threads", 
            "forum_post": "📋 Forum Posts",
            "forum_channel": "🗂️ Forum Channels"
        }

        for channel_type, channels in grouped_targets.items():
            embed.add_field(
                name=type_names.get(channel_type, channel_type.title()),
                value="\n".join(channels)[:1024],
                inline=False
            )

        await interaction.response.send_message(embed=embed)

    @autoreact.subcommand(
        name="add_exception",
        description="Add a channel where auto-reactions should NOT be applied"
    )
    async def add_exception(
        self,
        interaction: nextcord.Interaction,
        channel: nextcord.abc.GuildChannel = SlashOption(description="Channel to exclude from auto-reactions")
    ):
        """Add a channel exception."""
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("❌ You need 'Manage Server' permission to use this command.", ephemeral=True)
            return

        success = add_channel_exception(interaction.guild_id, channel.id)
        
        if success:
            await interaction.response.send_message(f"✅ Added {channel.mention} to auto-reaction exceptions.")
        else:
            await interaction.response.send_message(f"❌ {channel.mention} is already in the exceptions list.", ephemeral=True)

    @autoreact.subcommand(
        name="remove_exception",
        description="Remove a channel from the exceptions list"
    )
    async def remove_exception(
        self,
        interaction: nextcord.Interaction,
        channel: nextcord.abc.GuildChannel = SlashOption(description="Channel to remove from exceptions")
    ):
        """Remove a channel exception."""
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("❌ You need 'Manage Server' permission to use this command.", ephemeral=True)
            return

        success = remove_channel_exception(interaction.guild_id, channel.id)
        
        if success:
            await interaction.response.send_message(f"✅ Removed {channel.mention} from auto-reaction exceptions.")
        else:
            await interaction.response.send_message(f"❌ {channel.mention} was not in the exceptions list.", ephemeral=True)

    @autoreact.subcommand(
        name="list_exceptions",
        description="List all channels excluded from auto-reactions"
    )
    async def list_exceptions(self, interaction: nextcord.Interaction):
        """List channel exceptions."""
        exceptions = get_channel_exceptions(interaction.guild_id)
        
        if not exceptions:
            await interaction.response.send_message("📝 No channel exceptions configured.")
            return

        embed = nextcord.Embed(
            title="🚫 Auto-Reaction Channel Exceptions",
            color=0xff9900
        )
        
        exception_mentions = []
        for channel_id in exceptions:
            channel = interaction.guild.get_channel(channel_id)
            if channel:
                exception_mentions.append(channel.mention)
            else:
                exception_mentions.append(f"Unknown Channel ({channel_id})")

        embed.description = "\n".join(exception_mentions)
        await interaction.response.send_message(embed=embed)

    @autoreact.subcommand(
        name="settings",
        description="View current auto-reaction settings"
    )
    async def view_settings(self, interaction: nextcord.Interaction):
        """View current settings."""
        config = get_config(interaction.guild_id)
        
        if not config:
            await interaction.response.send_message("❌ No configuration found.", ephemeral=True)
            return

        embed = nextcord.Embed(
            title="⚙️ Auto-Reaction Settings",
            color=0x0099ff
        )
        
        # Basic settings
        embed.add_field(
            name="Status",
            value="✅ Enabled" if config.get('enabled') else "❌ Disabled",
            inline=True
        )
        
        mode_descriptions = {
            "all": "All messages",
            "forum_posts": "Forum posts only",
            "threads": "Threads only",
            "regular_channels": "Regular channels only",
            "forum_and_threads": "Forums and threads",
            "exclude_threads": "All except threads"
        }
        
        mode = config.get('reaction_mode', 'all')
        embed.add_field(
            name="Reaction Mode",
            value=mode_descriptions.get(mode, mode),
            inline=True
        )
        
        # Reaction sets
        reaction_sets = get_reaction_sets(interaction.guild_id)
        if reaction_sets:
            sets_text = "\n".join([f"**{name}**: {' '.join(reactions)}" for name, reactions in reaction_sets.items()])
            embed.add_field(
                name="Reaction Sets",
                value=sets_text[:1024],  # Discord field limit
                inline=False
            )
        else:
            embed.add_field(
                name="Reaction Sets",
                value="None configured",
                inline=False
            )

        # Targets
        targets = get_target_channels(interaction.guild_id)
        if targets:
            target_mentions = []
            for channel_id, channel_type in targets:
                channel = interaction.guild.get_channel(channel_id)
                if channel:
                    target_mentions.append(f"{channel.mention} ({channel_type})")
                else:
                    target_mentions.append(f"Unknown Channel ({channel_id}) ({channel_type})")
            
            targets_text = "\n".join(target_mentions)
            embed.add_field(
                name="Specific Targets",
                value=targets_text[:1024],
                inline=False
            )
        else:
            embed.add_field(
                name="Specific Targets",
                value="None - using global mode",
                inline=False
            )

        await interaction.response.send_message(embed=embed)

    @commands.Cog.listener()
    async def on_message(self, message: nextcord.Message):
        """Handle new messages and add reactions if configured."""
        # Ignore bot messages
        if message.author.bot:
            return
            
        # Ignore messages without a guild
        if not message.guild:
            return

        # Get configuration
        config = get_config(message.guild.id)
        
        # Check if auto-reactions are enabled
        if not config or not config.get('enabled', False):
            return

        # Check if channel is in exceptions
        exceptions = get_channel_exceptions(message.guild.id)
        if message.channel.id in exceptions:
            return

        # Check reaction mode
        mode = config.get('reaction_mode', 'all')
        should_react = self._should_react_based_on_mode(message, mode)
        
        if not should_react:
            return

        # Get reaction sets
        reaction_sets = get_reaction_sets(message.guild.id)
        if not reaction_sets:
            return

        # Check if we have specific targets configured
        targets = get_target_channels(message.guild.id)
        
        if targets:
            # If we have specific targets, only react in those channels
            target_ids = [target_id for target_id, _ in targets]
            
            # Check if current channel is in targets
            is_target = message.channel.id in target_ids
            
            # For forum channels, also check if the parent is a target
            if not is_target and isinstance(message.channel, nextcord.Thread):
                if message.channel.parent and message.channel.parent.id in target_ids:
                    is_target = True
            
            if not is_target:
                return

        # Apply all reactions from all sets
        for reactions in reaction_sets.values():
            for reaction in reactions:
                try:
                    await message.add_reaction(reaction)
                except nextcord.HTTPException as e:
                    logging.warning(f"Failed to add reaction {reaction} to message {message.id}: {e}")
                except Exception as e:
                    logging.error(f"Unexpected error adding reaction {reaction}: {e}")

    def _should_react_based_on_mode(self, message: nextcord.Message, mode: str) -> bool:
        """Determine if we should react based on the configured mode."""
        is_thread = isinstance(message.channel, nextcord.Thread)
        is_forum_post = (isinstance(message.channel, nextcord.Thread) and 
                        isinstance(message.channel.parent, nextcord.ForumChannel) and
                        message.channel.owner_id == message.author.id and
                        message.id == message.channel.id)
        is_regular_channel = not is_thread

        mode_rules = {
            "all": True,
            "forum_posts": is_forum_post,
            "threads": is_thread,
            "regular_channels": is_regular_channel,
            "forum_and_threads": is_thread,
            "exclude_threads": not is_thread
        }

        return mode_rules.get(mode, True)

def setup(bot):
    bot.add_cog(AutoReactionCog(bot))