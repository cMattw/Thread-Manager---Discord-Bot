# cogs/suggestions_cog.py

import nextcord
from nextcord.ext import commands, application_checks
from nextcord import Interaction, SlashOption, Embed, Color, ForumChannel, Webhook, ui, TextInputStyle, Thread, ForumTag
import logging
from typing import Optional, List, Dict
import asyncio

# Import the database utility
from db_utils import suggestions_database as db

# --- UI Components (Modal, Views) ---
class SuggestionModal(ui.Modal):
    def __init__(self, bot: commands.Bot, config: Dict):
        super().__init__("Submit a Suggestion", timeout=600)
        self.bot = bot
        self.config = config

        self.suggestion_title = ui.TextInput(label="Suggestion Title", style=TextInputStyle.short, placeholder="Enter a concise title for your suggestion", min_length=config.get('title_min_length', 10), max_length=min(config.get('title_max_length', 45), 45), required=True)
        self.add_item(self.suggestion_title)

        self.suggestion_desc = ui.TextInput(label="Suggestion Description", style=TextInputStyle.paragraph, placeholder="Describe your suggestion in detail.", min_length=config.get('description_min_length', 50), max_length=min(config.get('description_max_length', 4000), 4000), required=True)
        self.add_item(self.suggestion_desc)

    async def callback(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        view = SuggestionView(bot=self.bot, original_interaction=interaction, config=self.config, title=self.suggestion_title.value, description=self.suggestion_desc.value)
        await view.send_initial_message()

class PreSuggestionView(ui.View):
    def __init__(self, bot: commands.Bot, config: Dict):
        super().__init__(timeout=300)
        self.bot = bot
        self.config = config

    @ui.button(label="Understood", style=nextcord.ButtonStyle.primary, custom_id="pre_suggest_ack")
    async def confirm_button(self, button: ui.Button, interaction: Interaction):
        modal = SuggestionModal(self.bot, self.config)
        await interaction.response.send_modal(modal)
        self.clear_items()
        await interaction.edit_original_message(view=self)

class SuggestionView(ui.View):
    def __init__(self, bot: commands.Bot, original_interaction: Interaction, config: Dict, title: str, description: str):
        super().__init__(timeout=600)
        self.bot = bot
        self.original_interaction = original_interaction
        self.config = config
        self.title = title
        self.description = description
        self.selected_tags: List[nextcord.ForumTag] = []

    async def send_initial_message(self):
        forum_channel_id = self.config.get('forum_channel_id')
        forum_channel: Optional[ForumChannel] = self.bot.get_channel(int(forum_channel_id)) if forum_channel_id else None
        if not forum_channel:
            await self.original_interaction.followup.send("Configuration error: Forum channel not found.", ephemeral=True); return

        user_selectable_tags = [tag for tag in forum_channel.available_tags if not tag.moderated]

        if user_selectable_tags:
            tag_options = [nextcord.SelectOption(label=tag.name, value=str(tag.id)) for tag in user_selectable_tags]
            max_selectable = min(len(tag_options), 5)
            tag_select = ui.Select(placeholder="Select relevant tags", options=tag_options, min_values=0, max_values=max_selectable, custom_id="suggestion_tag_select")
            tag_select.callback = self.on_tag_select
            self.add_item(tag_select)
            await self.original_interaction.followup.send("Please select the relevant tags for your suggestion.", view=self, ephemeral=True)
        else:
            self.prepare_anonymity_buttons(self.original_interaction.user)
            await self.original_interaction.followup.send("Submitting with your name helps us give you credit! However, you may choose to submit anonymously.", view=self, ephemeral=True)

    def prepare_anonymity_buttons(self, user: nextcord.User):
        self.clear_items()
        post_as_self_button = ui.Button(label=f"Post as {user.display_name}", style=nextcord.ButtonStyle.green, custom_id="post_as_self")
        post_as_self_button.callback = self.on_anonymity_choice
        self.add_item(post_as_self_button)
        post_anonymously_button = ui.Button(label="Post Anonymously", style=nextcord.ButtonStyle.grey, custom_id="post_anonymously")
        post_anonymously_button.callback = self.on_anonymity_choice
        self.add_item(post_anonymously_button)

    async def on_tag_select(self, interaction: Interaction):
        selected_tag_ids = interaction.data.get('values', [])
        forum_channel: ForumChannel = self.bot.get_channel(int(self.config['forum_channel_id']))
        self.selected_tags = [tag for tag in forum_channel.available_tags if str(tag.id) in selected_tag_ids]
        self.prepare_anonymity_buttons(interaction.user)
        await interaction.response.edit_message(content="Submitting with your name helps us give you credit! However, you may choose to submit anonymously.", view=self)

    async def on_anonymity_choice(self, interaction: Interaction):
        is_anonymous = (interaction.data['custom_id'] == "post_anonymously")
        await interaction.response.edit_message(content="Processing your suggestion... Please wait.", view=None)
        forum_channel: Optional[ForumChannel] = self.bot.get_channel(int(self.config['forum_channel_id']))
        if not forum_channel:
            await self.original_interaction.followup.send("Error: Suggestion channel not found.", ephemeral=True); return
        
        bot_perms = forum_channel.permissions_for(interaction.guild.me)
        if not all([bot_perms.manage_webhooks, bot_perms.manage_threads, bot_perms.create_public_threads]):
            await self.original_interaction.followup.send("Error: I'm missing permissions (`Manage Webhooks`, `Manage Threads`, `Create Public Threads`).", ephemeral=True); return

        webhook: Optional[Webhook] = None
        try:
            webhook = await forum_channel.create_webhook(name="Suggestion Poster")
            webhook_params = {"thread_name": self.title, "content": self.description, "wait": True}
            if not is_anonymous:
                webhook_params["username"] = interaction.user.display_name
                webhook_params["avatar_url"] = interaction.user.display_avatar.url if interaction.user.display_avatar else None
            else:
                webhook_params["username"] = "Anonymous Suggestion"

            thread_message = await webhook.send(**webhook_params)
            new_thread = thread_message.thread
            
            if new_thread is None:
                logging.error(f"Failed to create thread for suggestion in guild {interaction.guild.id}.")
                await self.original_interaction.followup.send("❌ **Error:** Failed to create a thread. Please check my permissions.", ephemeral=True); return

            if not is_anonymous:
                db.add_suggestion(new_thread.id, interaction.user.id, interaction.guild.id)

            if self.selected_tags:
                await new_thread.edit(applied_tags=self.selected_tags)

            await self.original_interaction.followup.send(f"✅ Your suggestion has been posted! View it here: {new_thread.jump_url}", ephemeral=True)
        except Exception as e:
            logging.error(f"Error posting suggestion for user {interaction.user.id}: {e}", exc_info=True)
            await self.original_interaction.followup.send(f"An unexpected error occurred: {e}", ephemeral=True)
        finally:
            if webhook: await webhook.delete()

    async def on_timeout(self):
        try: await self.original_interaction.edit_original_message(content="Suggestion submission timed out.", view=None)
        except nextcord.NotFound: pass


# --- Cog Definition ---
class SuggestionsCog(commands.Cog, name="Suggestions"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        logging.info("SuggestionsCog loaded.")

    @commands.Cog.listener()
    async def on_guild_join(self, guild: nextcord.Guild):
        db.initialize_database(guild.id)
        logging.info(f"Initialized suggestions database for new guild: {guild.name} ({guild.id})")

    # === THE FIX IS HERE ===
    # The user-facing command is now a top-level /suggest command.
    @nextcord.slash_command(name="suggest", description="Submit a suggestion for Katipunan SMP.")
    async def suggest(self, interaction: Interaction):
        config = db.get_config(interaction.guild.id)
        if not config or not config.get('forum_channel_id'):
            await interaction.response.send_message("The suggestion system is not configured yet.", ephemeral=True); return
        if config.get('pre_modal_message'):
            await interaction.response.send_message(config['pre_modal_message'], view=PreSuggestionView(self.bot, config), ephemeral=True)
        else:
            await interaction.response.send_modal(SuggestionModal(self.bot, config))

    # --- Admin Command Group ---
    # The admin commands are now under the /suggestion group.
    @nextcord.slash_command(name="suggestion", description="Manage a suggestion.")
    async def suggestion_group(self, interaction: Interaction):
        pass

    @suggestion_group.subcommand(name="update", description="Approve, deny, or mark a suggestion as planned.")
    # @application_checks.has_permissions(manage_threads=True)
    async def update_suggestion(self, interaction: Interaction,
        status: str = SlashOption(name="status", description="The new status for the suggestion.", choices=["Planned", "Implemented", "Denied"], required=True),
        reason: str = SlashOption(name="reason", description="An optional reason for this status change.", required=False)):
        
        if not isinstance(interaction.channel, Thread):
            await interaction.response.send_message("This command can only be used inside a suggestion thread.", ephemeral=True); return

        config = db.get_config(interaction.guild.id)
        if not config or str(interaction.channel.parent_id) != config.get('forum_channel_id'):
            await interaction.response.send_message("This command can only be used in the configured suggestions forum.", ephemeral=True); return
        
        await interaction.response.defer(ephemeral=True)

        status_key = status.lower()
        tag_id = config.get(f"{status_key}_tag_id")
        message_template = config.get(f"{status_key}_message")

        if not tag_id:
            await interaction.followup.send(f"The tag for the '{status}' status has not been configured. Use `/suggestion config set_tag`.", ephemeral=True); return
        
        target_tag: Optional[ForumTag] = interaction.channel.parent.get_tag(int(tag_id))
        if not target_tag:
            await interaction.followup.send(f"The configured tag for '{status}' could not be found. It may have been deleted.", ephemeral=True); return

        current_tags = interaction.channel.applied_tags
        status_tag_ids = [
            config.get('planned_tag_id'), config.get('implemented_tag_id'), config.get('denied_tag_id')
        ]
        new_tags = [tag for tag in current_tags if str(tag.id) not in status_tag_ids]
        new_tags.append(target_tag)
        
        status_colors = { "Planned": Color.gold(), "Implemented": Color.green(), "Denied": Color.red() }
        embed_color = status_colors.get(status, Color.blurple())
        
        final_message = message_template.replace('{user}', interaction.user.mention)
        embed = Embed(title=f"Suggestion {status}", color=embed_color, description=final_message)
        if reason:
            embed.add_field(name="Reason", value=reason, inline=False)

        embed.set_footer(
            text=f"Status updated by {interaction.user.display_name}",
            icon_url=interaction.user.display_avatar.url
        )
        
        lock_thread = (status in ["Implemented", "Denied"])
        await interaction.channel.edit(applied_tags=new_tags, locked=lock_thread)
        await interaction.channel.send(embed=embed)

        suggester_id = db.get_suggestion_suggester(interaction.channel.id)
        if suggester_id and suggester_id != interaction.user.id:
            try:
                suggester_user = await self.bot.fetch_user(suggester_id)
                dm_embed = Embed(
                    title="Your Suggestion has been Updated!",
                    description=f"Your suggestion, '{interaction.channel.name}', has been marked as **{status}** in **{interaction.guild.name}**.\n\n[Click here to view the suggestion.]({interaction.channel.jump_url})",
                    color=embed.color
                )
                if reason: dm_embed.add_field(name="Reason", value=reason, inline=False)
                await suggester_user.send(embed=dm_embed)
            except Exception as e:
                logging.warning(f"Failed to DM suggester {suggester_id}: {e}")

        await interaction.followup.send(f"Successfully updated the suggestion to '{status}'.", ephemeral=True)

    @suggestion_group.subcommand(name="config", description="Configure the suggestion system.")
    async def config_group(self, interaction: Interaction):
        pass

    @config_group.subcommand(name="set_channel", description="Sets the forum channel for suggestions.")
    @application_checks.has_permissions(manage_guild=True)
    async def set_channel(self, interaction: Interaction, channel: ForumChannel = SlashOption(required=True)):
        db.update_config(interaction.guild.id, {"forum_channel_id": str(channel.id)})
        await interaction.response.send_message(f"✅ Suggestion channel set to {channel.mention}.", ephemeral=True)

    @config_group.subcommand(name="set_pre_modal_message", description="Set a message to show before the suggestion form.")
    @application_checks.has_permissions(manage_guild=True)
    async def set_pre_modal_message(self, interaction: Interaction, message: Optional[str] = SlashOption(required=False)):
        db.update_config(interaction.guild.id, {"pre_modal_message": message})
        await interaction.response.send_message("✅ Pre-suggestion message updated.", ephemeral=True)

    @config_group.subcommand(name="set_status_tag", description="Assign a tag to a suggestion status.")
    @application_checks.has_permissions(manage_guild=True)
    async def set_status_tag(self, interaction: Interaction,
        status: str = SlashOption(name="status", choices=["Planned", "Implemented", "Denied"], required=True),
        tag_name: str = SlashOption(name="tag_name", description="The exact name of the tag to associate with this status.", required=True)):
        
        await interaction.response.defer(ephemeral=True)
        config = db.get_config(interaction.guild.id)
        forum_channel_id = config.get('forum_channel_id')
        if not forum_channel_id:
            await interaction.followup.send("Please set the suggestion forum channel first.", ephemeral=True); return

        forum_channel: Optional[ForumChannel] = self.bot.get_channel(int(forum_channel_id))
        if not forum_channel:
            await interaction.followup.send("The configured forum channel could not be found.", ephemeral=True); return

        found_tag = nextcord.utils.get(forum_channel.available_tags, name=tag_name)
        if not found_tag:
            await interaction.followup.send(f"No tag with the name `{tag_name}` was found in {forum_channel.mention}.", ephemeral=True); return

        db.update_config(interaction.guild.id, {f"{status.lower()}_tag_id": str(found_tag.id)})
        await interaction.followup.send(f"✅ Tag for '{status}' status set to `{found_tag.name}`.", ephemeral=True)

    @config_group.subcommand(name="set_status_message", description="Set the message for a status. Use {user} and {reason}.")
    @application_checks.has_permissions(manage_guild=True)
    async def set_status_message(self, interaction: Interaction,
        status: str = SlashOption(name="status", choices=["Planned", "Implemented", "Denied"], required=True),
        message: str = SlashOption(name="message", description="The message template to use.", required=True)):
        db.update_config(interaction.guild.id, {f"{status.lower()}_message": message})
        await interaction.response.send_message(f"✅ Message for '{status}' status updated.", ephemeral=True)

    @config_group.subcommand(name="view", description="Displays the current suggestion system configuration.")
    @application_checks.has_permissions(manage_guild=True)
    async def view_config(self, interaction: Interaction):
        config = db.get_config(interaction.guild.id)
        if not config: await interaction.response.send_message("No config found.", ephemeral=True); return
        
        embed = Embed(title="Suggestion System Configuration", color=Color.blurple())
        forum_channel = self.bot.get_channel(int(config.get('forum_channel_id'))) if config.get('forum_channel_id') else None
        embed.add_field(name="Forum Channel", value=forum_channel.mention if forum_channel else "Not Set", inline=False)
        embed.add_field(name="Pre-Modal Message", value=f"```{config.get('pre_modal_message')}```" if config.get('pre_modal_message') else "Not Set", inline=False)
        
        for status in ["Planned", "Implemented", "Denied"]:
            key = status.lower()
            tag_id = config.get(f"{key}_tag_id")
            tag_obj = forum_channel.get_tag(int(tag_id)) if forum_channel and tag_id else None
            tag_name = tag_obj.name if tag_obj else "Not Set"
            message = config.get(f"{key}_message")
            embed.add_field(name=f"{status} Status", value=f"**Tag:** `{tag_name}`\n**Message:**\n```{message}```", inline=False)
            
        await interaction.response.send_message(embed=embed, ephemeral=True)

def setup(bot: commands.Bot):
    bot.add_cog(SuggestionsCog(bot))