# cogs/suggestions_cog.py

import nextcord
from nextcord.ext import commands, application_checks
from nextcord import Interaction, SlashOption, Embed, Color, ForumChannel, Webhook, ui, TextInputStyle
import logging
from typing import Optional, List, Dict
import asyncio

# Import the database utility
from db_utils import suggestions_database as db

# --- Main Suggestion Modal ---
class SuggestionModal(ui.Modal):
    def __init__(self, bot: commands.Bot, config: Dict):
        super().__init__("Submit a Suggestion", timeout=600)
        self.bot = bot
        self.config = config

        self.suggestion_title = ui.TextInput(
            label="Suggestion Title",
            style=TextInputStyle.short,
            placeholder="Enter a concise title for your suggestion",
            min_length=config.get('title_min_length', 10),
            max_length=min(config.get('title_max_length', 45), 45),
            required=True
        )
        self.add_item(self.suggestion_title)

        self.suggestion_desc = ui.TextInput(
            label="Suggestion Description",
            style=TextInputStyle.paragraph,
            placeholder="Describe your suggestion in detail. What problem does it solve?",
            min_length=config.get('description_min_length', 50),
            max_length=min(config.get('description_max_length', 4000), 4000),
            required=True
        )
        self.add_item(self.suggestion_desc)

    async def callback(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        view = SuggestionView(
            bot=self.bot,
            original_interaction=interaction,
            config=self.config,
            title=self.suggestion_title.value,
            description=self.suggestion_desc.value
        )
        await view.send_initial_message()


# --- Pre-Modal Confirmation View ---
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


# --- Multi-Step Interaction View (SuggestionView) ---
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
            await self.original_interaction.followup.send("Configuration error: Forum channel not found.", ephemeral=True)
            return

        available_tags = forum_channel.available_tags
        if available_tags:
            # === DEBUGGING CHANGE: EMOJI LOGIC IS COMPLETELY REMOVED ===
            # This creates the simplest possible dropdown to isolate the error.
            tag_options = [
                nextcord.SelectOption(label=tag.name, value=str(tag.id))
                for tag in available_tags
            ]
            
            num_available_tags = len(tag_options)
            max_selectable = min(num_available_tags, 5) 

            tag_select = ui.Select(
                placeholder="Select relevant tags",
                options=tag_options, 
                min_values=0, 
                max_values=max_selectable,
                custom_id="suggestion_tag_select"
            )
            tag_select.callback = self.on_tag_select
            self.add_item(tag_select)
            
            await self.original_interaction.followup.send(
                "Please select the relevant tags for your suggestion.",
                view=self,
                ephemeral=True
            )
        else:
            self.prepare_anonymity_buttons(self.original_interaction.user)
            await self.original_interaction.followup.send(
                "Submitting with your name helps us give you credit and discuss your idea with you directly! However, you may choose to submit anonymously.",
                view=self,
                ephemeral=True
            )

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
        await interaction.response.edit_message(
            content="Submitting with your name helps us give you credit and discuss your idea with you directly! However, you may choose to submit anonymously.",
            view=self
        )

    async def on_anonymity_choice(self, interaction: Interaction):
        is_anonymous = (interaction.data['custom_id'] == "post_anonymously")

        await interaction.response.edit_message(content="Processing your suggestion... Please wait.", view=None)

        forum_channel_id = self.config.get('forum_channel_id')
        forum_channel: Optional[ForumChannel] = self.bot.get_channel(int(forum_channel_id))

        if not forum_channel:
            await self.original_interaction.followup.send("Error: Suggestion channel not found. Please contact an admin.", ephemeral=True)
            return

        bot_perms = forum_channel.permissions_for(interaction.guild.me)
        if not bot_perms.manage_webhooks or not bot_perms.manage_threads:
            await self.original_interaction.followup.send("Error: I'm missing `Manage Webhooks` or `Manage Threads` permission in the suggestions channel.", ephemeral=True)
            return

        webhook: Optional[Webhook] = None
        try:
            webhook = await forum_channel.create_webhook(name="Suggestion Poster")
            webhook_params = {"thread_name": self.title, "content": self.description, "wait": True}
            if is_anonymous:
                webhook_params["username"] = "Anonymous Suggestion"
            else:
                webhook_params["username"] = interaction.user.display_name
                webhook_params["avatar_url"] = interaction.user.display_avatar.url if interaction.user.display_avatar else None

            thread_message = await webhook.send(**webhook_params)
            new_thread = thread_message.thread

            if self.selected_tags and new_thread:
                await new_thread.edit(applied_tags=self.selected_tags)

            await self.original_interaction.followup.send(f"✅ Your suggestion has been posted! View it here: {new_thread.jump_url}", ephemeral=True)

        except nextcord.Forbidden:
            logging.error(f"Forbidden error during suggestion posting for user {interaction.user.id}")
            await self.original_interaction.followup.send("A permission error occurred. I might not be able to create webhooks or manage threads.", ephemeral=True)
        except Exception as e:
            logging.error(f"Error posting suggestion for user {interaction.user.id}: {e}", exc_info=True)
            await self.original_interaction.followup.send(f"An unexpected error occurred: {e}", ephemeral=True)
        finally:
            if webhook:
                await asyncio.sleep(1)
                await webhook.delete()

    async def on_timeout(self):
        try:
            await self.original_interaction.edit_original_message(content="Suggestion submission timed out.", view=None)
        except nextcord.NotFound:
            pass


# --- Cog Definition ---
class SuggestionsCog(commands.Cog, name="Suggestions"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        for guild in self.bot.guilds:
            db.initialize_database(guild.id)
        logging.info("SuggestionsCog loaded and databases initialized.")

    @commands.Cog.listener()
    async def on_guild_join(self, guild: nextcord.Guild):
        db.initialize_database(guild.id)
        logging.info(f"Initialized suggestions database for new guild: {guild.name} ({guild.id})")

    @nextcord.slash_command(name="suggest", description="Submit a suggestion for the community.")
    async def suggest(self, interaction: Interaction):
        config = db.get_config(interaction.guild.id)
        if not config or not config.get('forum_channel_id'):
            await interaction.response.send_message(
                "The suggestion system is not configured yet. An administrator must use `/suggestionadmin set_channel`.",
                ephemeral=True
            )
            return

        pre_modal_message = config.get('pre_modal_message')

        if pre_modal_message:
            view = PreSuggestionView(self.bot, config)
            await interaction.response.send_message(
                pre_modal_message,
                view=view,
                ephemeral=True
            )
        else:
            modal = SuggestionModal(self.bot, config)
            await interaction.response.send_modal(modal)

    @nextcord.slash_command(name="suggestionadmin", description="Admin commands for the suggestion system.")
    @application_checks.has_permissions(manage_guild=True)
    async def suggestionadmin(self, interaction: Interaction):
        pass

    @suggestionadmin.subcommand(name="set_channel", description="Sets the forum channel for suggestions.")
    async def set_channel(self, interaction: Interaction, channel: ForumChannel = SlashOption(description="The forum channel to post suggestions in.", required=True)):
        db.update_config(interaction.guild.id, {"forum_channel_id": str(channel.id)})
        await interaction.response.send_message(f"✅ Suggestion channel set to {channel.mention}.", ephemeral=True)

    @suggestionadmin.subcommand(name="set_pre_modal_message", description="Set a message to show before the suggestion form. Leave blank to disable.")
    async def set_pre_modal_message(self, interaction: Interaction, message: Optional[str] = SlashOption(description="The message to display.", required=False)):
        db.update_config(interaction.guild.id, {"pre_modal_message": message})
        if message:
            await interaction.response.send_message(f"✅ Pre-suggestion message has been set.", ephemeral=True)
        else:
            await interaction.response.send_message("✅ Pre-suggestion message has been removed.", ephemeral=True)

    @suggestionadmin.subcommand(name="set_title_length", description="Sets min/max character length for suggestion titles.")
    async def set_title_length(self, interaction: Interaction,
                                min_len: Optional[int] = SlashOption(name="min", description="Minimum characters (default 10).", required=False),
                                max_len: Optional[int] = SlashOption(name="max", description="Maximum characters (default 45).", required=False)):
        updates = {}
        if min_len is not None: updates["title_min_length"] = min_len
        if max_len is not None: updates["title_max_length"] = min(max_len, 45)
        if not updates: await interaction.response.send_message("No new lengths provided.", ephemeral=True); return
        db.update_config(interaction.guild.id, updates)
        await interaction.response.send_message("Suggestion title length constraints updated.", ephemeral=True)

    @suggestionadmin.subcommand(name="set_description_length", description="Sets min/max character length for suggestion descriptions.")
    async def set_description_length(self, interaction: Interaction,
                                      min_len: Optional[int] = SlashOption(name="min", description="Minimum characters (default 50).", required=False),
                                      max_len: Optional[int] = SlashOption(name="max", description="Maximum characters (default 4000).", required=False)):
        updates = {}
        if min_len is not None: updates["description_min_length"] = min_len
        if max_len is not None: updates["description_max_length"] = min(max_len, 4000)
        if not updates: await interaction.response.send_message("No new lengths provided.", ephemeral=True); return
        db.update_config(interaction.guild.id, updates)
        await interaction.response.send_message("Suggestion description length constraints updated.", ephemeral=True)

    @suggestionadmin.subcommand(name="view_config", description="Displays the current suggestion system configuration.")
    async def view_config(self, interaction: Interaction):
        config = db.get_config(interaction.guild.id)
        if not config: await interaction.response.send_message("No configuration found.", ephemeral=True); return
        embed = Embed(title="Suggestion System Configuration", color=Color.blurple())
        forum_id = config.get('forum_channel_id')
        forum_channel = self.bot.get_channel(int(forum_id)) if forum_id else None
        embed.add_field(name="Forum Channel", value=forum_channel.mention if forum_channel else "Not Set", inline=False)
        pre_message = config.get('pre_modal_message')
        embed.add_field(name="Pre-Modal Message", value=f"```{pre_message}```" if pre_message else "Not Set", inline=False)
        embed.add_field(name="Title Length", value=f"Min: `{config.get('title_min_length', 10)}`, Max: `{config.get('title_max_length', 45)}`", inline=True)
        embed.add_field(name="Description Length", value=f"Min: `{config.get('description_min_length', 50)}`, Max: `{config.get('description_max_length', 4000)}`", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)

def setup(bot: commands.Bot):
    bot.add_cog(SuggestionsCog(bot))