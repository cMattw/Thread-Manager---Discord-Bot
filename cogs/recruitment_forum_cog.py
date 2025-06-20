import nextcord
from nextcord.ext import commands, tasks, application_checks
from nextcord import (
    Interaction, SlashOption, Embed, Color, ForumChannel, Thread, Message,
    ui, ButtonStyle, TextInputStyle, Attachment, Webhook, ForumTag, Member
)
import logging
from typing import Optional, List, Dict
from datetime import datetime, timedelta, timezone
import asyncio
import aiohttp
import io

# This utility file does not need to be changed, but must exist
from db_utils import recruitment_database as db

logger = logging.getLogger('nextcord.recruitment_forum_cog')

def get_unix_time(offset_seconds: int = 0) -> int:
    return int((datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)).timestamp())

# --- CONSTANTS ---
APPLICATION_DELETION_SECONDS = 10800
WEEK_IN_SECONDS = 604800
REMINDER_TIMEOUT_SECONDS = 43200

# --- UI COMPONENTS ---

class GuidelineView(ui.View):
    def __init__(self, bot, cog):
        super().__init__(timeout=300)
        self.bot = bot
        self.cog = cog
    @ui.button(label="Understood", style=ButtonStyle.green)
    async def confirm(self, button: ui.Button, interaction: Interaction):
        await interaction.response.send_modal(RecruitModal(self.bot, self.cog))
        self.stop()

class RecruitModal(ui.Modal):
    def __init__(self, bot, cog, title="", requirements=""):
        super().__init__("Recruitment Post Details", timeout=600)
        self.bot = bot
        self.cog = cog
        self.team_name = ui.TextInput(label="Team Name (also the post title)", style=TextInputStyle.short, placeholder="Your team's name", default_value=title, required=True, min_length=5, max_length=90)
        self.add_item(self.team_name)
        self.requirements = ui.TextInput(label="Member Requirements", style=TextInputStyle.paragraph, placeholder="List your requirements, rules, and other details here.", default_value=requirements, required=True, min_length=20, max_length=2000)
        self.add_item(self.requirements)
    async def callback(self, interaction: Interaction):
        embed = Embed(title=f"Preview: {self.team_name.value}", description=self.requirements.value, color=Color.orange())
        view = MainDetailsPreviewView(self.bot, self.cog, self.team_name.value, self.requirements.value)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class MainDetailsPreviewView(ui.View):
    def __init__(self, bot, cog, title, requirements):
        super().__init__(timeout=600)
        self.bot = bot
        self.cog = cog
        self.title = title
        self.requirements = requirements
    @ui.button(label="Edit", style=ButtonStyle.grey)
    async def edit(self, button: ui.Button, interaction: Interaction):
        await interaction.response.send_modal(RecruitModal(self.bot, self.cog, self.title, self.requirements))
        self.stop()
    @ui.button(label="Confirm", style=ButtonStyle.green)
    async def confirm(self, button: ui.Button, interaction: Interaction):
        await interaction.response.edit_message(content="Would you like to add a team logo?", view=LogoUploadView(self.bot, self.cog, self.title, self.requirements))
        self.stop()

class LogoUploadView(ui.View):
    def __init__(self, bot, cog, title, requirements):
        super().__init__(timeout=180)
        self.bot = bot
        self.cog = cog
        self.title = title
        self.requirements = requirements
    @ui.button(label="Add Logo", style=ButtonStyle.grey)
    async def add_logo(self, button: ui.Button, interaction: Interaction):
        await interaction.response.edit_message(content="Please upload your team logo now.", view=None, embed=None)

        asset_channel_id = self.cog.config.get('asset_channel_id')
        if not asset_channel_id:
            await interaction.edit_original_message(
                content="❌ **Error for Admins:** The asset storage channel has not been configured. Please use `/recruit_admin set_asset_channel`.",
                view=None
            )
            self.stop()
            return

        try:
            asset_channel = await self.bot.fetch_channel(int(asset_channel_id))
        except (nextcord.NotFound, nextcord.Forbidden):
            await interaction.edit_original_message(
                content="❌ **Error for Admins:** I cannot find or access the configured asset storage channel.",
                view=None
            )
            self.stop()
            return

        try:
            msg = await self.bot.wait_for(
                "message",
                timeout=180.0,
                check=lambda m: m.author == interaction.user and m.channel == interaction.channel and m.attachments
            )
            logo_attachment = msg.attachments[0]

            if not logo_attachment.content_type or not logo_attachment.content_type.startswith("image/"):
                await interaction.edit_original_message(content="That file was not a valid image. Process cancelled.", view=None)
                return

            # Re-upload the image to get a permanent URL
            image_bytes = await logo_attachment.read()
            reuploaded_file = nextcord.File(io.BytesIO(image_bytes), filename=logo_attachment.filename)
            asset_message = await asset_channel.send(file=reuploaded_file)
            permanent_logo_url = asset_message.attachments[0].url

            # Delete the user's temporary upload message
            await msg.delete()

            final_embed = Embed(
                title=f"{self.title}",
                description=f"__**Member Requirements**__\n{self.requirements}",
                color=Color.blue()
            )
            if permanent_logo_url:
                final_embed.set_image(url=permanent_logo_url)

            await interaction.edit_original_message(
                content="This is the final preview.",
                embed=final_embed,
                view=FinalSubmitView(self.bot, self.cog, self.title, self.requirements, logo_url=permanent_logo_url)
            )

        except asyncio.TimeoutError:
            await interaction.edit_original_message(content="You took too long to upload a logo.", view=None)
        
        self.stop()

    @ui.button(label="Skip & Continue", style=ButtonStyle.grey)
    async def skip(self, button: ui.Button, interaction: Interaction):
        final_embed = Embed(title=f"{self.title}", description=f"__**Member Requirements**__\n{self.requirements}", color=Color.blue())
        await interaction.response.edit_message(content="This is the final preview.", embed=final_embed, view=FinalSubmitView(self.bot, self.cog, self.title, self.requirements, logo_url=None))
        self.stop()

class FinalSubmitView(ui.View):
    def __init__(self, bot, cog, title, requirements, logo_url=None):
        super().__init__(timeout=600)
        self.bot = bot
        self.cog = cog
        self.title = title
        self.requirements = requirements
        self.logo_url = logo_url

    @ui.button(label="Submit Recruitment", style=ButtonStyle.green)
    async def submit(self, button: ui.Button, interaction: Interaction):
        await interaction.response.edit_message(content="Submitting your post...", view=None, embed=None)
        success, message = await self.cog.create_recruitment_post(interaction, self.title, self.requirements, self.logo_url)
        await interaction.edit_original_message(content=f"✅ **Success!** Your post has been created: {message}" if success else f"❌ **Error:** {message}", view=None, embed=None)
        self.stop()

    @ui.button(label="Cancel", style=ButtonStyle.red)
    async def cancel(self, button: ui.Button, interaction: Interaction):
        await interaction.response.edit_message(content="Submission cancelled.", view=None, embed=None)
        self.stop()

class EditPostModal(ui.Modal):
    def __init__(self, cog, thread_id: int, current_title: str, current_reqs: str):
        super().__init__("Edit Recruitment Post", timeout=600)
        self.cog = cog
        self.thread_id = thread_id
        self.team_name = ui.TextInput(
            label="Team Name (Post Title)",
            style=TextInputStyle.short,
            default_value=current_title,
            required=True,
            max_length=90
        )
        self.add_item(self.team_name)
        self.requirements = ui.TextInput(
            label="Member Requirements",
            style=TextInputStyle.paragraph,
            default_value=current_reqs,
            required=True,
            max_length=2000
        )
        self.add_item(self.requirements)

    async def callback(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        main_post = await self.cog.get_main_post_message(self.thread_id)
        if not main_post:
            await interaction.followup.send(
                "Error: Could not find the original post to edit. It may have been deleted.",
                ephemeral=True
            )
            return

        # Preserve Team Members section if present
        original_content = main_post.content
        members_section_text = ""
        if "\n\n**Team Members:**\n" in original_content:
            other_sections = original_content.split("\n\n**Member Requirements:**\n", 1)[1]
            if "**Team Members:**\n" in other_sections:
                members_section_text = "**Team Members:**\n" + other_sections.split("**Team Members:**\n", 1)[1]

        # Construct the new content from the modal's input fields (NO logo URL)
        new_content = f"## {self.team_name.value}\n\n**Member Requirements:**\n{self.requirements.value}"
        if members_section_text:
            new_content += f"\n\n{members_section_text}"

        thread = main_post.channel
        forum_channel = thread.parent

        try:
            await main_post.delete()
            webhook = await forum_channel.create_webhook(name=f"Recruiter {interaction.user.display_name[:30]}")
            # Re-attach the logo if it exists
            files = main_post.attachments
            if files:
                # Download the attachment and re-upload it
                file_objs = []
                for att in files:
                    att_bytes = await att.read()
                    file_objs.append(nextcord.File(io.BytesIO(att_bytes), filename=att.filename))
                new_main_post_message = await webhook.send(
                    thread=thread,
                    content=new_content,
                    username=interaction.user.display_name,
                    avatar_url=interaction.user.display_avatar.url,
                    files=file_objs,
                    wait=True
                )
            else:
                new_main_post_message = await webhook.send(
                    thread=thread,
                    content=new_content,
                    username=interaction.user.display_name,
                    avatar_url=interaction.user.display_avatar.url,
                    wait=True
                )
            await webhook.delete()
            db.update_main_post_id(self.thread_id, new_main_post_message.id)
            await self.cog.refresh_manager_panel(thread)  # <-- ADD THIS LINE
            await interaction.followup.send("Your recruitment post has been updated.", ephemeral=True)
        except nextcord.errors.Forbidden as e:
            logger.error(f"Failed to edit post for thread {self.thread_id}: Bot lacks permissions. {e}", exc_info=True)
            await interaction.followup.send(
                "Error: I lack the permissions to manage messages or create webhooks in the forum channel.",
                ephemeral=True
            )
        except Exception as e:
            logger.error(f"Failed to edit post for thread {self.thread_id}: {e}", exc_info=True)
            await interaction.followup.send("An unexpected error occurred while updating the post.", ephemeral=True)

class EditLogoView(ui.View):
    def __init__(self, cog, thread_id: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.thread_id = thread_id
    async def _edit_logo_url(self, interaction: Interaction, new_url: Optional[str]):
        thread_data = db.get_managed_thread(self.thread_id)
        if not thread_data:
            await interaction.followup.send("Could not find post to edit.", ephemeral=True)
            return

        thread = await self.cog.bot.fetch_channel(self.thread_id)
        starter_message_id = thread_data.get('starter_message_id', self.thread_id)
        starter_message = await thread.fetch_message(int(starter_message_id))

        try:
            content = starter_message.content
            # Remove any existing image link (http/https line)
            lines = content.splitlines()
            lines = [line for line in lines if not (line.strip().startswith("http://") or line.strip().startswith("https://"))]
            content = "\n".join(lines)
            # Add the new image link if provided
            if new_url:
                content += f"\n{new_url}"
            await starter_message.edit(content=content)
            await self.cog.refresh_manager_panel(thread)
            if new_url:
                await interaction.followup.send("Logo link has been updated in the post!", ephemeral=True)
            else:
                await interaction.followup.send("Logo has been removed from the post!", ephemeral=True)
        except Exception as e:
            logger.error(f"Failed to edit logo link for thread {self.thread_id}: {e}", exc_info=True)
            await interaction.followup.send("An error occurred while editing the logo link.", ephemeral=True)
    @ui.button(label="Upload New Logo", style=ButtonStyle.primary)
    async def upload_logo(self, button: ui.Button, interaction: Interaction):
        await interaction.response.send_message("Please upload your new team logo now.", ephemeral=True)
        
        asset_channel_id = self.cog.config.get('asset_channel_id')
        if not asset_channel_id:
            await interaction.followup.send("❌ **Error for Admins:** The asset storage channel has not been configured.", ephemeral=True)
            return

        try:
            asset_channel = await self.cog.bot.fetch_channel(int(asset_channel_id))
        except (nextcord.NotFound, nextcord.Forbidden):
            await interaction.followup.send("❌ **Error for Admins:** I cannot find or access the configured asset storage channel.", ephemeral=True)
            return

        try:
            msg = await self.cog.bot.wait_for(
                "message",
                timeout=180.0,
                check=lambda m: m.author == interaction.user and m.channel == interaction.channel and m.attachments
            )
            logo_attachment = msg.attachments[0]
            if not logo_attachment.content_type or not logo_attachment.content_type.startswith("image/"):
                await interaction.followup.send("That was not an image.", ephemeral=True)
                return

            # Re-upload the image to get a permanent URL
            image_bytes = await logo_attachment.read()
            reuploaded_file = nextcord.File(io.BytesIO(image_bytes), filename=logo_attachment.filename)
            asset_message = await asset_channel.send(file=reuploaded_file)
            permanent_logo_url = asset_message.attachments[0].url

            # Call the internal edit function with the new permanent URL
            await self._edit_logo_url(interaction, permanent_logo_url)
            
            # Delete the user's temporary upload message
            await msg.delete()

        except asyncio.TimeoutError:
            await interaction.followup.send("You took too long to upload a logo.", ephemeral=True)
        
        self.stop()
    @ui.button(label="Remove Logo", style=ButtonStyle.red)
    async def remove_logo(self, button: ui.Button, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        await self._edit_logo_url(interaction, None)
        self.stop()

class UpdateMembersModal(ui.Modal):
    def __init__(self, cog, thread_id: int, current_members_text: str = ""):
        super().__init__("Update Team Members", timeout=600)
        self.cog = cog
        self.thread_id = thread_id
        self.members = ui.TextInput(label="Team Members", style=TextInputStyle.paragraph, placeholder="List members, one per line.", default_value=current_members_text, required=False, max_length=1000)
        self.add_item(self.members)
    async def callback(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        main_post = await self.cog.get_main_post_message(self.thread_id)
        if not main_post:
            await interaction.followup.send("Could not find the original post to update.", ephemeral=True); return
        content_parts = main_post.content.split("\n\n**Team Members:**\n")
        base_content = content_parts[0]
        new_content = base_content
        member_list_text = self.members.value.strip()
        if member_list_text:
            members = [f"• {line.strip()}" for line in member_list_text.split('\n') if line.strip()]
            new_content += f"\n\n**Team Members:**\n" + "\n".join(members)
        thread, forum_channel = main_post.channel, main_post.channel.parent
        try:
            await main_post.delete()
            webhook = await forum_channel.create_webhook(name=f"Recruiter {interaction.user.display_name[:30]}")
            new_main_post_message = await webhook.send(thread=thread, content=new_content, username=interaction.user.display_name, avatar_url=interaction.user.display_avatar.url, wait=True)
            await webhook.delete()
            db.update_main_post_id(self.thread_id, new_main_post_message.id)
            await self.cog.refresh_manager_panel(thread)  # <-- ADD THIS LINE
            await interaction.followup.send("Team members list updated.", ephemeral=True)
        except Exception as e:
            logger.error(f"Failed to update members for thread {self.thread_id}: {e}")
            await interaction.followup.send("An error occurred.", ephemeral=True)

class ManagerPanelView(ui.View):
    def __init__(self, cog: commands.Cog, thread_id: int, main_post_url: str, is_closed: bool = False, team_name: str = "the Team"):
        super().__init__(timeout=None)
        self.cog = cog
        self.thread_id = thread_id

        if is_closed:
            pass
        else:
            close_button = ui.Button(emoji="🔒", style=ButtonStyle.grey, row=0, custom_id="recman:close")
            close_button.callback = self.close_callback
            self.add_item(close_button)

            edit_post_button = ui.Button(emoji="✏️", style=ButtonStyle.grey, row=0, custom_id="recman:edit_post")
            edit_post_button.callback = self.edit_post_callback
            self.add_item(edit_post_button)

            edit_logo_button = ui.Button(emoji="🖼️", style=ButtonStyle.grey, row=0, custom_id="recman:edit_logo")
            edit_logo_button.callback = self.edit_logo_callback
            self.add_item(edit_logo_button)

            update_members_button = ui.Button(emoji="👥", style=ButtonStyle.grey, row=0, custom_id="recman:update_members")
            update_members_button.callback = self.update_members_callback
            self.add_item(update_members_button)

            button_team_name = (team_name[:20] + '…') if len(team_name) > 22 else team_name
            apply_button = ui.Button(label=f"Join {button_team_name}", emoji="🤝", style=ButtonStyle.blurple, row=1, custom_id="recman:apply")
            apply_button.callback = self.apply_callback
            self.add_item(apply_button)

        if main_post_url:
            link_button = ui.Button(label="Back to Top", emoji="⬆️", style=ButtonStyle.link, url=main_post_url, row=2)
            self.add_item(link_button)

    async def interaction_check(self, interaction: Interaction) -> bool:
        # Use the channel ID from the interaction for persistent views
        thread_id = getattr(self, "thread_id", None) or getattr(interaction.channel, "id", None)
        custom_id = interaction.data.get("custom_id", "")
        if "apply" in custom_id:
            return True # Allow everyone to click the apply button

        thread_data = db.get_managed_thread(thread_id)
        if not thread_data or interaction.user.id != int(thread_data['op_id']):
            await interaction.response.send_message("This is not your post or you lack permissions.", ephemeral=True)
            return False
        return True

    # --- Callback Implementations ---
    async def close_callback(self, interaction: Interaction):
        await self.cog._handle_close(interaction, interaction.channel.id)
    async def edit_post_callback(self, interaction: Interaction):
        await self.cog._handle_edit_post(interaction, interaction.channel.id)
    async def edit_logo_callback(self, interaction: Interaction):
        await self.cog._handle_edit_logo(interaction, interaction.channel.id)
    async def update_members_callback(self, interaction: Interaction):
        await self.cog._handle_update_members(interaction, interaction.channel.id)
    async def apply_callback(self, interaction: Interaction):
        thread_data = db.get_managed_thread(interaction.channel.id)
        if not thread_data: return

        # Prevent OP from applying to their own post
        if interaction.user.id == int(thread_data['op_id']):
            await interaction.response.send_message("You cannot apply to your own post.", ephemeral=True)
            return

        # Check for existing application status
        application_status = db.get_applicant_status(interaction.channel.id, interaction.user.id)
        if application_status == 'pending':
            await interaction.response.send_message("You already have a pending application for this post.", ephemeral=True)
            return
        if application_status == 'accepted':
            await interaction.response.send_message("You have already been accepted to this team.", ephemeral=True)
            return

        # If all checks pass, show the application modal
        await self.cog._handle_apply(interaction, interaction.channel.id)

class ApplicationModal(ui.Modal):
    def __init__(self, cog, thread_id: int, team_name: str):
        super().__init__(f"Apply to {team_name}", timeout=600)
        self.cog = cog
        self.thread_id = thread_id
        self.ingame_name = ui.TextInput(label="In-Game Username", style=TextInputStyle.short, required=True, max_length=100)
        self.add_item(self.ingame_name)
        self.reason = ui.TextInput(label="Reason for Application", style=TextInputStyle.paragraph, required=True, max_length=1500, placeholder="Why do you want to join?")
        self.add_item(self.reason)
    async def callback(self, interaction: Interaction):
        await self.cog._handle_apply_submit(interaction, self.thread_id, self.ingame_name.value, self.reason.value)

class ApplicationActionView(ui.View):
    def __init__(self, cog, applicant_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.applicant_id = applicant_id

    async def interaction_check(self, interaction: Interaction) -> bool:
        # This check runs before any button callback.
        # It ensures only the post owner can click the buttons.
        thread_data = db.get_managed_thread(interaction.channel.id)
        if not thread_data or interaction.user.id != int(thread_data['op_id']):
            await interaction.response.send_message("You are not the owner of this post and cannot decide on applications.", ephemeral=True)
            return False
        return True

    @ui.button(label="Accept", style=ButtonStyle.green)
    async def accept(self, button: ui.Button, interaction: Interaction):
        # The permission check has already passed. Now we show the decision modal.
        modal = ApplicationDecisionModal(self.cog, "accept", self.applicant_id, interaction.message.id)
        await interaction.response.send_modal(modal)

    @ui.button(label="Deny", style=ButtonStyle.red)
    async def deny(self, button: ui.Button, interaction: Interaction):
        # The permission check has already passed. Now we show the decision modal.
        modal = ApplicationDecisionModal(self.cog, "deny", self.applicant_id, interaction.message.id)
        await interaction.response.send_modal(modal)

class ApplicationDecisionModal(ui.Modal):
    def __init__(self, cog, action: str, applicant_id: int, original_message_id: int):
        super().__init__(f"{action.capitalize()} Application", timeout=300)
        self.cog = cog
        self.action = action
        self.applicant_id = applicant_id
        self.original_message_id = original_message_id
        self.admin_message = ui.TextInput(label="Optional Message to Applicant", style=TextInputStyle.paragraph, placeholder="This message will be included in the status update.", required=False, max_length=1024)
        self.add_item(self.admin_message)
    async def callback(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            original_message = await interaction.channel.fetch_message(self.original_message_id)
            await original_message.delete()
        except (nextcord.NotFound, nextcord.Forbidden): pass
        try:
            applicant = await interaction.guild.fetch_member(self.applicant_id)
        except nextcord.NotFound:
            await interaction.followup.send("The applicant could not be found in the server.", ephemeral=True); return
        db.update_applicant_status(interaction.channel.id, applicant.id, self.action)
        thread = interaction.channel
        main_post = await self.cog.get_main_post_message(thread.id)
        team_name = "a team"
        if main_post:
            first_line = main_post.content.split('\n', 1)[0]
            if first_line.startswith("## "): team_name = first_line[3:].strip()
        action_past_tense = "Accepted" if self.action == 'accept' else "Denied"
        desc = (f"{applicant.mention}, your application to join **{team_name}** has been {self.action}ed by {interaction.user.mention}."
                f"\n\n-# This message will be deleted <t:{get_unix_time(APPLICATION_DELETION_SECONDS)}:R>")
        embed = Embed(title=f"Application {action_past_tense}!", color=Color.green() if self.action == 'accept' else Color.red(), description=desc)
        if self.admin_message.value: embed.add_field(name="Message from Admin", value=self.admin_message.value, inline=False)
        result_msg = await thread.send(content=applicant.mention, embed=embed)
        await self.cog.refresh_manager_panel(thread)
        db.add_scheduled_deletion(result_msg.id, thread.id, get_unix_time(APPLICATION_DELETION_SECONDS))
        await interaction.followup.send(f"You have `{self.action}ed` the application.", ephemeral=True)

class WeeklyReminderView(ui.View):
    def __init__(self, thread_id: int):
        super().__init__(timeout=REMINDER_TIMEOUT_SECONDS)
        self.children[0].custom_id = f"recman_remind:keep:{thread_id}"
        self.children[1].custom_id = f"recman_remind:close:{thread_id}"
    async def on_timeout(self):
        for item in self.children: item.disabled = True
        try: await self.message.edit(content="*This reminder has expired.*", view=self)
        except nextcord.NotFound: pass
    async def handle_reminder_response(self, interaction: Interaction, keep: bool):
        thread_id = int(interaction.data['custom_id'].split(':')[-1])
        thread_data = db.get_managed_thread(thread_id)
        if not thread_data or interaction.user.id != int(thread_data['op_id']):
            await interaction.response.send_message("This is not for you.", ephemeral=True); return
        await interaction.message.delete()
        if keep:
            db.update_reminder_timestamp(thread_id, None)
            await interaction.response.send_message("Thanks! Your post will remain open.", ephemeral=True)
        else:
            cog = interaction.client.get_cog("RecruitmentForumManager")
            thread = await interaction.guild.fetch_channel(thread_id)
            await cog.update_thread_state(thread, is_closing=True)
            await interaction.response.send_message("Post closed.", ephemeral=True)
        self.stop()

# --- THE COG ---
class RecruitmentForumManager(commands.Cog, name="RecruitmentForumManager"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = {}
        self.initialized = False
        self.panel_refresh_lock = asyncio.Lock()
        self.session: Optional[aiohttp.ClientSession] = None
        self.scheduled_deletion_task.start()
        self.weekly_reminder_task.start()
        self.inactivity_check_task.start()

    @commands.Cog.listener()
    async def on_ready(self):
        if self.initialized: return
        # Register persistent view template (thread_id=0 is a dummy, will be replaced at runtime)
        self.bot.add_view(ManagerPanelView(self, 0, "", is_closed=False, team_name="the Team"))
        self.session = aiohttp.ClientSession()
        guild_id = self.bot.guilds[0].id if self.bot.guilds else None
        if guild_id:
            db.initialize_database(guild_id)
            self.config = db.get_config(guild_id) or {}
            self.initialized = True
            logger.info("RecruitmentForumManager Cog Initialized.")
        else: logger.error("RecruitmentForumManager could not initialize: Bot is not in any guilds.")

    def cog_unload(self):
        """
        This is the crucial cleanup method. It's called when the cog is unloaded.
        """
        self.scheduled_deletion_task.cancel()
        self.weekly_reminder_task.cancel()
        self.inactivity_check_task.cancel()

        self.bot.remove_listener(self.on_interaction)
        self.bot.remove_listener(self.on_message)
  
        if self.session:
            self.bot.loop.create_task(self.session.close())

        logger.info("RecruitmentForumManager Cog Unloaded and listeners/tasks cleaned up.")

    async def system_check(self, interaction: Interaction) -> bool:
        if not all(self.config.get(k) for k in ['forum_channel_id', 'open_tag_id', 'closed_tag_id']):
            await interaction.response.send_message("Recruitment system is not configured by an admin.", ephemeral=True)
            return False
        return True

    async def get_main_post_message(self, thread_id: int) -> Optional[Message]:
        thread_data = db.get_managed_thread(thread_id)
        if not thread_data: return None
        try:
            thread = await self.bot.fetch_channel(thread_id)
            return await thread.fetch_message(int(thread_data['main_post_message_id']))
        except (nextcord.NotFound, nextcord.Forbidden): return None

    async def get_tags(self, guild: nextcord.Guild) -> Optional[tuple[ForumTag, ForumTag]]:
        try:
            forum = await guild.fetch_channel(int(self.config['forum_channel_id']))
            open_tag = nextcord.utils.get(forum.available_tags, id=int(self.config['open_tag_id']))
            closed_tag = nextcord.utils.get(forum.available_tags, id=int(self.config['closed_tag_id']))
            return (open_tag, closed_tag) if open_tag and closed_tag else (None, None)
        except Exception: return None, None

    def _get_message_link(self, guild_id: int, channel_id: int, message_id: int) -> str:
        return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"

    async def _get_thread_data(self, thread_id: int) -> tuple[Optional[Thread], Optional[dict]]:
        thread_data = db.get_managed_thread(thread_id)
        if not thread_data: return None, None
        try:
            thread = await self.bot.fetch_channel(thread_id)
            return (thread, thread_data) if isinstance(thread, nextcord.Thread) else (None, None)
        except nextcord.NotFound: return None, None
    
    async def refresh_manager_panel(self, thread: Thread):
        """
        Deletes the old manager panel and sends a new one with the latest state.
        """
        async with self.panel_refresh_lock:
            thread_data = db.get_managed_thread(thread.id)
            if not thread_data:
                return

            # Delete the old panel message
            try:
                if panel_id := thread_data.get('manager_panel_message_id'):
                    old_panel_message = await thread.fetch_message(int(panel_id))
                    await old_panel_message.delete()
            except (nextcord.NotFound, nextcord.Forbidden):
                pass

            # Create and send the new panel
            try:
                main_post = await self.get_main_post_message(thread.id)
                if not main_post:
                    return

                first_line = main_post.content.split('\n', 1)[0]
                team_name = first_line[3:].strip() if first_line.startswith("## ") else "Team"
                
                # Get the latest status from the database
                is_closed = thread_data['is_closed'] == 1
                
                main_post_url = self._get_message_link(thread.guild.id, main_post.channel.id, main_post.id)
                
                view = ManagerPanelView(self, thread.id, main_post_url, is_closed, team_name)
                
                if is_closed:
                    desc = "This recruitment post is currently closed.\nUse the `/recruitment reopen` command to reopen it."
                    color = Color.red()
                else:
                    desc = (f"`🔒` **Close Recruitment:** Locks this post.\n`✏️` **Edit Post:** Re-opens the modal.\n`🖼️` **Edit/Add Logo:** Change or remove the logo.\n`👥` **Update Members:** Edit the list of team members.\n🤝 **Join {team_name}:** Submit an application.\n`⬆️` **Back to Top:** Jumps to the top of the post.")
                    color = Color.dark_grey()

                embed = Embed(title="Recruitment Manager Panel", description=desc, color=color)
                new_panel = await thread.send(embed=embed, view=view)
                db.update_thread_panel_id(thread.id, new_panel.id)
            except Exception as e:
                logger.error(f"Failed to resend manager panel in {thread.id}: {e}", exc_info=True)

    async def update_thread_state(self, thread: Thread, is_closing: bool):
        open_tag, closed_tag = await self.get_tags(thread.guild)
        if not open_tag or not closed_tag: return
        current_tags = thread.applied_tags
        if is_closing:
            new_tags = [tag for tag in current_tags if tag.id != open_tag.id] + [closed_tag]
            await thread.edit(locked=True, archived=True, applied_tags=list(set(new_tags)))
            # db.update_thread_status(thread.id, is_closed=True) # <-- This line can be removed
        else:
            new_tags = [tag for tag in current_tags if tag.id != closed_tag.id] + [open_tag]
            await thread.edit(locked=False, archived=False, applied_tags=list(set(new_tags)))
            db.update_thread_status(thread.id, is_closed=False) # <-- Keep this one for reopening
            db.update_reminder_timestamp(thread.id, None)

    async def create_recruitment_post(self, interaction: Interaction, title: str, requirements: str, logo_url: Optional[str]) -> tuple[bool, str]:
        if not await self.system_check(interaction): return False, "System is not configured."
        try:
            forum_channel = await interaction.guild.fetch_channel(int(self.config['forum_channel_id']))
            open_tag, _ = await self.get_tags(interaction.guild)
            if not open_tag: return False, "Open tag not configured."
        except Exception as e:
            logger.error(f"Error fetching config during post creation: {e}")
            return False, "Could not find configured forum channel or tags."

        # Prepare the file if logo_url is provided
        file = None
        if logo_url:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(logo_url) as resp:
                        if resp.status == 200:
                            image_bytes = await resp.read()
                            filename = logo_url.split("/")[-1].split("?")[0]
                            file = nextcord.File(io.BytesIO(image_bytes), filename=filename)
            except Exception as e:
                logger.error(f"Error preparing logo file: {e}", exc_info=True)
                file = None

        try:
            # 1. Create the thread with the initial message (logo attached here)
            thread: Thread = await forum_channel.create_thread(
                name=title,
                content=f"'{title}' is recruiting! (Posted by: {interaction.user.mention})",
                applied_tags=[open_tag],
                file=file if file else None
            )
            starter_message = await thread.fetch_message(thread.id)  # This is the starter message

            # 2. Send the main post message (NO logo attached here)
            webhook = await forum_channel.create_webhook(name=f"Recruiter {interaction.user.display_name[:30]}")
            post_content = f"## {title}\n\n**Member Requirements:**\n{requirements}"
            main_post_message = await webhook.send(
                thread=thread,
                content=post_content,
                username=interaction.user.display_name,
                avatar_url=interaction.user.display_avatar.url,
                wait=True
            )
            await webhook.delete()

            main_post_url = self._get_message_link(interaction.guild.id, thread.id, main_post_message.id)
            panel_view = ManagerPanelView(self, thread.id, main_post_url, is_closed=False, team_name=title)
            desc = (f"`🔒` **Close Recruitment:** Locks this post.\n`✏️` **Edit Post:** Re-opens the modal.\n`🖼️` **Edit/Add Logo:** Change or remove the logo.\n`👥` **Update Members:** Edit the list of team members.\n🤝 **Join {title}:** Submit an application.\n`⬆️` **Back to Top:** Jumps to the top of the post.")
            panel_embed = Embed(title="Recruitment Manager Panel", description=desc, color=Color.dark_grey())
            panel_message = await thread.send(embed=panel_embed, view=panel_view)
            db.add_managed_thread(thread.id, interaction.user.id, main_post_message.id, panel_message.id, get_unix_time(), starter_message_id=starter_message.id)
            return True, main_post_message.jump_url
        except Exception as e:
            logger.error(f"Post creation failed: {e}", exc_info=True)
            return False, "An unexpected error occurred."

    @commands.Cog.listener()
    async def on_message(self, message: Message):
        if message.author.bot or not message.guild or not isinstance(message.channel, Thread):
            return
        thread_data = db.get_managed_thread(message.channel.id)
        if not thread_data:
            return
        # Only allow the OP to send messages
        if message.author.id != int(thread_data['op_id']):
            try:
                await message.delete()
            except Exception:
                pass
            try:
                warn_msg = await message.channel.send(
                    f"{message.author.mention}, you cannot send messages in this recruitment thread to prevent clutter. Only the original poster can send messages here."
                )
                await asyncio.sleep(10)
                await warn_msg.delete()
            except Exception:
                pass
            return
        # If OP, allow and refresh the manager panel
        await self.refresh_manager_panel(message.channel)

    @commands.Cog.listener()
    async def on_interaction(self, interaction: Interaction):
        custom_id = interaction.data.get("custom_id")
        if not custom_id:
            return

        if custom_id.startswith("recman_remind:"):
            await self.handle_reminder_response(interaction, "keep" in custom_id)

    async def _handle_close(self, interaction: Interaction, thread_id: int):
        await interaction.response.defer(ephemeral=True)
        thread, _ = await self._get_thread_data(thread_id)
        if not thread:
            await interaction.followup.send("Could not find the thread to close.", ephemeral=True)
            return

        # --- REVERSED ORDER ---
        # 1. First, update the panel to show the "Closed" state.
        #    To do this, we temporarily update the DB, refresh, then lock.
        db.update_thread_status(thread.id, is_closed=True)
        await self.refresh_manager_panel(thread)

        # 2. Now, lock and archive the thread.
        await self.update_thread_state(thread, is_closing=True)
        # --- END OF FIX ---
        
        await interaction.followup.send("Post closed.", ephemeral=True)

    async def _handle_reopen(self, interaction: Interaction, thread_id: int):
        await interaction.response.defer(ephemeral=True)
        thread, _ = await self._get_thread_data(thread_id)
        if not thread:
            await interaction.followup.send("Could not find the thread to reopen.", ephemeral=True)
            return

        # --- REVERSED ORDER ---
        # 1. First, un-archive and unlock the thread.
        await self.update_thread_state(thread, is_closing=False)

        # 2. Now that the thread is open, refresh the panel.
        await self.refresh_manager_panel(thread)
        # --- END OF FIX ---
        
        await interaction.followup.send("Post reopened.", ephemeral=True)

    async def _handle_apply(self, interaction: Interaction, thread_id: int):
        main_post = await self.get_main_post_message(thread_id)
        team_name = "this team"
        if main_post:
            first_line = main_post.content.split('\n', 1)[0]
            if first_line.startswith("## "): team_name = first_line[3:].strip()
        await interaction.response.send_modal(ApplicationModal(self, thread_id, team_name))

    async def _handle_apply_submit(self, interaction: Interaction, thread_id: int, ign: str, reason: str):
        await interaction.response.defer(ephemeral=True)
        # --- Prevent duplicate applications ---
        existing_status = db.get_applicant_status(thread_id, interaction.user.id)
        if existing_status is not None:
            await interaction.followup.send("You have already applied to this post.", ephemeral=True)
            return
        db.add_applicant(thread_id, interaction.user.id)
        thread_data = db.get_managed_thread(thread_id)
        op_user = await interaction.guild.fetch_member(int(thread_data['op_id']))
        thread = await self.bot.fetch_channel(thread_id)
        main_post = await self.get_main_post_message(thread_id)
        team_name = "a team"
        if main_post:
            first_line = main_post.content.split('\n', 1)[0]
            if first_line.startswith("## "): team_name = first_line[3:].strip()

        embed = Embed(title=f"New Application for {team_name}", color=Color.gold())
        embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)
        embed.add_field(name="Applicant", value=interaction.user.mention, inline=False)
        embed.add_field(name="In-Game Name", value=ign, inline=False)
        embed.add_field(name="Reason", value=reason, inline=False)
        await thread.send(content=f"{op_user.mention}, you have a new applicant.", embed=embed, view=ApplicationActionView(self, interaction.user.id))
        await interaction.followup.send("Your application has been submitted!", ephemeral=True)

    async def _handle_edit_post(self, interaction: Interaction, thread_id: int):
        thread, thread_data = await self._get_thread_data(thread_id)
        if not thread or not thread_data or interaction.user.id != int(thread_data['op_id']):
            await interaction.response.send_message("Post not found or you lack permissions.", ephemeral=True)
            return
        
        main_post = await self.get_main_post_message(thread_id)
        if not main_post:
            await interaction.response.send_message("Could not find the original post to edit.", ephemeral=True)
            return

        # --- NEW, more robust parsing logic ---
        content_lines = main_post.content.split('\n')
        current_title = ""
        requirements_lines = []
        in_requirements_section = False

        for line in content_lines:
            stripped_line = line.strip()

            # Find the title (which is always the first H2 header)
            if stripped_line.startswith("## "):
                current_title = stripped_line[3:].strip()
                continue # Move to the next line

            if in_requirements_section and stripped_line.startswith("**"):
                in_requirements_section = False
      
            if stripped_line.lower() == "**member requirements:**":
                in_requirements_section = True
                continue 

            if in_requirements_section:
                if not (stripped_line.startswith("https://") or stripped_line.startswith("http://")):
                    requirements_lines.append(line)

        current_reqs = "\n".join(requirements_lines).strip()

        modal = EditPostModal(self, thread_id, current_title, current_reqs)
        await interaction.response.send_modal(modal)

    async def _handle_edit_logo(self, interaction: Interaction, thread_id: int):
        if interaction.response.is_done():
            logger.warning("Edit Logo interaction already acknowledged."); return
        thread, thread_data = await self._get_thread_data(thread_id)
        if not thread or not thread_data or interaction.user.id != int(thread_data['op_id']):
            return await interaction.response.send_message("Post not found or you lack permissions.", ephemeral=True)
        await interaction.response.send_message("How would you like to change the logo?", view=EditLogoView(self, thread_id), ephemeral=True)

    async def _handle_update_members(self, interaction: Interaction, thread_id: int):
        if interaction.response.is_done():
            logger.warning("Update Members interaction already acknowledged."); return
        thread, thread_data = await self._get_thread_data(thread_id)
        if not thread or not thread_data or interaction.user.id != int(thread_data['op_id']):
            return await interaction.response.send_message("Post not found or you lack permissions.", ephemeral=True)
        main_post = await self.get_main_post_message(thread_id)
        current_members_text = ""
        if main_post:
            content_parts = main_post.content.split("\n\n**Team Members:**\n")
            if len(content_parts) > 1:
                current_members_text = content_parts[1].split("\n\nhttps://")[0].replace("• ", "")
        await interaction.response.send_modal(UpdateMembersModal(self, thread_id, current_members_text))
    
    # --- SLASH COMMANDS ---

    @nextcord.slash_command(name="recruitment")
    async def recruitment(self, interaction: Interaction): pass

    @recruitment.subcommand(name="create", description="Create a new recruitment post.")
    async def create(self, interaction: Interaction):
        if not await self.system_check(interaction): return
        open_post = next((t for t in db.get_user_threads(interaction.user.id) if t['is_closed'] == 0), None)
        if open_post:
            thread_link = f"https://discord.com/channels/{interaction.guild.id}/{open_post['thread_id']}"
            await interaction.response.send_message(f"❌ You already have an open recruitment post: {thread_link}", ephemeral=True)
            return

        guidelines = (
            "**Recruitment Guidelines**\n"
            "1. **Only the Team Leader should create a recruitment post.**\n"
            "2. **You may only have one (1) open recruitment post at a time.**\n"
            "3. **Do not spam or bump your post.**\n"
            "4. **Keep all communication in the thread relevant to recruitment.**\n"
            "5. **Only the original poster (OP) can send messages in the thread.**\n"
            "6. **Posts inactive for 7 days will be auto-closed.**\n"
            "7. **You can edit your post, logo, and team members at any time using the manager panel.**\n"
            "8. **Do not use offensive or misleading content.**\n"
            "9. **Breaking these rules may result in your post being removed and/or further moderation action.**\n"
            "\nBy clicking 'Understood', you agree to follow these guidelines."
        )

        await interaction.response.send_message(guidelines, view=GuidelineView(self.bot, self), ephemeral=True)

    @recruitment.subcommand(name="list", description="Shows your active and closed recruitment posts.")
    async def list_posts(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        user_threads = db.get_user_threads(interaction.user.id)
        if not user_threads:
            await interaction.followup.send("You do not have any recruitment posts.", ephemeral=True); return
        embed = Embed(title="Your Recruitment Posts", color=Color.blue(), timestamp=datetime.now(timezone.utc))
        for thread_data in user_threads:
            thread_id, status = thread_data['thread_id'], "Closed" if thread_data['is_closed'] == 1 else "Open"
            thread_link = self._get_message_link(interaction.guild.id, thread_id, thread_data['main_post_message_id'])
            try: thread_obj = await self.bot.fetch_channel(int(thread_id)); field_name = f"Post: {thread_obj.name}"
            except (nextcord.NotFound, nextcord.Forbidden): field_name = f"Post ID: {thread_id}"
            embed.add_field(name=field_name, value=f"**Status:** {status}\n[Jump to Post]({thread_link})", inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @recruitment.subcommand(name="close", description="Closes one of your open recruitment posts.")
    async def close(self, interaction: Interaction, post: str = SlashOption(required=True)):
        thread_id = int(post)
        thread_data = db.get_managed_thread(thread_id)
        if not thread_data or int(thread_data['op_id']) != interaction.user.id:
            await interaction.response.send_message("This is not your post or it is not tracked.", ephemeral=True); return
        if thread_data['is_closed'] == 1:
            await interaction.response.send_message("This post is already closed.", ephemeral=True); return
        try:
            thread = await self.bot.fetch_channel(thread_id)
            await self._handle_close(interaction, thread_id)
        except nextcord.NotFound:
            await interaction.response.defer(ephemeral=True)
            db.update_thread_status(thread_id, is_closed=True)
            await interaction.followup.send("That thread no longer exists, but I have closed it in the database.", ephemeral=True)

    @close.on_autocomplete("post")
    async def close_autocomplete(self, interaction: Interaction, post: str):
        if not interaction.user: return
        choices = {}
        for thread_data in [t for t in db.get_user_threads(interaction.user.id) if t['is_closed'] == 0]:
            try:
                thread_obj = await self.bot.fetch_channel(int(thread_data['thread_id']))
                choices[thread_obj.name[:100]] = thread_data['thread_id']
            except (nextcord.NotFound, nextcord.Forbidden):
                choices[f"ID (may be deleted): {thread_data['thread_id']}"] = thread_data['thread_id']
        filtered_choices = {k: v for k, v in choices.items() if post.lower() in k.lower()}
        try:
            await interaction.response.send_autocomplete(filtered_choices)
        except nextcord.NotFound:
            pass

    @recruitment.subcommand(name="reopen", description="Reopens one of your closed recruitment posts.")
    async def reopen(self, interaction: Interaction, post: str = SlashOption(required=True)):
        """
        Allows a user to reopen one of their closed posts via a slash command.
        """
        try:
            thread_id = int(post)
        except ValueError:
            # This can happen if the autocomplete fails or is bypassed.
            await interaction.response.send_message("Invalid post ID provided.", ephemeral=True)
            return

        thread_data = db.get_managed_thread(thread_id)
        if not thread_data or int(thread_data['op_id']) != interaction.user.id:
            await interaction.response.send_message("This is not your post or it is not tracked.", ephemeral=True)
            return

        if thread_data['is_closed'] == 0:
            await interaction.response.send_message("This post is already open.", ephemeral=True)
            return

        # We can reuse the internal _handle_reopen function.
        # It already has the logic to un-archive the thread and refresh the panel.
        await self._handle_reopen(interaction, thread_id)

    @reopen.on_autocomplete("post")
    async def reopen_autocomplete(self, interaction: Interaction, post: str):
        if not interaction.user:
            return
        choices = {}
        for thread_data in [t for t in db.get_user_threads(interaction.user.id) if t['is_closed'] == 1]:
            try:
                thread_obj = await self.bot.fetch_channel(int(thread_data['thread_id']))
                choices[thread_obj.name[:100]] = str(thread_data['thread_id'])
            except (nextcord.NotFound, nextcord.Forbidden):
                choices[f"ID (may be deleted): {thread_data['thread_id']}"] = str(thread_data['thread_id'])
        filtered_choices = {k: v for k, v in choices.items() if post.lower() in k.lower()}
        try:
            await interaction.response.send_autocomplete(filtered_choices)
        except nextcord.NotFound:
            pass

    @recruitment.subcommand(name="delete", description="Delete one of your closed recruitment posts.")
    async def delete_post(self, interaction: Interaction, post: str = SlashOption(required=True, description="Select a closed post to delete")):
        """
        Allows a user to delete one of their closed recruitment posts.
        """
        try:
            thread_id = int(post)
        except ValueError:
            await interaction.response.send_message("Invalid post ID provided.", ephemeral=True)
            return

        thread_data = db.get_managed_thread(thread_id)
        if not thread_data or int(thread_data['op_id']) != interaction.user.id:
            await interaction.response.send_message("This is not your post or it is not tracked.", ephemeral=True)
            return

        if thread_data['is_closed'] == 0:
            await interaction.response.send_message("You can only delete closed posts. Please close the post first.", ephemeral=True)
            return

        try:
            thread = await self.bot.fetch_channel(thread_id)
            await thread.delete()
        except (nextcord.NotFound, nextcord.Forbidden):
            pass

        db.delete_managed_thread(thread_id)
        await interaction.response.send_message("Your closed recruitment post has been deleted.", ephemeral=True)

    @delete_post.on_autocomplete("post")
    async def delete_post_autocomplete(self, interaction: Interaction, post: str):
        if not interaction.user:
            return
        choices = {}
        for thread_data in [t for t in db.get_user_threads(interaction.user.id) if t['is_closed'] == 1]:
            thread_id = str(thread_data['thread_id'])
            # If you want to avoid repeated fetches, you could cache missing IDs in memory or DB.
            try:
                thread_obj = await self.bot.fetch_channel(int(thread_id))
                choices[thread_obj.name[:100]] = thread_id
            except (nextcord.NotFound, nextcord.Forbidden):
                # Only add the "may be deleted" entry, do not try to fetch again
                choices[f"ID (may be deleted): {thread_id}"] = thread_id
                # Optionally, mark as closed in DB here if you want to clean up
                db.update_thread_status(thread_id, is_closed=True)
        filtered_choices = {k: v for k, v in choices.items() if post.lower() in k.lower()}
        try:
            await interaction.response.send_autocomplete(filtered_choices)
        except nextcord.NotFound:
            pass

    @nextcord.slash_command(name="recruit_admin")
    @application_checks.has_permissions(manage_guild=True)
    async def recruit_admin(self, interaction: Interaction): pass

    @recruit_admin.subcommand(name="set_channel", description="Set the forum channel for recruitment posts.")
    async def set_channel(self, interaction: Interaction, channel: ForumChannel):
        db.update_config(interaction.guild.id, {'forum_channel_id': str(channel.id)})
        self.config = db.get_config(interaction.guild.id) or {}
        await interaction.response.send_message(f"✅ Recruitment channel set to {channel.mention}.", ephemeral=True)

    @recruit_admin.subcommand(name="set_tags", description="Set the 'Open' and 'Closed' tags for posts.")
    async def set_tags(self, interaction: Interaction, open_tag_name: str, closed_tag_name: str):
        if not self.config.get('forum_channel_id'):
            await interaction.response.send_message("Please set the recruitment channel first.", ephemeral=True); return
        try:
            forum_channel = await interaction.guild.fetch_channel(int(self.config['forum_channel_id']))
            open_tag = nextcord.utils.get(forum_channel.available_tags, name=open_tag_name)
            closed_tag = nextcord.utils.get(forum_channel.available_tags, name=closed_tag_name)
            if not open_tag or not closed_tag:
                await interaction.response.send_message(f"Could not find tags named `{open_tag_name}` and/or `{closed_tag_name}`.", ephemeral=True); return
            db.update_config(interaction.guild.id, {'open_tag_id': str(open_tag.id), 'closed_tag_id': str(closed_tag.id)})
            self.config = db.get_config(interaction.guild.id) or {}
            await interaction.response.send_message(f"✅ Tags configured.", ephemeral=True)
        except (nextcord.NotFound, nextcord.Forbidden):
            await interaction.response.send_message("Could not access the configured forum channel.", ephemeral=True)

    @recruit_admin.subcommand(name="set_asset_channel", description="Set a channel for storing permanent image assets like logos.")
    async def set_asset_channel(self, interaction: Interaction, channel: nextcord.TextChannel):
        """
        Sets the channel where the bot will upload images to get a permanent URL.
        """
        db.update_config(interaction.guild.id, {'asset_channel_id': str(channel.id)})
        self.config = db.get_config(interaction.guild.id) or {}
        await interaction.response.send_message(
            f"✅ Asset storage channel set to {channel.mention}. The bot will now re-upload logos there to create permanent links.",
            ephemeral=True
        )

    # --- TASKS ---

    @tasks.loop(minutes=30)
    async def scheduled_deletion_task(self):
        await self.bot.wait_until_ready()
        now = get_unix_time()
        for item in db.get_due_deletions(now):
            try:
                channel = await self.bot.fetch_channel(int(item['channel_id']))
                message = await channel.fetch_message(int(item['message_id']))
                await message.delete()
            except (nextcord.NotFound, nextcord.Forbidden): pass
            finally: db.remove_scheduled_deletion(int(item['message_id']))

    @tasks.loop(hours=24)
    async def weekly_reminder_task(self):
        await self.bot.wait_until_ready()
        now = get_unix_time()
        for thread_data in db.get_all_open_threads():
            if thread_data.get('last_reminder_sent_timestamp'): continue
            if (now - thread_data['creation_timestamp']) > WEEK_IN_SECONDS:
                try:
                    thread = await self.bot.fetch_channel(int(thread_data['thread_id']))
                    op = await thread.guild.fetch_member(int(thread_data['op_id']))
                    await thread.send(f"{op.mention}, is this post still active? It will be auto-closed in 12 hours if there is no response.", view=WeeklyReminderView(thread.id))
                    db.update_reminder_timestamp(thread.id, now)
                    await self.refresh_manager_panel(thread)
                except Exception as e:
                    logger.warning(f"Could not send reminder to thread {thread_data['thread_id']}: {e}")
                    db.update_thread_status(thread_data['thread_id'], is_closed=True)

    @tasks.loop(hours=1)
    async def inactivity_check_task(self):
        await self.bot.wait_until_ready()
        now = get_unix_time()
        for thread_data in db.get_all_open_threads():
            if not (remind_ts := thread_data.get('last_reminder_sent_timestamp')): continue
            if (now - remind_ts) > REMINDER_TIMEOUT_SECONDS:
                try:
                    thread = await self.bot.fetch_channel(int(thread_data['thread_id']))
                except (nextcord.NotFound, nextcord.Forbidden):
                    db.update_thread_status(thread_data['thread_id'], is_closed=True)
                    continue
                try:
                    op = await thread.guild.fetch_member(int(thread_data['op_id']))
                    await self.update_thread_state(thread, is_closing=True)
                    await thread.send(f"{op.mention}, this post has been automatically closed due to inactivity.")
                    await self.refresh_manager_panel(thread)
                except Exception as e:
                    logger.error(f"Failed to auto-close thread {thread_data['thread_id']}: {e}")

def setup(bot):
    bot.add_cog(RecruitmentForumManager(bot))