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

from db_utils import recruitment_database as db

logger = logging.getLogger('nextcord.recruitment_forum_cog')

def get_unix_time(offset_seconds: int = 0) -> int:
    return int((datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)).timestamp())

# --- CONSTANTS ---
APPLICATION_DELETION_SECONDS = 10800
WEEK_IN_SECONDS = 604800
REMINDER_TIMEOUT_SECONDS = 43200

# --- UI FOR CREATION FLOW ---

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
        try:
            msg = await self.bot.wait_for("message", timeout=180.0, check=lambda m: m.author == interaction.user and m.channel == interaction.channel and m.attachments)
            logo = msg.attachments[0]
            if not logo.content_type or not logo.content_type.startswith("image/"):
                await interaction.edit_original_message(content="That was not an image. Process cancelled.", view=None)
                return
            await msg.delete()
            final_embed = Embed(title=f"{self.title}", description=f"__**Member Requirements**__\n{self.requirements}", color=Color.blue()).set_image(url=logo.url)
            await interaction.edit_original_message(content="This is the final preview.", embed=final_embed, view=FinalSubmitView(self.bot, self.cog, self.title, self.requirements, logo_url=logo.url))
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

# --- UI FOR POST MANAGEMENT ---

class EditPostModal(ui.Modal):
    def __init__(self, cog: "RecruitmentForumManager", thread_id: int, current_title: str, current_reqs: str):
        super().__init__("Edit Recruitment Post", timeout=600)
        self.cog = cog
        self.thread_id = thread_id
        self.team_name = ui.TextInput(label="Team Name (Post Title)", style=TextInputStyle.short, default_value=current_title, required=True, max_length=90)
        self.add_item(self.team_name)
        self.requirements = ui.TextInput(label="Member Requirements", style=TextInputStyle.paragraph, default_value=current_reqs, required=True, max_length=2000)
        self.add_item(self.requirements)
    async def callback(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        main_post = await self.cog.get_main_post_message(self.thread_id)
        if not main_post: return await interaction.followup.send("Could not find the original post to edit.", ephemeral=True)
        original_content, members_section, logo_url_line = main_post.content, "", ""
        if "\n\n**Team Members:**\n" in original_content:
            members_section = "**Team Members:**\n" + original_content.split("\n\n**Team Members:**\n", 1)[1]
        for line in original_content.split('\n'):
            s_line = line.strip()
            if s_line.startswith("http"):
                logo_url_line = s_line
                if logo_url_line in members_section: members_section = members_section.replace(logo_url_line, "").strip()
                break
        new_content = f"## {self.team_name.value}\n\n**Member Requirements:**\n{self.requirements.value}"
        if members_section: new_content += f"\n\n{members_section}"
        if logo_url_line and logo_url_line not in new_content: new_content += f"\n\n{logo_url_line}"
        thread, forum_channel = main_post.channel, main_post.channel.parent
        try:
            await main_post.delete()
            webhook = await forum_channel.create_webhook(name=f"Recruiter {interaction.user.display_name[:30]}")
            new_main_post_message = await webhook.send(thread=thread, content=new_content, username=interaction.user.display_name, avatar_url=interaction.user.display_avatar.url, wait=True)
            await webhook.delete()
            db.update_main_post_id(self.thread_id, new_main_post_message.id)
            if isinstance(thread, nextcord.Thread) and thread.name != self.team_name.value: await thread.edit(name=self.team_name.value)
            await interaction.followup.send("Your recruitment post has been updated.", ephemeral=True)
        except Exception as e:
            logger.error(f"Failed to edit post for thread {self.thread_id}: {e}", exc_info=True)
            await interaction.followup.send("An unexpected error occurred while updating the post.", ephemeral=True)

class EditLogoView(ui.View):
    def __init__(self, cog, thread_id: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.thread_id = thread_id
    async def _edit_logo_url(self, interaction: Interaction, new_url: Optional[str]):
        main_post = await self.cog.get_main_post_message(self.thread_id)
        if not main_post:
            await interaction.followup.send("Could not find post to edit.", ephemeral=True); return
        content_lines = main_post.content.split('\n')
        new_content_lines = [line for line in content_lines if not (line.strip().startswith("http"))]
        if new_url: new_content_lines.append(f"\n{new_url}")
        final_content = "\n".join(new_content_lines)
        thread, forum_channel = main_post.channel, main_post.channel.parent
        try:
            await main_post.delete()
            webhook = await forum_channel.create_webhook(name=f"Recruiter {interaction.user.display_name[:30]}")
            new_main_post_message = await webhook.send(thread=thread, content=final_content, username=interaction.user.display_name, avatar_url=interaction.user.display_avatar.url, wait=True)
            await webhook.delete()
            db.update_main_post_id(self.thread_id, new_main_post_message.id)
            await interaction.followup.send("Logo has been updated successfully!", ephemeral=True)
        except Exception as e:
            logger.error(f"Failed to edit logo for thread {self.thread_id}: {e}")
            await interaction.followup.send("An error occurred while editing the logo.", ephemeral=True)
    @ui.button(label="Upload New Logo", style=ButtonStyle.primary)
    async def upload_logo(self, button: ui.Button, interaction: Interaction):
        await interaction.response.send_message("Please upload your new team logo now.", ephemeral=True)
        try:
            msg = await self.cog.bot.wait_for("message", timeout=180.0, check=lambda m: m.author == interaction.user and m.channel == interaction.channel and m.attachments)
            logo = msg.attachments[0]
            if not logo.content_type or not logo.content_type.startswith("image/"):
                await interaction.followup.send("That was not an image.", ephemeral=True); return
            await self._edit_logo_url(interaction, logo.url)
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
            await interaction.followup.send("Team members list updated.", ephemeral=True)
        except Exception as e:
            logger.error(f"Failed to update members for thread {self.thread_id}: {e}")
            await interaction.followup.send("An error occurred.", ephemeral=True)

class ManagerPanelView(ui.View):
    def __init__(self, cog: "RecruitmentForumManager", thread_id: int, main_post_url: str, is_closed: bool = False, team_name: str = "the Team"):
        super().__init__(timeout=None)
        self.cog = cog
        self.thread_id = thread_id

        if is_closed:
            reopen_button = ui.Button(label="Reopen", emoji="🔓", style=ButtonStyle.green)
            reopen_button.callback = self.reopen_callback
            self.add_item(reopen_button)
        else:
            close_button = ui.Button(emoji="🔒", style=ButtonStyle.grey)
            close_button.callback = self.close_callback
            self.add_item(close_button)

            edit_button = ui.Button(emoji="✏️", style=ButtonStyle.grey)
            edit_button.callback = self.edit_post_callback
            self.add_item(edit_button)

            logo_button = ui.Button(emoji="🖼️", style=ButtonStyle.grey)
            logo_button.callback = self.edit_logo_callback
            self.add_item(logo_button)

            members_button = ui.Button(emoji="👥", style=ButtonStyle.grey)
            members_button.callback = self.update_members_callback
            self.add_item(members_button)

            button_team_name = (team_name[:20] + '…') if len(team_name) > 22 else team_name
            apply_button = ui.Button(label=f"Join {button_team_name}", emoji="🤝", style=ButtonStyle.blurple)
            apply_button.callback = self.apply_callback
            self.add_item(apply_button)

        if main_post_url:
            self.add_item(ui.Button(label="Back to Top", emoji="⬆️", style=ButtonStyle.link, url=main_post_url))

    async def _check_permissions(self, interaction: Interaction) -> bool:
        thread, thread_data = await self.cog._get_thread_data(self.thread_id)
        if not thread or not thread_data or interaction.user.id != int(thread_data['op_id']):
            await interaction.response.send_message("Post not found or you lack permissions.", ephemeral=True)
            return False
        return True

    # --- CALLBACKS FOR THE BUTTONS ---
    
    async def close_callback(self, interaction: Interaction):
        if not await self._check_permissions(interaction): return
        thread, _ = await self.cog._get_thread_data(self.thread_id)
        await interaction.response.defer(ephemeral=True)
        await self.cog.update_thread_state(thread, is_closing=True)
        await interaction.followup.send("Post closed.", ephemeral=True)

    async def reopen_callback(self, interaction: Interaction):
        if not await self._check_permissions(interaction): return
        thread, _ = await self.cog._get_thread_data(self.thread_id)
        await interaction.response.defer(ephemeral=True)
        await self.cog.update_thread_state(thread, is_closing=False)
        await interaction.followup.send("Post reopened.", ephemeral=True)

    async def edit_post_callback(self, interaction: Interaction):
        if not await self._check_permissions(interaction): return
        main_post = await self.cog.get_main_post_message(self.thread_id)
        if not main_post: return await interaction.response.send_message("Could not find the original post to edit.", ephemeral=True)
        content_lines, title, req_lines, in_reqs = main_post.content.split('\n'), "", [], False
        for line in content_lines:
            s_line = line.strip()
            if s_line.startswith("## "): title = s_line[3:].strip(); continue
            if s_line.lower() == "**member requirements:**": in_reqs = True; continue
            if in_reqs and (s_line.startswith('**') or s_line.startswith('http')): in_reqs = False
            if in_reqs: req_lines.append(line)
        await interaction.response.send_modal(EditPostModal(self.cog, self.thread_id, title, "\n".join(req_lines).strip()))

    async def edit_logo_callback(self, interaction: Interaction):
        if not await self._check_permissions(interaction): return
        await interaction.response.send_message("How would you like to change the logo?", view=EditLogoView(self.cog, self.thread_id), ephemeral=True)

    async def update_members_callback(self, interaction: Interaction):
        if not await self._check_permissions(interaction): return
        main_post = await self.cog.get_main_post_message(self.thread_id)
        current_members_text = ""
        if main_post and "\n\n**Team Members:**\n" in main_post.content:
            current_members_text = main_post.content.split("\n\n**Team Members:**\n", 1)[1].split("\n\nhttp")[0].replace("• ", "")
        await interaction.response.send_modal(UpdateMembersModal(self.cog, self.thread_id, current_members_text))

    async def apply_callback(self, interaction: Interaction):
        thread, thread_data = await self.cog._get_thread_data(self.thread_id)
        if not thread or not thread_data: return await interaction.response.send_message("This recruitment post could not be found.", ephemeral=True)
        if interaction.user.id == int(thread_data['op_id']): return await interaction.response.send_message("You cannot apply to your own post.", ephemeral=True)
        main_post = await self.cog.get_main_post_message(self.thread_id)
        team_name = main_post.content.split('\n', 1)[0][3:].strip() if main_post and main_post.content.startswith("## ") else "this team"
        await interaction.response.send_modal(ApplicationModal(self.cog, self.thread_id, team_name))

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
    def __init__(self, applicant: Member):
        super().__init__(timeout=None)
        self.children[0].custom_id = f"recman_app:accept:{applicant.id}"
        self.children[1].custom_id = f"recman_app:deny:{applicant.id}"
    @ui.button(label="Accept", style=ButtonStyle.green)
    async def accept(self, button: ui.Button, interaction: Interaction): pass
    @ui.button(label="Deny", style=ButtonStyle.red)
    async def deny(self, button: ui.Button, interaction: Interaction): pass

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
        thread = interaction.channel
        main_post = await self.cog.get_main_post_message(thread.id)
        team_name = "a team"
        if main_post:
            first_line = main_post.content.split('\n', 1)[0]
            if first_line.startswith("## "): team_name = first_line[3:].strip()
        action_past_tense = "Accepted" if self.action == 'accept' else "Denied"
        deletion_timestamp = get_unix_time(APPLICATION_DELETION_SECONDS)
        desc = (f"{applicant.mention}, your application to join **{team_name}** has been {self.action}ed by {interaction.user.mention}."
                f"\n\n-# This message will be deleted <t:{deletion_timestamp}:R>")
        embed = Embed(title=f"Application {action_past_tense}!", color=Color.green() if self.action == 'accept' else Color.red(), description=desc)
        if self.admin_message.value: embed.add_field(name="Message from Admin", value=self.admin_message.value, inline=False)
        result_msg = await thread.send(content=applicant.mention, embed=embed)
        db.add_scheduled_deletion(result_msg.id, thread.id, deletion_timestamp)
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
        self.session = aiohttp.ClientSession()
        guild_id = self.bot.guilds[0].id if self.bot.guilds else None
        if guild_id:
            db.initialize_database(guild_id)
            self.config = db.get_config(guild_id) or {}
            self.initialized = True
            logger.info("RecruitmentForumManager Cog Initialized.")
        else: logger.error("RecruitmentForumManager could not initialize: Bot is not in any guilds.")

    def cog_unload(self):
        self.scheduled_deletion_task.cancel()
        self.weekly_reminder_task.cancel()
        self.inactivity_check_task.cancel()
        if self.session: self.bot.loop.create_task(self.session.close())

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

    async def update_thread_state(self, thread: Thread, is_closing: bool):
        open_tag, closed_tag = await self.get_tags(thread.guild)
        if not open_tag or not closed_tag: return
        current_tags = thread.applied_tags
        if is_closing:
            new_tags = [tag for tag in current_tags if tag.id != open_tag.id] + [closed_tag]
            await thread.edit(locked=True, archived=True, applied_tags=list(set(new_tags)))
            db.update_thread_status(thread.id, is_closed=True)
        else:
            new_tags = [tag for tag in current_tags if tag.id != closed_tag.id] + [open_tag]
            await thread.edit(locked=False, archived=False, applied_tags=list(set(new_tags)))
            db.update_thread_status(thread.id, is_closed=False)
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
        post_content = f"## {title}\n\n**Member Requirements:**\n{requirements}"
        if logo_url: post_content += f"\n\n{logo_url}"
        try:
            thread: Thread = await forum_channel.create_thread(name=title, content=f"'{title}' is recruiting! (Posted by: {interaction.user.mention})", applied_tags=[open_tag])
            webhook = await forum_channel.create_webhook(name=f"Recruiter {interaction.user.display_name[:30]}")
            main_post_message = await webhook.send(thread=thread, content=post_content, username=interaction.user.display_name, avatar_url=interaction.user.display_avatar.url, wait=True)
            await webhook.delete()
            main_post_url = self._get_message_link(interaction.guild.id, thread.id, main_post_message.id)
            panel_view = ManagerPanelView(thread.id, main_post_url, is_closed=False, team_name=title)
            desc = (f"`🔒` **Close Recruitment:** Locks this post.\n`✏️` **Edit Post:** Re-opens the modal.\n`🖼️` **Edit/Add Logo:** Change or remove the logo.\n`👥` **Update Members:** Edit the list of team members.\n🤝 **Join {title}:** Submit an application.\n`⬆️` **Back to Top:** Jumps to the top of the post.")
            panel_embed = Embed(title="Recruitment Manager Panel", description=desc, color=Color.dark_grey())
            panel_message = await thread.send(embed=panel_embed, view=panel_view)
            db.add_managed_thread(thread.id, interaction.user.id, main_post_message.id, panel_message.id, get_unix_time())
            return True, main_post_message.jump_url
        except Exception as e:
            logger.error(f"Post creation failed: {e}", exc_info=True)
            return False, "An unexpected error occurred."

    @commands.Cog.listener()
    async def on_message(self, message: Message):
        if message.author.bot or not message.guild or not isinstance(message.channel, Thread): return
        thread_data = db.get_managed_thread(message.channel.id)
        if not thread_data: return
        async with self.panel_refresh_lock:
            try:
                if panel_id := thread_data.get('manager_panel_message_id'):
                    await (await message.channel.fetch_message(int(panel_id))).delete()
            except (nextcord.NotFound, nextcord.Forbidden): pass
            try:
                main_post = await self.get_main_post_message(message.channel.id)
                if not main_post: return
                first_line = main_post.content.split('\n', 1)[0]
                team_name = first_line[3:].strip() if first_line.startswith("## ") else "Team"
                is_closed = thread_data['is_closed'] == 1
                main_post_url = self._get_message_link(message.guild.id, main_post.channel.id, main_post.id)
                view = ManagerPanelView(message.channel.id, main_post_url, is_closed, team_name)
                if is_closed:
                    desc, color = "This recruitment post is currently closed.", Color.red()
                else:
                    desc = (f"`🔒` **Close Recruitment:** Locks this post.\n`✏️` **Edit Post:** Re-opens the modal.\n`🖼️` **Edit/Add Logo:** Change or remove the logo.\n`👥` **Update Members:** Edit the list of team members.\n🤝 **Join {team_name}:** Submit an application.\n`⬆️` **Back to Top:** Jumps to the top of the post.")
                    color = Color.dark_grey()
                embed = Embed(title="Recruitment Manager Panel", description=desc, color=color)
                new_panel = await message.channel.send(embed=embed, view=view)
                db.update_thread_panel_id(message.channel.id, new_panel.id)
            except Exception as e:
                logger.error(f"Failed to resend manager panel in {message.channel.id}: {e}", exc_info=True)
    
    @nextcord.slash_command(name="recruitment")
    async def recruitment(self, interaction: Interaction): pass

    @recruitment.subcommand(name="create", description="Create a new recruitment post.")
    async def create(self, interaction: Interaction):
        if not await self.system_check(interaction): return
        open_post = next((t for t in db.get_user_threads(interaction.user.id) if t['is_closed'] == 0), None)
        if open_post:
            thread_link = f"https://discord.com/channels/{interaction.guild.id}/{open_post['thread_id']}"
            await interaction.response.send_message(f"❌ You already have an open recruitment post: {thread_link}", ephemeral=True); return
        await interaction.response.send_message("**Recruitment Guidelines**...", view=GuidelineView(self.bot, self), ephemeral=True)

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
        await interaction.response.send_autocomplete({k: v for k, v in choices.items() if post.lower() in k.lower()})

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
                    await thread.send(f"{op.mention}, is this post still active?", view=WeeklyReminderView(thread.id))
                    db.update_reminder_timestamp(thread.id, now)
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
                    op = await thread.guild.fetch_member(int(thread_data['op_id']))
                    await self.update_thread_state(thread, is_closing=True)
                    await thread.send(f"{op.mention}, this post has been automatically closed due to inactivity.")
                except Exception as e:
                    logger.error(f"Failed to auto-close thread {thread_data['thread_id']}: {e}")

def setup(bot):
    bot.add_cog(RecruitmentForumManager(bot))