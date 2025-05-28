import nextcord
from nextcord.ext import commands, application_checks
import json
import logging
import os # For path joining

# Path to the JSON file - assumes it's in the same directory as main.py
# Adjust if your bot's root directory is structured differently when running.
# For example, if main.py is in /app and cogs are in /app/cogs, this should be fine.
RESPONSES_FILE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'auto_responses.json')
# This constructs a path like ../auto_responses.json relative to the cog file.

class AutoResponderCog(commands.Cog, name="Auto Responder"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.auto_responses = []
        self.load_responses()

    def load_responses(self):
        try:
            # Ensure the path is correct based on your project structure
            # If main.py is in the root and cogs/ is a subdir, this should work.
            # If you run main.py from a different working directory, this path might need adjustment
            # or use an absolute path if known, or pass root_dir from main.py to the cog.
            # For now, assuming main.py is in project root.
            
            # Corrected path assuming cogs folder is one level down from project root where auto_responses.json is
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            responses_file = os.path.join(project_root, 'auto_responses.json')

            if not os.path.exists(responses_file):
                logging.warning(f"Auto-responses file not found at: {responses_file}. Creating an empty one.")
                # Create a default empty list or example structure if file doesn't exist
                with open(responses_file, 'w') as f:
                    json.dump([
                        {
                            "triggers": ["example trigger"], 
                            "response": "This is an example response from a newly created file!", 
                            "case_sensitive": False, 
                            "match_type": "exact"
                        }
                    ], f, indent=2)
                # Then load it
                with open(responses_file, 'r', encoding='utf-8') as f:
                    self.auto_responses = json.load(f)

            else: # File exists, load it
                with open(responses_file, 'r', encoding='utf-8') as f:
                    self.auto_responses = json.load(f)
                logging.info(f"Successfully loaded {len(self.auto_responses)} auto-response rule(s) from {responses_file}.")

        except FileNotFoundError:
            logging.error(f"CRITICAL: Auto-responses file not found at {responses_file}. Auto-responder will not work.")
            self.auto_responses = []
        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON from {responses_file}. Auto-responder will not work. Please check the file for syntax errors.")
            self.auto_responses = []
        except Exception as e:
            logging.error(f"An unexpected error occurred loading auto-responses: {e}", exc_info=True)
            self.auto_responses = []

    @commands.Cog.listener()
    async def on_message(self, message: nextcord.Message):
        if message.author.bot:  # Ignore messages from bots (including self)
            return
        
        # For single-server bot, ensure message is from the target guild
        if hasattr(self.bot, 'target_guild_id') and self.bot.target_guild_id:
            if message.guild is None or message.guild.id != self.bot.target_guild_id:
                return # Not in the target guild

        msg_content = message.content
        
        for entry in self.auto_responses:
            triggers = entry.get("triggers", [])
            response_text = entry.get("response")
            case_sensitive = entry.get("case_sensitive", False) # Default to case-insensitive
            match_type = entry.get("match_type", "exact") # Default to exact match

            if not response_text or not triggers:
                continue

            text_to_check = msg_content if case_sensitive else msg_content.lower()

            triggered = False
            for trigger in triggers:
                current_trigger = trigger if case_sensitive else trigger.lower()
                if match_type == "exact":
                    if text_to_check == current_trigger:
                        triggered = True
                        break
                elif match_type == "contains":
                    if current_trigger in text_to_check:
                        triggered = True
                        break
            
            if triggered:
                # Replace placeholders
                formatted_response = response_text.replace("{user_mention}", message.author.mention)
                formatted_response = formatted_response.replace("{user_name}", message.author.name)
                formatted_response = formatted_response.replace("{user_display_name}", message.author.display_name)
                
                try:
                    await message.channel.send(formatted_response)
                    logging.info(f"Auto-responded to '{msg_content}' from {message.author.name} with '{response_text}'")
                except nextcord.Forbidden:
                    logging.warning(f"Missing permissions to send auto-response in channel {message.channel.id}")
                except Exception as e:
                    logging.error(f"Error sending auto-response: {e}", exc_info=True)
                return # Stop processing after the first match

    @nextcord.slash_command(name="reload_autoresponses", description="Reloads auto-response phrases from the JSON file.")
    @application_checks.has_permissions(manage_guild=True) # Or a more specific admin permission
    async def reload_autoresponses_command(self, interaction: nextcord.Interaction):
        # Cog check for target guild will apply
        await interaction.response.defer(ephemeral=True)
        self.load_responses()
        await interaction.followup.send(f"Reloaded {len(self.auto_responses)} auto-response rule(s).", ephemeral=True)


def setup(bot: commands.Bot):
    bot.add_cog(AutoResponderCog(bot))