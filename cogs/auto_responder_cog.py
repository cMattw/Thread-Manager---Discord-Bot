import nextcord
from nextcord.ext import commands, application_checks # Added application_checks for slash command permissions
from nextcord import Interaction, SlashOption # Added Interaction, SlashOption
import json
import logging
import os
from typing import List, Dict, Any, Optional # For type hinting

# Path to the JSON file - assumes auto_responses.json is in the bot's root project directory
# This path goes up one level from the cogs/ directory to the project root.
RESPONSES_FILE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'auto_responses.json')

class AutoResponderCog(commands.Cog, name="Auto Responder"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.auto_responses: List[Dict[str, Any]] = []
        self.load_responses()

    def load_responses(self):
        """Loads auto-response rules from the JSON file."""
        try:
            if not os.path.exists(RESPONSES_FILE_PATH):
                logging.warning(f"Auto-responses file not found at: {RESPONSES_FILE_PATH}. Creating an empty example file.")
                # Create a default empty list or example structure if file doesn't exist
                with open(RESPONSES_FILE_PATH, 'w', encoding='utf-8') as f:
                    json.dump([
                        {
                            "triggers": ["example trigger", "another example"], 
                            "response": "This is an example response from a newly created file, {user_mention}!", 
                            "case_sensitive": False, 
                            "match_type": "exact"
                        },
                        {
                            "triggers": ["loaded cogs"],
                            "response": "{loaded_cogs_list}",
                            "case_sensitive": False,
                            "match_type": "exact"
                        }
                    ], f, indent=2)
                # Then load it
                with open(RESPONSES_FILE_PATH, 'r', encoding='utf-8') as f:
                    self.auto_responses = json.load(f)
            else: # File exists, load it
                with open(RESPONSES_FILE_PATH, 'r', encoding='utf-8') as f:
                    self.auto_responses = json.load(f)
            
            logging.info(f"AutoResponderCog: Successfully loaded {len(self.auto_responses)} auto-response rule(s) from {RESPONSES_FILE_PATH}.")

        except FileNotFoundError: # Should be caught by os.path.exists, but as a fallback
            logging.error(f"CRITICAL: Auto-responses file NOT FOUND at {RESPONSES_FILE_PATH} after attempting to create. Auto-responder will not work.")
            self.auto_responses = []
        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON from {RESPONSES_FILE_PATH}. Auto-responder will not work. Please check the file for syntax errors.")
            self.auto_responses = []
        except Exception as e:
            logging.error(f"An unexpected error occurred loading auto-responses: {e}", exc_info=True)
            self.auto_responses = []

    async def cog_check(self, interaction: Interaction) -> bool:
        # Cog check for slash commands: ensure they are used in the target guild if bot is single-server configured
        if hasattr(self.bot, 'target_guild_id') and self.bot.target_guild_id:
            if interaction.guild is None or interaction.guild.id != self.bot.target_guild_id:
                if not interaction.response.is_done():
                    try: await interaction.response.defer(ephemeral=True)
                    except nextcord.NotFound: pass 
                target_guild_name = getattr(self.bot, 'target_guild_name', 'the configured server')
                await interaction.followup.send(f"This bot is configured for a specific server. Please use commands in '{target_guild_name}'.", ephemeral=True)
                return False
        return True

    @commands.Cog.listener()
    async def on_message(self, message: nextcord.Message):
        if message.author.bot:
            return
        
        # For single-server bot, ensure message is from the target guild
        if hasattr(self.bot, 'target_guild_id') and self.bot.target_guild_id:
            if message.guild is None or message.guild.id != self.bot.target_guild_id:
                return 

        msg_content = message.content
        
        for entry in self.auto_responses:
            triggers: List[str] = entry.get("triggers", [])
            response_text: Optional[str] = entry.get("response")
            case_sensitive: bool = entry.get("case_sensitive", False) 
            match_type: str = entry.get("match_type", "exact") 

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
                # Can add more match_types here like "startswith", "endswith"
            
            if triggered:
                final_response_message = ""

                if response_text == "{loaded_cogs_list}":
                    if hasattr(self.bot, 'extensions') and self.bot.extensions:
                        cog_names = [name.split('.')[-1] for name in self.bot.extensions.keys()]
                        if cog_names:
                            final_response_message = f"✅ Currently loaded cogs: `{', '.join(sorted(cog_names))}`."
                        else:
                            final_response_message = "🤔 No cogs appear to be loaded (which is strange if this one is running!)."
                    else:
                        final_response_message = "⚠️ Could not retrieve the list of loaded cogs at the moment."
                else:
                    final_response_message = response_text.replace("{user_mention}", message.author.mention)
                    final_response_message = final_response_message.replace("{user_name}", message.author.name)
                    final_response_message = final_response_message.replace("{user_display_name}", message.author.display_name)
                
                try:
                    if final_response_message: # Ensure there's something to send
                        await message.channel.send(final_response_message)
                        logging.info(f"AutoResponderCog: Responded to '{msg_content}' from {message.author.name} in {message.channel.name}.")
                except nextcord.Forbidden:
                    logging.warning(f"AutoResponderCog: Missing permissions to send auto-response in channel {message.channel.id} for guild {message.guild.id if message.guild else 'DM'}")
                except Exception as e:
                    logging.error(f"AutoResponderCog: Error sending auto-response: {e}", exc_info=True)
                return # Stop processing further auto-responses for this message

    @nextcord.slash_command(name="reload_autoresponses", description="Reloads auto-response phrases from the JSON file (Admin).")
    @application_checks.has_permissions(manage_guild=True) 
    async def reload_autoresponses_command(self, interaction: Interaction):
        # cog_check will ensure this is run in the target guild
        await interaction.response.defer(ephemeral=True)
        
        previous_count = len(self.auto_responses)
        self.load_responses()
        new_count = len(self.auto_responses)
        
        await interaction.followup.send(f"Reloaded auto-responses. Found {new_count} rule(s) (previously {previous_count}).", ephemeral=True)
        logging.info(f"AutoResponderCog: Auto-responses reloaded by {interaction.user.name}. Found {new_count} rules.")


def setup(bot: commands.Bot):
    bot.add_cog(AutoResponderCog(bot))