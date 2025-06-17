import nextcord
from nextcord.ext import commands
import os
import logging
from dotenv import load_dotenv
from db_utils import database # Assuming this is your main DB util, not specific to invites
from typing import Optional

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(name)s - [%(module)s.%(funcName)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# --- Load Environment Variables ---
load_dotenv()
BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
TARGET_GUILD_ID_STR = os.getenv("TARGET_GUILD_ID") # Get TARGET_GUILD_ID as string

if not BOT_TOKEN:
    logging.error("FATAL: DISCORD_BOT_TOKEN not found in .env file. Exiting.")
    exit()

TARGET_GUILD_ID_INT: Optional[int] = None
if TARGET_GUILD_ID_STR:
    try:
        TARGET_GUILD_ID_INT = int(TARGET_GUILD_ID_STR)
    except ValueError:
        logging.error("FATAL: TARGET_GUILD_ID in .env is not a valid integer. Exiting.")
        exit()
else:
    logging.error("FATAL: TARGET_GUILD_ID not found in .env file. This is required for guild-specific command registration. Exiting.")
    exit()

# --- Bot Intents and Initialization ---
intents = nextcord.Intents.default()
intents.guilds = True
intents.messages = True
intents.message_content = True
intents.presences = True
intents.members = True
intents.invites = True

# Custom Bot class to hold target guild information
class SingleServerBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # target_guild_id is already passed via default_guild_ids to the superclass constructor
        # We can still store it for easy access if needed, or rely on default_guild_ids[0]
        self.target_guild_id: Optional[int] = kwargs.get('default_guild_ids', [None])[0]
        self.target_guild_name: Optional[str] = None

# Initialize bot with default_guild_ids
# This tells Nextcord to register all slash commands from all cogs to this specific guild by default.
bot = SingleServerBot(command_prefix="!", intents=intents, default_guild_ids=[TARGET_GUILD_ID_INT])

# --- Cog Loading ---
INITIAL_EXTENSIONS = [
    'cogs.config_cog',
    'cogs.ticket_manager_cog',
    'cogs.thread_exemption_cog',
    'cogs.status_monitor_cog',
    'cogs.announcement_cog',
    'cogs.invite_tracker_cog',
    'cogs.auto_responder_cog',
    'cogs.role_monitor_cog',
    'cogs.leveling_leaderboard_cog',
    'cogs.rainbow_role_cog',
    'cogs.suggestions_cog',
    'cogs.boost_tracker_cog'
]

@bot.event
async def on_ready():
    logging.info(f"Logged in as {bot.user.name} (ID: {bot.user.id})")
    logging.info(f"Nextcord Version: {nextcord.__version__}")

    if not bot.guilds:
        logging.error("CRITICAL: Bot is not in any servers!")
        return

    # Set the target_guild_name based on the target_guild_id
    # The target_guild_id is already set from default_guild_ids
    target_guild = bot.get_guild(bot.target_guild_id)
    if target_guild:
        bot.target_guild_name = target_guild.name
        logging.info(f"Target server identified: '{bot.target_guild_name}' (ID: {bot.target_guild_id})")
        if target_guild not in bot.guilds:
             logging.error(f"CRITICAL: Bot is not a member of the configured TARGET_GUILD_ID: {bot.target_guild_id}. Please check .env and invite the bot.")
             return
    else:
        logging.error(f"CRITICAL: Could not find target guild with ID {bot.target_guild_id} from .env. Ensure the bot is in this server.")
        return

    if len(bot.guilds) > 1:
        logging.warning(f"Bot is in {len(bot.guilds)} servers, but commands are specifically registered to '{bot.target_guild_name}'.")

    # Initialize the main database schema (if any)
    database.initialize_database() # Assuming this is for other DBs, not the invite cog's
    logging.info("Main database schema (ticket_bot_settings.db) checked/initialized.")

    # Check if this target guild has any main configuration (example)
    # settings = database.get_guild_settings(bot.target_guild_id) # Your main DB
    # if not settings:
    #     logging.warning(f"Target guild '{bot.target_guild_name}' has no core configuration in the 'settings' table.")

    print(f"----- BOT IS READY & TARGETING SERVER: {bot.target_guild_name} -----")

if __name__ == '__main__':
    # Load cogs
    for extension in INITIAL_EXTENSIONS:
        try:
            bot.load_extension(extension)
            logging.info(f'Successfully loaded extension: {extension}')
        except Exception as e:
            logging.error(f'Failed to load extension {extension}.', exc_info=True)

    # Run the bot
    bot.run(BOT_TOKEN)