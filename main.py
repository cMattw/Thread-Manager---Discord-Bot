import nextcord
from nextcord.ext import commands
import os
import logging
from dotenv import load_dotenv
from db_utils import database
from typing import Optional 

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(name)s - [%(module)s.%(funcName)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# --- Load Environment Variables ---
load_dotenv()
BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")

if not BOT_TOKEN:
    logging.error("FATAL: DISCORD_BOT_TOKEN not found in .env file. Please create .env and add it.")
    exit()

# --- Bot Intents and Initialization ---
intents = nextcord.Intents.default()
intents.guilds = True         # For guild information, on_ready guild list
intents.messages = True       # For message content (e.g. auto-responder)
intents.message_content = True # For message content (privileged)
intents.presences = True      # For on_presence_update (needed by StatusMonitorCog)
intents.members = True        # For member iteration and fetching (StatusMonitorCog, InviteTrackerCog initial scans)
intents.invites = True        # <<< For on_invite_create/delete and fetching invite details reliably

# Custom Bot class to hold target guild information
class SingleServerBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_guild_id: Optional[int] = None
        self.target_guild_name: Optional[str] = None 

bot = SingleServerBot(command_prefix="!", intents=intents) # command_prefix not used for slash, but good to have

# --- Cog Loading ---
INITIAL_EXTENSIONS = [
    'cogs.config_cog',          
    'cogs.ticket_manager_cog',  
    'cogs.thread_exemption_cog',
    'cogs.status_monitor_cog',  
    'cogs.announcement_cog',    
    'cogs.invite_tracker_cog',  # <<< Ensure this is listed
    'cogs.auto_responder_cog',  # Uncomment if you are using this
]

@bot.event
async def on_ready():
    logging.info(f"Logged in as {bot.user.name} (ID: {bot.user.id})")
    logging.info(f"Nextcord Version: {nextcord.__version__}")
    
    if len(bot.guilds) == 0:
        logging.error("CRITICAL: Bot is not in any servers! Please invite it to your target server.")
        return
    elif len(bot.guilds) > 1:
        logging.warning(f"Bot is in {len(bot.guilds)} servers. This bot is designed for single-server operation.")
        logging.warning(f"Targeting the first server in the list: {bot.guilds[0].name} (ID: {bot.guilds[0].id})")
        # Consider more robust handling or erroring out if strict single-server operation is required.
    
    # Set the target guild ID and name
    bot.target_guild_id = bot.guilds[0].id
    bot.target_guild_name = bot.guilds[0].name
    logging.info(f"Target server identified: '{bot.target_guild_name}' (ID: {bot.target_guild_id})")
    
    # Initialize the main database schema (ticket_bot_settings.db)
    # Individual cogs like InviteTrackerCog will initialize their own DBs if needed in their cog_load/on_ready
    database.initialize_database()
    logging.info("Main database schema (ticket_bot_settings.db) checked/initialized.")

    # Check if this target guild has any main configuration
    settings = database.get_guild_settings(bot.target_guild_id)
    if not settings:
        logging.warning(f"Target guild '{bot.target_guild_name}' has no core configuration in the 'settings' table. Admins should use /config commands.")
    
    print(f"----- BOT IS READY & TARGETING SERVER: {bot.target_guild_name} -----")

if __name__ == '__main__':
    # Load cogs
    for extension in INITIAL_EXTENSIONS:
        try:
            bot.load_extension(extension)
            logging.info(f'Successfully loaded extension: {extension}')
        except Exception as e:
            logging.error(f'Failed to load extension {extension}: {e}', exc_info=True) 
    
    # Run the bot
    bot.run(BOT_TOKEN)