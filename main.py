import nextcord
from nextcord.ext import commands
import os
import logging
from dotenv import load_dotenv
import database # Import to ensure initialize_database can be called

# --- Logging Setup ---
# Basic console logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# You can also add a file handler if desired
# file_handler = logging.FileHandler('bot.log')
# file_handler.setLevel(logging.INFO)
# file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s'))
# logging.getLogger().addHandler(file_handler)

# --- Load Environment Variables ---
load_dotenv()
BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")

if not BOT_TOKEN:
    logging.error("FATAL: DISCORD_BOT_TOKEN not found in .env file. Please create .env and add it.")
    exit()

# --- Bot Intents and Initialization ---
intents = nextcord.Intents.default()
intents.guilds = True
intents.messages = True       # For reading messages in threads (needed for "closed" phrase)
intents.message_content = True # PRIVILEGED INTENT - Enable in Developer Portal
# intents.members = True # If you need member information not directly available

# Use `commands.Bot` for more control, or `nextcord.Client` if no commands are needed (but we have slash commands)
bot = commands.Bot(intents=intents)

# --- Cog Loading ---
INITIAL_EXTENSIONS = [
    'cogs.config_cog',
    'cogs.ticket_manager_cog'
]

@bot.event
async def on_ready():
    logging.info(f'Logged in as {bot.user.name} (ID: {bot.user.id})')
    logging.info(f'Nextcord Version: {nextcord.__version__}')
    print("----- BOT IS READY -----")
    
    # Initialize database on startup
    database.initialize_database()
    logging.info("Database checked/initialized from main.")

    # Guild check and initial config prompt (optional)
    for guild in bot.guilds:
        settings = database.get_guild_settings(guild.id)
        if not settings:
            logging.warning(f"Guild '{guild.name}' (ID: {guild.id}) has no configuration. Admins should use /config commands.")
            # You could try to find an owner or admin channel to send a message, but console log is safer.

if __name__ == '__main__':
    for extension in INITIAL_EXTENSIONS:
        try:
            bot.load_extension(extension)
            logging.info(f'Successfully loaded extension: {extension}')
        except Exception as e:
            logging.error(f'Failed to load extension {extension}: {e}', exc_info=True)
    
    bot.run(BOT_TOKEN)
