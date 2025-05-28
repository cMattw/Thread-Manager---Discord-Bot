import nextcord
from nextcord.ext import commands
import os
import logging
from dotenv import load_dotenv
import database 
from typing import Optional 

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

load_dotenv()
BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")

if not BOT_TOKEN:
    logging.error("FATAL: DISCORD_BOT_TOKEN not found in .env file.")
    exit()

intents = nextcord.Intents.default()
intents.guilds = True        
intents.messages = True       
intents.message_content = True 
intents.presences = True      
intents.members = True        

class SingleServerBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_guild_id: Optional[int] = None
        self.target_guild_name: Optional[str] = None 

bot = SingleServerBot(command_prefix="!", intents=intents)

INITIAL_EXTENSIONS = [
    'cogs.config_cog',
    'cogs.ticket_manager_cog',
    'cogs.thread_exemption_cog',
    'cogs.status_monitor_cog',
    'cogs.announcement_cog',
    'cogs.auto_responder_cog', 
]

@bot.event
async def on_ready():
    logging.info(f"Logged in as {bot.user.name} (ID: {bot.user.id})")
    logging.info(f"Nextcord Version: {nextcord.__version__}")
    
    if len(bot.guilds) == 0:
        logging.error("CRITICAL: Bot is not in any servers! Please invite it.")
        return
    elif len(bot.guilds) > 1:
        logging.warning(f"Bot is in {len(bot.guilds)} servers. Designed for single-server. Using first: {bot.guilds[0].name} (ID: {bot.guilds[0].id})")
    
    bot.target_guild_id = bot.guilds[0].id
    bot.target_guild_name = bot.guilds[0].name
    logging.info(f"Target server identified: '{bot.target_guild_name}' (ID: {bot.target_guild_id})")
    
    database.initialize_database()
    logging.info("Database schema checked/initialized.")

    settings = database.get_guild_settings(bot.target_guild_id)
    if not settings:
        logging.warning(f"Target guild '{bot.target_guild_name}' has no core configuration in 'settings' table. Use /config commands.")
    
    print(f"----- BOT IS READY & TARGETING SERVER: {bot.target_guild_name} -----")

if __name__ == '__main__':
    for extension in INITIAL_EXTENSIONS:
        try:
            bot.load_extension(extension)
            logging.info(f'Successfully loaded extension: {extension}')
        except Exception as e:
            logging.error(f'Failed to load extension {extension}: {e}', exc_info=True) 
    
    bot.run(BOT_TOKEN)