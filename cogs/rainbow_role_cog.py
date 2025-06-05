# cogs/rainbow_role_cog.py

import nextcord
from nextcord.ext import commands, tasks
from nextcord import Interaction, SlashOption, Embed, Color, Role, Guild, Member, Permissions
import sqlite3
import os
import logging
import json
import time
import asyncio
from typing import Optional, List, Dict, Any, Union, Tuple

# --- Configuration ---
# !!! SET THIS VALUE FOR YOUR BOT !!!
# This should ideally be loaded from the bot's main config or environment variables
# For this example, we'll define it here as per the context of a single-server bot.
TARGET_GUILD_ID = 992662612401725502  # YOUR SERVER ID

# --- Database Path Logic (adapted from LevelingLeaderboardCog) ---
DEFAULT_DB_FILENAME = "rainbow_role.db"
# Adjust these paths as needed for your environment
DEV_DB_PATH_DIR = "/home/mattw/Projects/discord_ticket_manager/data/" # Example directory
PROD_DB_PATH_DIR = "/home/container/data/" # Example directory

# Determine which base DB directory to use
db_base_dir = PROD_DB_PATH_DIR if os.path.exists(PROD_DB_PATH_DIR) and os.path.isdir(PROD_DB_PATH_DIR) else DEV_DB_PATH_DIR

# Ensure the chosen base directory exists
if db_base_dir and not os.path.exists(db_base_dir):
    try:
        os.makedirs(db_base_dir, exist_ok=True)
        print(f"INFO: RainbowRoleCog: Created directory for database: {db_base_dir}")
    except Exception as e:
        print(f"ERROR: RainbowRoleCog: Could not create directory {db_base_dir}: {e}")
        db_base_dir = "" # Fallback to current directory if creation fails

DB_PATH = os.path.join(db_base_dir, DEFAULT_DB_FILENAME)
if not db_base_dir: # If db_base_dir is empty (e.g. creation failed or not specified as absolute)
    DB_PATH = DEFAULT_DB_FILENAME

# --- Logging Setup ---
logger = logging.getLogger('nextcord.rainbow_role_cog')
logger.setLevel(logging.INFO)
if not logger.handlers:
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

# --- Constants ---
DEFAULT_UPDATE_INTERVAL = 60  # seconds
MIN_UPDATE_INTERVAL = 10      # seconds (to prevent API abuse)
MAX_UPDATE_INTERVAL = 3600    # seconds (1 hour)
DEFAULT_HUE_INCREMENT = 0.03  # For HSV rainbow speed
MAX_ERROR_COUNT_BEFORE_DISABLE = 5

PRESET_PALETTES: Dict[str, List[int]] = {
    "vibrant_rainbow": [
        0xFF0000, 0xFF7F00, 0xFFFF00, 0x00FF00, 0x0000FF, 0x4B0082, 0x8B00FF
    ],
    "pastel_dream": [
        0xFFB6C1, 0xFFE4E1, 0xFAFAD2, 0xADD8E6, 0xAFEEEE, 0x98FB98, 0xDDA0DD
    ],
    "ocean_breeze": [
        0x00FFFF, 0x7FFFD4, 0x40E0D0, 0x20B2AA, 0x008080, 0x5F9EA0
    ],
    "forest_whisper": [
        0x228B22, 0x006400, 0x90EE90, 0x3CB371, 0x8FBC8F, 0x556B2F
    ],
    "fire_and_ice": [
        0xFF4500, 0xFF8C00, 0xFFFF00, 0x00FFFF, 0x1E90FF, 0xADD8E6
    ]
}

# --- Database Helper Class ---
class RainbowRoleDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        logger.info(f"RainbowRoleDatabase will be initialized at: {os.path.abspath(self.db_path)}")
        self._create_tables()

    def _get_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _create_tables(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Stores configuration for each role being managed
            # Corrected DEFAULT values by embedding them directly into the DDL string
            # using an f-string.
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS managed_roles (
                    role_id INTEGER PRIMARY KEY,
                    guild_id INTEGER NOT NULL,
                    is_enabled BOOLEAN DEFAULT TRUE NOT NULL,
                    update_interval_seconds INTEGER DEFAULT {DEFAULT_UPDATE_INTERVAL} NOT NULL,
                    color_mode TEXT DEFAULT 'hsv_rainbow' NOT NULL, 
                    palette_name TEXT,
                    custom_colors_json TEXT,
                    current_hue_or_index REAL DEFAULT 0.0 NOT NULL,
                    hue_increment_or_step REAL DEFAULT {DEFAULT_HUE_INCREMENT} NOT NULL,
                    last_updated_timestamp INTEGER DEFAULT 0 NOT NULL,
                    error_count INTEGER DEFAULT 0 NOT NULL
                )
            """
            # No parameters are passed to execute() for this DDL statement now
            cursor.execute(create_table_sql)
            conn.commit()
            logger.info("Database tables for RainbowRoleCog created/verified.")

    def add_role_config(self, guild_id: int, role_id: int, interval: int, mode: str,
                        hue_increment: float, palette_name: Optional[str] = None,
                        custom_colors_json: Optional[str] = None) -> bool:
        with self._get_connection() as conn:
            try:
                conn.execute("""
                    INSERT INTO managed_roles (guild_id, role_id, update_interval_seconds, color_mode, 
                                               palette_name, custom_colors_json, hue_increment_or_step, is_enabled)
                    VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
                """, (guild_id, role_id, interval, mode, palette_name, custom_colors_json, hue_increment))
                conn.commit()
                logger.info(f"DB: Added new rainbow config for role {role_id} in guild {guild_id}.")
                return True
            except sqlite3.IntegrityError:
                logger.warning(f"DB: Role {role_id} already has a rainbow config. Use update instead.")
                return False # Role already exists

    def get_role_config(self, role_id: int) -> Optional[Dict[str, Any]]:
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM managed_roles WHERE role_id = ?", (role_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_enabled_roles_for_guild(self, guild_id: int) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM managed_roles WHERE guild_id = ? AND is_enabled = TRUE", (guild_id,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def update_role_setting(self, role_id: int, settings_to_update: Dict[str, Any]) -> bool:
        if not settings_to_update:
            return False
        set_clauses = []
        params = []
        for key, value in settings_to_update.items():
            # Basic validation to prevent injection, ensure key is a valid column name
            if key not in ["is_enabled", "update_interval_seconds", "color_mode", "palette_name",
                           "custom_colors_json", "current_hue_or_index", "hue_increment_or_step",
                           "error_count", "last_updated_timestamp"]:
                logger.error(f"DB: Invalid key '{key}' for update_role_setting.")
                continue
            set_clauses.append(f"{key} = ?")
            params.append(value)

        if not set_clauses:
            return False

        params.append(role_id)
        query = f"UPDATE managed_roles SET {', '.join(set_clauses)} WHERE role_id = ?"

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, tuple(params))
            conn.commit()
            updated = cursor.rowcount > 0
            if updated:
                logger.info(f"DB: Updated settings for role {role_id}: {settings_to_update}")
            return updated

    def update_role_color_state_and_timestamp(self, role_id: int, new_hue_or_index: float, timestamp: int) -> bool:
        return self.update_role_setting(role_id, {
            "current_hue_or_index": new_hue_or_index,
            "last_updated_timestamp": timestamp
        })

    def increment_role_error_count(self, role_id: int) -> int:
        config = self.get_role_config(role_id)
        if config:
            new_error_count = config["error_count"] + 1
            self.update_role_setting(role_id, {"error_count": new_error_count})
            logger.debug(f"DB: Incremented error count for role {role_id} to {new_error_count}")
            return new_error_count
        return 0

    def reset_role_error_count(self, role_id: int):
        self.update_role_setting(role_id, {"error_count": 0})
        logger.debug(f"DB: Reset error count for role {role_id}")


    def remove_role_config(self, role_id: int) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM managed_roles WHERE role_id = ?", (role_id,))
            conn.commit()
            deleted = cursor.rowcount > 0
            if deleted:
                logger.info(f"DB: Removed rainbow config for role {role_id}.")
            return deleted

# --- Cog Class ---
class RainbowRoleCog(commands.Cog, name="RainbowRole"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = RainbowRoleDatabase(DB_PATH)
        self.target_guild_id_int = TARGET_GUILD_ID # Ensure this is set correctly
        self.rainbow_update_loop.start()
        logger.info("RainbowRoleCog initialized and task started.")

    def cog_unload(self):
        self.rainbow_update_loop.cancel()
        logger.info("RainbowRoleCog unloaded and task cancelled.")

    async def _get_guild(self) -> Optional[Guild]:
        guild = self.bot.get_guild(self.target_guild_id_int)
        if not guild:
            logger.error(f"Target guild {self.target_guild_id_int} not found by RainbowRoleCog.")
        return guild

    def _get_hsv_color(self, hue: float) -> Color:
        return Color.from_hsv(hue, 1.0, 1.0) # Full saturation and value for vibrant colors

    def _get_palette_color(self, index: int, palette_name: str) -> Optional[Color]:
        palette = PRESET_PALETTES.get(palette_name)
        if palette:
            actual_index = int(index) % len(palette)
            return Color(palette[actual_index])
        return None

    def _get_custom_list_color(self, index: int, custom_colors_json: str) -> Optional[Color]:
        try:
            colors_hex = json.loads(custom_colors_json)
            if isinstance(colors_hex, list) and colors_hex:
                actual_index = int(index) % len(colors_hex)
                return Color(int(str(colors_hex[actual_index]).replace("#",""), 16))
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            logger.error(f"Error parsing custom color list or color value: {e} - JSON: '{custom_colors_json}'")
        return None

    async def _calculate_next_color_for_role(self, role_config: Dict[str, Any]) -> Tuple[Optional[Color], float]:
        """Calculates the next color and the new hue/index."""
        mode = role_config["color_mode"]
        current_val = role_config["current_hue_or_index"]
        increment = role_config["hue_increment_or_step"]
        next_color: Optional[Color] = None
        next_val = current_val

        if mode == "hsv_rainbow":
            next_val = (current_val + increment) % 1.0
            next_color = self._get_hsv_color(next_val)
        elif mode == "palette_cycle":
            palette_name = role_config.get("palette_name")
            if palette_name and palette_name in PRESET_PALETTES:
                next_val = (current_val + 1) % len(PRESET_PALETTES[palette_name]) # Simple index increment
                next_color = self._get_palette_color(int(next_val), palette_name)
            else:
                logger.warning(f"Palette '{palette_name}' not found for role {role_config['role_id']}.")
        elif mode == "custom_list":
            custom_json = role_config.get("custom_colors_json")
            if custom_json:
                try:
                    color_list = json.loads(custom_json)
                    if color_list and isinstance(color_list, list):
                         next_val = (current_val + 1) % len(color_list) # Simple index increment
                         next_color = self._get_custom_list_color(int(next_val), custom_json)
                    else:
                        logger.warning(f"Custom color list is empty or invalid for role {role_config['role_id']}")
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode custom_colors_json for role {role_config['role_id']}")
            else:
                 logger.warning(f"Custom_colors_json not set for role {role_config['role_id']} in custom_list mode")


        return next_color, next_val

    @tasks.loop(seconds=15) # Check frequently, but honor individual role intervals
    async def rainbow_update_loop(self):
        guild = await self._get_guild()
        if not guild:
            return

        if not guild.me.guild_permissions.manage_roles:
            logger.warning(f"Bot lacks 'Manage Roles' permission in guild {guild.id}. Rainbow effect paused.")
            return

        active_roles_config = self.db.get_all_enabled_roles_for_guild(guild.id)
        current_time = int(time.time())

        for config in active_roles_config:
            role_id = config["role_id"]
            interval = config["update_interval_seconds"]
            last_updated = config["last_updated_timestamp"]

            if current_time - last_updated >= interval:
                role_to_edit = guild.get_role(role_id)
                if not role_to_edit:
                    logger.warning(f"Role {role_id} not found in guild {guild.id}. Disabling.")
                    self.db.update_role_setting(role_id, {"is_enabled": False})
                    continue

                if role_to_edit.position >= guild.me.top_role.position and guild.owner_id != self.bot.user.id : # Check hierarchy
                    logger.warning(f"Cannot manage role '{role_to_edit.name}' (ID: {role_id}) due to hierarchy. Disabling.")
                    self.db.update_role_setting(role_id, {"is_enabled": False}) # Auto-disable
                    continue
                
                if role_to_edit.is_integration() or role_to_edit.is_bot_managed() or role_to_edit.is_premium_subscriber() or role_to_edit.is_default():
                    logger.warning(f"Cannot manage special role '{role_to_edit.name}' (ID: {role_id}). Disabling.")
                    self.db.update_role_setting(role_id, {"is_enabled": False})
                    continue


                new_color, new_hue_or_index = await self._calculate_next_color_for_role(config)

                if new_color:
                    try:
                        await role_to_edit.edit(color=new_color, reason="Rainbow Role Effect")
                        self.db.update_role_color_state_and_timestamp(role_id, new_hue_or_index, current_time)
                        self.db.reset_role_error_count(role_id)
                        logger.debug(f"Successfully updated role {role_id} to color {new_color.value:06X}")
                    except nextcord.Forbidden:
                        logger.error(f"Forbidden to edit role {role_id}. Check permissions and hierarchy. Error count incremented.")
                        self.db.increment_role_error_count(role_id)
                    except nextcord.HTTPException as e:
                        logger.error(f"HTTPException while editing role {role_id}: {e}. Error count incremented.")
                        self.db.increment_role_error_count(role_id)
                    except Exception as e:
                        logger.error(f"Unexpected error editing role {role_id}: {e}", exc_info=True)
                        self.db.increment_role_error_count(role_id)
                    
                    current_error_count = self.db.get_role_config(role_id)["error_count"] # Re-fetch to get latest
                    if current_error_count >= MAX_ERROR_COUNT_BEFORE_DISABLE:
                        logger.warning(f"Role {role_id} reached max error count ({current_error_count}). Disabling rainbow effect.")
                        self.db.update_role_setting(role_id, {"is_enabled": False})
                else:
                    logger.warning(f"Could not calculate next color for role {role_id} with mode {config['color_mode']}. Skipping update.")
                    # Consider incrementing error count here too or specific handling.
            await asyncio.sleep(0.1) # Small sleep to prevent tight loop if many roles have same interval

    @rainbow_update_loop.before_loop
    async def before_rainbow_update_loop(self):
        await self.bot.wait_until_ready()
        logger.info("RainbowRoleCog: Update loop is ready to start.")

    # --- Slash Commands ---
    @nextcord.slash_command(name="rainbowrole", description="Manage rainbow role effects.", guild_ids=[TARGET_GUILD_ID])
    async def rainbowrole_group(self, interaction: Interaction):
        pass # This is the base group, subcommands will be attached

    @rainbowrole_group.subcommand(name="add", description="Add a role to the rainbow effect.")
    @commands.has_permissions(manage_guild=True)
    async def add_role(self, interaction: Interaction,
                       role: Role = SlashOption(description="The role to make rainbow.", required=True),
                       mode: str = SlashOption(
                           description="Color cycling mode.",
                           choices={"HSV Rainbow": "hsv_rainbow", "Palette Cycle": "palette_cycle", "Custom Color List": "custom_list"},
                           default="hsv_rainbow", required=False
                       ),
                       interval: int = SlashOption(
                           description=f"Update frequency in seconds (Min: {MIN_UPDATE_INTERVAL}, Max: {MAX_UPDATE_INTERVAL}).",
                           default=DEFAULT_UPDATE_INTERVAL, required=False,
                           min_value=MIN_UPDATE_INTERVAL, max_value=MAX_UPDATE_INTERVAL
                       ),
                       speed_or_palette: Optional[str] = SlashOption(
                           name="speed_or_palette_name",
                           description="For HSV: Speed (0.01-0.2). For Palette: Palette Name. (See /rainbowrole listpalettes)",
                           required=False
                       ),
                       custom_colors: Optional[str] = SlashOption(
                           description="For Custom List: Comma-separated HEX colors (e.g., #FF0000,#00FF00).",
                           required=False
                       )):
        await interaction.response.defer(ephemeral=True)

        if not interaction.guild:
            await interaction.followup.send("This command must be used in a server.", ephemeral=True)
            return
        
        if role.position >= interaction.guild.me.top_role.position and interaction.guild.owner_id != self.bot.user.id:
            await interaction.followup.send(f"I cannot manage the role '{role.name}' as it is higher than or equal to my highest role.", ephemeral=True)
            return
        if not interaction.guild.me.guild_permissions.manage_roles:
            await interaction.followup.send("I lack the 'Manage Roles' permission.", ephemeral=True)
            return
        if role.is_integration() or role.is_bot_managed() or role.is_premium_subscriber() or role.is_default():
            await interaction.followup.send(f"The role '{role.name}' is a special role and cannot be managed by this feature.", ephemeral=True)
            return

        existing_config = self.db.get_role_config(role.id)
        if existing_config:
            await interaction.followup.send(f"Role {role.mention} is already configured. Use `/rainbowrole edit` to modify.", ephemeral=True)
            return

        hue_increment = DEFAULT_HUE_INCREMENT
        palette_name_to_save = None
        custom_colors_json_to_save = None

        if mode == "hsv_rainbow":
            if speed_or_palette:
                try:
                    hue_increment = float(speed_or_palette)
                    if not (0.001 <= hue_increment <= 0.5): # Wider range for speed
                        await interaction.followup.send("HSV speed must be between 0.001 and 0.5.", ephemeral=True)
                        return
                except ValueError:
                    await interaction.followup.send("Invalid speed format for HSV mode. Expected a number (e.g., 0.05).", ephemeral=True)
                    return
        elif mode == "palette_cycle":
            if not speed_or_palette or speed_or_palette not in PRESET_PALETTES:
                await interaction.followup.send(f"Invalid or missing palette name. Use `/rainbowrole listpalettes` to see options.", ephemeral=True)
                return
            palette_name_to_save = speed_or_palette
            hue_increment = 1 # For list-based modes, this means "next item"
        elif mode == "custom_list":
            if not custom_colors:
                await interaction.followup.send("Custom color list is required for 'Custom Color List' mode.", ephemeral=True)
                return
            try:
                colors_list = [c.strip() for c in custom_colors.split(',')]
                if not all(c.startswith("#") and len(c) == 7 for c in colors_list): # Basic HEX validation
                     for c_val in colors_list:
                        try: int(c_val.replace("#",""), 16)
                        except ValueError:
                            await interaction.followup.send(f"Invalid HEX color format in custom list: '{c_val}'. Example: #FF00AA", ephemeral=True); return
                if not colors_list:
                     await interaction.followup.send("Custom color list cannot be empty.", ephemeral=True); return

                custom_colors_json_to_save = json.dumps(colors_list)
                hue_increment = 1 # For list-based modes
            except Exception as e:
                await interaction.followup.send(f"Error processing custom colors: {e}", ephemeral=True)
                return
        
        success = self.db.add_role_config(
            guild_id=interaction.guild.id, role_id=role.id, interval=interval, mode=mode,
            hue_increment=hue_increment, palette_name=palette_name_to_save,
            custom_colors_json=custom_colors_json_to_save
        )

        if success:
            await interaction.followup.send(f"Rainbow effect added for {role.mention} with mode '{mode}' and interval {interval}s.", ephemeral=True)
        else:
            await interaction.followup.send(f"Failed to add rainbow effect for {role.mention}. It might already be configured.", ephemeral=True)

    @rainbowrole_group.subcommand(name="remove", description="Remove a role from the rainbow effect.")
    @commands.has_permissions(manage_guild=True)
    async def remove_role(self, interaction: Interaction,
                          role: Role = SlashOption(description="The role to remove from rainbow effect.", required=True)):
        await interaction.response.defer(ephemeral=True)
        if not interaction.guild:
             await interaction.followup.send("This command must be used in a server.", ephemeral=True); return

        config = self.db.get_role_config(role.id)
        if not config:
            await interaction.followup.send(f"{role.mention} is not currently configured for rainbow effect.", ephemeral=True)
            return

        if self.db.remove_role_config(role.id):
            # Optionally, try to reset the role color to its original or default here if desired
            await interaction.followup.send(f"Rainbow effect removed from {role.mention}.", ephemeral=True)
        else:
            await interaction.followup.send(f"Failed to remove rainbow effect from {role.mention}.", ephemeral=True)

    @rainbowrole_group.subcommand(name="list", description="List roles currently configured for rainbow effect.")
    @commands.has_permissions(manage_guild=True) # Or lower if desired
    async def list_roles(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        if not interaction.guild:
            await interaction.followup.send("This command must be used in a server.", ephemeral=True); return

        configs = self.db.get_all_enabled_roles_for_guild(interaction.guild.id) # Or all configs, then filter display
        if not configs:
            await interaction.followup.send("No roles are currently configured for the rainbow effect.", ephemeral=True)
            return

        embed = Embed(title="Rainbow Role Configurations", color=Color.blurple())
        for config in configs:
            role = interaction.guild.get_role(config["role_id"])
            role_mention = role.mention if role else f"ID: {config['role_id']} (Not Found)"
            status = "Enabled" if config["is_enabled"] else "Disabled"
            details = (
                f"Mode: `{config['color_mode']}`\n"
                f"Interval: `{config['update_interval_seconds']}s`\n"
                f"Status: `{status}`"
            )
            if config['color_mode'] == 'hsv_rainbow':
                details += f"\nSpeed (Hue Inc): `{config['hue_increment_or_step']}`"
            elif config['color_mode'] == 'palette_cycle':
                details += f"\nPalette: `{config['palette_name']}`"
            # Custom list details can be long, maybe just indicate it's set
            embed.add_field(name=f"{role_mention}", value=details, inline=False)
        
        if not embed.fields: # Should not happen if configs exist, but as a fallback
             await interaction.followup.send("No active rainbow role configurations found.", ephemeral=True)
             return
        await interaction.followup.send(embed=embed, ephemeral=True)

    @rainbowrole_group.subcommand(name="toggle", description="Toggle the rainbow effect for a configured role.")
    @commands.has_permissions(manage_guild=True)
    async def toggle_role(self, interaction: Interaction,
                           role: Role = SlashOption(description="The role to toggle.", required=True)):
        await interaction.response.defer(ephemeral=True)
        if not interaction.guild:
             await interaction.followup.send("This command must be used in a server.", ephemeral=True); return

        config = self.db.get_role_config(role.id)
        if not config:
            await interaction.followup.send(f"{role.mention} is not configured. Use `/rainbowrole add` first.", ephemeral=True)
            return

        new_status = not config["is_enabled"]
        if self.db.update_role_setting(role.id, {"is_enabled": new_status}):
            status_text = "ENABLED" if new_status else "DISABLED"
            await interaction.followup.send(f"Rainbow effect for {role.mention} has been {status_text}.", ephemeral=True)
            if new_status: # Reset error count when re-enabling
                self.db.reset_role_error_count(role.id)
        else:
            await interaction.followup.send(f"Failed to toggle rainbow effect for {role.mention}.", ephemeral=True)

    @rainbowrole_group.subcommand(name="edit", description="Edit the configuration for a rainbow role.")
    @commands.has_permissions(manage_guild=True)
    async def edit_role_config(self, interaction: Interaction,
                               role: Role = SlashOption(description="The role to edit.", required=True),
                               mode: Optional[str] = SlashOption(
                                   description="New color cycling mode.",
                                   choices={"HSV Rainbow": "hsv_rainbow", "Palette Cycle": "palette_cycle", "Custom Color List": "custom_list"},
                                   required=False
                               ),
                               interval: Optional[int] = SlashOption(
                                   description=f"New update frequency (Min: {MIN_UPDATE_INTERVAL}, Max: {MAX_UPDATE_INTERVAL}).",
                                   required=False, min_value=MIN_UPDATE_INTERVAL, max_value=MAX_UPDATE_INTERVAL
                               ),
                               speed_or_palette: Optional[str] = SlashOption(
                                   name="new_speed_or_palette_name",
                                   description="New HSV speed (0.01-0.2) or Palette Name.",
                                   required=False
                               ),
                               custom_colors: Optional[str] = SlashOption(
                                   description="New comma-separated HEX colors for Custom List mode.",
                                   required=False
                               ),
                               enabled: Optional[bool] = SlashOption(description="Set enabled state (True/False).", required=False)):
        await interaction.response.defer(ephemeral=True)
        if not interaction.guild:
            await interaction.followup.send("This command must be used in a server.", ephemeral=True); return

        config = self.db.get_role_config(role.id)
        if not config:
            await interaction.followup.send(f"{role.mention} is not configured. Use `/rainbowrole add` first.", ephemeral=True)
            return

        updates: Dict[str, Any] = {}
        response_messages = []

        if mode is not None:
            updates["color_mode"] = mode
            updates["current_hue_or_index"] = 0.0 # Reset progress when mode changes
            response_messages.append(f"Mode set to `{mode}`.")
            # If mode changes, dependent fields need re-evaluation
            current_mode_for_logic = mode
        else:
            current_mode_for_logic = config["color_mode"]


        if interval is not None:
            updates["update_interval_seconds"] = interval
            response_messages.append(f"Interval set to `{interval}s`.")

        if enabled is not None:
            updates["is_enabled"] = enabled
            response_messages.append(f"Status set to `{'Enabled' if enabled else 'Disabled'}`.")
            if enabled: self.db.reset_role_error_count(role.id) # Reset errors if manually re-enabled


        # Handle mode-dependent parameters (speed_or_palette, custom_colors)
        # This logic needs to be careful if mode itself is being changed in the same command
        if current_mode_for_logic == "hsv_rainbow":
            if speed_or_palette is not None: # This field is for HSV speed here
                try:
                    h_inc = float(speed_or_palette)
                    if not (0.001 <= h_inc <= 0.5):
                        await interaction.followup.send("HSV speed must be between 0.001 and 0.5.", ephemeral=True); return
                    updates["hue_increment_or_step"] = h_inc
                    response_messages.append(f"HSV speed (hue increment) set to `{h_inc}`.")
                except ValueError:
                    await interaction.followup.send("Invalid speed format for HSV. Expected a number.", ephemeral=True); return
            # Clear palette/custom if switching to HSV
            if mode == "hsv_rainbow": # if mode was explicitly changed to hsv
                updates["palette_name"] = None
                updates["custom_colors_json"] = None


        elif current_mode_for_logic == "palette_cycle":
            if speed_or_palette is not None: # This field is for palette name here
                if speed_or_palette not in PRESET_PALETTES:
                    await interaction.followup.send(f"Invalid palette name: '{speed_or_palette}'. Use `/rainbowrole listpalettes`.", ephemeral=True); return
                updates["palette_name"] = speed_or_palette
                updates["hue_increment_or_step"] = 1 # Index step
                response_messages.append(f"Palette set to `{speed_or_palette}`.")
            # Clear custom_colors if switching to palette
            if mode == "palette_cycle":
                updates["custom_colors_json"] = None

        elif current_mode_for_logic == "custom_list":
            if custom_colors is not None: # This field is for custom colors
                try:
                    colors_list = [c.strip() for c in custom_colors.split(',')]
                    if not all(c.startswith("#") and len(c) == 7 for c in colors_list):
                         for c_val in colors_list:
                            try: int(c_val.replace("#",""), 16)
                            except ValueError:
                                await interaction.followup.send(f"Invalid HEX color: '{c_val}'", ephemeral=True); return
                    if not colors_list:
                         await interaction.followup.send("Custom color list cannot be empty.", ephemeral=True); return
                    updates["custom_colors_json"] = json.dumps(colors_list)
                    updates["hue_increment_or_step"] = 1 # Index step
                    response_messages.append(f"Custom color list updated ({len(colors_list)} colors).")
                except Exception as e:
                    await interaction.followup.send(f"Error processing custom colors: {e}", ephemeral=True); return
            # Clear palette if switching to custom
            if mode == "custom_list":
                updates["palette_name"] = None


        if not updates:
            await interaction.followup.send("No changes specified.", ephemeral=True)
            return

        if self.db.update_role_setting(role.id, updates):
            final_response = f"Configuration for {role.mention} updated:\n" + "\n".join(f"- {msg}" for msg in response_messages)
            await interaction.followup.send(final_response, ephemeral=True)
        else:
            await interaction.followup.send(f"Failed to update configuration for {role.mention}.", ephemeral=True)


    @rainbowrole_group.subcommand(name="listpalettes", description="Lists available preset color palettes.")
    async def list_palettes(self, interaction: Interaction):
        embed = Embed(title="Available Preset Palettes", color=Color.gold())
        if not PRESET_PALETTES:
            embed.description = "No preset palettes are currently defined."
        else:
            for name, colors in PRESET_PALETTES.items():
                color_squares = " ".join([f"`{hex(c)}`" for c in colors[:5]]) # Show first 5 colors
                if len(colors) > 5: color_squares += " ..."
                embed.add_field(name=name, value=f"Colors: {color_squares}", inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)


def setup(bot: commands.Bot):
    # Ensure the target guild ID is an integer if it comes from elsewhere
    global TARGET_GUILD_ID
    try:
        TARGET_GUILD_ID = int(TARGET_GUILD_ID)
    except ValueError:
        logger.critical(f"CRITICAL: TARGET_GUILD_ID ('{TARGET_GUILD_ID}') for RainbowRoleCog is not a valid integer. Cog will not load.")
        return
        
    bot.add_cog(RainbowRoleCog(bot))
    logger.info("RainbowRoleCog has been loaded.")