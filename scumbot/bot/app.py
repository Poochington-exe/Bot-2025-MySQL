# ==========================================================
# SCUMBot – Discord Bot (app.py)
#
# Surgical patch set applied:
#   - Standardise SERVER_SETTINGS cache key to "location" (keeps "server_location" as a backwards-compatible alias)
#   - Fix PvP/Leaderboard modals sending description=None (now "")
#   - Fix PvP/Leaderboard titles to avoid double-prefixing (pass plain titles)
#   - Fix channel mention formatting (use f"<#{id}>", remove .replace hacks)
#   - Make fetch_all/execute safe if db_pool is not initialised
#   - Convert apply_bounties_for_kill to proper aiomysql async implementation
#   - Remove duplicated status interval constant drift (use UPDATE_STATUS_TIMER only)
#   - Ensure create_scumbot_embed wrapper falls back correctly to cached location
#   - Add __main__ runner (client.run)
# ==========================================================

import asyncio
import logging
import math
import os
import re
import secrets
import string
from datetime import datetime, timedelta

import aiomysql
import discord
import requests
from discord import app_commands
from discord.ext import commands

from ..db import create_db_pool
from ..updater import run_updater_loop  # background loop
from ..utils.embeds import (
    create_scumbot_embed as utils_create_scumbot_embed,
    set_bot_settings as utils_set_bot_settings,
)
from ..utils.flags import get_flag_url as utils_get_flag_url

logger = logging.getLogger("bot")

# ---------------- Configuration ----------------

SCUM_ORANGE = discord.Color.from_rgb(222, 133, 0)

BOT_SETTINGS: dict = {}
SERVER_SETTINGS: dict = {}

WANTED_ROLE_NAME = "WANTED"

# Presence update interval (seconds)
UPDATE_STATUS_TIMER = 20

# Dev/test guild (optional)
GUILD_ID = 847327487058247712

intents = discord.Intents.default()
intents.members = True
intents.guilds = True

client = commands.Bot(command_prefix="!", intents=intents)
db_pool: aiomysql.Pool | None = None

CHANNEL_ID_RE = re.compile(r"\d+")
LAST_ADMIN_TRACK_ID: dict[int, int] = {}

# ==========================================================
# Bounty helpers (WANTED role + target resolver + slash modal)
# ==========================================================


async def ensure_wanted_role(guild: discord.Guild | None, discord_id: int | None):
    """
    Ensure the WANTED role exists and is applied to the given member.
    Tries get_member first, then fetch_member so it also works for offline members.
    Best-effort: failures are logged but never raised.
    """
    if guild is None or discord_id is None:
        return

    member = guild.get_member(discord_id)
    if member is None:
        try:
            member = await guild.fetch_member(discord_id)
        except discord.NotFound:
            logger.warning(f"[WANTED] Member {discord_id} not found in guild {guild.id}.")
            return
        except discord.Forbidden:
            logger.warning(f"[WANTED] Missing perms to fetch member {discord_id} in guild {guild.id}.")
            return
        except Exception as e:
            logger.error(f"[WANTED] Error fetching member {discord_id} in guild {guild.id}: {e}")
            return

    role = discord.utils.get(guild.roles, name=WANTED_ROLE_NAME)
    if role is None:
        try:
            role = await guild.create_role(
                name=WANTED_ROLE_NAME,
                colour=discord.Color.red(),
                reason="SCUMBot bounty system – WANTED marker",
            )
        except Exception as e:
            logger.info(f"[WANTED] Failed to create WANTED role in guild {guild.id}: {e}")
            return
    else:
        if role.color != discord.Color.red():
            try:
                await role.edit(colour=discord.Color.red())
            except Exception as e:
                logger.info(f"[WANTED] Could not recolor WANTED role: {e}")

    if role not in member.roles:
        try:
            await member.add_roles(role, reason="SCUMBot bounty placed on this player")
        except Exception as e:
            logger.info(f"[WANTED] Failed to add WANTED role to {member.id} in guild {guild.id}: {e}")


async def resolve_scum_target(conn, guild_id: int, raw: str):
    """
    Resolve a bounty target from free text into (steam_id, username, discord_id).
    Supports:
      - Discord mention (@User) or Discord ID
      - SteamID (long numeric)
      - player_id (short numeric)
      - partial username match
    Returns (None, None, None) if no match.
    """
    if not raw:
        return None, None, None

    raw = raw.strip()

    def parse_discord_id(value: str):
        value = value.strip()
        if value.startswith("<@") and value.endswith(">"):
            value = value[2:-1]
            if value.startswith("!"):
                value = value[1:]
        return int(value) if value.isdigit() else None

    discord_id = parse_discord_id(raw)

    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # Case 1: Discord ID → player_statistics
            if discord_id is not None:
                await cur.execute(
                    """
                    SELECT steam_id, username
                    FROM player_statistics
                    WHERE guild_id=%s AND discord_id=%s
                    LIMIT 1
                    """,
                    (guild_id, discord_id),
                )
                row = await cur.fetchone()
                if row and row.get("steam_id"):
                    return str(row["steam_id"]), (row.get("username") or "Unknown"), discord_id

            # Case 2: pure digits (SteamID or player_id)
            if raw.isdigit():
                if len(raw) >= 12:
                    steam_id = raw
                    await cur.execute(
                        """
                        SELECT username, discord_id
                        FROM player_statistics
                        WHERE guild_id=%s AND steam_id=%s
                        LIMIT 1
                        """,
                        (guild_id, steam_id),
                    )
                    row = await cur.fetchone()
                    if row:
                        return (
                            steam_id,
                            row.get("username") or "Unknown",
                            int(row["discord_id"]) if row.get("discord_id") else None,
                        )
                else:
                    player_id = int(raw)
                    await cur.execute(
                        """
                        SELECT steam_id, username, discord_id
                        FROM player_statistics
                        WHERE guild_id=%s AND player_id=%s
                        LIMIT 1
                        """,
                        (guild_id, player_id),
                    )
                    row = await cur.fetchone()
                    if row and row.get("steam_id"):
                        return (
                            str(row["steam_id"]),
                            row.get("username") or "Unknown",
                            int(row["discord_id"]) if row.get("discord_id") else None,
                        )

            # Case 3: partial username search
            like_pattern = f"%{raw}%"
            await cur.execute(
                """
                SELECT steam_id, username, discord_id
                FROM player_statistics
                WHERE guild_id=%s AND username LIKE %s
                ORDER BY kills DESC, deaths ASC
                LIMIT 1
                """,
                (guild_id, like_pattern),
            )
            row = await cur.fetchone()
            if row and row.get("steam_id"):
                return (
                    str(row["steam_id"]),
                    row.get("username") or "Unknown",
                    int(row["discord_id"]) if row.get("discord_id") else None,
                )

    except Exception as e:
        logger.error(f"[BOUNTY] resolve_scum_target error (guild {guild_id}): {e}")

    return None, None, None


class SlashBountyModal(discord.ui.Modal, title="Place Bounty"):
    def __init__(self, guild_id: int, placed_by_discord_id: int, preset_target: str = ""):
        super().__init__()
        self.guild_id = guild_id
        self.placed_by_discord_id = placed_by_discord_id

        self.target_input = discord.ui.TextInput(
            label="Target (Discord @user / ID / SteamID / name)",
            placeholder="@Player or 7656... or name",
            required=True,
            max_length=120,
            default=preset_target,
        )
        self.amount_input = discord.ui.TextInput(
            label="Bounty amount",
            placeholder="e.g. 500",
            required=True,
            max_length=10,
        )
        self.reason_input = discord.ui.TextInput(
            label="Reason (optional)",
            style=discord.TextStyle.paragraph,
            required=False,
            max_length=200,
        )

        self.add_item(self.target_input)
        self.add_item(self.amount_input)
        self.add_item(self.reason_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This command can only be used inside a server.", ephemeral=True)
            return

        global db_pool
        if db_pool is None:
            await interaction.response.send_message("Database is not ready yet. Try again in a moment.", ephemeral=True)
            return

        guild_id = guild.id
        target_raw = str(self.target_input.value).strip()
        amount_str = str(self.amount_input.value).strip()
        reason = (self.reason_input.value or "").strip() or None

        try:
            amount = int(amount_str)
        except ValueError:
            await interaction.response.send_message("Amount must be a whole number (e.g. 500).", ephemeral=True)
            return

        if amount <= 0:
            await interaction.response.send_message("Amount must be a positive number.", ephemeral=True)
            return

        if amount > 1_000_000:
            await interaction.response.send_message("Maximum bounty amount is 1,000,000.", ephemeral=True)
            return

        caller_discord_id = self.placed_by_discord_id

        try:
            async with db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(
                        """
                        SELECT steam_id, cash, username
                        FROM player_statistics
                        WHERE guild_id=%s AND discord_id=%s
                        LIMIT 1
                        """,
                        (guild_id, caller_discord_id),
                    )
                    caller_row = await cur.fetchone()
                    if not caller_row or not caller_row.get("steam_id"):
                        embed = create_scumbot_embed(
                            guild_id=guild_id,
                            title="Bounty Error",
                            description=(
                                "You must be **registered** on this server to place a bounty.\n"
                                "Use `/register` first and make sure you've linked your SCUM profile."
                            ),
                            server_context=True,
                        )
                        await interaction.response.send_message(embed=embed, ephemeral=True)
                        return

                    caller_steam_id = str(caller_row["steam_id"])
                    caller_cash = int(caller_row.get("cash") or 0)

                    placement_fee = 100
                    total_cost = placement_fee + amount

                    if caller_cash < total_cost:
                        embed = create_scumbot_embed(
                            guild_id=guild_id,
                            title="Not enough cash",
                            description=(
                                f"You need `${total_cost}` cash to place this bounty "
                                f"(`{placement_fee}` placement fee + `{amount}` bounty)."
                            ),
                            server_context=True,
                        )
                        await interaction.response.send_message(embed=embed, ephemeral=True)
                        return

                    target_steam_id, target_username, target_discord_id = await resolve_scum_target(
                        conn, guild_id, target_raw
                    )

                    if not target_steam_id:
                        await interaction.response.send_message(
                            "❌ I couldn't find that player.\n"
                            "Try a Steam ID, player ID, Discord @mention/ID, or a more exact name.",
                            ephemeral=True,
                        )
                        return

                    if target_steam_id == caller_steam_id:
                        await interaction.response.send_message("❌ You can't place a bounty on yourself.", ephemeral=True)
                        return

                    await cur.execute(
                        """
                        UPDATE player_statistics
                        SET cash = cash - %s
                        WHERE guild_id=%s AND steam_id=%s
                        """,
                        (total_cost, guild_id, caller_steam_id),
                    )

                    await cur.execute(
                        """
                        INSERT INTO bounties
                            (guild_id, target_steam_id, target_username,
                             placed_by_discord_id, placed_by_steam_id,
                             amount, reason, status)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,'active')
                        """,
                        (
                            guild_id,
                            target_steam_id,
                            target_username,
                            caller_discord_id,
                            caller_steam_id,
                            amount,
                            reason,
                        ),
                    )

                    await cur.execute(
                        """
                        SELECT SUM(amount) AS total_amount
                        FROM bounties
                        WHERE guild_id=%s AND target_steam_id=%s AND status='active'
                        """,
                        (guild_id, target_steam_id),
                    )
                    sum_row = await cur.fetchone() or {}
                    total_reward_for_target = int(sum_row.get("total_amount") or 0)

                    await conn.commit()

        except Exception as e:
            logger.info(f"[BOUNTY] Slash bounty modal failed: {e}")
            await interaction.response.send_message(
                "❌ There was a problem placing the bounty. Try again later.",
                ephemeral=True,
            )
            return

        if target_discord_id:
            try:
                target_user = guild.get_member(target_discord_id) or await interaction.client.fetch_user(target_discord_id)
            except Exception:
                target_user = None

            if target_user:
                dm_desc = (
                    f"A new bounty has been placed on you in **{guild.name}**.\n\n"
                    f"**Target:** {target_username}\n"
                    f"**New bounty amount:** `${amount}`\n"
                    f"**Total active reward on your head:** `${total_reward_for_target}`\n\n"
                    f"While you have active bounties on this server you will be marked "
                    f"with the **{WANTED_ROLE_NAME}** role in Discord so everyone knows "
                    f"you are a WANTED prisoner."
                )
                if reason:
                    dm_desc += f"\n\n**Reason given:**\n> {reason}"

                dm_embed = create_scumbot_embed(
                    guild_id=guild_id,
                    title="You are now WANTED",
                    description=dm_desc,
                    server_context=True,
                )

                try:
                    await target_user.send(embed=dm_embed)
                except discord.Forbidden:
                    logger.warning(f"[BOUNTY] Could not DM bounty target {target_user.id} (forbidden).")
                except Exception as e:
                    logger.info(f"[BOUNTY] Failed to DM bounty target {target_user.id}: {e}")

                try:
                    await ensure_wanted_role(guild, target_discord_id)
                except Exception as e:
                    logger.error(
                        f"[BOUNTY] ensure_wanted_role failed (modal) for {target_discord_id} in guild {guild_id}: {e}"
                    )

        reason_line = f"\n**Reason:** {reason}" if reason else ""
        desc = (
            f"✅ Bounty placed on **{target_username}** (`{target_steam_id}`).\n"
            f"**Bounty amount:** `${amount}`.\n"
            f"**Placement fee:** `${placement_fee}`.\n"
            f"**Total cost deducted:** `${total_cost}`.\n"
            f"**Total reward now on their head:** `${total_reward_for_target}`."
            f"{reason_line}"
        )
        embed = create_scumbot_embed(
            guild_id=guild_id,
            title="Bounty placed",
            description=desc,
            server_context=True,
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)


# ==========================================================
# Database helpers
# ==========================================================


async def init_db_pool():
    """Initialize the global MySQL connection pool."""
    global db_pool
    if db_pool is None:
        db_pool = await create_db_pool(minsize=1, maxsize=5)
        logger.info("Database pool initialized.")


async def fetch_all(query: str, *params):
    """Run a SELECT query and return all rows as dicts."""
    global db_pool
    if db_pool is None:
        raise RuntimeError("Database pool not initialised")
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, params)
            return await cur.fetchall()


async def execute(query: str, *params):
    """Run a write query (INSERT/UPDATE/DELETE) and commit."""
    global db_pool
    if db_pool is None:
        raise RuntimeError("Database pool not initialised")
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, params)
            await conn.commit()


# ==========================================================
# Settings loaders (bot & per-guild server settings)
# ==========================================================


async def update_bot_status(bot: discord.Client, pool: aiomysql.Pool):
    """Periodically update the bot's Discord presence."""
    await bot.wait_until_ready()

    while not bot.is_closed():
        try:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT COUNT(*) AS c FROM login_logs WHERE status = 'logged in'")
                    row = await cur.fetchone()
                    online_players = int(row["c"]) if row and row.get("c") is not None else 0

            server_count = len(bot.guilds)

            activity = discord.Activity(
                type=discord.ActivityType.watching,
                name=f"{online_players} players in {server_count} servers",
            )
            await bot.change_presence(activity=activity)

        except Exception as e:
            logger.info(f"[STATUS] Failed to update bot presence: {e}")
            await asyncio.sleep(60)
            continue

        await asyncio.sleep(UPDATE_STATUS_TIMER)


async def load_bot_settings():
    """Load global bot settings from the database into BOT_SETTINGS."""
    global BOT_SETTINGS
    try:
        global db_pool
        if db_pool is None:
            raise RuntimeError("Database pool not initialised")

        async with db_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT * FROM bot_settings LIMIT 1")
                settings = await cur.fetchone()

        if not settings:
            logger.info("[WARNING] No bot_settings found.")
            BOT_SETTINGS = {}
            utils_set_bot_settings(BOT_SETTINGS)
            return

        BOT_SETTINGS = {
            "name": settings.get("bot_name", "SCUMBot"),
            "description": settings.get("bot_description", ""),
            "developer": settings.get("bot_developer", "Unknown"),
            "website": settings.get("bot_website", ""),
            "logo": settings.get("bot_logo", ""),
            "donation": settings.get("bot_donation", ""),
            "version": settings.get("bot_version", "v1.0.0"),
        }

        utils_set_bot_settings(BOT_SETTINGS)

        logger.info(
            "Bot settings loaded (name=%s, version=%s)",
            BOT_SETTINGS.get("name"),
            BOT_SETTINGS.get("version"),
        )
        logger.debug("Bot settings payload: %s", BOT_SETTINGS)
    except Exception as e:
        logger.error(f"[ERROR] Failed to load bot settings: {e}")
        BOT_SETTINGS = {}


async def load_server_settings():
    """
    Load per-guild server settings into SERVER_SETTINGS.

    Canonical cache key is "location".
    A backwards-compatible alias "server_location" is also stored.
    """
    global SERVER_SETTINGS
    new_settings: dict[int, dict] = {}

    try:
        rows = await fetch_all("SELECT * FROM server_settings")

        for row in rows:
            gid = int(row["guild_id"])
            loc = (row.get("server_location") or None)

            new_settings[gid] = {
                "guild_id": gid,
                "name": row.get("server_name", ""),
                "description": row.get("server_description", ""),
                "id": row.get("server_id", ""),  # BattleMetrics ID or URL

                # Canonical + alias (keeps older code safe)
                "location": loc,
                "server_location": loc,

                "discord_link": row.get("discord_link", ""),

                "post_chats": row.get("post_chats", 1),
                "post_logins": row.get("post_logins", 1),
                "post_kills": row.get("post_kills", 1),

                "chat_channel": row.get("chat_channel"),
                "logins_channel": row.get("logins_channel"),
                "kill_channel": row.get("kill_channel"),

                "post_admin": row.get("post_admin", 0),
                "admin_channel": row.get("admin_channel"),

                "post_steam_ban": row.get("post_steam_ban", 0),
                "steam_ban_channel": row.get("steam_ban_channel"),

                "post_bounties": row.get("post_bounties", 0),
                "bounty_channel": row.get("bounty_channel"),
                "bounty_message": row.get("bounty_message"),

                "post_sentries": row.get("post_sentries", 0),
                "sentry_channel": row.get("sentry_channel"),

                "post_pvp_board": row.get("post_pvp_board", 0),
                "pvp_channel": row.get("pvp_channel"),
                "pvp_period": row.get("pvp_period"),
                "pvp_prize": row.get("pvp_prize"),
                "pvp_payout": row.get("pvp_payout"),
                "pvp_message": row.get("pvp_message"),

                "post_leaderboard": row.get("post_leaderboard", 0),
                "leaderboard_channel": row.get("leaderboard_channel"),
                "leaderboard_message": row.get("leaderboard_message"),

                "owner": row.get("server_owner"),
                "track_admin": row.get("track_admin", 0),
            }

        SERVER_SETTINGS = new_settings
        logger.info("Server settings cached (servers=%s)", len(SERVER_SETTINGS))

    except Exception as e:
        logger.error(f"[ERROR] Failed to load server settings: {e}")
        SERVER_SETTINGS = {}


# ==========================================================
# /server command Helpers
# ==========================================================


def _fmt_int(v) -> str:
    try:
        return f"{int(v)}"
    except Exception:
        return "—"


def _fmt_players(players, max_players) -> str:
    if players is None and max_players is None:
        return "—"
    return f"{_fmt_int(players)}/{_fmt_int(max_players)}"


def _fmt_status(s: str | None) -> str:
    if not s:
        return "unknown"
    return str(s).strip().lower()


def _fmt_duration(seconds: int) -> str:
    if seconds < 0:
        seconds = 0
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h}h {m}m {s}s"


def _compute_next_restart_utc(now_utc: datetime, interval_minutes: int, anchor_utc: datetime | None) -> tuple[str, str]:
    if not interval_minutes or interval_minutes <= 0:
        return "—", "—"

    restart_text = f"Every {interval_minutes} min"
    if interval_minutes % 60 == 0:
        restart_text = f"Every {interval_minutes // 60} hour(s)"

    if not anchor_utc:
        return restart_text, "—"

    anchor = anchor_utc.replace(tzinfo=None)
    now = now_utc.replace(tzinfo=None)

    interval_s = interval_minutes * 60
    delta_s = (now - anchor).total_seconds()

    if delta_s <= 0:
        next_restart = anchor
    else:
        n = math.ceil(delta_s / interval_s)
        next_restart = anchor + timedelta(seconds=n * interval_s)

    remaining_s = int((next_restart - now).total_seconds())
    return restart_text, f"in {_fmt_duration(remaining_s)}"


async def _fetch_battlemetrics_server(server_id: str) -> dict | None:
    if not server_id:
        return None

    server_id = str(server_id).strip()
    if "/" in server_id:
        server_id = server_id.rstrip("/").split("/")[-1]

    url = f"https://api.battlemetrics.com/servers/{server_id}"

    def _do_request():
        r = requests.get(url, timeout=12)
        if r.status_code != 200:
            return None
        return r.json()

    return await asyncio.to_thread(_do_request)


def _bm_web_url(server_id: str) -> str | None:
    if not server_id:
        return None
    sid = str(server_id).strip()
    if "/" in sid:
        return sid
    return f"https://www.battlemetrics.com/servers/scum/{sid}"


# ==========================================================
# Flag / visual helpers
# ==========================================================


def get_flag_emoji(code: str) -> str:
    if not code or len(code) != 2:
        return "🌍"
    return chr(ord(code.upper()[0]) + 127397) + chr(ord(code.upper()[1]) + 127397)


def get_flag_url(flag: str | None) -> str | None:
    return utils_get_flag_url(flag)


def get_bot_avatar_url() -> str | None:
    if client.user:
        return client.user.display_avatar.url
    return None


def extract_channel_id(raw: str | None) -> int | None:
    if not raw:
        return None
    value = raw.strip()
    if not value:
        return None
    m = CHANNEL_ID_RE.search(value)
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None


# ==========================================================
# Unified embed factory
# ==========================================================


def create_scumbot_embed(
    *,
    title: str,
    description: str = "",
    guild_id: int | None = None,
    location: str | None = None,
    server_context: bool = True,
) -> discord.Embed:
    """
    Wrapper around utils_create_scumbot_embed.
    """
    desc = description or ""
    loc = (location or "").strip().upper()

    if not loc and server_context and guild_id is not None:
        cfg = SERVER_SETTINGS.get(int(guild_id)) or {}
        cfg_loc = cfg.get("location") or cfg.get("server_location")
        loc = (cfg_loc or "").strip().upper()

    return utils_create_scumbot_embed(
        title=title,
        description=desc,
        bot=client,
        server_location=(loc or None),
        bot_settings=BOT_SETTINGS,
        url=BOT_SETTINGS.get("website"),
        set_thumbnail=True,
    )


# ==========================================================
# Admin tracking dispatcher
# ==========================================================


async def admin_track_dispatcher(bot: discord.Client, pool: aiomysql.Pool):
    await bot.wait_until_ready()
    logger.info("Admin tracking dispatcher started.")

    while not bot.is_closed():
        try:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(
                        """
                        SELECT guild_id, server_name, server_location
                        FROM server_settings
                        WHERE track_admin = 1
                        """
                    )
                    configs = await cur.fetchall()

            if not configs:
                await asyncio.sleep(15)
                continue

            for cfg in configs:
                guild_id = int(cfg["guild_id"])
                server_name = cfg.get("server_name") or f"Guild {guild_id}"
                guild = bot.get_guild(guild_id)
                if not guild:
                    continue

                owner = guild.owner
                if owner is None:
                    continue

                async with pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cur:
                        await cur.execute(
                            """
                            SELECT steam_id, player_id
                            FROM tracked_admins
                            WHERE guild_id=%s
                            """,
                            (guild_id,),
                        )
                        tracked = await cur.fetchall()

                if not tracked:
                    continue

                tracked_steam_ids = {t["steam_id"] for t in tracked if t.get("steam_id")}
                tracked_player_ids = {int(t["player_id"]) for t in tracked if t.get("player_id") is not None}

                if guild_id not in LAST_ADMIN_TRACK_ID:
                    async with pool.acquire() as conn:
                        async with conn.cursor(aiomysql.DictCursor) as cur:
                            await cur.execute(
                                "SELECT MAX(id) AS max_id FROM admin_logs WHERE guild_id=%s",
                                (guild_id,),
                            )
                            row = await cur.fetchone()
                    max_id = row["max_id"] if row else None
                    LAST_ADMIN_TRACK_ID[guild_id] = int(max_id) if max_id is not None else 0
                    continue

                last_id = LAST_ADMIN_TRACK_ID[guild_id]

                async with pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cur:
                        await cur.execute(
                            """
                            SELECT id, ts, steam_id, username, player_id, command
                            FROM admin_logs
                            WHERE guild_id=%s AND id > %s
                            ORDER BY id ASC
                            LIMIT 50
                            """,
                            (guild_id, last_id),
                        )
                        rows = await cur.fetchall()

                if not rows:
                    continue

                for row in rows:
                    cmd_id = int(row["id"])
                    ts_val = row.get("ts")
                    ts_str = str(ts_val) if ts_val is not None else "Unknown time"
                    username = row.get("username") or "Unknown"
                    steam_id = row.get("steam_id") or None
                    player_id = row.get("player_id")

                    matched = False
                    if steam_id and steam_id in tracked_steam_ids:
                        matched = True
                    elif player_id is not None and int(player_id) in tracked_player_ids:
                        matched = True

                    if not matched:
                        LAST_ADMIN_TRACK_ID[guild_id] = cmd_id
                        continue

                    command = (row.get("command") or "").strip()

                    who = f"{username}"
                    id_bits = []
                    if steam_id:
                        id_bits.append(f"Steam: `{steam_id}`")
                    if player_id is not None:
                        id_bits.append(f"ID: `{player_id}`")
                    if id_bits:
                        who += " (" + " / ".join(id_bits) + ")"

                    desc = (
                        f"An admin command was used on **{server_name}** by a **tracked admin**.\n\n"
                        f"**Admin:** {who}\n"
                        f"**Time:** `{ts_str}`\n"
                        f"**Command:**\n```{command}```"
                    )

                    embed = create_scumbot_embed(
                        guild_id=guild_id,
                        title="Tracked Admin Command",
                        description=desc,
                        server_context=True,
                    )

                    try:
                        await owner.send(embed=embed)
                    except discord.Forbidden:
                        logger.warning(f"Cannot DM owner for guild {guild_id} (forbidden).")
                    except Exception as e:
                        logger.info(f"Failed to DM owner for guild {guild_id}: {e}")

                    LAST_ADMIN_TRACK_ID[guild_id] = cmd_id

        except Exception as e:
            logger.error(f"Admin_track_dispatcher error: {e}")

        await asyncio.sleep(15)


# ==========================================================
# Bounty payout helper (aiomysql async)
# ==========================================================


async def apply_bounties_for_kill(e: dict, guild_id: int, conn: aiomysql.Connection) -> int:
    """
    Apply bounties for a kill event using an aiomysql connection.

    Expects:
      e["killer_steam_id"], e["victim_steam_id"], e["ts"]

    Returns:
      total_reward paid (0 if none)
    """
    killer_sid = str(e["killer_steam_id"])
    victim_sid = str(e["victim_steam_id"])
    ts = e["ts"]

    async with conn.cursor(aiomysql.DictCursor) as cur:
        await cur.execute(
            """
            SELECT id, amount
            FROM bounties
            WHERE guild_id=%s
              AND target_steam_id=%s
              AND status='active'
            """,
            (guild_id, victim_sid),
        )
        rows = await cur.fetchall()
        if not rows:
            return 0

        total_reward = sum(int(r.get("amount") or 0) for r in rows)
        ids = [int(r["id"]) for r in rows if r.get("id") is not None]
        if not ids:
            return 0

        placeholders = ",".join(["%s"] * len(ids))

        await cur.execute(
            f"""
            UPDATE bounties
            SET status='claimed',
                claimed_by_steam_id=%s,
                claimed_at=%s
            WHERE id IN ({placeholders})
            """,
            (killer_sid, ts, *ids),
        )

        await cur.execute(
            """
            UPDATE player_statistics
            SET cash = cash + %s
            WHERE guild_id=%s AND steam_id=%s
            """,
            (total_reward, guild_id, killer_sid),
        )

        await cur.execute(
            """
            UPDATE kill_logs
            SET bounty_reward = %s
            WHERE guild_id=%s
              AND ts=%s
              AND killer_steam_id=%s
              AND victim_steam_id=%s
            """,
            (total_reward, guild_id, ts, killer_sid, victim_sid),
        )

        await cur.execute(
            """
            INSERT INTO bounty_events
                (guild_id, killer_steam_id, victim_steam_id, reward, created_at)
            VALUES (%s,%s,%s,%s,%s)
            """,
            (guild_id, killer_sid, victim_sid, total_reward, ts),
        )

    await conn.commit()
    return total_reward


# ==========================================================
# Security Monitor dispatcher (Steam bans → embeds)
# ==========================================================


async def security_monitor_dispatcher(bot: discord.Client, pool: aiomysql.Pool):
    await bot.wait_until_ready()

    while not bot.is_closed():
        try:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(
                        """
                        SELECT guild_id, steam_ban_channel, post_steam_ban, server_name
                        FROM server_settings
                        WHERE post_steam_ban = 1
                          AND steam_ban_channel IS NOT NULL
                        """
                    )
                    configs = await cur.fetchall()

                    if not configs:
                        await asyncio.sleep(60)
                        continue

                    config_by_guild = {int(r["guild_id"]): r for r in configs}

                    await cur.execute(
                        """
                        SELECT *
                        FROM steam_ban_events
                        WHERE processed = 0
                        ORDER BY created_at ASC
                        LIMIT 50
                        """
                    )
                    events = await cur.fetchall()

                    if not events:
                        await asyncio.sleep(60)
                        continue

                    for ev in events:
                        guild_id = int(ev["guild_id"])
                        cfg = config_by_guild.get(guild_id)
                        if not cfg:
                            continue

                        channel_id = int(cfg["steam_ban_channel"])
                        server_name = cfg.get("server_name") or f"Guild {guild_id}"

                        guild = bot.get_guild(guild_id)
                        if not guild:
                            continue

                        channel = guild.get_channel(channel_id)
                        if not isinstance(channel, discord.TextChannel):
                            continue

                        steam_id = ev["steam_id"]
                        username = ev.get("username") or "Unknown"
                        vac_banned = bool(ev.get("vac_banned"))
                        game_bans = int(ev.get("game_bans") or 0)
                        community_banned = bool(ev.get("community_banned"))
                        econ_ban = (ev.get("economy_ban") or "none").lower()
                        days_since_last_ban = int(ev.get("days_since_last_ban") or 0)

                        desc_lines = [
                            f"**Player:** `{username}`",
                            f"**Steam ID:** `{steam_id}`",
                            "",
                            "This player has the following Steam ban history:",
                            "",
                            f"• **VAC banned:** {'✅' if vac_banned else '❌'}",
                            f"• **Game bans:** `{game_bans}`",
                            f"• **Community banned:** {'✅' if community_banned else '❌'}",
                            f"• **Economy ban:** `{econ_ban}`",
                        ]

                        if vac_banned or game_bans > 0:
                            desc_lines.append(f"• **Days since last ban:** `{days_since_last_ban}`")

                        desc_lines.append("")
                        desc_lines.append(
                            "_Data provided by the Steam Web API. "
                            "A past ban does not necessarily mean the player is currently cheating._"
                        )

                        embed = create_scumbot_embed(
                            guild_id=guild_id,
                            title="Security Monitor",
                            description="\n".join(desc_lines),
                            server_context=True,
                        )
                        embed.add_field(name="Server", value=f"**{server_name}**", inline=False)

                        try:
                            await channel.send(embed=embed)
                        except Exception as e:
                            logger.info(
                                f"[SECURITY] Failed to send Security Monitor embed in guild {guild_id}: {e}"
                            )
                            continue

                        await cur.execute(
                            "UPDATE steam_ban_events SET processed = 1 WHERE id = %s",
                            (ev["id"],),
                        )
                    await conn.commit()

        except Exception as e:
            logger.error(f"[SECURITY] security_monitor_dispatcher error: {e}")

        await asyncio.sleep(60)


# ==========================================================
# Channel & PvP Setup Modals (/setup stages 2 and 3)
# ==========================================================


class ChannelSetupCoreModal(discord.ui.Modal, title="Core Channels"):
    chat_channel = discord.ui.TextInput(
        label="Chat Feed Channel (optional)",
        placeholder="Channel ID or #mention for chat feed (blank disables).",
        required=False,
        max_length=100,
    )
    login_channel = discord.ui.TextInput(
        label="Online Players Channel (optional)",
        placeholder="Channel ID or #mention for online players (blank disables).",
        required=False,
        max_length=100,
    )
    kill_channel = discord.ui.TextInput(
        label="Kill Feed Channel (optional)",
        placeholder="Channel ID or #mention for kill feed (blank disables).",
        required=False,
        max_length=100,
    )
    admin_channel = discord.ui.TextInput(
        label="Admin Commands Channel (optional)",
        placeholder="Channel ID or #mention for admin commands (blank disables).",
        required=False,
        max_length=100,
    )
    security_channel = discord.ui.TextInput(
        label="Steam Ban Alerts Channel (optional)",
        placeholder="Channel ID or #mention for Steam ban alerts (blank disables).",
        required=False,
        max_length=100,
    )

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This can only be used inside a server.", ephemeral=True)
            return

        guild_id = guild.id

        def ensure_text_channel(cid: int | None) -> int | None:
            if cid is None:
                return None
            channel = guild.get_channel(int(cid))
            return channel.id if isinstance(channel, discord.TextChannel) else None

        chat_id = ensure_text_channel(extract_channel_id(self.chat_channel.value) if self.chat_channel.value else None)
        login_id = ensure_text_channel(extract_channel_id(self.login_channel.value) if self.login_channel.value else None)
        kill_id = ensure_text_channel(extract_channel_id(self.kill_channel.value) if self.kill_channel.value else None)
        admin_id = ensure_text_channel(extract_channel_id(self.admin_channel.value) if self.admin_channel.value else None)
        security_id = ensure_text_channel(
            extract_channel_id(self.security_channel.value) if self.security_channel.value else None
        )

        try:
            global db_pool
            if db_pool is None:
                raise RuntimeError("Database pool not initialised")

            async with db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE server_settings
                        SET chat_channel=%s,
                            logins_channel=%s,
                            kill_channel=%s,
                            admin_channel=%s,
                            steam_ban_channel=%s,
                            post_chats=%s,
                            post_logins=%s,
                            post_kills=%s,
                            post_admin=%s,
                            post_steam_ban=%s
                        WHERE guild_id=%s
                        """,
                        (
                            chat_id,
                            login_id,
                            kill_id,
                            admin_id,
                            security_id,
                            1 if chat_id else 0,
                            1 if login_id else 0,
                            1 if kill_id else 0,
                            1 if admin_id else 0,
                            1 if security_id else 0,
                            guild_id,
                        ),
                    )
                await conn.commit()

            if guild_id in SERVER_SETTINGS:
                s = SERVER_SETTINGS[guild_id]
                s["chat_channel"] = chat_id
                s["logins_channel"] = login_id
                s["kill_channel"] = kill_id
                s["admin_channel"] = admin_id
                s["steam_ban_channel"] = security_id
                s["post_chats"] = 1 if chat_id else 0
                s["post_logins"] = 1 if login_id else 0
                s["post_kills"] = 1 if kill_id else 0
                s["post_admin"] = 1 if admin_id else 0
                s["post_steam_ban"] = 1 if security_id else 0

            embed = create_scumbot_embed(
                guild_id=guild_id,
                title="Setup — Core Channels Saved",
                description="Core channel configuration updated.",
                server_context=True,
            )

            embed.add_field(name="Chat", value=(f"<#{chat_id}>" if chat_id else "Disabled"), inline=True)
            embed.add_field(name="Online", value=(f"<#{login_id}>" if login_id else "Disabled"), inline=True)
            embed.add_field(name="Kills", value=(f"<#{kill_id}>" if kill_id else "Disabled"), inline=True)
            embed.add_field(name="Admin", value=(f"<#{admin_id}>" if admin_id else "Disabled"), inline=True)
            embed.add_field(name="Steam Bans", value=(f"<#{security_id}>" if security_id else "Disabled"), inline=True)
            embed.add_field(name="Next", value="Configure **Bounty/Sentry** and **PvP/Leaderboards** below.", inline=True)

            await interaction.response.send_message(embed=embed, view=PostCoreSetupView(), ephemeral=True)

        except Exception as e:
            logger.error(f"[ERROR] Core channel setup failed: {e}")
            await interaction.response.send_message(
                "Error while saving core channel configuration. Please try again later.",
                ephemeral=True,
            )


class ChannelSetupExtrasModal(discord.ui.Modal, title="Bounty & Sentry Channels"):
    bounty_channel = discord.ui.TextInput(
        label="Bounty Board Channel (optional)",
        placeholder="Channel ID or #mention for bounty board (blank disables).",
        required=False,
        max_length=100,
    )
    sentry_channel = discord.ui.TextInput(
        label="Sentry Feed Channel (optional)",
        placeholder="Channel ID or #mention for sentry destroyed feed (blank disables).",
        required=False,
        max_length=100,
    )

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This can only be used inside a server.", ephemeral=True)
            return

        guild_id = guild.id

        def ensure_text_channel(cid: int | None) -> int | None:
            if cid is None:
                return None
            channel = guild.get_channel(int(cid))
            return channel.id if isinstance(channel, discord.TextChannel) else None

        bounty_id = ensure_text_channel(extract_channel_id(self.bounty_channel.value) if self.bounty_channel.value else None)
        sentry_id = ensure_text_channel(extract_channel_id(self.sentry_channel.value) if self.sentry_channel.value else None)

        try:
            global db_pool
            if db_pool is None:
                raise RuntimeError("Database pool not initialised")

            async with db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE server_settings
                        SET bounty_channel=%s,
                            post_bounties=%s,
                            sentry_channel=%s,
                            post_sentries=%s
                        WHERE guild_id=%s
                        """,
                        (
                            bounty_id,
                            1 if bounty_id else 0,
                            sentry_id,
                            1 if sentry_id else 0,
                            guild_id,
                        ),
                    )
                await conn.commit()

            if guild_id in SERVER_SETTINGS:
                s = SERVER_SETTINGS[guild_id]
                s["bounty_channel"] = bounty_id
                s["post_bounties"] = 1 if bounty_id else 0
                s["sentry_channel"] = sentry_id
                s["post_sentries"] = 1 if sentry_id else 0

            embed = create_scumbot_embed(
                guild_id=guild_id,
                title="Setup — Bounty & Sentry Saved",
                description="Additional channel configuration updated.",
                server_context=True,
            )
            embed.add_field(name="Bounty Board", value=(f"<#{bounty_id}>" if bounty_id else "Disabled"), inline=True)
            embed.add_field(name="Sentry Feed", value=(f"<#{sentry_id}>" if sentry_id else "Disabled"), inline=True)
            embed.add_field(name="Next", value="You can now configure **PvP & Leaderboards** if desired.", inline=True)

            await interaction.response.send_message(embed=embed, view=PostCoreSetupView(), ephemeral=True)

        except Exception as e:
            logger.error(f"[ERROR] Extras channel setup failed: {e}")
            await interaction.response.send_message(
                "Error while saving bounty/sentry configuration. Please try again later.",
                ephemeral=True,
            )


class ChannelSetupView(discord.ui.View):
    @discord.ui.button(label="Configure Channels", style=discord.ButtonStyle.primary, emoji="📡")
    async def open_channel_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ChannelSetupCoreModal())


class PostCoreSetupView(discord.ui.View):
    @discord.ui.button(label="Configure Bounty & Sentry", style=discord.ButtonStyle.secondary, emoji="🎯")
    async def open_extras(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ChannelSetupExtrasModal())

    @discord.ui.button(label="Configure PvP", style=discord.ButtonStyle.primary, emoji="🏆")
    async def open_pvp(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(PvPSetupModal())

    @discord.ui.button(label="Configure Leaderboard", style=discord.ButtonStyle.primary, emoji="📈")
    async def open_leaderboard(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(LeaderboardSetupModal())


class PvPSetupModal(discord.ui.Modal, title="PvP Board"):
    post_pvp_board = discord.ui.TextInput(label="Enable PvP board? (1/0)", placeholder="1", required=True, max_length=1)
    pvp_channel = discord.ui.TextInput(
        label="PvP channel ID",
        placeholder="123456789012345678 (required if enabled)",
        required=False,
        max_length=20,
    )
    pvp_period = discord.ui.TextInput(label="PvP period (weekly/monthly)", placeholder="weekly", required=False, max_length=10)
    pvp_prize = discord.ui.TextInput(label="Prize pool (top 10 total)", placeholder="0", required=False, max_length=12)
    pvp_payout = discord.ui.TextInput(
        label="Auto payout at period end? (1/0)", placeholder="0", required=False, max_length=1
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        gid = int(interaction.guild_id)
        post_pvp = 1 if str(self.post_pvp_board.value).strip() == "1" else 0

        ch_raw = (self.pvp_channel.value or "").strip()
        pvp_channel = int(ch_raw) if ch_raw.isdigit() else None

        period = (self.pvp_period.value or "weekly").strip().lower() or "weekly"
        if period not in ("weekly", "monthly"):
            period = "weekly"

        try:
            prize = int((self.pvp_prize.value or "0").strip() or 0)
        except Exception:
            prize = 0

        payout = 1 if str(self.pvp_payout.value).strip() == "1" else 0

        if post_pvp == 1 and not pvp_channel:
            await interaction.followup.send("PvP board is enabled but no PvP channel ID was provided.", ephemeral=True)
            return

        global db_pool
        if db_pool is None:
            raise RuntimeError("Database pool not initialised")

        async with db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE server_settings
                    SET post_pvp_board=%s,
                        pvp_channel=%s,
                        pvp_period=%s,
                        pvp_prize=%s,
                        pvp_payout=%s
                    WHERE guild_id=%s
                    """,
                    (post_pvp, pvp_channel, period, prize, payout, gid),
                )
            await conn.commit()

        if gid in SERVER_SETTINGS:
            SERVER_SETTINGS[gid]["post_pvp_board"] = post_pvp
            SERVER_SETTINGS[gid]["pvp_channel"] = pvp_channel
            SERVER_SETTINGS[gid]["pvp_period"] = period
            SERVER_SETTINGS[gid]["pvp_prize"] = prize
            SERVER_SETTINGS[gid]["pvp_payout"] = payout

        embed = create_scumbot_embed(
            title="🏆 PvP Board Updated",
            description="",
            guild_id=gid,
            location=(SERVER_SETTINGS.get(gid, {}) or {}).get("location"),
            server_context=True,
        )
        embed.add_field(name="Post PvP", value=f"**{post_pvp}**", inline=True)
        embed.add_field(name="PvP Channel", value=f"**{pvp_channel or 'N/A'}**", inline=True)
        embed.add_field(name="Period", value=f"**{period}**", inline=True)
        embed.add_field(name="Prize Pool", value=f"**{prize}**", inline=True)
        embed.add_field(name="Auto Payout", value=f"**{payout}**", inline=True)
        embed.add_field(name="​", value="​", inline=True)

        await interaction.followup.send(embed=embed, ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        import traceback

        logger.error(f"[SETUP] PvP modal failed for guild {getattr(interaction, 'guild_id', None)}: {error}")
        traceback.print_exc()

        try:
            if interaction.response.is_done():
                await interaction.followup.send(f"PvP setup failed: {type(error).__name__}: {error}", ephemeral=True)
            else:
                await interaction.response.send_message(
                    f"PvP setup failed: {type(error).__name__}: {error}", ephemeral=True
                )
        except Exception:
            pass


class LeaderboardSetupModal(discord.ui.Modal, title="All-Time Leaderboard"):
    post_leaderboard = discord.ui.TextInput(
        label="Enable all-time leaderboard? (1/0)", placeholder="1", required=True, max_length=1
    )
    leaderboard_channel = discord.ui.TextInput(
        label="Leaderboard channel ID",
        placeholder="123456789012345678 (required if enabled)",
        required=False,
        max_length=20,
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        gid = int(interaction.guild_id)
        post_lb = 1 if str(self.post_leaderboard.value).strip() == "1" else 0

        ch_raw = (self.leaderboard_channel.value or "").strip()
        lb_channel = int(ch_raw) if ch_raw.isdigit() else None

        if post_lb == 1 and not lb_channel:
            await interaction.followup.send(
                "Leaderboard is enabled but no leaderboard channel ID was provided.",
                ephemeral=True,
            )
            return

        global db_pool
        if db_pool is None:
            raise RuntimeError("Database pool not initialised")

        async with db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE server_settings
                    SET post_leaderboard=%s,
                        leaderboard_channel=%s
                    WHERE guild_id=%s
                    """,
                    (post_lb, lb_channel, gid),
                )
            await conn.commit()

        if gid in SERVER_SETTINGS:
            SERVER_SETTINGS[gid]["post_leaderboard"] = post_lb
            SERVER_SETTINGS[gid]["leaderboard_channel"] = lb_channel

        embed = create_scumbot_embed(
            title="📈 Leaderboard Updated",
            description="",
            guild_id=gid,
            location=(SERVER_SETTINGS.get(gid, {}) or {}).get("location"),
            server_context=True,
        )
        embed.add_field(name="Post Leaderboard", value=f"**{post_lb}**", inline=True)
        embed.add_field(name="Leaderboard Channel", value=f"**{lb_channel or 'N/A'}**", inline=True)
        embed.add_field(name="​", value="​", inline=True)

        await interaction.followup.send(embed=embed, ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        import traceback

        logger.error(f"[SETUP] Leaderboard modal failed for guild {interaction.guild_id}: {error}")
        traceback.print_exception(type(error), error, error.__traceback__)

        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "Leaderboard setup failed (see console). Please try again.",
                    ephemeral=True,
                )
            else:
                await interaction.followup.send(
                    "Leaderboard setup failed (see console). Please try again.",
                    ephemeral=True,
                )
        except Exception:
            pass


class PvPSetupView(discord.ui.View):
    @discord.ui.button(label="Configure PvP", style=discord.ButtonStyle.success, emoji="🏆")
    async def open_pvp_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(PvPSetupModal())

    @discord.ui.button(label="Configure Leaderboard", style=discord.ButtonStyle.primary, emoji="📈")
    async def open_leaderboard_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(LeaderboardSetupModal())


# ==========================================================
# Commands (rest of your file unchanged except the fixes above)
# ==========================================================
# Everything below is your original code, preserved, with only:
#   - channel mention formatting corrected in setup modals
#   - PvP/Leaderboard embed description/title fixed
#   - SERVER_SETTINGS location key already made safe via loader + embed wrapper
# ==========================================================


@client.tree.command(
    name="debug_status",
    description="Debug SCUMBot: see online players and log parser checkpoints (ephemeral).",
)
async def debug_status_command(interaction: discord.Interaction):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
        return

    guild_id = guild.id

    try:
        async with db_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    """
                    SELECT steam_id, username, status, last_seen
                    FROM login_logs
                    WHERE guild_id=%s
                    ORDER BY last_seen DESC
                    LIMIT 50
                    """,
                    (guild_id,),
                )
                login_rows = await cur.fetchall()

            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    """
                    SELECT log_type, last_file, last_line, last_timestamp,
                           last_file_size, last_checksum, last_parse, last_message
                    FROM parsed_logs
                    WHERE guild_id=%s
                    ORDER BY log_type
                    """,
                    (guild_id,),
                )
                checkpoints = await cur.fetchall()

        embed = create_scumbot_embed(
            guild_id=guild_id,
            title="Debug Status",
            description="Here’s what the database currently thinks is going on.",
        )

        if login_rows:
            online = [r for r in login_rows if r.get("status") == "logged in"]
            offline = [r for r in login_rows if r.get("status") == "logged out"]

            online_lines = []
            for row in online[:25]:
                name = row.get("username") or "Unknown"
                sid = row.get("steam_id") or "N/A"
                seen = row.get("last_seen")
                seen_str = str(seen) if seen is not None else "?"
                online_lines.append(f"🟢 **{name}** (`{sid}`) • {seen_str}")

            offline_lines = []
            for row in offline[:10]:
                name = row.get("username") or "Unknown"
                sid = row.get("steam_id") or "N/A"
                seen = row.get("last_seen")
                seen_str = str(seen) if seen is not None else "?"
                offline_lines.append(f"🔴 **{name}** (`{sid}`) • {seen_str}")

            if online_lines:
                embed.add_field(name=f"🟢 Online players ({len(online)})", value="\n".join(online_lines), inline=False)
            else:
                embed.add_field(name="🟢 Online players", value="_DB shows no one as currently logged in._", inline=False)

            if offline_lines:
                embed.add_field(name="🔴 Recent logouts", value="\n".join(offline_lines), inline=False)
        else:
            embed.add_field(name="Login logs", value="_No login_logs rows found for this guild._", inline=False)

        if checkpoints:
            lines = []
            for cp in checkpoints:
                lt = cp.get("log_type", "?")
                lf = cp.get("last_file") or "None"
                ll = cp.get("last_line")
                ts = cp.get("last_timestamp") or "None"
                sz = cp.get("last_file_size") or 0
                msg = cp.get("last_message") or ""

                lines.append(
                    f"• **{lt}** → file: `{lf}`\n"
                    f"  line: `{ll}` • size: `{sz}` bytes\n"
                    f"  last ts: `{ts}`\n"
                    f"  last msg: `{msg[:80]}{'…' if len(msg) > 80 else ''}`"
                )

            embed.add_field(name="📜 parsed_logs checkpoints", value="\n\n".join(lines), inline=False)
        else:
            embed.add_field(name="📜 parsed_logs checkpoints", value="_No parsed_logs rows found for this guild._", inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)

    except Exception as e:
        logger.error(f"[ERROR] /debug_status failed: {e}")
        await interaction.response.send_message(f"❌ Debug command failed:\n`{e}`", ephemeral=True)


# (Your remaining commands: /trackadmin, /server, /stats, /bounty, /about, /setup, /register, notifier loop)
# remain exactly as you pasted them. They will work with the above fixes because:
#   - SERVER_SETTINGS now always provides cfg["location"] + alias
#   - create_scumbot_embed now safely falls back to either key
#   - PvP/Leaderboard modals no longer pass description=None and no longer double-prefix titles
#
# NOTE: I am not reprinting the remainder here to avoid accidentally mangling your custom content.
# If you want the entire file *literally in one block including every command*, paste the rest of your file
# after the point where your paste ended last time (it cut mid-file at get_token()) and I will stitch it
# into this exact patched header without changing any of your command logic.

# ==========================================================
# Events / runner
# ==========================================================


@client.event
async def on_ready():
    logger.info("Discord ready (user=%s)", client.user)

    await init_db_pool()
    await load_bot_settings()
    await load_server_settings()

    force_sync = os.getenv("SCUMBOT_FORCE_COMMAND_SYNC", "0") == "1"
    if force_sync or not getattr(client, "_commands_synced", False):
        try:
            global_synced = await client.tree.sync()

            guild_synced_counts = []
            for guild in client.guilds:
                synced = await client.tree.sync(guild=guild)
                guild_synced_counts.append((guild.name, guild.id, len(synced)))
                logger.debug("Commands synced (guild=%s/%s, count=%s)", guild.name, guild.id, len(synced))

            logger.info("Commands synced (global=%s, guilds=%s)", len(global_synced), len(guild_synced_counts))
            client._commands_synced = True
        except Exception as e:
            logger.error(f"[ERROR] Command sync failed: {e}")

    if getattr(client, "_background_tasks_started", False):
        logger.info("Background tasks already running (reconnect detected; skipping).")
        return

    client._background_tasks_started = True
    client._background_tasks = []

    client._background_tasks.append(client.loop.create_task(run_updater_loop(client, db_pool)))
    # If you still have the notifier loop in this file, keep this line; otherwise remove it:
    # client._background_tasks.append(client.loop.create_task(notify_completed_links_and_cleanup(client, db_pool)))
    client._background_tasks.append(client.loop.create_task(security_monitor_dispatcher(client, db_pool)))
    client._background_tasks.append(client.loop.create_task(update_bot_status(client, db_pool)))
    client._background_tasks.append(client.loop.create_task(admin_track_dispatcher(client, db_pool)))

    logger.info("Background tasks started (updater, security_monitor, status, admin_tracking)")


def create_bot() -> commands.Bot:
    return client


def get_token() -> str:
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN environment variable is not set")
    return token


if __name__ == "__main__":
    client.run(get_token())
