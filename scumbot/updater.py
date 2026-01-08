# ==========================================================
# SCUMBot – Unified Feed Updater (Rolling Embeds + Per-Event Feeds)
#
# Rolling embeds (1 message edited per guild):
#   - Chat feed (last N chat lines)
#   - Online players (current online list)
#   - Bounty board
#   - PvP board (weekly/monthly) + optional payouts
#   - All-time leaderboard
#
# Per-event embeds:
#   - Kill feed (each kill -> embed)
#   - Sentry destroyed (each event -> embed)
#
# Admin commands:
#   - Rolling embed (edited in place) to avoid spam.
#
# NOTES:
# - Downloader parses + stores logs. Updater only POSTS to Discord based on toggles.
# - For per-event feeds we checkpoint by AUTO-INCREMENT ID (robust vs late-arriving timestamps).
# ==========================================================

import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiomysql
import discord
from aiomysql import DictCursor
import logging
from .logging_utils import server_label, new_error_id, warn_ratelimited

# Shared utilities (single source of truth)
from .utils.flags import get_flag_url as utils_get_flag_url
from .utils.embeds import apply_scumbot_footer as utils_apply_scumbot_footer
from .utils.embeds import create_scumbot_embed as utils_create_scumbot_embed
logger = logging.getLogger("updater")

# Prevent concurrent edits to the SAME rolling embed message.
# Without this, two overlapping updater tasks can race:
#   - Task A edits message with footer+flag
#   - Task B edits message with footer+avatar (if it read incomplete settings)
# The result can look like the footer icon "flashes".
_ROLLING_EDIT_LOCKS: dict[tuple[int, str], asyncio.Lock] = {}


def _get_rolling_lock(guild_id: int, settings_column: str) -> asyncio.Lock:
    key = (guild_id, settings_column)
    lock = _ROLLING_EDIT_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _ROLLING_EDIT_LOCKS[key] = lock
    return lock

# ---------------- Configuration ----------------
UPDATE_INTERVAL = 20

DISPLAY_CHAT_MESSAGES = 20
DISPLAY_ADMIN_MESSAGES = 10
DISPLAY_ONLINE_MAX = 40

SCUM_ORANGE = discord.Color.from_rgb(222, 133, 0)
SUICIDE_IMAGE_URL = "https://i.ibb.co/DfCW5HxR/3a497a76-c443-42c3-a4c2-746040dd7cfa.png"

# Tournament weights for PvP payouts (sum=100). Pool is total.
PVP_WEIGHTS = [30, 18, 12, 9, 7, 6, 5, 5, 4, 4]

# In-memory checkpoints to avoid re-posting old events
LAST_KILL_ID: Dict[int, int] = {}
LAST_SENTRY_ID: Dict[int, int] = {}

# Bot settings cache (loaded once in updater)
BOT_SETTINGS: dict = {
    "name": "SCUMBot",
    "version": "v1.0.0",
    "website": "",
    "logo": "",
    "_loaded": False,
}

DB_POOL: aiomysql.Pool | None = None


# ==========================================================
# Weapon Catalog (local JSON file)
# ==========================================================
@dataclass
class WeaponEntry:
    display_name: str
    image_url: Optional[str]


class WeaponCatalog:
    """
    Loads scum_weapons_catalog.json created by your scraper.
    Provides .resolve(raw_weapon) -> (clean_name, image_url)
    """

    def __init__(self, path: str):
        self.path = path
        self._loaded = False
        self._by_spawn: Dict[str, WeaponEntry] = {}
        self._by_name: Dict[str, WeaponEntry] = {}

    def load(self) -> None:
        if self._loaded:
            return
        self._loaded = True

        if not os.path.exists(self.path):
            logger.info(f"[WEAPONS] Catalog missing: {self.path} (weapon thumbnails will be skipped)")
            return

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)

            for row in data:
                spawn = (row.get("spawn_id") or "").strip()
                disp = (row.get("display_name") or "").strip()
                img = (row.get("image_url") or None)

                entry = WeaponEntry(display_name=disp or spawn or "Unknown", image_url=img)

                if spawn:
                    self._by_spawn[self._norm(spawn)] = entry
                if disp:
                    self._by_name[self._norm(disp)] = entry

            logger.info(f"[WEAPONS] Loaded catalog entries: {len(data)} from {self.path}")
        except Exception as e:
            logger.info(f"[WEAPONS] Failed to load catalog: {e}")

    @staticmethod
    def _norm(s: str) -> str:
        return "".join(ch for ch in s.lower().strip() if ch.isalnum())

    def resolve(self, raw_weapon: str | None) -> Tuple[str, Optional[str]]:
        self.load()
        if not raw_weapon:
            return ("Unknown", None)

        raw = raw_weapon.strip()

        # Common log formats: "Weapon_SDASS_C [Projectile]" → want SDASS
        candidate = raw.split("[", 1)[0].strip()

        key = self._norm(candidate)
        if key in self._by_spawn:
            e = self._by_spawn[key]
            return (e.display_name, e.image_url)
        if key in self._by_name:
            e = self._by_name[key]
            return (e.display_name, e.image_url)

        cleaned = candidate
        for prefix in ("Weapon_", "BP_Weapon_", "BPC_Weapon_", "BPWeapon_", "BPCWeapon_"):
            if cleaned.startswith(prefix):
                cleaned = cleaned[len(prefix) :]
                break
        cleaned = cleaned.replace("_C", "").replace("_", " ").strip()

        key2 = self._norm(cleaned)
        if key2 in self._by_name:
            e = self._by_name[key2]
            return (e.display_name, e.image_url)

        return (cleaned or "Unknown", None)


_WEAPONS_PATH = os.path.join(os.path.dirname(__file__), "scum_weapons_catalog.json")
WEAPON_CATALOG = WeaponCatalog(_WEAPONS_PATH)


# ==========================================================
# Footer / Flags / Utilities
# ==========================================================
def get_flag_url(flag: str | None) -> str | None:
    # Backwards compatible wrapper — real implementation lives in utils.flags.
    return utils_get_flag_url(flag)


def apply_scumbot_footer(embed: discord.Embed, *, bot: discord.Client | None, server_location: str | None) -> None:
    # Backwards compatible wrapper — real implementation lives in utils.embeds.
    return utils_apply_scumbot_footer(embed, bot=bot, server_location=server_location, bot_settings=BOT_SETTINGS)


def safe_kd(kills: int | None, deaths: int | None) -> float:
    k = int(kills or 0)
    d = int(deaths or 0)
    return float(k) if d <= 0 else (k / d)


def coords_to_sector(x: float | None, y: float | None) -> str:
    if x is None or y is None:
        return "N/A"
    MAP_MIN = -600000.0
    MAP_MAX = 600000.0
    GRID = 15  # A-O
    try:
        xf = float(x)
        yf = float(y)
    except Exception:
        return "N/A"

    xf = max(MAP_MIN, min(MAP_MAX, xf))
    yf = max(MAP_MIN, min(MAP_MAX, yf))

    span = (MAP_MAX - MAP_MIN) or 1.0
    col = int(((xf - MAP_MIN) / span) * GRID)
    row = int(((MAP_MAX - yf) / span) * GRID)
    col = max(0, min(GRID - 1, col))
    row = max(0, min(GRID - 1, row))

    letter = chr(ord("A") + col)
    number = row + 1
    return f"{letter}{number}"


def apply_weapon_thumbnail_from_row(
    embed: discord.Embed,
    row: dict,
    *,
    weapon_key: str = "weapon",
    fallback_guild: discord.Guild | None = None,
    fallback_bot: discord.Client | None = None,
    suicide: bool = False,
) -> None:
    if suicide and SUICIDE_IMAGE_URL:
        embed.set_thumbnail(url=SUICIDE_IMAGE_URL)
        return

    weapon_raw = row.get(weapon_key)
    _, weapon_img = WEAPON_CATALOG.resolve(weapon_raw)

    img = (weapon_img or "").strip()
    if img.startswith("http://"):
        img = "https://" + img[len("http://") :]
    if img.startswith("https://"):
        embed.set_thumbnail(url=img)
        return

    if fallback_guild and fallback_guild.icon:
        embed.set_thumbnail(url=fallback_guild.icon.url)
    elif fallback_bot and fallback_bot.user:
        embed.set_thumbnail(url=fallback_bot.user.display_avatar.url)


def _clean_line(s: str, max_len: int = 120) -> str:
    s = (s or "").replace("\n", " ").replace("\r", " ").strip()
    s = s.replace("```", "'''")
    if len(s) > max_len:
        s = s[: max_len - 3] + "..."
    return s


def _fmt_time(value: Any) -> str:
    # aiomysql returns TIME columns as datetime.timedelta
    if value is None:
        return "??:??:??"
    if isinstance(value, timedelta):
        total = int(value.total_seconds())
        if total < 0:
            total = 0
        h = total // 3600
        m = (total % 3600) // 60
        s = total % 60
        return f"{h:02d}:{m:02d}:{s:02d}"
    if isinstance(value, datetime):
        return value.strftime("%H:%M:%S")
    return str(value)[:8]


async def ensure_bot_settings_loaded(pool: aiomysql.Pool) -> None:
    if BOT_SETTINGS.get("_loaded"):
        return
    try:
        async with pool.acquire() as conn:
            async with conn.cursor(DictCursor) as cur:
                await cur.execute("SELECT * FROM bot_settings LIMIT 1")
                row = await cur.fetchone()

        if row:
            BOT_SETTINGS["name"] = row.get("bot_name", BOT_SETTINGS["name"])
            BOT_SETTINGS["version"] = row.get("bot_version", BOT_SETTINGS["version"])
            BOT_SETTINGS["website"] = row.get("bot_website", "")
            BOT_SETTINGS["logo"] = row.get("bot_logo", "")
        BOT_SETTINGS["_loaded"] = True
        # Keep INFO logs concise; the full dict is available at DEBUG when needed.
        logger.info(
            "Bot settings loaded (name=%s, version=%s)",
            BOT_SETTINGS.get("name"),
            BOT_SETTINGS.get("version"),
        )
        logger.debug("Bot settings payload: %s", BOT_SETTINGS)
    except Exception as e:
        logger.error(f"[ERROR] Updater failed to load bot_settings: {e}")


async def upsert_rolling_embed(
    *,
    pool: aiomysql.Pool,
    guild_id: int,
    channel: discord.TextChannel,
    message_id: Optional[int],
    embed: discord.Embed,
    settings_column: str,
) -> tuple[int, str]:
    lock = _get_rolling_lock(guild_id, settings_column)
    async with lock:
        msg: Optional[discord.Message] = None

        if message_id:
            try:
                msg = await channel.fetch_message(int(message_id))
                await msg.edit(embed=embed)
                return msg.id, "edit"
            except Exception:
                msg = None

        msg = await channel.send(embed=embed)

        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"UPDATE server_settings SET {settings_column}=%s WHERE guild_id=%s",
                    (msg.id, guild_id),
                )
            await conn.commit()

        # If we had a message id but failed to fetch/edit, we effectively recreated it.
        action = "recreate" if message_id else "create"
        return msg.id, action


# ==========================================================
# Embed Builders
# ==========================================================
def build_kill_embed(
    *,
    server_name: str,
    row: dict,
    bot: discord.Client | None,
    guild: discord.Guild | None,
    server_location: str | None,
) -> discord.Embed:
    title_prefix = BOT_SETTINGS.get("name", "SCUMBot")

    k_name = row.get("killer_username") or "Unknown"
    v_name = row.get("victim_username") or "Unknown"
    k_sid = row.get("killer_steam_id")
    v_sid = row.get("victim_steam_id")

    weapon_raw = row.get("weapon") or "Unknown"
    dist = row.get("distance")

    killer_kills = int(row.get("killer_kills") or 0)
    killer_deaths = int(row.get("killer_deaths") or 0)
    victim_kills = int(row.get("victim_kills") or 0)
    victim_deaths = int(row.get("victim_deaths") or 0)

    killer_kd = safe_kd(killer_kills, killer_deaths)
    victim_kd = safe_kd(victim_kills, victim_deaths)

    is_suicide = (row.get("src_tag") == "SUICIDE") or (k_sid and v_sid and k_sid == v_sid)
    sector = coords_to_sector(row.get("victim_x"), row.get("victim_y"))

    ach_lines: List[str] = []
    try:
        dist_f = float(dist) if dist is not None else None
    except Exception:
        dist_f = None

    if dist_f is not None and not is_suicide:
        if dist_f >= 1000:
            ach_lines.append(f"🏅 **Achievement:** Long shot ({dist_f:.2f} m)")
        elif dist_f >= 500:
            ach_lines.append(f"🏅 **Achievement:** Marksman ({dist_f:.2f} m)")

    bounty_lines: List[str] = []
    reward = int(row.get("bounty_reward") or 0)
    if reward > 0 and not is_suicide:
        bounty_lines.append(f"💰 **Bounty payout:** ${reward:,} awarded to **{k_name}**")

    embed = discord.Embed(
        title=f"{title_prefix} ┃ ☠ Kill — {server_name}",
        color=SCUM_ORANGE,
        url=BOT_SETTINGS.get("website") or discord.Embed.Empty,
    )

    header = ach_lines + bounty_lines
    if header:
        embed.description = "\n".join(header)

    embed.add_field(name="Killer Name", value=f"**{k_name}**", inline=True)
    embed.add_field(name="Total Kills", value=f"**{killer_kills:,}**", inline=True)
    embed.add_field(name="KD", value=f"**{killer_kd:.2f}**", inline=True)

    embed.add_field(name="Victim Name", value=f"**{v_name}**", inline=True)
    embed.add_field(name="Total Deaths", value=f"**{victim_deaths:,}**", inline=True)
    embed.add_field(name="KD", value=f"**{victim_kd:.2f}**", inline=True)

    if is_suicide:
        weapon_name = "Suicide"
    else:
        weapon_name, _ = WEAPON_CATALOG.resolve(weapon_raw)

    dist_str = f"**{float(dist):.2f} m**" if dist is not None else "**N/A**"

    embed.add_field(name="Weapon", value=f"**{weapon_name}**", inline=True)
    embed.add_field(name="Distance", value=dist_str, inline=True)
    embed.add_field(name="Sector", value=f"**{sector}**", inline=True)

    apply_weapon_thumbnail_from_row(
        embed,
        row,
        weapon_key="weapon",
        fallback_guild=guild,
        fallback_bot=bot,
        suicide=is_suicide,
    )

    apply_scumbot_footer(embed, bot=bot, server_location=server_location)
    return embed


def build_sentry_embed(
    *,
    server_name: str,
    row: dict,
    bot: discord.Client | None,
    guild: discord.Guild | None,
    server_location: str | None,
) -> discord.Embed:
    title_prefix = BOT_SETTINGS.get("name", "SCUMBot")

    killer = row.get("killer_username") or "Unknown"
    sid = row.get("killer_steam_id") or "N/A"

    ts = row.get("ts")
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S") if isinstance(ts, datetime) else str(ts or "N/A")

    weapon_name, _ = WEAPON_CATALOG.resolve(row.get("weapon"))
    dmg = float(row.get("damage") or 0.0)

    x = row.get("x")
    y = row.get("y")
    z = row.get("z")
    sector = coords_to_sector(x, y)

    embed = discord.Embed(
        title=f"{title_prefix} ┃ 🤖 Sentry Destroyed — {server_name}",
        color=SCUM_ORANGE,
        url=BOT_SETTINGS.get("website") or discord.Embed.Empty,
    )

    embed.add_field(name="Killer", value=f"**{killer}**", inline=True)
    embed.add_field(name="Weapon", value=f"**{weapon_name}**", inline=True)
    embed.add_field(name="Damage", value=f"**{dmg:.2f}**", inline=True)

    embed.add_field(name="Steam ID", value=f"`{sid}`", inline=True)
    embed.add_field(name="Time", value=f"**{ts_str}**", inline=True)
    embed.add_field(name="Sector", value=f"**{sector}**", inline=True)

    try:
        cx = float(x) if x is not None else 0.0
        cy = float(y) if y is not None else 0.0
        cz = float(z) if z is not None else 0.0
        embed.add_field(name="Coords", value=f"**X:{cx:.0f} Y:{cy:.0f} Z:{cz:.0f}**", inline=False)
    except Exception:
        embed.add_field(name="Coords", value="**N/A**", inline=False)

    apply_weapon_thumbnail_from_row(
        embed,
        row,
        weapon_key="weapon",
        fallback_guild=guild,
        fallback_bot=bot,
        suicide=False,
    )

    apply_scumbot_footer(embed, bot=bot, server_location=server_location)
    return embed


def build_admin_embed(
    *,
    server_name: str,
    row: dict,
    bot: discord.Client | None,
    guild: discord.Guild | None,
    server_location: str | None,
) -> discord.Embed:
    title_prefix = BOT_SETTINGS.get("name", "SCUMBot")

    username = row.get("username") or "Unknown"
    steam_id = row.get("steam_id") or "N/A"
    command = row.get("command") or "N/A"
    ts = row.get("ts")
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S") if isinstance(ts, datetime) else str(ts or "N/A")

    embed = discord.Embed(
        title=f"{title_prefix} ┃ 🛡 Admin Command — {server_name}",
        color=SCUM_ORANGE,
        url=BOT_SETTINGS.get("website") or discord.Embed.Empty,
    )
    embed.add_field(name="Admin", value=f"**{username}**", inline=True)
    embed.add_field(name="Steam ID", value=f"`{steam_id}`", inline=True)
    embed.add_field(name="Time", value=f"**{ts_str}**", inline=True)

    embed.add_field(name="Command", value=f"```{_clean_line(command, 180)}```", inline=False)

    if guild and guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    elif bot and bot.user:
        embed.set_thumbnail(url=bot.user.display_avatar.url)

    apply_scumbot_footer(embed, bot=bot, server_location=server_location)
    return embed


def build_chat_embed(
    *,
    server_name: str,
    rows: List[dict],
    bot: discord.Client,
    server_location: str | None,
) -> discord.Embed:
    title_prefix = BOT_SETTINGS.get("name", "SCUMBot")

    embed = discord.Embed(
        title=f"{title_prefix} ┃ 💬 Chat Feed — {server_name}",
        color=SCUM_ORANGE,
        url=BOT_SETTINGS.get("website") or discord.Embed.Empty,
    )

    if not rows:
        embed.description = "_No chat messages recorded yet._"
        apply_scumbot_footer(embed, bot=bot, server_location=server_location)
        return embed

    lines: List[str] = []
    for r in rows:
        t = _fmt_time(r.get("time"))
        u = _clean_line(r.get("username") or "Unknown", 20)
        c = _clean_line(r.get("chat_type") or "CHAT", 10).upper()
        m = _clean_line(r.get("message") or "", 120)
        lines.append(f"[{t}] ({c}) {u}: {m}")

    body = "\n".join(lines)
    embed.description = f"```{body}```"
    apply_scumbot_footer(embed, bot=bot, server_location=server_location)
    return embed


def build_online_embed(
    *,
    server_name: str,
    online_rows: List[dict],
    bot: discord.Client,
    server_location: str | None,
) -> discord.Embed:
    title_prefix = BOT_SETTINGS.get("name", "SCUMBot")
    embed = discord.Embed(
        title=f"{title_prefix} ┃ 🟢 Online Players — {server_name}",
        color=SCUM_ORANGE,
        url=BOT_SETTINGS.get("website") or discord.Embed.Empty,
    )

    count = len(online_rows)
    embed.description = f"**Online:** **{count}**\n_Last refresh: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC_"

    if count == 0:
        embed.add_field(name="Players", value="_Nobody is currently online._", inline=False)
        apply_scumbot_footer(embed, bot=bot, server_location=server_location)
        return embed

    lines: List[str] = []
    shown = 0
    for r in online_rows:
        if shown >= DISPLAY_ONLINE_MAX:
            break
        u = _clean_line(r.get("username") or str(r.get("steam_id") or "Unknown"), 22)
        sector = coords_to_sector(r.get("x"), r.get("y"))
        last_seen = r.get("last_seen")
        ls = last_seen.strftime("%H:%M:%S") if isinstance(last_seen, datetime) else (str(last_seen or "")[:8] or "N/A")
        lines.append(f"{u:<22}  {sector:<3}  last:{ls}")
        shown += 1

    extra = count - shown
    if extra > 0:
        lines.append(f"... and {extra} more")

    embed.add_field(name="Players", value=f"```{chr(10).join(lines)}```", inline=False)
    apply_scumbot_footer(embed, bot=bot, server_location=server_location)
    return embed


# ==========================================================
# PvP / Leaderboard SQL helpers (unchanged)
# ==========================================================
def split_prize_pool(pool_total: int) -> List[int]:
    if pool_total <= 0:
        return [0] * 10
    payouts = [0] * 10
    for i in range(1, 10):
        payouts[i] = int(round(pool_total * (PVP_WEIGHTS[i] / 100.0)))
    payouts[0] = pool_total - sum(payouts[1:])
    return payouts


def period_window(now_utc: datetime, period: str) -> Tuple[datetime, datetime, datetime, datetime]:
    period = (period or "weekly").lower()

    if period == "monthly":
        cur_start = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if cur_start.month == 12:
            cur_end = cur_start.replace(year=cur_start.year + 1, month=1)
        else:
            cur_end = cur_start.replace(month=cur_start.month + 1)

        prev_end = cur_start
        if cur_start.month == 1:
            prev_start = cur_start.replace(year=cur_start.year - 1, month=12)
        else:
            prev_start = cur_start.replace(month=cur_start.month - 1)

        return cur_start, cur_end, prev_start, prev_end

    monday = (now_utc - timedelta(days=now_utc.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    cur_start = monday
    cur_end = monday + timedelta(days=7)
    prev_end = cur_start
    prev_start = prev_end - timedelta(days=7)
    return cur_start, cur_end, prev_start, prev_end


async def fetch_top_pvp(pool: aiomysql.Pool, guild_id: int, start: datetime, end: datetime) -> List[dict]:
    sql = """
    SELECT
      kl.killer_steam_id AS steam_id,
      MAX(kl.killer_username) AS player_name,
      COUNT(*) AS kills,
      AVG(kl.distance) AS avg_distance
    FROM kill_logs kl
    WHERE kl.guild_id = %s
      AND kl.ts >= %s AND kl.ts < %s
      AND (kl.src_tag IS NULL OR kl.src_tag <> 'SUICIDE')
    GROUP BY kl.killer_steam_id
    ORDER BY kills DESC
    LIMIT 10;
    """
    async with pool.acquire() as conn:
        async with conn.cursor(DictCursor) as cur:
            await cur.execute(sql, (guild_id, start, end))
            return await cur.fetchall()


async def fetch_top_weapon_for_player_period(
    pool: aiomysql.Pool, guild_id: int, steam_id: str, start: datetime, end: datetime
) -> Optional[str]:
    sql = """
    SELECT weapon
    FROM kill_logs
    WHERE guild_id = %s
      AND killer_steam_id = %s
      AND ts >= %s AND ts < %s
      AND (src_tag IS NULL OR src_tag <> 'SUICIDE')
    GROUP BY weapon
    ORDER BY COUNT(*) DESC
    LIMIT 1;
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (guild_id, steam_id, start, end))
            row = await cur.fetchone()
            return row[0] if row else None


async def fetch_leaderboard_all_time(pool: aiomysql.Pool, guild_id: int) -> List[Dict[str, Any]]:
    sql = """
    SELECT
      ws.steam_id,
      COALESCE(ps.username, ku.username, ws.steam_id) AS player_name,
      SUM(ws.kills) AS kills,
      COALESCE(ps.deaths, 0) AS deaths,
      MAX(ws.longest_kill) AS longest_kill,
      CASE
        WHEN SUM(ws.kills) > 0 THEN SUM(ws.total_distance) / SUM(ws.kills)
        ELSE 0
      END AS avg_distance
    FROM weapon_stats ws
    LEFT JOIN player_statistics ps
      ON ps.guild_id = ws.guild_id AND ps.steam_id = ws.steam_id
    LEFT JOIN (
      SELECT
        guild_id,
        killer_steam_id AS steam_id,
        MAX(killer_username) AS username
      FROM kill_logs
      WHERE guild_id = %s
      GROUP BY guild_id, killer_steam_id
    ) ku
      ON ku.guild_id = ws.guild_id AND ku.steam_id = ws.steam_id
    WHERE ws.guild_id = %s
    GROUP BY ws.guild_id, ws.steam_id, ps.username, ps.deaths, ku.username
    ORDER BY kills DESC
    LIMIT 10;
    """
    async with pool.acquire() as conn:
        async with conn.cursor(DictCursor) as cur:
            await cur.execute(sql, (guild_id, guild_id))
            return await cur.fetchall()


async def fetch_top_weapon_for_player_all_time(pool: aiomysql.Pool, guild_id: int, steam_id: str) -> Optional[str]:
    sql = """
    SELECT ws.weapon
    FROM weapon_stats ws
    JOIN (
      SELECT steam_id, MAX(kills) AS max_kills
      FROM weapon_stats
      WHERE guild_id = %s
      GROUP BY steam_id
    ) mx
      ON mx.steam_id = ws.steam_id AND mx.max_kills = ws.kills
    WHERE ws.guild_id = %s AND ws.steam_id = %s
    LIMIT 1;
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (guild_id, guild_id, steam_id))
            row = await cur.fetchone()
            return row[0] if row else None


async def has_paid_period(pool: aiomysql.Pool, guild_id: int, period_type: str, period_start: datetime) -> bool:
    sql = """
    SELECT 1
    FROM pvp_payouts
    WHERE guild_id=%s AND period_type=%s AND period_start=%s
    LIMIT 1;
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (guild_id, period_type, period_start))
            return (await cur.fetchone()) is not None


async def mark_paid_period(pool: aiomysql.Pool, guild_id: int, period_type: str, start: datetime, end: datetime) -> None:
    sql = """
    INSERT INTO pvp_payouts (guild_id, period_type, period_start, period_end, paid_at)
    VALUES (%s, %s, %s, %s, NOW());
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (guild_id, period_type, start, end))
        await conn.commit()


async def award_cash(pool: aiomysql.Pool, guild_id: int, steam_id: str, amount: int) -> None:
    if amount <= 0:
        return
    sql = """
    UPDATE player_statistics
    SET cash = COALESCE(cash, 0) + %s
    WHERE guild_id=%s AND steam_id=%s;
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (amount, guild_id, steam_id))
        await conn.commit()


# ==========================================================
# Rolling: Admin Commands (last N commands)
# ==========================================================
async def update_admin_board(
    *,
    bot: discord.Client,
    pool: aiomysql.Pool,
    guild: discord.Guild,
    settings: dict,
    server_name: str,
    server_location: str | None,
) -> str:
    if int(settings.get("post_admin", 0) or 0) != 1:
        return "disabled"

    channel_id = settings.get("admin_channel")
    if not channel_id:
        return "missing_channel"

    channel = guild.get_channel(int(channel_id))
    if not isinstance(channel, discord.TextChannel):
        return "invalid_channel"

    sql = """
    SELECT ts, username, command
    FROM admin_logs
    WHERE guild_id=%s
    ORDER BY id DESC
    LIMIT %s
    """
    async with pool.acquire() as conn:
        async with conn.cursor(DictCursor) as cur:
            await cur.execute(sql, (guild.id, DISPLAY_ADMIN_MESSAGES))
            rows = await cur.fetchall()

    title_prefix = BOT_SETTINGS.get("name", "SCUMBot")
    embed = discord.Embed(
        title=f"{title_prefix} ┃ 🛡 Admin Commands — {server_name}",
        color=SCUM_ORANGE,
        url=BOT_SETTINGS.get("website") or discord.Embed.Empty,
    )

    embed.description = (
        f"Last **{DISPLAY_ADMIN_MESSAGES}** admin commands.\n"
        f"_Last refresh: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC_"
    )

    if not rows:
        embed.add_field(name="Commands", value="_No admin commands recorded yet._", inline=False)
    else:
        # Oldest -> newest for readability
        rows = list(reversed(rows))

        lines: List[str] = []

        for r in rows:
            username = _clean_line(r.get("username") or "Unknown", 18)
            ts = r.get("ts")
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S") if isinstance(ts, datetime) else str(ts or "N/A")
            ts_str = ts_str[:19]

            # Keep commands short to stay under embed limits
            cmd = _clean_line(r.get("command") or "", 42)

            lines.append(f"{ts_str} | {username:<18}\n{cmd}")

        body = "\n".join(lines)

        # Field value max is 1024 chars; embed total max is 6000.
        # Keep this conservative to avoid hitting the total limit once footer/title are included.
        MAX_FIELD_CHARS = 950
        if len(body) > MAX_FIELD_CHARS:
            body = body[: MAX_FIELD_CHARS - 12] + "\n...(trimmed)"

        embed.add_field(name=f"Last {min(DISPLAY_ADMIN_MESSAGES, len(rows))} Commands", value=f"```{body}```", inline=False)

    # Thumbnail
    if guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    elif bot.user:
        embed.set_thumbnail(url=bot.user.display_avatar.url)

    apply_scumbot_footer(embed, bot=bot, server_location=server_location)

    new_id, action = await upsert_rolling_embed(
        pool=pool,
        guild_id=guild.id,
        channel=channel,
        message_id=settings.get("admin_message"),
        embed=embed,
        settings_column="admin_message",
    )
    settings["admin_message"] = new_id

    return action

# ==========================================================
# Rolling: Chat Feed
# ==========================================================
async def update_chat_feed(
    *,
    bot: discord.Client,
    pool: aiomysql.Pool,
    guild: discord.Guild,
    settings: dict,
    server_name: str,
    server_location: str | None,
) -> str:
    """Update the rolling chat feed message for a guild.

    Returns a short status string for logging summaries.
    """
    if int(settings.get("post_chats", 0) or 0) != 1:
        return "disabled"

    channel_id = settings.get("chat_channel")
    if not channel_id:
        return "missing_channel"

    channel = guild.get_channel(int(channel_id))
    if not isinstance(channel, discord.TextChannel):
        return "invalid_channel"

    sql = """
    SELECT date, time, username, chat_type, message
    FROM chat_logs
    WHERE guild_id=%s
    ORDER BY date DESC, time DESC
    LIMIT %s
    """
    async with pool.acquire() as conn:
        async with conn.cursor(DictCursor) as cur:
            await cur.execute(sql, (guild.id, DISPLAY_CHAT_MESSAGES))
            rows = await cur.fetchall()

    rows = list(reversed(rows))  # show oldest -> newest
    embed = build_chat_embed(server_name=server_name, rows=rows, bot=bot, server_location=server_location)

    new_id, action = await upsert_rolling_embed(
        pool=pool,
        guild_id=guild.id,
        channel=channel,
        message_id=settings.get("chat_message"),
        embed=embed,
        settings_column="chat_message",
    )
    settings["chat_message"] = new_id

    return action


# ==========================================================
# Rolling: Online Players (from login_logs current status)
# ==========================================================
async def update_online_players(
    *,
    bot: discord.Client,
    pool: aiomysql.Pool,
    guild: discord.Guild,
    settings: dict,
    server_name: str,
    server_location: str | None,
) -> str:
    if int(settings.get("post_logins", 0) or 0) != 1:
        return "disabled"

    channel_id = settings.get("logins_channel")
    if not channel_id:
        return "missing_channel"

    channel = guild.get_channel(int(channel_id))
    if not isinstance(channel, discord.TextChannel):
        return "invalid_channel"

    sql = """
    SELECT steam_id, username, x, y, z, last_seen
    FROM login_logs
    WHERE guild_id=%s AND status='logged in'
    ORDER BY username ASC
    """
    async with pool.acquire() as conn:
        async with conn.cursor(DictCursor) as cur:
            await cur.execute(sql, (guild.id,))
            online_rows = await cur.fetchall()

    embed = build_online_embed(
        server_name=server_name,
        online_rows=online_rows,
        bot=bot,
        server_location=server_location,
    )

    new_id, action = await upsert_rolling_embed(
        pool=pool,
        guild_id=guild.id,
        channel=channel,
        message_id=settings.get("logins_message"),
        embed=embed,
        settings_column="logins_message",
    )
    settings["logins_message"] = new_id

    return action


# ==========================================================
# Rolling: Bounty Board
# ==========================================================
async def update_bounty_board(
    *,
    bot: discord.Client,
    pool: aiomysql.Pool,
    guild: discord.Guild,
    settings: dict,
    server_name: str,
    server_location: str | None,
) -> str:
    if int(settings.get("post_bounties", 0) or 0) != 1:
        return "disabled"

    channel_id = settings.get("bounty_channel")
    if not channel_id:
        return "missing_channel"

    channel = guild.get_channel(int(channel_id))
    if not isinstance(channel, discord.TextChannel):
        return "invalid_channel"

    async with pool.acquire() as conn:
        async with conn.cursor(DictCursor) as cur:
            await cur.execute(
                """
                SELECT target_steam_id,
                       target_username,
                       COUNT(*) AS num_bounties,
                       SUM(amount) AS total_amount
                FROM bounties
                WHERE guild_id=%s AND status='active'
                GROUP BY target_steam_id, target_username
                ORDER BY total_amount DESC, target_username ASC
                """,
                (guild.id,),
            )
            rows = await cur.fetchall()

    title_prefix = BOT_SETTINGS.get("name", "SCUMBot")
    embed = discord.Embed(
        title=f"{title_prefix} ┃ 🎯 Bounty Board — {server_name}",
        color=SCUM_ORANGE,
        url=BOT_SETTINGS.get("website") or discord.Embed.Empty,
    )

    if not rows:
        embed.description = "_No active bounties right now._\n\nPlace a bounty using the **Place bounty** button on kill feeds."
    else:
        lines: List[str] = []
        lines.append("**Target** ┃ **Bounties** ┃ **Total Reward**")
        lines.append("```")
        for r in rows:
            uname = (r["target_username"] or "Unknown")[:20]
            count = int(r["num_bounties"] or 0)
            total = int(r["total_amount"] or 0)
            lines.append(f"{uname:<20} x{count:<2}  →  ${total}")
        lines.append("```")
        embed.description = "\n".join(lines)

    if bot.user:
        embed.set_thumbnail(url=bot.user.display_avatar.url)
    apply_scumbot_footer(embed, bot=bot, server_location=server_location)

    new_id, action = await upsert_rolling_embed(
        pool=pool,
        guild_id=guild.id,
        channel=channel,
        message_id=settings.get("bounty_message"),
        embed=embed,
        settings_column="bounty_message",
    )
    settings["bounty_message"] = new_id

    return action


# ==========================================================
# PvP Board (rolling embed) + optional payouts
# ==========================================================
def build_pvp_board_embed(
    *,
    server_name: str,
    server_location: str | None,
    bot: discord.Client,
    period_type: str,
    period_start: datetime,
    period_end: datetime,
    prize_pool: int,
    rows: List[dict],
) -> discord.Embed:
    period_type = (period_type or "weekly").lower()
    period_label = "Weekly" if period_type == "weekly" else "Monthly"

    embed = discord.Embed(
        title=f"{period_label} Top PvP Kills — {server_name}",
        color=SCUM_ORANGE,
        url=BOT_SETTINGS.get("website") or discord.Embed.Empty,
    )

    start_label = period_start.strftime("%Y-%m-%d")
    end_label = (period_end - timedelta(seconds=1)).strftime("%Y-%m-%d")

    desc_lines = [
        "**How this works:** This board tracks the **Top 10 PvP kills** for the current period.",
        f"**Period:** **{start_label} → {end_label}** (UTC)",
        f"**Updates:** Every **{UPDATE_INTERVAL}s** (rolling embed).",
    ]

    if prize_pool > 0:
        desc_lines.append(f"**Prize Pool:** **{prize_pool}** (tournament split across Top 10)")
        desc_lines.append("**Payout:** Issued when the period ends (if enabled by the owner).")
    else:
        desc_lines.append("**Prize Pool:** Disabled")

    embed.description = "\n".join(desc_lines)

    if not rows:
        embed.add_field(
            name="Top PvP Players",
            value="No PvP kills recorded for this period yet.",
            inline=False,
        )
        apply_scumbot_footer(embed, bot=bot, server_location=server_location)
        return embed

    for r in rows[:10]:
        rank = int(r.get("rank") or 0)
        name = r.get("player_name") or r.get("steam_id") or "Unknown"

        kills = int(r.get("kills") or 0)
        avgd = float(r.get("avg_distance") or 0.0)
        weapon = r.get("top_weapon_name") or "Unknown"
        prize = int(r.get("prize") or 0)

        embed.add_field(name=f"#{rank} Player", value=f"**{name}**", inline=True)
        embed.add_field(name="Weapon", value=f"**{weapon}**", inline=True)
        embed.add_field(name="Kills", value=f"**{kills}**", inline=True)

        embed.add_field(name="Avg Distance", value=f"**{avgd:.1f} m**", inline=True)
        embed.add_field(name="Prize", value=f"**{prize}**", inline=True)
        embed.add_field(name="Period", value=f"**{period_label}**", inline=True)

        embed.add_field(name="\u200b", value="\u200b", inline=False)

    thumb = rows[0].get("top_weapon_img")
    if isinstance(thumb, str) and thumb.strip().startswith("http"):
        embed.set_thumbnail(url=thumb)

    apply_scumbot_footer(embed, bot=bot, server_location=server_location)
    return embed


async def update_pvp_board_and_optional_payout(
    *,
    bot: discord.Client,
    pool: aiomysql.Pool,
    guild: discord.Guild,
    settings: dict,
    server_name: str,
    server_location: str | None,
) -> str:
    if int(settings.get("post_pvp_board", 0) or 0) != 1:
        return "disabled"

    channel_id = settings.get("pvp_channel")
    if not channel_id:
        return "missing_channel"

    channel = guild.get_channel(int(channel_id))
    if not isinstance(channel, discord.TextChannel):
        return "invalid_channel"

    period_type = (settings.get("pvp_period") or "weekly").lower()
    prize_pool = int(settings.get("pvp_prize") or 0)
    payout_enabled = int(settings.get("pvp_payout") or 0) == 1

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    cur_start, cur_end, prev_start, prev_end = period_window(now_utc, period_type)

    top = await fetch_top_pvp(pool, guild.id, cur_start, cur_end)
    payouts = split_prize_pool(prize_pool) if prize_pool > 0 else [0] * 10

    async def _enrich_one(i: int, r: dict) -> dict:
        r["rank"] = i + 1
        r["prize"] = payouts[i] if i < 10 else 0
        tw = await fetch_top_weapon_for_player_period(pool, guild.id, str(r["steam_id"]), cur_start, cur_end)
        clean, img = WEAPON_CATALOG.resolve(tw or "")
        r["top_weapon_name"] = clean
        r["top_weapon_img"] = img
        return r

    if top:
        await asyncio.gather(*[_enrich_one(i, r) for i, r in enumerate(top[:10])])

    embed = build_pvp_board_embed(
        server_name=server_name,
        server_location=server_location,
        bot=bot,
        period_type=period_type,
        period_start=cur_start,
        period_end=cur_end,
        prize_pool=prize_pool,
        rows=top[:10],
    )

    new_id, action = await upsert_rolling_embed(
        pool=pool,
        guild_id=guild.id,
        channel=channel,
        message_id=settings.get("pvp_message"),
        embed=embed,
        settings_column="pvp_message",
    )
    settings["pvp_message"] = new_id

    if payout_enabled and prize_pool > 0:
        already = await has_paid_period(pool, guild.id, period_type, prev_start)
        if not already:
            prev_top = await fetch_top_pvp(pool, guild.id, prev_start, prev_end)
            prev_payouts = split_prize_pool(prize_pool)

            dm_sent = 0
            dm_failed = 0
            paid_any = False

            for i, r in enumerate(prev_top[:10]):
                r["rank"] = i + 1
                r["prize"] = prev_payouts[i] if i < 10 else 0

                tw = await fetch_top_weapon_for_player_period(pool, guild.id, str(r["steam_id"]), prev_start, prev_end)
                clean, img = WEAPON_CATALOG.resolve(tw or "")
                r["top_weapon_name"] = clean
                r["top_weapon_img"] = img

                amt = int(r.get("prize") or 0)
                if amt > 0:
                    await award_cash(pool, guild.id, str(r["steam_id"]), amt)
                    paid_any = True

                sent = await try_send_pvp_payout_dm(
                    bot=bot,
                    pool=pool,
                    guild_id=guild.id,
                    server_name=server_name,
                    server_location=server_location,
                    period_type=period_type,
                    period_start=prev_start,
                    period_end=prev_end,
                    payout_row=r,
                )
                if sent:
                    dm_sent += 1
                else:
                    dm_failed += 1

            await mark_paid_period(pool, guild.id, period_type, prev_start, prev_end)

            if paid_any:
                try:
                    summary = discord.Embed(
                        title="PvP Payouts Issued",
                        description=(
                            f"Payouts have been issued for the completed **{period_type}** period.\n"
                            f"**Prize Pool:** **${prize_pool:,}**\n"
                            f"**DMs Sent:** **{dm_sent}** ┃ **DMs Failed:** **{dm_failed}**"
                        ),
                        color=SCUM_ORANGE,
                        url=BOT_SETTINGS.get("website") or discord.Embed.Empty,
                    )
                    apply_scumbot_footer(summary, bot=bot, server_location=server_location)
                    await channel.send(embed=summary)
                except Exception:
                    pass

    return action


# ==========================================================
# All-time leaderboard (rolling embed)
# ==========================================================
def build_all_time_leaderboard_embed(
    *,
    server_name: str,
    server_location: str | None,
    bot: discord.Client,
    rows: List[dict],
) -> discord.Embed:
    embed = discord.Embed(
        title=f"All-Time Leaderboard — {server_name}",
        color=SCUM_ORANGE,
        url=BOT_SETTINGS.get("website") or discord.Embed.Empty,
    )

    embed.description = (
        "**How this works:** This is the **Top 10 all-time** performance on this server.\n"
        f"**Updates:** Every **{UPDATE_INTERVAL}s** (rolling embed).\n"
        "**Wipes/Resets:** Only changes as players earn stats. Wiped only if the DB/stat tables are reset.\n\n"
    )

    if not rows:
        embed.add_field(name="Top 10", value="No weapon statistics recorded yet.", inline=False)
        apply_scumbot_footer(embed, bot=bot, server_location=server_location)
        return embed

    for r in rows[:10]:
        rank = int(r.get("rank") or 0)
        name = r.get("player_name") or r.get("steam_id") or "Unknown"

        kills = int(r.get("kills") or 0)
        deaths = int(r.get("deaths") or 0)
        kd = safe_kd(kills, deaths)

        longest = float(r.get("longest_kill") or 0.0)
        avgd = float(r.get("avg_distance") or 0.0)

        weap = r.get("top_weapon_name") or "Unknown"

        embed.add_field(name=f"#{rank} Player", value=f"**{name}**", inline=True)
        embed.add_field(name="Weapon", value=f"**{weap}**", inline=True)
        embed.add_field(name="Longest Kill", value=f"**{longest:.1f} m**", inline=True)

        embed.add_field(name="Kills", value=f"**{kills}**", inline=True)
        embed.add_field(name="Deaths", value=f"**{deaths}**", inline=True)
        embed.add_field(name="KD / Avg Distance", value=f"**{kd:.2f}** / **{avgd:.1f} m**", inline=True)

        embed.add_field(name="\u200b", value="\u200b", inline=False)

    thumb = rows[0].get("top_weapon_img")
    if isinstance(thumb, str) and thumb.strip().startswith("http"):
        embed.set_thumbnail(url=thumb)

    apply_scumbot_footer(embed, bot=bot, server_location=server_location)
    return embed


async def update_all_time_leaderboard(
    *,
    bot: discord.Client,
    pool: aiomysql.Pool,
    guild: discord.Guild,
    settings: dict,
    server_name: str,
    server_location: str | None,
) -> str:
    if int(settings.get("post_leaderboard", 0) or 0) != 1:
        return "disabled"

    channel_id = settings.get("leaderboard_channel")
    if not channel_id:
        return "missing_channel"

    channel = guild.get_channel(int(channel_id))
    if not isinstance(channel, discord.TextChannel):
        return "invalid_channel"

    rows = await fetch_leaderboard_all_time(pool, guild.id)

    async def _enrich_lb(i: int, r: dict) -> dict:
        r["rank"] = i + 1
        tw = await fetch_top_weapon_for_player_all_time(pool, guild.id, str(r["steam_id"]))
        clean, img = WEAPON_CATALOG.resolve(tw or "")
        r["top_weapon_name"] = clean
        r["top_weapon_img"] = img
        return r

    if rows:
        await asyncio.gather(*[_enrich_lb(i, r) for i, r in enumerate(rows[:10])])

    embed = build_all_time_leaderboard_embed(
        server_name=server_name,
        server_location=server_location,
        bot=bot,
        rows=rows[:10],
    )

    new_id, action = await upsert_rolling_embed(
        pool=pool,
        guild_id=guild.id,
        channel=channel,
        message_id=settings.get("leaderboard_message"),
        embed=embed,
        settings_column="leaderboard_message",
    )
    settings["leaderboard_message"] = new_id

    return action


# ==========================================================
# Per-event feeds (checkpoint by ID)
# ==========================================================
async def _seed_max_id(pool: aiomysql.Pool, guild_id: int, table: str) -> int:
    sql = f"SELECT MAX(id) AS max_id FROM {table} WHERE guild_id=%s"
    async with pool.acquire() as conn:
        async with conn.cursor(DictCursor) as cur:
            await cur.execute(sql, (guild_id,))
            row = await cur.fetchone()
            mid = row.get("max_id") if row else None
            return int(mid or 0)


async def _fetch_rows_since_id(pool: aiomysql.Pool, guild_id: int, sql: str, args: tuple) -> List[dict]:
    async with pool.acquire() as conn:
        async with conn.cursor(DictCursor) as cur:
            await cur.execute(sql, args)
            return await cur.fetchall()


async def update_kill_feed(
    *,
    bot: discord.Client,
    pool: aiomysql.Pool,
    guild: discord.Guild,
    settings: dict,
    server_name: str,
    server_location: str | None,
) -> int:
    if int(settings.get("post_kills", 0) or 0) != 1:
        return 0

    channel_id = settings.get("kill_channel")
    if not channel_id:
        return 0

    channel = guild.get_channel(int(channel_id))
    if not isinstance(channel, discord.TextChannel):
        return 0

    gid = guild.id
    last_id = LAST_KILL_ID.get(gid)
    if last_id is None:
        # Seed to newest id to avoid flooding historical rows on first boot.
        LAST_KILL_ID[gid] = await _seed_max_id(pool, gid, "kill_logs")
        return 0

    sql = """
    SELECT
      k.id,
      k.ts, k.killer_steam_id, k.killer_username,
      k.victim_steam_id, k.victim_username,
      k.weapon, k.distance,
      k.victim_x, k.victim_y,
      k.src_tag, k.bounty_reward,
      ks.kills AS killer_kills, ks.deaths AS killer_deaths,
      vs.kills AS victim_kills, vs.deaths AS victim_deaths
    FROM kill_logs k
    LEFT JOIN player_statistics ks
      ON ks.guild_id=k.guild_id AND ks.steam_id=k.killer_steam_id
    LEFT JOIN player_statistics vs
      ON vs.guild_id=k.guild_id AND vs.steam_id=k.victim_steam_id
    WHERE k.guild_id=%s AND k.id > %s
    ORDER BY k.id ASC
    """
    rows = await _fetch_rows_since_id(pool, gid, sql, (gid, last_id))

    posted = 0
    for r in rows:
        try:
            LAST_KILL_ID[gid] = int(r["id"])
        except Exception:
            pass

        embed = build_kill_embed(
            server_name=server_name,
            row=r,
            bot=bot,
            guild=guild,
            server_location=server_location,
        )
        await channel.send(embed=embed)
        posted += 1

    return posted


async def update_sentry_feed(
    *,
    bot: discord.Client,
    pool: aiomysql.Pool,
    guild: discord.Guild,
    settings: dict,
    server_name: str,
    server_location: str | None,
) -> int:
    if int(settings.get("post_sentries", 0) or 0) != 1:
        return 0

    channel_id = settings.get("sentry_channel")
    if not channel_id:
        return 0

    channel = guild.get_channel(int(channel_id))
    if not isinstance(channel, discord.TextChannel):
        return 0

    gid = guild.id
    last_id = LAST_SENTRY_ID.get(gid)
    if last_id is None:
        LAST_SENTRY_ID[gid] = await _seed_max_id(pool, gid, "sentry_logs")
        return 0

    sql = """
    SELECT id, ts, killer_steam_id, killer_username, weapon, damage, x, y, z
    FROM sentry_logs
    WHERE guild_id=%s AND id > %s
    ORDER BY id ASC
    """
    rows = await _fetch_rows_since_id(pool, gid, sql, (gid, last_id))

    posted = 0
    for r in rows:
        try:
            LAST_SENTRY_ID[gid] = int(r["id"])
        except Exception:
            pass

        embed = build_sentry_embed(
            server_name=server_name,
            row=r,
            bot=bot,
            guild=guild,
            server_location=server_location,
        )
        await channel.send(embed=embed)
        posted += 1

    return posted


# ==========================================================
# Main loop entrypoint called by app.py
# ==========================================================
async def run_updater_loop(bot: discord.Client, pool: aiomysql.Pool) -> None:
    global DB_POOL
    DB_POOL = pool

    await bot.wait_until_ready()
    await ensure_bot_settings_loaded(pool)

    logger.info(
        "Updater started (interval=%ss; rolling=chat,online,bounty,pvp,leaderboard,admin; events=kill,sentry)",
        UPDATE_INTERVAL,
    )

    tick = 0
    while not bot.is_closed():
        tick += 1
        try:
            tick_start = asyncio.get_running_loop().time()
            async with pool.acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute("SELECT * FROM server_settings")
                    guild_settings = await cur.fetchall()

            for settings in guild_settings:
                guild_id = int(settings["guild_id"])
                guild = bot.get_guild(guild_id)
                if not guild:
                    continue

                server_name = settings.get("server_name") or f"Guild {guild_id}"
                server_location = settings.get("server_location")
                server_ctx = server_label(guild_id, server_name)
                lg = logging.LoggerAdapter(logger, {"server": server_ctx})

                # Run each update and capture a short status for a single-line tick summary.
                statuses: dict[str, str] = {}
                events: dict[str, int] = {"kill": 0, "sentry": 0}

                guild_tick_start = asyncio.get_running_loop().time()

                try:
                    statuses["chat"] = await update_chat_feed(
                        bot=bot,
                        pool=pool,
                        guild=guild,
                        settings=settings,
                        server_name=server_name,
                        server_location=server_location,
                    )
                except Exception as e:
                    err = new_error_id()
                    lg.error("ERR-%s chat update failed: %s", err, e)
                    statuses["chat"] = "error"

                if statuses.get("chat") in ("missing_channel", "invalid_channel"):
                    warn_ratelimited(logging.getLogger("config"), key=f"chat_channel:{guild_id}", message="Chat feed enabled but chat_channel is not set/invalid; skipping.", every_seconds=3600, server=server_ctx)

                try:
                    statuses["online"] = await update_online_players(
                        bot=bot,
                        pool=pool,
                        guild=guild,
                        settings=settings,
                        server_name=server_name,
                        server_location=server_location,
                    )
                except Exception as e:
                    err = new_error_id()
                    lg.error("ERR-%s online update failed: %s", err, e)
                    statuses["online"] = "error"

                if statuses.get("online") in ("missing_channel", "invalid_channel"):
                    warn_ratelimited(logging.getLogger("config"), key=f"logins_channel:{guild_id}", message="Online list enabled but logins_channel is not set/invalid; skipping.", every_seconds=3600, server=server_ctx)

                try:
                    statuses["bounty"] = await update_bounty_board(
                        bot=bot,
                        pool=pool,
                        guild=guild,
                        settings=settings,
                        server_name=server_name,
                        server_location=server_location,
                    )
                except Exception as e:
                    err = new_error_id()
                    lg.error("ERR-%s bounty update failed: %s", err, e)
                    statuses["bounty"] = "error"

                if statuses.get("bounty") in ("missing_channel", "invalid_channel"):
                    warn_ratelimited(logging.getLogger("config"), key=f"bounty_channel:{guild_id}", message="Bounty board enabled but bounty_channel is not set/invalid; skipping.", every_seconds=3600, server=server_ctx)

                try:
                    statuses["pvp"] = await update_pvp_board_and_optional_payout(
                        bot=bot,
                        pool=pool,
                        guild=guild,
                        settings=settings,
                        server_name=server_name,
                        server_location=server_location,
                    )
                except Exception as e:
                    err = new_error_id()
                    lg.error("ERR-%s pvp update failed: %s", err, e)
                    statuses["pvp"] = "error"

                if statuses.get("pvp") in ("missing_channel", "invalid_channel"):
                    warn_ratelimited(logging.getLogger("config"), key=f"pvp_channel:{guild_id}", message="PvP board enabled but pvp_channel is not set/invalid; skipping.", every_seconds=3600, server=server_ctx)

                try:
                    statuses["leaderboard"] = await update_all_time_leaderboard(
                        bot=bot,
                        pool=pool,
                        guild=guild,
                        settings=settings,
                        server_name=server_name,
                        server_location=server_location,
                    )
                except Exception as e:
                    err = new_error_id()
                    lg.error("ERR-%s leaderboard update failed: %s", err, e)
                    statuses["leaderboard"] = "error"

                if statuses.get("leaderboard") in ("missing_channel", "invalid_channel"):
                    warn_ratelimited(logging.getLogger("config"), key=f"leaderboard_channel:{guild_id}", message="Leaderboard enabled but leaderboard_channel is not set/invalid; skipping.", every_seconds=3600, server=server_ctx)

                try:
                    statuses["admin"] = await update_admin_board(
                        bot=bot,
                        pool=pool,
                        guild=guild,
                        settings=settings,
                        server_name=server_name,
                        server_location=server_location,
                    )
                except Exception as e:
                    err = new_error_id()
                    lg.error("ERR-%s admin board update failed: %s", err, e)
                    statuses["admin"] = "error"

                if statuses.get("admin") in ("missing_channel", "invalid_channel"):
                    warn_ratelimited(logging.getLogger("config"), key=f"admin_channel:{guild_id}", message="Admin board enabled but admin_channel is not set/invalid; skipping.", every_seconds=3600, server=server_ctx)

                # Per-event feeds (counts only)
                try:
                    events["sentry"] = await update_sentry_feed(
                        bot=bot,
                        pool=pool,
                        guild=guild,
                        settings=settings,
                        server_name=server_name,
                        server_location=server_location,
                    )
                except Exception as e:
                    err = new_error_id()
                    lg.error("ERR-%s sentry feed update failed: %s", err, e)

                try:
                    events["kill"] = await update_kill_feed(
                        bot=bot,
                        pool=pool,
                        guild=guild,
                        settings=settings,
                        server_name=server_name,
                        server_location=server_location,
                    )
                except Exception as e:
                    err = new_error_id()
                    lg.error("ERR-%s kill feed update failed: %s", err, e)

                guild_ms = int((asyncio.get_running_loop().time() - guild_tick_start) * 1000)
                msg = (
                    f"tick ok | rolling: "
                    f"chat={statuses.get('chat','?')} online={statuses.get('online','?')} "
                    f"bounty={statuses.get('bounty','?')} pvp={statuses.get('pvp','?')} "
                    f"lb={statuses.get('leaderboard','?')} admin={statuses.get('admin','?')} "
                    f"| events: kill={events['kill']} sentry={events['sentry']} "
                    f"| {guild_ms}ms"
                )

                if tick % 5 == 0:
                    lg.info(msg)
                else:
                    lg.debug(msg)

            tick_ms = int((asyncio.get_running_loop().time() - tick_start) * 1000)
            if tick % 5 == 0:
                logger.info("Tick complete (%sms, servers=%s)", tick_ms, len(guild_settings))

            await asyncio.sleep(UPDATE_INTERVAL)

        except Exception as e:
            err = new_error_id()
            logger.error("ERR-%s updater loop error: %s", err, e)
            await asyncio.sleep(UPDATE_INTERVAL)


# ==========================================================
# PvP payout DM helpers (unchanged)
# ==========================================================
async def fetch_discord_id_for_steam_id(pool: aiomysql.Pool, guild_id: int, steam_id: str) -> Optional[int]:
    sql = """
    SELECT discord_id
    FROM player_statistics
    WHERE guild_id=%s AND steam_id=%s
    LIMIT 1;
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (guild_id, steam_id))
            row = await cur.fetchone()
            if not row:
                return None
            discord_id = row[0]
            return int(discord_id) if discord_id else None


def build_pvp_payout_dm_embed(
    *,
    server_name: str,
    server_location: str | None,
    bot: discord.Client,
    period_type: str,
    period_start: datetime,
    period_end: datetime,
    rank: int,
    player_name: str,
    kills: int,
    avg_distance: float,
    weapon_name: str,
    prize: int,
    weapon_img: str | None,
) -> discord.Embed:
    period_type = (period_type or "weekly").lower()
    period_label = "Weekly" if period_type == "weekly" else "Monthly"
    start_label = period_start.strftime("%Y-%m-%d")
    end_label = (period_end - timedelta(seconds=1)).strftime("%Y-%m-%d")

    embed = discord.Embed(
        title=f"{period_label} PvP Payout — {server_name}",
        color=SCUM_ORANGE,
        url=BOT_SETTINGS.get("website") or discord.Embed.Empty,
    )

    embed.description = (
        "Your PvP payout has been issued for the completed period.\n"
        f"**Period:** **{start_label} → {end_label}** (UTC)"
    )

    embed.add_field(name="Rank", value=f"**#{rank}**", inline=True)
    embed.add_field(name="Prize", value=f"**${prize:,}**", inline=True)
    embed.add_field(name="Weapon", value=f"**{weapon_name}**", inline=True)

    embed.add_field(name="Kills", value=f"**{kills:,}**", inline=True)
    embed.add_field(name="Avg Distance", value=f"**{avg_distance:.1f} m**", inline=True)
    embed.add_field(name="Player", value=f"**{player_name}**", inline=True)

    if weapon_img and str(weapon_img).startswith("http"):
        embed.set_thumbnail(url=weapon_img)

    apply_scumbot_footer(embed, bot=bot, server_location=server_location)
    return embed


async def try_send_pvp_payout_dm(
    *,
    bot: discord.Client,
    pool: aiomysql.Pool,
    guild_id: int,
    server_name: str,
    server_location: str | None,
    period_type: str,
    period_start: datetime,
    period_end: datetime,
    payout_row: dict,
) -> bool:
    steam_id = str(payout_row.get("steam_id") or "")
    if not steam_id:
        return False

    discord_id = await fetch_discord_id_for_steam_id(pool, guild_id, steam_id)
    if not discord_id:
        return False

    try:
        user = bot.get_user(discord_id) or await bot.fetch_user(discord_id)
    except Exception:
        return False

    embed = build_pvp_payout_dm_embed(
        server_name=server_name,
        server_location=server_location,
        bot=bot,
        period_type=period_type,
        period_start=period_start,
        period_end=period_end,
        rank=int(payout_row.get("rank") or 0),
        player_name=payout_row.get("player_name") or steam_id,
        kills=int(payout_row.get("kills") or 0),
        avg_distance=float(payout_row.get("avg_distance") or 0.0),
        weapon_name=payout_row.get("top_weapon_name") or "Unknown",
        prize=int(payout_row.get("prize") or 0),
        weapon_img=payout_row.get("top_weapon_img"),
    )

    try:
        await user.send(embed=embed)
        return True
    except Exception:
        return False
