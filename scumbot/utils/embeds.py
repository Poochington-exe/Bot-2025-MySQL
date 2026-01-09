"""Embed factory and shared footer logic.

This module ensures ALL embeds (bot commands + updater rolling boards) use:
  - The same SCUMBot colour
  - The same footer text format
  - The same flag resolution rules

Keeping it here avoids the common bug where two codepaths set different footers,
which can cause the footer icon to "flash" between values during edits.
"""

from __future__ import annotations

from typing import Optional

import discord

# NOTE:
# This module is imported by both the bot runtime and the updater runtime.
# Avoid importing bot.app or other heavy modules here, otherwise you can create
# circular imports (bot.app -> updater -> utils.embeds -> bot.app).
#
# Instead we keep a lightweight, optional, in-module cache which callers can set
# at startup via set_bot_settings(), or pass ad-hoc via the bot_settings= kwarg.
BOT_SETTINGS: dict = {}


def set_bot_settings(settings: Optional[dict]) -> None:
    """Set the global bot settings cache used by embed helpers.

    This is safe to call from bot startup code after BOT_SETTINGS has been
    loaded from the database.
    """

    global BOT_SETTINGS
    BOT_SETTINGS = dict(settings or {})


from .flags import get_flag_url


def apply_scumbot_footer(
    embed: discord.Embed,
    *,
    bot: Optional[discord.Client],
    server_location: Optional[str],
    bot_settings: Optional[dict] = None,
) -> None:
    """Apply the standard SCUMBot footer.

    If server_location is provided and maps to a flag, we use that flag.
    Otherwise we fall back to the bot avatar (if available).
    """
    settings = bot_settings or BOT_SETTINGS
    name = settings.get("name", "SCUMBot")
    version = settings.get("version", "v1.0.0")
    base_text = f"{name} {version} - Want this bot for your server too?"

    if server_location:
        footer_text = f"SERVER LOCATION: {server_location}\n{base_text}"
    else:
        footer_text = base_text

    icon_url = get_flag_url(server_location)
    if not icon_url and bot and getattr(bot, "user", None):
        icon_url = bot.user.display_avatar.url

    if icon_url:
        embed.set_footer(text=footer_text, icon_url=icon_url)
    else:
        embed.set_footer(text=footer_text)


def create_scumbot_embed(
    title: str,
    description: str = "",
    *,
    color: Optional[discord.Color] = None,
    bot: Optional[discord.Client] = None,
    server_location: Optional[str] = None,
    bot_settings: Optional[dict] = None,
    set_thumbnail: bool = True,
    url: str | None = None,
) -> discord.Embed:
    """Create an embed with SCUMBot defaults and footer applied."""
    if color is None:
        # SCUM-like dark orange (prison jumpsuit vibe)
        color = discord.Color.from_rgb(222, 133, 0)

    if url is None and bot_settings:
        url = bot_settings.get("bot_website") or bot_settings.get("website")

    embed = discord.Embed(title=title, description=description, color=color, url=url)
    apply_scumbot_footer(embed, bot=bot, server_location=server_location, bot_settings=bot_settings)
    return embed
