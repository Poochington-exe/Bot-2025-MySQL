"""scumbot.logging_utils

Centralised logging configuration for SCUMBot.

Design goals:
- Single, uniform console format across bot + downloader + updater
- UTC timestamps (to avoid confusion across hosts/timezones)
- Optional context (server name + guild id) without forcing every callsite to supply it
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from typing import Optional


class _RateLimiter:
    """In-process rate limiter for repetitive log lines."""

    def __init__(self) -> None:
        self._next_allowed: dict[str, float] = {}

    def allow(self, key: str, every_seconds: int) -> bool:
        now = time.time()
        next_ok = self._next_allowed.get(key, 0.0)
        if now < next_ok:
            return False
        self._next_allowed[key] = now + float(every_seconds)
        return True


_rate_limiter = _RateLimiter()


class _DefaultFieldsFilter(logging.Filter):
    """Ensure optional fields exist so formatters never KeyError."""

    def filter(self, record: logging.LogRecord) -> bool:
        # A short identifier like: "My Server (1429...)" or "-"
        if not hasattr(record, "server"):
            record.server = "-"
        return True


def setup_logging(level: Optional[str] = None) -> None:
    """Configure process-wide logging once.

    Args:
        level: Logging level name (e.g. 'INFO', 'DEBUG'). If omitted, uses
               SCUMBOT_LOG_LEVEL env var, falling back to 'INFO'.
    """
    level_name = (level or os.getenv("SCUMBOT_LOG_LEVEL", "INFO")).upper()
    numeric_level = getattr(logging, level_name, logging.INFO)

    root = logging.getLogger()
    if getattr(root, "_scumbot_configured", False):
        # Idempotent: safe if called multiple times.
        return

    root.setLevel(numeric_level)

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-5s | %(name)s | %(server)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Force UTC timestamps.
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)
    handler.addFilter(_DefaultFieldsFilter())

    root.addHandler(handler)
    root._scumbot_configured = True  # type: ignore[attr-defined]

    # Reduce third-party noise. We keep warnings/errors, but suppress INFO spam.
    logging.getLogger("discord").setLevel(logging.WARNING)
    logging.getLogger("discord.http").setLevel(logging.WARNING)
    logging.getLogger("discord.gateway").setLevel(logging.WARNING)
    logging.getLogger("asyncssh").setLevel(logging.WARNING)
    logging.getLogger("aioftp").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def server_label(guild_id: int | None, server_name: str | None = None) -> str:
    """Return a consistent server label for logs."""
    if guild_id is None:
        return "-"
    if server_name:
        return f"{server_name} ({guild_id})"
    return str(guild_id)


class ServerLoggerAdapter(logging.LoggerAdapter):
    """Inject a consistent 'server' field so the formatter stays uniform."""

    def __init__(self, logger: logging.Logger, guild_id: int | None, server_name: str | None = None):
        super().__init__(logger, {"server": server_label(guild_id, server_name)})

    @classmethod
    def for_guild(cls, logger_name: str, guild_id: int | None, server_name: str | None = None) -> "ServerLoggerAdapter":
        return cls(logging.getLogger(logger_name), guild_id, server_name)


def new_error_id() -> str:
    """Short correlation id for error lines (useful when users paste logs)."""
    return uuid.uuid4().hex[:6].upper()


def warn_ratelimited(
    logger: logging.Logger,
    *,
    key: str,
    message: str,
    every_seconds: int = 3600,
    server: str | None = None,
) -> None:
    """Emit a WARNING at most once per interval for a given key."""
    if not _rate_limiter.allow(key, every_seconds):
        return
    extra = {"server": server} if server else None
    if extra:
        logger.warning(message, extra=extra)
    else:
        logger.warning(message)
