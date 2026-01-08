# ==========================================================
# SCUMBot – Database Helpers
#
# Features:
#   - Synchronous MySQL connector for the log parser
#   - Async aiomysql pool for the Discord bot/updater
#   - Simple fetch helpers (fetch_one, fetch_all, execute)
# ==========================================================

from __future__ import annotations

import os
from typing import Any, Dict

import aiomysql
import mysql.connector

# Base config shared by sync + async
BASE_DB_CONFIG: Dict[str, Any] = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "scumbot"),
    "autocommit": True,
}

# Synchronous config for mysql.connector
SYNC_DB_CONFIG: Dict[str, Any] = {
    "host": BASE_DB_CONFIG["host"],
    "user": BASE_DB_CONFIG["user"],
    "password": BASE_DB_CONFIG["password"],
    "database": BASE_DB_CONFIG["database"],
    "autocommit": BASE_DB_CONFIG["autocommit"],
}

# Async config for aiomysql (uses "db" instead of "database")
ASYNC_DB_CONFIG: Dict[str, Any] = {
    "host": BASE_DB_CONFIG["host"],
    "user": BASE_DB_CONFIG["user"],
    "password": BASE_DB_CONFIG["password"],
    "db": BASE_DB_CONFIG["database"],
    "autocommit": BASE_DB_CONFIG["autocommit"],
}


def db_connect() -> mysql.connector.MySQLConnection:
    """
    Create a new synchronous MySQL connection.

    Used by the log downloader and any other sync scripts.
    """
    return mysql.connector.connect(**SYNC_DB_CONFIG)


async def create_db_pool(minsize: int = 1, maxsize: int = 5) -> aiomysql.Pool:
    """
    Create an aiomysql connection pool.

    Intended for the Discord bot / updater.
    """
    return await aiomysql.create_pool(minsize=minsize, maxsize=maxsize, **ASYNC_DB_CONFIG)
