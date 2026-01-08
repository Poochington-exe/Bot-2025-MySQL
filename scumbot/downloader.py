# ==========================================================
# SCUMBot – Log Downloader & Parser
#
# IMPORTANT DESIGN:
#   - Downloader ALWAYS parses + stores logs.
#   - Discord posting toggles belong in updater.py.
# ==========================================================

import asyncio
import ftplib
import hashlib
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aioftp
import asyncssh
import requests

from .db import db_connect
from .logging_utils import server_label

import logging
logger = logging.getLogger("downloader")

LOGS_DIR = Path(os.getcwd()) / "Logs"
LOGS_DIR.mkdir(exist_ok=True)

POLL_SECONDS = 20  # how often to poll all guilds

# Transport preference memory (per guild)
# Values: "sftp" | "aioftp" | "safe_ftp"
TRANSPORT_PREF: Dict[int, str] = {}

# If aioftp fails in a known-incompatible way, we back off and prefer safe_ftp.
AIOFTP_BACKOFF_UNTIL: Dict[int, float] = {}
AIOFTP_BACKOFF_SECONDS = 24 * 60 * 60  # 24 hours

# Rate-limit repeated warnings so console stays clean
WARN_SUPPRESS_UNTIL: Dict[Tuple[int, str], float] = {}
WARN_SUPPRESS_SECONDS = 60 * 60  # 1 hour

STEAM_WEB_API_KEY = os.getenv("STEAM_WEB_API_KEY")

# ----------------------------------------------------------
# SCUM Log Regex Patterns
# ----------------------------------------------------------

CHAT_PATTERN = re.compile(
    r"(?P<datetime>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}): "
    r"'(?P<steam_id>\d+):(?P<username>[^(]+)\((?P<player_id>\d+)\)' "
    r"'(?P<chat_type>\w+): (?P<message>.+)'"
)

LOGIN_PATTERN = re.compile(
    r"^(?P<datetime>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}): "
    r"'(?P<ip>\d+\.\d+\.\d+\.\d+)\s+(?P<steam_id>\d+):(?P<username>[^()]+)\((?P<player_id>\d+)\)' "
    r"logged\s+(?P<state>in|out)\s+at:\s+X=(?P<x>[-\d\.]+)\s+Y=(?P<y>[-\d\.]+)\s+Z=(?P<z>[-\d\.]+)",
    re.IGNORECASE,
)

KILL_SUMMARY_PATTERN = re.compile(
    r"^(?P<dt>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}):\s*"
    r"Died:\s*(?P<v_name>.+?)\s*\((?P<v_sid>\d+)\),\s*"
    r"Killer:\s*(?P<k_name>.+?)\s*\((?P<k_sid>\d+)\)\s*"
    r"Weapon:\s*(?P<weapon>.+?)"
    r"(?:\s+S|\s+C|\s+S\[|\s+C\[|$)",
    re.IGNORECASE,
)

SUICIDE_PATTERN = re.compile(
    r"^(?P<dt>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}):\s*"
    r"Comitted suicide\. User:\s*(?P<username>.+?)\s*"
    r"\(\s*(?P<player_id>\d+)\s*,\s*(?P<steam_id>\d+)\s*\),"
    r".*?Location:\s*X=(?P<x>[-\d\.]+)\s+Y=(?P<y>[-\d\.]+)\s+Z=(?P<z>[-\d\.]+)",
    re.IGNORECASE,
)

KILL_DISTANCE_PATTERN = re.compile(r"Distance:\s*(?P<dist>\d+\.?\d*)\s*m", re.IGNORECASE)
JSON_KILL_PATTERN = re.compile(r"^(?P<dt>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}):\s*(\{.*\})$")

ADMIN_PATTERN = re.compile(
    r"^(?P<date>\d{4}\.\d{2}\.\d{2})-(?P<time>\d{2}\.\d{2}\.\d{2}):\s+"
    r"'(?P<steam_id>\d+):(?P<username>[^()]+)\((?P<player_id>\d+)\)'\s+"
    r"Command:\s+'(?P<command>.+)'$"
)

SENTRY_DESTROY_RE = re.compile(
    r"^(?P<ts>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}):\s+\[Sentry\]\s+Was destroyed at the location:\s+"
    r"X=(?P<x>-?\d+(?:\.\d+)?)\s+Y=(?P<y>-?\d+(?:\.\d+)?)\s+Z=(?P<z>-?\d+(?:\.\d+)?),\s+"
    r"by:\s+(?P<killer>.+?)\((?P<steam>\d+)\),\s+using:\s+(?P<weapon>[^,]+),\s+"
    r"last hit caused\s+(?P<damage>-?\d+(?:\.\d+)?)\s+damage\.$"
)

REG_CODE_RE = re.compile(r"SCUMBot-[A-Za-z0-9]{6}-[A-Za-z0-9]{6}", re.IGNORECASE)

# ==========================================================
# ====================== LOG HELPERS =======================
# ==========================================================

def warn_once_per_hour(guild_id: int, key: str, lg: logging.LoggerAdapter, msg: str, *args):
    """
    Rate-limits repeated warnings per guild+key.
    Keeps console readable when a server is permanently misconfigured.
    """
    now = time.time()
    k = (guild_id, key)
    until = WARN_SUPPRESS_UNTIL.get(k, 0.0)
    if now < until:
        lg.debug(msg, *args)
        return
    WARN_SUPPRESS_UNTIL[k] = now + WARN_SUPPRESS_SECONDS
    lg.warning(msg, *args)

def _aioftp_known_incompatible(exc: Exception) -> bool:
    # Common real-world mismatch: aioftp expects EPSV (229) but server replies PASV (227)
    s = str(exc)
    return "Waiting for ('229',) but got 227" in s or "Waiting for ('229',)" in s and "227" in s

def _choose_transport(host: str, port: int) -> str:
    """
    Simple auto mode:
      - Port 22 => SFTP
      - Otherwise => FTP
    Host hints ("sftp") can override.
    """
    if "sftp" in host:
        return "sftp"
    if port == 22:
        return "sftp"
    return "ftp"

# ==========================================================
# ====================== DB HELPERS =========================
# ==========================================================

# ----------------------------------------------------------
# SAFE FTP directory listing helper (MLSD -> NLST -> LIST)
# Some FTP servers disable NLST (502), so SAFE mode must fall back cleanly.
# ----------------------------------------------------------

def ftp_list_files(ftp: ftplib.FTP) -> List[str]:
    """Return filenames in the current FTP directory using robust fallbacks."""
    # 1) MLSD (structured)
    try:
        out: List[str] = []
        for name, facts in ftp.mlsd():
            if isinstance(facts, dict) and facts.get("type") == "dir":
                continue
            if name:
                out.append(name)
        if out:
            return out
    except Exception:
        pass

    # 2) NLST (simple listing)
    try:
        return [n for n in ftp.nlst() if n]
    except Exception:
        pass

    # 3) LIST (parse last column)
    lines: List[str] = []
    ftp.retrlines("LIST", lines.append)

    files: List[str] = []
    for ln in lines:
        parts = ln.split(maxsplit=8)
        if len(parts) == 9:
            # skip directories if LIST begins with 'd'
            if parts[0].startswith("d"):
                continue
            name = parts[-1]
            if name:
                files.append(name)
    return files


def get_guild_rows() -> List[Dict[str, Any]]:
    """
    Load all guilds that have FTP/SFTP credentials configured.
    Also loads post_sentries so downloader can decide whether to increment sentry_kills.
    """
    conn = db_connect()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT guild_id, server_name,
               ftp_host, ftp_port, ftp_user, ftp_pass, ftp_dir,
               post_sentries
        FROM server_settings
        WHERE ftp_host IS NOT NULL AND ftp_user IS NOT NULL AND ftp_dir IS NOT NULL
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def get_server_counts() -> tuple[int, int]:
    """Return (total_servers, ftp_configured_servers)."""
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM server_settings")
    total = int(cur.fetchone()[0] or 0)
    cur.execute(
        """
        SELECT COUNT(*)
        FROM server_settings
        WHERE ftp_host IS NOT NULL AND ftp_user IS NOT NULL AND ftp_dir IS NOT NULL
        """
    )
    configured = int(cur.fetchone()[0] or 0)
    cur.close()
    conn.close()
    return total, configured

# ----------------------------------------------------------
# parsed_logs checkpoint helpers
# ----------------------------------------------------------

def get_parsed_checkpoint(guild_id: int, log_type: str) -> Dict[str, Any]:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT last_file, last_line, last_timestamp,
               last_file_size, last_checksum, last_parse, last_message
        FROM parsed_logs
        WHERE guild_id=%s AND log_type=%s
        """,
        (guild_id, log_type),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return {
            "last_file": None,
            "last_line": -1,
            "last_timestamp": None,
            "last_file_size": None,
            "last_checksum": None,
            "last_parse": None,
            "last_message": None,
        }

    return {
        "last_file": row[0],
        "last_line": int(row[1]) if row[1] is not None else -1,
        "last_timestamp": row[2],
        "last_file_size": int(row[3]) if row[3] is not None else None,
        "last_checksum": row[4],
        "last_parse": row[5],
        "last_message": row[6],
    }

def update_parsed_checkpoint(
    guild_id: int,
    log_type: str,
    last_file: str,
    last_line: int,
    last_timestamp: Optional[str],
    last_file_size: int,
    last_checksum: Optional[str],
    last_message: Optional[str],
):
    if last_checksum is None:
        last_checksum = ""
    if last_message is None:
        last_message = ""

    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO parsed_logs (
            guild_id, log_type,
            last_file, last_line, last_timestamp,
            last_file_size, last_checksum, last_parse, last_message
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,NOW(),%s)
        ON DUPLICATE KEY UPDATE
            last_file      = VALUES(last_file),
            last_line      = VALUES(last_line),
            last_timestamp = VALUES(last_timestamp),
            last_file_size = VALUES(last_file_size),
            last_checksum  = VALUES(last_checksum),
            last_parse     = VALUES(last_parse),
            last_message   = VALUES(last_message)
        """,
        (
            guild_id,
            log_type,
            last_file,
            last_line,
            last_timestamp,
            last_file_size,
            last_checksum,
            last_message,
        ),
    )
    conn.commit()
    cur.close()
    conn.close()

# ==========================================================
# ======================= PARSERS ==========================
# ==========================================================

def parse_kill_lines(lines: List[str], guild_id: int) -> List[dict]:
    events: List[dict] = []
    blocks: Dict[str, Dict[str, Any]] = {}

    def safe_float(v):
        try:
            return float(v)
        except Exception:
            return None

    for raw in lines:
        line = (raw or "").strip()
        if not line or len(line) < 30:
            continue

        msu = SUICIDE_PATTERN.match(line)
        if msu:
            g = msu.groupdict()
            ts_str = g["dt"]
            try:
                ts_obj = datetime.strptime(ts_str, "%Y.%m.%d-%H.%M.%S")
            except ValueError:
                ts_obj = datetime.utcnow()

            username = g["username"].strip()
            steam_id = int(g["steam_id"])
            player_id = int(g["player_id"])
            x = safe_float(g.get("x"))
            y = safe_float(g.get("y"))
            z = safe_float(g.get("z"))

            events.append(
                {
                    "guild_id": guild_id,
                    "ts": ts_obj,
                    "killer_steam_id": steam_id,
                    "killer_username": username,
                    "killer_player_id": player_id,
                    "victim_steam_id": steam_id,
                    "victim_username": username,
                    "victim_player_id": player_id,
                    "weapon": "Suicide",
                    "distance": None,
                    "killer_x": x,
                    "killer_y": y,
                    "killer_z": z,
                    "victim_x": x,
                    "victim_y": y,
                    "victim_z": z,
                    "time_of_day": None,
                    "src_tag": "SUICIDE",
                }
            )
            continue

        mj = JSON_KILL_PATTERN.match(line)
        if mj:
            ts_str = mj.group("dt")
            try:
                payload_str = line.split(":", 1)[1].strip()
                payload = json.loads(payload_str)
            except Exception:
                continue
            blk = blocks.setdefault(ts_str, {})
            blk["json"] = payload
            continue

        ms = KILL_SUMMARY_PATTERN.match(line)
        if ms:
            g = ms.groupdict()
            ts_str = g["dt"]

            md = KILL_DISTANCE_PATTERN.search(line)
            dist_val = None
            if md:
                try:
                    dist_val = float(md.group("dist"))
                except Exception:
                    dist_val = None

            summary = {
                "ts_str": ts_str,
                "killer_steam_id": int(g["k_sid"]),
                "killer_username": g["k_name"].strip(),
                "victim_steam_id": int(g["v_sid"]),
                "victim_username": g["v_name"].strip(),
                "weapon": g["weapon"].strip(),
                "distance": dist_val,
            }
            blk = blocks.setdefault(ts_str, {})
            blk["summary"] = summary
            continue

    for ts_str, data in blocks.items():
        summary = data.get("summary")
        if not summary:
            continue

        try:
            ts_obj = datetime.strptime(ts_str, "%Y.%m.%d-%H.%M.%S")
        except ValueError:
            ts_obj = datetime.utcnow()

        event = {
            "guild_id": guild_id,
            "ts": ts_obj,
            "killer_steam_id": summary["killer_steam_id"],
            "killer_username": summary["killer_username"],
            "killer_player_id": None,
            "victim_steam_id": summary["victim_steam_id"],
            "victim_username": summary["victim_username"],
            "victim_player_id": None,
            "weapon": summary["weapon"],
            "distance": summary["distance"],
            "killer_x": None,
            "killer_y": None,
            "killer_z": None,
            "victim_x": None,
            "victim_y": None,
            "victim_z": None,
            "time_of_day": None,
            "src_tag": None,
        }

        payload = data.get("json")
        if isinstance(payload, dict):
            try:
                k = payload.get("Killer", {}) or {}
                v = payload.get("Victim", {}) or {}
                tod = payload.get("TimeOfDay")

                ks = k.get("ServerLocation") or {}
                vs = v.get("ServerLocation") or {}

                event["killer_x"] = safe_float(ks.get("X"))
                event["killer_y"] = safe_float(ks.get("Y"))
                event["killer_z"] = safe_float(ks.get("Z"))
                event["victim_x"] = safe_float(vs.get("X"))
                event["victim_y"] = safe_float(vs.get("Y"))
                event["victim_z"] = safe_float(vs.get("Z"))

                if tod:
                    event["time_of_day"] = tod
            except Exception:
                pass

        events.append(event)

    return events

def parse_kill_lines_with_checkpoint(path: Path, guild_id: int) -> List[dict]:
    events: List[dict] = []
    log_file = path.name
    file_size = path.stat().st_size

    cp = get_parsed_checkpoint(guild_id, "kill")
    last_file = cp["last_file"]
    last_line_idx = cp["last_line"]
    last_size = cp["last_file_size"] or 0

    if last_file != log_file or file_size < last_size:
        last_line_idx = -1

    try:
        with open(path, "r", encoding="utf-16-le", errors="ignore") as f:
            lines = f.readlines()

        if last_line_idx >= len(lines) - 1:
            return []

        new_lines = [raw for i, raw in enumerate(lines) if i > last_line_idx]
        if not new_lines:
            return []

        parsed = parse_kill_lines(new_lines, guild_id)
        events.extend(parsed)

        new_last_line = last_line_idx + len(new_lines)

        last_ts = None
        last_sig = ""
        if events:
            last_event = events[-1]
            last_ts = last_event["ts"].strftime("%Y-%m-%d %H:%M:%S")
            last_sig = f"{last_event['killer_steam_id']}->{last_event['victim_steam_id']}:{last_event['weapon']}"

        checksum = hashlib.md5((last_sig or "").encode("utf-8")).hexdigest()

        update_parsed_checkpoint(
            guild_id=guild_id,
            log_type="kill",
            last_file=log_file,
            last_line=new_last_line,
            last_timestamp=last_ts,
            last_file_size=file_size,
            last_checksum=checksum,
            last_message=last_sig,
        )

    except Exception:
        logger.exception("Kill log parse failed (file=%s, guild=%s)", log_file, guild_id)

    return events

def parse_log_file(path: Path, guild_id: int, log_type: str) -> List[Dict[str, str]]:
    entries: List[Dict[str, str]] = []
    log_file = path.name
    file_size = path.stat().st_size

    pattern = CHAT_PATTERN if log_type == "chat" else LOGIN_PATTERN

    cp = get_parsed_checkpoint(guild_id, log_type)
    last_file = cp["last_file"]
    last_line_idx = cp["last_line"]
    last_size = cp["last_file_size"]

    if last_file != log_file or (last_size is not None and file_size < last_size):
        last_line_idx = -1

    seen_lines = set()
    last_processed_index = last_line_idx

    try:
        with open(path, "r", encoding="utf-16-le", errors="ignore") as f:
            for i, raw in enumerate(f):
                if i <= last_line_idx:
                    continue

                line = raw.strip()
                if len(line) < 30:
                    continue

                m = pattern.search(line)
                if not m:
                    continue

                d = m.groupdict()

                date_str, t_full = d.pop("datetime").split("-")
                hh, mm, _ = t_full.split(".")
                d["date"] = date_str.replace(".", "-")
                d["time"] = f"{hh}:{mm}:00"

                line_key = "|".join(str(v) for v in d.values())
                if line_key in seen_lines:
                    continue
                seen_lines.add(line_key)

                entries.append(d)
                last_processed_index = i

        if last_processed_index > last_line_idx:
            last_ts_str = None
            last_msg = ""

            if entries:
                last_entry = entries[-1]
                last_msg = last_entry.get("message", last_entry.get("state", "")) or ""
                last_ts_str = f"{last_entry['date']} {last_entry['time']}"

            checksum_source = f"{entries[-1].get('steam_id','') if entries else ''}|{last_msg}"
            last_checksum = hashlib.md5(checksum_source.encode("utf-8")).hexdigest()

            update_parsed_checkpoint(
                guild_id=guild_id,
                log_type=log_type,
                last_file=log_file,
                last_line=last_processed_index,
                last_timestamp=last_ts_str,
                last_file_size=file_size,
                last_checksum=last_checksum,
                last_message=last_msg,
            )

    except Exception:
        logger.exception("Parse failed (file=%s, type=%s, guild=%s)", log_file, log_type, guild_id)

    return entries

def parse_admin_lines(lines: List[str], guild_id: int) -> List[dict]:
    results: List[dict] = []
    for raw in lines:
        line = raw.strip()
        m = ADMIN_PATTERN.match(line)
        if not m:
            continue

        date = m.group("date")
        time_s = m.group("time")
        ts_str = f"{date.replace('.', '-')}" + " " + f"{time_s.replace('.', ':')}"

        results.append(
            {
                "guild_id": guild_id,
                "ts": ts_str,
                "steam_id": m.group("steam_id"),
                "username": m.group("username"),
                "player_id": int(m.group("player_id")),
                "command": m.group("command"),
                "raw": raw,
            }
        )
    return results

def parse_admin_file(path: Path, guild_id: int) -> List[dict]:
    log_file = path.name
    file_size = path.stat().st_size

    cp = get_parsed_checkpoint(guild_id, "admin")
    last_file = cp["last_file"]
    last_line_idx = cp["last_line"]
    last_size = cp["last_file_size"] or 0

    if last_file != log_file or file_size < last_size:
        last_line_idx = -1

    last_processed_index = last_line_idx
    new_lines: List[str] = []

    try:
        with open(path, "r", encoding="utf-16-le", errors="ignore") as f:
            for i, raw in enumerate(f):
                if i <= last_line_idx:
                    continue
                new_lines.append(raw)
                last_processed_index = i

        entries = parse_admin_lines(new_lines, guild_id)

        last_ts = None
        last_msg = ""
        if entries:
            last_event = entries[-1]
            last_ts = last_event["ts"]
            last_msg = last_event["command"]

        checksum_src = f"{entries[-1]['steam_id']}|{entries[-1]['command']}" if entries else ""
        checksum = hashlib.md5(checksum_src.encode("utf-8")).hexdigest()

        if last_processed_index > last_line_idx:
            update_parsed_checkpoint(
                guild_id=guild_id,
                log_type="admin",
                last_file=log_file,
                last_line=last_processed_index,
                last_timestamp=last_ts,
                last_file_size=file_size,
                last_checksum=checksum,
                last_message=last_msg,
            )

        return entries

    except Exception:
        logger.exception("Admin log parse failed (file=%s, guild=%s)", log_file, guild_id)
        return []

def parse_sentry_file(path: Path, guild_id: int) -> Tuple[List[dict], int]:
    """
    Parses ONLY NEW LINES since checkpoint for log_type='sentry'.
    Returns: (matched_entries, scanned_lines)
    """
    log_file = path.name
    file_size = path.stat().st_size

    cp = get_parsed_checkpoint(guild_id, "sentry")
    last_file = cp["last_file"]
    last_line_idx = cp["last_line"]
    last_size = cp["last_file_size"] or 0

    if last_file != log_file or file_size < last_size:
        last_line_idx = -1

    last_processed_index = last_line_idx
    matched: List[dict] = []

    try:
        with open(path, "r", encoding="utf-16-le", errors="ignore") as f:
            for i, raw in enumerate(f):
                if i <= last_line_idx:
                    continue

                last_processed_index = i
                raw_line = raw.rstrip("\n")
                line = (raw_line or "").strip()
                if not line:
                    continue

                m = SENTRY_DESTROY_RE.match(line)
                if not m:
                    continue

                try:
                    ts = datetime.strptime(m.group("ts"), "%Y.%m.%d-%H.%M.%S")
                except Exception:
                    ts = datetime.utcnow()

                matched.append(
                    {
                        "guild_id": guild_id,
                        "ts": ts,
                        "killer_steam_id": int(m.group("steam")),
                        "killer_username": (m.group("killer") or "").strip(),
                        "weapon": (m.group("weapon") or "").strip(),
                        "damage": float(m.group("damage")),
                        "x": float(m.group("x")),
                        "y": float(m.group("y")),
                        "z": float(m.group("z")),
                        "raw_line": raw_line,
                    }
                )

        if last_processed_index > last_line_idx:
            last_ts = None
            last_msg = ""

            if matched:
                last_ts = matched[-1]["ts"].strftime("%Y-%m-%d %H:%M:%S")
                last_msg = (matched[-1].get("raw_line") or "").strip()
            else:
                last_msg = "advanced_no_match"

            checksum = hashlib.md5((last_msg or "").encode("utf-8")).hexdigest()

            update_parsed_checkpoint(
                guild_id=guild_id,
                log_type="sentry",
                last_file=log_file,
                last_line=last_processed_index,
                last_timestamp=last_ts,
                last_file_size=file_size,
                last_checksum=checksum,
                last_message=last_msg[:512],
            )

    except Exception:
        logger.exception("Sentry log parse failed (file=%s, guild=%s)", log_file, guild_id)

    scanned = (last_processed_index - last_line_idx) if last_processed_index > last_line_idx else 0
    return matched, scanned

# ==========================================================
# ===================== SAVE FUNCTIONS =====================
# ==========================================================

def save_admin_logs_to_mysql(entries: List[dict]):
    if not entries:
        return

    conn = db_connect()
    cur = conn.cursor()

    sql = """
        INSERT INTO admin_logs
            (guild_id, ts, steam_id, username, player_id, command, raw_line)
        VALUES
            (%s,%s,%s,%s,%s,%s,%s)
    """

    vals = [
        (
            e["guild_id"],
            e["ts"],
            e["steam_id"],
            e["username"],
            e["player_id"],
            e["command"],
            e["raw"],
        )
        for e in entries
    ]

    cur.executemany(sql, vals)
    conn.commit()
    cur.close()
    conn.close()

def save_sentry_logs_to_mysql(entries: List[dict], guild_id: int, increment_stats: bool = False) -> None:
    if not entries:
        return

    conn = db_connect()
    try:
        cur = conn.cursor()

        sql = """
        INSERT IGNORE INTO sentry_logs
            (guild_id, ts, killer_steam_id, killer_username, weapon, damage, x, y, z, raw_line)
        VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        values = []
        for e in entries:
            sid = e.get("killer_steam_id")
            try:
                sid_int = int(sid) if sid is not None else None
            except Exception:
                sid_int = None

            values.append(
                (
                    guild_id,
                    e.get("ts"),
                    sid_int,
                    e.get("killer_username"),
                    e.get("weapon"),
                    e.get("damage"),
                    e.get("x"),
                    e.get("y"),
                    e.get("z"),
                    e.get("raw_line"),
                )
            )

        cur.executemany(sql, values)

        if increment_stats:
            counts: Dict[int, int] = {}
            for e in entries:
                sid = e.get("killer_steam_id")
                if not sid:
                    continue
                try:
                    sid_int = int(sid)
                except Exception:
                    continue
                counts[sid_int] = counts.get(sid_int, 0) + 1

            if counts:
                up_sql = """
                UPDATE player_statistics
                SET sentry_kills = COALESCE(sentry_kills, 0) + %s
                WHERE guild_id=%s AND steam_id=%s
                """
                cur.executemany(up_sql, [(cnt, guild_id, sid_int) for sid_int, cnt in counts.items()])

        conn.commit()
    finally:
        conn.close()

def handle_registration_in_chat(cur, guild_id: int, entry: Dict[str, str]):
    message = entry.get("message") or ""
    m = REG_CODE_RE.search(message)
    if not m:
        return

    code = m.group(0)
    steam_id = entry.get("steam_id")
    username = entry.get("username")
    player_id = entry.get("player_id")

    if not steam_id:
        return

    cur.execute(
        """
        SELECT discord_id, code, linked
        FROM pending_links
        WHERE guild_id = %s AND code = %s
        """,
        (guild_id, code),
    )
    row = cur.fetchone()
    if not row:
        return

    discord_id, db_code, linked_flag = row
    if linked_flag:
        return

    cur.execute(
        """
        INSERT INTO player_statistics (
            guild_id, steam_id, discord_id, username, player_id,
            kills, deaths, longest_kill, favorite_weapon, kd_ratio
        )
        VALUES (%s,%s,%s,%s,%s,0,0,0,NULL,0)
        ON DUPLICATE KEY UPDATE
            discord_id = VALUES(discord_id),
            username   = VALUES(username),
            player_id  = VALUES(player_id)
        """,
        (guild_id, steam_id, discord_id, username, player_id),
    )

    cur.execute(
        """
        UPDATE login_logs
        SET username = %s, player_id = %s
        WHERE guild_id = %s AND steam_id = %s
        """,
        (username, player_id, guild_id, steam_id),
    )

    cur.execute(
        """
        UPDATE pending_links
        SET linked = 1
        WHERE guild_id = %s AND discord_id = %s
        """,
        (guild_id, discord_id),
    )

def save_chats_to_mysql(entries: List[Dict[str, str]], guild_id: int):
    if not entries:
        return

    conn = db_connect()
    cur = conn.cursor()

    sql = """
        INSERT INTO chat_logs (guild_id, date, time, steam_id, username, player_id, chat_type, message)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    inserted = 0
    try:
        for e in entries:
            cur.execute(
                sql,
                (
                    guild_id,
                    e["date"],
                    e["time"],
                    e["steam_id"],
                    e["username"],
                    e["player_id"],
                    e["chat_type"],
                    e["message"],
                ),
            )
            inserted += 1
            handle_registration_in_chat(cur, guild_id, e)

        conn.commit()
    except Exception:
        logger.exception("Failed to save chats (guild=%s)", guild_id)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def fetch_steam_ban_info(steam_id: str) -> Optional[dict]:
    if not STEAM_WEB_API_KEY:
        return None

    url = "https://api.steampowered.com/ISteamUser/GetPlayerBans/v1/"
    params = {"key": STEAM_WEB_API_KEY, "steamids": steam_id}

    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            return None
        data = resp.json()
        players = data.get("players", [])
        return players[0] if players else None
    except Exception:
        return None

def scan_steam_bans(guild_id: int, steam_id: str, username: Optional[str] = None):
    if not STEAM_WEB_API_KEY:
        return

    ban_info = fetch_steam_ban_info(steam_id)
    if not ban_info:
        return

    vac_banned = bool(ban_info.get("VACBanned"))
    game_bans = int(ban_info.get("NumberOfGameBans", 0))
    community_banned = bool(ban_info.get("CommunityBanned"))
    econ_ban = (ban_info.get("EconomyBan") or "none").lower()
    days_since_last_ban = int(ban_info.get("DaysSinceLastBan", 0))

    if not (vac_banned or game_bans > 0 or community_banned or econ_ban != "none"):
        return

    sig = f"vac={int(vac_banned)}|games={game_bans}|comm={int(community_banned)}|econ={econ_ban}|days={days_since_last_ban}"

    conn = db_connect()
    try:
        cur = conn.cursor(dictionary=True)

        cur.execute(
            """
            SELECT id FROM steam_ban_events
            WHERE guild_id = %s AND steam_id = %s AND sig = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (guild_id, steam_id, sig),
        )
        if cur.fetchone():
            return

        cur.execute(
            """
            INSERT INTO steam_ban_events
              (guild_id, steam_id, username,
               vac_banned, game_bans, community_banned,
               economy_ban, days_since_last_ban,
               sig, raw_json)
            VALUES
              (%s,%s,%s,
               %s,%s,%s,
               %s,%s,
               %s,%s)
            """,
            (
                guild_id,
                steam_id,
                username,
                int(vac_banned),
                game_bans,
                int(community_banned),
                econ_ban,
                days_since_last_ban,
                sig,
                json.dumps(ban_info),
            ),
        )
        conn.commit()
    except Exception:
        logger.exception("Failed to insert steam_ban_event (guild=%s, steam=%s)", guild_id, steam_id)
    finally:
        conn.close()

def save_logins_to_mysql(entries: List[Dict[str, str]], guild_id: int):
    if not entries:
        return

    conn = db_connect()
    cur = conn.cursor()

    sql = """
        INSERT INTO login_logs
        (guild_id, steam_id, username, player_id, ip, status, x, y, z, last_seen)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            username  = VALUES(username),
            ip        = VALUES(ip),
            status    = VALUES(status),
            x         = VALUES(x),
            y         = VALUES(y),
            z         = VALUES(z),
            last_seen = VALUES(last_seen)
    """

    vals = []
    for e in entries:
        state = (e.get("state") or "").lower()
        status = "logged in" if state == "in" else "logged out"
        ts = f"{e['date']} {e['time']}"

        if state == "in":
            scan_steam_bans(guild_id=guild_id, steam_id=e["steam_id"], username=e.get("username"))

        vals.append(
            (
                guild_id,
                e["steam_id"],
                e["username"],
                e["player_id"],
                e["ip"],
                status,
                e["x"],
                e["y"],
                e["z"],
                ts,
            )
        )

    cur.executemany(sql, vals)
    conn.commit()
    cur.close()
    conn.close()

def save_kills_and_update_stats(events: List[dict], guild_id: int, conn) -> None:
    """
    Persist kill events into kill_logs, and update player_statistics + weapon_stats.

    Design notes:
    - This runs in the downloader (ingest), not the updater (presentation).
    - Handles SUICIDE events as death-only increments.
    - Uses ON DUPLICATE KEY to create/update player rows safely.
    """

    if not events:
        return

    cur = conn.cursor()

    # 1) Insert kill events
    kill_sql = """
        INSERT INTO kill_logs (
            guild_id, ts,
            killer_steam_id, killer_username, killer_player_id,
            victim_steam_id, victim_username, victim_player_id,
            weapon, distance,
            killer_x, killer_y, killer_z,
            victim_x, victim_y, victim_z,
            time_of_day, src_tag, bounty_reward
        )
        VALUES (
            %s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,%s
        )
    """

    kill_vals = []
    for e in events:
        kill_vals.append(
            (
                guild_id,
                e.get("ts"),
                e.get("killer_steam_id"),
                e.get("killer_username"),
                e.get("killer_player_id"),
                e.get("victim_steam_id"),
                e.get("victim_username"),
                e.get("victim_player_id"),
                e.get("weapon"),
                e.get("distance"),
                e.get("killer_x"),
                e.get("killer_y"),
                e.get("killer_z"),
                e.get("victim_x"),
                e.get("victim_y"),
                e.get("victim_z"),
                e.get("time_of_day"),
                e.get("src_tag"),
                0,  # bounty_reward (set elsewhere if you do bounty settlement)
            )
        )

    cur.executemany(kill_sql, kill_vals)

    # 2) Upsert/Update player stats
    # Ensure player rows exist (killer + victim)
    player_upsert_sql = """
        INSERT INTO player_statistics (guild_id, steam_id, username)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            username = VALUES(username)
    """

    # Increment stats
    inc_kills_sql = """
        UPDATE player_statistics
        SET
            kills = kills + 1,
            longest_kill = GREATEST(longest_kill, %s)
        WHERE guild_id = %s AND steam_id = %s
    """

    inc_deaths_sql = """
        UPDATE player_statistics
        SET deaths = deaths + 1
        WHERE guild_id = %s AND steam_id = %s
    """

    # Update K/D after modifications (simple + stable)
    kd_sql = """
        UPDATE player_statistics
        SET kd_ratio = CASE
            WHEN deaths = 0 THEN kills
            ELSE kills / deaths
        END
        WHERE guild_id = %s AND steam_id = %s
    """

    # Weapon stats (killer only; not for suicides)
    weapon_upsert_sql = """
        INSERT INTO weapon_stats (
            guild_id, steam_id, weapon,
            kills, longest_kill, total_distance,
            first_kill_ts, last_kill_ts
        )
        VALUES (%s,%s,%s, 1, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            kills = kills + 1,
            longest_kill = GREATEST(longest_kill, VALUES(longest_kill)),
            total_distance = total_distance + VALUES(total_distance),
            last_kill_ts = VALUES(last_kill_ts)
    """

    # Recompute favorite weapon from weapon_stats (killer only)
    fav_weapon_sql = """
        UPDATE player_statistics ps
        JOIN (
            SELECT weapon
            FROM weapon_stats
            WHERE guild_id = %s AND steam_id = %s
            ORDER BY kills DESC, longest_kill DESC, last_kill_ts DESC
            LIMIT 1
        ) w
        SET ps.favorite_weapon = w.weapon
        WHERE ps.guild_id = %s AND ps.steam_id = %s
    """

    for e in events:
        killer_sid = e.get("killer_steam_id")
        victim_sid = e.get("victim_steam_id")
        killer_name = (e.get("killer_username") or "").strip() or None
        victim_name = (e.get("victim_username") or "").strip() or None
        src_tag = e.get("src_tag")
        dist = e.get("distance")
        dist_val = float(dist) if dist is not None else 0.0

        # Ensure victim exists; increment deaths always
        if victim_sid:
            cur.execute(player_upsert_sql, (guild_id, victim_sid, victim_name))
            cur.execute(inc_deaths_sql, (guild_id, victim_sid))
            cur.execute(kd_sql, (guild_id, victim_sid))

        # For suicides: do not increment kills/weapon stats
        if src_tag == "SUICIDE":
            continue

        # Ensure killer exists; increment kills
        if killer_sid:
            cur.execute(player_upsert_sql, (guild_id, killer_sid, killer_name))
            cur.execute(inc_kills_sql, (dist_val, guild_id, killer_sid))

            weapon = (e.get("weapon") or "").strip() or "Unknown"
            ts = e.get("ts")

            cur.execute(
                weapon_upsert_sql,
                (guild_id, killer_sid, weapon, dist_val, dist_val, ts, ts),
            )

            cur.execute(fav_weapon_sql, (guild_id, killer_sid, guild_id, killer_sid))
            cur.execute(kd_sql, (guild_id, killer_sid))

    cur.close()


# ==========================================================
# ===================== DOWNLOADERS ========================
# ==========================================================

async def aioftp_download(guild: Dict[str, Any], local_dir: Path, lg: logging.LoggerAdapter) -> Dict[str, int]:
    guild_id = int(guild["guild_id"])
    stats = {"files": 0, "chat": 0, "login": 0, "kill": 0, "admin": 0, "sentry": 0}

    ftp = aioftp.Client()
    ftp.passive = True

    await ftp.connect(guild["ftp_host"], int(guild.get("ftp_port") or 21))
    await ftp.login(guild["ftp_user"], guild["ftp_pass"])
    await ftp.change_directory(guild["ftp_dir"])

    async for item in ftp.list():
        # aioftp may yield either a PathIO-like object or a (path, info) tuple depending on server.
        if isinstance(item, tuple):
            if not item:
                continue
            path_obj = item[0]
        else:
            path_obj = item

        fname = getattr(path_obj, 'name', None)
        if not fname:
            s = str(path_obj).strip()
            if not s:
                continue
            fname = s.rsplit('/', 1)[-1]
            if not fname:
                continue
        if not ("chat_" in fname or "login_" in fname or "kill_" in fname or "admin_" in fname or "sentry_" in fname.lower()):
            continue

        local_path = local_dir / fname
        stat = await ftp.stat(fname)
        size = stat.size

        if local_path.exists() and local_path.stat().st_size == size:
            continue

        stats["files"] += 1
        async with ftp.download_stream(fname) as stream:
            with open(local_path, "wb") as f:
                async for block in stream.iter_by_block():
                    f.write(block)

        if fname.startswith("chat_"):
            entries = parse_log_file(local_path, guild_id, "chat")
            stats["chat"] += len(entries)
            save_chats_to_mysql(entries, guild_id)

        elif fname.startswith("login_"):
            entries = parse_log_file(local_path, guild_id, "login")
            stats["login"] += len(entries)
            save_logins_to_mysql(entries, guild_id)

        elif fname.startswith("kill_"):
            events = parse_kill_lines_with_checkpoint(local_path, guild_id)
            stats["kill"] += len(events)
            if events:
                conn = db_connect()
                try:
                    save_kills_and_update_stats(events, guild_id, conn)
                    conn.commit()
                finally:
                    conn.close()

        elif fname.startswith("admin_"):
            entries = parse_admin_file(local_path, guild_id)
            stats["admin"] += len(entries)
            save_admin_logs_to_mysql(entries)

        elif "sentry" in fname.lower():
            entries, _scanned = parse_sentry_file(local_path, guild_id)
            stats["sentry"] += len(entries)
            increment_stats = int(guild.get("post_sentries", 0) or 0) == 1
            save_sentry_logs_to_mysql(entries, guild_id, increment_stats=increment_stats)

    await ftp.quit()
    return stats

def ftplib_safe_download(guild: Dict[str, Any], local_dir: Path) -> Dict[str, int]:
    guild_id = int(guild["guild_id"])
    stats = {"files": 0, "chat": 0, "login": 0, "kill": 0, "admin": 0, "sentry": 0}

    with ftplib.FTP() as ftp:
        ftp.connect(guild["ftp_host"], int(guild.get("ftp_port") or 21), timeout=20)
        ftp.login(guild["ftp_user"], guild["ftp_pass"])
        ftp.cwd(guild["ftp_dir"])
        ftp.set_pasv(True)

        for fname in ftp_list_files(ftp):
            if not ("chat_" in fname or "login_" in fname or "kill_" in fname or "admin_" in fname or "sentry_" in fname.lower()):
                continue

            local_path = local_dir / fname
            size = ftp.size(fname)
            if local_path.exists() and local_path.stat().st_size == size:
                continue

            stats["files"] += 1
            with open(local_path, "wb") as f:
                ftp.retrbinary("RETR " + fname, f.write)

            if fname.startswith("chat_"):
                entries = parse_log_file(local_path, guild_id, "chat")
                stats["chat"] += len(entries)
                save_chats_to_mysql(entries, guild_id)

            elif fname.startswith("login_"):
                entries = parse_log_file(local_path, guild_id, "login")
                stats["login"] += len(entries)
                save_logins_to_mysql(entries, guild_id)

            elif fname.startswith("kill_"):
                events = parse_kill_lines_with_checkpoint(local_path, guild_id)
                stats["kill"] += len(events)
                if events:
                    conn = db_connect()
                    try:
                        save_kills_and_update_stats(events, guild_id, conn)
                        conn.commit()
                    finally:
                        conn.close()

            elif fname.startswith("admin_"):
                entries = parse_admin_file(local_path, guild_id)
                stats["admin"] += len(entries)
                save_admin_logs_to_mysql(entries)

            elif "sentry" in fname.lower():
                entries, _scanned = parse_sentry_file(local_path, guild_id)
                stats["sentry"] += len(entries)
                increment_stats = int(guild.get("post_sentries", 0) or 0) == 1
                save_sentry_logs_to_mysql(entries, guild_id, increment_stats=increment_stats)

    return stats

async def asyncssh_sftp_download(guild: Dict[str, Any], local_dir: Path) -> Dict[str, int]:
    guild_id = int(guild["guild_id"])
    stats = {"files": 0, "chat": 0, "login": 0, "kill": 0, "admin": 0, "sentry": 0}

    host = guild["ftp_host"]
    port = int(guild.get("ftp_port") or 22)
    username = guild["ftp_user"]
    password = guild["ftp_pass"]
    remote_dir = guild["ftp_dir"]

    async with asyncssh.connect(host, port=port, username=username, password=password, known_hosts=None) as conn:
        async with conn.start_sftp_client() as sftp:
            await sftp.chdir(remote_dir)
            files = await sftp.listdir()

            for fname in files:
                if not ("chat_" in fname or "login_" in fname or "kill_" in fname or "admin_" in fname or "sentry_" in fname.lower()):
                    continue

                local_path = local_dir / fname
                attrs = await sftp.stat(fname)
                remote_size = attrs.size

                if local_path.exists() and local_path.stat().st_size == remote_size:
                    continue

                stats["files"] += 1
                await sftp.get(fname, local_path)

                if fname.startswith("chat_"):
                    entries = parse_log_file(local_path, guild_id, "chat")
                    stats["chat"] += len(entries)
                    save_chats_to_mysql(entries, guild_id)

                elif fname.startswith("login_"):
                    entries = parse_log_file(local_path, guild_id, "login")
                    stats["login"] += len(entries)
                    save_logins_to_mysql(entries, guild_id)

                elif fname.startswith("kill_"):
                    events = parse_kill_lines_with_checkpoint(local_path, guild_id)
                    stats["kill"] += len(events)
                    if events:
                        conn2 = db_connect()
                        try:
                            save_kills_and_update_stats(events, guild_id, conn2)
                            conn2.commit()
                        finally:
                            conn2.close()

                elif fname.startswith("admin_"):
                    entries = parse_admin_file(local_path, guild_id)
                    stats["admin"] += len(entries)
                    save_admin_logs_to_mysql(entries)

                elif "sentry" in fname.lower():
                    entries, _scanned = parse_sentry_file(local_path, guild_id)
                    stats["sentry"] += len(entries)
                    increment_stats = int(guild.get("post_sentries", 0) or 0) == 1
                    save_sentry_logs_to_mysql(entries, guild_id, increment_stats=increment_stats)

    return stats

# ==========================================================
# ==================== PER-GUILD WORKER ====================
# ==========================================================

async def process_guild(guild: Dict[str, Any], tick: int):
    guild_id = int(guild["guild_id"])
    server_name = guild.get("server_name")
    lg = logging.LoggerAdapter(logger, {"server": server_label(guild_id, server_name)})

    host = (guild.get("ftp_host") or "").strip().lower()
    port = int(guild.get("ftp_port") or 21)

    # Prefer remembered transport if present
    preferred = TRANSPORT_PREF.get(guild_id)

    # Auto-detect base transport if no preference
    base_transport = _choose_transport(host, port)

    # If aioftp is in backoff window, prefer safe_ftp
    now = time.time()
    if AIOFTP_BACKOFF_UNTIL.get(guild_id, 0.0) > now:
        if preferred != "safe_ftp":
            TRANSPORT_PREF[guild_id] = "safe_ftp"
        preferred = "safe_ftp"

    local_dir = LOGS_DIR / str(guild_id)
    local_dir.mkdir(exist_ok=True)

    scan_start = asyncio.get_running_loop().time()

    # Determine final transport order
    transport = preferred or base_transport

    stats: Dict[str, int] = {"files": 0, "chat": 0, "login": 0, "kill": 0, "admin": 0, "sentry": 0}
    chosen = ""

    try:
        if transport == "sftp":
            chosen = "sftp"
            stats = await asyncio.wait_for(asyncssh_sftp_download(guild, local_dir), timeout=90)
            TRANSPORT_PREF[guild_id] = "sftp"

        else:
            # FTP mode: prefer aioftp unless we learned it's incompatible
            if TRANSPORT_PREF.get(guild_id) == "safe_ftp":
                chosen = "safe_ftp"
                loop = asyncio.get_event_loop()
                stats = await loop.run_in_executor(None, ftplib_safe_download, guild, local_dir)
            else:
                try:
                    chosen = "aioftp"
                    stats = await asyncio.wait_for(aioftp_download(guild, local_dir, lg), timeout=60)
                    TRANSPORT_PREF[guild_id] = "aioftp"
                except Exception as e:
                    if _aioftp_known_incompatible(e):
                        AIOFTP_BACKOFF_UNTIL[guild_id] = time.time() + AIOFTP_BACKOFF_SECONDS
                        warn_once_per_hour(
                            guild_id,
                            "aioftp_incompatible",
                            lg,
                            "aioftp incompatible (EPSV/PASV mismatch). Falling back to safe FTP for %sh.",
                            int(AIOFTP_BACKOFF_SECONDS / 3600),
                        )
                    else:
                        warn_once_per_hour(guild_id, "aioftp_failed", lg, "aioftp failed; falling back to safe FTP (%s)", str(e))

                    chosen = "safe_ftp"
                    loop = asyncio.get_event_loop()
                    stats = await loop.run_in_executor(None, ftplib_safe_download, guild, local_dir)
                    TRANSPORT_PREF[guild_id] = "safe_ftp"

    except Exception:
        lg.exception("process_guild failed (transport=%s, host=%s, port=%s)", transport, host, port)
        return

    dur_ms = int((asyncio.get_running_loop().time() - scan_start) * 1000)

    # Option A: only INFO every 5 ticks; DEBUG otherwise.
    msg = (
        "scan ok | transport=%s | files=%s | new: chat=%s login=%s kill=%s admin=%s sentry=%s | %sms"
        % (chosen, stats["files"], stats["chat"], stats["login"], stats["kill"], stats["admin"], stats["sentry"], dur_ms)
    )
    if tick % 5 == 0:
        lg.info(msg)
    else:
        lg.debug(msg)

# ==========================================================
# ======================== MAIN LOOP =======================
# ==========================================================

async def run_downloader():
    # One-time startup summary
    logger.info("Downloader started (poll=%ss; logs=chat,login,kill,admin,sentry)", POLL_SECONDS)
    logger.info("Steam Web API key configured: %s", bool(STEAM_WEB_API_KEY))

    tick = 0
    try:
        while True:
            tick += 1
            tick_start = asyncio.get_running_loop().time()

            total, configured = get_server_counts()
            guilds = get_guild_rows()

            if not guilds:
                if tick % 5 == 0:
                    logger.info("No servers with FTP configured (%s/%s). Next scan in %ss.", configured, total, POLL_SECONDS)
                else:
                    logger.debug("No servers with FTP configured (%s/%s).", configured, total)
                await asyncio.sleep(POLL_SECONDS)
                continue

            if tick % 5 == 0:
                logger.info("Scan start (ftp_configured=%s/%s).", len(guilds), total)
            else:
                logger.debug("Scan start (ftp_configured=%s).", len(guilds))

            tasks = [asyncio.create_task(process_guild(g, tick)) for g in guilds]
            await asyncio.gather(*tasks, return_exceptions=True)

            tick_ms = int((asyncio.get_running_loop().time() - tick_start) * 1000)
            if tick % 5 == 0:
                logger.info("Scan complete (%sms). Next scan in %ss.", tick_ms, POLL_SECONDS)
            else:
                logger.debug("Scan complete (%sms).", tick_ms)

            await asyncio.sleep(POLL_SECONDS)

    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Downloader stopping (shutdown requested).")

async def main():
    await run_downloader()
