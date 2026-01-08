"""Country code → flag URL helpers.

Keep this as the ONLY flag mapping in the project.

Notes:
  - Accepts common synonyms (UK→GB, GE→DE).
  - Returns None for unknown/blank codes so callers can fall back to the bot avatar.
  - Uses FlagCDN for consistency. FlagCDN URLs use lowercase codes.
"""

from __future__ import annotations

from typing import Optional


_SYNONYMS = {
    "UK": "GB",
    "GE": "DE",
}


def get_flag_url(country_code: Optional[str]) -> Optional[str]:
    """Return a flag URL for a 2-letter country code, or None if unknown."""
    if not country_code:
        return None

    code = country_code.strip().upper()
    if not code:
        return None

    code = _SYNONYMS.get(code, code)

    # Some users paste emoji or longer strings; keep it strict.
    if len(code) != 2 or not code.isalpha():
        return None

    # FlagCDN expects lowercase in the path.
    return f"https://flagcdn.com/h20/{code.lower()}.png"
