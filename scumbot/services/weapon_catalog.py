import json
import os
import re
from typing import Any, Dict, Optional, Tuple


class WeaponCatalog:
    """Weapon catalog resolver.

    Loads scum_weapons_catalog.json (from your scraper) and resolves weapon identifiers from logs.

    Returns:
      - display_name: human-friendly name
      - image_url: thumbnail URL (if available)

    If no match is found, returns a reasonable fallback name and None.
    """

    def __init__(self, json_path: str = "scum_weapons_catalog.json"):
        self.json_path = json_path
        self.by_spawn: Dict[str, Dict[str, Any]] = {}
        self.by_key: Dict[str, Dict[str, Any]] = {}
        self._loaded = False

    def load(self) -> None:
        if self._loaded:
            return

        if not os.path.exists(self.json_path):
            # Best-effort: resolver will just fall back to cleaned names.
            self._loaded = True
            return

        with open(self.json_path, "r", encoding="utf-8") as f:
            rows = json.load(f)

        for r in rows:
            spawn = (r.get("spawn_id") or "").strip()
            key = (r.get("weapon_key") or "").strip()
            if spawn:
                self.by_spawn[spawn.upper()] = r
            if key:
                self.by_key[key.upper()] = r

        self._loaded = True

    @staticmethod
    def _strip_brackets(raw: str) -> str:
        # "Weapon_SDASS_C [Projectile]" -> "Weapon_SDASS_C"
        return re.sub(r"\s*\[.*?\]\s*", "", raw or "").strip()

    @staticmethod
    def _to_key(value: str) -> str:
        v = value or ""
        v = v.replace("Weapon_", "").replace("BP_Weapon_", "").replace("BPC_Weapon_", "")
        v = v.replace("BP_", "").replace("BPC_", "")
        v = re.sub(r"[^A-Za-z0-9]+", "", v)
        return v.upper()

    def _candidates(self, raw_weapon: str) -> list[str]:
        if not raw_weapon:
            return []

        s = self._strip_brackets(raw_weapon)
        # Sometimes logs include extra tokens; prefer the first token
        s = s.split()[0].strip()

        # Try variants with common suffixes removed
        variants = []
        for v in (s, re.sub(r"(_C|_D)$", "", s, flags=re.IGNORECASE), re.sub(r"(_C|_D|_A|_B)$", "", s, flags=re.IGNORECASE)):
            v = v.strip()
            if v and v not in variants:
                variants.append(v)

        # Spawn-id style first, then weapon_key style
        out = [v.upper() for v in variants]
        out.extend([self._to_key(v) for v in variants])
        # de-dupe preserving order
        seen = set()
        deduped = []
        for c in out:
            if c and c not in seen:
                seen.add(c)
                deduped.append(c)
        return deduped

    def resolve(self, raw_weapon: str) -> Tuple[str, Optional[str]]:
        """Return (display_name, image_url)."""
        self.load()

        for c in self._candidates(raw_weapon):
            # spawn_id match
            if c in self.by_spawn:
                r = self.by_spawn[c]
                return (r.get("display_name") or raw_weapon or "Unknown", r.get("image_url"))

        for c in self._candidates(raw_weapon):
            # weapon_key match
            if c in self.by_key:
                r = self.by_key[c]
                return (r.get("display_name") or raw_weapon or "Unknown", r.get("image_url"))

        # Soft contains match (rare, but helps if logs include extra suffixes)
        raw_u = (raw_weapon or "").upper()
        for spawn_u, r in self.by_spawn.items():
            if spawn_u and spawn_u in raw_u:
                return (r.get("display_name") or raw_weapon or "Unknown", r.get("image_url"))

        # Fallback: readable
        cleaned = self._strip_brackets(raw_weapon)
        cleaned = cleaned.replace("Weapon_", "").replace("BP_Weapon_", "").replace("BPC_Weapon_", "")
        cleaned = cleaned.replace("BP_", "").replace("BPC_", "")
        cleaned = cleaned.replace("_", " ").strip()
        return (cleaned or (raw_weapon or "Unknown"), None)
