# utils/insert_match.py
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from utils.supabaseClient import supabase

logger = logging.getLogger(__name__)

def _get(d: dict, path: str, default=None):
    """Safely get nested keys like 'fixture.id' from dicts."""
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur

def _ensure_jsonable(x):
    """
    If your Supabase column is JSON/JSONB, you can return dict/list directly.
    If it's TEXT, you may want to json.dumps it. We keep dicts/lists as-is.
    """
    return x

def _normalize_input(match: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Accepts either:
      A) flattened match_json you build in main.py (preferred)
      B) raw API-Football fixture object (has 'fixture', 'teams', 'league')
    Returns a normalized dict ready for DB insert, or None if invalid.
    """
    # Case A: already flattened
    if "fixture_id" in match and "home_team" in match and "away_team" in match:
        league = match.get("league") or {}
        league_str = f"{league.get('name','Unknown')} ({league.get('country','Unknown')} - {league.get('round','Unknown')})"
        return {
            "fixture_id": match.get("fixture_id"),
            "date": match.get("date"),
            "league": league_str,
            "home_team": _get(match, "home_team.name", match.get("home_team")),
            "away_team": _get(match, "away_team.name", match.get("away_team")),
            "odds": _ensure_jsonable(match.get("odds")),
            "injuries": _ensure_jsonable(match.get("injuries")),
            "venue": match.get("venue"),
            "head_to_head": _ensure_jsonable(match.get("head_to_head")),
            "created_at": match.get("created_at") or datetime.now(timezone.utc).isoformat(),
        }

    # Case B: raw API fixture object (from /fixtures?date=YYYY-MM-DD)
    if "fixture" in match and "teams" in match and "league" in match:
        fx = match.get("fixture") or {}
        teams = match.get("teams") or {}
        league = match.get("league") or {}
        venue = (fx.get("venue") or {}).get("name")
        home_name = (teams.get("home") or {}).get("name")
        away_name = (teams.get("away") or {}).get("name")
        league_str = f"{league.get('name','Unknown')} ({league.get('country','Unknown')} - {league.get('round','Unknown')})"

        # odds / injuries / h2h are not part of this raw object; keep None
        return {
            "fixture_id": fx.get("id"),
            "date": fx.get("date"),
            "league": league_str,
            "home_team": home_name,
            "away_team": away_name,
            "odds": None,
            "injuries": None,
            "venue": venue,
            "head_to_head": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    # Unknown shape
    return None


def insert_match(match_data: Dict[str, Any]) -> bool:
    """
    Insert (or upsert) a match row into 'matches'.
    Returns True if something was written, False otherwise.
    """
    try:
        normalized = _normalize_input(match_data)
        if not normalized:
            logger.error(f"❌ insert_match: unsupported payload shape: keys={list(match_data.keys())[:6]}")
            return False

        fixture_id = normalized.get("fixture_id")
        if not fixture_id:
            logger.error(f"❌ insert_match: missing fixture_id in payload")
            return False

        # Optional: prevent duplicates via upsert on fixture_id
        # Make sure you have a unique index: CREATE UNIQUE INDEX IF NOT EXISTS matches_fixture_id_key ON matches(fixture_id);
        res = (
            supabase.table("matches")
            .upsert(normalized, on_conflict="fixture_id")
            .execute()
        )

        logger.info(f"✅ Inserted/updated match {fixture_id}")
        return True

    except Exception as e:
        # Try to print a fixture id if present in raw payload
        fx = _get(match_data, "fixture.id") or match_data.get("fixture_id")
        logger.error(f"❌ Failed to insert fixture {fx}: {e}")
        return False
