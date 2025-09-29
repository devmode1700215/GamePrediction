# utils/insert_match.py
from typing import Any, Dict, Optional
from utils.supa import postgrest_upsert

def _as_text(x: Optional[dict], key: str) -> Optional[str]:
    if not isinstance(x, dict): return None
    v = x.get(key)
    return str(v) if v is not None else None

def insert_match(fx: Dict[str, Any]) -> bool:
    """
    Upsert into public.matches by fixture_id.
    Accepts either a raw API-Football fixture dict or the enriched dict from enrich_fixture(...).
    """
    fixture = fx.get("fixture") or {}
    league = fx.get("league") or {}
    teams = fx.get("teams") or {}
    venue = fixture.get("venue") or {}

    home = teams.get("home") or {}
    away = teams.get("away") or {}

    row = {
        "fixture_id": fixture.get("id"),
        "date": fixture.get("date"),
        "league": league.get("name"),
        "league_id": fx.get("league_id") or league.get("id"),
        "season": fx.get("season") or league.get("season"),
        "home_team": _as_text(home, "name"),
        "away_team": _as_text(away, "name"),
        "venue": _as_text(venue, "name"),
        "head_to_head": fx.get("head_to_head"),            # if you add it before calling insert
        "odds": fx.get("odds"),                            # legacy odds blob (ok to keep)
        "ou25_market": fx.get("ou25_market"),
        "btts_market": fx.get("btts_market"),
        "recent_form_home": fx.get("recent_form_home"),
        "recent_form_away": fx.get("recent_form_away"),
        "season_stats_home": fx.get("season_stats_home"),
        "season_stats_away": fx.get("season_stats_away"),
        "injuries_home": fx.get("injuries_home"),
        "injuries_away": fx.get("injuries_away"),
        "lineup_home": fx.get("lineup_home"),
        "lineup_away": fx.get("lineup_away"),
        "last_enriched_at": "now()",
    }

    if row["fixture_id"] is None:
        return False

    postgrest_upsert("matches", [row], on_conflict="fixture_id")
    return True
