# utils/get_football_data.py
from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from utils.safe_get import safe_get

# -----------------------------------------------------------------------------
# Setup
# -----------------------------------------------------------------------------
load_dotenv()
logger = logging.getLogger(__name__)

API_KEY = os.getenv("FOOTBALL_API_KEY")
BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_KEY}


# -----------------------------------------------------------------------------
# Fixtures (by single date YYYY-MM-DD)
# -----------------------------------------------------------------------------
def fetch_fixtures(date_str: str) -> List[Dict[str, Any]]:
    """
    Fetch fixtures for a given date (YYYY-MM-DD).
    Returns the list from API-Football: data['response'].
    """
    url = f"{BASE_URL}/fixtures?date={date_str}"
    resp = safe_get(url, headers=HEADERS)
    if resp is None:
        logger.error(f"⚠️ Failed to fetch fixtures for {date_str}: safe_get returned None")
        return []
    try:
        data = resp.json()
        fixtures = data.get("response", []) or []
        logger.info(f"📅 {date_str}: fetched {len(fixtures)} fixtures")
        return fixtures
    except Exception as e:
        logger.error(f"⚠️ Error parsing fixtures for {date_str}: {e}")
        return []


# -----------------------------------------------------------------------------
# Odds (normalized to a flat dict your pipeline expects)
# -----------------------------------------------------------------------------
def get_match_odds(
    fixture_id: int,
    preferred_bookmaker: str = "Bwin",
) -> Dict[str, Optional[float]]:
    """
    Return normalized odds for the fixture:
      {
        "home_win", "draw", "away_win",
        "btts_yes", "btts_no",
        "over_2_5", "under_2_5"
      }
    Tries the preferred bookmaker; falls back to the first with data.
    """
    url = f"{BASE_URL}/odds?fixture={fixture_id}"
    resp = safe_get(url, headers=HEADERS)

    out: Dict[str, Optional[float]] = {
        "home_win": None, "draw": None, "away_win": None,
        "btts_yes": None, "btts_no": None,
        "over_2_5": None, "under_2_5": None,
    }
    if resp is None:
        return out

    try:
        data = resp.json()
        blocks = data.get("response", []) or []
        if not blocks:
            return out

        # Pick bookmaker
        chosen = None
        for blk in blocks:
            for bm in blk.get("bookmakers", []) or []:
                if bm.get("name") == preferred_bookmaker:
                    chosen = bm
                    break
            if chosen:
                break
        if not chosen:
            # Fallback: first bookmaker with data
            for blk in blocks:
                bms = blk.get("bookmakers", []) or []
                if bms:
                    chosen = bms[0]
                    break

        if not chosen:
            return out

        for bet in chosen.get("bets", []) or []:
            name = (bet.get("name") or "").lower()
            values = bet.get("values", []) or []

            if name == "match winner":
                for v in values:
                    val = (v.get("value") or "").lower()
                    odd = _to_float(v.get("odd"))
                    if val == "home":
                        out["home_win"] = odd
                    elif val == "draw":
                        out["draw"] = odd
                    elif val == "away":
                        out["away_win"] = odd

            elif name in ("both teams to score", "btts", "both teams score"):
                for v in values:
                    val = (v.get("value") or "").lower()
                    odd = _to_float(v.get("odd"))
                    if val == "yes":
                        out["btts_yes"] = odd
                    elif val == "no":
                        out["btts_no"] = odd

            elif name in ("goals over/under", "over/under"):
                for v in values:
                    val = (v.get("value") or "")
                    odd = _to_float(v.get("odd"))
                    if val == "Over 2.5":
                        out["over_2_5"] = odd
                    elif val == "Under 2.5":
                        out["under_2_5"] = odd

        return out
    except Exception as e:
        logger.error(f"⚠️ Error parsing odds for fixture {fixture_id}: {e}")
        return out


# -----------------------------------------------------------------------------
# Head-to-Head (last few results)
# -----------------------------------------------------------------------------
def get_head_to_head(home_id: int, away_id: int, limit: int = 3) -> List[Dict[str, Any]]:
    """
    Fetch last `limit` H2H fixtures and return a tiny summary.
    """
    url = f"{BASE_URL}/fixtures/headtohead?h2h={home_id}-{away_id}"
    resp = safe_get(url, headers=HEADERS)
    if resp is None:
        return []
    try:
        data = resp.json()
        matches = (data.get("response", []) or [])[:limit]
        out: List[Dict[str, Any]] = []
        for m in matches:
            fx = m.get("fixture", {}) or {}
            goals = m.get("goals", {}) or {}
            out.append({
                "date": fx.get("date"),
                "score": f"{goals.get('home', 0)}-{goals.get('away', 0)}",
            })
        return out
    except Exception as e:
        logger.error(f"⚠️ Error parsing H2H {home_id}-{away_id}: {e}")
        return []


# -----------------------------------------------------------------------------
# Injuries (per team & season)
# -----------------------------------------------------------------------------
def get_team_injuries(team_id: int, season: Optional[int]) -> List[Dict[str, Any]]:
    """
    Return list of injuries for a team in a given season.
    If season is None, returns [] (API requires season).
    """
    if not season:
        return []
    url = f"{BASE_URL}/injuries?team={team_id}&season={season}"
    resp = safe_get(url, headers=HEADERS)
    if resp is None:
        return []
    try:
        data = resp.json()
        items = data.get("response", []) or []
        out: List[Dict[str, Any]] = []
        for i in items:
            player = i.get("player", {}) or {}
            out.append({
                "player": player.get("name"),
                "position": player.get("position"),
                "reason": i.get("reason"),
                "status": i.get("type") or "Out",
            })
        return out
    except Exception as e:
        logger.error(f"⚠️ Error parsing injuries for team {team_id}: {e}")
        return []


# -----------------------------------------------------------------------------
# League standings (position)
# -----------------------------------------------------------------------------
def get_team_position(team_id: int, league_id: Optional[int], season: Optional[int]) -> Optional[int]:
    """
    Return the team's rank from standings, or None.
    """
    if not league_id or not season:
        return None

    url = f"{BASE_URL}/standings?league={league_id}&season={season}"
    resp = safe_get(url, headers=HEADERS)
    if resp is None:
        return None

    try:
        data = resp.json()
        response_data = data.get("response", []) or []
        if not response_data:
            return None

        standings = response_data[0].get("league", {}).get("standings")
        if not standings or not standings[0]:
            return None

        for row in standings[0]:
            if row.get("team", {}).get("id") == team_id:
                return row.get("rank")
        return None
    except Exception as e:
        logger.error(f"⚠️ Error parsing standings for league {league_id}: {e}")
        return None


# -----------------------------------------------------------------------------
# Form + xG (API-based; returns EXACTLY (form_str, xg))
# -----------------------------------------------------------------------------
def get_team_form_and_goals(
    team_id: int,
    league_id: Optional[int],
    season: Optional[int],
) -> Tuple[Optional[str], Optional[float]]:
    """
    Matches your main.py expectations:
      returns (form_str, xg)

    - form_str: e.g. "W-W-D-L-W" (or None if unavailable)
    - xg: float from API 'expected.goals.for.average.total' (or None)
    """
    if not league_id or not season:
        return None, None

    url = f"{BASE_URL}/teams/statistics?team={team_id}&league={league_id}&season={season}"
    resp = safe_get(url, headers=HEADERS)
    if resp is None:
        return None, None

    try:
        stats = resp.json().get("response", {}) or {}

        # Form "WWDLW" -> "W-W-D-L-W"
        form_raw = stats.get("form") or ""
        form_str = "-".join(list(form_raw)) if form_raw else None

        # xG path: expected.goals.for.average.total
        expected = stats.get("expected", {}) or {}
        goals = expected.get("goals", {}) or {}
        for_side = goals.get("for", {}) or {}
        avg = for_side.get("average", {}) or {}
        xg = avg.get("total")
        try:
            xg = float(xg) if xg is not None else None
        except (TypeError, ValueError):
            xg = None

        return form_str, xg
    except Exception as e:
        logger.error(f"⚠️ Error parsing team stats for team {team_id}: {e}")
        return None, None


# -----------------------------------------------------------------------------
# Recent goals (last N via fixtures endpoint)
# -----------------------------------------------------------------------------
def get_recent_goals(team_id: int, last: int = 5) -> List[int]:
    """
    Get the team's goals scored in their last `last` matches via API.
    Returns list like [2,1,0,3,1].
    """
    url = f"{BASE_URL}/fixtures?team={team_id}&last={last}"
    resp = safe_get(url, headers=HEADERS)
    if resp is None:
        return []
    try:
        data = resp.json()
        matches = data.get("response", []) or []
        out: List[int] = []
        for m in matches:
            goals = m.get("goals", {}) or {}
            teams = m.get("teams", {}) or {}
            home = teams.get("home", {}) or {}
            if home.get("id") == team_id:
                out.append(goals.get("home", 0))
            else:
                out.append(goals.get("away", 0))
        return out
    except Exception as e:
        logger.error(f"⚠️ Error parsing recent goals for team {team_id}: {e}")
        return []


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _to_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


__all__ = [
    "fetch_fixtures",
    "get_match_odds",
    "get_head_to_head",
    "get_team_injuries",
    "get_team_position",
    "get_recent_goals",
    "get_team_form_and_goals",
]
