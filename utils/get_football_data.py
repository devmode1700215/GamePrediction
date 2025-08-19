# utils/get_football_data.py
import os
import logging
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from utils.safe_get import safe_get
from utils.supabaseClient import supabase

load_dotenv()
logger = logging.getLogger(__name__)

API_KEY = os.getenv("FOOTBALL_API_KEY")
BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_KEY}

# -------------------------------
# Fixtures (BY DATE, YYYY-MM-DD)
# -------------------------------
def fetch_fixtures(date_str: str) -> List[Dict[str, Any]]:
    """
    Fetch fixtures for a given date (YYYY-MM-DD).
    Returns the raw list from API-Football (data['response']).
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


# -------------------------------
# Odds (normalized to a flat dict)
# -------------------------------
def get_match_odds(fixture_id: int, preferred_bookmaker: str = "Bwin") -> Dict[str, Optional[float]]:
    """
    Return a normalized odds dict for the fixture:
      {
        "home_win", "draw", "away_win",
        "btts_yes", "btts_no",
        "over_2_5", "under_2_5"
      }
    If preferred bookmaker not found, fall back to first bookmaker with data.
    """
    url = f"{BASE_URL}/odds?fixture={fixture_id}"
    resp = safe_get(url, headers=HEADERS)
    out = {
        "home_win": None, "draw": None, "away_win": None,
        "btts_yes": None, "btts_no": None,
        "over_2_5": None, "under_2_5": None
    }
    if resp is None:
        return out

    try:
        data = resp.json()
        markets = data.get("response", []) or []
        if not markets:
            return out

        # Try to pick preferred bookmaker; else first with data
        chosen = None
        for entry in markets:
            for bm in entry.get("bookmakers", []) or []:
                if bm.get("name") == preferred_bookmaker:
                    chosen = bm
                    break
            if chosen:
                break
        if not chosen:
            for entry in markets:
                bms = entry.get("bookmakers", []) or []
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
                    odd = v.get("odd")
                    try:
                        oddf = float(odd) if odd is not None else None
                    except (TypeError, ValueError):
                        oddf = None
                    if val == "home":
                        out["home_win"] = oddf
                    elif val == "draw":
                        out["draw"] = oddf
                    elif val == "away":
                        out["away_win"] = oddf

            elif name in ("both teams to score", "btts", "both teams score"):
                for v in values:
                    val = (v.get("value") or "").lower()
                    odd = v.get("odd")
                    try:
                        oddf = float(odd) if odd is not None else None
                    except (TypeError, ValueError):
                        oddf = None
                    if val == "yes":
                        out["btts_yes"] = oddf
                    elif val == "no":
                        out["btts_no"] = oddf

            elif name in ("goals over/under", "over/under"):
                for v in values:
                    val = (v.get("value") or "")
                    odd = v.get("odd")
                    try:
                        oddf = float(odd) if odd is not None else None
                    except (TypeError, ValueError):
                        oddf = None
                    if val == "Over 2.5":
                        out["over_2_5"] = oddf
                    elif val == "Under 2.5":
                        out["under_2_5"] = oddf

        return out
    except Exception as e:
        logger.error(f"⚠️ Error parsing odds for fixture {fixture_id}: {e}")
        return out


# -------------------------------
# Head-to-Head (last few results)
# -------------------------------
def get_head_to_head(home_id: int, away_id: int, limit: int = 3) -> List[Dict[str, Any]]:
    """
    Fetch and return last N (default 3) head-to-head fixtures summary.
    """
    url = f"{BASE_URL}/fixtures/headtohead?h2h={home_id}-{away_id}"
    resp = safe_get(url, headers=HEADERS)
    if resp is None:
        return []
    try:
        data = resp.json()
        matches = (data.get("response", []) or [])[:limit]
        out = []
        for m in matches:
            out.append({
                "date": m.get("fixture", {}).get("date"),
                "score": f"{m.get('goals', {}).get('home', 0)}-{m.get('goals', {}).get('away', 0)}"
            })
        return out
    except Exception as e:
        logger.error(f"⚠️ Error parsing H2H {home_id}-{away_id}: {e}")
        return []


# -------------------------------
# Injuries (per team & season)
# -------------------------------
def get_team_injuries(team_id: int, season: Optional[int]) -> List[Dict[str, Any]]:
    """
    Return list of injuries for a team in a given season.
    If season is None, returns [].
    """
    if not season:
        return []
    url = f"{BASE_URL}/injuries?team={team_id}&season={season}"
    resp = safe_get(url, headers=HEADERS)
    if resp is None:
        return []
    try:
        data = resp.json()
        injuries = data.get("response", []) or []
        out = []
        for i in injuries:
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


# -------------------------------
# League standings (position)
# -------------------------------
def get_team_position(team_id: int, league_id: Optional[int], season: Optional[int]) -> Optional[int]:
    """
    Return the team's rank from standings (or None if not found/available).
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


# -------------------------------
# Recent goals (last 5 via API)
# -------------------------------
def get_recent_goals(team_id: int, last: int = 5) -> List[int]:
    """
    Get team's goals scored in their last N matches via API.
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


# -------------------------------
# Team form + goals from your DB
# -------------------------------
def get_team_form_and_goals(team_name: str, limit: int = 5) -> Dict[str, Any]:
    """
    Pull the last `limit` finished matches for a team from your own 'matches' table
    and compute simple form + average goals for/against.

    Assumes 'matches' has:
      - columns: fixture_id, home_team, away_team, date
      - a 'results' JSON or composite with score_home, score_away (if finished)

    Returns:
      {
        "form": ["W","D","L",...],  # recent first
        "avg_goals_for": float,
        "avg_goals_against": float,
        "samples": int
      }
    """
    try:
        # Pull recent matches involving the team, newest first (default ASC, so we set desc=True)
        q = (
            supabase.table("matches")
            .select("fixture_id, home_team, away_team, date, results")
            .or_(f"home_team.eq.{team_name},away_team.eq.{team_name}")
            .order("date", desc=True)  # IMPORTANT: no 'asc=' keyword
            .limit(limit)
        )
        resp = q.execute()
        rows = resp.data or []

        form: List[str] = []
        gf_total, ga_total, samples = 0, 0, 0

        for m in rows:
            res = m.get("results")
            # results could be None or missing if not finished
            if not res:
                continue

            # Support both JSON object or flat dict with keys
            # Try common keys:
            sh = None
            sa = None

            if isinstance(res, dict):
                # typical JSON storage
                sh = res.get("score_home")
                sa = res.get("score_away")
                # Sometimes stored nested (e.g., {"fulltime":{"home":x,"away":y}})
                if sh is None and isinstance(res.get("fulltime"), dict):
                    sh = res["fulltime"].get("home")
                    sa = res["fulltime"].get("away")

            # If still None, try fallback columns (in case your table stores them flat)
            if sh is None or sa is None:
                # The row might have score_home / score_away at top level (rare)
                sh = m.get("score_home", sh)
                sa = m.get("score_away", sa)

            # If we still don't have integers, skip
            try:
                sh = int(sh)
                sa = int(sa)
            except (TypeError, ValueError):
                continue

            # Decide perspective
            if m.get("home_team") == team_name:
                gf, ga = sh, sa
            else:
                gf, ga = sa, sh

            gf_total += gf
            ga_total += ga
            samples += 1

            if gf > ga:
                form.append("W")
            elif gf < ga:
                form.append("L")
            else:
                form.append("D")

        avg_for = (gf_total / samples) if samples else 0.0
        avg_against = (ga_total / samples) if samples else 0.0

        return {
            "form": form,
            "avg_goals_for": round(avg_for, 3),
            "avg_goals_against": round(avg_against, 3),
            "samples": samples,
        }

    except Exception as e:
        logger.error(f"⚠️ Error computing team form/goals for '{team_name}': {e}")
        return {"form": [], "avg_goals_for": 0.0, "avg_goals_against": 0.0, "samples": 0}


__all__ = [
    "fetch_fixtures",
    "get_match_odds",
    "get_head_to_head",
    "get_team_injuries",
    "get_team_position",
    "get_recent_goals",
    "get_team_form_and_goals",
]
