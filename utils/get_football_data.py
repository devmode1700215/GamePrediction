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
# Recent goals / form block (last N fixtures)
# -----------------------------------------------------------------------------
def get_team_recent_block(team_id: int, last: int = 5) -> Dict[str, Any]:
    """
    Build a compact 'recent_form' block from the last N fixtures for a team.
    {
      "last": 5,
      "results": ["W","D","L","W","W"],
      "goals_for": [2,1,0,3,1],
      "goals_against": [1,1,2,0,0],
      "gf_avg": 1.40,
      "ga_avg": 0.80,
      "ou25_rate": 0.60,
      "btts_rate": 0.40
    }
    """
    url = f"{BASE_URL}/fixtures?team={team_id}&last={last}"
    resp = safe_get(url, headers=HEADERS)
    if resp is None:
        return {"last": last, "results": [], "goals_for": [], "goals_against": [], "gf_avg": None, "ga_avg": None, "ou25_rate": None, "btts_rate": None}
    try:
        data = resp.json()
        matches = data.get("response", []) or []
        results: List[str] = []
        gf: List[int] = []
        ga: List[int] = []
        ou_hits = 0
        btts_hits = 0
        for m in matches:
            teams = m.get("teams", {}) or {}
            goals = m.get("goals", {}) or {}
            home = teams.get("home", {}) or {}
            away = teams.get("away", {}) or {}
            gh = int(goals.get("home") or 0)
            ga_ = int(goals.get("away") or 0)
            # goals for/against from perspective of team_id
            if home.get("id") == team_id:
                gf.append(gh)
                ga.append(ga_)
                results.append(_wdl(gh, ga_))
            else:
                gf.append(ga_)
                ga.append(gh)
                results.append(_wdl(ga_, gh))
            # OU/BTTS
            total = gh + ga_
            if total > 2:
                ou_hits += 1
            if gh > 0 and ga_ > 0:
                btts_hits += 1

        n = max(1, len(matches))
        gf_avg = round(sum(gf) / n, 2) if gf else None
        ga_avg = round(sum(ga) / n, 2) if ga else None
        ou_rate = round(ou_hits / n, 3) if matches else None
        btts_rate = round(btts_hits / n, 3) if matches else None
        return {
            "last": last,
            "results": results,
            "goals_for": gf,
            "goals_against": ga,
            "gf_avg": gf_avg,
            "ga_avg": ga_avg,
            "ou25_rate": ou_rate,
            "btts_rate": btts_rate,
        }
    except Exception as e:
        logger.error(f"⚠️ Error building recent block for team {team_id}: {e}")
        return {"last": last, "results": [], "goals_for": [], "goals_against": [], "gf_avg": None, "ga_avg": None, "ou25_rate": None, "btts_rate": None}


def _wdl(gf: int, ga: int) -> str:
    if gf > ga:
        return "W"
    if gf < ga:
        return "L"
    return "D"


# -----------------------------------------------------------------------------
# Team season context (baseline style)
# -----------------------------------------------------------------------------
def get_team_season_context(team_id: int, league_id: Optional[int], season: Optional[int]) -> Dict[str, Any]:
    """
    Pulls team/season-wide splits; returns compact block:
    {
      "matches": 28,
      "goals_for_pg": 1.75,
      "goals_against_pg": 1.00,
      "ou25_rate": 0.57,
      "btts_rate": 0.54,
      "form": "W-W-D-L-W"
    }
    """
    if not league_id or not season:
        return {"matches": None, "goals_for_pg": None, "goals_against_pg": None, "ou25_rate": None, "btts_rate": None, "form": None}

    url = f"{BASE_URL}/teams/statistics?team={team_id}&league={league_id}&season={season}"
    resp = safe_get(url, headers=HEADERS)
    if resp is None:
        return {"matches": None, "goals_for_pg": None, "goals_against_pg": None, "ou25_rate": None, "btts_rate": None, "form": None}
    try:
        s = resp.json().get("response", {}) or {}

        played = ((s.get("fixtures") or {}).get("played") or {}).get("total")
        goals_for_pg = (((s.get("goals") or {}).get("for") or {}).get("average") or {}).get("total")
        goals_against_pg = (((s.get("goals") or {}).get("against") or {}).get("average") or {}).get("total")

        # OU2.5 rate from 'goals' buckets if available; fallback None
        # API doesn't give OU2.5 explicitly; we estimate from distribution if present, else None.
        ou25_rate = None
        btts_rate = (((s.get("both_teams_to_score") or {}).get("total") or {}).get("percentage"))
        try:
            if isinstance(btts_rate, str) and btts_rate.endswith("%"):
                btts_rate = round(float(btts_rate[:-1]) / 100.0, 3)
            elif isinstance(btts_rate, (int, float)):
                btts_rate = round(float(btts_rate), 3)
            else:
                btts_rate = None
        except Exception:
            btts_rate = None

        form_raw = s.get("form") or ""
        form_str = "-".join(list(form_raw)) if form_raw else None

        def _num(x):
            try:
                return round(float(x), 2) if x is not None else None
            except Exception:
                return None

        return {
            "matches": played,
            "goals_for_pg": _num(goals_for_pg),
            "goals_against_pg": _num(goals_against_pg),
            "ou25_rate": ou25_rate,
            "btts_rate": btts_rate,
            "form": form_str,
        }
    except Exception as e:
        logger.error(f"⚠️ Error parsing team season context team {team_id}: {e}")
        return {"matches": None, "goals_for_pg": None, "goals_against_pg": None, "ou25_rate": None, "btts_rate": None, "form": None}


# -----------------------------------------------------------------------------
# Lineups (confirmed XI & formation) when available
# -----------------------------------------------------------------------------
def get_fixture_lineups(fixture_id: int) -> Dict[str, Any]:
    """
    Returns:
    {
      "home": {"formation": "4-3-3", "coach": "X", "players": [...]},
      "away": {"formation": "4-2-3-1", "coach": "Y", "players": [...]}
    }
    If not yet available, returns {}.
    """
    url = f"{BASE_URL}/fixtures/lineups?fixture={fixture_id}"
    resp = safe_get(url, headers=HEADERS)
    if resp is None:
        return {}
    try:
        data = resp.json()
        arr = data.get("response", []) or []
        out: Dict[str, Any] = {}
        for block in arr:
            team = (block.get("team") or {}).get("id")
            side = "home" if (block.get("team") or {}).get("name") == (block.get("team") or {}).get("name") else None  # we’ll map by team id later if needed
            item = {
                "formation": block.get("formation"),
                "coach": ((block.get("coach") or {}).get("name")),
                "players": block.get("startXI") or [],
                "substitutes": block.get("substitutes") or [],
            }
            # We can't know home/away here without fixture teams; caller will map by team ids if desired.
            # Return both entries keyed by team_id for safety:
            out[str(team)] = item
        return out
    except Exception as e:
        logger.error(f"⚠️ Error parsing lineups for fixture {fixture_id}: {e}")
        return {}


# -----------------------------------------------------------------------------
# Recent goals (kept for backward compatibility)
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
# Enrichment: build all blocks for a fixture (drop-in for main/insert)
# -----------------------------------------------------------------------------
def enrich_fixture(fx: Dict[str, Any], *, preferred_bookmaker: str = "Bwin") -> Dict[str, Any]:
    """
    Returns the original fixture dict plus:
      - league_id, season
      - recent_form_home / recent_form_away
      - season_stats_home / season_stats_away
      - injuries_home / injuries_away
      - lineup_home / lineup_away (when available)
      - btts_market, ou25_market
    """
    fixture = fx.get("fixture", {}) or {}
    league = fx.get("league", {}) or {}
    teams = fx.get("teams", {}) or {}

    fixture_id = fixture.get("id")
    league_id = league.get("id")
    season = league.get("season")

    home = teams.get("home", {}) or {}
    away = teams.get("away", {}) or {}
    home_id = home.get("id")
    away_id = away.get("id")

    # Markets from odds (single bookmaker snapshot)
    odds = get_match_odds(fixture_id, preferred_bookmaker=preferred_bookmaker)
    ou25_market = {"over": odds.get("over_2_5"), "under": odds.get("under_2_5")}
    btts_market = {"yes": odds.get("btts_yes"), "no": odds.get("btts_no")}

    # Recent form blocks
    recent_home = get_team_recent_block(home_id, last=5) if home_id else {}
    recent_away = get_team_recent_block(away_id, last=5) if away_id else {}

    # Season context
    season_home = get_team_season_context(home_id, league_id, season) if home_id else {}
    season_away = get_team_season_context(away_id, league_id, season) if away_id else {}

    # Injuries
    injuries_home = get_team_injuries(home_id, season) if home_id else []
    injuries_away = get_team_injuries(away_id, season) if away_id else []

    # Lineups (dictionary keyed by team_id as string)
    raw_lineups = get_fixture_lineups(fixture_id) if fixture_id else {}
    lineup_home = raw_lineups.get(str(home_id)) if home_id else None
    lineup_away = raw_lineups.get(str(away_id)) if away_id else None

    # Attach enrichment to a copy of the original
    out = dict(fx)
    out["league_id"] = league_id
    out["season"] = season
    out["ou25_market"] = ou25_market
    out["btts_market"] = btts_market
    out["recent_form_home"] = recent_home
    out["recent_form_away"] = recent_away
    out["season_stats_home"] = season_home
    out["season_stats_away"] = season_away
    out["injuries_home"] = injuries_home
    out["injuries_away"] = injuries_away
    out["lineup_home"] = lineup_home
    out["lineup_away"] = lineup_away
    return out


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
    "get_team_recent_block",
    "get_team_season_context",
    "get_fixture_lineups",
    "enrich_fixture",
]
