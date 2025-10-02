# utils/get_football_data.py
from __future__ import annotations

import os
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

from dotenv import load_dotenv
from utils.safe_get import safe_get

# -----------------------------------------------------------------------------
# Setup
# -----------------------------------------------------------------------------
load_dotenv()
logger = logging.getLogger(__name__)

API_KEY    = os.getenv("FOOTBALL_API_KEY")  # API-Football (API-Sports) key
BASE_URL   = os.getenv("FOOTBALL_BASE_URL", "https://v3.football.api-sports.io").rstrip("/")
DEFAULT_TZ = os.getenv("FOOTBALL_TZ", "UTC")  # keep UTC to avoid day-boundary surprises

HEADERS = {"x-apisports-key": API_KEY} if API_KEY else {}

def _build_url(path: str, params: Dict[str, Any]) -> str:
    q = {k: v for k, v in params.items() if v is not None and v != ""}
    return f"{BASE_URL}{path}?{urlencode(q)}"

def _now_iso() -> str:
    try:
        return datetime.now(timezone.utc).isoformat()
    except Exception:
        return datetime.utcnow().isoformat() + "Z"

def _has_params_support() -> bool:
    try:
        return "params" in safe_get.__code__.co_varnames
    except Exception:
        return False

def _to_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

# -----------------------------------------------------------------------------
# Fixtures (by date YYYY-MM-DD) — robust logs + tz
# -----------------------------------------------------------------------------
def fetch_fixtures(date_str: str) -> List[Dict[str, Any]]:
    """
    Fetch fixtures for a given date (YYYY-MM-DD) in DEFAULT_TZ.
    If empty, retry with UTC once (if DEFAULT_TZ != UTC).
    """
    if not API_KEY:
        logger.error("⚠️ FOOTBALL_API_KEY is not set — cannot fetch fixtures.")
        return []

    def _call(date_str: str, tz: str) -> Optional[List[Dict[str, Any]]]:
        path = "/fixtures"
        params = {"date": date_str, "timezone": tz}
        if _has_params_support():
            url = f"{BASE_URL}{path}"
            resp = safe_get(url, headers=HEADERS, params=params)
        else:
            url = _build_url(path, params)
            resp = safe_get(url, headers=HEADERS)

        if resp is None:
            logger.error(f"⚠️ Failed to fetch fixtures for {date_str} (tz={tz}): safe_get returned None")
            return None

        try:
            rl_rem = resp.headers.get("x-ratelimit-requests-remaining")
            rl_day = resp.headers.get("x-ratelimit-requests-limit")
            logger.info(f"📡 GET {resp.url} | {resp.status_code} | RL {rl_rem}/{rl_day}")
        except Exception:
            pass

        try:
            data = resp.json() or {}
        except Exception as e:
            logger.error(f"⚠️ JSON parse error for fixtures {date_str} (tz={tz}): {e}")
            return None

        errors = data.get("errors") or {}
        if errors:
            first_key = next(iter(errors.keys()), None)
            first_msg = errors.get(first_key)
            logger.error(f"⚠️ API errors for {date_str} (tz={tz}): {first_key} -> {first_msg}")

        fixtures = data.get("response", []) or []
        logger.info(f"📅 {date_str} (tz={tz}): fetched {len(fixtures)} fixtures")
        return fixtures

    fixtures = _call(date_str, DEFAULT_TZ)
    if fixtures is None:
        return []
    if len(fixtures) == 0 and DEFAULT_TZ != "UTC":
        logger.info(f"↻ Retrying fixtures for {date_str} with tz=UTC (first attempt returned 0).")
        fixtures = _call(date_str, "UTC") or []
    return fixtures

# -----------------------------------------------------------------------------
# Odds (flat dict your pipeline expects)
# -----------------------------------------------------------------------------
def get_match_odds(
    fixture_id: int,
    preferred_bookmaker: str = "Bwin",
) -> Dict[str, Optional[float]]:
    """
    Return normalized odds:
      home_win, draw, away_win, btts_yes, btts_no, over_2_5, under_2_5
    """
    path = "/odds"
    params = {"fixture": str(fixture_id)}
    if _has_params_support():
        url = f"{BASE_URL}{path}"
        resp = safe_get(url, headers=HEADERS, params=params)
    else:
        url = _build_url(path, params)
        resp = safe_get(url, headers=HEADERS)

    out: Dict[str, Optional[float]] = {
        "home_win": None, "draw": None, "away_win": None,
        "btts_yes": None, "btts_no": None,
        "over_2_5": None, "under_2_5": None,
    }
    if resp is None:
        logger.error(f"⚠️ odds request failed for fixture {fixture_id} (safe_get=None)")
        return out

    try:
        data = resp.json()
        blocks = data.get("response", []) or []
        if not blocks:
            return out

        chosen = None
        for blk in blocks:
            for bm in blk.get("bookmakers", []) or []:
                if bm.get("name") == preferred_bookmaker:
                    chosen = bm
                    break
            if chosen:
                break
        if not chosen:
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
    path = "/fixtures/headtohead"
    params = {"h2h": f"{home_id}-{away_id}"}
    if _has_params_support():
        url = f"{BASE_URL}{path}"
        resp = safe_get(url, headers=HEADERS, params=params)
    else:
        url = _build_url(path, params)
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
    if not season:
        return []
    path = "/injuries"
    params = {"team": str(team_id), "season": str(season)}
    if _has_params_support():
        url = f"{BASE_URL}{path}"
        resp = safe_get(url, headers=HEADERS, params=params)
    else:
        url = _build_url(path, params)
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
# Team position (standings) — optional helper if needed
# -----------------------------------------------------------------------------
def get_team_position(team_id: int, league_id: Optional[int], season: Optional[int]) -> Optional[int]:
    if not league_id or not season:
        return None

    path = "/standings"
    params = {"league": str(league_id), "season": str(season)}
    if _has_params_support():
        url = f"{BASE_URL}{path}"
        resp = safe_get(url, headers=HEADERS, params=params)
    else:
        url = _build_url(path, params)
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
# Form + xG (API: teams/statistics) — returns (form_str, xg)
# -----------------------------------------------------------------------------
def get_team_form_and_goals(
    team_id: int,
    league_id: Optional[int],
    season: Optional[int],
) -> Tuple[Optional[str], Optional[float]]:
    if not league_id or not season:
        return None, None

    path = "/teams/statistics"
    params = {"team": str(team_id), "league": str(league_id), "season": str(season)}
    if _has_params_support():
        url = f"{BASE_URL}{path}"
        resp = safe_get(url, headers=HEADERS, params=params)
    else:
        url = _build_url(path, params)
        resp = safe_get(url, headers=HEADERS)

    if resp is None:
        return None, None

    try:
        stats = resp.json().get("response", {}) or {}

        form_raw = stats.get("form") or ""  # e.g. "WWDLW"
        form_str = "-".join(list(form_raw)) if form_raw else None

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
    path = "/fixtures"
    params = {"team": str(team_id), "last": str(last)}
    if _has_params_support():
        url = f"{BASE_URL}{path}"
        resp = safe_get(url, headers=HEADERS, params=params)
    else:
        url = _build_url(path, params)
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
# Enrichment (used by main.py)
# -----------------------------------------------------------------------------
def enrich_fixture(
    fx_raw: Dict[str, Any],
    *,
    preferred_bookmaker: str = "Bwin",
) -> Dict[str, Any]:
    """
    Take a single fixture object from /fixtures and attach:
      - league_id, season
      - recent_form_home/away  (form_str, xg)
      - season_stats_home/away (currently xg only; extensible)
      - injuries_home/away     (list)
      - lineup_home/away       (None placeholder)
      - ou25_market            (over/under prices)
      - btts_market            (btts yes/no prices)
      - odds                   (flat odds for convenience)
      - last_enriched_at       (timestamptz string)
    """
    fixture = fx_raw.get("fixture", {}) or {}
    league  = fx_raw.get("league", {}) or {}
    teams   = fx_raw.get("teams", {}) or {}

    league_id = league.get("id")
    season    = league.get("season")
    home = teams.get("home", {}) or {}
    away = teams.get("away", {}) or {}
    home_id = home.get("id")
    away_id = away.get("id")
    fixture_id = fixture.get("id")

    # Form + xG
    form_home, xg_home = get_team_form_and_goals(home_id, league_id, season) if home_id else (None, None)
    form_away, xg_away = get_team_form_and_goals(away_id, league_id, season) if away_id else (None, None)

    season_home = {"xg_for_avg": xg_home}
    season_away = {"xg_for_avg": xg_away}

    # Injuries
    injuries_home = get_team_injuries(home_id, season) if home_id else []
    injuries_away = get_team_injuries(away_id, season) if away_id else []

    # Odds
    flat_odds = get_match_odds(fixture_id, preferred_bookmaker=preferred_bookmaker) if fixture_id else {}
    ou25_market = {
        "over": flat_odds.get("over_2_5"),
        "under": flat_odds.get("under_2_5"),
    }
    btts_market = {
        "yes": flat_odds.get("btts_yes"),
        "no":  flat_odds.get("btts_no"),
    }

    enriched = {
        "fixture": fixture,
        "league": league,
        "teams": teams,
        "league_id": league_id,
        "season": season,
        "home_team": {"id": home_id, "name": home.get("name")},
        "away_team": {"id": away_id, "name": away.get("name")},
        "venue": fixture.get("venue", {}) or {},
        "recent_form_home": {"form": form_home, "xg_for_avg": xg_home},
        "recent_form_away": {"form": form_away, "xg_for_avg": xg_away},
        "season_stats_home": season_home,
        "season_stats_away": season_away,
        "injuries_home": injuries_home,
        "injuries_away": injuries_away,
        "lineup_home": None,
        "lineup_away": None,
        "ou25_market": ou25_market,
        "btts_market": btts_market,
        "odds": flat_odds,
        "last_enriched_at": _now_iso(),
    }
    return enriched

# -----------------------------------------------------------------------------
# Exports
# -----------------------------------------------------------------------------
__all__ = [
    "fetch_fixtures",
    "get_match_odds",
    "get_head_to_head",
    "get_team_injuries",
    "get_team_position",
    "get_recent_goals",
    "get_team_form_and_goals",
    "enrich_fixture",
]
