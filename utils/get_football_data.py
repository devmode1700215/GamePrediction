# utils/get_football_data.py
import os
import time
import logging
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from utils.safe_get import safe_get

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

API_KEY = os.getenv("FOOTBALL_API_KEY")
BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_KEY}

# ---- general helpers ---------------------------------------------------------

def _retry_get(url: str, headers: Dict[str, str], max_retries: int = 3, backoff_sec: float = 1.5):
    """
    Call safe_get with small exponential backoff for common transient errors.
    Returns a requests.Response or None.
    """
    for attempt in range(1, max_retries + 1):
        resp = safe_get(url, headers=headers)
        if resp is None:
            # network error already logged by safe_get
            time.sleep(backoff_sec * attempt)
            continue

        # API-Sports returns 200/4xx/5xx; on 429/403 we back off once/twice
        if resp.status_code in (429, 403, 500, 502, 503, 504):
            try:
                msg = resp.json()
            except Exception:
                msg = resp.text
            logger.warning(f"⚠️ API {resp.status_code} for {url} (attempt {attempt}/{max_retries}): {msg}")
            time.sleep(backoff_sec * attempt)
            continue

        return resp

    return None

def _to_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

# ---- fixtures ---------------------------------------------------------------

def fetch_fixtures(date_str: str) -> List[Dict[str, Any]]:
    """
    Fetch fixtures for a single calendar date (YYYY-MM-DD).
    Returns an array (possibly empty). Never raises.
    """
    url = f"{BASE_URL}/fixtures?date={date_str}"
    resp = _retry_get(url, headers=HEADERS)
    if resp is None:
        logger.warning(f"⚠️ Failed to fetch fixtures for {date_str}")
        return []
    try:
        data = resp.json()
        return data.get("response", []) or []
    except Exception as e:
        logger.error(f"❌ Error parsing fixtures for {date_str}: {e}")
        return []

# ---- standings / team stats -------------------------------------------------

def get_team_position(team_id: Optional[int], league_id: Optional[int], season: Optional[int]) -> Optional[int]:
    if not (team_id and league_id and season):
        return None
    url = f"{BASE_URL}/standings?league={league_id}&season={season}"
    resp = _retry_get(url, headers=HEADERS)
    if resp is None:
        return None
    try:
        payload = resp.json().get("response", [])
        if not payload:
            return None
        standings_blocks = payload[0].get("league", {}).get("standings", [])
        if not standings_blocks:
            return None
        for row in standings_blocks[0]:
            t = (row or {}).get("team", {}) or {}
            if t.get("id") == team_id:
                return row.get("rank")
        return None
    except Exception:
        return None

def get_team_form_and_goals(team_id: Optional[int], league_id: Optional[int], season: Optional[int]) -> Tuple[Optional[str], Optional[float]]:
    """
    Returns (form_string like 'W-W-D-L-W', xg_for_total_avg) if available.
    """
    if not (team_id and league_id and season):
        return None, None
    url = f"{BASE_URL}/teams/statistics?team={team_id}&league={league_id}&season={season}"
    resp = _retry_get(url, headers=HEADERS)
    if resp is None:
        return None, None
    try:
        stats = resp.json().get("response", {}) or {}
        form_raw = stats.get("form") or ""
        form = "-".join(list(form_raw)) if form_raw else None

        expected = stats.get("expected", {}) or {}
        goals_for = (expected.get("goals", {}) or {}).get("for", {}) or {}
        average = goals_for.get("average", {}) or {}
        xg = average.get("total")
        xg = _to_float(xg)
        return form, xg
    except Exception:
        return None, None

def get_recent_goals(team_id: Optional[int]) -> List[int]:
    """
    Returns a list of the team's goals scored in each of their last 5 matches.
    """
    if not team_id:
        return []
    url = f"{BASE_URL}/fixtures?team={team_id}&last=5"
    resp = _retry_get(url, headers=HEADERS)
    if resp is None:
        return []
    try:
        matches = resp.json().get("response", []) or []
        out = []
        for m in matches:
            goals = (m.get("goals") or {})
            teams = (m.get("teams") or {})
            home = (teams.get("home") or {})
            if home.get("id") == team_id:
                out.append(int(goals.get("home") or 0))
            else:
                out.append(int(goals.get("away") or 0))
        return out
    except Exception:
        return []

# ---- match odds -------------------------------------------------------------

_ODDS_KEYS = ["home_win", "draw", "away_win", "btts_yes", "btts_no", "over_2_5", "under_2_5"]

def _empty_odds_dict():
    return {k: None for k in _ODDS_KEYS}

def get_match_odds(fixture_id: Optional[int]) -> Dict[str, Optional[float]]:
    """
    Returns a flat odds dict with the keys above, or all None if unavailable.
    Prefers Bwin; falls back to first bookmaker that has the markets.
    """
    out = _empty_odds_dict()
    if not fixture_id:
        return out

    url = f"{BASE_URL}/odds?fixture={fixture_id}"
    resp = _retry_get(url, headers=HEADERS)
    if resp is None:
        return out

    try:
        events = resp.json().get("response", []) or []
        if not events:
            return out

        # Choose preferred bookmaker (Bwin) if available; otherwise first bookmaker
        chosen = None
        for ev in events:
            for book in (ev.get("bookmakers") or []):
                if (book.get("name") or "").lower() == "bwin":
                    chosen = book
                    break
            if chosen:
                break
        if chosen is None:
            # fallback: first bookmaker found
            for ev in events:
                arr = ev.get("bookmakers") or []
                if arr:
                    chosen = arr[0]
                    break
        if chosen is None:
            return out

        # Parse bets / markets
        for bet in (chosen.get("bets") or []):
            name = (bet.get("name") or "").lower()

            if name in ("match winner", "1x2", "winner"):
                for v in (bet.get("values") or []):
                    val = (v.get("value") or "").lower()
                    odd = _to_float(v.get("odd"))
                    if val == "home":
                        out["home_win"] = odd
                    elif val == "draw":
                        out["draw"] = odd
                    elif val == "away":
                        out["away_win"] = odd

            elif name in ("goals over/under", "over/under"):
                for v in (bet.get("values") or []):
                    val = (v.get("value") or "").lower().replace(" ", "")
                    odd = _to_float(v.get("odd"))
                    if val in ("over2.5", "over2,5"):
                        out["over_2_5"] = odd
                    elif val in ("under2.5", "under2,5"):
                        out["under_2_5"] = odd

            elif name in ("both teams to score", "btts", "both teams score"):
                for v in (bet.get("values") or []):
                    val = (v.get("value") or "").lower()
                    odd = _to_float(v.get("odd"))
                    if val == "yes":
                        out["btts_yes"] = odd
                    elif val == "no":
                        out["btts_no"] = odd

        return out
    except Exception:
        return out

# ---- head to head -----------------------------------------------------------

def get_head_to_head(home_id: Optional[int], away_id: Optional[int]) -> List[Dict[str, Any]]:
    if not (home_id and away_id):
        return []
    url = f"{BASE_URL}/fixtures/headtohead?h2h={home_id}-{away_id}"
    resp = _retry_get(url, headers=HEADERS)
    if resp is None:
        return []
    try:
        matches = resp.json().get("response", [])[:3]  # last 3
        out = []
        for m in matches:
            out.append({
                "date": (m.get("fixture") or {}).get("date"),
                "score": f"{(m.get('goals') or {}).get('home', 0)}-{(m.get('goals') or {}).get('away', 0)}",
            })
        return out
    except Exception:
        return []

# ---- injuries ---------------------------------------------------------------

def get_team_injuries(team_id: Optional[int], season: Optional[int]) -> List[Dict[str, Any]]:
    if not (team_id and season):
        return []
    url = f"{BASE_URL}/injuries?team={team_id}&season={season}"
    resp = _retry_get(url, headers=HEADERS)
    if resp is None:
        return []
    try:
        injuries = resp.json().get("response", []) or []
        out = []
        for i in injuries:
            player = (i.get("player") or {})
            out.append({
                "player": player.get("name"),
                "position": player.get("position"),
                "reason": i.get("reason"),
                "status": "Out",  # API rarely has return dates; keep simple
            })
        return out
    except Exception:
        return []
