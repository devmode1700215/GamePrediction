# utils/get_football_data.py
import os
from dotenv import load_dotenv

from utils.safe_get import safe_get

load_dotenv()
API_KEY = os.getenv("FOOTBALL_API_KEY")
BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_KEY}

# -----------------------------
# Fixtures
# -----------------------------
def fetch_fixtures(date_str: str):
    """
    Fetch fixtures for a single YYYY-MM-DD date.
    Returns the `response` array from API-Sports, or [] on failure.
    """
    url = f"{BASE_URL}/fixtures?date={date_str}"
    resp = safe_get(url, headers=HEADERS)
    if resp is None:
        print(f"❌ Failed to fetch fixtures for {date_str}")
        return []
    try:
        data = resp.json()
        return data.get("response", []) or []
    except Exception as e:
        print(f"❌ Error parsing fixtures response for {date_str}: {e}")
        return []

# -----------------------------
# Standings / positions
# -----------------------------
def get_team_position(team_id, league_id, season):
    url = f"{BASE_URL}/standings?league={league_id}&season={season}"
    resp = safe_get(url, headers=HEADERS)
    if resp is None:
        return None
    try:
        res = resp.json()
        response_data = res.get("response", []) or []
        if not response_data:
            return None
        standings = response_data[0].get("league", {}).get("standings") or []
        if not standings or not standings[0]:
            return None
        for entry in standings[0]:
            if entry.get("team", {}).get("id") == team_id:
                return entry.get("rank")
        return None
    except Exception as e:
        print(f"❌ Error getting team position for team {team_id}: {e}")
        return None

# -----------------------------
# Team form & xG
# -----------------------------
def get_team_form_and_goals(team_id, league_id, season):
    url = f"{BASE_URL}/teams/statistics?team={team_id}&league={league_id}&season={season}"
    resp = safe_get(url, headers=HEADERS)
    if resp is None:
        return None, None
    try:
        res = resp.json()
        stats = res.get("response", {}) or {}

        # Form string like "W-W-D-L-W"
        form_raw = stats.get("form", "") or ""
        form = "-".join(list(form_raw)) if form_raw else None

        # Expected goals (xG) for (average total)
        expected = stats.get("expected", {}) or {}
        goals_data = expected.get("goals", {}) or {}
        for_data = goals_data.get("for", {}) or {}
        average_data = for_data.get("average", {}) or {}
        xg = average_data.get("total")

        return form, xg
    except Exception as e:
        print(f"❌ Error getting team form/goals for team {team_id}: {e}")
        return None, None

# -----------------------------
# Recent goals (last 5)
# -----------------------------
def get_recent_goals(team_id):
    url = f"{BASE_URL}/fixtures?team={team_id}&last=5"
    resp = safe_get(url, headers=HEADERS)
    if resp is None:
        return []
    try:
        if resp.status_code == 200:
            matches = resp.json().get("response", []) or []
            recent_goals = []
            for match in matches:
                goals = match.get("goals", {}) or {}
                teams = match.get("teams", {}) or {}
                home_team = teams.get("home", {}) or {}
                if home_team.get("id") == team_id:
                    recent_goals.append(goals.get("home", 0))
                else:
                    recent_goals.append(goals.get("away", 0))
            return recent_goals
        return []
    except Exception as e:
        print(f"❌ Error getting recent goals for team {team_id}: {e}")
        return []

# -----------------------------
# Fixture statistics (xG)
# -----------------------------
def get_xg_for_fixture(fixture_id):
    url = f"{BASE_URL}/fixtures/statistics?fixture={fixture_id}"
    resp = safe_get(url, headers=HEADERS)
    xg_data = {"home_xg": None, "away_xg": None}
    if resp is None:
        return xg_data["home_xg"], xg_data["away_xg"]

    if resp.status_code == 200:
        try:
            stats = resp.json().get("response", []) or []
            for entry in stats:
                team_side = "home_xg" if entry["teams"]["home"]["id"] == entry["team"]["id"] else "away_xg"
                for stat in entry.get("statistics", []) or []:
                    if (stat.get("type", "").lower() == "expected goals") and (stat.get("value") is not None):
                        xg_data[team_side] = stat["value"]
        except Exception:
            pass
    return xg_data["home_xg"], xg_data["away_xg"]

# -----------------------------
# Odds (with bookmaker fallbacks)
# -----------------------------
def get_match_odds(fixture_id):
    """
    Pull odds for key markets with bookmaker preference & fallback.
    Returns dict with keys:
    home_win, draw, away_win, btts_yes, btts_no, over_2_5, under_2_5
    """
    url = f"{BASE_URL}/odds?fixture={fixture_id}"
    resp = safe_get(url, headers=HEADERS)
    empty = {
        "home_win": None, "draw": None, "away_win": None,
        "btts_yes": None, "btts_no": None,
        "over_2_5": None, "under_2_5": None,
    }
    if resp is None:
        return empty
    try:
        payload = resp.json()
    except Exception:
        return empty

    entries = payload.get("response", []) or []
    odds = empty.copy()

    PREFERRED = ["Pinnacle", "bet365", "Bwin", "William Hill", "Unibet", "Betfair"]

    def extract_from_bookmaker(bm, out):
        bets = bm.get("bets", []) or []
        for market in bets:
            mname = (market.get("name") or "").strip().lower()
            vals = market.get("values", []) or []

            if mname == "match winner":
                for v in vals:
                    val = (v.get("value") or "").strip().lower()
                    try:
                        oddf = float(v.get("odd"))
                    except (TypeError, ValueError):
                        continue
                    if val == "home" and out["home_win"] is None:
                        out["home_win"] = oddf
                    elif val == "draw" and out["draw"] is None:
                        out["draw"] = oddf
                    elif val == "away" and out["away_win"] is None:
                        out["away_win"] = oddf

            elif mname in ("goals over/under", "over/under"):
                for v in vals:
                    val = (v.get("value") or "").strip().lower()
                    try:
                        oddf = float(v.get("odd"))
                    except (TypeError, ValueError):
                        continue
                    if val == "over 2.5" and out["over_2_5"] is None:
                        out["over_2_5"] = oddf
                    elif val == "under 2.5" and out["under_2_5"] is None:
                        out["under_2_5"] = oddf

            elif mname in ("both teams to score", "btts", "both teams score"):
                for v in vals:
                    val = (v.get("value") or "").strip().lower()
                    try:
                        oddf = float(v.get("odd"))
                    except (TypeError, ValueError):
                        continue
                    if val == "yes" and out["btts_yes"] is None:
                        out["btts_yes"] = oddf
                    elif val == "no" and out["btts_no"] is None:
                        out["btts_no"] = oddf

    # Pass 1: preferred bookmakers
    for entry in entries:
        for bm in entry.get("bookmakers", []) or []:
            if bm.get("name") in PREFERRED:
                tmp = odds.copy()
                extract_from_bookmaker(bm, tmp)
                if any(tmp.values()):
                    return tmp

    # Pass 2: any bookmaker with any prices
    for entry in entries:
        for bm in entry.get("bookmakers", []) or []:
            tmp = odds.copy()
            extract_from_bookmaker(bm, tmp)
            if any(tmp.values()):
                return tmp

    return odds  # all None

# -----------------------------
# Head-to-head
# -----------------------------
def get_head_to_head(home_id, away_id):
    url = f"{BASE_URL}/fixtures/headtohead?h2h={home_id}-{away_id}"
    resp = safe_get(url, headers=HEADERS)
    if resp is None:
        return []
    try:
        data = resp.json()
    except Exception:
        return []
    matches = data.get("response", [])[:3]  # last 3 only

    h2h = []
    for m in matches:
        h2h.append({
            "date": m.get("fixture", {}).get("date"),
            "score": f"{m.get('goals', {}).get('home')}-{m.get('goals', {}).get('away')}"
        })
    return h2h

# -----------------------------
# Injuries
# -----------------------------
def get_team_injuries(team_id, season):
    url = f"{BASE_URL}/injuries?team={team_id}&season={season}"
    resp = safe_get(url, headers=HEADERS)
    if resp is None:
        return []
    try:
        data = resp.json()
    except Exception:
        return []
    injuries = data.get("response", []) or []

    team_injuries = []
    for i in injuries:
        player = i.get("player", {}) or {}
        team_injuries.append({
            "player": player.get("name"),
            "position": player.get("position"),
            "reason": i.get("reason"),
            "status": "Out"
        })
    return team_injuries
