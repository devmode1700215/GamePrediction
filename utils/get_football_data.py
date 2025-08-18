# utils/get_football_data.py
import os
import httpx
import logging
from datetime import datetime, timedelta, timezone
from utils.supabaseClient import supabase

logger = logging.getLogger(__name__)

API_BASE = "https://api-football-v1.p.rapidapi.com/v3"
HEADERS = {
    "x-rapidapi-host": "api-football-v1.p.rapidapi.com",
    "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
}

# -----------------------
# Fixture Fetching
# -----------------------
def fetch_fixtures():
    """Fetch fixtures for the next 48 hours, only those with odds available."""
    now = datetime.now(timezone.utc)
    start_date = now.strftime("%Y-%m-%d")
    end_date = (now + timedelta(days=2)).strftime("%Y-%m-%d")

    url = f"{API_BASE}/fixtures"
    params = {"from": start_date, "to": end_date}

    try:
        resp = httpx.get(url, headers=HEADERS, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        fixtures = data.get("response", [])
    except Exception as e:
        logger.error(f"❌ Error fetching fixtures: {e}")
        return []

    cleaned = []
    for fx in fixtures:
        fixture_id = fx["fixture"]["id"]
        league = fx.get("league", {})
        home = fx["teams"]["home"]
        away = fx["teams"]["away"]
        venue = fx["fixture"].get("venue", {}).get("name")

        # -------------------------------
        # ✅ Odds filter: skip if missing
        # -------------------------------
        odds = None
        try:
            odds_url = f"{API_BASE}/odds"
            odds_params = {"fixture": fixture_id}
            odds_resp = httpx.get(odds_url, headers=HEADERS, params=odds_params, timeout=20)
            odds_resp.raise_for_status()
            odds_data = odds_resp.json()
            if odds_data.get("response"):
                odds = odds_data["response"][0].get("bookmakers", [])[0].get("bets", [])
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch odds for fixture {fixture_id}: {e}")

        if not odds:  # Skip if odds missing or empty
            logger.info(f"⏭️ Skipping fixture {fixture_id}: no odds available")
            continue

        cleaned.append({
            "fixture_id": fixture_id,
            "date": fx["fixture"]["date"],
            "league": {
                "name": league.get("name"),
                "country": league.get("country"),
                "round": league.get("round")
            },
            "home_team": {"name": home.get("name"), "injuries": None},
            "away_team": {"name": away.get("name"), "injuries": None},
            "venue": venue,
            "odds": odds,
            "head_to_head": None,
            "created_at": datetime.now(timezone.utc).isoformat()
        })

    logger.info(f"📅 Fixtures fetched: {len(cleaned)} with valid odds")
    return cleaned


# -----------------------
# Helpers (kept same)
# -----------------------
def get_match_odds(fixture_id: int):
    url = f"{API_BASE}/odds"
    params = {"fixture": fixture_id}
    try:
        resp = httpx.get(url, headers=HEADERS, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        return data.get("response", [])
    except Exception as e:
        logger.error(f"❌ Error fetching odds for fixture {fixture_id}: {e}")
        return None


def get_head_to_head(home_id: int, away_id: int):
    url = f"{API_BASE}/fixtures/headtohead"
    params = {"h2h": f"{home_id}-{away_id}"}
    try:
        resp = httpx.get(url, headers=HEADERS, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        return data.get("response", [])
    except Exception as e:
        logger.error(f"❌ Error fetching H2H: {e}")
        return None


def get_recent_goals(team_id: int):
    # Placeholder — expand as you already had
    return None


def get_team_form_and_goals(team_id: int):
    # Placeholder — expand as you already had
    return None


def get_team_injuries(team_id: int):
    # Placeholder — expand as you already had
    return None


def get_team_position(league_id: int, team_id: int):
    # Placeholder — expand as you already had
    return None
