import os
import requests
import logging
from datetime import datetime
from utils.supabaseClient import supabase

logger = logging.getLogger(__name__)

API_KEY = os.getenv("FOOTBALL_API_KEY")
BASE_URL = "https://v3.football.api-sports.io"

HEADERS = {"x-apisports-key": API_KEY}


# -------------------------------
# Fetch Fixtures
# -------------------------------
def fetch_fixtures(date_str: str):
    """
    Fetch fixtures for a given date (YYYY-MM-DD).
    """
    url = f"{BASE_URL}/fixtures"
    params = {"date": date_str}

    try:
        resp = requests.get(url, headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()

        fixtures = data.get("response", [])
        logger.info(f"📅 {date_str}: fetched {len(fixtures)} fixtures")

        return fixtures

    except Exception as e:
        logger.error(f"⚠️ Failed to fetch fixtures for {date_str}: {e}")
        return []


# -------------------------------
# Odds
# -------------------------------
def get_match_odds(fixture_id: int):
    url = f"{BASE_URL}/odds"
    params = {"fixture": fixture_id}
    try:
        resp = requests.get(url, headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()
        return data.get("response", [])
    except Exception as e:
        logger.error(f"⚠️ Failed to fetch odds for fixture {fixture_id}: {e}")
        return []


# -------------------------------
# Head to Head
# -------------------------------
def get_head_to_head(team1: int, team2: int):
    url = f"{BASE_URL}/fixtures/headtohead"
    params = {"h2h": f"{team1}-{team2}"}
    try:
        resp = requests.get(url, headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()
        return data.get("response", [])
    except Exception as e:
        logger.error(f"⚠️ Failed to fetch head-to-head for {team1} vs {team2}: {e}")
        return []


# -------------------------------
# Injuries
# -------------------------------
def get_team_injuries(team_id: int):
    url = f"{BASE_URL}/injuries"
    params = {"team": team_id}
    try:
        resp = requests.get(url, headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()
        return data.get("response", [])
    except Exception as e:
        logger.error(f"⚠️ Failed to fetch injuries for team {team_id}: {e}")
        return []


# -------------------------------
# Team Position
# -------------------------------
def get_team_position(league_id: int, season: int, team_id: int):
    url = f"{BASE_URL}/standings"
    params = {"league": league_id, "season": season}
    try:
        resp = requests.get(url, headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()

        standings = data.get("response", [])[0]["league"]["standings"][0]
        for team in standings:
            if team["team"]["id"] == team_id:
                return team
        return None

    except Exception as e:
        logger.error(f"⚠️ Failed to fetch standings for league {league_id}: {e}")
        return None


# -------------------------------
# Recent Goals (last 5 matches)
# -------------------------------
def get_recent_goals(team_id: int, season: int):
    url = f"{BASE_URL}/fixtures"
    params = {"team": team_id, "season": season, "last": 5}
    try:
        resp = requests.get(url, headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()
        matches = data.get("response", [])
        goals_for = sum([m["goals"]["for"]["total"]["home"] + m["goals"]["for"]["total"]["away"] for m in matches if "goals" in m])
        goals_against = sum([m["goals"]["against"]["total"]["home"] + m["goals"]["against"]["total"]["away"] for m in matches if "goals" in m])
        return {"for": goals_for, "against": goals_against}
    except Exception as e:
        logger.error(f"⚠️ Failed to fetch recent goals for team {team_id}: {e}")
        return {"for": 0, "against": 0}
