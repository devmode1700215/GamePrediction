import os
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

API_BASE_URL = "https://v3.football.api-sports.io"
API_HEADERS = {
    "x-rapidapi-key": os.getenv("API_SPORTS_KEY"),
    "x-rapidapi-host": "v3.football.api-sports.io"
}

def fetch_fixtures_next_48h() -> List[Dict[str, Any]]:
    """
    Fetch fixtures from now up to the next 48 hours.
    Works on all API-Sports tiers by fetching each date separately.
    """
    fixtures = []
    now = datetime.utcnow()
    for day_offset in range(0, 3):  # today, tomorrow, and possibly day after if <48h
        date_str = (now + timedelta(days=day_offset)).strftime("%Y-%m-%d")
        url = f"{API_BASE_URL}/fixtures?date={date_str}"
        resp = requests.get(url, headers=API_HEADERS)
        if resp.status_code == 200:
            fixtures.extend(resp.json().get("response", []))
        else:
            print(f"⚠️ Failed to fetch fixtures for {date_str}: {resp.status_code}")
    return fixtures

def get_team_position(team_id: int, league_id: int, season: int) -> Optional[int]:
    url = f"{API_BASE_URL}/standings?league={league_id}&season={season}"
    resp = requests.get(url, headers=API_HEADERS)
    if resp.status_code != 200:
        return None
    standings = resp.json().get("response", [])
    for league_data in standings:
        for table in league_data.get("league", {}).get("standings", []):
            for team_data in table:
                if team_data.get("team", {}).get("id") == team_id:
                    return team_data.get("rank")
    return None

def get_team_form_and_goals(team_id: int, league_id: int, season: int):
    url = f"{API_BASE_URL}/teams/statistics?league={league_id}&season={season}&team={team_id}"
    resp = requests.get(url, headers=API_HEADERS)
    if resp.status_code != 200:
        return None, None
    stats = resp.json().get("response", {})
    form = stats.get("form")
    xg = stats.get("fixtures", {}).get("wins", {}).get("total", None)
    return form, xg

def get_recent_goals(team_id: int) -> Optional[int]:
    url = f"{API_BASE_URL}/teams?id={team_id}"
    resp = requests.get(url, headers=API_HEADERS)
    if resp.status_code != 200:
        return None
    data = resp.json().get("response", [])
    if data and "statistics" in data[0]:
        return data[0]["statistics"].get("goals", {}).get("for", {}).get("total", {}).get("home", None)
    return None

def get_team_injuries(team_id: int, season: int) -> List[Dict[str, Any]]:
    url = f"{API_BASE_URL}/injuries?team={team_id}&season={season}"
    resp = requests.get(url, headers=API_HEADERS)
    if resp.status_code != 200:
        return []
    return resp.json().get("response", [])

def get_match_odds(fixture_id: int) -> List[Dict[str, Any]]:
    url = f"{API_BASE_URL}/odds?fixture={fixture_id}"
    resp = requests.get(url, headers=API_HEADERS)
    if resp.status_code != 200:
        return []
    return resp.json().get("response", [])

def get_head_to_head(home_team_id: int, away_team_id: int) -> List[Dict[str, Any]]:
    url = f"{API_BASE_URL}/fixtures/headtohead?h2h={home_team_id}-{away_team_id}"
    resp = requests.get(url, headers=API_HEADERS)
    if resp.status_code != 200:
        return []
    return resp.json().get("response", [])
