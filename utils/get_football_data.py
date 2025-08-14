import os
import requests
from typing import List, Dict, Any, Optional

API_BASE_URL = "https://v3.football.api-sports.io"
API_HEADERS = {
    "x-rapidapi-key": os.getenv("API_SPORTS_KEY"),
    "x-rapidapi-host": "v3.football.api-sports.io"
}

# -----------------------
# FETCH FIXTURES
# -----------------------
def fetch_fixtures(from_date: str, to_date: str) -> List[Dict[str, Any]]:
    """
    Fetch fixtures between two dates (inclusive).
    Dates must be in YYYY-MM-DD format.
    Example:
        fixtures = fetch_fixtures("2025-08-14", "2025-08-16")
    """
    url = f"{API_BASE_URL}/fixtures?from={from_date}&to={to_date}"
    resp = requests.get(url, headers=API_HEADERS)
    resp.raise_for_status()
    data = resp.json()
    return data.get("response", [])


# -----------------------
# GET TEAM POSITION
# -----------------------
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


# -----------------------
# GET TEAM FORM & GOALS
# -----------------------
def get_team_form_and_goals(team_id: int, league_id: int, season: int):
    url = f"{API_BASE_URL}/teams/statistics?league={league_id}&season={season}&team={team_id}"
    resp = requests.get(url, headers=API_HEADERS)
    if resp.status_code != 200:
        return None, None
    stats = resp.json().get("response", {})
    form = stats.get("form")
    xg = stats.get("fixtures", {}).get("wins", {}).get("total", None)
    return form, xg


# -----------------------
# GET RECENT GOALS
# -----------------------
def get_recent_goals(team_id: int) -> Optional[int]:
    url = f"{API_BASE_URL}/teams?id={team_id}"
    resp = requests.get(url, headers=API_HEADERS)
    if resp.status_code != 200:
        return None
    data = resp.json().get("response", [])
    if data and "statistics" in data[0]:
        return data[0]["statistics"].get("goals", {}).get("for", {}).get("total", {}).get("home", None)
    return None


# -----------------------
# GET TEAM INJURIES
# -----------------------
def get_team_injuries(team_id: int, season: int) -> List[Dict[str, Any]]:
    url = f"{API_BASE_URL}/injuries?team={team_id}&season={season}"
    resp = requests.get(url, headers=API_HEADERS)
    if resp.status_code != 200:
        return []
    return resp.json().get("response", [])


# -----------------------
# GET MATCH ODDS
# -----------------------
def get_match_odds(fixture_id: int) -> List[Dict[str, Any]]:
    url = f"{API_BASE_URL}/odds?fixture={fixture_id}"
    resp = requests.get(url, headers=API_HEADERS)
    if resp.status_code != 200:
        return []
    return resp.json().get("response", [])


# -----------------------
# GET HEAD TO HEAD
# -----------------------
def get_head_to_head(home_team_id: int, away_team_id: int) -> List[Dict[str, Any]]:
    url = f"{API_BASE_URL}/fixtures/headtohead?h2h={home_team_id}-{away_team_id}"
    resp = requests.get(url, headers=API_HEADERS)
    if resp.status_code != 200:
        return []
    return resp.json().get("response", [])
