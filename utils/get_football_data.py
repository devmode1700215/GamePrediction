import os
from dotenv import load_dotenv

from utils.safe_get import safe_get

load_dotenv()
API_KEY = os.getenv("FOOTBALL_API_KEY")
BASE_URL = 'https://v3.football.api-sports.io'
HEADERS = {
    'x-apisports-key': API_KEY
}


def fetch_fixtures(date_str):
    url = f"{BASE_URL}/fixtures?date={date_str}"
    response = safe_get(url, headers=HEADERS)
    if response is None:
        print(f"❌ Failed to fetch fixtures for {date_str}")
        return []
    
    try:
        data = response.json()
        return data.get('response', [])
    except Exception as e:
        print(f"❌ Error parsing fixtures response: {e}")
        return []

def get_team_position(team_id, league_id, season):
    url = f"{BASE_URL}/standings?league={league_id}&season={season}"
    response = safe_get(url, headers=HEADERS)
    if response is None:
        return None
        
    try:
        res = response.json()
        response_data = res.get("response", [])
        if not response_data:
            return None
            
        standings = response_data[0].get('league', {}).get('standings')
        if not standings or not standings[0]:
            return None
            
        for entry in standings[0]:
            if entry.get('team', {}).get('id') == team_id:
                return entry.get('rank')
        return None
    except Exception as e:
        print(f"❌ Error getting team position for team {team_id}: {e}")
        return None

def get_team_form_and_goals(team_id, league_id, season):
    url = f"{BASE_URL}/teams/statistics?team={team_id}&league={league_id}&season={season}"
    response = safe_get(url, headers=HEADERS)
    if response is None:
        return None, None
        
    try:
        res = response.json()
        stats = res.get("response", {})

        # Form string like "W-W-D-L-W"
        form_raw = stats.get("form", "") or ""
        form = '-'.join(list(form_raw)) if form_raw else None

        # Expected goals (xG)
        expected = stats.get("expected", {})
        goals_data = expected.get("goals", {}) if expected else {}
        for_data = goals_data.get("for", {}) if goals_data else {}
        average_data = for_data.get("average", {}) if for_data else {}
        xg = average_data.get("total") if average_data else None

        return form, xg
    except Exception as e:
        print(f"❌ Error getting team form/goals for team {team_id}: {e}")
        return None, None

def get_recent_goals(team_id):
    url = f'https://v3.football.api-sports.io/fixtures?team={team_id}&last=5'
    response = safe_get(url, headers=HEADERS)
    
    if response is None:
        return []
        
    try:
        if response.status_code == 200:
            matches = response.json().get('response', [])
            recent_goals = []
            for match in matches:
                goals = match.get('goals', {})
                teams = match.get('teams', {})
                home_team = teams.get('home', {})
                
                # Determine if the team is home or away and get goals scored by the team
                if home_team.get('id') == team_id:
                    recent_goals.append(goals.get('home', 0))
                else:
                    recent_goals.append(goals.get('away', 0))
            return recent_goals
        else:
            return []
    except Exception as e:
        print(f"❌ Error getting recent goals for team {team_id}: {e}")
        return []

def get_xg_for_fixture(fixture_id):
    url = f'https://v3.football.api-sports.io/fixtures/statistics?fixture={fixture_id}'
    res = res = safe_get(url, headers=HEADERS)
    xg_data = {"home_xg": None, "away_xg": None}

    if res.status_code == 200:
        stats = res.json().get('response', [])
        #print(f"********************************{stats}")
        for entry in stats:
            team_side = 'home_xg' if entry['teams']['home']['id'] == entry['team']['id'] else 'away_xg'
            for stat in entry['statistics']:
                if stat['type'].lower() == 'expected goals' and stat['value'] is not None:
                    xg_data[team_side] = stat['value']
    return xg_data['home_xg'], xg_data['away_xg']

def get_match_odds(fixture_id):
    url = f"{BASE_URL}/odds?fixture={fixture_id}"
    res = res = safe_get(url, headers=HEADERS).json()
    markets = res.get("response", [])

    # Default values
    odds = {
        "home_win": None, "draw": None, "away_win": None,
        "btts_yes": None, "btts_no": None,
        "over_2_5": None, "under_2_5": None
    }

    # Look for preferred bookmaker (optional)
    for bookmaker in markets:
        for bet in bookmaker.get("bookmakers", []):
            if bet["name"] == "Bwin":  # or any other trusted bookmaker
                for market in bet["bets"]:
                    if market["name"] == "Match Winner":
                        for val in market["values"]:
                            if val["value"] == "Home":
                                odds["home_win"] = float(val["odd"])
                            elif val["value"] == "Draw":
                                odds["draw"] = float(val["odd"])
                            elif val["value"] == "Away":
                                odds["away_win"] = float(val["odd"])
                    elif market["name"].lower() in ["goals over/under", "over/under"]:
                        for val in market["values"]:
                            if val["value"] == "Over 2.5":
                                odds["over_2_5"] = float(val["odd"])
                            elif val["value"] == "Under 2.5":
                                odds["under_2_5"] = float(val["odd"])
                    elif market["name"].lower() in ["both teams to score", "btts", "both teams score"]:
                        for val in market["values"]:
                            if val["value"].lower() == "yes":
                                odds["btts_yes"] = float(val["odd"])
                            elif val["value"].lower() == "no":
                                odds["btts_no"] = float(val["odd"])
                break
        if any(odds.values()):
            break

    return odds

def get_head_to_head(home_id, away_id):
    url = f"{BASE_URL}/fixtures/headtohead?h2h={home_id}-{away_id}"
    res = res = safe_get(url, headers=HEADERS).json()
    matches = res.get("response", [])[:3]  # last 3 only

    h2h = []
    for m in matches:
        h2h.append({
            "date": m["fixture"]["date"],
            "score": f"{m['goals']['home']}-{m['goals']['away']}"
        })

    return h2h

def get_team_injuries(team_id, season):
    url = f"{BASE_URL}/injuries?team={team_id}&season={season}"
    res = res = safe_get(url, headers=HEADERS).json()
    injuries = res.get("response", [])

    team_injuries = []
    for i in injuries:
        player = i.get("player", {})
        info = {
            "player": player.get("name"),
            "position": player.get("position"),
            "reason": i.get("reason"),
            "status": "Out"  # API doesn't give return date, assume "Out"
        }
        team_injuries.append(info)

    return team_injuries
