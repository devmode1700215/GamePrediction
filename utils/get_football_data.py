import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

API_BASE = "https://v3.football.api-sports.io"
API_KEY = os.getenv("FOOTBALL_API_KEY")
HEADERS = {"x-apisports-key": API_KEY}


from datetime import datetime, timedelta, timezone

def fetch_fixtures(from_date, to_date):
    """
    Fetch fixtures between from_date and to_date (YYYY-MM-DD format).
    Includes matches starting from now and up to the specified to_date.
    """
    fixtures = []
    try:
        current_date = datetime.strptime(from_date, "%Y-%m-%d")
        end_date = datetime.strptime(to_date, "%Y-%m-%d")
        now_utc = datetime.now(timezone.utc)  # Make aware

        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            url = f"{API_BASE}/fixtures?date={date_str}"
            resp = requests.get(url, headers=HEADERS, timeout=30)

            if resp.status_code == 200:
                day_fixtures = resp.json().get("response", [])

                for match in day_fixtures:
                    fixture_date = datetime.fromisoformat(
                        match["fixture"]["date"].replace("Z", "+00:00")
                    )  # This is aware

                    # Compare only aware datetimes
                    if fixture_date >= now_utc:
                        fixtures.append(match)

            else:
                print(f"⚠️ Failed to fetch fixtures for {date_str}: {resp.status_code}")

            current_date += timedelta(days=1)

    except Exception as e:
        print(f"❌ Error in fetch_fixtures: {e}")

    return fixtures


def get_head_to_head(home_id, away_id):
    try:
        url = f"{API_BASE}/fixtures/headtohead?h2h={home_id}-{away_id}"
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            return resp.json().get("response", [])
        else:
            print(f"⚠️ Failed to fetch head-to-head: {resp.status_code}")
            return []
    except Exception as e:
        print(f"❌ Error in get_head_to_head: {e}")
        return []


# utils/get_football_data.py
def get_match_odds(fixture_id):
    """
    Returns a flat dict of odds with keys we use downstream.
    Falls back across bookmakers/markets and returns None if nothing usable.
    """
    try:
        url = f"{API_BASE}/odds?fixture={fixture_id}"
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            print(f"⚠️ Failed to fetch odds for fixture {fixture_id}: {resp.status_code}")
            return None

        data = resp.json().get("response", [])
        if not data:
            return None

        # Preferred bookmakers first, then fallback to any
        preferred_books = {"Bwin", "Bet365", "William Hill", "Unibet"}

        def flatten_from_bookmaker(bookmaker_obj):
            out = {
                "home_win": None, "draw": None, "away_win": None,
                "btts_yes": None, "btts_no": None,
                "over_2_5": None, "under_2_5": None,
            }
            for bet in bookmaker_obj.get("bets", []):
                name = (bet.get("name") or "").lower()
                for val in bet.get("values", []) or []:
                    vlabel = (val.get("value") or "").strip()
                    odd_str = val.get("odd")
                    try:
                        odd = float(odd_str) if odd_str is not None else None
                    except (TypeError, ValueError):
                        odd = None

                    if name in ("match winner", "1x2"):
                        if vlabel.lower() in ("home", "1"):
                            out["home_win"] = odd
                        elif vlabel.lower() in ("draw", "x"):
                            out["draw"] = odd
                        elif vlabel.lower() in ("away", "2"):
                            out["away_win"] = odd

                    elif name in ("goals over/under", "over/under"):
                        if vlabel.lower() in ("over 2.5", "over 2,5"):
                            out["over_2_5"] = odd
                        elif vlabel.lower() in ("under 2.5", "under 2,5"):
                            out["under_2_5"] = odd

                    elif name in ("both teams to score", "btts", "both teams score"):
                        if vlabel.lower() == "yes":
                            out["btts_yes"] = odd
                        elif vlabel.lower() == "no":
                            out["btts_no"] = odd

            # If literally all None, treat as unusable
            return out if any(v is not None for v in out.values()) else None

        # API structure: response -> [ { bookmakers: [...] } ] (varies by provider)
        # Some providers wrap further; normalize a list of bookmaker blocks:
        bookmaker_blocks = []
        for item in data:
            for bm in item.get("bookmakers", []):
                bookmaker_blocks.append(bm)

        # Try preferred books first
        for bm in bookmaker_blocks:
            if bm.get("name") in preferred_books:
                flat = flatten_from_bookmaker(bm)
                if flat:
                    return flat

        # Fallback: first bookmaker that yields anything
        for bm in bookmaker_blocks:
            flat = flatten_from_bookmaker(bm)
            if flat:
                return flat

        return None

    except Exception as e:
        print(f"❌ Error in get_match_odds: {e}")
        return None


def get_recent_goals(team_id):
    try:
        url = f"{API_BASE}/teams/statistics?team={team_id}"
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            return resp.json().get("response", {})
        else:
            print(f"⚠️ Failed to fetch recent goals for team {team_id}: {resp.status_code}")
            return {}
    except Exception as e:
        print(f"❌ Error in get_recent_goals: {e}")
        return {}


def get_team_form_and_goals(team_id, league_id, season):
    try:
        url = f"{API_BASE}/teams/statistics?team={team_id}&league={league_id}&season={season}"
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            data = resp.json().get("response", {})
            form = data.get("form", "")
            xg = data.get("expected", {}).get("goals", {}).get("total", 0)
            return form, xg
        else:
            print(f"⚠️ Failed to fetch form/XG for team {team_id}: {resp.status_code}")
            return "", 0
    except Exception as e:
        print(f"❌ Error in get_team_form_and_goals: {e}")
        return "", 0


def get_team_injuries(team_id, season):
    try:
        url = f"{API_BASE}/injuries?team={team_id}&season={season}"
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            return resp.json().get("response", [])
        else:
            print(f"⚠️ Failed to fetch injuries for team {team_id}: {resp.status_code}")
            return []
    except Exception as e:
        print(f"❌ Error in get_team_injuries: {e}")
        return []


def get_team_position(team_id, league_id, season):
    try:
        url = f"{API_BASE}/standings?league={league_id}&season={season}"
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            standings = resp.json().get("response", [])
            for league_data in standings:
                for team in league_data.get("league", {}).get("standings", [[]])[0]:
                    if team["team"]["id"] == team_id:
                        return team.get("rank", None)
            return None
        else:
            print(f"⚠️ Failed to fetch standings for league {league_id}: {resp.status_code}")
            return None
    except Exception as e:
        print(f"❌ Error in get_team_position: {e}")
        return None
