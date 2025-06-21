from datetime import datetime
import os
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.safe_get import safe_get
from utils.supabaseClient import supabase
from utils.update_bankroll_log import update_bankroll_log
from utils.verify_predictions_for_fixture import verify_predictions_for_fixture
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("FOOTBALL_API_KEY")
BASE_URL = 'https://v3.football.api-sports.io'
HEADERS = {
    'x-apisports-key': API_KEY
}

def fetch_and_store_result(fixture_id):
    url = f"{BASE_URL}/fixtures?id={fixture_id}"
    res = safe_get(url, headers=HEADERS).json()
    if(res.get("results", 0) == 0) :
        return
    match = res.get("response", [])[0]
    status = match["fixture"]["status"]["short"]
    if status not in ["FT", "AET", "PEN"]:
        print(f"Match {fixture_id} not finished yet. Skipping.")
        return

    home_goals = match["goals"]["home"]
    away_goals = match["goals"]["away"]

    result = {
        "fixture_id": fixture_id,
        "score_home": home_goals,
        "score_away": away_goals,
        "result_1x2": (
            "Home" if home_goals > away_goals else "Away" if away_goals > home_goals else "Draw"
        ),
        "result_btts": "Yes" if home_goals > 0 and away_goals > 0 else "No",
        "result_ou": "Over" if (home_goals + away_goals) > 2.5 else "Under",
        "fetched_at": datetime.utcnow().isoformat()
    }

    supabase.table("results").upsert(result).execute()
    print(f"✅ Stored result for fixture {fixture_id}")
    verify_predictions_for_fixture(fixture_id)
    update_bankroll_log()
    
if __name__ == "__main__":
    fetch_and_store_result(1338429)