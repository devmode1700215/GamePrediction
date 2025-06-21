from utils.supabaseClient import supabase


def insert_match(match_data):
    
    fixture_id = match_data["fixture_id"]
    home_injuries = match_data["home_team"]["injuries"]
    away_injuries = match_data["away_team"]["injuries"]

    injuries_data = None
    if home_injuries or away_injuries:
        injuries_data = {
            match_data["home_team"]["name"]: home_injuries,
            match_data["away_team"]["name"]: away_injuries
        }

    # Prepare cleaned data
    league_info = match_data["league"]
    cleaned = {
        "fixture_id": fixture_id,
        "date": match_data["date"],
        "league": f"{league_info['name']} ({league_info['country']} - {league_info['round']})",
        "home_team": match_data["home_team"]["name"],
        "away_team": match_data["away_team"]["name"],
        "odds": match_data["odds"],
        "injuries": injuries_data,
        "venue": match_data["venue"],
        "head_to_head": match_data["head_to_head"],
        "created_at": match_data["created_at"]
    }
    supabase.table("matches").insert(cleaned).execute()