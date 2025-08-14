from utils.supabaseClient import supabase

ODDS_KEYS = [
    "home_win", "draw", "away_win",
    "btts_yes", "btts_no",
    "over_2_5", "under_2_5",
]

def _to_float(x):
    try:
        if x is None:
            return None
        return float(x)
    except (ValueError, TypeError):
        return None

def _empty_odds():
    return {k: None for k in ODDS_KEYS}

def _normalize_odds(odds_raw):
    """
    Normalize raw odds (dict/list/None) into a flat dict or None.
    """
    if odds_raw is None:
        return None

    # Already flat dict with expected keys
    if isinstance(odds_raw, dict):
        normalized = _empty_odds()
        for k in ODDS_KEYS:
            normalized[k] = _to_float(odds_raw.get(k))
        return normalized if any(v is not None for v in normalized.values()) else None

    # Array from API (take first usable dict or flatten bookmaker block)
    if isinstance(odds_raw, list):
        merged = _empty_odds()
        found = False
        for item in odds_raw:
            if isinstance(item, dict):
                # If it’s already flat
                if any(k in item for k in ODDS_KEYS):
                    for k in ODDS_KEYS:
                        if merged[k] is None:
                            merged[k] = _to_float(item.get(k))
                            if merged[k] is not None:
                                found = True
        return merged if found else None

    return None

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

    # Normalize odds
    normalized_odds = _normalize_odds(match_data.get("odds"))

    # Prepare cleaned data
    league_info = match_data["league"]
    cleaned = {
        "fixture_id": fixture_id,
        "date": match_data["date"],
        "league": f"{league_info['name']} ({league_info['country']} - {league_info['round']})",
        "home_team": match_data["home_team"]["name"],
        "away_team": match_data["away_team"]["name"],
        "odds": normalized_odds,
        "injuries": injuries_data,
        "venue": match_data["venue"],
        "head_to_head": match_data["head_to_head"],
        "created_at": match_data["created_at"]
    }

    supabase.table("matches").insert(cleaned).execute()
