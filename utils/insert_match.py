import logging
import json
from utils.supabaseClient import supabase
from utils.get_football_data import (
    get_match_odds,
    get_head_to_head,
    get_team_injuries,
    get_team_position
)

logger = logging.getLogger(__name__)

def insert_match(fixture):
    """
    Takes raw fixture from API-Football and enriches it with odds, injuries, H2H,
    standings. Then inserts into Supabase 'matches' table.
    """

    try:
        fixture_id = fixture["fixture"]["id"]
        league = fixture["league"]
        teams = fixture["teams"]

        # Odds
        odds_data = get_match_odds(fixture_id)
        odds_clean = None
        if odds_data:
            try:
                # Flatten common markets if available
                bookmaker = odds_data[0]["bookmakers"][0]
                bets = bookmaker.get("bets", [])
                odds_clean = {}
                for b in bets:
                    if b["name"].lower() in ["match winner", "both teams to score", "over/under"]:
                        for v in b["values"]:
                            odds_clean[v["value"].lower().replace(" ", "_")] = float(v["odd"])
            except Exception as e:
                logger.warning(f"⚠️ Could not clean odds for fixture {fixture_id}: {e}")

        # Injuries
        injuries = {
            teams["home"]["name"]: get_team_injuries(teams["home"]["id"]),
            teams["away"]["name"]: get_team_injuries(teams["away"]["id"]),
        }

        # Head-to-head
        h2h = get_head_to_head(teams["home"]["id"], teams["away"]["id"])

        # Standings positions
        home_pos = get_team_position(league["id"], league["season"], teams["home"]["id"])
        away_pos = get_team_position(league["id"], league["season"], teams["away"]["id"])

        cleaned = {
            "fixture_id": fixture_id,
            "date": fixture["fixture"]["date"],
            "league": f"{league['name']} ({league['country']} - {league['round']})",
            "home_team": teams["home"]["name"],
            "away_team": teams["away"]["name"],
            "venue": fixture["fixture"]["venue"]["name"] if fixture["fixture"]["venue"] else None,
            "odds": json.dumps(odds_clean) if odds_clean else None,
            "injuries": json.dumps(injuries) if injuries else None,
            "head_to_head": json.dumps(h2h) if h2h else None,
            "home_position": home_pos["rank"] if home_pos else None,
            "away_position": away_pos["rank"] if away_pos else None,
            "created_at": fixture["fixture"]["date"],  # could also be datetime.utcnow().isoformat()
        }

        supabase.table("matches").upsert(cleaned, on_conflict="fixture_id").execute()
        logger.info(f"✅ Stored fixture {fixture_id}: {teams['home']['name']} vs {teams['away']['name']}")

    except Exception as e:
        logger.error(f"❌ Failed to insert fixture {fixture.get('fixture', {}).get('id')}: {e}")
