import json
import logging
import sys
from utils.get_prediction import get_prediction
from utils.insert_value_predictions import insert_value_predictions
from utils.fetch_and_store_result import fetch_and_store_result
from utils.get_football_data import get_head_to_head, get_match_odds, get_recent_goals, get_team_form_and_goals, get_team_injuries, get_team_position, fetch_fixtures
from utils.insert_match import insert_match
from utils.get_matches_needing_results import get_matches_needing_results
from utils.supabaseClient import supabase
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def safe_extract_match_data(match):
    """Safely extract match data with error handling"""
    try:
        fixture = match.get("fixture", {})
        teams = match.get("teams", {})
        league = match.get("league", {})
        venue = fixture.get("venue", {})

        if not fixture or not teams or not league:
            return None

        home_team = teams.get("home", {})
        away_team = teams.get("away", {})

        if not home_team or not away_team:
            return None

        fixture_id = fixture.get("id")
        date = fixture.get("date")
        if not fixture_id or not date:
            return None

        return {
            "fixture_id": fixture_id,
            "date": date,
            "league": {
                "name": league.get("name", "Unknown"),
                "country": league.get("country", "Unknown"),
                "round": league.get("round", "Unknown"),
            },
            "venue": venue.get("name", "Unknown"),
            "home_team": {"id": home_team.get("id"), "name": home_team.get("name", "Unknown")},
            "away_team": {"id": away_team.get("id"), "name": away_team.get("name", "Unknown")},
            "season": league.get("season"),
            "league_id": league.get("id"),
        }
    except Exception:
        return None


def update_results_for_finished_matches():
    try:
        matches = get_matches_needing_results()
        if not matches:
            logger.info("No matches needing results update")
            return
        for match in matches:
            fixture_id = match.get("fixture_id")
            if fixture_id:
                fetch_and_store_result(fixture_id)
    except Exception as e:
        logger.error(f"❌ Error updating results: {e}")


def save_prediction_json(prediction_data, filename="predictions.json"):
    try:
        with open(filename, "a", encoding="utf-8") as f:
            f.write(json.dumps(prediction_data) + "\n")
    except Exception as e:
        logger.error(f"❌ Error saving prediction JSON: {e}")


def main():
    try:
        logger.info("🚀 Starting football prediction system...")
        update_results_for_finished_matches()

        now = datetime.utcnow()
        end = now + timedelta(hours=48)

        logger.info(f"📅 Fetching fixtures from {now} to {end}")
        fixtures = fetch_fixtures(
            from_date=now.strftime("%Y-%m-%d"),
            to_date=end.strftime("%Y-%m-%d")
        )

        if not isinstance(fixtures, list):
            logger.error("❌ Fixtures is not an array")
            return

        logger.info(f"🔍 Processing {len(fixtures)} fixtures...")

        successful_predictions = 0
        failed_predictions = 0

        for match in fixtures:
            match_basic = safe_extract_match_data(match)
            if not match_basic:
                failed_predictions += 1
                continue

            fixture_id = match_basic["fixture_id"]

            # Skip if already in DB
            existing = supabase.table("matches").select("fixture_id").eq("fixture_id", fixture_id).execute()
            if existing.data:
                logger.info(f"Match {fixture_id} already exists. Skipping.")
                continue

            season = match_basic["season"]
            league_id = match_basic["league_id"]
            if not season or not league_id:
                failed_predictions += 1
                continue

            home_team = match_basic["home_team"]
            away_team = match_basic["away_team"]

            home_team["position"] = get_team_position(home_team["id"], league_id, season)
            away_team["position"] = get_team_position(away_team["id"], league_id, season)

            home_form, home_xg = get_team_form_and_goals(home_team["id"], league_id, season)
            away_form, away_xg = get_team_form_and_goals(away_team["id"], league_id, season)
            home_team["form"], home_team["xg"] = home_form, home_xg
            away_team["form"], away_team["xg"] = away_form, away_xg

            home_team["recent_goals"] = get_recent_goals(home_team["id"])
            away_team["recent_goals"] = get_recent_goals(away_team["id"])

            home_team["injuries"] = get_team_injuries(home_team["id"], season)
            away_team["injuries"] = get_team_injuries(away_team["id"], season)

            odds = get_match_odds(fixture_id)
            head_to_head = get_head_to_head(home_team["id"], away_team["id"])

            match_json = {
                "fixture_id": fixture_id,
                "date": match_basic["date"],
                "league": match_basic["league"],
                "venue": match_basic["venue"],
                "home_team": home_team,
                "away_team": away_team,
                "odds": odds,
                "head_to_head": head_to_head,
                "created_at": datetime.utcnow().isoformat(),
            }

            insert_match(match_json)

            logger.info(f"🤖 Getting prediction for fixture {fixture_id}")
            prediction = get_prediction(match_json)

            if not prediction:
                logger.info(f"🟨 No prediction returned for fixture {fixture_id}")
                failed_predictions += 1
                continue

            insert_value_predictions(prediction)
            successful_predictions += 1
            logger.info(f"✅ Processed prediction for fixture {fixture_id}")

        logger.info(f"🎯 Processing complete: {successful_predictions} successful, {failed_predictions} failed")

    except Exception as e:
        logger.error(f"❌ Critical error in main: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
