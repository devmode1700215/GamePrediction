import json
import logging
import sys
from datetime import datetime, timedelta

from dotenv import load_dotenv

from utils.get_prediction import get_prediction
from utils.insert_value_predictions import insert_value_predictions
from utils.fetch_and_store_result import fetch_and_store_result
from utils.get_football_data import (
    fetch_fixtures,
    get_head_to_head,
    get_match_odds,
    get_recent_goals,
    get_team_form_and_goals,
    get_team_injuries,
    get_team_position,
)
from utils.insert_match import insert_match
from utils.get_matches_needing_results import get_matches_needing_results
from utils.supabaseClient import supabase

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()


def safe_extract_match_data(match):
    """Safely extract the minimal structure we need from the API fixture."""
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


def _has_any_odds(odds_dict):
    """True if odds dict has at least one numeric value."""
    if not isinstance(odds_dict, dict):
        return False
    return any(v is not None for v in odds_dict.values())


def main():
    try:
        logger.info("🚀 Starting football prediction system...")

        # 1) Update results first
        update_results_for_finished_matches()

        # 2) Build a 48h window: today + tomorrow + day after
        now = datetime.utcnow()
        d0 = now.strftime("%Y-%m-%d")
        d1 = (now + timedelta(days=1)).strftime("%Y-%m-%d")
        d2 = (now + timedelta(days=2)).strftime("%Y-%m-%d")

        logger.info(f"📅 Fetching fixtures for {d0}, {d1}, {d2}")

        fixtures = []
        for d in (d0, d1, d2):
            try:
                day = fetch_fixtures(d)
            except Exception as e:
                logger.error(f"⚠️ Failed to fetch fixtures for {d}: {e}")
                day = []
            if isinstance(day, list):
                fixtures.extend(day)

        # Filter fixtures to the next 48h by timestamp (if available)
        horizon = now + timedelta(hours=48)
        try:
            fixtures = [
                f
                for f in fixtures
                if f.get("fixture", {}).get("timestamp")
                and now.timestamp() <= f["fixture"]["timestamp"] <= horizon.timestamp()
            ]
        except Exception as e:
            logger.error(f"❌ Error filtering fixtures by time window: {e}")
            fixtures = []

        logger.info(f"🔍 Processing {len(fixtures)} fixtures...")

        successful = 0
        failed = 0

        for match in fixtures:
            try:
                base = safe_extract_match_data(match)
                if not base:
                    failed += 1
                    continue

                fixture_id = base["fixture_id"]

                # Check if we already have a matches row; if yes, we will NOT insert again,
                # but we will STILL run prediction + value insert (this was the previous blocker).
                already = supabase.table("matches").select("fixture_id").eq("fixture_id", fixture_id).execute()
                exists = bool(already.data)

                # Gather extra team data we feed to GPT
                season = base["season"]
                league_id = base["league_id"]
                if not season or not league_id:
                    logger.info(f"🟨 Missing season/league for {fixture_id}; skipping.")
                    failed += 1
                    continue

                home = base["home_team"]
                away = base["away_team"]

                # Team positions
                home["position"] = get_team_position(home["id"], league_id, season)
                away["position"] = get_team_position(away["id"], league_id, season)

                # Form & xG
                h_form, h_xg = get_team_form_and_goals(home["id"], league_id, season)
                a_form, a_xg = get_team_form_and_goals(away["id"], league_id, season)
                home["form"], home["xg"] = h_form, h_xg
                away["form"], away["xg"] = a_form, a_xg

                # Recent goals
                home["recent_goals"] = get_recent_goals(home["id"])
                away["recent_goals"] = get_recent_goals(away["id"])

                # Injuries
                home["injuries"] = get_team_injuries(home["id"], season)
                away["injuries"] = get_team_injuries(away["id"], season)

                # Odds + H2H
                odds = get_match_odds(fixture_id)
                h2h = get_head_to_head(home["id"], away["id"])

                match_json = {
                    "fixture_id": fixture_id,
                    "date": base["date"],
                    "league": base["league"],
                    "venue": base["venue"],
                    "home_team": home,
                    "away_team": away,
                    "odds": odds,
                    "head_to_head": h2h,
                    "created_at": datetime.utcnow().isoformat(),
                }

                # Insert (or skip) the match row
                if not exists:
                    try:
                        insert_match(match_json)  # your insert_match now normalizes odds
                        logger.info(f"✅ Inserted match {fixture_id}")
                    except Exception as e:
                        logger.error(f"❌ Error inserting match {fixture_id}: {e}")
                        # We still try prediction even if insert failed

                # Save tokens: only call GPT if we actually have odds
                if not _has_any_odds(match_json["odds"]):
                    logger.info(f"🪙 Skipping GPT for {fixture_id}: no usable odds.")
                    continue

                # Run prediction
                logger.info(f"🤖 Getting prediction for fixture {fixture_id}")
                prediction = get_prediction(match_json)
                if not prediction:
                    logger.info(f"🟨 No prediction returned for fixture {fixture_id}")
                    failed += 1
                    continue

                # Insert value predictions (po_value==True & odds in range handled inside)
                wrote = insert_value_predictions(prediction)
                logger.info(f"🟢 value_predictions wrote: {wrote} for fixture {fixture_id}")
                successful += 1

            except Exception as e:
                logger.error(f"❌ Unexpected error processing fixture: {e}")
                failed += 1
                continue

        logger.info(f"🎯 Processing complete: {successful} successful, {failed} failed")

    except Exception as e:
        logger.error(f"❌ Critical error in main: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
