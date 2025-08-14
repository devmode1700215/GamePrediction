# main.py

import json
import logging
import sys
from datetime import datetime, timedelta, timezone

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
from dotenv import load_dotenv

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()


def safe_extract_match_data(match):
    """Safely extract match data with error handling."""
    try:
        fixture = match.get("fixture", {}) or {}
        teams = match.get("teams", {}) or {}
        league = match.get("league", {}) or {}
        venue = fixture.get("venue", {}) or {}

        if not fixture or not teams or not league:
            logger.error("❌ Invalid match structure for fixture")
            return None

        home_team = teams.get("home", {}) or {}
        away_team = teams.get("away", {}) or {}
        if not home_team or not away_team:
            logger.error("❌ Missing team data for fixture")
            return None

        fixture_id = fixture.get("id")
        date = fixture.get("date")
        if not fixture_id or not date:
            logger.error("❌ Missing fixture ID or date")
            return None

        league_data = {
            "name": league.get("name", "Unknown"),
            "country": league.get("country", "Unknown"),
            "round": league.get("round", "Unknown"),
        }
        venue_name = venue.get("name", "Unknown")
        home_team_data = {"id": home_team.get("id"), "name": home_team.get("name", "Unknown")}
        away_team_data = {"id": away_team.get("id"), "name": away_team.get("name", "Unknown")}

        season = league.get("season")
        league_id = league.get("id")

        return {
            "fixture_id": fixture_id,
            "date": date,
            "league": league_data,
            "venue": venue_name,
            "home_team": home_team_data,
            "away_team": away_team_data,
            "season": season,
            "league_id": league_id,
        }

    except Exception as e:
        logger.error(f"❌ Error extracting match data: {e}")
        return None


def update_results_for_finished_matches():
    try:
        matches = get_matches_needing_results()
        if not matches:
            logger.info("No matches needing results update")
            return

        for match in matches:
            try:
                fixture_id = match.get("fixture_id")
                if fixture_id:
                    fetch_and_store_result(fixture_id)
                else:
                    logger.error("❌ Match missing fixture_id in results update")
            except Exception as e:
                logger.error(f"❌ Error updating result for match: {e}")
                continue
    except Exception as e:
        logger.error(f"❌ Error in update_results_for_finished_matches: {e}")


def save_match_json(match_data, filename="matches.json"):
    try:
        with open(filename, "a", encoding="utf-8") as f:
            f.write(json.dumps(match_data) + "\n")
    except Exception as e:
        logger.error(f"❌ Error saving match JSON: {e}")


def save_prediction_json(prediction_data, filename="predictions.json"):
    try:
        with open(filename, "a", encoding="utf-8") as f:
            f.write(json.dumps(prediction_data) + "\n")
    except Exception as e:
        logger.error(f"❌ Error saving prediction JSON: {e}")


def main():
    try:
        logger.info("🚀 Starting football prediction system...")

        # 1) Update results for recently finished matches
        update_results_for_finished_matches()

        # 2) Collect fixtures to analyze (next 24h by default)
        now = datetime.now(timezone.utc)
        end = now + timedelta(hours=24)
        today_str = now.strftime("%Y-%m-%d")
        tomorrow_str = end.strftime("%Y-%m-%d")
        logger.info(f"📅 Fetching fixtures for {today_str} and {tomorrow_str}")

        try:
            fixtures_today = fetch_fixtures(today_str)
            fixtures_tomorrow = fetch_fixtures(tomorrow_str)
            fixtures = (fixtures_today or []) + (fixtures_tomorrow or [])
            if not isinstance(fixtures, list):
                logger.error("❌ Fixtures is not an array")
                return
        except Exception as e:
            logger.error(f"❌ Error fetching fixtures: {e}")
            return

        if not fixtures:
            logger.info("No fixtures found for the specified dates")
            return

        # Filter matches to within the [now, end] window
        try:
            fixtures = [
                f for f in fixtures
                if f.get("fixture", {}).get("timestamp")
                and now.timestamp() <= f["fixture"]["timestamp"] <= end.timestamp()
            ]
        except Exception as e:
            logger.error(f"❌ Error filtering fixtures: {e}")
            fixtures = []

        logger.info(f"🔍 Processing {len(fixtures)} fixtures...")

        successful_predictions = 0
        failed_predictions = 0

        for match in fixtures:
            try:
                # Extract core data
                match_basic = safe_extract_match_data(match)
                if not match_basic:
                    failed_predictions += 1
                    continue

                fixture_id = match_basic["fixture_id"]

                # Build full match payload (fetch data regardless of "matches" presence)
                home_team = match_basic["home_team"]
                away_team = match_basic["away_team"]
                season = match_basic["season"]
                league_id = match_basic["league_id"]

                if not season or not league_id:
                    logger.error(f"❌ Missing season or league_id for fixture {fixture_id}")
                    failed_predictions += 1
                    continue

                # Positions
                home_team["position"] = get_team_position(home_team["id"], league_id, season)
                away_team["position"] = get_team_position(away_team["id"], league_id, season)

                # Form & xG
                home_form, home_xg = get_team_form_and_goals(home_team["id"], league_id, season)
                away_form, away_xg = get_team_form_and_goals(away_team["id"], league_id, season)
                home_team["form"] = home_form
                away_team["form"] = away_form
                home_team["xg"] = home_xg
                away_team["xg"] = away_xg

                # Recent goals
                home_team["recent_goals"] = get_recent_goals(home_team["id"])
                away_team["recent_goals"] = get_recent_goals(away_team["id"])

                # Injuries
                home_team["injuries"] = get_team_injuries(home_team["id"], season)
                away_team["injuries"] = get_team_injuries(away_team["id"], season)

                # Odds & H2H
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
                    "created_at": now.isoformat(timespec="seconds"),
                }

                # Insert match only if it's new; otherwise proceed to predictions
                try:
                    existing = supabase.table("matches").select("fixture_id").eq("fixture_id", fixture_id).execute()
                    match_already_in_db = bool(existing.data)
                except Exception as e:
                    logger.error(f"❌ Error checking existing match {fixture_id}: {e}")
                    failed_predictions += 1
                    continue

                if not match_already_in_db:
                    try:
                        insert_match(match_json)
                        logger.info(f"✅ Inserted match {fixture_id}")
                    except Exception as e:
                        logger.error(f"❌ Error inserting match {fixture_id}: {e}")
                        failed_predictions += 1
                        continue
                else:
                    logger.info(f"ℹ️ Match {fixture_id} already exists — will still try predictions if missing.")

                # If no odds returned at all, skip GPT & insertion (clear log)
                if not any(v for v in (odds or {}).values()):
                    logger.info(f"🟨 No odds available for fixture {fixture_id}. Skipping GPT and insertion.")
                    continue

                # Skip if we already have value_predictions for this fixture
                try:
                    vp_existing = supabase.table("value_predictions") \
                        .select("id") \
                        .eq("fixture_id", fixture_id) \
                        .limit(1) \
                        .execute()
                    if vp_existing.data:
                        logger.info(f"🟦 value_predictions already exist for {fixture_id}. Skipping.")
                        continue
                except Exception as e:
                    logger.error(f"❌ Error checking value_predictions for {fixture_id}: {e}")
                    # non-fatal; continue to try prediction

                # Get prediction from model
                try:
                    logger.info(f"🤖 Getting prediction for fixture {fixture_id}")
                    prediction = get_prediction(match_json)
                    if prediction is None:
                        logger.info(f"🟨 No prediction returned for
