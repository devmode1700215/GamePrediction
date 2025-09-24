# main.py
# -*- coding: utf-8 -*-

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from utils.get_prediction import get_prediction
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

# Prefer the writer that logs explicit reasons (EDGE_BELOW_MIN, CONFIDENCE_BELOW_MIN, NON_VALUE, etc.)
try:
    from utils.db_write import write_value_prediction as write_value_prediction_with_reasons
    _HAS_REASONED_WRITER = True
except Exception:
    # Fallback: your original insert (no detailed reasons)
    from utils.insert_value_predictions import insert_value_predictions as _legacy_insert_value_predictions
    _HAS_REASONED_WRITER = False

# Overtime bulk ingest (keeps everything simple in one module)
try:
    from utils.overtime_integration import ingest_all_overtime_soccer
    _HAS_OVERTIME = True
except Exception:
    _HAS_OVERTIME = False
    def ingest_all_overtime_soccer():  # type: ignore
        return 0, 0

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
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
            "date": date,  # ISO8601 string from provider (UTC)
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
    """Pull results for matches that need them and store into DB."""
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

def _has_over25_price(odds_dict):
    """True if odds.over_2_5 looks numeric (>1.0)."""
    try:
        if not isinstance(odds_dict, dict):
            return False
        o = odds_dict.get("over_2_5")
        if o is None:
            return False
        return float(o) > 1.0
    except Exception:
        return False

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main():
    try:
        # quick sanity that Render env is wired
        supa_url_present = bool(os.getenv("SUPABASE_URL"))
        supa_key_present = bool(os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_ANON_KEY"))
        ot_key_present   = bool(os.getenv("OVERTIME_API_KEY"))
        logger.info(
            "🚀 Starting system… (writer_with_reasons=%s, overtime_module=%s, SUPABASE_URL=%s, SUPABASE_KEY=%s, OVERTIME_KEY=%s)",
            _HAS_REASONED_WRITER, _HAS_OVERTIME, supa_url_present, supa_key_present, ot_key_present
        )

        # 0) Ingest ALL open Overtime soccer games into matches_ot (no linking yet)
        if _HAS_OVERTIME:
            try:
                games, written = ingest_all_overtime_soccer()
                logger.info("🧲 Overtime ingest complete: games=%s, rows_written=%s", games, written)
            except Exception as e:
                logger.warning("⚠️ Overtime ingest failed: %s", e)

        # 1) Update results first
        update_results_for_finished_matches()

        # 2) Build a 48h window (UTC, timezone-aware)
        now = datetime.now(timezone.utc)
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

                # Avoid duplicate 'matches' rows; still allow predictions
                try:
                    already = supabase.table("matches").select("fixture_id").eq("fixture_id", fixture_id).execute()
                    exists = bool(getattr(already, "data", None))
                except Exception as e:
                    logger.warning("⚠️ Could not check existing match %s: %s", fixture_id, e)
                    exists = False

                season = base["season"]
                league_id = base["league_id"]
                if not season or not league_id:
                    logger.info(f"🟨 Missing season/league for {fixture_id}; skipping.")
                    failed += 1
                    continue

                home = dict(base["home_team"])
                away = dict(base["away_team"])

                # Positions
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

                # Odds + H2H (from your existing provider)
                odds = get_match_odds(fixture_id) or {}
                h2h = get_head_to_head(home["id"], away["id"]) or []

                match_json = {
                    "fixture_id": fixture_id,
                    "date": base["date"],            # ISO string UTC
                    "league": base["league"],
                    "venue": base["venue"],
                    "home_team": home,
                    "away_team": away,
                    "odds": odds,
                    "head_to_head": h2h,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }

                # Insert the match row once
                if not exists:
                    try:
                        insert_match(match_json)
                        logger.info(f"✅ Inserted match {fixture_id}")
                    except Exception as e:
                        logger.error(f"❌ Error inserting match {fixture_id}: {e}")

                # Only predict if Over 2.5 odds exist (save tokens)
                if not _has_over25_price(match_json["odds"]):
                    logger.info(f"🪙 Skipping GPT for {fixture_id}: no usable Over 2.5 odds.")
                    continue

                # Prediction (returns only over_2_5 market in our latest utils)
                logger.info(f"🤖 Getting prediction for fixture {fixture_id}")
                prediction = get_prediction(match_json)
                if not prediction:
                    logger.info(f"🟨 No prediction returned for fixture {fixture_id}")
                    failed += 1
                    continue

                block = (prediction.get("predictions") or {}).get("over_2_5")

                if _HAS_REASONED_WRITER:
                    written, reason = write_value_prediction_with_reasons(int(fixture_id), "over_2_5", block)
                    logger.info("✍️ value_predictions wrote: %s (reason=%s) for fixture %s", written, reason, fixture_id)
                else:
                    # legacy path if you didn't add db_write.py
                    try:
                        wrote = _legacy_insert_value_predictions(prediction)
                        why = "LEGACY_PATH"
                    except Exception as e:
                        wrote = 0
                        why = f"LEGACY_EXCEPTION:{e}"
                    logger.info("✍️ value_predictions wrote: %s (reason=%s) for fixture %s", wrote, why, fixture_id)

                successful += 1

            except Exception as e:
                logger.error(f"❌ Unexpected error processing fixture {match.get('fixture', {}).get('id')}: {e}")
                failed += 1
                continue

        logger.info(f"🎯 Processing complete: {successful} successful, {failed} failed")

    except Exception as e:
        logger.error(f"❌ Critical error in main: {e}")
        sys.exit(1)

# ------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
