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

# Prefer the reasoned writer (hard-gates odds/conf/edge) if available
try:
    from utils.db_write import write_value_prediction as write_value_prediction_with_reasons
    _HAS_REASONED_WRITER = True
except Exception:
    from utils.insert_value_predictions import insert_value_predictions as _legacy_insert_value_predictions
    _HAS_REASONED_WRITER = False

# Overtime helpers (ingest + DB-based linker)
try:
    from utils.overtime_integration import (
        ingest_all_overtime_soccer,
        link_overtime_to_fixtures_from_db,
    )
    _HAS_OVERTIME = True
except Exception:
    _HAS_OVERTIME = False
    def ingest_all_overtime_soccer():
        return 0, 0
    def link_overtime_to_fixtures_from_db(window_days: int = 7):
        return 0, 0, 0

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

# ------------------------------------------------------------------------------
# Config / gates
# ------------------------------------------------------------------------------
ODDS_MIN = float(os.getenv("ODDS_MIN", "1.7"))
ODDS_MAX = float(os.getenv("ODDS_MAX", "2.3"))

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def safe_extract_match_data(match):
    """Safely extract the minimal structure we need from the fixture payload."""
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
            "date": date,  # ISO8601 string UTC
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


def _has_ou25_in_range(odds_dict, lo=ODDS_MIN, hi=ODDS_MAX):
    """True if odds.over_2_5 exists and is within [lo, hi]."""
    try:
        if not isinstance(odds_dict, dict):
            return False
        o = odds_dict.get("over_2_5")
        if o is None:
            return False
        v = float(o)
        return lo <= v <= hi
    except Exception:
        return False


def _try_enrich_over25_with_overtime(fixture_id: int, odds: dict) -> dict:
    """
    If fixture is linked to an Overtime game, pull OU 2.5 (over) from matches_ot.odds
    and inject it into our odds dict when it's within [ODDS_MIN, ODDS_MAX].
    """
    try:
        link = supabase.table("ot_links").select("game_id").eq("fixture_id", fixture_id).execute()
        rows = getattr(link, "data", None)
        if not rows:
            return odds
        game_id = rows[0].get("game_id")
        if not game_id:
            return odds

        ot = supabase.table("matches_ot").select("odds, bet_url").eq("game_id", game_id).execute()
        ot_rows = getattr(ot, "data", None)
        if not ot_rows:
            return odds

        ot_odds = ot_rows[0].get("odds") or {}
        ou25 = (ot_odds.get("ou_2_5") or {}).get("over")

        if ou25 is None:
            return odds

        v = float(ou25)
        if v < ODDS_MIN or v > ODDS_MAX:
            logger.info(
                "↩️ Ignoring Overtime OU2.5 %.3f (out of [%s,%s]) for fixture %s",
                v, ODDS_MIN, ODDS_MAX, fixture_id
            )
            return odds

        new_odds = dict(odds or {})
        new_odds["over_2_5"] = v
        logger.info("🔄 Enriched Over 2.5 from Overtime for fixture %s: %.3f", fixture_id, v)
        return new_odds
    except Exception as e:
        logger.warning("⚠️ Overtime enrich failed for fixture %s: %s", fixture_id, e)
        return odds

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main():
    try:
        # quick sanity that Render env is wired
        supa_url_present = bool(os.getenv("SUPABASE_URL"))
        supa_key_present = bool(
            os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_ANON_KEY")
        )
        ot_key_present = bool(os.getenv("OVERTIME_API_KEY"))
        logger.info(
            "🚀 Starting system… (reasoned_writer=%s, overtime=%s, SUPABASE_URL=%s, SUPABASE_KEY=%s, OVERTIME_KEY=%s)",
            _HAS_REASONED_WRITER, _HAS_OVERTIME, supa_url_present, supa_key_present, ot_key_present
        )

        # 0) Ingest Overtime games then LINK THEM (👉 this is the new linker call)
        if _HAS_OVERTIME:
            try:
                games, written = ingest_all_overtime_soccer()
                logger.info("🧲 Overtime ingest: games=%s, rows_written=%s", games, written)
            except Exception as e:
                logger.warning("⚠️ Overtime ingest failed: %s", e)

            # 👉 DB-based linker: matches_ot → fixtures (today..+7d by default)
            try:
                g, linked, skipped = link_overtime_to_fixtures_from_db()
                logger.info("🔗 Overtime linking (DB): games=%s, linked=%s, skipped=%s", g, linked, skipped)
            except Exception as e:
                logger.warning("⚠️ Overtime linking failed: %s", e)

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

                # First, pull odds (legacy) and try to enrich OU2.5 from Overtime via link
                odds = get_match_odds(fixture_id) or {}
                odds = _try_enrich_over25_with_overtime(fixture_id, odds)

                # Gate early: only proceed if OU 2.5 exists and is within [ODDS_MIN, ODDS_MAX]
                if not _has_ou25_in_range(odds):
                    logger.info(
                        f"🪙 Skipping GPT for {fixture_id}: Over 2.5 odds not in range [{ODDS_MIN},{ODDS_MAX}]."
                    )
                    # still insert match (if missing) so we have record/odds snapshot
                    match_json_min = {
                        "fixture_id": fixture_id,
                        "date": base["date"],
                        "league": base["league"],
                        "venue": base["venue"],
                        "home_team": home,
                        "away_team": away,
                        "odds": odds,
                        "head_to_head": [],
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    }
                    if not exists:
                        try:
                            insert_match(match_json_min)
                            logger.info(f"✅ Inserted match {fixture_id} (odds-only)")
                        except Exception as e:
                            logger.error(f"❌ Error inserting match {fixture_id}: {e}")
                    continue

                # Only now fetch heavy team data
                home["position"] = get_team_position(home["id"], league_id, season)
                away["position"] = get_team_position(away["id"], league_id, season)

                h_form, h_xg = get_team_form_and_goals(home["id"], league_id, season)
                a_form, a_xg = get_team_form_and_goals(away["id"], league_id, season)
                home["form"], home["xg"] = h_form, h_xg
                away["form"], away["xg"] = a_form, a_xg

                home["recent_goals"] = get_recent_goals(home["id"])
                away["recent_goals"] = get_recent_goals(away["id"])

                home["injuries"] = get_team_injuries(home["id"], season)
                away["injuries"] = get_team_injuries(away["id"], season)

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

                # Prediction (get_prediction returns only over_2_5 market)
                logger.info(f"🤖 Getting prediction for fixture {fixture_id}")
                prediction = get_prediction(match_json)
                if not prediction:
                    logger.info(f"🟨 No prediction returned for fixture {fixture_id}")
                    failed += 1
                    continue

                block = (prediction.get("predictions") or {}).get("over_2_5")

                if _HAS_REASONED_WRITER:
                    written, reason = write_value_prediction_with_reasons(int(fixture_id), "over_2_5", block)
                    logger.info(
                        "✍️ value_predictions wrote: %s (reason=%s) for fixture %s",
                        written, reason, fixture_id
                    )
                else:
                    try:
                        wrote = _legacy_insert_value_predictions(prediction)
                        why = "LEGACY_PATH"
                    except Exception as e:
                        wrote = 0
                        why = f"LEGACY_EXCEPTION:{e}"
                    logger.info(
                        "✍️ value_predictions wrote: %s (reason=%s) for fixture %s",
                        wrote, why, fixture_id
                    )

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
