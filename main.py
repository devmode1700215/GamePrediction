# main.py
import os
import logging
import sys
from copy import deepcopy
from datetime import datetime, timedelta, timezone

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

# ───────────────────────── Logging & ENV ─────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

ODDS_MIN = float(os.getenv("ODDS_MIN", "1.7"))
ODDS_MAX = float(os.getenv("ODDS_MAX", "2.3"))

# ───────────────────────── Helpers ─────────────────────────
def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None


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


def _read_over25_from_matches_odds(odds: dict):
    """Accept both flat and nested shapes from matches.odds."""
    if not isinstance(odds, dict):
        return None
    # flat: {"over_2_5": 1.85} or "1.85"
    if "over_2_5" in odds:
        v = odds["over_2_5"]
        if isinstance(v, dict) and "over" in v:
            return _to_float(v.get("over"))
        return _to_float(v)
    # sometimes inserted as {"ou_2_5": {"over": 1.85, "under": 1.95}}
    if "ou_2_5" in odds and isinstance(odds["ou_2_5"], dict):
        return _to_float(odds["ou_2_5"].get("over"))
    return None


def _read_over25_from_ot_odds(ot_odds: dict):
    """
    Overtime stores OU as {'ou_2_5': {'over': x, 'under': y}}.
    Some feeds flatten as 'ou_2_5_over' or 'ou25_over'.
    """
    if not isinstance(ot_odds, dict):
        return None
    if "ou_2_5" in ot_odds and isinstance(ot_odds["ou_2_5"], dict):
        return _to_float(ot_odds["ou_2_5"].get("over"))
    for k in ("ou_2_5_over", "ou25_over"):
        if k in ot_odds:
            v = _to_float(ot_odds.get(k))
            if v is not None:
                return v
    return None


def _select_over25_odds(fixture_id: int, matches_odds: dict):
    """
    Prefer Overtime OU2.5 'over' when a link exists; otherwise use matches.odds.
    Returns (odds_value or None, 'overtime'|'apifootball'|'none').
    """
    # 1) Try Overtime if linked
    try:
        link_q = (
            supabase.table("ot_links")
            .select("game_id")
            .eq("fixture_id", fixture_id)
            .execute()
        )
        game_id = (link_q.data or [{}])[0].get("game_id") if (link_q.data) else None
        if game_id:
            mot_q = (
                supabase.table("matches_ot")
                .select("odds")
                .eq("game_id", game_id)
                .execute()
            )
            ot_odds = (mot_q.data or [{}])[0].get("odds")
            over = _read_over25_from_ot_odds(ot_odds)
            if over is not None:
                return over, "overtime"
    except Exception:
        # fall through
        pass

    # 2) Fall back to matches.odds
    over = _read_over25_from_matches_odds(matches_odds or {})
    if over is not None:
        return over, "apifootball"

    return None, "none"


def _normalize_prediction_shape(pred: dict) -> dict:
    """Force a stable shape for insert_value_predictions."""
    if not isinstance(pred, dict):
        return {}
    out = dict(pred)  # shallow copy

    # 1) Market mapping / default to OU 2.5 for current pipeline
    m = (out.get("market") or out.get("market_name") or out.get("marketType") or "").lower().strip()
    if m in ("ou_2_5", "ou25", "over25", "o/u 2.5", "o/u2.5", "over_2_5"):
        m = "over_2_5"
    elif m in ("btts", "both_to_score", "both_teams_to_score", "gg", "yes/no"):
        m = "btts"
    if not m:
        m = "over_2_5"
    out["market"] = m

    # 2) Pick mapping
    pick = out.get("prediction") or out.get("pick") or out.get("side")
    if isinstance(pick, str):
        p = pick.lower().strip()
        mapping = {"over": "Over", "under": "Under", "yes": "Yes", "no": "No"}
        out["prediction"] = mapping.get(p, out.get("prediction")) or ("Over" if m == "over_2_5" else None)

    # 3) Numeric coercions & odds extraction
    def _f(x):
        try:
            return float(x)
        except Exception:
            return None

    if out.get("odds") is None:
        candidates = [
            out.get("market_odds"),
            (out.get("prices") or {}).get("over"),
            (out.get("selection") or {}).get("odds"),
            (out.get("markets") or {}).get(m, {}).get("odds") if isinstance(out.get("markets"), dict) else None,
            (out.get("markets") or {}).get(m, {}).get("over") if isinstance(out.get("markets"), dict) else None,
            (out.get("market_details") or {}).get("odds"),
        ]
        for c in candidates:
            fc = _f(c)
            if fc is not None:
                out["odds"] = fc
                break
    else:
        out["odds"] = _f(out.get("odds"))

    for k in ("confidence_pct", "edge", "stake_pct"):
        if k in out and not isinstance(out[k], (int, float)):
            out[k] = _f(out[k])

    # 4) Ensure po_value exists
    if "po_value" not in out:
        e = out.get("edge")
        out["po_value"] = True if (e is not None and e > 0) else bool(out.get("po_value", False))

    return out


# ───────────────────────── Main ─────────────────────────
def main():
    try:
        logger.info("🚀 Starting football prediction system… Odds gate: [%s, %s]", ODDS_MIN, ODDS_MAX)

        # 1) Update results first
        update_results_for_finished_matches()

        # 2) Build a 48h window: today + tomorrow + day after
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

        logger.info(f"🔍 Processing {len(fixtures)} fixtures…")

        successful = 0
        failed = 0

        for match in fixtures:
            try:
                base = safe_extract_match_data(match)
                if not base:
                    failed += 1
                    continue

                fixture_id = base["fixture_id"]

                # Check if we already have a matches row; if yes, do not insert again
                already = supabase.table("matches").select("fixture_id").eq("fixture_id", fixture_id).execute()
                exists = bool(already.data)

                # Gather extra team data we feed to GPT
                season = base["season"]
                league_id = base["league_id"]
                if not season or not league_id:
                    logger.info(f"🟨 Missing season/league for {fixture_id}; skipping.")
                    failed += 1
                    continue

                home = deepcopy(base["home_team"])
                away = deepcopy(base["away_team"])

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
                odds_raw = get_match_odds(fixture_id) or {}
                h2h = get_head_to_head(home["id"], away["id"])

                # Insert (or skip) the match row (store the raw odds snapshot)
                match_row = {
                    "fixture_id": fixture_id,
                    "date": base["date"],
                    "league": base["league"],
                    "venue": base["venue"],
                    "home_team": home,
                    "away_team": away,
                    "odds": odds_raw,
                    "head_to_head": h2h,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
                if not exists:
                    try:
                        insert_match(match_row)  # your insert_match normalizes odds if needed
                        logger.info(f"✅ Inserted match {fixture_id}")
                    except Exception as e:
                        logger.error(f"❌ Error inserting match {fixture_id}: {e}")
                        # continue to predictions anyway

                # Choose OU2.5 odds (prefer Overtime when linked)
                over_odds, src = _select_over25_odds(fixture_id, odds_raw)

                if over_odds is None:
                    logger.info(f"🪙 Skipping GPT for {fixture_id}: no Over 2.5 odds found (src={src}).")
                    continue

                if not (ODDS_MIN <= over_odds <= ODDS_MAX):
                    logger.info(
                        f"🪙 Skipping GPT for {fixture_id}: Over 2.5 {over_odds:.2f} not in range [{ODDS_MIN},{ODDS_MAX}] (src={src})."
                    )
                    continue

                # Build the model input – pass only the odds we truly used
                model_input = deepcopy(match_row)
                model_input["odds"] = {"over_2_5": over_odds, "source": src}

                # Run prediction
                logger.info(f"🤖 Getting prediction for fixture {fixture_id} (OU2.5={over_odds:.2f}, src={src})")
                prediction = get_prediction(model_input)
                if not prediction:
                    logger.info(f"🟨 No prediction returned for fixture {fixture_id}")
                    failed += 1
                    continue

                # Normalize shape so market is always present (defaults to over_2_5)
                prediction = _normalize_prediction_shape(prediction)
                
                # If the model didn’t echo odds, use the exact OU2.5 price we selected
if prediction.get("odds") is None:
    prediction["odds"] = float(over_odds)


                # Insert value predictions (handle both old and new return types)
                res = insert_value_predictions(prediction, odds_source=src)
                if isinstance(res, tuple):
                    wrote, reason = res
                else:
                    wrote, reason = (int(res) if res else 0), ("OK" if res else "UNKNOWN")

                if wrote:
                    logger.info(f"🟢 value_predictions wrote: {wrote} for fixture {fixture_id}")
                    successful += 1
                else:
                    logger.info(f"✍️ value_predictions wrote: {wrote} (reason={reason}) for fixture {fixture_id}")
                    failed += 1

            except Exception as e:
                logger.error(f"❌ Unexpected error processing fixture {match.get('fixture', {}).get('id')}: {e}")
                failed += 1
                continue

        logger.info(f"🎯 Processing complete: {successful} successful, {failed} failed")

    except Exception as e:
        logger.error(f"❌ Critical error in main: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
