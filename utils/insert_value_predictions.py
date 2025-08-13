# utils/insert_value_predictions.py

from typing import Dict, Any
from utils.supabaseClient import supabase

# Thresholds for inserting predictions
CONF_MIN = 70.0  # percent
ODDS_MIN, ODDS_MAX = 1.6, 2.3  # acceptable odds range


def _to_float(x):
    """Convert odds/confidence strings like '1.85' or '70%' to float."""
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip().replace("%", "")
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _to_bool_true(x):
    """Interpret various truthy values as True."""
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return x == 1
    if isinstance(x, str):
        return x.strip().lower() in ("true", "1", "yes")
    return False


def insert_value_predictions(prediction_data: Dict[str, Any]) -> int:
    """
    Insert markets from prediction_data into Supabase table value_predictions
    if they pass confidence, odds, and po_value checks.
    Returns number of markets inserted.
    """
    fixture_id = prediction_data.get("fixture_id")
    predictions = prediction_data.get("predictions", {}) or {}
    inserted_count = 0

    if not isinstance(predictions, dict) or not predictions:
        print(f"ℹ️ No valid prediction markets for fixture {fixture_id}")
        return 0

    for market, details in predictions.items():
        if not isinstance(details, dict):
            print(f"⚠️ Invalid prediction format for {fixture_id} | {market}: {details}")
            continue

        odds = _to_float(details.get("odds"))
        confidence = _to_float(details.get("confidence"))
        po_value = _to_bool_true(details.get("po_value"))

        # Detailed skip logs
        if odds is None:
            print(f"⛔ SKIP {fixture_id} | {market}: odds missing or invalid ({details.get('odds')!r})")
            continue
        if not (ODDS_MIN <= odds <= ODDS_MAX):
            print(f"⛔ SKIP {fixture_id} | {market}: odds {odds} outside range {ODDS_MIN}-{ODDS_MAX}")
            continue
        if confidence is None:
            print(f"⛔ SKIP {fixture_id} | {market}: confidence missing or invalid ({details.get('confidence')!r})")
            continue
        if confidence < CONF_MIN:
            print(f"⛔ SKIP {fixture_id} | {market}: confidence {confidence} < min {CONF_MIN}")
            continue
        if not po_value:
            print(f"⛔ SKIP {fixture_id} | {market}: po_value is not true ({details.get('po_value')!r})")
            continue

        # Build entry
        entry = {
            "fixture_id": fixture_id,
            "market": market,
            "prediction": details.get("prediction"),
            "confidence_pct": confidence,
            "po_value": True,
            "stake_pct": _to_float(details.get("bankroll_pct")),
            "edge": _to_float(details.get("edge")),
            "odds": odds,
            "rationale": details.get("rationale"),
        }

        try:
            supabase.table("value_predictions").insert(entry).execute()
            inserted_count += 1
            print(f"✅ INSERTED {fixture_id} | {market} | odds={odds} conf={confidence}")
        except Exception as e:
            print(f"❌ FAILED insert for {fixture_id} | {market}: {e}")

    print(f"📦 Fixture {fixture_id}: inserted {inserted_count} market(s)")
    return inserted_count
