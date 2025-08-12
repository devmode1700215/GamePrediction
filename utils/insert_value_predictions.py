# utils/insert_value_predictions.py
from typing import Dict, Any
from utils.supabaseClient import supabase

# Minimum confidence threshold for value predictions
CONF_MIN = 70.0  # percent
ODDS_MIN, ODDS_MAX = 1.6, 2.3

def _to_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def insert_value_predictions(prediction_data: Dict[str, Any]):
    fixture_id = prediction_data.get("fixture_id")
    predictions = prediction_data.get("predictions", {}) or {}

    for market, details in predictions.items():
        if not isinstance(details, dict):
            continue

        odds = _to_float(details.get("odds"))
        confidence = _to_float(details.get("confidence"))

        # Skip if odds not in range or missing
        if odds is None or not (ODDS_MIN <= odds <= ODDS_MAX):
            print(f"⚠️ ODDS OUT OF RANGE: {fixture_id} | {market} | {odds}")
            continue

        # Skip if confidence too low/missing
        if confidence is None or confidence < CONF_MIN:
            print(f"⚠️ CONFIDENCE TOO LOW: {fixture_id} | {market} | {confidence}")
            continue

        if details.get("po_value") is True:
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
                print(f"✅ Inserted value prediction: {fixture_id} | {market}")
            except Exception as e:
                print(f"❌ Failed to insert value prediction for {fixture_id} | {market}: {e}")
