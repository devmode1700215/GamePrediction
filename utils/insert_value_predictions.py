# utils/insert_value_predictions.py
from typing import Dict, Any
from utils.supabaseClient import supabase

ODDS_MIN, ODDS_MAX = 1.6, 2.3

def _to_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def insert_value_predictions(prediction_data: Dict[str, Any]) -> int:
    """
    Insert/Upsert value predictions for a single fixture.
    Filters:
      - odds between ODDS_MIN and ODDS_MAX
      - po_value == True
    Returns the number of rows successfully written.
    """
    fixture_id = prediction_data.get("fixture_id")
    predictions = (prediction_data.get("predictions") or {})

    if not fixture_id:
        print("❌ insert_value_predictions: missing fixture_id")
        return 0
    if not isinstance(predictions, dict) or not predictions:
        print(f"ℹ️ insert_value_predictions: no predictions object for fixture {fixture_id}")
        return 0

    to_write = []
    for market, details in predictions.items():
        if not isinstance(details, dict):
            print(f"⚠️ skipping market {market}: not a dict")
            continue

        # Odds filter
        odds = _to_float(details.get("odds"))
        if odds is None:
            print(f"⚠️ skipping {fixture_id} | {market}: odds is None/invalid")
            continue
        if not (ODDS_MIN <= odds <= ODDS_MAX):
            print(f"⚠️ skipping {fixture_id} | {market}: odds {odds} outside [{ODDS_MIN},{ODDS_MAX}]")
            continue

        # PO filter
        po_value = details.get("po_value")
        if po_value is not True:
            print(f"⚠️ skipping {fixture_id} | {market}: po_value is {po_value} (needs to be True)")
            continue

        entry = {
            "fixture_id": fixture_id,
            "market": market,
            "prediction": details.get("prediction"),
            "confidence_pct": _to_float(details.get("confidence")),
            "po_value": True,
            "stake_pct": _to_float(details.get("bankroll_pct")),
            "edge": _to_float(details.get("edge")),
            "odds": odds,
            "rationale": details.get("rationale"),
        }
        to_write.append(entry)

    if not to_write:
        print(f"ℹ️ insert_value_predictions: nothing to insert for fixture {fixture_id}")
        return 0

    try:
        resp = supabase.table("value_predictions") \
            .upsert(to_write, on_conflict="fixture_id,market") \
            .execute()
        written = len(resp.data or [])
        print(f"✅ value_predictions upsert OK: wrote {written}/{len(to_write)} for fixture {fixture_id}")
        if written < len(to_write):
            print(f"ℹ️ upsert returned fewer rows than attempted; check unique constraint or RLS.")
        return written
    except Exception as e:
        print(f"❌ value_predictions upsert failed for fixture {fixture_id}: {e}")
        return 0
