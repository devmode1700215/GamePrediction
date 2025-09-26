# utils/insert_value_predictions.py
from datetime import datetime, timezone
from utils.supabaseClient import supabase

ODDS_MIN = 1.7
ODDS_MAX = 2.3
CONF_MIN = 70.0

def _to_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def insert_value_predictions(prediction: dict, odds_source: str = "apifootball") -> int:
    """
    Writes value predictions.
    odds_source: 'overtime' or 'apifootball' – used to set is_overtime_odds + odds_source columns.
    Returns number of rows written (0/1 in your current flow).
    """
    try:
        market = prediction.get("market")
        if market not in ("over_2_5", "btts"):
            return 0

        fixture_id = prediction.get("fixture_id")
        odds = _to_float(prediction.get("odds"))
        confidence = _to_float(prediction.get("confidence_pct"))
        edge = _to_float(prediction.get("edge"))
        po_value = bool(prediction.get("po_value"))
        stake_pct = _to_float(prediction.get("stake_pct"))
        pick = prediction.get("prediction")
        rationale = prediction.get("rationale")

        # Enforce your gates (same as before)
        if odds is None or not (ODDS_MIN <= odds <= ODDS_MAX):
            return 0
        if confidence is None or confidence < CONF_MIN:
            return 0
        if edge is None or edge <= 0:
            return 0
        if not po_value:
            return 0

        row = {
            "fixture_id": fixture_id,
            "market": market,
            "prediction": pick,
            "confidence_pct": confidence,
            "po_value": True,
            "stake_pct": stake_pct,
            "odds": odds,
            "rationale": rationale,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "edge": edge,
            # NEW:
            "is_overtime_odds": (odds_source == "overtime"),
            "odds_source": odds_source,  # if you added the optional column
        }

        # upsert on (fixture_id, market) if you have that constraint; otherwise simple insert
        res = supabase.table("value_predictions").upsert(
            row, on_conflict="fixture_id,market"
        ).execute()

        return 1 if res.data else 0

    except Exception as e:
        # keep your existing logging if you have one
        print(f"❌ insert_value_predictions failed: {e}")
        return 0
