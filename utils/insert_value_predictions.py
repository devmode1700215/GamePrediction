# utils/insert_value_predictions.py
from __future__ import annotations
from typing import Any, Dict, Tuple, Optional

from utils.supa import postgrest_upsert

def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _clip(x: Optional[float], lo: float, hi: float) -> Optional[float]:
    try:
        return max(lo, min(hi, float(x))) if x is not None else None
    except Exception:
        return None

def _derive_confidence(pred: Dict[str, Any]) -> float:
    pick = pred.get("prediction")
    prob_over = _to_float(pred.get("prob_over"))
    conf = _to_float(pred.get("confidence"))
    if conf is None and prob_over is not None:
        conf = prob_over if pick == "Over" else (1.0 - prob_over)
    conf = _clip(conf, 0.05, 0.99)
    return conf if conf is not None else 0.50

def insert_value_predictions(pred: Dict[str, Any], *, odds_source: str = None) -> Tuple[int, str]:
    """
    Expects:
      fixture_id:int, market:str ('over_2_5'), prediction:'Over'|'Under',
      confidence/ prob_over (floats), odds, edge, rationale
    Upsert key: (fixture_id, market)
    Returns (count, message) where count is 1 on success.
    """
    fixture_id = pred.get("fixture_id")
    market = pred.get("market") or "over_2_5"
    if fixture_id is None or not market:
        return (0, "missing fixture_id/market")

    prediction = pred.get("prediction")
    if prediction not in ("Over", "Under"):
        prediction = "Over"

    confidence = _derive_confidence(pred)
    odds = _to_float(pred.get("odds"))
    edge = _to_float(pred.get("edge"))

    rationale = pred.get("rationale")
    if isinstance(rationale, list):
        rationale = " • ".join([str(x) for x in rationale[:6]])
    elif rationale is not None:
        rationale = str(rationale)

    row = {
        "fixture_id": fixture_id,
        "market": market,
        "prediction": prediction,
        "confidence_pct": round(confidence, 4),
        "po_value": (edge is not None and edge > 0.0),
        "stake_pct": 0.0,  # staking handled elsewhere
        "odds": odds,
        "rationale": rationale,
        "edge": round(edge, 4) if edge is not None else None,
        "is_overtime_odds": False,
        "odds_source": odds_source,
    }

    resp = postgrest_upsert("value_predictions", [row], on_conflict="fixture_id,market")

    # --- Make this work across supabase-py versions ---
    # v2: APIResponse with .data (list|None) and .status_code
    # Some setups return 201 with empty body unless Prefer:return=representation is set.
    count = 0
    msg = "unknown"

    # Try .data first
    data = getattr(resp, "data", None)
    if isinstance(data, list):
        count = len(data)
        msg = "upserted"
    else:
        # Fallback to status code
        status = getattr(resp, "status_code", None) or getattr(resp, "status", None)
        if isinstance(status, int) and status in (200, 201, 204):
            count = 1
            msg = "upserted"
        else:
            # Best effort error text
            err = getattr(resp, "error", None) or getattr(resp, "message", None)
            if err:
                msg = f"error:{err}"
            else:
                msg = f"unexpected_response:{type(resp).__name__}"

    return (count, msg)
