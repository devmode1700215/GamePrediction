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

    # Rationale: store NULL if empty
    rationale = pred.get("rationale")
    if isinstance(rationale, list):
        rationale = " • ".join([str(x) for x in rationale[:6]])
    if isinstance(rationale, str) and rationale.strip() == "":
        rationale = None
    if rationale == []:
        rationale = None

    is_ot = bool(pred.get("is_overtime_odds"))

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
        "is_overtime_odds": is_ot,
        "odds_source": odds_source,
    }

    resp = postgrest_upsert("value_predictions", [row], on_conflict="fixture_id,market")

    # count from .data if present; else from status
    data = getattr(resp, "data", None)
    if isinstance(data, list):
        return (len(data), "upserted")
    status = getattr(resp, "status_code", None) or getattr(resp, "status", None)
    if isinstance(status, int) and status in (200, 201, 204):
        return (1, "upserted")
    err = getattr(resp, "error", None) or getattr(resp, "message", None) or f"unexpected:{type(resp).__name__}"
    return (0, f"error:{err}")
