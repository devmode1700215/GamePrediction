# utils/insert_value_predictions.py
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

def insert_value_predictions(pred: Dict[str, Any], *, odds_source: str = None) -> Tuple[int, str]:
    """
    Expects:
      fixture_id:int, market:str ('over_2_5'), prediction:'Over'|'Under',
      confidence:float (0..1), odds:float|None, edge:float|None, rationale:list[str]|str
    Upsert key: (fixture_id, market)
    """
    fixture_id = pred.get("fixture_id")
    market = pred.get("market") or "over_2_5"
    if fixture_id is None or not market:
        return (0, "missing fixture_id/market")

    prediction = pred.get("prediction")
    if prediction not in ("Over", "Under"):
        prediction = "Over"

    conf = _to_float(pred.get("confidence"))
    prob_over = _to_float(pred.get("prob_over"))
    if conf is None:
        if prob_over is not None:
            conf = prob_over if prediction == "Over" else (1.0 - prob_over)
    conf = _clip(conf, 0.05, 0.99)
    if conf is None:
        conf = 0.50  # final fallback

    odds = _to_float(pred.get("odds"))
    edge = _to_float(pred.get("edge"))

    rationale = pred.get("rationale")
    if isinstance(rationale, list):
        rationale = " • ".join([str(x) for x in rationale[:6]])
    elif rationale is not None:
        rationale = str(rationale)

    signals = pred.get("signals")
    # Optional: you can persist signals in rationale tail if you like (kept simple here)

    row = {
        "fixture_id": fixture_id,
        "market": market,
        "prediction": prediction,
        "confidence_pct": round(conf, 4),
        "po_value": (edge is not None and edge > 0.0),
        "stake_pct": 0.0,  # your staking module can update later if needed
        "odds": odds,
        "rationale": rationale,
        "edge": round(edge, 4) if edge is not None else None,
        "is_overtime_odds": False,
        "odds_source": odds_source,
    }

    recs = postgrest_upsert("value_predictions", [row], on_conflict="fixture_id,market")
    return (len(recs), "upserted")
