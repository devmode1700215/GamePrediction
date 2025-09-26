# utils/insert_value_predictions.py
from datetime import datetime, timezone
from typing import Tuple, Optional
from utils.supabaseClient import supabase
import os

ODDS_MIN = float(os.getenv("ODDS_MIN", "1.7"))
ODDS_MAX = float(os.getenv("ODDS_MAX", "2.3"))
CONF_MIN = float(os.getenv("CONF_MIN", "70"))

def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def _nz(v, default=0.0):
    f = _to_float(v)
    return f if f is not None else default

def insert_value_predictions(pred: dict, *, odds_source: str = "apifootball") -> Tuple[int, str]:
    """
    Returns (rows_written, reason). reason is 'OK' on success or a clear block reason.
    """
    try:
        market = pred.get("market")
        if market not in ("over_2_5", "btts"):
            return 0, f"UNSUPPORTED_MARKET:{market}"

        fixture_id   = pred.get("fixture_id")
        pick         = pred.get("prediction")
        confidence   = _to_float(pred.get("confidence_pct"))
        edge         = _to_float(pred.get("edge"))
        po_value     = bool(pred.get("po_value"))
        stake_pct    = _to_float(pred.get("stake_pct"))
        odds         = _to_float(pred.get("odds"))
        rationale    = pred.get("rationale")

        # Gate checks (same rules you’ve been using)
        if odds is None:
            return 0, "NO_ODDS"
        if not (ODDS_MIN <= odds <= ODDS_MAX):
            return 0, f"ODDS_OUT_OF_RANGE:{odds}"
        if confidence is None or confidence < CONF_MIN:
            return 0, f"CONFIDENCE_BELOW_MIN:{confidence}"
        if edge is None or edge <= 0:
            return 0, f"EDGE<=0:{edge}"
        if not po_value:
            return 0, "PO_VALUE_FALSE"
        if stake_pct is None or stake_pct <= 0:
            return 0, f"STAKE_PCT_INVALID:{stake_pct}"

        row = {
            "fixture_id": fixture_id,
            "market": market,
            "prediction": pick,
            "confidence_pct": _nz(confidence),
            "po_value": True,
            "stake_pct": _nz(stake_pct),
            "odds": _nz(odds),
            "rationale": rationale,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "edge": _nz(edge),
            # new flags/metadata
            "is_overtime_odds": (odds_source == "overtime"),
            "odds_source": odds_source,
        }

        # Upsert on (fixture_id, market) if you have that unique constraint/index
        res = (
            supabase.table("value_predictions")
            .upsert(row, on_conflict="fixture_id,market")
            .execute()
        )

        # res.data can be empty depending on PostgREST prefs; treat no-error as success
        return 1, "OK"
    except Exception as e:
        return 0, f"HTTP_EXCEPTION:{e}"
