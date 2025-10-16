# utils/insert_value_predictions.py
from __future__ import annotations

import os
import json
import logging
from typing import Any, Dict, Tuple

from utils.supabaseClient import supabase

DEFAULT_STAKE_AMOUNT = float(os.getenv("DEFAULT_STAKE_AMOUNT", "5"))

def _try_upsert(payload: Dict[str, Any], include_stake_amount: bool) -> Tuple[int, str]:
    data = dict(payload)
    if not include_stake_amount:
        data.pop("stake_amount", None)

    try:
        res = supabase.table("value_predictions").upsert(
            data,
            on_conflict="fixture_id,market",
        ).execute()

        # supabase-py returns an APIResponse-like object
        rows = getattr(res, "data", None) or []
        return (len(rows), "upserted" if rows else "nochange")
    except Exception as e:
        return (0, f"error:{e}")

def insert_value_predictions(pred: Dict[str, Any], odds_source: str = "apifootball") -> Tuple[int, str]:
    """
    pred is the dict returned by get_prediction(), minimally containing:
      - fixture_id (int)
      - market       (e.g. 'over_2_5')
      - prediction   ('Over'/'Under')
      - odds         (float)
      - confidence   (0..1) or confidence_pct
      - edge         (float, optional)
      - po_value     (bool, optional)

    Writes with stake_amount=DEFAULT_STAKE_AMOUNT.
    """
    fixture_id = pred.get("fixture_id")
    market     = pred.get("market") or "over_2_5"
    prediction = pred.get("prediction")
    odds       = pred.get("odds")
    confidence = pred.get("confidence") or pred.get("confidence_pct")

    # normalize confidence to pct
    if confidence is not None and confidence <= 1.0:
        confidence_pct = round(float(confidence) * 100.0, 2)
    else:
        try:
            confidence_pct = round(float(confidence), 2) if confidence is not None else None
        except Exception:
            confidence_pct = None

    payload = {
        "fixture_id": fixture_id,
        "market": market,
        "prediction": prediction,
        "odds": odds,
        "confidence_pct": confidence_pct,
        "edge": pred.get("edge"),
        "po_value": bool(pred.get("po_value", True)),
        "odds_source": odds_source,
        # NEW: fixed stake (fallback to DEFAULT_STAKE_AMOUNT env)
        "stake_amount": float(pred.get("stake_amount") or DEFAULT_STAKE_AMOUNT),
    }

    # Optional rationale (short list of bullets or string)
    rationale = pred.get("rationale")
    if isinstance(rationale, list):
        payload["rationale"] = json.dumps(rationale, ensure_ascii=False)
    elif isinstance(rationale, str):
        payload["rationale"] = rationale

    # Try with stake_amount first; if schema doesn't have it, retry without.
    count, msg = _try_upsert(payload, include_stake_amount=True)
    if msg.startswith("error:") and "column" in msg.lower() and "stake_amount" in msg.lower():
        logging.info("[value_predictions] stake_amount not in schema; retrying without it.")
        count, msg = _try_upsert(payload, include_stake_amount=False)

    return count, msg
