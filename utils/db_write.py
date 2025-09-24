# utils/db_write.py
# -*- coding: utf-8 -*-
import os
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

import requests

log = logging.getLogger(__name__)

# ---- Config (overridable via env) -------------------------------------------
ODDS_MIN = float(os.getenv("ODDS_MIN", "1.6"))
ODDS_MAX = float(os.getenv("ODDS_MAX", "2.3"))
CONF_MIN = float(os.getenv("CONFIDENCE_MIN", "70"))
EDGE_MIN = float(os.getenv("EDGE_MIN", "5"))

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_KEY")
    or os.getenv("SUPABASE_ANON_KEY")
)
TABLE = os.getenv("VALUE_PREDICTIONS_TABLE", "value_predictions")

_sess = requests.Session()
if SUPABASE_KEY:
    _sess.headers.update({
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    })

# ---- Helpers ----------------------------------------------------------------
def _num(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _headers_ok():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY/ANON_KEY")

def _post_row(row: Dict[str, Any]) -> Tuple[int, str, int, str]:
    """HTTP insert with safe manual upsert fallback when unique constraint isn't present."""
    _headers_ok()
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?on_conflict=fixture_id,market"
    r = _sess.post(url, data=json.dumps(row))
    if r.status_code < 400:
        return 1, "OK", r.status_code, r.text or ""
    # Manual upsert if table lacks unique index
    if "no unique or exclusion constraint" in (r.text or "").lower():
        _sess.delete(
            f"{SUPABASE_URL}/rest/v1/{TABLE}?fixture_id=eq.{row['fixture_id']}&market=eq.{row['market']}",
            headers={"Prefer": "return=minimal"}
        )
        ins = _sess.post(f"{SUPABASE_URL}/rest/v1/{TABLE}", data=json.dumps(row))
        if ins.status_code < 400:
            return 1, "MANUAL_UPSERT_OK", ins.status_code, ins.text or ""
        return 0, f"INSERT_FAIL:{ins.text}", ins.status_code, ins.text or ""
    return 0, f"HTTP_{r.status_code}", r.status_code, r.text or ""

# ---- Public API -------------------------------------------------------------
def write_value_prediction(fixture_id: int, market: str, block: Dict[str, Any]) -> Tuple[int, str]:
    """
    Gate & write a single market block into value_predictions.
    Returns: (written_count, reason)
    - Enforces ODDS_MIN..ODDS_MAX (hard gate)
    - Enforces CONF_MIN (hard gate)
    - Enforces EDGE_MIN (hard gate)
    """
    try:
        if not isinstance(block, dict):
            return 0, "EMPTY_BLOCK"

        # Normalize fields from block
        prediction = (block.get("prediction") or block.get("pick") or "").strip()  # e.g., "Over" / "Under"
        odds = _num(block.get("odds"))
        stake_pct = _num(block.get("stake_pct"), 0.0)
        confidence = _num(block.get("confidence_pct"))
        edge = _num(block.get("edge"))
        po_value = bool(block.get("po_value", False))
        rationale = block.get("rationale") or block.get("reason") or ""

        # ---- Hard gates ------------------------------------------------------
        if odds is None:
            return 0, "MISSING_ODDS"
        if not (ODDS_MIN <= odds <= ODDS_MAX):
            return 0, "ODDS_OUT_OF_RANGE"
        if confidence is None or confidence < CONF_MIN:
            return 0, "CONFIDENCE_BELOW_MIN"
        if edge is None or edge < EDGE_MIN:
            return 0, "EDGE_BELOW_MIN"
        if stake_pct is None or stake_pct <= 0:
            return 0, "STAKE_ZERO_OR_NEG"
        if not po_value:
            return 0, "NON_VALUE"

        # Minimal prediction text fallback
        if not prediction:
            prediction = "Over" if "over" in market else "Under"

        row = {
            "fixture_id": int(fixture_id),
            "market": str(market),
            "prediction": prediction,
            "odds": float(odds),
            "stake_pct": float(stake_pct),
            "confidence_pct": float(confidence),
            "edge": float(edge),
            "po_value": True,
            "rationale": rationale,
            "created_at": _now_iso(),
        }

        wrote, reason, status, text = _post_row(row)
        if wrote == 0:
            log.error("❌ HTTP_EXCEPTION during Supabase write: %s (%s) %s", reason, status, text)
            return 0, "HTTP_4XX" if 400 <= status < 500 else "HTTP_EXCEPTION"

        return wrote, reason

    except Exception as e:
        log.exception("write_value_prediction failed: %s", e)
        return 0, "EXCEPTION"
