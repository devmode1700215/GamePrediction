# utils/db_write.py
# -*- coding: utf-8 -*-
import os
import json
import logging
from typing import Dict, Tuple, Any, Optional
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
TABLE = os.getenv("VALUE_PREDICTIONS_TABLE", "value_predictions")

# Behavior flags
ALWAYS_WRITE = os.getenv("OU25_ALWAYS_WRITE", "0") == "1"  # write even when non-value
ONLY_WRITE_VALUE = os.getenv("OU25_ONLY_WRITE_VALUE", "0") == "1"  # force gate by value

def _headers() -> Dict[str, str]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY/ANON_KEY")
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }

def _explain_postgrest_error(text: str) -> str:
    """
    Extract a friendly reason from PostgREST error JSON/text.
    """
    try:
        data = json.loads(text)
    except Exception:
        data = {}
    msg = (data.get("message") or "").lower()
    details = (data.get("details") or "").lower()
    hint = (data.get("hint") or "").lower()

    if "upsert" in msg and "unique" in msg:
        return "UPSERT_NEEDS_UNIQUE_CONSTRAINT"
    if "policy" in msg or "rls" in msg or "not authorized" in msg or "permission" in msg:
        return "RLS_FORBIDDEN"
    if "column" in msg and ("does not exist" in msg or "missing" in msg):
        return "PAYLOAD_COLUMN_MISMATCH"
    if "type" in msg and "cannot be cast" in msg:
        return "PAYLOAD_TYPE_CAST_ERROR"
    if "violates unique" in msg:
        return "UNIQUE_VIOLATION"
    if "not null" in msg:
        return "NOT_NULL_VIOLATION"
    if "json" in msg and "malformed" in msg:
        return "MALFORMED_JSON"
    if any(k in hint for k in ("create unique index", "unique constraint")):
        return "UPSERT_NEEDS_UNIQUE_CONSTRAINT"
    return "HTTP_4XX"

def _post_row(row: Dict[str, Any]) -> Tuple[int, str, int, str]:
    """
    Returns: (written_count, reason, status_code, resp_text)
    """
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?on_conflict=fixture_id,market"
    headers = _headers()
    resp = requests.post(url, headers=headers, data=json.dumps(row))
    if resp.status_code >= 400:
        reason = _explain_postgrest_error(resp.text)
        return 0, reason, resp.status_code, resp.text

    # With "return=representation", success returns an array of rows
    try:
        payload = resp.json()
        written = len(payload) if isinstance(payload, list) else (1 if payload else 0)
    except Exception:
        written = 0
    return written, "OK", resp.status_code, resp.text

def build_row_from_prediction(fixture_id: int, market: str, block: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map model/local fields to DB row. Adjust here if your DB uses different names.
    """
    return {
        "fixture_id": int(fixture_id or 0),
        "market": market,                                          # "over_2_5"
        "prediction": block.get("prediction"),                     # "Over"/"Under"
        "edge": float(block.get("edge") or 0),
        "po_value": bool(block.get("po_value") or False),
        "odds": float(block.get("odds") or 0),
        "stake_pct": float(block.get("bankroll_pct") or 0),        # map bankroll_pct -> stake_pct
        "confidence_pct": int(block.get("confidence") or 0),       # map confidence -> confidence_pct
        "rationale": str(block.get("rationale") or ""),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

def write_value_prediction(fixture_id: int, market: str, block: Optional[Dict[str, Any]]) -> Tuple[int, str]:
    """
    High-level writer with explicit reasons for '0' writes.
    Returns (written_count, reason).
    """
    if not isinstance(block, dict):
        logger.warning("✋ MISSING_MARKET block for fixture %s", fixture_id)
        return 0, "MISSING_MARKET"

    odds = block.get("odds")
    try:
        odds_f = float(odds)
    except Exception:
        odds_f = None

    if odds_f is None or odds_f <= 1.0:
        logger.info("ℹ️ NO_ODDS for fixture %s (market=%s)", fixture_id, market)
        return 0, "NO_ODDS"

    po_value = bool(block.get("po_value"))
    if ONLY_WRITE_VALUE and not po_value and not ALWAYS_WRITE:
        # User explicitly wants to write only value bets
        logger.info("ℹ️ NON_VALUE gated write for fixture %s (po_value=false)", fixture_id)
        return 0, "NON_VALUE"

    row = build_row_from_prediction(fixture_id, market, block)
    try:
        written, reason, status, text = _post_row(row)
        if written == 0 and reason != "OK":
            logger.error("❌ Supabase write failed (%s, %s): %s", status, reason, text)
        else:
            logger.info("✅ Supabase wrote %s row(s) for fixture %s", written, fixture_id)
        return written, reason
    except Exception as e:
        logger.exception("❌ HTTP_EXCEPTION during Supabase write: %s", e)
        return 0, "HTTP_EXCEPTION"
