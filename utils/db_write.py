# utils/db_write.py
# -*- coding: utf-8 -*-
import os
import json
import logging
from typing import Dict, Tuple, Any, Optional
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# ENV (your keys from Render: SUPABASE_URL + SUPABASE_KEY)
# ──────────────────────────────────────────────────────────────────────────────
SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").rstrip("/")

# Prefer service role if present; otherwise accept SUPABASE_KEY (your current name),
# or anon (only works if RLS policies allow inserts/deletes).
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_KEY")
    or os.getenv("SUPABASE_ANON_KEY")
)

TABLE = os.getenv("VALUE_PREDICTIONS_TABLE", "value_predictions")

# Behavior flags
ALWAYS_WRITE = os.getenv("OU25_ALWAYS_WRITE", "0") == "1"             # write even when non-value
ONLY_WRITE_VALUE = os.getenv("OU25_ONLY_WRITE_VALUE", "0") == "1"     # gate by po_value

# NEW: minimum edge (percent) required to write
MIN_EDGE_PCT_TO_WRITE = float(os.getenv("OU25_MIN_EDGE_PCT_TO_WRITE", "5"))

def _headers() -> Dict[str, str]:
    if not SUPABASE_URL:
        raise RuntimeError("Missing SUPABASE_URL (e.g. https://<project>.supabase.co)")
    if not SUPABASE_KEY:
        raise RuntimeError("Missing Supabase key (set SUPABASE_SERVICE_ROLE_KEY or SUPABASE_KEY or SUPABASE_ANON_KEY)")
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }

def _explain_postgrest_error(text: str) -> str:
    """Turn PostgREST error JSON into a friendly reason string."""
    try:
        data = json.loads(text)
    except Exception:
        data = {}
    msg = (data.get("message") or "").lower()
    hint = (data.get("hint") or "").lower()

    if "no unique or exclusion constraint" in msg:
        return "NO_UNIQUE_FOR_ON_CONFLICT"
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

def _manual_upsert(row: Dict[str, Any]) -> Tuple[int, str, int, str]:
    """
    Fallback when ON CONFLICT can't be used (42P10).
    DELETE existing (fixture_id, market), then INSERT without on_conflict.
    Requires permission to DELETE/INSERT (service role recommended).
    """
    headers = _headers()

    # 1) DELETE existing rows for this (fixture_id, market)
    del_url = (
        f"{SUPABASE_URL}/rest/v1/{TABLE}"
        f"?fixture_id=eq.{row['fixture_id']}&market=eq.{row['market']}"
    )
    del_headers = dict(headers)
    del_headers["Prefer"] = "return=minimal"
    del_resp = requests.delete(del_url, headers=del_headers)
    if del_resp.status_code >= 400:
        reason = _explain_postgrest_error(del_resp.text)
        return 0, f"DELETE_FAILED_{reason}", del_resp.status_code, del_resp.text

    # 2) INSERT fresh row (no on_conflict)
    ins_url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    ins_resp = requests.post(ins_url, headers=headers, data=json.dumps(row))
    if ins_resp.status_code >= 400:
        reason = _explain_postgrest_error(ins_resp.text)
        return 0, reason, ins_resp.status_code, ins_resp.text

    try:
        payload = ins_resp.json()
        written = len(payload) if isinstance(payload, list) else (1 if payload else 0)
    except Exception:
        written = 1  # 201 with no body still means success
    return written, "MANUAL_UPSERT_OK", ins_resp.status_code, ins_resp.text

def _post_row(row: Dict[str, Any]) -> Tuple[int, str, int, str]:
    """
    Try native upsert first (on_conflict=fixture_id,market).
    On 42P10 (no unique constraint), fallback to manual upsert.
    Returns: (written_count, reason, status_code, resp_text)
    """
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?on_conflict=fixture_id,market"
    headers = _headers()
    resp = requests.post(url, headers=headers, data=json.dumps(row))

    if resp.status_code >= 400:
        reason = _explain_postgrest_error(resp.text)
        if reason in ("NO_UNIQUE_FOR_ON_CONFLICT", "UPSERT_NEEDS_UNIQUE_CONSTRAINT"):
            return _manual_upsert(row)
        return 0, reason, resp.status_code, resp.text

    try:
        payload = resp.json()
        written = len(payload) if isinstance(payload, list) else (1 if payload else 0)
    except Exception:
        written = 1
    return written, "OK", resp.status_code, resp.text

def _edge_pct_from_block(block: Dict[str, Any]) -> float:
    """
    Normalize edge to % for gating:
    - If |edge| ≤ 1 → treat as decimal and multiply by 100 (e.g., 0.07 → 7%).
    - Else → assume it's already percent (e.g., 7.0 → 7%).
    """
    try:
        raw = float(block.get("edge") or 0)
    except Exception:
        return 0.0
    return (raw * 100.0) if (-1.0 <= raw <= 1.0) else raw

def build_row_from_prediction(fixture_id: int, market: str, block: Dict[str, Any]) -> Dict[str, Any]:
    """Map model/local fields to DB columns (edge stays as-is from the block)."""
    return {
        "fixture_id": int(fixture_id or 0),
        "market": market,                                          # "over_2_5"
        "prediction": block.get("prediction"),                     # "Over"/"Under"
        "edge": float(block.get("edge") or 0),                     # may be decimal or percent (unchanged)
        "po_value": bool(block.get("po_value") or False),
        "odds": float(block.get("odds") or 0),
        "stake_pct": float(block.get("bankroll_pct") or 0),        # bankroll_pct -> stake_pct
        "confidence_pct": int(block.get("confidence") or 0),       # confidence -> confidence_pct
        "rationale": str(block.get("rationale") or ""),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

def write_value_prediction(fixture_id: int, market: str, block: Optional[Dict[str, Any]]) -> Tuple[int, str]:
    """
    High-level writer with explicit reasons for '0' writes.
    Enforces: only write when edge >= MIN_EDGE_PCT_TO_WRITE.
    Returns (written_count, reason).
    """
    if not isinstance(block, dict):
        logger.warning("✋ MISSING_MARKET block for fixture %s", fixture_id)
        return 0, "MISSING_MARKET"

    # Must have usable odds
    try:
        odds_f = float(block.get("odds"))
    except Exception:
        odds_f = None
    if odds_f is None or odds_f <= 1.0:
        logger.info("ℹ️ NO_ODDS for fixture %s (market=%s)", fixture_id, market)
        return 0, "NO_ODDS"

    # Enforce minimum edge % to write
    edge_pct = _edge_pct_from_block(block)
    if edge_pct < MIN_EDGE_PCT_TO_WRITE:
        logger.info("ℹ️ EDGE_BELOW_MIN for fixture %s: %.3f%% < %.3f%%", fixture_id, edge_pct, MIN_EDGE_PCT_TO_WRITE)
        return 0, "EDGE_BELOW_MIN"

    # Optional additional gate: only write value bets (po_value==True)
    po_value = bool(block.get("po_value"))
    if ONLY_WRITE_VALUE and not po_value and not ALWAYS_WRITE:
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
