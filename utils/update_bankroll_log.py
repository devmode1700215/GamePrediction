# utils/update_bankroll_log.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Optional, Set, List, Dict, Any, Tuple

from utils.supabaseClient import supabase

# =============================================================================
# Settings (override via env)
# =============================================================================
LABEL               = os.getenv("BANKROLL_LABEL", "default")
DEFAULT_BANKROLL    = float(os.getenv("BANKROLL_START", "1000"))
DEFAULT_UNIT_PCT    = float(os.getenv("BANKROLL_DEFAULT_UNIT_PCT", "1.0"))   # % of bankroll when stake_pct is missing/invalid
MIN_STAKE_AMOUNT    = float(os.getenv("BANKROLL_MIN_STAKE", "0.10"))         # avoid 0 after rounding
REBUILD             = os.getenv("BANKROLL_REBUILD", "false").lower() in ("1","true","yes","y")
PO_VALUE_ONLY       = os.getenv("BANKROLL_PO_VALUE_ONLY", "true").lower() in ("1","true","yes","y")

# Optional filters
ODDS_MIN            = float(os.getenv("BANKROLL_ODDS_MIN", "1.01"))          # let you filter later if you like
ODDS_MAX            = float(os.getenv("BANKROLL_ODDS_MAX", "1000"))
CONF_MIN            = float(os.getenv("BANKROLL_CONF_MIN", "0"))             # value_predictions.confidence_pct (0-100)
ONLY_MARKETS        = [m.strip() for m in os.getenv("BANKROLL_ONLY_MARKETS", "over_2_5").split(",") if m.strip()]

# Paging
BATCH_SIZE          = int(os.getenv("BANKROLL_BATCH_SIZE", "1000"))

# =============================================================================
# Helpers
# =============================================================================
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def _as_fraction(pct_or_frac: Any) -> float:
    """
    Accepts:
      - 2   -> 0.02 (2%)
      - 0.02-> 0.02 (2%)
      - "2" or "0.02" -> parsed accordingly
    Returns 0.0 on invalid.
    """
    v = _to_float(pct_or_frac)
    if v is None:
        return 0.0
    return v / 100.0 if v > 1 else v

def _safe_round_money(x: float) -> float:
    return round(x + 1e-9, 2)

# =============================================================================
# IO to Supabase
# =============================================================================
def _delete_rows_for_label(label: str) -> int:
    try:
        res = supabase.table("bankroll_log").delete().eq("label", label).execute()
        data = getattr(res, "data", None) or []
        return len(data)
    except Exception:
        # Some clients return count via headers; ignore exact count on delete
        return 0

def _read_last_bankroll_state(label: str) -> Optional[float]:
    # Try bankroll_state
    try:
        r = supabase.table("bankroll_state").select("bankroll").eq("label", label).limit(1).execute()
        rows = getattr(r, "data", None) or []
        if rows:
            return _to_float(rows[0].get("bankroll"))
    except Exception:
        pass
    # Fallback to last bankroll_log row for label
    try:
        r = supabase.table("bankroll_log").select("bankroll_after").eq("label", label)\
            .order("created_at", desc=True).limit(1).execute()
        rows = getattr(r, "data", None) or []
        if rows:
            return _to_float(rows[0].get("bankroll_after"))
    except Exception:
        pass
    return None

def _write_state_bankroll(label: str, bankroll: float):
    try:
        supabase.table("bankroll_state").upsert(
            {"label": label, "bankroll": float(bankroll), "updated_at": _now_iso()},
            on_conflict="label",
        ).execute()
    except Exception:
        pass

def _load_all_verifications_sorted() -> List[Dict[str, Any]]:
    """
    Get ALL verifications in ascending time order.
    We’ll join to value_predictions and filter there.
    """
    out: List[Dict[str, Any]] = []
    page = None
    while True:
        try:
            q = supabase.table("verifications").select(
                "prediction_id, fixture_id, verified_at, is_correct"
            ).order("verified_at", desc=False).limit(BATCH_SIZE)
            if page:
                q = q.range(page, page + BATCH_SIZE - 1)
            res = q.execute()
            rows = getattr(res, "data", None) or []
            if not rows:
                break
            out.extend(rows)
            if len(rows) < BATCH_SIZE:
                break
            page = (page or 0) + BATCH_SIZE
        except Exception:
            break
    return out

def _load_predictions_for_ids(pred_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Load value_predictions rows for given prediction ids and apply filters.
    Returns dict[id] = row
    """
    out: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(pred_ids), BATCH_SIZE):
        chunk = pred_ids[i:i+BATCH_SIZE]
        try:
            q = supabase.table("value_predictions").select(
                "id, fixture_id, market, prediction, odds, confidence_pct, stake_pct"
            ).in_("id", chunk)

            # Filters
            if ONLY_MARKETS:
                q = q.in_("market", ONLY_MARKETS)
            if PO_VALUE_ONLY:
                q = q.eq("po_value", True)
            q = q.gte("odds", ODDS_MIN).lte("odds", ODDS_MAX)
            if CONF_MIN > 0:
                q = q.gte("confidence_pct", CONF_MIN)

            res = q.execute()
            rows = getattr(res, "data", None) or []
            for r in rows:
                out[r["id"]] = r
        except Exception:
            continue
    return out

def _upsert_logs(rows: List[Dict[str, Any]]) -> bool:
    """
    Upsert by prediction_id to keep idempotency.
    """
    if not rows:
        return True
    try:
        supabase.table("bankroll_log").upsert(rows, on_conflict="prediction_id").execute()
        return True
    except Exception as e:
        print(f"upsert bankroll_log failed: {e}")
        return False

# =============================================================================
# Core
# =============================================================================
def _compute_row(
    bankroll_before: float,
    v: Dict[str, Any],
    p: Dict[str, Any],
    label: str
) -> Tuple[float, Dict[str, Any]]:
    """
    Returns (new_bankroll_after, row_dict)
    """
    verified_at = v.get("verified_at") or _now_iso()
    date_str    = (verified_at or "").split("T")[0]

    odds        = _to_float(p.get("odds")) or 0.0
    is_correct  = bool(v.get("is_correct"))
    stake_pct   = p.get("stake_pct")

    frac = _as_fraction(stake_pct)
    if frac <= 0:
        frac = _as_fraction(DEFAULT_UNIT_PCT)

    stake_amount = _safe_round_money(max(MIN_STAKE_AMOUNT, bankroll_before * frac)) if frac > 0 else 0.0
    profit = _safe_round_money(stake_amount * (odds - 1.0)) if is_correct else _safe_round_money(-stake_amount)
    after  = _safe_round_money(bankroll_before + profit)

    row = {
        "id": str(uuid.uuid4()),
        "label": label,
        "prediction_id": p["id"],
        "fixture_id": p.get("fixture_id"),
        "verified_at": verified_at,
        "date": date_str,
        "market": p.get("market"),
        "prediction": p.get("prediction"),
        "odds": odds,
        "stake_pct": float(frac * 100.0),  # store as percentage number for readability (e.g., 1.5)
        "stake_amount": stake_amount,
        "result": "win" if is_correct else "lose",
        "profit": profit,
        "bankroll_before": bankroll_before,
        "bankroll_after": after,
        "created_at": _now_iso(),
    }
    return after, row

def rebuild_or_append():
    print("=== bankroll_log updater starting ===")
    print(f"LABEL={LABEL}  REBUILD={REBUILD}  DEFAULT_BANKROLL={DEFAULT_BANKROLL:.2f}  DEFAULT_UNIT_PCT={DEFAULT_UNIT_PCT}%  MIN_STAKE={MIN_STAKE_AMOUNT:.2f}")
    print(f"Filters: PO_VALUE_ONLY={PO_VALUE_ONLY}  CONF>={CONF_MIN}  ODDS[{ODDS_MIN},{ODDS_MAX}]  MARKETS={ONLY_MARKETS or 'ALL'}")

    # Load all verifications in chronological order
    verifs = _load_all_verifications_sorted()
    if not verifs:
        print("No verifications found. Nothing to do.")
        return

    # Load predictions for these verifications (apply filters)
    pid_list = [v["prediction_id"] for v in verifs if v.get("prediction_id")]
    preds = _load_predictions_for_ids(pid_list)
    if not preds:
        print("No matching predictions after filters. Nothing to do.")
        return

    # If REBUILD: delete this label’s rows and start from DEFAULT_BANKROLL
    bankroll = None
    if REBUILD:
        deleted = _delete_rows_for_label(LABEL)
        print(f"[REBUILD] Deleted {deleted} previous rows for label='{LABEL}'.")
        bankroll = DEFAULT_BANKROLL
    else:
        bankroll = _read_last_bankroll_state(LABEL)
        if bankroll is None:
            bankroll = DEFAULT_BANKROLL
        print(f"Starting bankroll: {bankroll:.2f}")

    # Compose rows
    rows_to_write: List[Dict[str, Any]] = []
    used_ids: Set[str] = set()

    for v in verifs:
        pid = v.get("prediction_id")
        if not pid or pid in used_ids:
            continue
        p = preds.get(pid)
        if not p:
            continue

        bankroll, row = _compute_row(bankroll, v, p, LABEL)
        rows_to_write.append(row)
        used_ids.add(pid)

        # Write in chunks to avoid large payloads
        if len(rows_to_write) >= 1000:
            ok = _upsert_logs(rows_to_write)
            if not ok:
                print("Batch upsert failed; stopping early.")
                break
            rows_to_write = []

    # Final flush
    if rows_to_write:
        ok = _upsert_logs(rows_to_write)
        if not ok:
            print("Final upsert failed.")
            return

    # Persist current bankroll
    _write_state_bankroll(LABEL, bankroll)

    print(f"✅ Done. Final bankroll for '{LABEL}': {bankroll:.2f}")

# =============================================================================
# CLI
# =============================================================================
if __name__ == "__main__":
    rebuild_or_append()
