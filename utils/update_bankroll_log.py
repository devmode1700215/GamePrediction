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
DEFAULT_BANKROLL    = float(os.getenv("BANKROLL_START", "1000"))      # starting bankroll for REBUILD
BANKROLL_UNIT_PCT   = float(os.getenv("BANKROLL_UNIT_PCT", "1.0"))    # e.g. 1.0 = 1% of bankroll per bet
MIN_STAKE_AMOUNT    = float(os.getenv("BANKROLL_MIN_STAKE", "0.10"))  # avoid 0 after rounding
REBUILD             = os.getenv("BANKROLL_REBUILD", "false").lower() in ("1","true","yes","y")

# Optional filters (keep simple; adjust later if needed)
PO_VALUE_ONLY       = os.getenv("BANKROLL_PO_VALUE_ONLY", "true").lower() in ("1","true","yes","y")
ONLY_MARKETS        = [m.strip() for m in os.getenv("BANKROLL_ONLY_MARKETS", "over_2_5").split(",") if m.strip()]
ODDS_MIN            = float(os.getenv("BANKROLL_ODDS_MIN", "1.01"))
ODDS_MAX            = float(os.getenv("BANKROLL_ODDS_MAX", "1000"))

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

def _safe_round_money(x: float) -> float:
    return round(x + 1e-9, 2)

def _unit_fraction() -> float:
    # 1.0 => 1% ; 0.5 => 0.5% ; 2.0 => 2%
    pct = BANKROLL_UNIT_PCT
    return pct / 100.0 if pct > 1 else pct

# =============================================================================
# Supabase I/O
# =============================================================================
def _delete_rows_for_label(label: str) -> int:
    try:
        res = supabase.table("bankroll_log").delete().eq("label", label).execute()
        data = getattr(res, "data", None) or []
        return len(data)
    except Exception:
        return 0

def _read_last_bankroll_state(label: str) -> Optional[float]:
    # Try bankroll_state first
    try:
        r = supabase.table("bankroll_state").select("bankroll").eq("label", label).limit(1).execute()
        rows = getattr(r, "data", None) or []
        if rows:
            return _to_float(rows[0].get("bankroll"))
    except Exception:
        pass
    # Fallback to last bankroll_log
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
    Load ALL verifications in ascending verified_at order.
    """
    out: List[Dict[str, Any]] = []
    page = None
    while True:
        try:
            q = supabase.table("verifications").select(
                "prediction_id, fixture_id, verified_at, is_correct"
            ).order("verified_at", desc=False).limit(BATCH_SIZE)
            if page is not None:
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
    Load value_predictions for given ids (to get odds/market/prediction).
    Apply minimal filters; stake% from predictions is ignored.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(pred_ids), BATCH_SIZE):
        chunk = pred_ids[i:i+BATCH_SIZE]
        try:
            q = supabase.table("value_predictions").select(
                "id, fixture_id, market, prediction, odds, po_value"
            ).in_("id", chunk)
            if ONLY_MARKETS:
                q = q.in_("market", ONLY_MARKETS)
            q = q.gte("odds", ODDS_MIN).lte("odds", ODDS_MAX)
            if PO_VALUE_ONLY:
                q = q.eq("po_value", True)
            res = q.execute()
            rows = getattr(res, "data", None) or []
            for r in rows:
                out[r["id"]] = r
        except Exception:
            continue
    return out

def _upsert_logs(rows: List[Dict[str, Any]]) -> bool:
    if not rows: 
        return True
    try:
        supabase.table("bankroll_log").upsert(rows, on_conflict="prediction_id").execute()
        return True
    except Exception as e:
        print(f"upsert bankroll_log failed: {e}")
        return False

# =============================================================================
# Core compounding
# =============================================================================
def _compute_row(
    bankroll_before: float,
    v: Dict[str, Any],
    p: Dict[str, Any],
    label: str
) -> Tuple[float, Dict[str, Any]]:
    """
    Constant % stake of CURRENT bankroll; bankroll updates after result → compounding.
    """
    verified_at = v.get("verified_at") or _now_iso()
    date_str    = (verified_at or "").split("T")[0]

    odds        = _to_float(p.get("odds")) or 0.0
    is_correct  = bool(v.get("is_correct"))

    frac = _unit_fraction()                 # constant fraction for all bets
    stake_amount = _safe_round_money(max(MIN_STAKE_AMOUNT, bankroll_before * frac))
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
        "stake_pct": float(BANKROLL_UNIT_PCT),  # store human-readable percent (e.g., 1.0)
        "stake_amount": stake_amount,
        "result": "win" if is_correct else "lose",
        "profit": profit,
        "bankroll_before": bankroll_before,
        "bankroll_after": after,
        "created_at": _now_iso(),
    }
    return after, row

def rebuild_or_append():
    print("=== bankroll_log updater (constant % compounding) ===")
    print(f"LABEL={LABEL}  REBUILD={REBUILD}  START={DEFAULT_BANKROLL:.2f}  UNIT={BANKROLL_UNIT_PCT}%  MIN_STAKE={MIN_STAKE_AMOUNT:.2f}")
    print(f"Filters: PO_VALUE_ONLY={PO_VALUE_ONLY}  ODDS[{ODDS_MIN},{ODDS_MAX}]  MARKETS={ONLY_MARKETS or 'ALL'}")

    # Load verifications & predictions
    verifs = _load_all_verifications_sorted()
    if not verifs:
        print("No verifications found. Nothing to do.")
        return

    pid_list = [v["prediction_id"] for v in verifs if v.get("prediction_id")]
    preds = _load_predictions_for_ids(pid_list)
    if not preds:
        print("No matching predictions after filters. Nothing to do.")
        return

    # Determine starting bankroll
    if REBUILD:
        deleted = _delete_rows_for_label(LABEL)
        print(f"[REBUILD] Deleted {deleted} previous rows for label='{LABEL}'.")
        bankroll = DEFAULT_BANKROLL
    else:
        bankroll = _read_last_bankroll_state(LABEL)
        if bankroll is None:
            bankroll = DEFAULT_BANKROLL
        print(f"Starting bankroll: {bankroll:.2f}")

    rows_to_write: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    for v in verifs:
        pid = v.get("prediction_id")
        if not pid or pid in seen:
            continue
        p = preds.get(pid)
        if not p:
            continue

        bankroll, row = _compute_row(bankroll, v, p, LABEL)
        rows_to_write.append(row)
        seen.add(pid)

        if len(rows_to_write) >= 1000:
            if not _upsert_logs(rows_to_write):
                print("Batch upsert failed; stopping.")
                break
            rows_to_write = []

    if rows_to_write:
        if not _upsert_logs(rows_to_write):
            print("Final upsert failed.")
            return

    _write_state_bankroll(LABEL, bankroll)
    print(f"✅ Done. Final bankroll for '{LABEL}': {bankroll:.2f}")

# =============================================================================
# CLI
# =============================================================================
if __name__ == "__main__":
    rebuild_or_append()
