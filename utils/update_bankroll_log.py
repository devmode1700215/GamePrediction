# utils/update_bankroll_log.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Optional, Set, List, Dict, Any, Tuple

from utils.supabaseClient import supabase

# ============================================================================
# Settings (ENV)
# ============================================================================
START_DATE          = os.getenv("BANKROLL_START_DATE", "2025-06-22")  # inclusive (YYYY-MM-DD)
EXCLUDE_DATES: Set[str] = set(filter(None, os.getenv("BANKROLL_EXCLUDE_DATES", "").split(",")))

# Include filters (kept flexible; you can loosen them)
CONF_MIN            = float(os.getenv("BANKROLL_CONF_MIN", "0"))    # confidence lower bound (pct)
ONLY_MARKETS        = [m.strip() for m in os.getenv("BANKROLL_ONLY_MARKETS", "").split(",") if m.strip()]
ONLY_PO_VALUE       = os.getenv("BANKROLL_ONLY_PO_VALUE", "false").lower() in ("1","true","yes","y")

# Compounding parameters
DEFAULT_BANKROLL    = float(os.getenv("BANKROLL_START", "1000"))
UNITS_PER_BET       = float(os.getenv("BANKROLL_UNITS_PER_BET", "5"))  # "5 stake" = 5 units
UNIT_PCT            = float(os.getenv("BANKROLL_UNIT_PCT", "1.0"))     # 1 unit = 1% of bankroll

# Batching
BATCH_SIZE          = int(os.getenv("BANKROLL_BATCH_SIZE", "1000"))

# ============================================================================
# Helpers
# ============================================================================
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def _date_str(ts_iso: str) -> Optional[str]:
    try:
        return ts_iso.split("T", 1)[0]
    except Exception:
        return None

def _load_state(label: str) -> Optional[float]:
    try:
        r = supabase.table("bankroll_state").select("bankroll").eq("label", label).limit(1).execute()
        rows = getattr(r, "data", None) or []
        if rows:
            return _to_float(rows[0].get("bankroll"))
    except Exception:
        pass
    return None

def _save_state(label: str, bankroll: float):
    try:
        supabase.table("bankroll_state").upsert(
            {"label": label, "bankroll": float(bankroll), "updated_at": _now_iso()},
            on_conflict="label",
        ).execute()
    except Exception:
        pass

def _already_logged_ids(table: str) -> Set[str]:
    try:
        r = supabase.table(table).select("prediction_id").execute()
        rows = getattr(r, "data", None) or []
        return {row["prediction_id"] for row in rows if row.get("prediction_id")}
    except Exception:
        return set()

# ============================================================================
# Data loaders
# ============================================================================
def _load_verifications_since(start_date: str) -> List[Dict[str, Any]]:
    try:
        r = (
            supabase.table("verifications")
            .select("prediction_id, verified_at, is_correct")
            .gte("verified_at", f"{start_date}T00:00:00Z")
            .order("verified_at", desc=False)
            .limit(100000)
            .execute()
        )
        return getattr(r, "data", None) or []
    except Exception:
        return []

def _load_predictions_by_ids(pred_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(pred_ids), BATCH_SIZE):
        chunk = pred_ids[i:i+BATCH_SIZE]
        try:
            q = supabase.table("value_predictions").select(
                "id, fixture_id, market, odds, confidence_pct, po_value"
            ).in_("id", chunk)
            if CONF_MIN > 0:
                q = q.gte("confidence_pct", CONF_MIN)
            if ONLY_MARKETS:
                q = q.in_("market", ONLY_MARKETS)
            if ONLY_PO_VALUE:
                q = q.eq("po_value", True)

            res = q.execute()
            rows = getattr(res, "data", None) or []
            for r in rows:
                out[r["id"]] = r
        except Exception:
            continue
    return out

def _load_top10_map_by_prediction_ids(pred_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Returns {prediction_id: {id: top10_row_id}} for quick membership checks.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(pred_ids), BATCH_SIZE):
        chunk = pred_ids[i:i+BATCH_SIZE]
        try:
            r = (
                supabase.table("top10_predictions")
                .select("id, prediction_id")
                .in_("prediction_id", chunk)
                .execute()
            )
            rows = getattr(r, "data", None) or []
            for x in rows:
                pid = x.get("prediction_id")
                if pid:
                    out[pid] = {"top10_id": x.get("id")}
        except Exception:
            continue
    return out

# ============================================================================
# Core compounding logic
# ============================================================================
def _compute_stake_amount(current_bankroll: float, units: float, unit_pct: float) -> float:
    # stake = bankroll * (units * unit_pct / 100)
    frac = (units * unit_pct) / 100.0
    amt = max(0.0, current_bankroll * frac)
    return round(amt, 2)

def _profit(odds: float, stake_amount: float, is_correct: bool) -> float:
    if odds is None or stake_amount is None:
        return 0.0
    return round(stake_amount * (odds - 1.0), 2) if is_correct else round(-stake_amount, 2)

def _make_logrow_common(
    pid: str,
    pred: Dict[str, Any],
    when_iso: str,
    odds: float,
    starting: float,
    after: float,
    stake_units: float,
    stake_amount: float,
    is_correct: bool,
) -> Dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "prediction_id": pid,
        "fixture_id": pred.get("fixture_id"),
        "market": pred.get("market"),
        "date": _date_str(when_iso),
        "stake_units": stake_units,
        "stake_amount": stake_amount,
        "odds": round(odds, 2) if odds is not None else None,
        "result": "win" if is_correct else "lose",
        "profit": _profit(odds, stake_amount, is_correct),
        "starting_bankroll": round(starting, 2),
        "bankroll_after": round(after, 2),
        "created_at": _now_iso(),
    }

# ============================================================================
# Public API
# ============================================================================
def update_bankroll_full(label: str = "bankroll_full") -> None:
    """
    Compounding bankroll on ALL verified predictions (respecting optional filters).
    Writes to public.bankroll_log, state label 'bankroll_full'.
    """
    print("=== bankroll FULL updater starting ===")
    print(f"UNITS_PER_BET={UNITS_PER_BET} | UNIT_PCT={UNIT_PCT} | START_DATE={START_DATE}")
    bankroll = _load_state(label)
    if bankroll is None:
        bankroll = DEFAULT_BANKROLL
    bankroll = float(bankroll)
    print(f"Starting bankroll: {bankroll:.2f}")

    logged = _already_logged_ids("bankroll_log")
    verifs_raw = _load_verifications_since(START_DATE)
    print(f"Verifications fetched: {len(verifs_raw)}")

    verifs = []
    for v in verifs_raw:
        pid = v.get("prediction_id")
        ts  = v.get("verified_at")
        if not pid or not ts:
            continue
        d = _date_str(ts)
        if d in EXCLUDE_DATES:
            continue
        if pid in logged:
            continue
        verifs.append(v)

    if not verifs:
        print("No new verifications to process.")
        return

    pred_ids = list({v["prediction_id"] for v in verifs})
    preds = _load_predictions_by_ids(pred_ids)
    if not preds:
        print("No predictions matched filters.")
        return

    # chronological compounding
    verifs.sort(key=lambda v: v["verified_at"])
    rows = []
    current = round(bankroll, 2)
    for v in verifs:
        pid = v["prediction_id"]
        pred = preds.get(pid)
        if not pred:
            continue
        odds = _to_float(pred.get("odds"))
        if odds is None:
            continue
        is_correct = bool(v.get("is_correct"))
        stake_amt = _compute_stake_amount(current, UNITS_PER_BET, UNIT_PCT)
        after = round(current + _profit(odds, stake_amt, is_correct), 2)
        row = _make_logrow_common(
            pid=pid,
            pred=pred,
            when_iso=v["verified_at"],
            odds=odds,
            starting=current,
            after=after,
            stake_units=UNITS_PER_BET,
            stake_amount=stake_amt,
            is_correct=is_correct,
        )
        rows.append(row)
        current = after

    if not rows:
        print("All new verifications were filtered out.")
        return

    try:
        supabase.table("bankroll_log").upsert(rows, on_conflict="prediction_id").execute()
        print(f"Inserted/updated rows: {len(rows)} | Final bankroll: {current:.2f}")
        _save_state(label, current)
    except Exception as e:
        print(f"bankroll_log upsert failed: {e}")


def update_bankroll_top10(label: str = "bankroll_top10") -> None:
    """
    Compounding bankroll on verified predictions that were part of Top-10 snapshots.
    Writes to public.bankroll_log_top10, state label 'bankroll_top10'.
    """
    print("=== bankroll TOP10 updater starting ===")
    print(f"UNITS_PER_BET={UNITS_PER_BET} | UNIT_PCT={UNIT_PCT} | START_DATE={START_DATE}")
    bankroll = _load_state(label)
    if bankroll is None:
        bankroll = DEFAULT_BANKROLL
    bankroll = float(bankroll)
    print(f"Starting bankroll (top10): {bankroll:.2f}")

    logged = _already_logged_ids("bankroll_log_top10")
    verifs_raw = _load_verifications_since(START_DATE)
    print(f"Verifications fetched: {len(verifs_raw)}")

    verifs = []
    for v in verifs_raw:
        pid = v.get("prediction_id")
        ts  = v.get("verified_at")
        if not pid or not ts:
            continue
        d = _date_str(ts)
        if d in EXCLUDE_DATES:
            continue
        if pid in logged:
            continue
        verifs.append(v)

    if not verifs:
        print("No new verifications to process (top10).")
        return

    pred_ids = list({v["prediction_id"] for v in verifs})
    preds = _load_predictions_by_ids(pred_ids)
    if not preds:
        print("No predictions matched filters (top10).")
        return

    # Membership in top10
    top10_map = _load_top10_map_by_prediction_ids(pred_ids)

    # chronological compounding
    verifs.sort(key=lambda v: v["verified_at"])
    rows = []
    current = round(bankroll, 2)
    for v in verifs:
        pid = v["prediction_id"]
        if pid not in top10_map:
            continue  # skip if not in Top10
        pred = preds.get(pid)
        if not pred:
            continue
        odds = _to_float(pred.get("odds"))
        if odds is None:
            continue
        is_correct = bool(v.get("is_correct"))
        stake_amt = _compute_stake_amount(current, UNITS_PER_BET, UNIT_PCT)
        after = round(current + _profit(odds, stake_amt, is_correct), 2)
        row = _make_logrow_common(
            pid=pid,
            pred=pred,
            when_iso=v["verified_at"],
            odds=odds,
            starting=current,
            after=after,
            stake_units=UNITS_PER_BET,
            stake_amount=stake_amt,
            is_correct=is_correct,
        )
        # add top10_id + source override
        row["top10_id"] = top10_map[pid].get("top10_id")
        row["source"]   = "top10"
        rows.append(row)
        current = after

    if not rows:
        print("All new verifications were filtered out (top10).")
        return

    try:
        supabase.table("bankroll_log_top10").upsert(rows, on_conflict="prediction_id").execute()
        print(f"Inserted/updated TOP10 rows: {len(rows)} | Final bankroll (top10): {current:.2f}")
        _save_state(label, current)
    except Exception as e:
        print(f"bankroll_log_top10 upsert failed: {e}")
