# utils/update_bankroll_log.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Optional, Set, List, Dict, Any, Tuple

from utils.supabaseClient import supabase

# =============================================================================
# Settings (ENV)
# =============================================================================
START_DATE = os.getenv("BANKROLL_START_DATE", "2025-06-22")  # inclusive (YYYY-MM-DD)
EXCLUDE_DATES: Set[str] = set(filter(None, os.getenv("BANKROLL_EXCLUDE_DATES", "").split(",")))

# Master switch to ignore ALL filters (use this first to get data flowing)
DISABLE_FILTERS = os.getenv("BANKROLL_DISABLE_FILTERS", "true").lower() in ("1", "true", "yes", "y")

# Optional filters (used only if DISABLE_FILTERS=false)
CONF_MIN       = float(os.getenv("BANKROLL_CONF_MIN", "0"))
ONLY_MARKETS   = [m.strip() for m in os.getenv("BANKROLL_ONLY_MARKETS", "").split(",") if m.strip()]
ONLY_PO_VALUE  = os.getenv("BANKROLL_ONLY_PO_VALUE", "false").lower() in ("1","true","yes","y")

# Compounding parameters (stake is computed from bankroll — stake_amount, NOT stake_pct)
DEFAULT_BANKROLL  = float(os.getenv("BANKROLL_START", "1000"))
UNITS_PER_BET     = float(os.getenv("BANKROLL_UNITS_PER_BET", "5"))   # e.g. 5 units
UNIT_PCT          = float(os.getenv("BANKROLL_UNIT_PCT", "1.0"))      # 1 unit = 1% of bankroll

# Batching
BATCH_SIZE = int(os.getenv("BANKROLL_BATCH_SIZE", "1000"))

# Source names
VIEW_WITH_RESULTS = os.getenv("BANKROLL_RESULTS_VIEW", "value_predictions_with_results")

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
            v = _to_float(rows[0].get("bankroll"))
            if v is not None:
                return v
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

def _distinct(seq):
    return list(dict.fromkeys(seq))

# =============================================================================
# Loaders (path A: verifications + join by ID)
# =============================================================================
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
            res = (supabase.table("value_predictions")
                   .select("id, fixture_id, market, odds, confidence_pct, po_value")
                   .in_("id", chunk)
                   .execute())
            rows = getattr(res, "data", None) or []
            for r in rows:
                out[r["id"]] = r
        except Exception:
            continue
    return out

# =============================================================================
# Loader (path B: direct from view with results)
# =============================================================================
def _load_verified_rows_from_view(start_date: str) -> List[Dict[str, Any]]:
    """
    Pull already-joined rows from value_predictions_with_results (or override view).
    Expected columns (flexible; we map safely):
      - id (prediction_id), fixture_id, market, odds, confidence_pct, po_value
      - verified_at, is_correct
    """
    rows_all: List[Dict[str, Any]] = []
    from_dt = f"{start_date}T00:00:00Z"
    try:
        # Pull in chunks ordered by verified_at ASC for stable compounding
        # Some instances don’t support range pagination; we over-fetch once (limit 50000).
        r = (
            supabase.table(VIEW_WITH_RESULTS)
            .select("id, prediction_id, fixture_id, market, odds, confidence_pct, po_value, verified_at, is_correct")
            .gte("verified_at", from_dt)
            .order("verified_at", desc=False)
            .limit(50000)
            .execute()
        )
        rows_all = getattr(r, "data", None) or []
    except Exception as e:
        print(f"[bankroll] fallback view load failed: {e}")
        rows_all = []
    return rows_all

# =============================================================================
# Core compounding
# =============================================================================
def _compute_stake_amount(current_bankroll: float, units: float, unit_pct: float) -> float:
    # stake_amount = bankroll * (units * unit_pct / 100)
    frac = (units * unit_pct) / 100.0
    amt = max(0.0, current_bankroll * frac)
    return round(amt, 2)

def _profit(odds: float, stake_amount: float, is_correct: bool) -> float:
    if odds is None or stake_amount is None:
        return 0.0
    return round(stake_amount * (odds - 1.0), 2) if is_correct else round(-stake_amount, 2)

def _apply_filters(pred: Dict[str, Any]) -> Tuple[bool, str]:
    if DISABLE_FILTERS:
        return True, "filters_disabled"
    # market
    if ONLY_MARKETS and (pred.get("market") not in ONLY_MARKETS):
        return False, "market"
    # confidence
    conf = _to_float(pred.get("confidence_pct"))
    if (conf is None) or (conf < CONF_MIN):
        return False, "conf"
    # po_value
    if ONLY_PO_VALUE and (not bool(pred.get("po_value"))):
        return False, "po_value"
    # odds present
    if _to_float(pred.get("odds")) is None:
        return False, "no_odds"
    return True, "kept"

def _log_row(pred_or_view_row: Dict[str, Any], when_iso: str,
             starting: float, after: float,
             odds: float, stake_amt: float, units: float,
             is_correct: bool, source: str) -> Dict[str, Any]:
    pid = pred_or_view_row.get("id") or pred_or_view_row.get("prediction_id")
    return {
        "id": str(uuid.uuid4()),
        "prediction_id": pid,
        "fixture_id": pred_or_view_row.get("fixture_id"),
        "market": pred_or_view_row.get("market"),
        "date": _date_str(when_iso),
        "stake_units": units,
        "stake_amount": stake_amt,
        "odds": round(odds, 2) if odds is not None else None,
        "result": "win" if is_correct else "lose",
        "profit": _profit(odds, stake_amt, is_correct),
        "starting_bankroll": round(starting, 2),
        "bankroll_after": round(after, 2),
        "source": source,
        "created_at": _now_iso(),
    }

def _run_from_ids(mode_label: str, target_table: str, top10_only: bool) -> int:
    """Returns number of rows written. 0 means: try the view fallback."""
    print(f"=== bankroll {'TOP10' if top10_only else 'FULL'} updater starting ===")
    print(f"UNITS_PER_BET={UNITS_PER_BET} | UNIT_PCT={UNIT_PCT} | START_DATE={START_DATE}")
    print(f"Filters: DISABLE_FILTERS={DISABLE_FILTERS} | ONLY_MARKETS={ONLY_MARKETS or 'ALL'} | "
          f"CONF_MIN={CONF_MIN} | ONLY_PO_VALUE={ONLY_PO_VALUE}")

    bankroll = _load_state(mode_label)
    if bankroll is None:
        bankroll = DEFAULT_BANKROLL
    bankroll = float(bankroll)
    print(f"Starting bankroll: {bankroll:.2f}")

    logged = _already_logged_ids(target_table)

    verifs_raw = _load_verifications_since(START_DATE)
    print(f"Verifications fetched: {len(verifs_raw)}")

    verifs: List[Dict[str, Any]] = []
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

    unique_pids = _distinct([v["prediction_id"] for v in verifs])
    print(f"After de-dupe: verifs={len(verifs)} | unique prediction_ids={len(unique_pids)}")

    if not verifs or not unique_pids:
        print("No new verifications to process.")
        return 0

    preds_map = _load_predictions_by_ids(unique_pids)
    print(f"Predictions fetched by id: {len(preds_map)}")
    missing = set(unique_pids) - set(preds_map.keys())
    if missing:
        print(f"WARNING: {len(missing)} prediction_id(s) missing from value_predictions.")

    if len(preds_map) == 0:
        return 0  # signal to caller to try the view fallback

    # Optional: ensure top10 membership (only when top10 ledger)
    top10_set: Set[str] = set()
    if top10_only:
        try:
            r = (supabase.table("top10_predictions")
                 .select("prediction_id")
                 .in_("prediction_id", unique_pids)
                 .execute())
            rows = getattr(r, "data", None) or []
            top10_set = {x["prediction_id"] for x in rows if x.get("prediction_id")}
            print(f"Top10 membership found for {len(top10_set)} prediction_id(s).")
        except Exception:
            pass

    # Compound
    verifs.sort(key=lambda v: v["verified_at"])
    current = round(bankroll, 2)
    rows: List[Dict[str, Any]] = []
    kept = 0
    for v in verifs:
        pid = v["prediction_id"]
        pred = preds_map.get(pid)
        if not pred:
            continue
        if top10_only and (pid not in top10_set):
            continue

        ok, reason = _apply_filters(pred)
        if not ok:
            continue

        odds = _to_float(pred.get("odds"))
        if odds is None:
            continue
        is_correct = bool(v.get("is_correct"))

        stake_amt = _compute_stake_amount(current, UNITS_PER_BET, UNIT_PCT)
        after = round(current + _profit(odds, stake_amt, is_correct), 2)

        rows.append(_log_row(pred, v["verified_at"], current, after, odds, stake_amt, UNITS_PER_BET, is_correct,
                             source="all" if not top10_only else "top10"))
        current = after
        kept += 1

    print(f"Rows to upsert: {len(rows)}")
    if not rows:
        print("No predictions matched filters.")
        return 0

    supabase.table(target_table).upsert(rows, on_conflict="prediction_id").execute()
    print(f"Inserted/updated rows: {len(rows)} | Final bankroll: {current:.2f}")
    _save_state(mode_label, current)
    return len(rows)

def _run_from_view(mode_label: str, target_table: str, top10_only: bool) -> int:
    print(f"[bankroll] Falling back to {VIEW_WITH_RESULTS} (joined view)…")
    bankroll = _load_state(mode_label)
    if bankroll is None:
        bankroll = DEFAULT_BANKROLL
    current = round(float(bankroll), 2)

    logged = _already_logged_ids(target_table)
    rows_view = _load_verified_rows_from_view(START_DATE)
    print(f"Rows from view since {START_DATE}: {len(rows_view)}")

    if top10_only:
        # keep only rows that exist in top10_predictions
        try:
            r = supabase.table("top10_predictions").select("prediction_id").execute()
            tset = {x["prediction_id"] for x in (getattr(r, "data", None) or []) if x.get("prediction_id")}
        except Exception:
            tset = set()
        rows_view = [r for r in rows_view if r.get("prediction_id") in tset]
        print(f"Rows in view that are also Top10: {len(rows_view)}")

    # sort chronologically
    rows_view.sort(key=lambda r: r.get("verified_at") or "")

    out_rows: List[Dict[str, Any]] = []
    kept = 0
    for r in rows_view:
        pid = r.get("id") or r.get("prediction_id")
        if not pid or pid in logged:
            continue
        if _date_str(r.get("verified_at") or "") in EXCLUDE_DATES:
            continue

        ok, reason = _apply_filters(r)
        if not ok:
            continue

        odds = _to_float(r.get("odds"))
        if odds is None:
            continue
        is_correct = bool(r.get("is_correct"))

        stake_amt = _compute_stake_amount(current, UNITS_PER_BET, UNIT_PCT)
        after = round(current + _profit(odds, stake_amt, is_correct), 2)

        out_rows.append(_log_row(r, r.get("verified_at"), current, after, odds, stake_amt, UNITS_PER_BET, is_correct,
                                 source="all" if not top10_only else "top10"))
        current = after
        kept += 1

    print(f"[view] Rows to upsert: {len(out_rows)}")
    if not out_rows:
        print("[view] Nothing to write.")
        return 0

    supabase.table(target_table).upsert(out_rows, on_conflict="prediction_id").execute()
    print(f"[view] Inserted/updated rows: {len(out_rows)} | Final bankroll: {current:.2f}")
    _save_state(mode_label, current)
    return len(out_rows)

# Public API
def update_bankroll_full(label: str = "bankroll_full") -> None:
    wrote = _run_from_ids(mode_label=label, target_table="bankroll_log", top10_only=False)
    if wrote == 0:
        _run_from_view(mode_label=label, target_table="bankroll_log", top10_only=False)

def update_bankroll_top10(label: str = "bankroll_top10") -> None:
    wrote = _run_from_ids(mode_label=label, target_table="bankroll_log_top10", top10_only=True)
    if wrote == 0:
        _run_from_view(mode_label=label, target_table="bankroll_log_top10", top10_only=True)

# Auto-run when used as a module: python -m utils.update_bankroll_log
if __name__ == "__main__":
    print("✅ Supabase connection successful")
    update_bankroll_full()
    update_bankroll_top10()
