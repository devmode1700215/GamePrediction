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

def _distinct(seq):
    return list(dict.fromkeys(seq))

# =============================================================================
# Data loaders
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
    """
    Load predictions WITHOUT filters so we can log exactly what gets filtered later.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(pred_ids), BATCH_SIZE):
        chunk = pred_ids[i:i+BATCH_SIZE]
        try:
            res = (supabase.table("value_predictions")
                   .select("id, fixture_id, market, odds, confidence_pct, po_value, stake_amount")
                   .in_("id", chunk)
                   .execute())
            rows = getattr(res, "data", None) or []
            for r in rows:
                out[r["id"]] = r
        except Exception:
            continue
    return out

def _load_top10_map_by_prediction_ids(pred_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(pred_ids), BATCH_SIZE):
        chunk = pred_ids[i:i+BATCH_SIZE]
        try:
            r = (supabase.table("top10_predictions")
                 .select("id, prediction_id")
                 .in_("prediction_id", chunk)
                 .execute())
            rows = getattr(r, "data", None) or []
            for x in rows:
                pid = x.get("prediction_id")
                if pid:
                    out[pid] = {"top10_id": x.get("id")}
        except Exception:
            continue
    return out

# =============================================================================
# Core compounding logic
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
        "stake_amount": stake_amount,        # <— we persist stake_amount, not stake_pct
        "odds": round(odds, 2) if odds is not None else None,
        "result": "win" if is_correct else "lose",
        "profit": _profit(odds, stake_amount, is_correct),
        "starting_bankroll": round(starting, 2),
        "bankroll_after": round(after, 2),
        "created_at": _now_iso(),
    }

# =============================================================================
# Filtering (with logging)
# =============================================================================
def _apply_filters(preds: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, int]]:
    """
    Returns (kept_predictions_map, counters)
    """
    if DISABLE_FILTERS:
        return preds, {"disabled": 1}

    counters = {
        "market": 0,
        "conf": 0,
        "po_value": 0,
        "no_odds": 0,
        "kept": 0,
    }
    kept: Dict[str, Dict[str, Any]] = {}

    for pid, p in preds.items():
        # odds present?
        odds = _to_float(p.get("odds"))
        if odds is None:
            counters["no_odds"] += 1
            continue

        # market filter
        if ONLY_MARKETS and (p.get("market") not in ONLY_MARKETS):
            counters["market"] += 1
            continue

        # confidence filter
        conf = _to_float(p.get("confidence_pct"))
        if (conf is None) or (conf < CONF_MIN):
            counters["conf"] += 1
            continue

        # po_value filter
        if ONLY_PO_VALUE and (not bool(p.get("po_value"))):
            counters["po_value"] += 1
            continue

        kept[pid] = p
        counters["kept"] += 1

    return kept, counters

# =============================================================================
# Public API: Full + Top10 ledgers
# =============================================================================
def _run_bankroll(mode_label: str, target_table: str, top10_only: bool) -> None:
    print(f"=== bankroll {'TOP10' if top10_only else 'FULL'} updater starting ===")
    print(f"UNITS_PER_BET={UNITS_PER_BET} | UNIT_PCT={UNIT_PCT} | START_DATE={START_DATE}")
    print(f"Filters: DISABLE_FILTERS={DISABLE_FILTERS} | ONLY_MARKETS={ONLY_MARKETS or 'ALL'} | "
          f"CONF_MIN={CONF_MIN} | ONLY_PO_VALUE={ONLY_PO_VALUE}")

    bankroll = _load_state(mode_label)
    if bankroll is None:
        bankroll = DEFAULT_BANKROLL
    bankroll = float(bankroll)
    print(f"Starting bankroll{ ' (top10)' if top10_only else '' }: {bankroll:.2f}")

    # Read existing logged ids for idempotency
    logged = _already_logged_ids(target_table)

    # Load verifications
    verifs_raw = _load_verifications_since(START_DATE)
    print(f"Verifications fetched: {len(verifs_raw)}")

    # Filter verifs (exclude dates, already logged)
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
        return

    # Load predictions by id (no filters)
    preds_all = _load_predictions_by_ids(unique_pids)
    print(f"Predictions fetched by id: {len(preds_all)}")
    missing = set(unique_pids) - set(preds_all.keys())
    if missing:
        print(f"WARNING: {len(missing)} prediction_id(s) missing from value_predictions.")

    # Optional membership in Top10
    top10_map: Dict[str, Dict[str, Any]] = {}
    if top10_only:
        top10_map = _load_top10_map_by_prediction_ids(unique_pids)
        print(f"Top10 membership found for {len(top10_map)} prediction_id(s).")

    # Apply filters (unless disabled)
    preds_kept, counters = _apply_filters(preds_all)
    if not DISABLE_FILTERS:
        print(f"Filter counters: {counters}")
    print(f"Kept predictions after filters: {len(preds_kept)}")

    if not preds_kept:
        print("No predictions matched filters.")
        return

    # Sort verifs chronologically & compound
    verifs.sort(key=lambda v: v["verified_at"])
    rows: List[Dict[str, Any]] = []
    current = round(bankroll, 2)
    kept_count = 0
    skipped_not_in_top10 = 0
    for v in verifs:
        pid = v["prediction_id"]
        pred = preds_kept.get(pid)
        if not pred:
            continue
        if top10_only and (pid not in top10_map):
            skipped_not_in_top10 += 1
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

        if top10_only:
            row["top10_id"] = top10_map.get(pid, {}).get("top10_id")
            row["source"] = "top10"
        else:
            row["source"] = "all"

        rows.append(row)
        current = after
        kept_count += 1

    print(f"Rows to upsert: {len(rows)} | kept={kept_count} | skipped_not_in_top10={skipped_not_in_top10}")

    if not rows:
        print("All new verifications were filtered out.")
        return

    try:
        supabase.table(target_table).upsert(rows, on_conflict="prediction_id").execute()
        print(f"Inserted/updated rows: {len(rows)} | Final bankroll: {current:.2f}")
        _save_state(mode_label, current)
    except Exception as e:
        print(f"{target_table} upsert failed: {e}")

def update_bankroll_full(label: str = "bankroll_full") -> None:
    _run_bankroll(mode_label=label, target_table="bankroll_log", top10_only=False)

def update_bankroll_top10(label: str = "bankroll_top10") -> None:
    _run_bankroll(mode_label=label, target_table="bankroll_log_top10", top10_only=True)
