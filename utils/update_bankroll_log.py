# utils/update_bankroll_log.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Optional, Set, List, Dict, Any

from utils.supabaseClient import supabase

# =============================================================================
# Settings (ENV)
# =============================================================================
# We read directly from your existing table (singular):
SRC_TABLE = os.getenv("BANKROLL_SOURCE_TABLE", "value_predictions_with_result")

# Start date (inclusive)
START_DATE = os.getenv("BANKROLL_START_DATE", "2025-06-22")  # YYYY-MM-DD

# Optional exclude specific ISO dates like "2025-09-30,2025-10-05"
EXCLUDE_DATES: Set[str] = set(filter(None, os.getenv("BANKROLL_EXCLUDE_DATES", "").split(",")))

# Compounding parameters: stake_amount = bankroll * (UNITS_PER_BET * UNIT_PCT / 100)
DEFAULT_BANKROLL  = float(os.getenv("BANKROLL_START", "1000"))
UNITS_PER_BET     = float(os.getenv("BANKROLL_UNITS_PER_BET", "5"))   # e.g. 5 units
UNIT_PCT          = float(os.getenv("BANKROLL_UNIT_PCT", "1.0"))      # 1 unit = 1% of bankroll

# Filters — set to False/0 to effectively allow all
ONLY_MARKETS   = [m.strip() for m in os.getenv("BANKROLL_ONLY_MARKETS", "").split(",") if m.strip()]  # e.g. "over_2_5"
CONF_MIN       = float(os.getenv("BANKROLL_CONF_MIN", "0"))
ONLY_PO_VALUE  = os.getenv("BANKROLL_ONLY_PO_VALUE", "false").lower() in ("1", "true", "yes", "y")

# Batching (not strictly needed here but kept for consistency)
BATCH_SIZE = int(os.getenv("BANKROLL_BATCH_SIZE", "5000"))


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

def _date_str_from_iso(ts_iso: Optional[str]) -> Optional[str]:
    try:
        return ts_iso.split("T", 1)[0]
    except Exception:
        return None

def _compute_stake_amount(current_bankroll: float, units: float, unit_pct: float) -> float:
    # stake_amount = bankroll * (units * unit_pct / 100)
    frac = max(0.0, (units * unit_pct) / 100.0)
    return round(current_bankroll * frac, 2)

def _profit(odds: Optional[float], stake_amount: float, is_correct: bool) -> float:
    if odds is None:
        return 0.0
    return round(stake_amount * (odds - 1.0), 2) if is_correct else round(-stake_amount, 2)

def _load_state(label: str) -> Optional[float]:
    try:
        r = supabase.table("bankroll_state").select("bankroll").eq("label", label).limit(1).execute()
        rows = getattr(r, "data", None) or []
        if rows:
            val = _to_float(rows[0].get("bankroll"))
            if val is not None:
                return val
    except Exception:
        pass
    return None

def _save_state(label: str, bankroll: float) -> None:
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

def _pass_filters(row: Dict[str, Any]) -> bool:
    """Apply optional filters client-side for transparency."""
    if ONLY_MARKETS and (row.get("market") not in ONLY_MARKETS):
        return False
    conf = _to_float(row.get("confidence_pct"))
    if (conf is None) or (conf < CONF_MIN):
        return False
    if ONLY_PO_VALUE and (not bool(row.get("po_value"))):
        return False
    return True


# =============================================================================
# Loaders
# =============================================================================
def _load_settled_rows(start_date: str) -> List[Dict[str, Any]]:
    """
    Pull rows from value_predictions_with_result (singular) that are settled:
      - is_correct is not null
      - settled_at >= <start_date>T00:00:00Z
    Order by settled_at ASC for stable compounding.
    """
    from_dt = f"{start_date}T00:00:00Z"
    try:
        r = (
            supabase.table(SRC_TABLE)
            .select(
                "id, fixture_id, market, prediction, odds, confidence_pct, po_value, "
                "created_at, is_correct, home_team, away_team, result_side, result_goals, settled_at"
            )
            .not_.is_("is_correct", "null")
            .gte("settled_at", from_dt)
            .order("settled_at", desc=False)
            .limit(50000)
            .execute()
        )
        return getattr(r, "data", None) or []
    except Exception as e:
        print(f"[bankroll] load from {SRC_TABLE} failed: {e}")
        return []

def _top10_membership(prediction_ids: List[str]) -> Set[str]:
    try:
        res = (
            supabase.table("top10_predictions")
            .select("prediction_id")
            .in_("prediction_id", prediction_ids)
            .execute()
        )
        rows = getattr(res, "data", None) or []
        return {x["prediction_id"] for x in rows if x.get("prediction_id")}
    except Exception:
        return set()


# =============================================================================
# Core compounding
# =============================================================================
def _run(mode_label: str, target_table: str, top10_only: bool) -> None:
    print(f"=== bankroll {'TOP10' if top10_only else 'FULL'} updater starting ===")
    print(f"SRC_TABLE={SRC_TABLE} | START_DATE={START_DATE}")
    print(f"UNITS_PER_BET={UNITS_PER_BET} | UNIT_PCT={UNIT_PCT}")
    print(f"Filters: ONLY_MARKETS={ONLY_MARKETS or 'ALL'} | CONF_MIN={CONF_MIN} | ONLY_PO_VALUE={ONLY_PO_VALUE}")

    bankroll = _load_state(mode_label)
    if bankroll is None:
        bankroll = DEFAULT_BANKROLL
    bankroll = float(bankroll)
    print(f"Starting bankroll: {bankroll:.2f}")

    already = _already_logged_ids(target_table)
    rows = _load_settled_rows(START_DATE)
    print(f"Settled rows fetched: {len(rows)}")

    if not rows:
        print("No settled rows found. Nothing to do.")
        return

    # Ensure chronological order by settled_at
    rows.sort(key=lambda r: r.get("settled_at") or "")

    # Restrict to Top10 membership if requested
    if top10_only:
        pid_list = [str(r.get("id")) for r in rows if r.get("id")]
        tset = _top10_membership(pid_list)
        print(f"Top10 membership matches: {len(tset)}")

    out_rows: List[Dict[str, Any]] = []
    current = round(bankroll, 2)
    new_count = 0
    skipped_existing = 0
    skipped_filters = 0
    skipped_excluded = 0

    for r in rows:
        pid = str(r.get("id") or "")
        if not pid:
            continue
        if pid in already:
            skipped_existing += 1
            continue

        d = _date_str_from_iso(r.get("settled_at") or r.get("created_at") or "")
        if d in EXCLUDE_DATES:
            skipped_excluded += 1
            continue

        if top10_only and (pid not in tset):
            continue

        if not _pass_filters(r):
            skipped_filters += 1
            continue

        odds = _to_float(r.get("odds"))
        if odds is None:
            continue
        is_correct = bool(r.get("is_correct"))

        stake_amt = _compute_stake_amount(current, UNITS_PER_BET, UNIT_PCT)
        profit = _profit(odds, stake_amt, is_correct)
        after = round(current + profit, 2)

        out_rows.append({
            "id": str(uuid.uuid4()),
            "prediction_id": pid,
            "fixture_id": r.get("fixture_id"),
            "market": r.get("market"),
            "date": d,
            "stake_units": UNITS_PER_BET,
            "stake_amount": stake_amt,               # <-- store amount, not pct
            "odds": round(odds, 2),
            "result": "win" if is_correct else "lose",
            "profit": profit,
            "starting_bankroll": current,
            "bankroll_after": after,
            "created_at": _now_iso(),
            "source": "top10" if top10_only else "all",
        })

        current = after
        new_count += 1

    print(f"Prepared rows: {len(out_rows)} | new_count={new_count} | "
          f"skipped_existing={skipped_existing} | skipped_filters={skipped_filters} | skipped_excluded={skipped_excluded}")

    if not out_rows:
        print("Nothing to upsert.")
        return

    # Upsert by prediction_id for idempotency
    try:
        supabase.table(target_table).upsert(out_rows, on_conflict="prediction_id").execute()
        print(f"Inserted/updated: {len(out_rows)} | Final bankroll: {current:.2f}")
        _save_state(mode_label, current)
    except Exception as e:
        print(f"{target_table} upsert failed: {e}")


def update_bankroll_full(label: str = "bankroll_full") -> None:
    _run(mode_label=label, target_table="bankroll_log", top10_only=False)

def update_bankroll_top10(label: str = "bankroll_top10") -> None:
    _run(mode_label=label, target_table="bankroll_log_top10", top10_only=True)


# Auto-run when invoked as: python -m utils.update_bankroll_log
if __name__ == "__main__":
    print("✅ Supabase connection successful")
    update_bankroll_full()
    update_bankroll_top10()
