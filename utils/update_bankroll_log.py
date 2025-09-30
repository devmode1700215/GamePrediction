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
# Time window
START_DATE       = os.getenv("BANKROLL_START_DATE", "2025-06-22")  # inclusive (YYYY-MM-DD)
EXCLUDE_DATES: Set[str] = set(filter(None, os.getenv("BANKROLL_EXCLUDE_DATES", "").split(",")))

# Selection filters (keep light; compounding robustness > strict filtering)
ODDS_MIN         = float(os.getenv("BANKROLL_ODDS_MIN", "1.01"))   # allow near-evens by default
ODDS_MAX         = float(os.getenv("BANKROLL_ODDS_MAX", "100.0"))
ONLY_MARKETS     = [m.strip() for m in os.getenv("BANKROLL_ONLY_MARKETS", "").split(",") if m.strip()]  # e.g. "over_2_5,btts"
REQUIRE_PO_VALUE = os.getenv("BANKROLL_REQUIRE_PO_VALUE", "false").lower() in ("1","true","yes","y")
CONF_MIN         = float(os.getenv("BANKROLL_CONF_MIN", "0"))      # your confidence_pct is 0..1 in DB; set 0.7 if needed

# Bankroll behavior
DEFAULT_BANKROLL = float(os.getenv("BANKROLL_START", "1000"))      # ← your example starts at 1000
LABEL            = os.getenv("BANKROLL_LABEL", "default")          # multi-ledger support

# Staking mode:
#   - "units": use UNIT_PCT on every bet (compounding)
#   - "from_pred": use prediction.stake_pct if set; else fall back to UNIT_PCT
STAKE_MODE       = os.getenv("BANKROLL_STAKE_MODE", "units")       # "units" | "from_pred"

# Unit size (accepts 1 => 1%, or 0.01 => 1%)
UNIT_PCT_RAW     = os.getenv("BANKROLL_UNIT", "1")                 # default 1% unit
ROUND_STAKE_2DP  = os.getenv("BANKROLL_ROUND_STAKE_2DP", "true").lower() in ("1","true","yes","y")

# Batching
BATCH_SIZE       = int(os.getenv("BANKROLL_BATCH_SIZE", "1000"))

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

def _pct(raw) -> float:
    """
    Accepts:
      - 1      => 0.01  (1%)
      - 0.01   => 0.01  (1%)
    """
    v = _to_float(raw)
    if v is None: 
        return 0.0
    return v / 100.0 if v > 1 else v

UNIT_FRAC = _pct(UNIT_PCT_RAW)

def _round_money(x: float) -> float:
    return round(x, 2) if ROUND_STAKE_2DP else float(x)

# ---- state snapshot (optional table) ----------------------------------------
def _read_state_bankroll(label: str) -> Optional[float]:
    try:
        r = supabase.table("bankroll_state").select("bankroll").eq("label", label).limit(1).execute()
        rows = getattr(r, "data", None) or []
        if rows:
            return _to_float(rows[0].get("bankroll"))
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
        # table may not exist; ignore
        pass

# ---- idempotency set --------------------------------------------------------
def _existing_logged_prediction_ids(label: str) -> Set[str]:
    """
    If bankroll_log has a label column, filter by it; else read all and treat as global.
    """
    try:
        # Try label-aware
        r = supabase.table("bankroll_log").select("prediction_id,label").eq("label", label).execute()
        rows = getattr(r, "data", None) or []
        return {row["prediction_id"] for row in rows if row.get("prediction_id")}
    except Exception:
        pass

    try:
        r = supabase.table("bankroll_log").select("prediction_id").execute()
        rows = getattr(r, "data", None) or []
        return {row["prediction_id"] for row in rows if row.get("prediction_id")}
    except Exception:
        return set()

# ---- verifications ----------------------------------------------------------
def _load_verifications_since(start_date: str) -> List[Dict[str, Any]]:
    try:
        r = (
            supabase.table("verifications")
            .select("prediction_id, verified_at, is_correct")
            .gte("verified_at", f"{start_date}T00:00:00Z")
            .order("verified_at", desc=True)
            .limit(10000)
            .execute()
        )
        return getattr(r, "data", None) or []
    except Exception:
        return []

# ---- value_predictions ------------------------------------------------------
def _load_predictions_by_ids(pred_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Load minimal fields needed for bankroll calc.
    confidence_pct assumed 0..1 scale (if yours is 0..100, set CONF_MIN accordingly).
    """
    out: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(pred_ids), BATCH_SIZE):
        chunk = pred_ids[i : i + BATCH_SIZE]
        try:
            q = supabase.table("value_predictions").select(
                "id, market, prediction, stake_pct, odds, confidence_pct, po_value"
            ).in_("id", chunk)

            # Optional soft filters
            if ONLY_MARKETS:
                q = q.in_("market", ONLY_MARKETS)
            if CONF_MIN > 0:
                q = q.gte("confidence_pct", CONF_MIN)
            q = q.gte("odds", ODDS_MIN).lte("odds", ODDS_MAX)

            if REQUIRE_PO_VALUE:
                q = q.eq("po_value", True)

            res = q.execute()
            rows = getattr(res, "data", None) or []
            for r in rows:
                out[r["id"]] = r
        except Exception:
            continue
    return out

# =============================================================================
# Core compounding
# =============================================================================
def _stake_fraction_for(pred: Dict[str, Any]) -> float:
    """
    Returns fraction of bankroll to stake (0..1).
    - "units": always UNIT_FRAC
    - "from_pred": use prediction.stake_pct if > 0, else UNIT_FRAC
    """
    if STAKE_MODE == "from_pred":
        frac = _pct(pred.get("stake_pct"))
        if frac and frac > 0:
            return float(frac)
    return float(UNIT_FRAC)

def _compute_after_bankroll(start_bankroll: float, stake_frac: float, odds: float, is_win: bool) -> Tuple[float, float]:
    """
    Returns (profit, bankroll_after), compounding on each settled bet.
    Profit is rounded; bankroll_after is computed from rounded stake/profit for clean statements.
    """
    stake_amt = _round_money(start_bankroll * stake_frac)
    if is_win:
        profit = _round_money(stake_amt * (odds - 1.0))
    else:
        profit = _round_money(-stake_amt)
    after = _round_money(start_bankroll + profit)
    return profit, after

# =============================================================================
# Entrypoint
# =============================================================================
def update_bankroll_log():
    print("=== bankroll_log updater starting ===")
    print(f"Settings: LABEL={LABEL} START_DATE={START_DATE} EXCLUDE={sorted(EXCLUDE_DATES) if EXCLUDE_DATES else '[]'}")
    print(f"Stake mode: {STAKE_MODE} | UNIT_FRAC={UNIT_FRAC:.4f} ({UNIT_FRAC*100:.2f}%)")
    print(f"Filters: CONF>={CONF_MIN} ODDS[{ODDS_MIN},{ODDS_MAX}] ONLY_MARKETS={ONLY_MARKETS or 'ALL'} "
          f"REQUIRE_PO_VALUE={REQUIRE_PO_VALUE}")

    # A) Determine starting bankroll (state → last log → default)
    bankroll = _read_state_bankroll(LABEL)
    if bankroll is None:
        # Try last row for this label first
        try:
            r = supabase.table("bankroll_log").select("bankroll_after,label,created_at")\
                .eq("label", LABEL).order("created_at", desc=True).limit(1).execute()
            rows = getattr(r, "data", None) or []
            if rows:
                bankroll = _to_float(rows[0].get("bankroll_after"))
        except Exception:
            bankroll = None

    if bankroll is None:
        # Or any last row if label column doesn't exist
        try:
            r = supabase.table("bankroll_log").select("bankroll_after,created_at")\
                .order("created_at", desc=True).limit(1).execute()
            rows = getattr(r, "data", None) or []
            if rows:
                bankroll = _to_float(rows[0].get("bankroll_after"))
        except Exception:
            bankroll = None

    if bankroll is None:
        bankroll = DEFAULT_BANKROLL
    bankroll = float(bankroll)
    print(f"A) starting bankroll: {bankroll:.2f}")

    # B) Read idempotency set
    logged_ids = _existing_logged_prediction_ids(LABEL)
    print(f"B) already logged prediction_ids (for label='{LABEL}' if available): {len(logged_ids)}")

    # C) Load verifications since START_DATE
    verifs_raw = _load_verifications_since(START_DATE)
    print(f"C) verifications fetched: {len(verifs_raw)}")

    # D) Filter verifications by date & idempotency
    verifs = []
    excl_dates = 0
    for v in verifs_raw:
        pid = v.get("prediction_id")
        ts  = v.get("verified_at")
        if not pid or not ts:
            continue
        d = ts.split("T", 1)[0]
        if d in EXCLUDE_DATES:
            excl_dates += 1
            continue
        if pid in logged_ids:
            continue
        verifs.append(v)
    print(f"D) after filters: {len(verifs)} (excluded_by_date={excl_dates}, new_ids={len({v['prediction_id'] for v in verifs})})")
    if not verifs:
        print("Nothing new to process.")
        return

    # E) Load predictions for those IDs
    pred_ids = list({v["prediction_id"] for v in verifs})
    preds = _load_predictions_by_ids(pred_ids)
    if not preds:
        print("E) no predictions match current filters. Nothing to do.")
        return
    print(f"E) loaded predictions passing filters: {len(preds)}")

    # F) Sort verifications ASC (chronological compounding)
    verifs.sort(key=lambda v: v["verified_at"])

    rows_to_upsert: List[Dict[str, Any]] = []
    current_bankroll = _round_money(bankroll)

    kept = 0
    skipped_no_pred = 0
    skipped_bad_odds = 0

    for v in verifs:
        pid = v["prediction_id"]
        pred = preds.get(pid)
        if not pred:
            skipped_no_pred += 1
            continue

        odds = _to_float(pred.get("odds"))
        if odds is None or odds < ODDS_MIN or odds > ODDS_MAX:
            skipped_bad_odds += 1
            continue

        stake_frac = _stake_fraction_for(pred)
        if stake_frac <= 0:
            # zero stake? skip silently
            continue

        is_correct = bool(v.get("is_correct"))
        profit, after = _compute_after_bankroll(current_bankroll, stake_frac, odds, is_correct)

        row = {
            "id": str(uuid.uuid4()),
            "label": LABEL,  # if your table doesn't have it, we'll remove in fallback
            "prediction_id": pid,
            "date": v["verified_at"].split("T")[0],
            "stake_amount": _round_money(current_bankroll * stake_frac),
            "stake_pct": round(stake_frac, 4),
            "odds": _round_money(odds),
            "result": "win" if is_correct else "lose",
            "profit": profit,
            "starting_bankroll": current_bankroll,
            "bankroll_after": after,
            "created_at": _now_iso(),  # nice for ordering if column exists
        }

        rows_to_upsert.append(row)
        current_bankroll = after
        kept += 1

    print(f"F) ready: {len(rows_to_upsert)} (kept={kept}, skipped_no_pred={skipped_no_pred}, skipped_bad_odds={skipped_bad_odds})")
    if not rows_to_upsert:
        print("All new verifications were filtered out or had zero stake.")
        return

    # G) Upsert rows (idempotent). Prefer on_conflict=(prediction_id,label). Fallbacks handle missing columns.
    def _attempt_upsert(rows: List[Dict[str, Any]], remove_cols: List[str], on_conflict: str) -> bool:
        payload = [
            {k: v for k, v in r.items() if k not in remove_cols}
            for r in rows
        ]
        try:
            supabase.table("bankroll_log").upsert(payload, on_conflict=on_conflict).execute()
            return True
        except Exception as e:
            print(f"G) upsert failed ({on_conflict}, -{remove_cols}): {e}")
            return False

    # Try most expressive first
    if not _attempt_upsert(rows_to_upsert, remove_cols=[], on_conflict="prediction_id,label"):
        if not _attempt_upsert(rows_to_upsert, remove_cols=["created_at"], on_conflict="prediction_id,label"):
            if not _attempt_upsert(rows_to_upsert, remove_cols=["label"], on_conflict="prediction_id"):
                if not _attempt_upsert(rows_to_upsert, remove_cols=["label", "created_at"], on_conflict="prediction_id"):
                    print("G) all upserts failed. Aborting.")
                    return

    print(f"G) upserted rows: {len(rows_to_upsert)}")

    # H) Persist snapshot for next run
    _write_state_bankroll(LABEL, current_bankroll)

    # I) Show last few steps
    for log in rows_to_upsert[-10:]:
        print(f"✅ {log['result']:4} | {log['date']} | {log['starting_bankroll']} → {log['bankroll_after']} "
              f"(odds={log['odds']}, stake={log['stake_amount']}) [pid={log['prediction_id']}]")

    print(f"=== bankroll_log updater complete — final bankroll: {current_bankroll:.2f} ===")
