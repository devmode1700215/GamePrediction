# utils/update_bankroll_log.py
# -*- coding: utf-8 -*-
import os
import uuid
from datetime import datetime, timezone
from typing import Optional, Set, List, Dict, Any

from utils.supabaseClient import supabase

# =============================================================================
# Settings (override via env)
# =============================================================================
START_DATE       = os.getenv("BANKROLL_START_DATE", "2025-06-22")   # inclusive (YYYY-MM-DD)
EXCLUDE_DATES: Set[str] = set(filter(None, os.getenv("BANKROLL_EXCLUDE_DATES", "").split(",")))

# Filters for value_predictions used in bankroll compounding
ODDS_MIN         = float(os.getenv("BANKROLL_ODDS_MIN", "1.7"))
ODDS_MAX         = float(os.getenv("BANKROLL_ODDS_MAX", "2.3"))
CONF_MIN         = float(os.getenv("BANKROLL_CONF_MIN", "70"))
ONLY_MARKETS     = [m.strip() for m in os.getenv("BANKROLL_ONLY_MARKETS", "").split(",") if m.strip()]  # e.g. "over_2_5,btts"

# Bankroll behavior
DEFAULT_BANKROLL = float(os.getenv("BANKROLL_START", "100"))
LABEL            = os.getenv("BANKROLL_LABEL", "default")
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

def _stake_fraction(raw) -> float:
    """
    Accepts 1 => 1% and 0.01 => 1%. If invalid, returns 0.0.
    """
    v = _to_float(raw)
    if v is None: return 0.0
    return v / 100.0 if v > 1 else v

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

# ---- fallback to last log row -----------------------------------------------
def _read_last_bankroll_from_log() -> Optional[float]:
    # prefer created_at if present
    try:
        r = supabase.table("bankroll_log").select("bankroll_after, created_at")\
            .order("created_at", desc=True).limit(1).execute()
        rows = getattr(r, "data", None) or []
        if rows:
            b = _to_float(rows[0].get("bankroll_after"))
            if b is not None:
                return b
    except Exception:
        pass

    # fallback to date field
    try:
        r = supabase.table("bankroll_log").select("bankroll_after, date")\
            .order("date", desc=True).limit(1).execute()
        rows = getattr(r, "data", None) or []
        if rows:
            b = _to_float(rows[0].get("bankroll_after"))
            if b is not None:
                return b
    except Exception:
        pass

    return None

# ---- idempotency set --------------------------------------------------------
def _existing_logged_prediction_ids() -> Set[str]:
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
            .limit(5000)
            .execute()
        )
        return getattr(r, "data", None) or []
    except Exception:
        return []

# ---- value_predictions with filters ----------------------------------------
def _load_predictions_by_ids(pred_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(pred_ids), BATCH_SIZE):
        chunk = pred_ids[i : i + BATCH_SIZE]
        try:
            q = supabase.table("value_predictions").select(
                "id, market, prediction, stake_pct, odds, confidence_pct, po_value"
            ).in_("id", chunk)\
             .gte("confidence_pct", CONF_MIN)\
             .gte("odds", ODDS_MIN)\
             .lte("odds", ODDS_MAX)\
             .eq("po_value", True)

            if ONLY_MARKETS:
                # add market filter
                q = q.in_("market", ONLY_MARKETS)

            res = q.execute()
            rows = getattr(res, "data", None) or []
            for r in rows:
                out[r["id"]] = r
        except Exception:
            # skip batch on error
            continue
    return out

# =============================================================================
# Main entrypoint
# =============================================================================
def update_bankroll_log():
    print("=== bankroll_log updater starting ===")
    print(f"Settings: LABEL={LABEL} START_DATE={START_DATE} EXCLUDE={sorted(EXCLUDE_DATES) if EXCLUDE_DATES else '[]'}")
    print(f"Filters: CONF>={CONF_MIN} ODDS[{ODDS_MIN},{ODDS_MAX}] ONLY_MARKETS={ONLY_MARKETS or 'ALL'}")

    # A) Determine starting bankroll
    bankroll = _read_state_bankroll(LABEL)
    if bankroll is None:
        bankroll = _read_last_bankroll_from_log()
    if bankroll is None:
        bankroll = DEFAULT_BANKROLL
    bankroll = float(bankroll)
    print(f"A) starting bankroll: {bankroll:.2f}")

    # B) Get existing logged prediction_ids
    logged_ids = _existing_logged_prediction_ids()
    print(f"B) already logged prediction_ids: {len(logged_ids)}")

    # C) Load verifications window
    verifs_raw = _load_verifications_since(START_DATE)
    print(f"C) verifications fetched: {len(verifs_raw)}")

    # D) Filter verifications: has pid, not excluded by date, not already logged
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

    # E) Load matching predictions (with filters) in batches
    pred_ids = list({v["prediction_id"] for v in verifs})
    preds = _load_predictions_by_ids(pred_ids)
    if not preds:
        print("E) no predictions match filters (po_value=true, conf/odds range, market). Nothing to do.")
        return
    print(f"E) loaded predictions passing filters: {len(preds)}")

    # F) Sort verifications chronologically ASC for compounding
    verifs.sort(key=lambda v: v["verified_at"])

    logs_to_insert: List[Dict[str, Any]] = []
    current_bankroll = round(bankroll, 2)

    kept = 0
    skipped_no_pred = 0
    skipped_bad_stake = 0

    for v in verifs:
        pid = v["prediction_id"]
        pred = preds.get(pid)
        if not pred:
            skipped_no_pred += 1
            continue

        stake_pct = _to_float(pred.get("stake_pct"))
        odds      = _to_float(pred.get("odds"))
        conf      = _to_float(pred.get("confidence_pct"))
        po_value  = bool(pred.get("po_value"))

        # safety: ensure stake/odds/conf/povalue still valid
        if stake_pct is None or stake_pct <= 0:
            skipped_bad_stake += 1
            continue
        if odds is None or not (ODDS_MIN <= odds <= ODDS_MAX):
            continue
        if conf is None or conf < CONF_MIN:
            continue
        if not po_value:
            continue

        is_correct = bool(v.get("is_correct"))

        frac = _stake_fraction(stake_pct)
        if frac <= 0:
            skipped_bad_stake += 1
            continue

        stake_amount = round(frac * current_bankroll, 2)
        profit = round(stake_amount * (odds - 1.0), 2) if is_correct else round(-stake_amount, 2)
        after  = round(current_bankroll + profit, 2)

        # minimal columns (keep schema-agnostic)
        row = {
            "id": str(uuid.uuid4()),
            "prediction_id": pid,
            "date": v["verified_at"].split("T")[0],
            "stake_amount": stake_amount,
            "odds": round(odds, 2),
            "result": "win" if is_correct else "lose",
            "profit": profit,
            "starting_bankroll": current_bankroll,
            "bankroll_after": after,
            # created_at helps ordering if column exists
            "created_at": _now_iso(),
        }

        logs_to_insert.append(row)
        current_bankroll = after
        kept += 1

    print(f"F) ready to upsert: {len(logs_to_insert)} (kept={kept}, skipped_no_pred={skipped_no_pred}, skipped_bad_stake={skipped_bad_stake})")
    if not logs_to_insert:
        print("All new verifications were filtered out by rules.")
        return

    # G) Upsert by prediction_id (idempotent). If created_at column doesn't exist, retry without it.
    def _upsert(rows: List[Dict[str, Any]], include_created_at: bool) -> bool:
        payload = rows if include_created_at else [{k:v for k,v in r.items() if k != "created_at"} for r in rows]
        try:
            supabase.table("bankroll_log").upsert(payload, on_conflict="prediction_id").execute()
            return True
        except Exception as e:
            print(f"G) upsert failed ({'with' if include_created_at else 'without'} created_at): {e}")
            return False

    ok = _upsert(logs_to_insert, include_created_at=True)
    if not ok:
        ok = _upsert(logs_to_insert, include_created_at=False)
        if not ok:
            print("G) both upserts failed. Aborting.")
            return
        else:
            print("G) upsert succeeded without created_at.")

    print(f"G) upserted rows: {len(logs_to_insert)}")

    # H) Persist snapshot for next run
    _write_state_bankroll(LABEL, current_bankroll)

    # I) Console feedback
    for log in logs_to_insert[-10:]:
        print(f"✅ {log['result']:4} | {log['date']} | {log['starting_bankroll']} → {log['bankroll_after']} (pid={log['prediction_id']})")

    print(f"=== bankroll_log updater complete — final bankroll: {current_bankroll:.2f} ===")
