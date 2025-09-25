# utils/update_bankroll_log.py
# -*- coding: utf-8 -*-
import os
import uuid
from datetime import datetime, timezone
from typing import Optional, Tuple, Set

from utils.supabaseClient import supabase

# --- Settings ---------------------------------------------------------------
START_DATE       = os.getenv("BANKROLL_START_DATE", "2025-06-22")  # inclusive
EXCLUDE_DATES: Set[str] = set(filter(None, os.getenv("BANKROLL_EXCLUDE_DATES", "2025-08-08").split(",")))
ODDS_MIN         = float(os.getenv("BANKROLL_ODDS_MIN", "1.3"))
ODDS_MAX         = float(os.getenv("BANKROLL_ODDS_MAX", "4"))
CONF_MIN         = float(os.getenv("BANKROLL_CONF_MIN", "70"))
DEFAULT_BANKROLL = float(os.getenv("BANKROLL_START", "100"))
BATCH_SIZE       = int(os.getenv("BANKROLL_BATCH_SIZE", "1000"))
LABEL            = os.getenv("BANKROLL_LABEL", "default")  # optional “multi-bankroll” label

# --- Helpers ----------------------------------------------------------------
def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def _stake_fraction(raw) -> float:
    """
    Accepts 1 => 1%, 0.01 => 1%.
    """
    v = _to_float(raw) or 0.0
    return v / 100.0 if v > 1 else v

# --- State readers/writers --------------------------------------------------
def _read_state_bankroll(label: str) -> Optional[float]:
    """
    Read bankroll from optional bankroll_state table if it exists.
    If table/policy missing, just return None.
    """
    try:
        r = (
            supabase.table("bankroll_state")
            .select("bankroll")
            .eq("label", label)
            .limit(1)
            .execute()
        )
        rows = getattr(r, "data", None) or []
        if rows:
            return _to_float(rows[0].get("bankroll"))
    except Exception:
        pass
    return None

def _write_state_bankroll(label: str, bankroll: float):
    """
    Best-effort upsert to bankroll_state. If table/policy missing, just skip.
    """
    try:
        supabase.table("bankroll_state").upsert(
            {"label": label, "bankroll": float(bankroll), "updated_at": _now_iso()},
            on_conflict="label",
        ).execute()
    except Exception:
        pass

def _read_last_bankroll_from_log() -> Optional[float]:
    """
    Fallback: read last bankroll from bankroll_log.
    Try ordering by created_at (if present), else by date desc.
    """
    # Try created_at first
    try:
        r = (
            supabase.table("bankroll_log")
            .select("bankroll_after, created_at")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        rows = getattr(r, "data", None) or []
        if rows:
            b = _to_float(rows[0].get("bankroll_after"))
            if b is not None:
                return b
    except Exception:
        pass

    # Fallback: order by date desc (text yyyy-mm-dd)
    try:
        r = (
            supabase.table("bankroll_log")
            .select("bankroll_after, date")
            .order("date", desc=True)
            .limit(1)
            .execute()
        )
        rows = getattr(r, "data", None) or []
        if rows:
            b = _to_float(rows[0].get("bankroll_after"))
            if b is not None:
                return b
    except Exception:
        pass

    return None

# --- Core -------------------------------------------------------------------
def update_bankroll_log():
    print("=== bankroll_log updater starting ===")
    print(f"Settings: START_DATE={START_DATE} CONF_MIN={CONF_MIN} ODDS[{ODDS_MIN},{ODDS_MAX}] LABEL={LABEL}")

    # A) Determine starting bankroll (state -> log -> default)
    bankroll = _read_state_bankroll(LABEL)
    if bankroll is None:
        bankroll = _read_last_bankroll_from_log()
    if bankroll is None:
        bankroll = DEFAULT_BANKROLL

    print(f"A) starting bankroll: {bankroll:.2f}")

    # B) Already logged prediction_ids (idempotency)
    try:
        logged_ids_q = supabase.table("bankroll_log").select("prediction_id").execute()
        logged_ids = {r["prediction_id"] for r in (logged_ids_q.data or []) if r.get("prediction_id")}
        print(f"B) already logged prediction_ids: {len(logged_ids)}")
    except Exception as e:
        print(f"B) failed to load existing logs: {e}")
        logged_ids = set()

    # C) Verifications in window
    try:
        verifs_q = (
            supabase.table("verifications")
            .select("prediction_id, verified_at, is_correct")
            .gte("verified_at", f"{START_DATE}T00:00:00Z")
            .order("verified_at", desc=True)
            .limit(5000)
            .execute()
        )
        verifs_raw = verifs_q.data or []
        print(f"C) verifications fetched (since {START_DATE}): {len(verifs_raw)}")
    except Exception as e:
        print(f"C) failed to fetch verifications: {e}")
        verifs_raw = []

    # D) Filter: has prediction_id, not already logged, not in EXCLUDE_DATES
    verifs = []
    excluded_dates = 0
    for v in verifs_raw:
        pid = v.get("prediction_id")
        ts = v.get("verified_at")
        if not pid or not ts:
            continue
        date_only = ts.split("T", 1)[0]
        if date_only in EXCLUDE_DATES:
            excluded_dates += 1
            continue
        if pid in logged_ids:
            continue
        verifs.append(v)

    print(f"D) after removing duplicates & excluded dates: {len(verifs)} (excluded {excluded_dates} by date)")

    if not verifs:
        print("Nothing new to process.")
        return

    # E) Load matching value_predictions with filters
    pred_ids = list({v["prediction_id"] for v in verifs})
    print(f"E) unique prediction_ids to load: {len(pred_ids)}")

    pred_by_id = {}
    # chunk IN list
    for i in range(0, len(pred_ids), BATCH_SIZE):
        chunk = pred_ids[i : i + BATCH_SIZE]
        try:
            pq = (
                supabase.table("value_predictions")
                .select("id, stake_pct, odds, confidence_pct, po_value, market, prediction")
                .in_("id", chunk)
                .gte("confidence_pct", CONF_MIN)
                .gte("odds", ODDS_MIN)
                .lte("odds", ODDS_MAX)
                .eq("po_value", True)
                .execute()
            )
            rows = pq.data or []
            for p in rows:
                pred_by_id[p["id"]] = p
            print(f"E) loaded predictions batch {i//BATCH_SIZE + 1}: {len(rows)}")
        except Exception as e:
            print(f"E) failed to load predictions batch {i//BATCH_SIZE + 1}: {e}")

    if not pred_by_id:
        print("E) no predictions match filters (po_value=true, conf>=min, odds in range). Nothing to do.")
        return

    # F) Sort verifications chronologically ASC for compounding
    verifs.sort(key=lambda v: v["verified_at"])
    logs_to_insert = []
    current_bankroll = round(bankroll, 2)

    kept = skipped_no_pred = skipped_bad_stake = 0

    for v in verifs:
        pid = v["prediction_id"]
        pred = pred_by_id.get(pid)
        if not pred:
            skipped_no_pred += 1
            continue

        stake_pct = _to_float(pred.get("stake_pct"))
        odds      = _to_float(pred.get("odds"))
        conf      = _to_float(pred.get("confidence_pct"))

        if stake_pct is None or stake_pct <= 0:
            skipped_bad_stake += 1
            continue
        if odds is None or not (ODDS_MIN <= odds <= ODDS_MAX):
            continue
        if conf is None or conf < CONF_MIN:
            continue

        is_correct = bool(v.get("is_correct"))
        frac = _stake_fraction(stake_pct)
        stake_amount = round(frac * current_bankroll, 2)

        profit = round(stake_amount * (odds - 1.0), 2) if is_correct else round(-stake_amount, 2)
        after  = round(current_bankroll + profit, 2)

        # Prefer real market/prediction from value_predictions if available
        market = (pred.get("market") or "over_2_5")
        pred_label = (pred.get("prediction") or ("Over" if "over" in market else "Under"))

        logs_to_insert.append({
            "id": str(uuid.uuid4()),
            "prediction_id": pid,
            "date": v["verified_at"].split("T")[0],
            "stake_amount": stake_amount,
            "odds": round(odds, 2),
            "result": "win" if is_correct else "lose",
            "profit": profit,
            "starting_bankroll": current_bankroll,
            "bankroll_after": after,
            "created_at": _now_iso(),     # ensure ordering works on next runs
            "market": market,
            "prediction": pred_label,
        })

        current_bankroll = after
        kept += 1

    print(f"F) ready to insert: {len(logs_to_insert)} "
          f"(kept={kept}, skipped_no_pred={skipped_no_pred}, skipped_bad_stake={skipped_bad_stake})")

    if not logs_to_insert:
        print("All new verifications were filtered out by rules.")
        return

    # G) Upsert by prediction_id (idempotent)
    try:
        supabase.table("bankroll_log").upsert(
            logs_to_insert, on_conflict="prediction_id"
        ).execute()
        print(f"G) upserted rows: {len(logs_to_insert)}")
    except Exception as e:
        print(f"G) upsert failed, trying fallback insert-only: {e}")
        try:
            existing = (
                supabase.table("bankroll_log")
                .select("prediction_id")
                .in_("prediction_id", [r["prediction_id"] for r in logs_to_insert])
                .execute()
                .data
                or []
            )
            have = {r["prediction_id"] for r in existing}
            to_insert = [r for r in logs_to_insert if r["prediction_id"] not in have]
            if to_insert:
                supabase.table("bankroll_log").insert(to_insert).execute()
                print(f"G) inserted (fallback) rows: {len(to_insert)}")
            else:
                print("G) nothing new to insert after fallback (all existed).")
        except Exception as e2:
            print(f"G) fallback insert failed: {e2}")
            return

    # H) Persist the new bankroll for next run (if bankroll_state exists)
    _write_state_bankroll(LABEL, current_bankroll)

    # I) Console feedback
    for log in logs_to_insert:
        print(f"✅ {log['result']:4} | {log['date']} | {log['starting_bankroll']} → {log['bankroll_after']} "
              f"(pid={log['prediction_id']})")

    print(f"=== bankroll_log updater complete — final bankroll: {current_bankroll:.2f} ===")
