# utils/update_bankroll_log.py
# -*- coding: utf-8 -*-
import os
import uuid
from datetime import datetime, timezone
from typing import Optional, Set, List, Dict, Any

from utils.supabaseClient import supabase

START_DATE       = os.getenv("BANKROLL_START_DATE", "2025-06-22")
EXCLUDE_DATES: Set[str] = set(filter(None, os.getenv("BANKROLL_EXCLUDE_DATES", "").split(",")))

# Filters
ODDS_MIN         = float(os.getenv("BANKROLL_ODDS_MIN", "1.0"))
ODDS_MAX         = float(os.getenv("BANKROLL_ODDS_MAX", "100.0"))
CONF_MIN         = float(os.getenv("BANKROLL_CONF_MIN", "0"))
ONLY_MARKETS     = [m.strip() for m in os.getenv("BANKROLL_ONLY_MARKETS", "").split(",") if m.strip()]

# NEW: disable odds filtering entirely
DISABLE_ODDS_FILTER = os.getenv("BANKROLL_DISABLE_ODDS_FILTER", "true").lower() in ("1","true","yes","y")

DEFAULT_BANKROLL = float(os.getenv("BANKROLL_START", "1000"))
BANKROLL_MODE    = os.getenv("BANKROLL_MODE", "fixed").lower()  # "fixed" or "percent"
FIXED_STAKE      = float(os.getenv("BANKROLL_FIXED_STAKE", "5"))
BATCH_SIZE       = int(os.getenv("BANKROLL_BATCH_SIZE", "1000"))

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def _stake_fraction(raw) -> float:
    v = _to_float(raw)
    if v is None: return 0.0
    return v / 100.0 if v > 1 else v

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
        pass

def _existing_logged_prediction_ids() -> Set[str]:
    try:
        r = supabase.table("bankroll_log").select("prediction_id").execute()
        rows = getattr(r, "data", None) or []
        return {row["prediction_id"] for row in rows if row.get("prediction_id")}
    except Exception:
        return set()

def _load_verifications_since(start_date: str) -> List[Dict[str, Any]]:
    try:
        r = (
            supabase.table("verifications")
            .select("prediction_id, verified_at, is_correct")
            .gte("verified_at", f"{start_date}T00:00:00Z")
            .order("verified_at", desc=True)
            .limit(20000)
            .execute()
        )
        return getattr(r, "data", None) or []
    except Exception:
        return []

def _load_predictions_by_ids(pred_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(pred_ids), BATCH_SIZE):
        chunk = pred_ids[i : i + BATCH_SIZE]
        try:
            q = supabase.table("value_predictions").select(
                "id, fixture_id, market, prediction, stake_pct, stake_amount, odds, confidence_pct, po_value"
            ).in_("id", chunk)
            if CONF_MIN > 0:
                q = q.gte("confidence_pct", CONF_MIN)
            if ONLY_MARKETS:
                q = q.in_("market", ONLY_MARKETS)
            # NOTE: apply odds filter only if enabled
            if not DISABLE_ODDS_FILTER:
                q = q.gte("odds", ODDS_MIN).lte("odds", ODDS_MAX)
            res = q.execute()
            rows = getattr(res, "data", None) or []
            for r in rows:
                out[r["id"]] = r
        except Exception:
            continue
    return out

def rebuild_or_append(label: str = "default"):
    print("=== bankroll_log updater starting ===")
    print(f"Mode={BANKROLL_MODE} | START_DATE={START_DATE} | EXCLUDE={sorted(EXCLUDE_DATES) if EXCLUDE_DATES else '[]'}")
    print(f"Filters: CONF>={CONF_MIN} ODDS[{'OFF' if DISABLE_ODDS_FILTER else f'{ODDS_MIN},{ODDS_MAX}'}] ONLY_MARKETS={ONLY_MARKETS or 'ALL'}")

    bankroll = _read_state_bankroll(label)
    if bankroll is None:
        try:
            r = supabase.table("bankroll_log").select("bankroll_after").order("created_at", desc=True).limit(1).execute()
            rows = getattr(r, "data", None) or []
            bankroll = _to_float(rows[0]["bankroll_after"]) if rows else None
        except Exception:
            bankroll = None
    if bankroll is None:
        bankroll = DEFAULT_BANKROLL
    bankroll = float(bankroll)
    print(f"A) starting bankroll: {bankroll:.2f}")

    logged_ids = _existing_logged_prediction_ids()
    print(f"B) already logged prediction_ids: {len(logged_ids)}")

    verifs_raw = _load_verifications_since(START_DATE)
    print(f"C) verifications fetched: {len(verifs_raw)}")

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

    pred_ids = list({v["prediction_id"] for v in verifs})
    preds = _load_predictions_by_ids(pred_ids)
    if not preds:
        print("E) no predictions match filters. Nothing to do.")
        return
    print(f"E) loaded predictions passing filters: {len(preds)}")

    verifs.sort(key=lambda v: v["verified_at"])

    logs_to_insert: List[Dict[str, Any]] = []
    current_bankroll = round(bankroll, 2)

    for v in verifs:
        pid = v["prediction_id"]
        pred = preds.get(pid)
        if not pred:
            continue

        odds = _to_float(pred.get("odds"))
        if odds is None:
            continue

        # if odds filter enabled, enforce bounds
        if not DISABLE_ODDS_FILTER and not (ODDS_MIN <= odds <= ODDS_MAX):
            continue

        is_correct = bool(v.get("is_correct"))
        when = v["verified_at"].split("T")[0]

        if BANKROLL_MODE == "percent":
            frac = _stake_fraction(pred.get("stake_pct"))
            stake_amount = round(max(frac, 0.0) * current_bankroll, 2)
        else:
            stake_amount = _to_float(pred.get("stake_amount"))
            if stake_amount is None or stake_amount <= 0:
                stake_amount = FIXED_STAKE
            stake_amount = round(float(stake_amount), 2)

        profit = round(stake_amount * (odds - 1.0), 2) if is_correct else round(-stake_amount, 2)
        after  = round(current_bankroll + profit, 2)

        logrow = {
            "id": str(uuid.uuid4()),
            "prediction_id": pid,
            "date": when,
            "stake_amount": stake_amount,
            "odds": round(odds, 2),
            "result": "win" if is_correct else "lose",
            "profit": profit,
            "starting_bankroll": current_bankroll,
            "bankroll_after": after,
            "created_at": _now_iso(),
        }
        logs_to_insert.append(logrow)
        current_bankroll = after

    if not logs_to_insert:
        print("All new verifications were filtered out by rules.")
        return

    try:
        supabase.table("bankroll_log").upsert(logs_to_insert, on_conflict="prediction_id").execute()
        print(f"G) upserted rows: {len(logs_to_insert)}")
    except Exception as e:
        print(f"G) upsert failed: {e}")
        return

    _write_state_bankroll(label, current_bankroll)

    for log in logs_to_insert[-10:]:
        print(f"✅ {log['result']:4} | {log['date']} | {log['starting_bankroll']} → {log['bankroll_after']} (pid={log['prediction_id']})")

    print(f"=== bankroll_log complete — final bankroll: {current_bankroll:.2f} ===")
