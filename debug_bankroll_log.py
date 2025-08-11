# debug_bankroll_log.py
import os, sys, traceback
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

# --- make sure we can import utils.supabaseClient regardless of where we run from ---
ROOT = os.path.abspath(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

try:
    from utils.supabaseClient import supabase
except Exception as e:
    print("Import error: cannot import utils.supabaseClient")
    traceback.print_exc()
    sys.exit(1)

START_DATE = date(2025, 6, 22)
TZ = ZoneInfo("Europe/Brussels")
EXCLUDE_DATES_LOCAL = {date(2025, 8, 8)}  # set() to disable

def _to_local_date(ts: str):
    # robust ISO parse; handles ...Z and +00:00
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(TZ).date()

def debug_bankroll_run():
    # A) verifications since START_DATE (ASC)
    vq = (supabase.table("verifications")
          .select("prediction_id, verified_at, is_correct")
          .gte("verified_at", f"{START_DATE.isoformat()}T00:00:00Z")
          .order("verified_at", desc=False)   # ASC (no 'asc=' kw)
          .execute())
    verifs = vq.data or []
    print("A) verifications fetched:", len(verifs), flush=True)

    # B) with prediction_id
    verifs = [v for v in verifs if v.get("prediction_id")]
    print("B) with prediction_id:", len(verifs), flush=True)

    # C) exclude Aug 8 local
    verifs2 = [v for v in verifs if _to_local_date(v["verified_at"]) not in EXCLUDE_DATES_LOCAL]
    print("C) after Aug 8 exclusion:", len(verifs2), flush=True)

    # D) join predictions
    pred_ids = list({v["prediction_id"] for v in verifs2})
    preds = (supabase.table("value_predictions")
             .select("id, stake_pct, odds, confidence_pct")
             .in_("id", pred_ids).execute()).data or []
    print("D) predictions loaded:", len(preds), flush=True)
    pred_by_id = {p["id"]: p for p in preds}

    # E) apply stake/odds/conf filters
    kept = 0
    for v in verifs2:
        p = pred_by_id.get(v["prediction_id"])
        if not p:
            continue
        try:
            stake_pct = float(p.get("stake_pct") or 0)
            odds = float(p.get("odds") or 0)
            conf = float(p.get("confidence_pct") or 0)
        except Exception:
            continue
        if stake_pct <= 0 or odds <= 0:
            continue
        if not (1.6 <= odds <= 2.3 and conf > 50):
            continue
        kept += 1
    print("E) after odds/conf/stake filters:", kept, flush=True)

    # F) skip already-logged IDs
    logged = (supabase.table("bankroll_log")
              .select("prediction_id")
              .gte("date", START_DATE.isoformat()).execute()).data or []
    already = {r["prediction_id"] for r in logged if r.get("prediction_id")}
    remaining = [v for v in verifs2 if v["prediction_id"] not in already]
    print("F) after skipping already-logged IDs:", len(remaining), flush=True)

    if remaining:
        pid = remaining[0]["prediction_id"]
        p = pred_by_id.get(pid, {})
        print("Sample:", {
            "prediction_id": pid,
            "verified_at": remaining[0]["verified_at"],
            "stake_pct": p.get("stake_pct"),
            "odds": p.get("odds"),
            "confidence_pct": p.get("confidence_pct"),
            "local_date": _to_local_date(remaining[0]["verified_at"]).isoformat(),
        }, flush=True)

if __name__ == "__main__":
    try:
        print("▶ Running debug_bankroll_run()", flush=True)
        debug_bankroll_run()
        print("✔ Done.", flush=True)
    except Exception:
        print("✖ Exception while running debug_bankroll_run():", flush=True)
        traceback.print_exc()
        sys.exit(1)
