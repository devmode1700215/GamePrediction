# utils/update_bankroll_log.py
import uuid
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Any, List
from utils.supabaseClient import supabase

# ---- config ----
START_DATE = date(2025, 6, 22)
DEFAULT_BANKROLL = 100.0
ODDS_MIN, ODDS_MAX = 1.6, 2.3
CONF_MIN = 50.0
BATCH_SIZE = 500
TZ = ZoneInfo("Europe/Brussels")
EXCLUDE_DATES_LOCAL = set()  # add {date(2025, 8, 8)} if you still want to skip Aug 8

def _to_local_date(ts: str) -> date:
    # handles ...Z or +00:00
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(TZ).date()

def _seed_bankroll() -> float:
    q = (supabase.table("bankroll_log")
         .select("bankroll_after,date")
         .lt("date", START_DATE.isoformat())
         .order("date", desc=True)  # DESC
         .limit(1).execute())
    return float(q.data[0]["bankroll_after"]) if q.data else DEFAULT_BANKROLL

def _already_logged_ids() -> set:
    q = (supabase.table("bankroll_log")
         .select("prediction_id")
         .gte("date", START_DATE.isoformat())
         .execute())
    return {r["prediction_id"] for r in (q.data or []) if r.get("prediction_id")}

def _fetch_verifs_since() -> List[Dict[str, Any]]:
    start_iso = f"{START_DATE.isoformat()}T00:00:00Z"
    last_seen = None
    out: List[Dict[str, Any]] = []
    while True:
        q = (supabase.table("verifications")
             .select("prediction_id, verified_at, is_correct")
             .gte("verified_at", start_iso)
             .order("verified_at", desc=False)   # ASC (correct signature)
             .limit(BATCH_SIZE))
        if last_seen:
            q = q.gt("verified_at", last_seen)
        chunk = q.execute().data or []
        if not chunk:
            break
        out.extend(chunk)
        last_seen = chunk[-1]["verified_at"]
    return out

def update_bankroll_log():
    current_bankroll = _seed_bankroll()

    # A) verifications since start
    verifs = _fetch_verifs_since()
    print("A) verifications fetched:", len(verifs))

    # B) require prediction_id
    verifs = [v for v in verifs if v.get("prediction_id")]
    print("B) with prediction_id:", len(verifs))

    # C) exclude specific local dates if configured
    if EXCLUDE_DATES_LOCAL:
        verifs = [v for v in verifs if _to_local_date(v["verified_at"]) not in EXCLUDE_DATES_LOCAL]
    print("C) after local-date exclusion:", len(verifs))
    if not verifs:
        print("Nothing to process after date/ID filters."); return

    # D) skip already-logged (delta mode)
    already = _already_logged_ids()
    verifs = [v for v in verifs if v["prediction_id"] not in already]
    print("D) verifs after skipping already-logged:", len(verifs))
    if not verifs:
        print("Nothing new to insert."); return

    # chronological processing
    verifs.sort(key=lambda v: v["verified_at"])

    logs_to_insert: List[Dict[str, Any]] = []
    i = 0
    while i < len(verifs):
        batch = verifs[i:i + BATCH_SIZE]
        pred_ids = list({v["prediction_id"] for v in batch})

        preds = (supabase.table("value_predictions")
                 .select("id, stake_pct, odds, confidence_pct")
                 .in_("id", pred_ids)
                 .execute().data or [])
        pred_by_id = {p["id"]: p for p in preds}
        print(f"E) loaded predictions for batch {i//BATCH_SIZE+1}: {len(preds)}")

        for v in batch:
            pid = v["prediction_id"]
            p = pred_by_id.get(pid)
            if not p:
                continue

            try:
                stake_pct = float(p.get("stake_pct") or 0)
                odds = float(p.get("odds") or 0)
                conf = float(p.get("confidence_pct") or 0)
            except (TypeError, ValueError):
                continue

            # Rules — match your SQL
            if stake_pct <= 0 or odds <= 0:
                continue
            if not (ODDS_MIN <= odds <= ODDS_MAX and conf > CONF_MIN):
                continue

            is_correct = bool(v.get("is_correct"))
            stake_amount = round((stake_pct / 100.0) * current_bankroll, 2)
            profit = round(stake_amount * (odds - 1), 2) if is_correct else round(-stake_amount, 2)
            after = round(current_bankroll + profit, 2)

            d_local = _to_local_date(v["verified_at"]).isoformat()
            logs_to_insert.append({
                "id": str(uuid.uuid4()),
                "prediction_id": pid,
                "date": d_local,
                "stake_amount": stake_amount,
                "odds": round(odds, 2),
                "result": "win" if is_correct else "lose",
                "profit": profit,
                "starting_bankroll": current_bankroll,
                "bankroll_after": after,
            })

            current_bankroll = after

        i += BATCH_SIZE

    print("F) rows ready to insert:", len(logs_to_insert))
    if not logs_to_insert:
        print("No bankroll rows to insert (filters/joins)."); return

    supabase.table("bankroll_log").upsert(logs_to_insert, on_conflict="prediction_id").execute()
    for r in logs_to_insert[:10]:
        print(f"✅ {r['result']} | {r['date']} | {r['starting_bankroll']} → {r['bankroll_after']} (pid={r['prediction_id']})")
    if len(logs_to_insert) > 10:
        print(f"...and {len(logs_to_insert)-10} more.")

if __name__ == "__main__":
    update_bankroll_log()
