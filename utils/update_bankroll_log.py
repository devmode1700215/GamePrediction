import uuid
from datetime import date, datetime
from zoneinfo import ZoneInfo
from typing import Dict, Any, List
from utils.supabaseClient import supabase

# ---- config ----
START_DATE = date(2025, 6, 22)      # historical start of your system
DEFAULT_BANKROLL = 100.0
ODDS_MIN, ODDS_MAX = 1.6, 2.3
CONF_MIN = 70.0
BATCH_SIZE = 500
TZ = ZoneInfo("Europe/Brussels")

# dates to remove completely
DELETE_DATES_LOCAL = {
    date(2025, 8, 5),
    date(2025, 8, 6),
    date(2025, 8, 7),
    date(2025, 8, 8),
    date(2025, 8, 9),
}

def _to_local_date(ts: str) -> date:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(TZ).date()

def _delete_target_dates():
    """Delete bankroll rows matching DELETE_DATES_LOCAL."""
    for d in DELETE_DATES_LOCAL:
        supabase.table("bankroll_log").delete().eq("date", d.isoformat()).execute()

def _seed_bankroll_after_last_kept() -> float:
    """Get bankroll_after from the last kept date before first DELETE_DATES_LOCAL."""
    first_deleted = min(DELETE_DATES_LOCAL)
    q = (
        supabase.table("bankroll_log")
        .select("bankroll_after,date")
        .lt("date", first_deleted.isoformat())
        .order("date", desc=True)
        .limit(1)
        .execute()
    )
    return float(q.data[0]["bankroll_after"]) if q.data else DEFAULT_BANKROLL

def _fetch_verifs_after_last_deleted() -> List[Dict[str, Any]]:
    """Get verifications strictly after the last deleted date."""
    after_date = max(DELETE_DATES_LOCAL)
    start_iso = f"{after_date.isoformat()}T00:00:00Z"
    last_seen = None
    out: List[Dict[str, Any]] = []
    while True:
        q = (
            supabase.table("verifications")
            .select("prediction_id, verified_at, is_correct")
            .gte("verified_at", start_iso)
            .order("verified_at", desc=False)
            .limit(BATCH_SIZE)
        )
        if last_seen:
            q = q.gt("verified_at", last_seen)
        chunk = q.execute().data or []
        if not chunk:
            break
        out.extend(chunk)
        last_seen = chunk[-1]["verified_at"]
    return out

def update_bankroll_log():
    # 1) delete the unwanted match dates
    _delete_target_dates()

    # 2) seed from last kept bankroll_before
    current_bankroll = _seed_bankroll_after_last_kept()

    # 3) fetch verifications strictly after Aug 9
    verifs = _fetch_verifs_after_last_deleted()
    print("A) verifications fetched:", len(verifs))

    # 4) require prediction_id
    verifs = [v for v in verifs if v.get("prediction_id")]
    print("B) with prediction_id:", len(verifs))
    if not verifs:
        print("No verifications to process after deletion.")
        return

    # 5) chronological order
    verifs.sort(key=lambda v: v["verified_at"])

    logs_to_insert: List[Dict[str, Any]] = []
    i = 0
    while i < len(verifs):
        batch = verifs[i:i + BATCH_SIZE]
        pred_ids = list({v["prediction_id"] for v in batch})

        preds = (
            supabase.table("value_predictions")
            .select("id, stake_pct, odds, confidence_pct")
            .in_("id", pred_ids)
            .execute().data or []
        )
        pred_by_id = {p["id"]: p for p in preds}
        print(f"C) loaded predictions for batch {i//BATCH_SIZE+1}: {len(preds)}")

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

            if stake_pct <= 0 or odds <= 0:
                continue
            if not (ODDS_MIN <= odds <= ODDS_MAX):
                continue
            if conf < CONF_MIN:
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

    print("D) rows ready to insert:", len(logs_to_insert))
    if not logs_to_insert:
        print("No bankroll rows to insert.")
        return

    supabase.table("bankroll_log").insert(logs_to_insert).execute()

    for r in logs_to_insert[:10]:
        print(f"✅ {r['result']} | {r['date']} | {r['starting_bankroll']} → {r['bankroll_after']} (pid={r['prediction_id']})")
    if len(logs_to_insert) > 10:
        print(f"...and {len(logs_to_insert)-10} more.")

if __name__ == "__main__":
    update_bankroll_log()
