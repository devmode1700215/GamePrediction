import uuid
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo
from utils.supabaseClient import supabase

# Config
START_DATE = date(2025, 6, 22)                 # backfill start
EXCLUDE_DATES_LOCAL = {date(2025, 8, 8)}       # skip these local dates entirely
DEFAULT_BANKROLL = 100.0
ODDS_MIN, ODDS_MAX = 1.6, 2.3                  # loosen/remove if too strict
CONF_MIN = 50.0
TZ = ZoneInfo("Europe/Brussels")

def _to_local_date(ts: str) -> date:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(TZ).date()

def update_bankroll_log():
    # 0) starting bankroll = last row BEFORE START_DATE, else default
    prev = (supabase.table("bankroll_log")
            .select("bankroll_after,date")
            .lt("date", START_DATE.isoformat())
            .order("date", desc=True)
            .limit(1).execute()).data
    current_bankroll = float(prev[0]["bankroll_after"]) if prev else DEFAULT_BANKROLL

    # 1) pull verifications since START_DATE (ASC)
    vq = (supabase.table("verifications")
          .select("prediction_id, verified_at, is_correct")
          .gte("verified_at", f"{START_DATE.isoformat()}T00:00:00Z")
          .order("verified_at", desc=False)   # ASC
          .execute())
    verifs = [v for v in (vq.data or []) if v.get("prediction_id")]
    if not verifs:
        print("No verifications to process."); return

    # 2) exclude specific local dates (e.g., Aug 8 Brussels)
    verifs = [v for v in verifs if _to_local_date(v["verified_at"]) not in EXCLUDE_DATES_LOCAL]
    if not verifs:
        print("All verifications excluded by local date."); return

    # 3) load needed predictions in batch
    pred_ids = list({v["prediction_id"] for v in verifs})
    preds = (supabase.table("value_predictions")
             .select("id, stake_pct, odds, confidence_pct")
             .in_("id", pred_ids).execute()).data or []
    pred_by_id = {p["id"]: p for p in preds}

    # 4) skip ones already logged since START_DATE (delta mode)
    existing = (supabase.table("bankroll_log")
                .select("prediction_id")
                .gte("date", START_DATE.isoformat()).execute()).data or []
    already = {r["prediction_id"] for r in existing if r.get("prediction_id")}

    # 5) compute chronologically
    verifs.sort(key=lambda v: v["verified_at"])
    rows = []

    for v in verifs:
        pid = v["prediction_id"]
        if pid in already:
            continue
        p = pred_by_id.get(pid)
        if not p:
            continue

        try:
            stake_pct = float(p.get("stake_pct") or 0)
            odds = float(p.get("odds") or 0)
            conf = float(p.get("confidence_pct") or 0)
        except (TypeError, ValueError):
            continue

        # filters — remove if you want to include everything
        if not (ODDS_MIN <= odds <= ODDS_MAX and conf > CONF_MIN):
            continue
        if stake_pct <= 0 or odds <= 0:
            continue

        is_correct = bool(v.get("is_correct"))
        stake_amount = round((stake_pct / 100.0) * current_bankroll, 2)
        profit = round(stake_amount * (odds - 1), 2) if is_correct else round(-stake_amount, 2)
        after = round(current_bankroll + profit, 2)

        d_local = _to_local_date(v["verified_at"]).isoformat()
        rows.append({
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

    if not rows:
        print("No bankroll rows to insert (filters/duplicates)."); return

    supabase.table("bankroll_log").upsert(rows, on_conflict="prediction_id").execute()
    for r in rows:
        print(f"✅ {r['result']} | {r['date']} | {r['starting_bankroll']} → {r['bankroll_after']} (pid={r['prediction_id']})")
