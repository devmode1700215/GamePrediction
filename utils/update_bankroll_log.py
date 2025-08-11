import uuid
from datetime import date, datetime
from zoneinfo import ZoneInfo
from utils.supabaseClient import supabase

START_DATE = date(2025, 6, 22)      # backfill start
EXCLUDE_LOCAL_DATES = {date(2025, 8, 8)}  # local (Brussels) calendar date(s) to skip
DEFAULT_BANKROLL = 100.00
TZ = ZoneInfo("Europe/Brussels")
BATCH_SIZE = 500  # tune as needed

def parse_iso(ts: str) -> datetime:
    # robust ISO parser; Supabase returns ISO8601 strings
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def local_calendar_date(ts: str) -> date:
    return parse_iso(ts).astimezone(TZ).date()

def fetch_verifications_keyset(start_dt_iso: str):
    """
    Keyset pagination by verified_at ASC:
    repeatedly fetch rows where verified_at > last_seen, ordered ASC, limited by BATCH_SIZE.
    """
    last_seen = None
    while True:
        q = (
            supabase.table("verifications")
            .select("prediction_id, verified_at, is_correct")
            .gte("verified_at", start_dt_iso)
            .order("verified_at", asc=True)
            .limit(BATCH_SIZE)
        )
        if last_seen:
            q = q.gt("verified_at", last_seen)  # keyset step

        chunk = q.execute().data or []
        if not chunk:
            break

        for row in chunk:
            yield row

        # advance key
        last_seen = chunk[-1]["verified_at"]

def update_bankroll_log():
    # 1) Determine starting bankroll from the last row BEFORE START_DATE (if present)
    #    If you don't store a 'date' column, switch this filter to created_at.
    prev_q = (
        supabase.table("bankroll_log")
        .select("bankroll_after, date")
        .lt("date", START_DATE.isoformat())
        .order("date", desc=True)
        .limit(1)
        .execute()
    )
    current_bankroll = float(prev_q.data[0]["bankroll_after"]) if prev_q.data else DEFAULT_BANKROLL

    # 2) Build a set of already-logged prediction_ids from START_DATE onward (delta mode)
    existing_q = (
        supabase.table("bankroll_log")
        .select("prediction_id")
        .gte("date", START_DATE.isoformat())
        .execute()
    )
    already = {r["prediction_id"] for r in (existing_q.data or []) if r.get("prediction_id")}

    # 3) Stream verifications from START_DATE (UTC midnight) using keyset pagination
    start_iso = f"{START_DATE.isoformat()}T00:00:00Z"

    # 4) Buffer verifications we’ll actually process (date filter + not already logged)
    to_process = []
    for v in fetch_verifications_keyset(start_iso):
        pid = v.get("prediction_id")
        if not pid or pid in already:
            continue

        # Exclude Aug 8 by local (Brussels) calendar date
        if local_calendar_date(v["verified_at"]) in EXCLUDE_LOCAL_DATES:
            continue

        to_process.append(v)

    if not to_process:
        print("Nothing to process after pagination/date filters.")
        return

    # 5) Load all predictions in one go
    pred_ids = list({v["prediction_id"] for v in to_process})
    preds_q = (
        supabase.table("value_predictions")
        .select("id, stake_pct, odds, confidence_pct")
        .in_("id", pred_ids)
        .execute()
    )
    pred_by_id = {p["id"]: p for p in (preds_q.data or [])}

    # 6) Sort chronologically (ASC) and compute bankroll forward
    to_process.sort(key=lambda r: r["verified_at"])
    logs = []

    for v in to_process:
        pid = v["prediction_id"]
        pred = pred_by_id.get(pid)
        if not pred:
            continue

        try:
            stake_pct = float(pred.get("stake_pct") or 0)
            odds = float(pred.get("odds") or 0)
            confidence = float(pred.get("confidence_pct") or 0)
        except (TypeError, ValueError):
            continue

        # Keep your filters (remove if you want everything)
        if not (1.6 <= odds <= 2.3 and confidence > 50):
            continue
        if stake_pct <= 0 or odds <= 0:
            continue

        # Use local date for the log's date column
        d_local = local_calendar_date(v["verified_at"])
        if d_local in EXCLUDE_LOCAL_DATES or d_local < START_DATE:
            continue

        is_correct = bool(v.get("is_correct"))
        result = "win" if is_correct else "lose"

        stake_amount = round((stake_pct / 100.0) * current_bankroll, 2)
        profit = round(stake_amount * (odds - 1), 2) if is_correct else round(-stake_amount, 2)
        after = round(current_bankroll + profit, 2)

        logs.append({
            "id": str(uuid.uuid4()),
            "prediction_id": pid,
            "date": d_local.isoformat(),
            "stake_amount": stake_amount,
            "odds": round(odds, 2),
            "result": result,
            "profit": profit,
            "starting_bankroll": current_bankroll,
            "bankroll_after": after,
        })

        current_bankroll = after

    if not logs:
        print("All verifications were filtered out (rules/exclusions).")
        return

    supabase.table("bankroll_log").upsert(logs, on_conflict="prediction_id").execute()

    for log in logs:
        print(f"✅ {log['result']} | {log['date']} | bankroll: {log['starting_bankroll']} → {log['bankroll_after']} (pid={log['prediction_id']})")
