import uuid
from utils.supabaseClient import supabase

def update_bankroll_log():
    # --- 1) Get last known bankroll safely (latest inserted row) ---
    # Prefer a created_at / inserted_at desc if available; fallback to date desc.
    # Change "created_at" to your actual timestamp column if different.
    last_row_q = supabase.table("bankroll_log") \
        .select("bankroll_after") \
        .order("created_at", desc=True) \
        .limit(1) \
        .execute()
    if last_row_q.data:
        bankroll = float(last_row_q.data[0]["bankroll_after"])
    else:
        bankroll = 100.00  # default starting point

    # --- 2) Get already-logged prediction_ids to skip local work quickly (optional optimization) ---
    logged_ids_q = supabase.table("bankroll_log") \
        .select("prediction_id") \
        .execute()
    logged_ids = {row["prediction_id"] for row in logged_ids_q.data if row.get("prediction_id")}

    # --- 3) Fetch verifications to process, newest first then we'll sort ascending for bankroll math ---
    verifications_q = supabase.table("verifications") \
        .select("prediction_id, verified_at, is_correct") \
        .order("verified_at", desc=True) \
        .limit(500) \
        .execute()
    verifications_raw = verifications_q.data or []
    print(f"=== Found {len(verifications_raw)} verifications to consider.")

    # --- 4) Filter out ones we already logged; keep only with a prediction_id ---
    verifications = [
        v for v in verifications_raw
        if v.get("prediction_id") and v["prediction_id"] not in logged_ids
    ]

    if not verifications:
        print("Nothing new to process.")
        return

    # --- 5) Batch-load predictions to avoid N queries ---
    pred_ids = list({v["prediction_id"] for v in verifications})
    preds_q = supabase.table("value_predictions") \
        .select("id, stake_pct, odds, confidence_pct") \
        .in_("id", pred_ids) \
        .execute()
    pred_by_id = {p["id"]: p for p in (preds_q.data or [])}

    # --- 6) Sort by verified_at ASC to compute bankroll chronologically ---
    verifications.sort(key=lambda v: v["verified_at"])

    logs_to_insert = []
    current_bankroll = round(bankroll, 2)

    for v in verifications:
        pid = v["prediction_id"]
        pred = pred_by_id.get(pid)
        if not pred:
            # No matching prediction; skip safely
            continue

        # Pull fields with safe defaults
        try:
            stake_pct = float(pred.get("stake_pct", 0) or 0)
            odds = float(pred.get("odds", 0) or 0)
            confidence = float(pred.get("confidence_pct", 0) or 0)
        except (TypeError, ValueError):
            # Bad data; skip
            continue

        # Keep only the bets you care about
        if not (1.6 <= odds <= 2.3 and confidence > 50):
            continue
        if stake_pct <= 0 or odds <= 0:
            continue

        is_correct = bool(v.get("is_correct"))
        result = "win" if is_correct else "lose"

        stake_amount = round((stake_pct / 100.0) * current_bankroll, 2)
        profit = round(stake_amount * (odds - 1), 2) if is_correct else round(-stake_amount, 2)

        after = round(current_bankroll + profit, 2)

        logs_to_insert.append({
            "id": str(uuid.uuid4()),
            "prediction_id": pid,
            "date": v["verified_at"].split("T")[0],  # keep as YYYY-MM-DD
            "stake_amount": stake_amount,
            "odds": round(odds, 2),
            "result": result,
            "profit": profit,
            "starting_bankroll": current_bankroll,
            "bankroll_after": after,
        })

        current_bankroll = after

    if not logs_to_insert:
        print("All new verifications were filtered out by rules.")
        return

    # --- 7) Bulk UPSERT to avoid duplicates (requires a unique index on prediction_id) ---
    # If a row with the same prediction_id exists, it will be ignored/merged depending on your Postgres settings.
    upsert_res = supabase.table("bankroll_log") \
        .upsert(logs_to_insert, on_conflict="prediction_id") \
        .execute()

    # --- 8) Console feedback (show what we attempted to write) ---
    for log in logs_to_insert:
        print(f"✅ {log['result']} | {log['date']} | bankroll: {log['starting_bankroll']} → {log['bankroll_after']} (pid={log['prediction_id']})")
