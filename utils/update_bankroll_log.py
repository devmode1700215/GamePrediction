import uuid
from utils.supabaseClient import supabase

def update_bankroll_log():
    # Fetch all verified predictions
    verifications = supabase.table("verifications").select("*").order("verified_at").execute().data

    # Fetch existing logs
    existing_logs = supabase.table("bankroll_log").select("*").order("date").execute().data
    logged_ids = {log["prediction_id"] for log in existing_logs}

    # Determine starting bankroll
    if existing_logs:
        last_bankroll = float(existing_logs[-1]["bankroll_after"])
    else:
        last_bankroll = 100.00  # default starting point

    bankroll = round(last_bankroll, 2)
    logs_to_insert = []

    for v in verifications:
        prediction_id = v["prediction_id"]
        if prediction_id in logged_ids:
            continue

        prediction = supabase.table("value_predictions").select("*").eq("id", prediction_id).single().execute().data

        stake_pct = float(prediction.get("stake_pct", 0))
        odds = float(prediction.get("odds") or 0)
        confidence = float(prediction.get("confidence_pct", 0)) 
        is_correct = v["is_correct"]
        result = "win" if is_correct else "lose"

        if not (1.6 <= odds <= 2.3 and confidence > 70):
            continue

        if stake_pct == 0 or odds == 0:
            continue

        stake_amount = round((stake_pct / 100) * bankroll, 2)

        if is_correct:
            profit = round(stake_amount * (odds - 1), 2)
        else:
            profit = round(-stake_amount, 2)

        log = {
            "id": str(uuid.uuid4()),
            "prediction_id": prediction_id,
            "date": v["verified_at"].split("T")[0],
            "stake_amount": stake_amount,
            "odds": round(odds, 2),
            "result": result,
            "profit": profit,
            "starting_bankroll": bankroll,
            "bankroll_after": round(bankroll + profit, 2)
        }

        bankroll = log["bankroll_after"]
        logs_to_insert.append(log)

    for log in logs_to_insert:
        supabase.table("bankroll_log").insert(log).execute()
        print(f"✅ {log['result']} | bankroll: {log['starting_bankroll']} → {log['bankroll_after']}")
