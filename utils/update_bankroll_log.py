from datetime import datetime, timezone
from utils.supabaseClient import supabase

# Config: these should match your bankroll rules
MIN_ODDS = 1.50
MAX_ODDS = 3.50
MIN_CONFIDENCE = 0.0   # adjust if needed
MIN_STAKE = 1.0

def update_bankroll_log():
    print("📊 Fetching predictions eligible for bankroll logging...")

    # Step 1: Fetch predictions meeting rules
    predictions = (
        supabase.table("verifications")
        .select(
            "prediction_id, is_correct, verified_at, "
            "value_predictions(odds,confidence,stake)"
        )
        .gte("value_predictions.odds", MIN_ODDS)
        .lte("value_predictions.odds", MAX_ODDS)
        .gte("value_predictions.confidence", MIN_CONFIDENCE)
        .gte("value_predictions.stake", MIN_STAKE)
        .execute()
        .data
    )

    if not predictions:
        print("⚠️ No predictions found meeting bankroll rules.")
        return

    print(f"✅ Found {len(predictions)} eligible predictions before log check.")

    # Step 2: Fetch existing bankroll log IDs
    existing_logs = (
        supabase.table("bankroll_log")
        .select("prediction_id")
        .execute()
        .data
    )
    existing_ids = {row["prediction_id"] for row in existing_logs}

    # Step 3: Filter out already logged predictions
    to_log = [p for p in predictions if p["prediction_id"] not in existing_ids]
    print(f"🆕 {len(to_log)} predictions will be inserted into bankroll_log.")

    if not to_log:
        print("✅ Nothing new to insert.")
        return

    # Step 4: Prepare payload
    now_iso = datetime.now(timezone.utc).isoformat()
    payload = []
    for p in to_log:
        vp = p["value_predictions"]
        payload.append({
            "prediction_id": p["prediction_id"],
            "odds": vp["odds"],
            "confidence": vp["confidence"],
            "stake": vp["stake"],
            "is_correct": p["is_correct"],
            "logged_at": now_iso
        })

    # Step 5: Insert into bankroll_log
    res = supabase.table("bankroll_log").upsert(payload).execute()
    print(f"💾 Inserted {len(payload)} new bankroll log entries.")
    print("🔍 First few inserted:", payload[:5])


if __name__ == "__main__":
    update_bankroll_log()
