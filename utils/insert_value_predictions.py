from utils.supabaseClient import supabase

# Minimum confidence threshold for value predictions
CONF_MIN = 70.0  # percent

def insert_value_predictions(prediction_data):
    fixture_id = prediction_data.get("fixture_id")
    predictions = prediction_data.get("predictions", {})

    for market, details in predictions.items():
        odds = details.get("odds")
        confidence = details.get("confidence")

        # Skip if odds not in range
        if odds is None or not (1.6 <= odds <= 2.3):
            print(f"⚠️ ODDS OUT OF RANGE: {fixture_id} | {market} | {odds}")
            continue

        # ✅ Skip if confidence too low
        if confidence is None or confidence < CONF_MIN:
            print(f"⚠️ CONFIDENCE TOO LOW: {fixture_id} | {market} | {confidence}")
            continue

        # Only insert if marked as value and passes both odds/conf filters
        if details.get("po_value") is True:
            entry = {
                "fixture_id": fixture_id,
                "market": market,
                "prediction": details.get("prediction"),
                "confidence_pct": confidence,
                "po_value": True,
                "stake_pct": details.get("bankroll_pct"),
                "edge": details.get("edge"),
                "odds": odds,
                "rationale": details.get("rationale")
            }
            try:
                supabase.table("value_predictions").insert(entry).execute()
                print(f"✅ Inserted value prediction: {fixture_id} | {market}")
            except Exception as e:
                print(f"❌ Failed to insert value prediction for {fixture_id} | {market}: {e}")
