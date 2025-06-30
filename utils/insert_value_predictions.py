from utils.supabaseClient import supabase

def insert_value_predictions(prediction_data):

    fixture_id = prediction_data.get("fixture_id")
    predictions = prediction_data.get("predictions", {})

    for market, details in predictions.items():
        odds = details.get("odds")
        if odds is not None and not (1.6 <= odds <= 2.3):
            print(f"⚠️ ODDS OUT OF RANGE: {fixture_id} | {market} | {odds}")
            continue

        if details.get("po_value") is True and odds is not None and 1.6 <= odds <= 2.3:
            entry = {
                "fixture_id": fixture_id,
                "market": market,
                "prediction": details.get("prediction"),
                "confidence_pct": details.get("confidence"),
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
