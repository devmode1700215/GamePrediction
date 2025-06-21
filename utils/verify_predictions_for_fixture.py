from datetime import datetime
from utils.supabaseClient import supabase

def verify_predictions_for_fixture(fixture_id):
    # Fetch related predictions
    preds_resp = supabase.table("value_predictions").select("*").eq("fixture_id", fixture_id).execute()
    predictions = preds_resp.data

    # Fetch result
    result_resp = supabase.table("results").select("*").eq("fixture_id", fixture_id).single().execute()
    result = result_resp.data

    for p in predictions:
        predicted = p["prediction"]
        market = p["market"]

        actual = None
        if market == "winner":
            actual = result["result_1x2"]
        elif market == "btts":
            actual = result["result_btts"]
        elif market == "over_2_5":
            actual = result["result_ou"]

        is_correct = (predicted == actual)

        verification_entry = {
            "prediction_id": p["id"],
            "is_correct": is_correct,
            "verified_at": datetime.utcnow().isoformat()
        }

        supabase.table("verifications").upsert(verification_entry).execute()
        print(f"✅ Verified prediction {p['id']} → {'✔️' if is_correct else '❌'}")

