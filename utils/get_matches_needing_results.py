from datetime import datetime
from utils.supabaseClient import supabase



def get_matches_needing_results():
    now_iso = datetime.utcnow().isoformat()
    response = supabase.table("matches").select("*").lt("date", now_iso).execute()
    all_matches = response.data

    # Filter out ones that already exist in results
    result_ids = supabase.table("results").select("fixture_id").execute()
    completed_ids = {r["fixture_id"] for r in result_ids.data}

    return [m for m in all_matches if m["fixture_id"] not in completed_ids]
