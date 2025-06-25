import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from datetime import datetime
from utils.supabaseClient import supabase


def get_matches_needing_results():
    now_iso = datetime.utcnow().isoformat()
    
    # Fetch all matches in chunks
    all_matches = []
    offset = 0
    limit = 500
    
    while True:
        response = supabase.table("matches").select("*").lt("date", now_iso).range(offset, offset + limit - 1).execute()
        chunk_matches = response.data
        
        if not chunk_matches:  # No more data
            break
            
        all_matches.extend(chunk_matches)
        
        # If we got less than the limit, we've reached the end
        if len(chunk_matches) < limit:
            break
            
        offset += limit

    print(f"Total matches fetched: {len(all_matches)}")

    # Filter out ones that already exist in results (with pagination)
    all_results = []
    offset = 0
    limit = 500
    
    while True:
        response = supabase.table("results").select("fixture_id").range(offset, offset + limit - 1).execute()
        chunk_results = response.data
        
        if not chunk_results:  # No more data
            break
            
        all_results.extend(chunk_results)
        
        # If we got less than the limit, we've reached the end
        if len(chunk_results) < limit:
            break
            
        offset += limit

    print(f"Total results fetched: {len(all_results)}")
    completed_ids = {r["fixture_id"] for r in all_results}

    filtered_matches = [m for m in all_matches if m["fixture_id"] not in completed_ids]
    print(f"Matches needing results: {len(filtered_matches)}")
    
    return filtered_matches

if __name__ == "__main__":
    matches = get_matches_needing_results()
    print(len(matches))