from datetime import datetime, timedelta, timezone
from utils.supabaseClient import supabase

# Tunables
WINDOW_HOURS = 36          # 24h + buffer for late results
BATCH_SIZE = 500

def get_matches_needing_results():
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=WINDOW_HOURS)
    start_iso = window_start.isoformat(timespec="seconds").replace("+00:00", "Z")
    end_iso = now.isoformat(timespec="seconds").replace("+00:00", "Z")

    # keyset over 'date' ASC within the window
    last_seen = None
    needing = []

    while True:
        q = (
            supabase.table("matches")
            .select("fixture_id,date")          # only what you need
            .gte("date", start_iso)
            .lt("date", end_iso)
            .order("date", desc=False)                 # ASC (correct signature)
            .limit(BATCH_SIZE)
        )
        if last_seen:
            q = q.gt("date", last_seen)               # keyset step

        chunk = q.execute().data or []
        if not chunk:
            break

        # collect fixture_ids
        ids = [m["fixture_id"] for m in chunk if m.get("fixture_id")]
        if not ids:
            last_seen = chunk[-1]["date"]
            continue

        # fetch only results for this batch of fixtures
        res = (
            supabase.table("results")
            .select("fixture_id")
            .in_("fixture_id", ids)
            .execute()
            .data
            or []
        )
        done_ids = {r["fixture_id"] for r in res}

        # keep matches that don't have results yet
        for m in chunk:
            if m["fixture_id"] not in done_ids:
                needing.append(m)

        last_seen = chunk[-1]["date"]

    return needing

if __name__ == "__main__":
    missing = get_matches_needing_results()
    print(f"Matches needing results in last {WINDOW_HOURS}h: {len(missing)}")
