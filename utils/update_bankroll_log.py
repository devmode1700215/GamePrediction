# update_bankroll_log.py
import uuid
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Any, List

from utils.supabaseClient import supabase

# -------------------- Config --------------------
START_DATE = date(2025, 6, 22)                 # backfill start
EXCLUDE_DATES_LOCAL = {date(2025, 8, 8)}       # skip these local dates entirely
DEFAULT_BANKROLL = 100.0
BATCH_SIZE = 500                                # verifications page size
TZ = ZoneInfo("Europe/Brussels")

# Optional filters (loosen/remove if too strict)
ODDS_MIN, ODDS_MAX = 1.6, 2.3
CONF_MIN = 50.0

# -------------------- Helpers --------------------
def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _to_local_date(ts: str) -> date:
    # robust ISO8601 parse (handles trailing Z)
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(TZ).date()

def _seed_bankroll() -> float:
    """Get last bankroll BEFORE START_DATE, else default."""
    q = (
        supabase.table("bankroll_log")
        .select("bankroll_after,date")
        .lt("date", START_DATE.isoformat())
        .order("date", desc=True)  # DESC
        .limit(1)
        .execute()
    )
    return float(q.data[0]["bankroll_after"]) if q.data else DEFAULT_BANKROLL

def _existing_logged_ids() -> set:
    """Prediction IDs already logged since START_DATE (delta mode)."""
    q = (
        supabase.table("bankroll_log")
        .select("prediction_id")
        .gte("date", START_DATE.isoformat())
        .execute()
    )
    return {r["prediction_id"] for r in (q.data or []) if r.get("prediction_id")}

def _fetch_verifications_since() -> List[Dict[str, Any]]:
    """Keyset-pagination from START_DATE UTC, ASC by verified_at."""
    start_iso = f"{START_DATE.isoformat()}T00:00:00Z"
    last_seen = None
    out: List[Dict[str, Any]] = []

    while True:
        q = (
            supabase.table("verifications")
            .select("prediction_id, verified_at, is_correct")
            .gte("verified_at", start_iso)
            .order("verified_at", desc=False)   # ASC
            .limit(BATCH_SIZE)
        )
        if last_seen:
            q = q.gt("verified_at", last_seen)

        chunk = q.execute().data or []
        if not chunk:
            break

        out.extend(chunk)
        last_seen = chunk[-1]["verified_at"]

    return out

# -------------------- Main updater --------------------
def update_bankroll_log():
    current_bankroll = _seed_bankroll()

    # Pull ALL verifications since START_DATE via keyset
    verifs = _fetch_verifications_since()
    if not verifs:
        print("No verifications to process.")
        return

    # Filter: must have prediction_id and not on excluded local dates
    verifs = [
        v for v in verifs
        if v.get("prediction_id") and _to_local_date(v["verified_at"]) not in EXCLUDE_DATES_LOCAL
    ]
    if not verifs:
        print("All verifications excluded by date/ID.")
        return

    # Skip IDs already logged (delta mode)
    already = _existing_logged_ids()
    verifs = [v for v in verifs if v["prediction_id"] not in already]
    if not verifs:
        print("Nothing new (all prediction_ids already logged).")
        return

    # Chronological order (verified_at ASC)
    verifs.sort(key=lambda v: v["verified_at"])

    # Load predictions for the remaining verifs in batches to keep memory/network in check
    logs_to_insert: List[Dict[str, Any]] = []
    i = 0
    while i < len(verifs):
        batch = verifs[i:i + BATCH_SIZE]
        pred_ids = list({v["prediction_id"] for v in batch})

        preds = (
            supabase.table("value_predictions")
            .select("id, stake_pct, odds, confidence_pct")
            .in_("id", pred_ids)
            .execute()
            .data or []
        )
        pred_by_id = {p["id"]: p for p in preds}

        for v in batch:
            pid = v["prediction_id"]
            p = pred_by_id.get(pid)
            if not p:
                continue

            # Parse numeric fields safely
            try:
                stake_pct = float(p.get("stake_pct") or 0)
                odds = float(p.get("odds") or 0)
                conf = float(p.get("confidence_pct") or 0)
            except (TypeError, ValueError):
                continue

            # Apply business filters (remove if you want every bet)
            if not (ODDS_MIN <= odds <= ODDS_MAX and conf > CONF_MIN):
                continue
            if stake_pct <= 0 or odds <= 0:
                continue

            is_correct = bool(v.get("is_correct"))

            stake_amount = round((stake_pct / 100.0) * current_bankroll, 2)
            profit = round(stake_amount * (odds - 1), 2) if is_correct else round(-stake_amount, 2)
            after = round(current_bankroll + profit, 2)

            d_local = _to_local_date(v["verified_at"]).isoformat()
            logs_to_insert.append({
                "id": str(uuid.uuid4()),
                "prediction_id": pid,
                "date": d_local,
                "stake_amount": stake_amount,
                "odds": round(odds, 2),
                "result": "win" if is_correct else "lose",
                "profit": profit,
                "starting_bankroll": current_bankroll,
                "bankroll_after": after,
            })

            current_bankroll = after

        i += BATCH_SIZE

    if not logs_to_insert:
        print("No bankroll rows to insert (filters/duplicates).")
        return

    # Single bulk upsert (requires UNIQUE INDEX on bankroll_log(prediction_id))
    supabase.table("bankroll_log").upsert(logs_to_insert, on_conflict="prediction_id").execute()

    for r in logs_to_insert:
        print(f"✅ {r['result']} | {r['date']} | {r['starting_bankroll']} → {r['bankroll_after']} (pid={r['prediction_id']})")

# Optional: allow running this module directly
if __name__ == "__main__":
    update_bankroll_log()
