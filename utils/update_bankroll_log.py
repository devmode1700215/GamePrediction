# utils/update_bankroll_log.py
import uuid
from datetime import datetime, timezone
from utils.supabaseClient import supabase

# --- Settings ---------------------------------------------------------------
START_DATE = "2025-06-22"               # inclusive
EXCLUDE_DATES = {"2025-08-08"}          # YYYY-MM-DD to skip entirely
ODDS_MIN, ODDS_MAX = 1.6, 2.3
CONF_MIN = 70.0
DEFAULT_BANKROLL = 100.00
BATCH_SIZE = 1000

def _to_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def update_bankroll_log():
    print("=== bankroll_log updater starting ===")

    # A) Last bankroll
    try:
        last_row_q = (
            supabase.table("bankroll_log")
            .select("bankroll_after, created_at")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if last_row_q.data:
            bankroll = _to_float(last_row_q.data[0]["bankroll_after"]) or DEFAULT_BANKROLL
        else:
            bankroll = DEFAULT_BANKROLL
        print(f"A) starting bankroll: {bankroll}")
    except Exception as e:
        print(f"A) failed to fetch last bankroll, using default {DEFAULT_BANKROLL}: {e}")
        bankroll = DEFAULT_BANKROLL

    # B) Already logged prediction_ids
    try:
        logged_ids_q = supabase.table("bankroll_log").select("prediction_id").execute()
        logged_ids = {r["prediction_id"] for r in (logged_ids_q.data or []) if r.get("prediction_id")}
        print(f"B) already logged prediction_ids: {len(logged_ids)}")
    except Exception as e:
        print(f"B) failed to load existing logs: {e}")
        logged_ids = set()

    # C) Load verifications in window (≥ START_DATE), newest first; we’ll sort asc later
    try:
        verifs_q = (
            supabase.table("verifications")
            .select("prediction_id, verified_at, is_correct")
            .gte("verified_at", f"{START_DATE}T00:00:00Z")
            .order("verified_at", desc=True)
            .limit(5000)  # adjust if needed; we also support batching below
            .execute()
        )
        verifs_raw = verifs_q.data or []
        print(f"C) verifications fetched (since {START_DATE}): {len(verifs_raw)}")
    except Exception as e:
        print(f"C) failed to fetch verifications: {e}")
        verifs_raw = []

    # D) Filter: has prediction_id, not already logged, not in EXCLUDE_DATES
    verifs = []
    excluded_aug8 = 0
    for v in verifs_raw:
        pid = v.get("prediction_id")
        ts = v.get("verified_at")
        if not pid or not ts:
            continue
        date_only = ts.split("T", 1)[0]
        if date_only in EXCLUDE_DATES:
            excluded_aug8 += 1
            continue
        if pid in logged_ids:
            continue
        verifs.append(v)

    print(f"D) after removing duplicates & excluded dates: {len(verifs)} (excluded {excluded_aug8} on excluded dates)")

    if not verifs:
        print("Nothing new to process.")
        return

    # E) Batch-load corresponding value_predictions with strict filters
    #    po_value TRUE, conf >= 70, odds in [1.6, 2.3], stake_pct > 0
    #    Build a map for quick lookups
    pred_ids = list({v["prediction_id"] for v in verifs})
    print(f"E) unique prediction_ids to load: {len(pred_ids)}")

    pred_by_id = {}
    total_loaded = 0

    # chunk IN() to avoid URL limits
    for i in range(0, len(pred_ids), BATCH_SIZE):
        chunk = pred_ids[i : i + BATCH_SIZE]
        try:
            pq = (
                supabase.table("value_predictions")
                .select("id, stake_pct, odds, confidence_pct, po_value")
                .in_("id", chunk)
                .gte("confidence_pct", CONF_MIN)
                .gte("odds", ODDS_MIN)
                .lte("odds", ODDS_MAX)
                .eq("po_value", True)
                .execute()
            )
            rows = pq.data or []
            for p in rows:
                pred_by_id[p["id"]] = p
            total_loaded += len(rows)
            print(f"E) loaded filtered predictions for batch {i//BATCH_SIZE + 1}: {len(rows)}")
        except Exception as e:
            print(f"E) failed to load predictions for batch {i//BATCH_SIZE + 1}: {e}")

    if not pred_by_id:
        print("E) no predictions match filters (po=true, conf>=70, odds in range). Nothing to do.")
        return

    # F) Sort verifications chronologically ASC for bankroll math
    verifs.sort(key=lambda v: v["verified_at"])
    logs_to_insert = []
    current_bankroll = round(bankroll, 2)

    kept = 0
    skipped_no_pred = 0
    skipped_bad_stake = 0

    for v in verifs:
        pid = v["prediction_id"]
        pred = pred_by_id.get(pid)
        if not pred:
            skipped_no_pred += 1
            continue

        stake_pct = _to_float(pred.get("stake_pct"))
        odds = _to_float(pred.get("odds"))
        conf = _to_float(pred.get("confidence_pct"))

        # extra safety: ensure stake > 0 and odds/conf still valid
        if stake_pct is None or stake_pct <= 0:
            skipped_bad_stake += 1
            continue
        if odds is None or not (ODDS_MIN <= odds <= ODDS_MAX):
            continue
        if conf is None or conf < CONF_MIN:
            continue

        is_correct = bool(v.get("is_correct"))
        stake_amount = round((stake_pct / 100.0) * current_bankroll, 2)
        profit = round(stake_amount * (odds - 1), 2) if is_correct else round(-stake_amount, 2)
        after = round(current_bankroll + profit, 2)

        logs_to_insert.append({
            "id": str(uuid.uuid4()),
            "prediction_id": pid,
            "date": v["verified_at"].split("T")[0],
            "stake_amount": stake_amount,
            "odds": round(odds, 2),
            "result": "win" if is_correct else "lose",
            "profit": profit,
            "starting_bankroll": current_bankroll,
            "bankroll_after": after,
        })

        current_bankroll = after
        kept += 1

    print(f"F) ready to insert: {len(logs_to_insert)} "
          f"(kept={kept}, skipped_no_pred={skipped_no_pred}, skipped_bad_stake={skipped_bad_stake})")

    if not logs_to_insert:
        print("All new verifications were filtered out by rules.")
        return

    # G) Upsert by prediction_id (fallback if unique constraint not present)
    try:
        up = (
            supabase.table("bankroll_log")
            .upsert(logs_to_insert, on_conflict="prediction_id")
            .execute()
        )
        print(f"G) upserted rows: {len(logs_to_insert)}")
    except Exception as e:
        print(f"G) upsert failed (no unique constraint on prediction_id?), falling back: {e}")
        # fallback: insert only those not present
        try:
            existing = supabase.table("bankroll_log").select("prediction_id").in_("prediction_id", [r["prediction_id"] for r in logs_to_insert]).execute().data or []
            have = {r["prediction_id"] for r in existing}
            to_insert = [r for r in logs_to_insert if r["prediction_id"] not in have]
            if to_insert:
                supabase.table("bankroll_log").insert(to_insert).execute()
                print(f"G) inserted (fallback) rows: {len(to_insert)}")
            else:
                print("G) nothing new to insert after fallback (all existed).")
        except Exception as e2:
            print(f"G) fallback insert failed: {e2}")
            return

    # H) Console feedback
    for log in logs_to_insert:
        print(f"✅ {log['result']:4} | {log['date']} | {log['starting_bankroll']} → {log['bankroll_after']} "
              f"(pid={log['prediction_id']})")

    print("=== bankroll_log updater complete ===")
