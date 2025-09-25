# utils/update_bankroll_log.py
import os, logging, datetime as dt
from decimal import Decimal, ROUND_HALF_UP
import requests

SB_URL  = os.getenv("SUPABASE_URL")
SB_KEY  = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_ANON_KEY")
BANKROLL_START = Decimal(os.getenv("BANKROLL_START", "100"))
BANKROLL_LABEL = os.getenv("BANKROLL_LABEL", "default")

HDRS = {
    "apikey": SB_KEY,
    "Authorization": f"Bearer {SB_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=representation",
}

def d(x) -> Decimal:
    if isinstance(x, Decimal): return x
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def sb_select(table, query=""):
    r = requests.get(f"{SB_URL}/rest/v1/{table}{query}", headers=HDRS, timeout=30)
    r.raise_for_status()
    return r.json()

def sb_upsert(table, payload):
    r = requests.post(f"{SB_URL}/rest/v1/{table}", headers=HDRS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def get_last_bankroll():
    # 1) bankroll_state snapshot
    st = sb_select("bankroll_state", "?select=bankroll,updated_at,label&order=updated_at.desc&limit=1")
    if st:
        return d(st[0]["bankroll"])
    # 2) last log row
    lg = sb_select("bankroll_log", "?select=bankroll_after,created_at&order=created_at.desc&limit=1")
    if lg:
        return d(lg[0]["bankroll_after"])
    # 3) fallback
    return BANKROLL_START

def fetch_newly_settled_picks():
    """
    Your existing logic likely reads from 'verifications' joined to 'value_predictions'.
    We assume it returns rows with:
      - source_id (unique id to ensure idempotency; e.g., verification_id or fixture_id||market)
      - settled_at (timestamp for chronological ordering)
      - outcome ('win'|'loss'|'push')
      - odds (decimal)
      - stake_pct (decimal percent of bankroll, e.g., 2.5 -> 2.5%)
      - optional notes
    And we exclude those already in bankroll_log via a NOT IN list.
    Adapt the SELECT below to match your schema.
    """
    # Find existing source_ids to exclude
    existing = sb_select("bankroll_log", "?select=source_id&order=created_at.asc")
    existing_ids = {row["source_id"] for row in existing if row.get("source_id")}
    # Pull candidate settled predictions (you already have a view or endpoint for this)
    # Example REST view name: 'vw_settled_value_predictions'
    cand = sb_select("vw_settled_value_predictions",
                     "?select=source_id,settled_at,outcome,odds,stake_pct,notes"
                     "&order=settled_at.asc")
    return [c for c in cand if c["source_id"] not in existing_ids]

def apply_outcome(bankroll: Decimal, odds: Decimal, stake_pct: Decimal, outcome: str):
    stake = (bankroll * d(stake_pct) / Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if outcome == "win":
        pnl = (stake * (d(odds) - Decimal("1"))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    elif outcome == "push":
        pnl = Decimal("0.00")
    else:  # loss
        pnl = (stake * Decimal("-1")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return stake, pnl, (bankroll + pnl).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def run():
    logging.info("Starting bankroll updater…")
    mode = "SERVICE_ROLE" if os.getenv("SUPABASE_SERVICE_ROLE_KEY") else ("SUPABASE_KEY" if os.getenv("SUPABASE_KEY") else "ANON")
    logging.info("Supabase auth mode: %s", mode)

    bankroll = get_last_bankroll()
    logging.info("Current bankroll baseline: %s", bankroll)

    new_rows = fetch_newly_settled_picks()
    if not new_rows:
        logging.info("No new settled picks to log.")
        return

    inserted = 0
    for row in new_rows:
        source_id  = row["source_id"]
        settled_at = row.get("settled_at") or dt.datetime.utcnow().isoformat()
        outcome    = (row["outcome"] or "loss").lower()
        odds       = d(row["odds"])
        stake_pct  = d(row["stake_pct"])
        notes      = row.get("notes")

        start_before = bankroll
        stake, pnl, bankroll_after = apply_outcome(bankroll, odds, stake_pct, outcome)

        payload = [{
            "source_id": source_id,
            "created_at": settled_at,
            "starting_bankroll": str(start_before),
            "bet_amount": str(stake),
            "profit_loss": str(pnl),
            "bankroll_after": str(bankroll_after),
            "outcome": outcome,
            "odds": str(odds),
            "stake_pct": str(stake_pct),
            "label": BANKROLL_LABEL,
            "notes": notes
        }]
        sb_upsert("bankroll_log?on_conflict=source_id", payload)

        bankroll = bankroll_after
        inserted += 1

    # snapshot
    sb_upsert("bankroll_state", [{
        "label": BANKROLL_LABEL,
        "bankroll": str(bankroll)
    }])

    logging.info("Bankroll updated. Inserted %d rows. New bankroll: %s", inserted, bankroll)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
