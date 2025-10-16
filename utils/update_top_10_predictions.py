# utils/update_top10_predictions.py
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

from utils.supabaseClient import supabase

# ----------------------------
# Config (ENV overrides)
# ----------------------------
ONLY_MARKET       = os.getenv("TOP10_ONLY_MARKET", "over_2_5")  # market to consider
ONLY_PO_VALUE     = os.getenv("TOP10_ONLY_PO_VALUE", "true").lower() in ("1","true","yes","y")
TOPK              = int(os.getenv("TOP10_K", "10"))             # top K per day
STAKE_AMOUNT      = float(os.getenv("TOP10_STAKE_AMOUNT", "5")) # fixed stake per pick
BACKFILL_START    = os.getenv("TOP10_BACKFILL_START", None)     # e.g. "2025-09-01"
BACKFILL_ALL      = os.getenv("TOP10_BACKFILL_ALL", "false").lower() in ("1","true","yes","y")

BATCH_SIZE        = int(os.getenv("TOP10_BATCH_SIZE", "800"))

# ----------------------------
# Helpers
# ----------------------------
def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _today_utc_ds() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _datestr(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")

# ----------------------------
# Data access
# ----------------------------
def _fixture_ids_for_date(date_str: str) -> List[int]:
    """
    Return fixture_ids from matches for a given date.
    """
    try:
        res = (
            supabase.table("matches")
            .select("fixture_id")
            .eq("date", date_str)
            .execute()
        )
        rows = getattr(res, "data", None) or []
        ids = []
        for r in rows:
            fid = r.get("fixture_id")
            if isinstance(fid, int):
                ids.append(fid)
        return ids
    except Exception:
        return []

def _match_names_for_fixtures(fixture_ids: List[int]) -> Dict[int, Dict[str, Optional[str]]]:
    """
    Map fixture_id -> {home_team, away_team}
    """
    out: Dict[int, Dict[str, Optional[str]]] = {}
    for i in range(0, len(fixture_ids), BATCH_SIZE):
        chunk = fixture_ids[i:i+BATCH_SIZE]
        try:
            res = (
                supabase.table("matches")
                .select("fixture_id, home_team, away_team")
                .in_("fixture_id", chunk)
                .execute()
            )
            rows = getattr(res, "data", None) or []
            for r in rows:
                fid = r.get("fixture_id")
                ht = (r.get("home_team") or {}).get("name")
                at = (r.get("away_team") or {}).get("name")
                out[int(fid)] = {"home_team": ht, "away_team": at}
        except Exception:
            continue
    return out

def _predictions_for_fixtures(fixture_ids: List[int]) -> List[Dict[str, Any]]:
    """
    Load value_predictions for the fixture_ids, filter by market and po_value if requested.
    """
    out: List[Dict[str, Any]] = []
    for i in range(0, len(fixture_ids), BATCH_SIZE):
        chunk = fixture_ids[i:i+BATCH_SIZE]
        try:
            q = (
                supabase.table("value_predictions")
                .select("id, fixture_id, market, prediction, odds, confidence_pct, edge, po_value, created_at")
                .in_("fixture_id", chunk)
                .eq("market", ONLY_MARKET)
            )
            if ONLY_PO_VALUE:
                q = q.eq("po_value", True)
            res = q.execute()
            rows = getattr(res, "data", None) or []
            out.extend(rows)
        except Exception:
            continue
    return out

def _upsert_top10(date_str: str, rows: List[Dict[str, Any]]) -> bool:
    """
    Upsert rows into top10_predictions. Unique keys (date, rank) and (date, prediction_id).
    """
    if not rows:
        return True
    # Ensure stake amount is set
    payload = []
    for r in rows:
        rr = dict(r)
        rr.setdefault("stake_amount", STAKE_AMOUNT)
        payload.append(rr)
    try:
        supabase.table("top10_predictions").upsert(payload, on_conflict="date,rank,prediction_id").execute()
        return True
    except Exception as e:
        print(f"[top10] upsert failed for {date_str}: {e}")
        return False

def _distinct_match_dates() -> List[str]:
    """
    Return distinct dates from matches. Used for backfill.
    """
    try:
        # Pull a lot; we can filter client-side
        res = supabase.table("matches").select("date").order("date", desc=False).limit(50000).execute()
        rows = getattr(res, "data", None) or []
        ds = sorted({r.get("date") for r in rows if r.get("date")})
        return ds
    except Exception:
        return []

# ----------------------------
# Core logic
# ----------------------------
def compute_top10_for_date(date_str: str) -> int:
    """
    Compute and store top 10 by edge for a given date.
    Returns number of rows written (0..TOPK)
    """
    fixture_ids = _fixture_ids_for_date(date_str)
    if not fixture_ids:
        print(f"[top10] {date_str}: no fixtures in matches.")
        return 0

    preds = _predictions_for_fixtures(fixture_ids)
    if not preds:
        print(f"[top10] {date_str}: no predictions found.")
        return 0

    # filter where edge is numeric
    filtered = []
    for p in preds:
        edge = _to_float(p.get("edge"))
        if edge is None:
            continue
        filtered.append({
            "prediction_id": p["id"],
            "fixture_id": p["fixture_id"],
            "market": p.get("market"),
            "prediction": p.get("prediction"),
            "odds": _to_float(p.get("odds")),
            "confidence_pct": _to_float(p.get("confidence_pct")),
            "edge": edge,
            "po_value": bool(p.get("po_value")),
        })

    if not filtered:
        print(f"[top10] {date_str}: predictions had no numeric edge.")
        return 0

    # sort by edge DESC, pick TOPK
    filtered.sort(key=lambda r: (r["edge"] if r["edge"] is not None else -1), reverse=True)
    topk = filtered[:TOPK]

    # Add rank and team names (nice for front-end)
    names = _match_names_for_fixtures([r["fixture_id"] for r in topk])
    rows = []
    for i, r in enumerate(topk, start=1):
        nm = names.get(int(r["fixture_id"])) or {}
        rows.append({
            "id": str(uuid.uuid4()),
            "date": date_str,
            "rank": i,
            "prediction_id": r["prediction_id"],
            "fixture_id": r["fixture_id"],
            "market": r.get("market"),
            "prediction": r.get("prediction"),
            "odds": r.get("odds"),
            "confidence_pct": r.get("confidence_pct"),
            "edge": r.get("edge"),
            "po_value": r.get("po_value"),
            "stake_amount": STAKE_AMOUNT,
            "home_team": nm.get("home_team"),
            "away_team": nm.get("away_team"),
        })

    ok = _upsert_top10(date_str, rows)
    if ok:
        print(f"[top10] {date_str}: upserted {len(rows)} rows.")
        return len(rows)
    else:
        return 0

def backfill_all():
    """
    Backfill for all dates present in matches, up to (and including) today.
    Optional: limit via TOP10_BACKFILL_START=YYYY-MM-DD
    """
    all_dates = _distinct_match_dates()
    if not all_dates:
        print("[top10] no dates found to backfill.")
        return

    if BACKFILL_START:
        all_dates = [d for d in all_dates if d >= BACKFILL_START]

    # Only backfill up to today UTC
    today = _today_utc_ds()
    all_dates = [d for d in all_dates if d <= today]

    total = 0
    for ds in all_dates:
        total += compute_top10_for_date(ds)
    print(f"[top10] backfill complete, total rows written: {total}")

# ----------------------------
# CLI entry
# ----------------------------
if __name__ == "__main__":
    # If called directly, default to today's UTC top10; optionally backfill.
    if BACKFILL_ALL:
        backfill_all()
    else:
        ds = _today_utc_ds()
        compute_top10_for_date(ds)
