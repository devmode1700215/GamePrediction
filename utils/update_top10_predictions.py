# utils/update_top10_predictions.py
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from utils.supabaseClient import supabase

# ----------------------------
# Config (ENV overrides)
# ----------------------------
ONLY_MARKET       = os.getenv("TOP10_ONLY_MARKET", "over_2_5")  # market considered
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

def _day_bounds_utc(date_str: str) -> tuple[str, str]:
    """Return [start, end) UTC ISO strings for the day."""
    d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start = d.isoformat()
    end = (d + timedelta(days=1)).isoformat()
    return start, end

def _distinct(a):
    return list(dict.fromkeys(a))

# ----------------------------
# Data access
# ----------------------------
def _fixture_ids_for_date(date_str: str) -> List[int]:
    """
    Use a UTC day window so it works whether 'matches.date' is timestamptz or ISO text.
    """
    start, end = _day_bounds_utc(date_str)
    try:
        res = (
            supabase.table("matches")
            .select("fixture_id, date")
            .gte("date", start)
            .lt("date", end)
            .limit(20000)
            .execute()
        )
        rows = getattr(res, "data", None) or []
        return [int(r["fixture_id"]) for r in rows if r.get("fixture_id") is not None]
    except Exception:
        return []

def _match_names_for_fixtures(fixture_ids: List[int]) -> Dict[int, Dict[str, Optional[str]]]:
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
                fid = int(r["fixture_id"])
                ht = (r.get("home_team") or {}).get("name")
                at = (r.get("away_team") or {}).get("name")
                out[fid] = {"home_team": ht, "away_team": at}
        except Exception:
            continue
    return out

def _predictions_for_fixtures(fixture_ids: List[int]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i in range(0, len(fixture_ids), BATCH_SIZE):
        chunk = fixture_ids[i:i+BATCH_SIZE]
        try:
            q = (
                supabase.table("value_predictions")
                .select("id, fixture_id, market, prediction, odds, confidence_pct, edge, po_value")
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
    if not rows:
        return True
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
    Distinct UTC dates derived from matches.date using day windows.
    """
    try:
        res = supabase.table("matches").select("date").order("date", desc=False).limit(200000).execute()
        rows = getattr(res, "data", None) or []
        ds = set()
        for r in rows:
            iso = r.get("date")
            if not iso:
                continue
            try:
                dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00")).astimezone(timezone.utc)
                ds.add(dt.strftime("%Y-%m-%d"))
            except Exception:
                continue
        return sorted(ds)
    except Exception:
        return []

# ----------------------------
# Core: compute & store Top-10
# ----------------------------
def compute_top10_for_date(date_str: str) -> int:
    fixture_ids = _fixture_ids_for_date(date_str)
    if not fixture_ids:
        print(f"[top10] {date_str}: no fixtures in matches (window).")
        return 0

    preds = _predictions_for_fixtures(fixture_ids)
    if not preds:
        print(f"[top10] {date_str}: no predictions found.")
        return 0

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

    filtered.sort(key=lambda r: (r["edge"] if r["edge"] is not None else -1), reverse=True)
    topk = filtered[:TOPK]

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
            "prediction": r.get("prediction"),          # keep for backward compatibility
            "original_prediction": r.get("prediction"), # snapshot
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
    all_dates = _distinct_match_dates()
    if not all_dates:
        print("[top10] no dates found to backfill.")
        return
    if BACKFILL_START:
        all_dates = [d for d in all_dates if d >= BACKFILL_START]
    today = _today_utc_ds()
    all_dates = [d for d in all_dates if d <= today]

    total = 0
    for ds in all_dates:
        total += compute_top10_for_date(ds)
    print(f"[top10] backfill complete, total rows written: {total}")

# ----------------------------
# Result sync (fills outcome + scores)
# ----------------------------
def _load_top10_rows_without_result(limit: int = 5000) -> List[Dict[str, Any]]:
    try:
        r = (supabase.table("top10_predictions")
             .select("id, prediction_id, odds, stake_amount")
             .is_("result", None)
             .limit(limit)
             .execute())
        return getattr(r, "data", None) or []
    except Exception:
        return []

def _load_verifications_for_pids(pids: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not pids:
        return out
    BATCH = 1000
    for i in range(0, len(pids), BATCH):
        chunk = pids[i:i+BATCH]
        try:
            r = (supabase.table("verifications")
                 .select("prediction_id, verified_at, is_correct, goals_home, goals_away, total_goals")
                 .in_("prediction_id", chunk)
                 .execute())
            rows = getattr(r, "data", None) or []
            for v in rows:
                pid = v.get("prediction_id")
                if pid:
                    out[pid] = v
        except Exception:
            continue
    return out

def _calc_profit(odds: float, stake: float, is_correct: bool) -> float:
    if odds is None or stake is None:
        return 0.0
    return round(stake * (odds - 1.0), 2) if is_correct else round(-stake, 2)

def sync_results_for_all() -> int:
    rows = _load_top10_rows_without_result()
    if not rows:
        print("[top10] no pending rows to sync.")
        return 0

    pid_list = _distinct([r["prediction_id"] for r in rows if r.get("prediction_id")])
    verifs = _load_verifications_for_pids(pid_list)

    updates = []
    for r in rows:
        pid = r.get("prediction_id")
        v = verifs.get(pid)
        if not v:
            continue
        is_correct = bool(v.get("is_correct"))
        odds  = float(r.get("odds") or 0)
        stake = float(r.get("stake_amount") or 0)
        profit = _calc_profit(odds, stake, is_correct)

        gh = v.get("goals_home")
        ga = v.get("goals_away")
        tg = v.get("total_goals")
        if tg is None and (gh is not None) and (ga is not None):
            try:
                tg = int(gh) + int(ga)
            except Exception:
                tg = None

        updates.append({
            "id": r["id"],
            "verified_at": v.get("verified_at"),
            "is_correct": is_correct,
            "result": "win" if is_correct else "lose",
            "outcome": "win" if is_correct else "lose",
            "profit": profit,
            "goals_home": gh,
            "goals_away": ga,
            "total_goals": tg,
        })

    if not updates:
        print("[top10] no matches between pending top10 and verifications.")
        return 0

    try:
        supabase.table("top10_predictions").upsert(updates, on_conflict="id").execute()
        print(f"[top10] synced results for {len(updates)} rows.")
        return len(updates)
    except Exception as e:
        print(f"[top10] sync upsert failed: {e}")
        return 0

# ----------------------------
# CLI entry
# ----------------------------
if __name__ == "__main__":
    if BACKFILL_ALL:
        backfill_all()
    else:
        ds = _today_utc_ds()
        compute_top10_for_date(ds)
    try:
        sync_results_for_all()
    except Exception:
        pass
