# utils/update_top10_predictions.py
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from utils.supabaseClient import supabase

# ----------------------------
# Config (ENV overrides)
# ----------------------------
ONLY_MARKET       = os.getenv("TOP10_ONLY_MARKET", "over_2_5")   # market considered
ONLY_PO_VALUE     = os.getenv("TOP10_ONLY_PO_VALUE", "true").lower() in ("1","true","yes","y")
TOPK              = int(os.getenv("TOP10_K", "10"))              # top K per day
STAKE_AMOUNT      = float(os.getenv("TOP10_STAKE_AMOUNT", "5"))  # fixed stake per pick
BACKFILL_START    = os.getenv("TOP10_BACKFILL_START", None)      # e.g. "2025-09-01"
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
# Low-level data access
# ----------------------------
def _fixture_ids_for_date_via_matches(date_str: str) -> List[int]:
    """
    Prefer using matches.date as a UTC-day window. If matches.date is DATE, this still works.
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
        # Requires a unique constraint on (date, rank, prediction_id)
        supabase.table("top10_predictions").upsert(
            payload, on_conflict="date,rank,prediction_id"
        ).execute()
        return True
    except Exception as e:
        print(f"[top10] upsert failed for {date_str}: {e}")
        return False

# ----------------------------
# Backfill helpers (robust): get dates from value_predictions + matches
# ----------------------------
def _all_fixture_ids_from_value_predictions() -> List[int]:
    try:
        res = supabase.table("value_predictions").select("fixture_id").limit(500000).execute()
        rows = getattr(res, "data", None) or []
        return [int(r["fixture_id"]) for r in rows if r.get("fixture_id") is not None]
    except Exception:
        return []

def _fixture_dates_map(fixture_ids: List[int]) -> Dict[int, str]:
    """
    Map fixture_id -> YYYY-MM-DD (UTC) using matches.date, in batches.
    """
    out: Dict[int, str] = {}
    for i in range(0, len(fixture_ids), BATCH_SIZE):
        chunk = fixture_ids[i:i+BATCH_SIZE]
        try:
            res = (
                supabase.table("matches")
                .select("fixture_id, date")
                .in_("fixture_id", chunk)
                .execute()
            )
            rows = getattr(res, "data", None) or []
            for r in rows:
                fid = r.get("fixture_id")
                ds  = r.get("date")
                if not fid or not ds:
                    continue
                try:
                    dt = datetime.fromisoformat(str(ds).replace("Z", "+00:00")).astimezone(timezone.utc)
                    out[int(fid)] = dt.strftime("%Y-%m-%d")
                except Exception:
                    # if it's a plain YYYY-MM-DD (DATE type), accept directly
                    out[int(fid)] = str(ds)[:10]
        except Exception:
            continue
    return out

def _dates_from_value_predictions(backfill_start: Optional[str]) -> List[str]:
    """
    Build a sorted list of distinct dates by reading fixture_ids in value_predictions
    and mapping them to matches.date. This is more robust than relying on matches alone.
    """
    fids = _all_fixture_ids_from_value_predictions()
    if not fids:
        return []
    fmap = _fixture_dates_map(_distinct(fids))
    ds_set = set(fmap.values())
    if backfill_start:
        ds_set = {d for d in ds_set if d >= backfill_start}
    today = _today_utc_ds()
    ds_set = {d for d in ds_set if d <= today}
    return sorted(ds_set)

# ----------------------------
# Core: compute & store Top-10 for a specific date
# ----------------------------
def compute_top10_for_date(date_str: str) -> int:
    fixture_ids = _fixture_ids_for_date_via_matches(date_str)
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

# ----------------------------
# Backfill across all dates found in value_predictions->matches
# ----------------------------
def backfill_all():
    all_dates = _dates_from_value_predictions(BACKFILL_START)
    if not all_dates:
        print("[top10] no dates found to backfill (from value_predictions).")
        return
    total = 0
    for ds in all_dates:
        total += compute_top10_for_date(ds)
    print(f"[top10] backfill complete, total rows written: {total}")

# ----------------------------
# Result sync (UPDATE-by-id to avoid NOT NULL issues)
# ----------------------------
def _load_top10_rows_without_result(limit: int = 5000) -> List[Dict[str, Any]]:
    try:
        r = (supabase.table("top10_predictions")
             .select("id, date, prediction_id, odds, stake_amount")
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

    updated = 0
    for r in rows:
        row_id = r.get("id")
        pid    = r.get("prediction_id")
        if not row_id or not pid:
            continue

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

        try:
            supabase.table("top10_predictions").update({
                "verified_at": v.get("verified_at"),
                "is_correct": is_correct,
                "result": "win" if is_correct else "lose",
                "outcome": "win" if is_correct else "lose",
                "profit": profit,
                "goals_home": gh,
                "goals_away": ga,
                "total_goals": tg,
            }).eq("id", row_id).execute()
            updated += 1
        except Exception as e:
            print(f"[top10] update failed for id={row_id}: {e}")

    print(f"[top10] synced results for {updated} rows.")
    return updated

# ----------------------------
# CLI entry
# ----------------------------
if __name__ == "__main__":
    print("✅ Supabase connection successful")
    if BACKFILL_ALL:
        backfill_all()
    else:
        ds = _today_utc_ds()
        compute_top10_for_date(ds)
    try:
        sync_results_for_all()
    except Exception:
        pass
