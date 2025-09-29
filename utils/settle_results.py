# utils/settle_results.py
import os
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

# --- ENV / REST setup ---
SB_URL = os.getenv("SUPABASE_URL")
SB_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
VP_TABLE = os.getenv("VALUE_PREDICTIONS_TABLE", "value_predictions")
VER_TABLE = os.getenv("VERIFICATIONS_TABLE", "verifications")
MARKET = os.getenv("MARKET_NAME", "over_2_5")  # we settle OU2.5

if not SB_URL or not SB_KEY:
    raise RuntimeError("Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY/ANON_KEY")

HDRS = {
    "apikey": SB_KEY,
    "Authorization": f"Bearer {SB_KEY}",
    "Content-Type": "application/json",
}

# --- Use your existing fetcher for fixtures ---
try:
    from utils.get_football_data import fetch_fixtures
except Exception as e:
    raise ImportError("utils.get_football_data.fetch_fixtures is required for settlement") from e


# ---------- helpers ----------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _pg_get(path: str, params: Dict[str, Any]) -> requests.Response:
    return requests.get(f"{SB_URL.rstrip('/')}{path}", headers=HDRS, params=params, timeout=25)

def _pg_patch(path: str, params: Dict[str, Any], payload: Dict[str, Any]) -> requests.Response:
    return requests.patch(f"{SB_URL.rstrip('/')}{path}", headers=HDRS, params=params, data=json.dumps(payload), timeout=25)

def _pg_post(path: str, params: Dict[str, Any], payload: Any) -> requests.Response:
    return requests.post(f"{SB_URL.rstrip('/')}{path}", headers=HDRS, params=params, data=json.dumps(payload), timeout=25)


# ---------- value_predictions lookup / write ----------
def _get_latest_prediction_row(fixture_id: int, market: str = MARKET) -> Optional[Dict[str, Any]]:
    """
    Fetch latest value_prediction for fixture+market.
    We order by created_at desc if present; otherwise rely on default.
    """
    params = {
        "fixture_id": f"eq.{fixture_id}",
        "market": f"eq.{market}",
        "select": "id,fixture_id,market,prediction,odds,confidence_pct,created_at",
        "order": "created_at.desc",
        "limit": "1",
    }
    r = _pg_get(f"/rest/v1/{VP_TABLE}", params)
    if r.status_code >= 400:
        logging.error(f"[settle] GET {VP_TABLE} failed: {r.status_code} {r.text}")
        return None
    rows = r.json()
    return rows[0] if rows else None


def _patch_value_prediction_result(fixture_id: int, update: Dict[str, Any]) -> bool:
    """
    Best-effort patch to add result info into value_predictions.
    If your schema doesn’t have these columns, we log and continue.
    """
    params = {"fixture_id": f"eq.{fixture_id}", "market": f"eq.{MARKET}"}
    r = _pg_patch(f"/rest/v1/{VP_TABLE}", params, update)
    if r.status_code >= 400:
        logging.info(f"[settle] PATCH {VP_TABLE} ignored or failed (maybe columns not present): {r.status_code} {r.text}")
        return False
    return True


def _upsert_verification(prediction_id: str, is_correct: Optional[bool]) -> bool:
    """
    Upsert into verifications with PK/FK on prediction_id.
    If your table has only (prediction_id, is_correct), this will work.
    If it also has timestamps, PostgREST will fill defaults.
    """
    params = {"on_conflict": "prediction_id"}
    payload = [{"prediction_id": prediction_id, "is_correct": is_correct}]
    r = _pg_post(f"/rest/v1/{VER_TABLE}", params, payload)
    if r.status_code >= 400:
        logging.error(f"[settle] POST {VER_TABLE} failed: {r.status_code} {r.text}")
        return False
    return True


# ---------- fixture parsing ----------
def _is_finished(fx: Dict[str, Any]) -> bool:
    status = (((fx or {}).get("fixture") or {}).get("status") or {}).get("short")
    return status in {"FT", "AET", "PEN"}

def _total_goals(fx: Dict[str, Any]) -> Optional[int]:
    # Primary: goals.home/away
    goals = (fx or {}).get("goals") or {}
    try:
        return int(goals.get("home") or 0) + int(goals.get("away") or 0)
    except Exception:
        pass
    # Fallback: score.fulltime.home/away
    score = (fx or {}).get("score") or {}
    ft = score.get("fulltime") or {}
    try:
        return int(ft.get("home") or 0) + int(ft.get("away") or 0)
    except Exception:
        return None

def _result_side_from_total(total: Optional[int]) -> Optional[str]:
    if total is None:
        return None
    return "Over" if total > 2 else "Under"  # OU 2.5


# ---------- public API ----------
def settle_date(date_str: str) -> int:
    """
    Settle all finished fixtures for a given YYYY-MM-DD date.
    Returns: number of value_prediction rows updated (verifications written).
    """
    try:
        fixtures: List[Dict[str, Any]] = fetch_fixtures(date_str) or []
    except Exception as e:
        logging.error(f"[settle] fetch_fixtures failed for {date_str}: {e}")
        return 0

    updated = 0
    for fx in fixtures:
        try:
            if not _is_finished(fx):
                continue

            fixture_id = ((fx.get("fixture") or {}).get("id"))
            if not fixture_id:
                continue

            total = _total_goals(fx)
            side = _result_side_from_total(total)
            if side is None:
                logging.info(f"[settle] Fixture {fixture_id} finished but no total goals found; skipping.")
                continue

            vp = _get_latest_prediction_row(fixture_id, MARKET)
            if not vp:
                # no prediction saved for this fixture/market
                continue

            pick = (vp.get("prediction") or "").strip()
            pred_id = vp.get("id")
            won = (pick == side)

            # Try to patch the prediction row with result data (best-effort)
            _patch_value_prediction_result(
                fixture_id,
                {
                    "settled": True,
                    "settled_at": _now_iso(),
                    "result_goals": total,
                    "result_side": side,
                    "won": won,
                },
            )

            # Write to verifications table (authoritative correctness link)
            if pred_id and _upsert_verification(pred_id, won):
                updated += 1
                logging.info(f"[settle] ✅ Fixture {fixture_id}: total={total} side={side} pick={pick} won={won}")
        except Exception as e:
            logging.error(f"[settle] error: {e}")

    return updated


def settle_fixtures(fixture_ids: List[int]) -> int:
    """
    Settle a provided list of fixture IDs (useful for re-runs).
    """
    # We don’t have a 'fetch_fixture_by_id', so we run by date around now.
    # Prefer settle_date() for accuracy; this is a shim for quick replays.
    count = 0
    for _ in fixture_ids:
        # Nothing reliable w/o a by-id fetch; keep interface for future extension.
        pass
    return count
