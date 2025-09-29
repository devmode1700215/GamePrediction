# utils/settle_results.py
import os
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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

# Use your existing fetcher for fixtures
try:
    from utils.get_football_data import fetch_fixtures
except Exception as e:
    raise ImportError("utils.get_football_data.fetch_fixtures is required for settlement") from e


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pg_get(path: str, params: Dict[str, Any]) -> requests.Response:
    return requests.get(f"{SB_URL.rstrip('/')}{path}", headers=HDRS, params=params, timeout=25)


def _verifications_upsert(prediction_id: str, is_correct: Optional[bool]) -> bool:
    """
    Proper upsert into verifications with PK on prediction_id.
    """
    if not prediction_id:
        return False
    params = {"on_conflict": "prediction_id"}
    headers = dict(HDRS)
    headers["Prefer"] = "resolution=merge-duplicates,return=representation"
    payload = [{"prediction_id": prediction_id, "is_correct": is_correct}]
    r = requests.post(f"{SB_URL.rstrip('/')}/rest/v1/{VER_TABLE}",
                      headers=headers, params=params, data=json.dumps(payload), timeout=25)
    if r.status_code >= 400:
        logging.error(f"[settle] UPSERT {VER_TABLE} failed: {r.status_code} {r.text}")
        return False
    return True


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


def _is_finished(fx: Dict[str, Any]) -> bool:
    status = (((fx or {}).get("fixture") or {}).get("status") or {}).get("short")
    return status in {"FT", "AET", "PEN"}


def _total_goals(fx: Dict[str, Any]) -> Optional[int]:
    goals = (fx or {}).get("goals") or {}
    try:
        return int(goals.get("home") or 0) + int(goals.get("away") or 0)
    except Exception:
        pass
    score = (fx or {}).get("score") or {}
    ft = score.get("fulltime") or {}
    try:
        return int(ft.get("home") or 0) + int(ft.get("away") or 0)
    except Exception:
        return None


def _result_side_from_total(total: Optional[int]) -> Optional[str]:
    if total is None:
        return None
    return "Over" if total > 2 else "Under"  # OU 2.5 threshold


def settle_date(date_str: str) -> int:
    """
    Settle all finished fixtures for a given YYYY-MM-DD date.
    Writes/updates verifications rows; no longer patches value_predictions columns
    that are not in your schema.
    Returns count of verifications written/updated.
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

            # Authoritative correctness record (UPSERT)
            if pred_id and _verifications_upsert(pred_id, won):
                updated += 1
                logging.info(f"[settle] ✅ Fixture {fixture_id}: total={total} side={side} pick={pick} won={won}")

        except Exception as e:
            logging.error(f"[settle] error: {e}")

    return updated
