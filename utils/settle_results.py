# utils/settle_results.py
import os
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

# Use your existing fetcher so we get the same fixture shape
try:
    from utils.get_football_data import fetch_fixtures
except Exception as e:
    raise ImportError("utils.get_football_data.fetch_fixtures is required for settlement") from e

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
TABLE = os.getenv("VALUE_PREDICTIONS_TABLE", "value_predictions")  # allow override if your table is named differently

def _sb_headers() -> Dict[str, str]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Supabase env not set: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY/ANON_KEY are required")
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }

def _sb_get_prediction(fixture_id: int) -> Optional[Dict[str, Any]]:
    """Fetch existing prediction row for this fixture so we can compute win/loss."""
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{TABLE}"
    params = {
        "fixture_id": f"eq.{fixture_id}",
        "select": "fixture_id,market,prediction,odds,won,settled,result_side,result_goals"
    }
    resp = requests.get(url, headers=_sb_headers(), params=params, timeout=20)
    if resp.status_code >= 400:
        logging.error(f"Supabase GET failed: {resp.status_code} {resp.text}")
        return None
    rows = resp.json()
    if not rows:
        return None
    # if multiple, take latest
    return rows[-1]

def _sb_update_result(fixture_id: int, update: Dict[str, Any]) -> bool:
    """PATCH the row for this fixture_id with result fields."""
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{TABLE}"
    params = {"fixture_id": f"eq.{fixture_id}"}
    resp = requests.patch(url, headers=_sb_headers(), params=params, data=json.dumps(update), timeout=20)
    if resp.status_code >= 400:
        logging.error(f"Supabase PATCH failed (fixture {fixture_id}): {resp.status_code} {resp.text}")
        return False
    return True

def _is_finished(fx: Dict[str, Any]) -> bool:
    status = (((fx or {}).get("fixture") or {}).get("status") or {}).get("short")
    return status in {"FT", "AET", "PEN"}  # full time / extra time / penalties (total goals are 90’+ET only per many feeds; we use goals field below)

def _total_goals(fx: Dict[str, Any]) -> Optional[int]:
    goals = (fx or {}).get("goals") or {}
    try:
        h = int(goals.get("home") or 0)
        a = int(goals.get("away") or 0)
        return h + a
    except Exception:
        # Some feeds expose in score: { fulltime: { home, away } }
        score = (fx or {}).get("score") or {}
        ft = score.get("fulltime") or {}
        try:
            h = int(ft.get("home") or 0)
            a = int(ft.get("away") or 0)
            return h + a
        except Exception:
            return None

def _result_side_from_total(total: Optional[int]) -> Optional[str]:
    if total is None:
        return None
    return "Over" if total > 2 else "Under"  # OU 2.5 threshold

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def settle_date(date_str: str) -> int:
    """
    Settle all finished fixtures for a given YYYY-MM-DD date.
    Returns the count of rows updated.
    """
    try:
        fixtures: List[Dict[str, Any]] = fetch_fixtures(date_str) or []
    except Exception as e:
        logging.error(f"fetch_fixtures failed for {date_str}: {e}")
        return 0

    updated = 0
    for fx in fixtures:
        try:
            if not _is_finished(fx):
                continue
            fid = ((fx.get("fixture") or {}).get("id"))
            if not fid:
                continue

            total = _total_goals(fx)
            side = _result_side_from_total(total)
            if side is None:
                logging.warning(f"Fixture {fid} finished but no total goals found; skipping.")
                continue

            pred = _sb_get_prediction(fid)
            if not pred:
                # nothing to settle for this fixture_id
                continue

            pick = (pred.get("prediction") or "").strip()
            won = (pick == side)

            update = {
                "settled": True,
                "settled_at": _now_iso(),
                "result_goals": total,
                "result_side": side,
                "won": won,
            }
            if _sb_update_result(fid, update):
                updated += 1
                logging.info(f"Settled fixture {fid}: total={total} side={side} won={won}")
        except Exception as e:
            logging.error(f"Settlement error on fixture: {e}")
    return updated
