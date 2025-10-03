# utils/settle_results.py
from __future__ import annotations

import os
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from utils.supabaseClient import supabase

# Reuse API-Football credentials
FOOTBALL_API_KEY = os.getenv("FOOTBALL_API_KEY")
BASE_URL = os.getenv("FOOTBALL_BASE_URL", "https://v3.football.api-sports.io")
HEADERS = {"x-apisports-key": FOOTBALL_API_KEY} if FOOTBALL_API_KEY else {}

APP_TZ = os.getenv("APP_TZ", "UTC")
logger = logging.getLogger(__name__)

# ---- HTTP (independent of utils.safe_get to avoid signature mismatches) ----
def _http_get(url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=15)
        if r.status_code != 200:
            logger.warning(f"[settle] GET {url} -> {r.status_code} {r.text[:150]}")
            return None
        return r.json()
    except Exception as e:
        logger.warning(f"[settle] GET failed {url}: {e}")
        return None

# ---- API-Football helpers ---------------------------------------------------
def _fetch_fixtures_for_date(date_str: str) -> List[Dict[str, Any]]:
    """Return fixtures (with scores & status) for YYYY-MM-DD."""
    url = f"{BASE_URL}/fixtures"
    data = _http_get(url, params={"date": date_str}) or {}
    return data.get("response", []) or []

def _fixture_result_from_api_node(node: Dict[str, Any]) -> Optional[Tuple[int, int, int, str]]:
    """
    From one API-Football fixture node, return:
      (goals_home, goals_away, total_goals, status_short) or None if not finished.
    """
    try:
        status = (((node.get("fixture") or {}).get("status") or {}).get("short") or "").upper()
        if status not in ("FT", "AET", "PEN", "ABD", "AWD", "WO"):
            return None
        goals = node.get("goals") or {}
        gh = int(goals.get("home") or 0)
        ga = int(goals.get("away") or 0)
        tot = gh + ga
        return gh, ga, tot, status
    except Exception:
        return None

# ---- Supabase helpers -------------------------------------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _select_predictions_for_fixture(fid: int) -> List[Dict[str, Any]]:
    """All value_predictions rows for a fixture (any markets)."""
    try:
        r = supabase.table("value_predictions").select(
            "id, fixture_id, market, prediction, odds"
        ).eq("fixture_id", fid).execute()
        return getattr(r, "data", None) or []
    except Exception as e:
        logger.warning(f"[settle] load predictions failed for {fid}: {e}")
        return []

def _upsert_verification(rows: List[Dict[str, Any]]) -> bool:
    """
    Upsert into verifications on conflict 'prediction_id'.
    This avoids the 409 duplicate PK error you saw earlier.
    """
    try:
        supabase.table("verifications").upsert(
            rows, on_conflict="prediction_id"
        ).execute()
        return True
    except Exception as e:
        logger.error(f"[settle] POST/UPSERT verifications failed: {e}")
        return False

def _try_patch_value_predictions_result(pid: str, patch: Dict[str, Any]) -> None:
    """
    Best-effort patch back into value_predictions (only if those columns exist).
    We swallow 400 PGRST204 'column not in schema cache' errors.
    """
    try:
        supabase.table("value_predictions").update(patch).eq("id", pid).execute()
    except Exception as e:
        msg = str(e)
        if "PGRST204" in msg or "column" in msg.lower():
            logger.info(f"[settle] PATCH value_predictions ignored or failed (maybe columns not present): {msg[:200]}")
        else:
            logger.warning(f"[settle] PATCH value_predictions failed: {msg[:200]}")

# ---- Core settlement --------------------------------------------------------
def _ou25_is_correct(pick: str, total_goals: int) -> Optional[bool]:
    p = (pick or "").strip().lower()
    if p == "over":
        return total_goals >= 3
    if p == "under":
        return total_goals <= 2
    return None

def settle_date(date_str: str) -> int:
    """
    Verify & upsert results for all finished fixtures of the given day (YYYY-MM-DD).
    Returns number of verifications written.
    """
    fixtures = _fetch_fixtures_for_date(date_str)
    if not fixtures:
        logger.info(f"[settle] No fixtures from API for {date_str}")
        return 0

    # Map fixture_id -> result tuple
    finished: Dict[int, Tuple[int,int,int,str]] = {}
    for n in fixtures:
        fid = ((n.get("fixture") or {}).get("id")) or None
        if not isinstance(fid, int):
            continue
        res = _fixture_result_from_api_node(n)
        if res is None:
            continue
        finished[fid] = res

    if not finished:
        logger.info(f"[settle] No finished fixtures for {date_str}")
        return 0

    total_written = 0
    batch_verifs: List[Dict[str, Any]] = []

    for fid, (gh, ga, tot, status) in finished.items():
        preds = _select_predictions_for_fixture(fid)
        if not preds:
            continue

        for p in preds:
            pid   = p.get("id")
            pick  = (p.get("prediction") or "").strip()
            market= (p.get("market") or "").strip().lower()

            if not pid:
                continue
            if market != "over_2_5":
                # You can extend this later for BTTS/1X2
                continue

            is_correct = _ou25_is_correct(pick, tot)
            if is_correct is None:
                continue

            row = {
                "prediction_id": pid,
                "fixture_id": fid,
                "verified_at": _now_iso(),
                "is_correct": bool(is_correct),
                # write expanded result fields if the table has them (safe to upsert anyway)
                "goals_home": gh,
                "goals_away": ga,
                "total_goals": tot,
                "status": status,
                "market": "over_2_5",
                "pick": pick,
            }
            batch_verifs.append(row)

            # (Optional) also write the outcome back to value_predictions if columns exist
            _try_patch_value_predictions_result(pid, {
                "is_correct": bool(is_correct),
                "result": "win" if is_correct else "lose",
                "goals_home": gh,
                "goals_away": ga,
                "total_goals": tot,
            })

        # Flush periodically to keep payloads small
        if len(batch_verifs) >= 500:
            if _upsert_verification(batch_verifs):
                total_written += len(batch_verifs)
            batch_verifs = []

    # Final flush
    if batch_verifs:
        if _upsert_verification(batch_verifs):
            total_written += len(batch_verifs)

    return total_written
