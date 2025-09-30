# utils/odds_sources.py
from __future__ import annotations
import os
import logging
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from utils.safe_get import safe_get
from utils.get_football_data import get_match_odds as apifootball_odds  # reuse your parser

load_dotenv()
logger = logging.getLogger(__name__)

# -------- Overtime config (set these in your env if you have Overtime) --------
OT_BASE = os.getenv("OVERTIME_BASE_URL")         # e.g. https://api.overtime.example
OT_KEY  = os.getenv("OVERTIME_API_KEY")          # whatever token the service uses
OT_HDR  = {"Authorization": f"Bearer {OT_KEY}"} if OT_KEY else {}

def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _overtime_fetch_ou25(fixture_id: int) -> Optional[Dict[str, Any]]:
    """
    Try to fetch OU 2.5 odds from Overtime.
    Expected JSON (flexible): { "over_2_5": 1.95, "under_2_5": 1.87 }
    You can adapt the endpoint/shape to your provider without touching the rest of the code.
    """
    if not OT_BASE:
        return None
    # Example endpoint; adjust to your provider path/params:
    url = f"{OT_BASE.rstrip('/')}/odds/ou25?fixture_id={fixture_id}"
    resp = safe_get(url, headers=OT_HDR)
    if resp is None:
        return None
    try:
        data = resp.json() or {}
        over_ = _to_float(data.get("over_2_5"))
        under_ = _to_float(data.get("under_2_5"))
        if over_ is None and under_ is None:
            return None
        return {
            "over_2_5": over_,
            "under_2_5": under_,
            "source": "overtime",
            "is_overtime_odds": True,
        }
    except Exception as e:
        logger.warning(f"[overtime] parse error for fixture {fixture_id}: {e}")
        return None

def _apifootball_fetch_ou25(fixture_id: int, preferred_bookmaker: str = "Bwin") -> Optional[Dict[str, Any]]:
    flat = apifootball_odds(fixture_id, preferred_bookmaker=preferred_bookmaker) or {}
    if flat.get("over_2_5") is None and flat.get("under_2_5") is None:
        return None
    return {
        "over_2_5": flat.get("over_2_5"),
        "under_2_5": flat.get("under_2_5"),
        "source": "apifootball",
        "is_overtime_odds": False,
    }

def get_ou25_best(fixture_id: int, preferred_bookmaker: str = "Bwin") -> Dict[str, Any]:
    """
    Try Overtime first (if configured), then API-Football.
    Returns { over_2_5, under_2_5, source, is_overtime_odds }
    """
    # 1) Overtime (if env configured)
    ot = _overtime_fetch_ou25(fixture_id)
    if ot:
        return ot
    # 2) API-Football
    af = _apifootball_fetch_ou25(fixture_id, preferred_bookmaker=preferred_bookmaker)
    if af:
        return af
    # 3) Empty fallback
    return {"over_2_5": None, "under_2_5": None, "source": None, "is_overtime_odds": False}
