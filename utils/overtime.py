# utils/overtime.py
from __future__ import annotations
import os
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from utils.safe_get import safe_get
from utils.supabaseClient import supabase

logger = logging.getLogger(__name__)

OT_BASE = os.getenv("OVERTIME_BASE_URL")            # e.g. https://api.overtime.example
OT_KEY  = os.getenv("OVERTIME_API_KEY")             # Bearer token
# If your provider uses a different route, override via env; default is a "list all soccer with odds" endpoint.
OT_SOCCER_PATH = os.getenv("OVERTIME_SOCCER_PATH", "/odds/soccer")

def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _slug(s: str) -> str:
    s = (s or "").strip().lower()
    out = []
    for ch in s:
        out.append(ch if ch.isalnum() else "-")
    return "-".join([seg for seg in "".join(out).split("-") if seg])

def _provider_key(date_iso: str, home: str, away: str) -> str:
    return f"{date_iso}|{_slug(home)}|{_slug(away)}"

def _first(*vals):
    for v in vals:
        if v is not None:
            return v
    return None

def _extract_1x2(item: Dict[str, Any]) -> Dict[str, Optional[float]]:
    m = item.get("markets") or {}
    oneXtwo = m.get("1x2") or m.get("1X2") or m.get("match_winner") or m.get("matchWinner") or {}
    # handle dict of {home: 1.90, draw: 3.30, away: 4.20} or list of {outcome, odd}
    if isinstance(oneXtwo, dict):
        return {
            "home": _to_float(_first(oneXtwo.get("home"), oneXtwo.get("Home"), oneXtwo.get("H"))),
            "draw": _to_float(_first(oneXtwo.get("draw"), oneXtwo.get("Draw"), oneXtwo.get("D"))),
            "away": _to_float(_first(oneXtwo.get("away"), oneXtwo.get("Away"), oneXtwo.get("A"))),
        }
    elif isinstance(oneXtwo, list):
        odds_map = {}
        for v in oneXtwo:
            k = (v.get("outcome") or v.get("value") or "").lower()
            o = _to_float(v.get("odd"))
            if k in ("home", "1"): odds_map["home"] = o
            elif k in ("draw", "x"): odds_map["draw"] = o
            elif k in ("away", "2"): odds_map["away"] = o
        return {"home": odds_map.get("home"), "draw": odds_map.get("draw"), "away": odds_map.get("away")}
    return {"home": None, "draw": None, "away": None}

def _extract_btts(item: Dict[str, Any]) -> Dict[str, Optional[float]]:
    m = item.get("markets") or {}
    btts = m.get("btts") or m.get("both_teams_to_score") or m.get("bothTeamsToScore") or {}
    if isinstance(btts, dict):
        return {
            "yes": _to_float(_first(btts.get("yes"), btts.get("Yes"))),
            "no":  _to_float(_first(btts.get("no"), btts.get("No"))),
        }
    elif isinstance(btts, list):
        odds_map = {}
        for v in btts:
            k = (v.get("value") or v.get("label") or "").lower()
            o = _to_float(v.get("odd"))
            if k == "yes": odds_map["yes"] = o
            elif k == "no": odds_map["no"] = o
        return {"yes": odds_map.get("yes"), "no": odds_map.get("no")}
    return {"yes": None, "no": None}

def _extract_ou25(item: Dict[str, Any]) -> Dict[str, Optional[float]]:
    m = item.get("markets") or {}
    ou = m.get("ou25") or m.get("over_under_2_5") or m.get("overUnder2_5") or m.get("goals_over_under") or {}
    if isinstance(ou, dict):
        return {
            "over_2_5": _to_float(_first(ou.get("over_2_5"), ou.get("Over 2.5"), ou.get("over"), ou.get("Over"))),
            "under_2_5": _to_float(_first(ou.get("under_2_5"), ou.get("Under 2.5"), ou.get("under"), ou.get("Under"))),
        }
    elif isinstance(ou, list):
        odds_map = {}
        for v in ou:
            label = (v.get("value") or v.get("label") or "").lower()
            o = _to_float(v.get("odd"))
            if "over" in label and "2.5" in label: odds_map["over_2_5"] = o
            if "under" in label and "2.5" in label: odds_map["under_2_5"] = o
        return {"over_2_5": odds_map.get("over_2_5"), "under_2_5": odds_map.get("under_2_5")}
    # flat fields fallback
    return {
        "over_2_5": _to_float(item.get("over_2_5")),
        "under_2_5": _to_float(item.get("under_2_5")),
    }

def _normalize_item(item: Dict[str, Any], date_iso: str) -> Optional[Dict[str, Any]]:
    # Home/Away names
    home = _first(item.get("home"), item.get("home_name"), (item.get("teams") or {}).get("home"), "")
    away = _first(item.get("away"), item.get("away_name"), (item.get("teams") or {}).get("away"), "")
    home = home.get("name") if isinstance(home, dict) else home
    away = away.get("name") if isinstance(away, dict) else away
    if not home or not away:
        return None

    # Kickoff
    kickoff = _first(item.get("kickoff"), item.get("commence_time"), item.get("startTime"))
    # League/Country
    comp = item.get("competition") or {}
    league = _first(item.get("league"), comp.get("name"))
    country = _first(item.get("country"), comp.get("country"))

    # Odds
    x12 = _extract_1x2(item)
    btts = _extract_btts(item)
    ou  = _extract_ou25(item)

    provider_match_id = _first(item.get("id"), item.get("match_id"), item.get("event_id"))
    pk = _provider_key(date_iso, home, away)

    return {
        "provider": "overtime",
        "provider_key": pk,
        "provider_match_id": provider_match_id,
        "date": date_iso,
        "kickoff_utc": kickoff,
        "league_name": league,
        "country": country,
        "home_name": home,
        "away_name": away,
        "odds_home": x12.get("home"),
        "odds_draw": x12.get("draw"),
        "odds_away": x12.get("away"),
        "btts_yes": btts.get("yes"),
        "btts_no": btts.get("no"),
        "over_2_5": ou.get("over_2_5"),
        "under_2_5": ou.get("under_2_5"),
        "raw": item,
    }

def fetch_overtime_soccer(date_iso: str) -> List[Dict[str, Any]]:
    """
    Fetch *all* soccer matches with odds from Overtime for a specific date.
    Endpoint is configurable; we pass the date and market hints if supported.
    """
    if not OT_BASE or not OT_KEY:
        logger.info("Overtime not configured; set OVERTIME_BASE_URL and OVERTIME_API_KEY.")
        return []

    url = f"{OT_BASE.rstrip('/')}{OT_SOCCER_PATH}"
    headers = {"Authorization": f"Bearer {OT_KEY}"}
    params = {"date": date_iso, "markets": "1x2,btts,ou25"}  # provider may ignore
    resp = safe_get(url, headers=headers, params=params)
    if resp is None:
        logger.warning("[overtime] no response for %s", date_iso)
        return []

    try:
        data = resp.json() or {}
        # Accept flexible container keys
        items = data.get("results") or data.get("matches") or data.get("data") or data.get("items") or []
        if not isinstance(items, list):
            logger.warning("[overtime] unexpected payload shape for %s", url)
            return []
        out: List[Dict[str, Any]] = []
        for it in items:
            norm = _normalize_item(it, date_iso)
            if norm:
                out.append(norm)
        logger.info("[overtime] %s: normalized %d rows", date_iso, len(out))
        return out
    except Exception as e:
        logger.warning("[overtime] parse error for %s: %s", date_iso, e)
        return []

def upsert_overtime_games(rows: List[Dict[str, Any]]) -> int:
    """
    Upsert into overtime_games with unique (provider, provider_key).
    """
    if not rows:
        return 0
    try:
        res = supabase.table("overtime_games").upsert(
            rows,
            on_conflict="provider,provider_key",
        ).execute()
        # Supabase python client returns .data as list on success (or None on 204)
        data = getattr(res, "data", None)
        if isinstance(data, list):
            return len(data)
        return len(rows)  # best effort
    except Exception as e:
        logger.error("upsert_overtime_games failed: %s", e)
        return 0

def refresh_overtime_games_for_range(start_date_iso: str, days_ahead: int) -> int:
    """
    Pull and store Overtime soccer odds for start_date..start_date+days_ahead (inclusive).
    Returns total rows upserted (best effort).
    """
    try:
        base = datetime.strptime(start_date_iso, "%Y-%m-%d")
    except Exception:
        logger.error("Invalid start_date_iso=%s", start_date_iso)
        return 0

    total = 0
    for i in range(days_ahead + 1):
        ds = (base + timedelta(days=i)).strftime("%Y-%m-%d")
        items = fetch_overtime_soccer(ds)
        cnt = upsert_overtime_games(items)
        logger.info("[overtime] %s: upserted %d rows", ds, cnt)
        total += cnt
    logger.info("[overtime] total upserted across range: %d", total)
    return total
