# utils/overtime.py
from __future__ import annotations
import os
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from utils.safe_get import safe_get
from utils.supabaseClient import supabase

logger = logging.getLogger(__name__)

# ========= v2 config =========
OT_BASE = os.getenv("OVERTIME_BASE_URL", "https://api.overtime.io").rstrip("/")
OT_KEY  = os.getenv("OVERTIME_API_KEY")           # required
OT_NET  = os.getenv("OVERTIME_NETWORK_ID")        # required by v2 routes
OT_SPORT = os.getenv("OVERTIME_SPORT", "soccer")  # default soccer

HEADERS = {"Authorization": f"Bearer {OT_KEY}"} if OT_KEY else {}

# ========= helpers =========
def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _decimal_from_any(node: Any) -> Optional[float]:
    """
    Try to obtain DECIMAL odds. Fall back to normalizedImplied (1/p) if needed.
    Accepts structures like:
      {"decimal": 1.95} or {"normalizedImplied": 0.5123} or just 1.95
    """
    if isinstance(node, (int, float)):
        return _to_float(node)
    if isinstance(node, dict):
        dec = node.get("decimal")
        if dec is not None:
            return _to_float(dec)
        ni = node.get("normalizedImplied") or node.get("implied")
        if ni is not None:
            try:
                p = float(ni)
                if p > 0:
                    return 1.0 / p
            except Exception:
                pass
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

# ========= markets parsing (v2) =========
def _extract_1x2(market: Any) -> Dict[str, Optional[float]]:
    """
    v2 1x2 odds may arrive as:
      - list of selections with {label/outcome, odds:{decimal,...}}
      - dict {home, draw, away} where values can be odds or objects
    """
    out = {"home": None, "draw": None, "away": None}
    if isinstance(market, list):
        for sel in market:
            k = (sel.get("label") or sel.get("outcome") or sel.get("value") or "").lower()
            o = _decimal_from_any(sel.get("odds") or sel.get("price") or sel.get("odd"))
            if k in ("home", "1"): out["home"] = o
            elif k in ("draw", "x"): out["draw"] = o
            elif k in ("away", "2"): out["away"] = o
    elif isinstance(market, dict):
        out["home"] = _decimal_from_any(_first(market.get("home"), market.get("Home"), market.get("1"), market.get("H")))
        out["draw"] = _decimal_from_any(_first(market.get("draw"), market.get("Draw"), market.get("X"), market.get("D")))
        out["away"] = _decimal_from_any(_first(market.get("away"), market.get("Away"), market.get("2"), market.get("A")))
    return out

def _extract_btts(market: Any) -> Dict[str, Optional[float]]:
    out = {"yes": None, "no": None}
    if isinstance(market, list):
        for sel in market:
            k = (sel.get("label") or sel.get("value") or "").lower()
            o = _decimal_from_any(sel.get("odds") or sel.get("price") or sel.get("odd"))
            if k == "yes": out["yes"] = o
            elif k == "no": out["no"] = o
    elif isinstance(market, dict):
        out["yes"] = _decimal_from_any(_first(market.get("yes"), market.get("Yes")))
        out["no"]  = _decimal_from_any(_first(market.get("no"),  market.get("No")))
    return out

def _extract_ou25(market: Any) -> Dict[str, Optional[float]]:
    """
    Accept: dict with keys, or list of selections where label includes "Over 2.5"/"Under 2.5"
    """
    out = {"over_2_5": None, "under_2_5": None}
    if isinstance(market, list):
        for sel in market:
            label = (sel.get("label") or sel.get("value") or "").lower()
            o = _decimal_from_any(sel.get("odds") or sel.get("price") or sel.get("odd"))
            if "over" in label and "2.5" in label:
                out["over_2_5"] = o
            elif "under" in label and "2.5" in label:
                out["under_2_5"] = o
    elif isinstance(market, dict):
        out["over_2_5"]  = _decimal_from_any(_first(market.get("over_2_5"), market.get("Over 2.5"), market.get("over"), market.get("Over")))
        out["under_2_5"] = _decimal_from_any(_first(market.get("under_2_5"), market.get("Under 2.5"), market.get("under"), market.get("Under")))
    return out

def _pull_market(m: Dict[str, Any], *aliases: str):
    """
    Try to fetch a submarket by a set of alias keys (robust to naming).
    """
    mk = m.get("markets") or {}
    for name in aliases:
        if name in mk:
            return mk.get(name)
    # some payloads already put the selections at top-level
    for name in aliases:
        if name in m:
            return m.get(name)
    return None

def _normalize_v2_item(m: Dict[str, Any], date_iso: str) -> Optional[Dict[str, Any]]:
    """
    v2 'markets' feed entries often include team names at top level:
      homeTeam / awayTeam OR home / away, and a markets bag.
    """
    home = _first(m.get("homeTeam"), m.get("home"), m.get("home_name"))
    away = _first(m.get("awayTeam"), m.get("away"), m.get("away_name"))
    if isinstance(home, dict): home = home.get("name")
    if isinstance(away, dict): away = away.get("name")
    if not home or not away:
        return None

    league = _first(m.get("league"), (m.get("competition") or {}).get("name"))
    country = _first(m.get("country"), (m.get("competition") or {}).get("country"))
    kickoff = _first(m.get("maturityIso"), m.get("kickoff"), m.get("commence_time"), m.get("startTime"))
    provider_match_id = _first(m.get("gameId"), m.get("id"), m.get("match_id"), m.get("event_id"))

    # Markets: be liberal with aliases
    x12  = _extract_1x2(_pull_market(m, "1x2", "match_winner", "winner"))
    btts = _extract_btts(_pull_market(m, "btts", "both_teams_to_score"))
    ou   = _extract_ou25(_pull_market(m, "ou25", "over_under_2_5", "totals", "goals_over_under"))

    return {
        "provider": "overtime",
        "provider_key": _provider_key(date_iso, home, away),
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
        "raw": m,
    }

# ========= fetchers =========
def _markets_url() -> Optional[str]:
    if not OT_NET:
        logger.error("OVERTIME_NETWORK_ID is required for v2 API paths.")
        return None
    # Format from docs: /overtime-v2/networks/{NETWORK_ID}/markets
    return f"{OT_BASE}/overtime-v2/networks/{OT_NET}/markets"

def fetch_overtime_markets_v2(date_iso: str) -> List[Dict[str, Any]]:
    """
    Pull all markets for a date & sport (ungrouped so selections are explicit).
    Docs show this route pattern with NETWORK_ID and 'ungroup=true'. :contentReference[oaicite:2]{index=2}
    """
    url = _markets_url()
    if not url or not OT_KEY:
        return []

    params = {"ungroup": "true", "sport": OT_SPORT, "date": date_iso}
    resp = safe_get(url, headers=HEADERS, params=params)
    if resp is None:
        logger.info("[overtime v2] no response for %s", date_iso)
        return []

    try:
        data = resp.json() or {}
        # Some accounts return a plain list; others embed in "data" or similar.
        items = data if isinstance(data, list) else (
            data.get("data") or data.get("results") or data.get("matches") or data.get("items") or []
        )
        if not isinstance(items, list):
            logger.info("[overtime v2] unexpected payload shape for %s", url)
            return []
        out: List[Dict[str, Any]] = []
        for it in items:
            norm = _normalize_v2_item(it, date_iso)
            if norm:
                out.append(norm)
        logger.info("[overtime v2] %s: normalized %d rows", date_iso, len(out))
        return out
    except Exception as e:
        logger.warning("[overtime v2] parse error for %s: %s", date_iso, e)
        return []

# ========= storage =========
def upsert_overtime_games(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    try:
        res = supabase.table("overtime_games").upsert(
            rows,
            on_conflict="provider,provider_key",
        ).execute()
        data = getattr(res, "data", None)
        if isinstance(data, list):
            return len(data)
        return len(rows)
    except Exception as e:
        logger.error("upsert_overtime_games failed: %s", e)
        return 0

# ========= range helper =========
def refresh_overtime_games_for_range(start_date_iso: str, days_ahead: int) -> int:
    """
    Fetch & store v2 markets for [start_date .. start_date+days_ahead].
    """
    try:
        base = datetime.strptime(start_date_iso, "%Y-%m-%d")
    except Exception:
        logger.error("Invalid start_date_iso=%s", start_date_iso)
        return 0

    total = 0
    for i in range(days_ahead + 1):
        ds = (base + timedelta(days=i)).strftime("%Y-%m-%d")
        items = fetch_overtime_markets_v2(ds)
        cnt = upsert_overtime_games(items)
        logger.info("[overtime v2] %s: upserted %d rows", ds, cnt)
        total += cnt
    logger.info("[overtime v2] total upserted across range: %d", total)
    return total
