# utils/overtime_integration.py
# -*- coding: utf-8 -*-

import os
import re
import json
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple, List

import requests

log = logging.getLogger(__name__)

# ── ENV ───────────────────────────────────────────────────────────────────────
OVERTIME_API_BASE = os.getenv("OVERTIME_API_BASE", "https://api.overtime.io")
OVERTIME_API_KEY = os.getenv("OVERTIME_API_KEY", "")
OVERTIME_NETWORK_ID = int(os.getenv("OVERTIME_NETWORK_ID", "10"))  # 10=Optimism
OVERTIME_SPORT = os.getenv("OVERTIME_SPORT", "Soccer")
OVERTIME_REFERRER_ID = os.getenv("OVERTIME_REFERRER_ID", "").strip()

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_KEY")
    or os.getenv("SUPABASE_ANON_KEY")
)
OT_TABLE = os.getenv("MATCHES_OT_TABLE", "matches_ot")

# ── HTTP sessions ────────────────────────────────────────────────────────────
_ot = requests.Session()
if OVERTIME_API_KEY:
    _ot.headers.update({"x-api-key": OVERTIME_API_KEY})

_sb = requests.Session()
if SUPABASE_KEY:
    _sb.headers.update({
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    })

# ── helpers ──────────────────────────────────────────────────────────────────
def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

def _bet_urls(game_id: str, network_id: int) -> Dict[str, str]:
    base = "https://www.overtimemarkets.xyz/#/markets"
    url = f"{base}?gameId={game_id}&networkId={network_id}"
    if OVERTIME_REFERRER_ID:
        url += f"&referrerId={OVERTIME_REFERRER_ID}"
    return {"bet_url": url, "bet_url_fallback": base}

def _overtime_get_markets(network_id: int) -> List[dict]:
    if not OVERTIME_API_KEY:
        raise RuntimeError("Missing OVERTIME_API_KEY")
    url = f"{OVERTIME_API_BASE.rstrip('/')}/overtime-v2/networks/{network_id}/markets"
    r = _ot.get(url, params={"ungroup": "true"}, timeout=40)
    r.raise_for_status()
    data = r.json()
    # keep only our sport and open/ongoing markets
    out: List[dict] = []
    for m in (data if isinstance(data, list) else []):
        try:
            if m.get("sport") != OVERTIME_SPORT:
                continue
            status_code = (m.get("statusCode") or "").lower()
            if status_code not in {"open", "ongoing"} and not m.get("isOpen", False):
                continue
            out.append(m)
        except Exception:
            continue
    return out

def _get_name_lower(o: dict) -> str:
    # Try common outcome name keys; fall back to label/side
    for k in ("name", "outcomeName", "label", "side", "key"):
        v = o.get(k)
        if isinstance(v, str):
            return v.lower()
    return ""

def _safe_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _extract_three_way_winner(m: dict) -> Optional[dict]:
    """Map 3-way winner odds to {home, draw, away}."""
    odds = m.get("odds") or []
    if len(odds) < 3:
        return None
    # Prefer names if present
    home = draw = away = None
    for o in odds:
        n = _get_name_lower(o)
        dec = _safe_float(o.get("decimal"))
        if dec is None:
            continue
        if n in ("home", "1", "team1", "local"):
            home = dec
        elif n in ("draw", "x"):
            draw = dec
        elif n in ("away", "2", "team2", "visitor"):
            away = dec
    # Fallback by position if names missing
    if home is None or draw is None or away is None:
        try:
            home = home or _safe_float(odds[0].get("decimal"))
            draw = draw or _safe_float(odds[1].get("decimal"))
            away = away or _safe_float(odds[2].get("decimal"))
        except Exception:
            return None
    if None in (home, draw, away):
        return None
    return {"home": home, "draw": draw, "away": away}

def _parse_line_to_float(x) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        try:
            return float(x.replace(",", "."))
        except Exception:
            return None
    return None

def _looks_like_ou25_market(m: dict) -> bool:
    # Heuristics: market name mentions over/under + 2.5 OR explicit line == 2.5
    name = " ".join(str(m.get(k, "")) for k in ("marketName", "marketLabel", "type", "marketType")).lower()
    line = _parse_line_to_float(m.get("line") or m.get("total") or m.get("handicap"))
    has_ou = any(tok in name for tok in ("over", "under", "total"))
    has_25 = ("2.5" in name) or (line is not None and abs(line - 2.5) < 1e-6)
    return has_ou and has_25

def _extract_ou25(m: dict) -> Optional[dict]:
    """Map OU 2.5 to {over, under}."""
    if not _looks_like_ou25_market(m):
        return None
    odds = m.get("odds") or []
    if len(odds) < 2:
        return None
    over = under = None
    for o in odds:
        n = _get_name_lower(o)
        dec = _safe_float(o.get("decimal"))
        if dec is None:
            continue
        if n.startswith("over"):
            over = dec
        elif n.startswith("under"):
            under = dec
    # Fallback by index
    if over is None or under is None:
        try:
            over = over or _safe_float(odds[0].get("decimal"))
            under = under or _safe_float(odds[1].get("decimal"))
        except Exception:
            return None
    if None in (over, under):
        return None
    return {"over": over, "under": under}

def _looks_like_btts_market(m: dict) -> bool:
    # Heuristics: name mentions 'both teams to score'/'btts'/'gg'
    name = " ".join(str(m.get(k, "")) for k in ("marketName", "marketLabel", "type", "marketType")).lower()
    return any(tok in name for tok in ("both teams to score", "btts", "gg"))

def _extract_btts(m: dict) -> Optional[dict]:
    """Map BTTS to {yes, no}."""
    if not _looks_like_btts_market(m):
        return None
    odds = m.get("odds") or []
    if len(odds) < 2:
        return None
    yes = no = None
    for o in odds:
        n = _get_name_lower(o)
        dec = _safe_float(o.get("decimal"))
        if dec is None:
            continue
        if n in ("yes", "y", "gg", "true"):
            yes = dec
        elif n in ("no", "n", "ng", "false"):
            no = dec
    # Fallback by index
    if yes is None or no is None:
        try:
            yes = yes or _safe_float(odds[0].get("decimal"))
            no = no or _safe_float(odds[1].get("decimal"))
        except Exception:
            return None
    if None in (yes, no):
        return None
    return {"yes": yes, "no": no}

def _bundle_odds_for_game(all_markets: List[dict], game_id: str) -> Dict[str, Any]:
    """Collect winner + OU2.5 + BTTS from any markets that share the same gameId (or are child markets)."""
    out: Dict[str, Any] = {}
    siblings = [m for m in all_markets if str(m.get("gameId")) == str(game_id)]
    for m in siblings:
        try:
            # 3-way winner
            if "winner" not in out:
                winner = _extract_three_way_winner(m)
                if winner:
                    out["winner"] = winner
            # OU 2.5
            if "ou_2_5" not in out:
                ou = _extract_ou25(m)
                if ou:
                    out["ou_2_5"] = ou
            # BTTS
            if "btts" not in out:
                btts = _extract_btts(m)
                if btts:
                    out["btts"] = btts
            # Also scan child markets if present
            for cm in m.get("childMarkets") or []:
                if "ou_2_5" not in out:
                    ou = _extract_ou25(cm)
                    if ou:
                        out["ou_2_5"] = ou
                if "btts" not in out:
                    btts = _extract_btts(cm)
                    if btts:
                        out["btts"] = btts
        except Exception:
            continue
    return out

def _closest_match(markets: List[dict], home: str, away: str, kickoff_iso: str) -> Optional[dict]:
    hn, an = _norm(home), _norm(away)
    try:
        t_target = datetime.fromisoformat(kickoff_iso.replace("Z", "+00:00"))
    except Exception:
        t_target = None

    best, best_score = None, float("-inf")
    for m in markets:
        mh, ma = _norm(m.get("homeTeam", "")), _norm(m.get("awayTeam", ""))
        if not mh or not ma:
            continue
        team_ok = ((hn == mh and an == ma) or (hn in mh and an in ma) or (mh in hn and ma in an))
        if not team_ok:
            continue

        score = 0.0
        if t_target and m.get("maturityDate"):
            try:
                t_m = datetime.fromisoformat(m["maturityDate"].replace("Z", "+00:00"))
                diff = abs((t_m - t_target).total_seconds())
                score -= diff / 60.0  # minutes distance
            except Exception:
                pass
        if len(m.get("odds") or []) >= 3:
            score += 50.0
        if score > best_score:
            best, best_score = m, score
    return best

def _sb_upsert_row(row: Dict[str, Any]) -> Tuple[int, str]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

    # native upsert
    url = f"{SUPABASE_URL}/rest/v1/{OT_TABLE}?on_conflict=game_id"
    r = _sb.post(url, data=json.dumps(row))
    if r.status_code < 400:
        try:
            payload = r.json()
            return (len(payload) if isinstance(payload, list) else 1), "OK"
        except Exception:
            return 1, "OK"

    # 42P10: no unique index → manual upsert
    msg = (r.text or "").lower()
    if "no unique or exclusion constraint" in msg or ("upsert" in msg and "unique" in msg):
        del_url = f"{SUPABASE_URL}/rest/v1/{OT_TABLE}?game_id=eq.{row['game_id']}"
        _sb.delete(del_url, headers={"Prefer": "return=minimal"})
        ins_url = f"{SUPABASE_URL}/rest/v1/{OT_TABLE}"
        ins = _sb.post(ins_url, data=json.dumps(row))
        if ins.status_code < 400:
            try:
                payload = ins.json()
                return (len(payload) if isinstance(payload, list) else 1), "MANUAL_UPSERT_OK"
            except Exception:
                return 1, "MANUAL_UPSERT_OK"
        return 0, f"INSERT_FAIL:{ins.text}"

    return 0, f"HTTP_{r.status_code}:{r.text}"

# ── public API ────────────────────────────────────────────────────────────────
def build_matches_ot_row(ot_market: dict, odds_bundle: Dict[str, Any], fixture_id: Optional[int] = None) -> Dict[str, Any]:
    urls = _bet_urls(ot_market.get("gameId"), int(ot_market.get("networkId") or OVERTIME_NETWORK_ID))
    return {
        "game_id": ot_market.get("gameId"),
        "network_id": int(ot_market.get("networkId") or OVERTIME_NETWORK_ID),
        "sport": ot_market.get("sport"),
        "league": ot_market.get("leagueName"),
        "maturity": ot_market.get("maturityDate"),
        "home_team": ot_market.get("homeTeam"),
        "away_team": ot_market.get("awayTeam"),
        # Odds JSONB now bundles: winner, ou_2_5, btts (include only found keys)
        "odds": odds_bundle,
        "bet_url": urls["bet_url"],
        "bet_url_fallback": urls["bet_url_fallback"],
        "fixture_id": int(fixture_id) if fixture_id is not None else None,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

def upsert_overtime_from_fixture(match_json: dict) -> Tuple[int, str]:
    """
    From your existing fixture JSON:
    1) fetch all Overtime markets
    2) find best match by team names & kickoff
    3) collect winner + OU2.5 + BTTS for the same gameId
    4) upsert to public.matches_ot
    Returns (written_count, reason).
    """
    home = (match_json.get("home_team") or {}).get("name") or (match_json.get("home_team") or {}).get("team")
    away = (match_json.get("away_team") or {}).get("name") or (match_json.get("away_team") or {}).get("team")
    kickoff = match_json.get("date")
    fixture_id = match_json.get("fixture_id")

    if not (home and away and kickoff):
        return 0, "MISSING_FIXTURE_FIELDS"

    markets = _overtime_get_markets(OVERTIME_NETWORK_ID)
    best = _closest_match(markets, home, away, kickoff)
    if not best:
        return 0, "NO_MATCH_FOUND"

    # Bundle all relevant odds for this gameId
    game_id = best.get("gameId")
    odds_bundle = _bundle_odds_for_game(markets, game_id)
    if not odds_bundle:
        # Still store the row with minimal info (so you have the bet link)
        odds_bundle = {}

    best = dict(best)
    best["networkId"] = best.get("networkId") or OVERTIME_NETWORK_ID

    row = build_matches_ot_row(best, odds_bundle=odds_bundle, fixture_id=fixture_id)
    written, reason = _sb_upsert_row(row)
    return written, reason
