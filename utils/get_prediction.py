# utils/overtime_integration.py
# -*- coding: utf-8 -*-
import os
import re
import json
import math
import logging
import unicodedata
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
from typing import Any, Dict, List, Tuple, Optional

import requests

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Config (ENV)
# ─────────────────────────────────────────────────────────────────────────────
SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")  # MUST be service role for writes under RLS
    or os.getenv("SUPABASE_KEY")
    or os.getenv("SUPABASE_ANON_KEY")
)

OVERTIME_API_URL = os.getenv("OVERTIME_API_URL", "https://api.overtime.io")
OVERTIME_API_KEY = os.getenv("OVERTIME_API_KEY", "")
OVERTIME_NETWORK_ID = int(os.getenv("OVERTIME_NETWORK_ID", "8453"))     # Base by default
OVERTIME_NETWORK_NAME = os.getenv("OVERTIME_NETWORK_NAME", "base")
OT_TABLE = os.getenv("OT_TABLE", "matches_ot")

# Optional: base URL to show a generic "view on Overtime" link (no guaranteed deep link)
OVERTIME_MARKET_BASE_URL = os.getenv("OVERTIME_MARKET_BASE_URL", "").rstrip("/")

# Linker tunables
OVERTIME_MATCH_WINDOW_MIN = int(os.getenv("OVERTIME_MATCH_WINDOW_MIN", "2160"))   # ±36h
OVERTIME_MATCH_MIN_RATIO  = float(os.getenv("OVERTIME_MATCH_MIN_RATIO", "0.60"))
OVERTIME_DEBUG_TOPK       = int(os.getenv("OVERTIME_DEBUG_TOPK", "5"))
LINK_FIXTURE_HORIZON_DAYS = int(os.getenv("LINK_FIXTURE_HORIZON_DAYS", "7"))
DB_READ_SINCE_HOURS       = int(os.getenv("DB_READ_SINCE_HOURS", "48"))
DB_READ_AHEAD_DAYS        = int(os.getenv("DB_READ_AHEAD_DAYS", "10"))

# ─────────────────────────────────────────────────────────────────────────────
# HTTP sessions
# ─────────────────────────────────────────────────────────────────────────────
_sb = requests.Session()
if SUPABASE_KEY:
    _sb.headers.update({
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    })

_ot = requests.Session()
if OVERTIME_API_KEY:
    _ot.headers.update({
        "x-api-key": OVERTIME_API_KEY,
        "Content-Type": "application/json",
    })

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _iso(dt: Any) -> Optional[str]:
    """Accepts ISO string or epoch seconds; returns ISO8601 UTC or None."""
    if dt is None:
        return None
    if isinstance(dt, (int, float)):
        return datetime.fromtimestamp(dt, tz=timezone.utc).isoformat()
    s = str(dt)
    try:
        # normalize Z
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except Exception:
        try:
            # maybe numeric string epoch
            if s.isdigit():
                return datetime.fromtimestamp(int(s), tz=timezone.utc).isoformat()
        except Exception:
            pass
    return None

def _get_supabase(path: str) -> List[dict]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY/KEY/ANON_KEY")
    r = _sb.get(f"{SUPABASE_URL}{path}", timeout=30)
    r.raise_for_status()
    return r.json() if r.text else []

def _post_supabase(path: str, payload: Any) -> requests.Response:
    return _sb.post(f"{SUPABASE_URL}{path}", data=json.dumps(payload))

def _delete_supabase(path: str) -> requests.Response:
    return _sb.delete(f"{SUPABASE_URL}{path}", headers={"Prefer": "return=minimal"})

# ─────────────────────────────────────────────────────────────────────────────
# Overtime V2 API calls
# ─────────────────────────────────────────────────────────────────────────────
def _ot_url(endpoint: str) -> str:
    # Ex: /overtime-v2/networks/{id}/markets
    return f"{OVERTIME_API_URL}/overtime-v2/networks/{OVERTIME_NETWORK_ID}{endpoint}"

def _fetch_markets_ungrouped_safely(page: Optional[int] = None) -> List[dict]:
    """
    Fetch ungrouped markets for soccer. If the API supports pagination, honor 'page'.
    We keep this defensive: if the API supports 'sport=soccer', include it; otherwise omit.
    """
    params = {"ungroup": "true", "sport": "soccer"}
    if page is not None:
        params["page"] = page
    try:
        r = _ot.get(_ot_url("/markets"), params=params, timeout=30)
        r.raise_for_status()
        data = r.json() if r.text else []
        # Some APIs nest under 'markets' or 'data'; normalize
        if isinstance(data, dict):
            if "markets" in data and isinstance(data["markets"], list):
                return data["markets"]
            if "data" in data and isinstance(data["data"], list):
                return data["data"]
        return data if isinstance(data, list) else []
    except Exception as e:
        log.warning("Overtime markets fetch failed (page=%s): %s", page, e)
        return []

# ─────────────────────────────────────────────────────────────────────────────
# Market extraction / odds mapping
# ─────────────────────────────────────────────────────────────────────────────
def _norm_name(s: str) -> str:
    return (s or "").strip()

def _float_or_none(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _positions_odds(odds_arr: List[dict]) -> List[float]:
    """Extract normalizedImplied array (floats) from odds array of objects."""
    out = []
    for o in odds_arr or []:
        v = _float_or_none(o.get("normalizedImplied"))
        if v is None:
            # also try direct float or 'price'
            v = _float_or_none(o.get("price"))
        if v is not None:
            out.append(v)
    return out

def _infer_kind_and_map(m: dict) -> Tuple[Optional[str], Optional[dict]]:
    """
    Infer market kind and map to our canonical odds structure.
    Returns (kind, odds_mapping) where:
      - kind in {"OU_2_5","BTTS","WINNER"} (or None)
      - odds_mapping is:
         * OU_2_5: {"over": x, "under": y}
         * BTTS : {"yes": x, "no": y}
         * WINNER: {"home": x, "draw": y, "away": z}
    """
    name = (_norm_name(m.get("name") or m.get("marketName"))).lower()
    type_id = str(m.get("typeId") or "").upper()
    line = _float_or_none(m.get("line"))

    odds = _positions_odds(m.get("odds") or [])
    # Helpers for position labels if present
    pos_labels = [str(p.get("label") or p.get("position") or "").lower()
                  for p in (m.get("positions") or [])]

    # Over / Under 2.5
    if (line is not None and abs(line - 2.5) < 1e-9) and (
        "total" in name or "over/under" in name or "goals" in name or type_id in {"7", "TOTALS", "TOTAL_GOALS"}
    ):
        if len(odds) >= 2:
            # If labels provided, try to map correctly
            over_i = 0
            under_i = 1
            if pos_labels:
                for i, lbl in enumerate(pos_labels):
                    if "over" in lbl: over_i = i
                    if "under" in lbl: under_i = i
            try:
                return "OU_2_5", {"over": odds[over_i], "under": odds[under_i]}
            except Exception:
                return "OU_2_5", {"over": odds[0], "under": odds[1]}
        return "OU_2_5", None

    # Both Teams To Score
    if ("both teams to score" in name) or (type_id in {"BTTS", "17"}):
        if len(odds) >= 2:
            yes_i = 0
            no_i = 1
            if pos_labels:
                for i, lbl in enumerate(pos_labels):
                    if "yes" == lbl: yes_i = i
                    if "no" == lbl:  no_i = i
            try:
                return "BTTS", {"yes": odds[yes_i], "no": odds[no_i]}
            except Exception:
                return "BTTS", {"yes": odds[0], "no": odds[1]}
        return "BTTS", None

    # Winner (1X2 / Moneyline / Match Result)
    if any(k in name for k in ["winner", "moneyline", "match result", "1x2", "result"]) or type_id in {"WINNER", "1X2", "3"}:
        # odds length: expect 2 (no draw league) or 3 (1x2)
        if len(odds) == 3:
            # Try mapping using labels when present
            h = d = a = None
            if pos_labels and len(pos_labels) == 3:
                for i, lbl in enumerate(pos_labels):
                    if lbl in ("home", "1", "team1"): h = odds[i]
                    elif lbl in ("draw", "x"): d = odds[i]
                    elif lbl in ("away", "2", "team2"): a = odds[i]
            if h is None or a is None:
                h, d, a = odds[0], odds[1], odds[2]
            return "WINNER", {"home": h, "draw": d, "away": a}
        elif len(odds) == 2:
            # two-way winner (no draw)
            h, a = odds[0], odds[1]
            return "WINNER", {"home": h, "away": a}
        return "WINNER", None

    return None, None

def _shape_trade_data(mkt: dict) -> dict:
    """
    Shape one market into a single-item 'quoteTradeData' entry compatible with
    the Overtime V2 Quote API and Sports AMM V2 `trade(...)` call.
    Fields aligned to docs:
      - sportId: use subLeagueId (per docs notes)
      - line: keep as float here (we'll scale *100 on-chain)
      - odds: normalizedImplied[] floats
    """
    return {
        "gameId": mkt.get("gameId"),
        "sportId": mkt.get("subLeagueId"),
        "typeId": mkt.get("typeId"),
        "maturity": mkt.get("maturity"),
        "status": mkt.get("status"),
        "line": mkt.get("line"),
        "playerId": (mkt.get("playerProps") or {}).get("playerId"),
        "odds": _positions_odds(mkt.get("odds") or []),
        "merkleProof": mkt.get("proof"),
        "combinedPositions": mkt.get("combinedPositions", []),
        "live": False,
    }

def _extract_trade_payloads(ungrouped: List[dict]) -> Tuple[Optional[dict], Optional[dict]]:
    """Find one OU 2.5 market and one BTTS market for trade payloads."""
    trade_ou = None
    trade_btts = None
    for m in ungrouped:
        kind, mapped = _infer_kind_and_map(m)
        if kind == "OU_2_5" and (trade_ou is None):
            if _float_or_none(m.get("line")) == 2.5:
                trade_ou = _shape_trade_data(m)
        elif kind == "BTTS" and (trade_btts is None):
            trade_btts = _shape_trade_data(m)
        if trade_ou and trade_btts:
            break
    return trade_ou, trade_btts

# ─────────────────────────────────────────────────────────────────────────────
# Ingestion into matches_ot
# ─────────────────────────────────────────────────────────────────────────────
def _build_bet_url(game_id: str) -> Optional[str]:
    if not OVERTIME_MARKET_BASE_URL:
        return None
    # No official per-market deep link; keep generic per-game path if your dapp defines it:
    return f"{OVERTIME_MARKET_BASE_URL}/game/{game_id}"

def _make_ot_row(game_id: str,
                 home: str, away: str, league: str,
                 maturity: Any,
                 grouped: Dict[str, dict],
                 trade_ou: Optional[dict],
                 trade_btts: Optional[dict]) -> dict:
    return {
        "game_id": str(game_id),
        "network_id": OVERTIME_NETWORK_ID,
        "maturity": _iso(maturity),
        "league": league,
        "home_team": home,
        "away_team": away,
        "odds": grouped,                  # {"winner": {...}, "ou_2_5": {...}, "btts": {...}}
        "bet_url": _build_bet_url(str(game_id)),
        "trade_ou_2_5": trade_ou,
        "trade_btts": trade_btts,
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
    }

def _upsert_matches_ot(rows: List[dict]) -> Tuple[int, str]:
    if not rows:
        return 0, "EMPTY"
    try:
        r = _post_supabase(f"/rest/v1/{OT_TABLE}?on_conflict=game_id", rows)
        if r.status_code < 400:
            return len(rows), "OK"
        # fallback manual upsert when unique constraint missing
        wrote = 0
        for row in rows:
            _delete_supabase(f"/rest/v1/{OT_TABLE}?game_id=eq.{row['game_id']}")
            ins = _post_supabase(f"/rest/v1/{OT_TABLE}", row)
            if ins.status_code < 400:
                wrote += 1
        return wrote, "FALLBACK_UPSERT"
    except Exception as e:
        log.error("❌ Upsert matches_ot failed: %s", e)
        return 0, "HTTP_EXCEPTION"

def _group_odds_from_ungrouped(markets: List[dict]) -> Tuple[Dict[str, dict], Dict[str, Any]]:
    """
    Collapse many ungrouped child markets into our compact odds dict + metadata
    Assumes all belong to the same game (you should pass only that game's markets).
    """
    out: Dict[str, dict] = {}
    meta = {"home": None, "away": None, "league": None, "maturity": None, "gameId": None}
    for m in markets:
        # capture meta
        meta["gameId"] = meta["gameId"] or m.get("gameId")
        meta["home"] = meta["home"] or m.get("homeTeam")
        meta["away"] = meta["away"] or m.get("awayTeam")
        meta["league"] = meta["league"] or (m.get("leagueName") or m.get("league"))
        meta["maturity"] = meta["maturity"] or m.get("maturity")

        kind, mapped = _infer_kind_and_map(m)
        if not kind or not mapped:
            continue
        if kind == "OU_2_5":
            out["ou_2_5"] = mapped
        elif kind == "BTTS":
            out["btts"] = mapped
        elif kind == "WINNER":
            out["winner"] = mapped

    return out, meta

def _group_by_game_id(markets: List[dict]) -> Dict[str, List[dict]]:
    g: Dict[str, List[dict]] = {}
    for m in markets:
        gid = str(m.get("gameId") or "")
        if not gid:
            # skip invalid
            continue
        g.setdefault(gid, []).append(m)
    return g

def ingest_all_overtime_soccer() -> Tuple[int, int]:
    """
    Fetch soccer markets from Overtime V2 (Base 8453 by default) and upsert into matches_ot.
    Returns: (games_seen, rows_written)
    """
    if not OVERTIME_API_KEY:
        log.warning("⚠️ OVERTIME_API_KEY not set; skipping Overtime ingest.")
        return 0, 0

    log.info("Overtime V2 ingest: network_id=%s (%s)", OVERTIME_NETWORK_ID, OVERTIME_NETWORK_NAME)

    # 1) Pull ungrouped markets (you can add pagination if needed)
    markets = _fetch_markets_ungrouped_safely()
    if not markets:
        log.info("No Overtime markets returned.")
        return 0, 0

    # 2) Group by gameId
    by_gid = _group_by_game_id(markets)
    rows: List[dict] = []

    # 3) For each game, build compact row
    for gid, items in by_gid.items():
        grouped_odds, meta = _group_odds_from_ungrouped(items)
        # Contract-ready payloads
        trade_ou, trade_btts = _extract_trade_payloads(items)

        row = _make_ot_row(
            game_id=gid,
            home=_norm_name(meta["home"]),
            away=_norm_name(meta["away"]),
            league=_norm_name(meta["league"]),
            maturity=meta["maturity"],
            grouped=grouped_odds,
            trade_ou=trade_ou,
            trade_btts=trade_btts,
        )
        rows.append(row)

    # 4) Upsert rows
    wrote, reason = _upsert_matches_ot(rows)
    log.info("Overtime matches_ot wrote: %s (reason=%s)", wrote, reason)
    return len(by_gid), wrote

# ─────────────────────────────────────────────────────────────────────────────
# LINKER: matches_ot ↔ fixtures
# ─────────────────────────────────────────────────────────────────────────────
_STOP = {"fc","cf","sc","ac","afc","cfc","club","ii","b","u18","u19","u20","u21","u23","women","ladies","the"}
_ABBR = {"utd":"united","st":"saint","st.":"saint","intl":"international","int'l":"international",
         "dep":"deportivo","ath":"athletic","rb":"rasenballsport","&":"and"}
_BUILTIN_ALIASES = {
    "riga fc":"riga","man utd":"manchester united","psg":"paris saint germain",
    "inter":"internazionale","newcastle utd":"newcastle united",
}

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def _load_aliases_from_db() -> Dict[str,str]:
    try:
        rows = _get_supabase("/rest/v1/ot_team_aliases?select=from_text,to_text")
        extra = { (r["from_text"] or "").strip().lower(): (r["to_text"] or "").strip().lower() for r in rows }
        m = dict(_BUILTIN_ALIASES); m.update({k:v for k,v in extra.items() if k and v}); return m
    except Exception:
        return dict(_BUILTIN_ALIASES)

def _tokens(name: str, aliases: Dict[str,str]) -> List[str]:
    s = _strip_accents((name or "").lower()).strip()
    s = aliases.get(s, s)
    s = re.sub(r"[^\w\s]", " ", s)
    out=[]
    for t in s.split():
        t=_ABBR.get(t,t)
        if t and t not in _STOP: out.append(t)
    return out

def _norm_for_match(name: str, aliases: Dict[str,str]) -> str:
    return "".join(_tokens(name, aliases))

def _ratio(a: str, b: str, aliases: Dict[str,str]) -> float:
    ta,tb=set(_tokens(a,aliases)), set(_tokens(b,aliases))
    if not ta or not tb: return 0.0
    jacc = len(ta & tb) / len(ta | tb)
    seq  = SequenceMatcher(None, _norm_for_match(a,aliases), _norm_for_match(b,aliases)).ratio()
    return 0.6*jacc + 0.4*seq

def _overlap(a: str, b: str, aliases: Dict[str,str]) -> bool:
    ta,tb=set(_tokens(a,aliases)), set(_tokens(b,aliases))
    return bool(ta and tb and (ta & tb))

def _minutes_diff(a_iso: str, b_iso: str) -> Optional[float]:
    try:
        ta = datetime.fromisoformat((a_iso or "").replace("Z","+00:00"))
        tb = datetime.fromisoformat((b_iso or "").replace("Z","+00:00"))
        return abs((ta - tb).total_seconds()) / 60.0
    except Exception:
        return None

def _best_fixture_match(ot_game: dict, fixtures: List[dict], aliases: Dict[str,str]) -> Tuple[Optional[dict], float, Optional[float], List[dict]]:
    mh, ma = ot_game.get("homeTeam",""), ot_game.get("awayTeam","")
    mdt    = ot_game.get("maturityDate","")
    scored=[]
    for f in fixtures:
        fx=f.get("fixture",{}); t=f.get("teams",{})
        h,a=(t.get("home") or {}).get("name",""), (t.get("away") or {}).get("name","")
        if not h or not a or not fx.get("date"): continue
        r1=_ratio(mh,h,aliases)*_ratio(ma,a,aliases)
        r2=_ratio(mh,a,aliases)*_ratio(ma,h,aliases)
        name_score = max(r1, r2)
        if ((_overlap(mh,h,aliases) and _overlap(ma,a,aliases)) or (_overlap(mh,a,aliases) and _overlap(ma,h,aliases))):
            name_score = max(name_score, 0.62)
        mdiff = _minutes_diff(mdt, fx["date"])
        time_score = 0 if mdiff is None else max(0, 1 - (mdiff / max(1, OVERTIME_MATCH_WINDOW_MIN)))
        score = 0.8*name_score + 0.2*time_score
        scored.append((score, f, name_score, mdiff))
    if not scored:
        return None, 0.0, None, []
    scored.sort(key=lambda x: x[0], reverse=True)
    best_s, best_fx, best_n, best_m = scored[0][0], scored[0][1], scored[0][2], scored[0][3]
    # topk debug (not returned to caller, can be logged if needed)
    topk = [{
        "score": round(s,3),
        "fixture_id": (fx.get("fixture") or {}).get("id"),
        "name_score": round(ns,3),
        "minutes_diff": md,
        "home": (fx.get("teams") or {}).get("home",{}).get("name"),
        "away": (fx.get("teams") or {}).get("away",{}).get("name"),
        "kickoff": (fx.get("fixture") or {}).get("date")
    } for s, fx, ns, md in scored[:OVERTIME_DEBUG_TOPK]]
    return best_fx, best_s, best_m, topk

def _sb_list_matches_ot(since_iso: str, until_iso: str, page_size: int = 1000) -> List[dict]:
    rows, offset = [], 0
    while True:
        url = (
            f"/rest/v1/{OT_TABLE}"
            f"?select=game_id,home_team,away_team,maturity"
            f"&maturity=gte.{since_iso}&maturity=lte.{until_iso}"
            f"&order=maturity.asc&limit={page_size}&offset={offset}"
        )
        batch = _get_supabase(url)
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows

def _sb_upsert_link(game_id: str, fixture_id: int, confidence: float, matched_by: str = "auto") -> Tuple[int, str]:
    url = "/rest/v1/ot_links?on_conflict=game_id"
    payload = {
        "game_id": str(game_id),
        "fixture_id": int(fixture_id),
        "confidence": float(confidence),
        "matched_by": matched_by,
    }
    r = _post_supabase(url, payload)
    if r.status_code < 400:
        return 1, "OK"
    # manual upsert fallback
    _delete_supabase(f"/rest/v1/ot_links?game_id=eq.{game_id}")
    ins = _post_supabase("/rest/v1/ot_links", payload)
    if ins.status_code < 400:
        return 1, "INSERT_OK"
    return 0, f"HTTP_{ins.status_code}:{ins.text}"

def link_overtime_to_fixtures_from_db(window_days: int = LINK_FIXTURE_HORIZON_DAYS) -> Tuple[int, int, int]:
    """
    Link ONLY games already present in matches_ot to fixtures (today..+window_days).
    Returns: (games_seen, linked_count, skipped_count)
    """
    aliases = _load_aliases_from_db()

    now = datetime.now(timezone.utc)
    since = (now - timedelta(hours=DB_READ_SINCE_HOURS)).isoformat()
    until = (now + timedelta(days=DB_READ_AHEAD_DAYS)).isoformat()

    # 1) OT games from DB
    try:
        ot_games = _sb_list_matches_ot(since, until)
    except Exception as e:
        log.error("Reading matches_ot failed: %s", e)
        return 0, 0, 0

    # 2) Fixtures today .. +window_days (use your existing fetch_fixtures)
    from utils.get_football_data import fetch_fixtures  # local import to avoid cycles
    fixtures: List[dict] = []
    for i in range(window_days + 1):
        d = (now + timedelta(days=i)).strftime("%Y-%m-%d")
        try:
            fx = fetch_fixtures(d) or []
            fixtures.extend(fx)
        except Exception as e:
            log.warning("fetch_fixtures(%s) failed: %s", d, e)

    if not fixtures:
        log.warning("linker: no fixtures fetched for next %s days.", window_days)
        return len(ot_games), 0, len(ot_games)

    linked = skipped = 0
    for g in ot_games:
        game = {
            "gameId": g.get("game_id"),
            "homeTeam": g.get("home_team"),
            "awayTeam": g.get("away_team"),
            "maturityDate": g.get("maturity"),
        }
        best_fx, score, mdiff, topk = _best_fixture_match(game, fixtures, aliases)
        time_ok = (mdiff is not None and mdiff <= OVERTIME_MATCH_WINDOW_MIN)
        if not (best_fx and score >= OVERTIME_MATCH_MIN_RATIO and time_ok):
            if OVERTIME_DEBUG_TOPK and topk:
                log.info("LINK SKIP gid=%s score=%.3f mdiff=%s topk=%s",
                         g.get("game_id"), score, mdiff, json.dumps(topk, ensure_ascii=False))
            skipped += 1
            continue

        fixture_id = (best_fx.get("fixture") or {}).get("id")
        if not fixture_id:
            skipped += 1
            continue

        w, reason = _sb_upsert_link(str(g.get("game_id")), int(fixture_id), float(score), matched_by="auto")
        if w > 0:
            linked += 1
        else:
            log.warning("Link upsert failed gid=%s→fixture=%s: %s", g.get("game_id"), fixture_id, reason)

    log.info("🔗 Linking summary: games=%s, linked=%s, skipped=%s", len(ot_games), linked, skipped)
    return len(ot_games), linked, skipped
