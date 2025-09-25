# utils/overtime_integration.py
# -*- coding: utf-8 -*-

import os
import re
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple, List

import requests

log = logging.getLogger(__name__)

# ── ENV ───────────────────────────────────────────────────────────────────────
OVERTIME_API_BASE = os.getenv("OVERTIME_API_BASE", "https://api.overtime.io")
OVERTIME_API_KEY  = os.getenv("OVERTIME_API_KEY", "")
OVERTIME_NETWORK  = int(os.getenv("OVERTIME_NETWORK_ID", "10"))  # 10 = Optimism
OVERTIME_SPORT    = os.getenv("OVERTIME_SPORT", "Soccer")
OVERTIME_REFERRER = os.getenv("OVERTIME_REFERRER_ID", "").strip()

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
# --- COMPACT DB LINKER -------------------------------------------------------
import os, re, json, unicodedata, logging, requests
from difflib import SequenceMatcher
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_ANON_KEY")
OT_TABLE     = os.getenv("OT_TABLE", "matches_ot")
_s = requests.Session()
if SUPABASE_KEY:
    _s.headers.update({"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type":"application/json"})

# Tunables
OVERTIME_MATCH_WINDOW_MIN = int(os.getenv("OVERTIME_MATCH_WINDOW_MIN", "2160"))   # ±36h
OVERTIME_MATCH_MIN_RATIO  = float(os.getenv("OVERTIME_MATCH_MIN_RATIO", "0.60"))
LINK_FIXTURE_HORIZON_DAYS = int(os.getenv("LINK_FIXTURE_HORIZON_DAYS", "7"))

_STOP = {"fc","cf","sc","ac","afc","cfc","club","ii","b","u18","u19","u20","u21","u23","women","ladies","the"}
_ABBR = {"utd":"united","st":"saint","st.":"saint","intl":"international","int'l":"international","dep":"deportivo","ath":"athletic","rb":"rasenballsport","&":"and"}
_BUILTIN_ALIASES = {"riga fc":"riga","man utd":"manchester united","psg":"paris saint germain","inter":"internazionale","newcastle utd":"newcastle united"}

def _get(path): r=_s.get(f"{SUPABASE_URL}{path}", timeout=25); r.raise_for_status(); return r.json() if r.text else []
def _post(path, payload): return _s.post(f"{SUPABASE_URL}{path}", data=json.dumps(payload))

def _aliases():
    try:
        rows = _get(f"/rest/v1/ot_team_aliases?select=from_text,to_text")
        m = { (r["from_text"] or "").strip().lower(): (r["to_text"] or "").strip().lower() for r in rows }
        z = dict(_BUILTIN_ALIASES); z.update({k:v for k,v in m.items() if k and v}); return z
    except Exception:
        return dict(_BUILTIN_ALIASES)

def _strip_accents(s): return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
def _tokens(name, ali):
    s = _strip_accents((name or "").lower()).strip()
    s = ali.get(s, s)
    s = re.sub(r"[^\w\s]", " ", s)
    out=[]
    for t in s.split():
        t=_ABBR.get(t,t)
        if t and t not in _STOP: out.append(t)
    return out
def _norm(name, ali): return "".join(_tokens(name, ali))
def _ratio(a,b,ali):
    ta,tb=set(_tokens(a,ali)),set(_tokens(b,ali))
    if not ta or not tb: return 0.0
    jacc=len(ta&tb)/len(ta|tb); seq=SequenceMatcher(None,_norm(a,ali),_norm(b,ali)).ratio()
    return 0.6*jacc+0.4*seq
def _overlap(a,b,ali): 
    ta,tb=set(_tokens(a,ali)),set(_tokens(b,ali)); return bool(ta and tb and (ta&tb))
def _minutes(a_iso,b_iso):
    try:
        ta=datetime.fromisoformat((a_iso or "").replace("Z","+00:00"))
        tb=datetime.fromisoformat((b_iso or "").replace("Z","+00:00"))
        return abs((ta-tb).total_seconds())/60.0
    except Exception: return None

def _best_match(ot, fixtures, ali):
    mh,ma,mdt = ot.get("homeTeam",""), ot.get("awayTeam",""), ot.get("maturityDate","")
    best=None; best_s=0; best_m=None
    for f in fixtures:
        fx=f.get("fixture",{}); t=f.get("teams",{})
        h,a=(t.get("home") or {}).get("name",""), (t.get("away") or {}).get("name","")
        if not h or not a or not fx.get("date"): continue
        r1=_ratio(mh,h,ali)*_ratio(ma,a,ali); r2=_ratio(mh,a,ali)*_ratio(ma,h,ali)
        s=max(r1,r2)
        if ((_overlap(mh,h,ali) and _overlap(ma,a,ali)) or (_overlap(mh,a,ali) and _overlap(ma,h,ali))):
            s=max(s,0.62)
        md=_minutes(mdt, fx["date"]); ts=0 if md is None else max(0, 1-(md/max(1,OVERTIME_MATCH_WINDOW_MIN)))
        score=0.8*s+0.2*ts
        if score>best_s: best, best_s, best_m = f, score, md
    return best, best_s, best_m

def link_overtime_to_fixtures_from_db(window_days: int = LINK_FIXTURE_HORIZON_DAYS):
    ali=_aliases()
    now=datetime.now(timezone.utc)
    since=(now - timedelta(hours=48)).isoformat()
    until=(now + timedelta(days=10)).isoformat()

    # 1) OT games in DB
    ot = _get(f"/rest/v1/{OT_TABLE}?select=game_id,home_team,away_team,maturity&"
              f"maturity=gte.{since}&maturity=lte.{until}&order=maturity.asc&limit=2000")
    # 2) Fixtures today .. +window_days
    from utils.get_football_data import fetch_fixtures
    fixtures=[]
    for i in range(window_days+1):
        d=(now+timedelta(days=i)).strftime("%Y-%m-%d")
        try: fixtures.extend(fetch_fixtures(d) or [])
        except Exception as e: log.warning("fixtures(%s) failed: %s", d, e)

    linked=skipped=0
    for g in ot:
        best,score,md=_best_match({"homeTeam":g["home_team"],"awayTeam":g["away_team"],"maturityDate":g["maturity"]}, fixtures, ali)
        ok = best and score>=OVERTIME_MATCH_MIN_RATIO and (md is not None and md<=OVERTIME_MATCH_WINDOW_MIN)
        if not ok: skipped+=1; continue
        fx_id=(best.get("fixture") or {}).get("id")
        if not fx_id: skipped+=1; continue
        payload={"game_id":str(g["game_id"]),"fixture_id":int(fx_id),"confidence":float(score),"matched_by":"auto"}
        r=_post("/rest/v1/ot_links?on_conflict=game_id", payload)
        if r.status_code<400: linked+=1
        else:
            # manual upsert fallback
            _s.delete(f"{SUPABASE_URL}/rest/v1/ot_links?game_id=eq.{g['game_id']}", headers={"Prefer":"return=minimal"})
            r2=_post("/rest/v1/ot_links", payload)
            linked += 1 if r2.status_code<400 else 0

    log.info("🔗 Linking: games=%s, linked=%s, skipped=%s", len(ot), linked, skipped)
    return len(ot), linked, skipped




# ── helpers ──────────────────────────────────────────────────────────────────
def _safe_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _bet_url(game_id: str, network_id: int) -> str:
    base = "https://www.overtimemarkets.xyz/#/markets"
    url = f"{base}?gameId={game_id}&networkId={network_id}"
    if OVERTIME_REFERRER:
        url += f"&referrerId={OVERTIME_REFERRER}"
    return url

def _overtime_get_markets() -> List[dict]:
    """Fetch all open/ongoing Overtime markets for the configured network & sport."""
    if not OVERTIME_API_KEY:
        raise RuntimeError("Missing OVERTIME_API_KEY")
    url = f"{OVERTIME_API_BASE.rstrip('/')}/overtime-v2/networks/{OVERTIME_NETWORK}/markets"
    r = _ot.get(url, params={"ungroup": "true"}, timeout=45)
    r.raise_for_status()
    data = r.json()
    out = []
    for m in (data if isinstance(data, list) else []):
        try:
            if m.get("sport") != OVERTIME_SPORT:
                continue
            status = (m.get("statusCode") or "").lower()
            if status not in {"open", "ongoing"} and not m.get("isOpen", False):
                continue
            out.append(m)
        except Exception:
            continue
    return out

def _bundle_odds(markets_for_game: List[dict]) -> Dict[str, Any]:
    """
    Collect odds for a single gameId:
      - winner: {home, draw, away}
      - ou_2_5: {over, under}
      - btts:   {yes, no}
    Scans sibling markets and childMarkets.
    """
    out: Dict[str, Any] = {}

    def extract_winner(m):
        if "winner" in out: return
        odds = m.get("odds") or []
        if len(odds) >= 3:
            h = _safe_float(odds[0].get("decimal"))
            d = _safe_float(odds[1].get("decimal"))
            a = _safe_float(odds[2].get("decimal"))
            if None not in (h, d, a):
                out["winner"] = {"home": h, "draw": d, "away": a}

    def looks_ou25(m):
        name = " ".join(str(m.get(k, "")) for k in ("marketName", "marketLabel", "type", "marketType")).lower()
        line = m.get("line") or m.get("total") or m.get("handicap")
        try:
            line = float(str(line).replace(",", "."))
        except Exception:
            line = None
        return (("over" in name or "under" in name or "total" in name) and
                (("2.5" in name) or (line is not None and abs(line - 2.5) < 1e-6)))

    def extract_ou25(m):
        if "ou_2_5" in out: return
        if not looks_ou25(m): return
        odds = m.get("odds") or []
        if len(odds) >= 2:
            over = _safe_float(odds[0].get("decimal"))
            under = _safe_float(odds[1].get("decimal"))
            if None not in (over, under):
                out["ou_2_5"] = {"over": over, "under": under}

    def looks_btts(m):
        name = " ".join(str(m.get(k, "")) for k in ("marketName", "marketLabel", "type", "marketType")).lower()
        return ("both teams to score" in name) or ("btts" in name) or ("gg" in name)

    def extract_btts(m):
        if "btts" in out: return
        if not looks_btts(m): return
        odds = m.get("odds") or []
        if len(odds) >= 2:
            yes = _safe_float(odds[0].get("decimal"))
            no  = _safe_float(odds[1].get("decimal"))
            if None not in (yes, no):
                out["btts"] = {"yes": yes, "no": no}

    for m in markets_for_game:
        extract_winner(m)
        extract_ou25(m)
        extract_btts(m)
        for cm in (m.get("childMarkets") or []):
            extract_ou25(cm)
            extract_btts(cm)
    return out

def _sb_upsert_matches_ot(row: Dict[str, Any]) -> Tuple[int, str]:
    """Upsert one row into matches_ot. Falls back to manual upsert if unique index is missing."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return 0, "NO_SUPABASE"
    url = f"{SUPABASE_URL}/rest/v1/{OT_TABLE}?on_conflict=game_id"
    r = _sb.post(url, data=json.dumps(row))
    if r.status_code < 400:
        try:
            payload = r.json()
            return (len(payload) if isinstance(payload, list) else 1), "OK"
        except Exception:
            return 1, "OK"
    # Manual upsert if unique index missing
    if "no unique or exclusion constraint" in (r.text or "").lower():
        _sb.delete(f"{SUPABASE_URL}/rest/v1/{OT_TABLE}?game_id=eq.{row['game_id']}",
                   headers={"Prefer": "return=minimal"})
        ins = _sb.post(f"{SUPABASE_URL}/rest/v1/{OT_TABLE}", data=json.dumps(row))
        if ins.status_code < 400:
            try:
                payload = ins.json()
                return (len(payload) if isinstance(payload, list) else 1), "MANUAL_UPSERT_OK"
            except Exception:
                return 1, "MANUAL_UPSERT_OK"
        return 0, f"INSERT_FAIL:{ins.text}"
    return 0, f"HTTP_{r.status_code}:{r.text}"

# utils/overtime_integration.py (append)
import json, requests, os, logging
log = logging.getLogger(__name__)

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_ANON_KEY")

def run_sql_linker(minutes_window: int = 72*60, min_ratio: float = 0.58):
    """Calls the SQL linker function via PostgREST RPC."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_*KEY")

    url = f"{SUPABASE_URL}/rest/v1/rpc/link_ot_to_matches_next_2d"
    payload = {"p_minutes_window": int(minutes_window), "p_min_ratio": float(min_ratio)}
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"RPC failed {r.status_code}: {r.text}")
    data = r.json() or [{}]
    upd = data[0].get("updated_matches_ot", 0)
    lnk = data[0].get("upserted_links", 0)
    log.info("🔁 SQL linker: updated_matches_ot=%s, upserted_links=%s", upd, lnk)
    return upd, lnk


# ── PUBLIC 1: Ingest ALL open Soccer games into matches_ot ───────────────────
def ingest_all_overtime_soccer() -> Tuple[int, int]:
    """
    Fetch all open/ongoing Soccer markets from Overtime,
    group by gameId, bundle odds, and upsert into matches_ot.
    Returns: (games_seen, rows_written)
    """
    markets = _overtime_get_markets()
    if not markets:
        log.info("No open Overtime %s markets.", OVERTIME_SPORT)
        return 0, 0

    # group by gameId
    by_gid: Dict[str, List[dict]] = defaultdict(list)
    for m in markets:
        gid = str(m.get("gameId"))
        if gid:
            by_gid[gid].append(m)

    written = 0
    for gid, group in by_gid.items():
        try:
            base = group[0]
            bundle = _bundle_odds(group)

            row = {
                "game_id": gid,
                "network_id": int(base.get("networkId") or OVERTIME_NETWORK),
                "sport": base.get("sport"),
                "league": base.get("leagueName"),
                "maturity": base.get("maturityDate"),
                "home_team": base.get("homeTeam"),
                "away_team": base.get("awayTeam"),
                "odds": bundle,  # {"winner": {...}, "ou_2_5": {...}, "btts": {...}}
                "bet_url": _bet_url(gid, int(base.get("networkId") or OVERTIME_NETWORK)),
                "bet_url_fallback": "https://www.overtimemarkets.xyz/#/markets",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            w, reason = _sb_upsert_matches_ot(row)
            if w == 0:
                log.warning("Overtime upsert failed for %s: %s", gid, reason)
            else:
                log.info("Overtime upsert %s: %s", gid, reason)
            written += (1 if w > 0 else 0)
        except Exception as e:
            log.exception("Error processing Overtime game %s: %s", gid, e)

    log.info("Overtime ingest complete. games=%s, written=%s", len(by_gid), written)
    return len(by_gid), written

# ── PUBLIC 2: (kept) Upsert a single game by matching a legacy fixture ───────
def upsert_overtime_from_fixture(match_json: dict) -> Tuple[int, str]:
    """
    Given your existing fixture JSON, try to find the Overtime game
    (by team names/time), bundle odds, and upsert one row to matches_ot.
    Returns (written_count, reason). Keeps it simple (no heavy fuzzy here).
    """
    home = (match_json.get("home_team") or {}).get("name") or (match_json.get("home_team") or {}).get("team")
    away = (match_json.get("away_team") or {}).get("name") or (match_json.get("away_team") or {}).get("team")
    kickoff = match_json.get("date")
    if not (home and away and kickoff):
        return 0, "MISSING_FIXTURE_FIELDS"

    markets = _overtime_get_markets()
    # naive normalized compare + ±8h window
    def _norm(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", (s or "").lower())
    def _mins(a: str, b: str) -> float:
        try:
            ta = datetime.fromisoformat(a.replace("Z","+00:00"))
            tb = datetime.fromisoformat(b.replace("Z","+00:00"))
            return abs((ta - tb).total_seconds()) / 60.0
        except Exception:
            return 9e9

    hn, an = _norm(home), _norm(away)
    best = None
    best_minutes = 9e9
    for m in markets:
        mh, ma = _norm(m.get("homeTeam","")), _norm(m.get("awayTeam",""))
        if not mh or not ma: continue
        # accept same-order or swapped
        teams_ok = (hn == mh and an == ma) or (hn == ma and an == mh)
        if not teams_ok: continue
        dmin = _mins(kickoff, m.get("maturityDate",""))
        if dmin <= 480 and dmin < best_minutes:  # ±8h
            best, best_minutes = m, dmin

    if not best:
        return 0, "NO_MATCH_FOUND"

    gid = best.get("gameId")
    network_id = int(best.get("networkId") or OVERTIME_NETWORK)
    bundle = _bundle_odds([mm for mm in markets if str(mm.get("gameId")) == str(gid)])

    row = {
        "game_id": gid,
        "network_id": network_id,
        "sport": best.get("sport"),
        "league": best.get("leagueName"),
        "maturity": best.get("maturityDate"),
        "home_team": best.get("homeTeam"),
        "away_team": best.get("awayTeam"),
        "odds": bundle,
        "bet_url": _bet_url(gid, network_id),
        "bet_url_fallback": "https://www.overtimemarkets.xyz/#/markets",
        "fixture_id": int(match_json.get("fixture_id")) if match_json.get("fixture_id") else None,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    return _sb_upsert_matches_ot(row)
