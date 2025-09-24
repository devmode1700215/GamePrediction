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
# ─────────────────────────────────────────────────────────────────────────────
# LINKER: Overtime games → your fixtures (writes to public.ot_links)
# ─────────────────────────────────────────────────────────────────────────────
import unicodedata
from difflib import SequenceMatcher

# Read tuning from env (already defined earlier or add here)
OVERTIME_MATCH_WINDOW_MIN = int(os.getenv("OVERTIME_MATCH_WINDOW_MIN", "480"))   # ±8h
OVERTIME_MATCH_MIN_RATIO  = float(os.getenv("OVERTIME_MATCH_MIN_RATIO", "0.76"))
OVERTIME_DEBUG_TOPK       = int(os.getenv("OVERTIME_DEBUG_TOPK", "3"))

# Small REST client for links (reuse _sb headers)
def _sb_upsert_link(game_id: str, fixture_id: int, confidence: float, matched_by: str = "auto") -> tuple[int, str]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return 0, "NO_SUPABASE"
    url = f"{SUPABASE_URL}/rest/v1/ot_links?on_conflict=game_id"
    payload = {
        "game_id": game_id,
        "fixture_id": int(fixture_id),
        "confidence": float(confidence),
        "matched_by": matched_by,
    }
    r = _sb.post(url, data=json.dumps(payload))
    if r.status_code < 400:
        return 1, "OK"
    # manual upsert if unique constraint missing (unlikely here, PK on game_id)
    if "no unique or exclusion constraint" in (r.text or "").lower():
        _sb.delete(f"{SUPABASE_URL}/rest/v1/ot_links?game_id=eq.{game_id}",
                   headers={"Prefer": "return=minimal"})
        ins = _sb.post(f"{SUPABASE_URL}/rest/v1/ot_links", data=json.dumps(payload))
        return (1, "MANUAL_UPSERT_OK") if ins.status_code < 400 else (0, f"INSERT_FAIL:{ins.text}")
    return 0, f"HTTP_{r.status_code}:{r.text}"

# Normalization helpers
_STOP = {"fc","cf","sc","ac","afc","cfc","club","ii","b","women","ladies","the"}
_ABBR = {"utd":"united","st":"saint","st.":"saint","intl":"international","int'l":"international","dep":"deportivo","&":"and"}

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def _tokens(name: str) -> list[str]:
    s = _strip_accents((name or "").lower())
    s = re.sub(r"[^\w\s]", " ", s)
    toks = []
    for t in s.split():
        t = _ABBR.get(t, t)
        if t and t not in _STOP:
            toks.append(t)
    return toks

def _norm(name: str) -> str:
    return "".join(_tokens(name))

def _token_set_ratio(a: str, b: str) -> float:
    ta, tb = set(_tokens(a)), set(_tokens(b))
    if not ta or not tb:
        return 0.0
    jacc = len(ta & tb) / len(ta | tb)
    seq  = SequenceMatcher(None, _norm(a), _norm(b)).ratio()
    return 0.6 * jacc + 0.4 * seq

def _minutes_diff(a_iso: str, b_iso: str) -> float | None:
    try:
        ta = datetime.fromisoformat(a_iso.replace("Z","+00:00"))
        tb = datetime.fromisoformat(b_iso.replace("Z","+00:00"))
        return abs((ta - tb).total_seconds()) / 60.0
    except Exception:
        return None

def _best_fixture_match(ot_game: dict, fixtures: list[dict]) -> tuple[dict | None, float, float | None, list]:
    """Return (best_fixture, score, minutes_diff, debug_candidates[])"""
    mh, ma = ot_game.get("homeTeam",""), ot_game.get("awayTeam","")
    mdt    = ot_game.get("maturityDate","")
    scored = []
    for f in fixtures:
        fx  = f.get("fixture", {})
        tms = f.get("teams", {})
        h, a = (tms.get("home") or {}).get("name",""), (tms.get("away") or {}).get("name","")
        if not h or not a or not fx.get("date"):
            continue
        r1 = _token_set_ratio(mh, h) * _token_set_ratio(ma, a)
        r2 = _token_set_ratio(mh, a) * _token_set_ratio(ma, h)  # swapped
        name_score = max(r1, r2)  # 0..1

        mdiff = _minutes_diff(mdt, fx["date"])
        time_score = 0.0
        if mdiff is not None:
            time_score = max(0.0, 1.0 - (mdiff / max(1.0, OVERTIME_MATCH_WINDOW_MIN)))

        score = 0.75*name_score + 0.25*time_score
        scored.append((score, f, name_score, mdiff))

    if not scored:
        return None, 0.0, None, []

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_fx, best_name, best_mins = scored[0]
    # shortlist for logs
    topk = [{
        "score": round(s,3),
        "fixture_id": (fx.get("fixture") or {}).get("id"),
        "name_score": round(ns,3),
        "minutes_diff": md,
        "home": (fx.get("teams") or {}).get("home",{}).get("name"),
        "away": (fx.get("teams") or {}).get("away",{}).get("name"),
        "kickoff": (fx.get("fixture") or {}).get("date")
    } for s, fx, ns, md in scored[:OVERTIME_DEBUG_TOPK]]
    return best_fx, best_score, best_mins, topk

def link_overtime_to_fixtures(window_days: int = 2) -> tuple[int, int, int]:
    """
    1) Fetch all open Overtime soccer games (already in memory via _overtime_get_markets()).
    2) Fetch your fixtures for today..today+window_days.
    3) Fuzzy+time match → upsert ot_links (game_id ↔ fixture_id).
    Returns: (games_seen, linked_count, skipped_count)
    """
    # 1) Overtime games
    markets = _overtime_get_markets()
    by_gid: dict[str, dict] = {}
    for m in markets:
        gid = str(m.get("gameId"))
        if gid and gid not in by_gid:
            by_gid[gid] = m  # one representative per game

    # 2) Your fixtures (next N days)
    from utils.get_football_data import fetch_fixtures  # local import to avoid cycles
    now = datetime.now(timezone.utc)
    days = [(now + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(window_days+1)]
    fixtures: list[dict] = []
    for d in days:
        try:
            arr = fetch_fixtures(d) or []
            fixtures.extend(arr)
        except Exception as e:
            log.warning("fetch_fixtures(%s) failed: %s", d, e)

    linked = 0
    skipped = 0
    for gid, game in by_gid.items():
        best_fx, score, mdiff, topk = _best_fixture_match(game, fixtures)
        if OVERTIME_DEBUG_TOPK and topk:
            log.info("OT match candidates for %s: %s", gid, json.dumps(topk, ensure_ascii=False))

        # accept if score & time within window
        minutes_ok = (mdiff is not None and mdiff <= OVERTIME_MATCH_WINDOW_MIN)
        if score >= OVERTIME_MATCH_MIN_RATIO and minutes_ok and best_fx:
            fixture_id = (best_fx.get("fixture") or {}).get("id")
            if fixture_id:
                w, reason = _sb_upsert_link(gid, int(fixture_id), float(score), matched_by="auto")
                if w > 0:
                    linked += 1
                else:
                    log.warning("Link upsert failed for gid=%s → fixture=%s: %s", gid, fixture_id, reason)
            else:
                skipped += 1
        else:
            skipped += 1

    log.info("Linking summary: games=%s, linked=%s, skipped=%s", len(by_gid), linked, skipped)
    return len(by_gid), linked, skipped

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
