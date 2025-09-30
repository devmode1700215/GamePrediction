# main.py
import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# --------------------------------------------------------------------
# Config
# --------------------------------------------------------------------
ODDS_SOURCE = os.getenv("ODDS_SOURCE", "apifootball")
PREFERRED_BOOK = os.getenv("PREF_BOOK", "Bwin")
INVERT_PREDICTIONS = os.getenv("INVERT_PREDICTIONS", "false").lower() in ("1", "true", "yes", "y")
SCORING_DEBUG = os.getenv("SCORING_DEBUG", "true").lower() in ("1", "true", "yes", "y")
LOOKAHEAD_DAYS = int(os.getenv("LOOKAHEAD_DAYS", "2"))  # today + N days ahead

# --------------------------------------------------------------------
# Repo utils
# --------------------------------------------------------------------
from utils.get_football_data import (
    fetch_fixtures,
    get_head_to_head,
    get_match_odds,
    enrich_fixture,
)
from utils.insert_match import insert_match
from utils.insert_value_predictions import insert_value_predictions
from utils.get_prediction import get_prediction
from utils.safe_get import safe_get  # used for Overtime queries

# Optional inverter
try:
    from utils.value_inversion import invert_ou25_prediction
    _HAS_INVERTER = True
except Exception:
    _HAS_INVERTER = False

# Optional result settlement
try:
    from utils.settle_results import settle_date
    _HAS_SETTLER = True
except Exception as _e:
    logging.info(f"Result settlement module not found (utils.settle_results): {_e}")
    _HAS_SETTLER = False


# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def _today_str_brussels() -> str:
    try:
        from zoneinfo import ZoneInfo
        now_bru = datetime.now(ZoneInfo("Europe/Brussels"))
    except Exception:
        now_bru = datetime.now()
    return now_bru.strftime("%Y-%m-%d")


def _date_str_brussels_days_ago(days: int) -> str:
    try:
        from zoneinfo import ZoneInfo
        base = datetime.now(ZoneInfo("Europe/Brussels"))
    except Exception:
        base = datetime.now()
    target = base - timedelta(days=days)
    return target.strftime("%Y-%m-%d")


def _date_str_brussels_days_ahead(days: int) -> str:
    try:
        from zoneinfo import ZoneInfo
        base = datetime.now(ZoneInfo("Europe/Brussels"))
    except Exception:
        base = datetime.now()
    target = base + timedelta(days=days)
    return target.strftime("%Y-%m-%d")


def _clip(x: Optional[float], lo: float, hi: float) -> Optional[float]:
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return None


# ----------------------------
# Odds helpers
# ----------------------------
def _choose_ou_odds_from_enriched_or_api(fx: Dict[str, Any], fixture_id: int) -> Dict[str, Optional[float]]:
    """
    Return {'over_2_5': float|None, 'under_2_5': float|None} using:
      1) enriched 'ou25_market' if present
      2) fallback to get_match_odds(...)
    """
    ou = (fx.get("ou25_market") or {}) if isinstance(fx, dict) else {}
    over_ = ou.get("over")
    under_ = ou.get("under")
    if over_ is not None or under_ is not None:
        return {"over_2_5": over_, "under_2_5": under_}
    odds_flat = get_match_odds(fixture_id) or {}
    return {"over_2_5": odds_flat.get("over_2_5"), "under_2_5": odds_flat.get("under_2_5")}

def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _slugify(s: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in (s or "").strip()).strip("-")

def _fetch_overtime_candidates(date_iso: str, home_name: str, away_name: str) -> List[Dict[str, Any]]:
    """
    Minimal Overtime search by date + names (no mapping yet).
    Expected API (adjust as needed):
      GET {OVERTIME_BASE_URL}/odds/search?date=YYYY-MM-DD&home=..&away=..&market=ou25
    Returns a list of candidate matches; we'll do a naive name check.
    """
    base = os.getenv("OVERTIME_BASE_URL")
    key  = os.getenv("OVERTIME_API_KEY")
    if not base or not key:
        return []
    url = f"{base.rstrip('/')}/odds/search"
    headers = {"Authorization": f"Bearer {key}"}
    params  = {"date": date_iso, "home": home_name, "away": away_name, "market": "ou25"}
    resp = safe_get(url, headers=headers, params=params)
    if resp is None:
        logging.info("[overtime] no response for %s %s vs %s", date_iso, home_name, away_name)
        return []
    try:
        data = resp.json() or {}
        items = data.get("results") or data.get("matches") or data.get("data") or []
        if not isinstance(items, list):
            logging.info("[overtime] unexpected payload shape for %s", url)
            return []
        logging.info("[overtime] %d candidates for %s %s vs %s", len(items), date_iso, home_name, away_name)
        return items
    except Exception as e:
        logging.warning(f"[overtime] parse error: {e}")
        return []

def _extract_ou25_from_overtime_item(item: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """
    Try multiple shapes to extract OU2.5 from a candidate item.
    Accepted shapes:
      item["markets"]["over_2_5"], ["markets"]["under_2_5"]
      item["ou25"]["over"], item["ou25"]["under"]
      item["over_2_5"], item["under_2_5"]
    """
    markets = item.get("markets") or {}
    if isinstance(markets, dict):
        over_ = _to_float(markets.get("over_2_5"))
        under_ = _to_float(markets.get("under_2_5"))
        if over_ is not None or under_ is not None:
            return {"over_2_5": over_, "under_2_5": under_}

    ou25 = item.get("ou25") or {}
    if isinstance(ou25, dict):
        over_ = _to_float(ou25.get("over"))
        under_ = _to_float(ou25.get("under"))
        if over_ is not None or under_ is not None:
            return {"over_2_5": over_, "under_2_5": under_}

    over_ = _to_float(item.get("over_2_5"))
    under_ = _to_float(item.get("under_2_5"))
    return {"over_2_5": over_, "under_2_5": under_}

def _get_best_ou25_odds_overtime_first(fx_enriched: Dict[str, Any], preferred_bookmaker: str = "Bwin") -> Dict[str, Any]:
    """
    STEP 1: Try Overtime by date+names (naive matching).
    If nothing found/parsed, fall back to API-Football.
    """
    fixture = fx_enriched.get("fixture", {}) or {}
    teams = fx_enriched.get("teams", {}) or {}
    home = teams.get("home", {}) or {}
    away = teams.get("away", {}) or {}

    date_iso = (fixture.get("date") or "")[:10]
    af_home = home.get("name") or ""
    af_away = away.get("name") or ""
    s_home, s_away = _slugify(af_home), _slugify(af_away)

    # 1) Overtime: simple search
    candidates = _fetch_overtime_candidates(date_iso, af_home, af_away)
    chosen = None
    for it in candidates:
        # naive name check (we'll add proper mapping later)
        ot_home = _slugify(it.get("home") or it.get("home_name") or "")
        ot_away = _slugify(it.get("away") or it.get("away_name") or "")
        if not ot_home or not ot_away:
            continue
        # accept if both names roughly match (contains or equal)
        if (s_home in ot_home or ot_home in s_home) and (s_away in ot_away or ot_away in s_away):
            chosen = it
            break

    if chosen:
        ou = _extract_ou25_from_overtime_item(chosen)
        if ou.get("over_2_5") is not None or ou.get("under_2_5") is not None:
            return {"over_2_5": ou.get("over_2_5"), "under_2_5": ou.get("under_2_5"),
                    "source": "overtime", "is_overtime_odds": True}

    # 2) Fallback: enriched/API-Football odds
    fid = fixture.get("id")
    flat = _choose_ou_odds_from_enriched_or_api(fx_enriched, fid)
    return {"over_2_5": flat.get("over_2_5"), "under_2_5": flat.get("under_2_5"),
            "source": "apifootball", "is_overtime_odds": False}


def _build_llm_payload_from_enriched(
    fx_enriched: Dict[str, Any],
    odds_src: str,
) -> Dict[str, Any]:
    fixture = fx_enriched.get("fixture", {}) or {}
    league = fx_enriched.get("league", {}) or {}
    teams = fx_enriched.get("teams", {}) or {}
    venue = fixture.get("venue", {}) or {}

    home = teams.get("home", {}) or {}
    away = teams.get("away", {}) or {}

    fixture_id = fixture.get("id")
    league_id = fx_enriched.get("league_id") or league.get("id")
    season = fx_enriched.get("season") or league.get("season")

    # H2H (last 3)
    h2h = []
    try:
        hid, aid = home.get("id"), away.get("id")
        if hid and aid:
            h2h = get_head_to_head(hid, aid, limit=3)
    except Exception:
        h2h = []

    # NOTE: odds overridden later with chosen source
    payload = {
        "fixture_id": fixture_id,
        "date": fixture.get("date"),
        "league": {
            "id": league_id,
            "name": league.get("name"),
            "country": league.get("country"),
            "season": season,
        },
        "venue": {
            "id": venue.get("id"),
            "name": venue.get("name"),
            "city": venue.get("city"),
        },
        "home_team": {"id": home.get("id"), "name": home.get("name")},
        "away_team": {"id": away.get("id"), "name": away.get("name")},
        "head_to_head": h2h,
        "odds": {"over_2_5": None, "under_2_5": None, "source": odds_src},
        "recent_form": {"home": fx_enriched.get("recent_form_home"), "away": fx_enriched.get("recent_form_away")},
        "season_context": {"home": fx_enriched.get("season_stats_home"), "away": fx_enriched.get("season_stats_away")},
        "injuries": {"home": fx_enriched.get("injuries_home"), "away": fx_enriched.get("injuries_away")},
        "lineups": {"home": fx_enriched.get("lineup_home"), "away": fx_enriched.get("lineup_away")},
    }
    return payload


def _maybe_invert(pred: Dict[str, Any], odds_raw: Dict[str, Any]) -> Dict[str, Any]:
    if not INVERT_PREDICTIONS:
        logging.info("Inversion disabled: posting TRUE model prediction.")
        return pred
    if not _HAS_INVERTER:
        logging.warning("Inversion requested but inverter not available. Posting TRUE prediction.")
        return pred
    try:
        flipped = invert_ou25_prediction(dict(pred), odds_raw=odds_raw, src=ODDS_SOURCE)
        logging.info("Inversion enabled: posting INVERTED prediction.")
        return flipped
    except Exception as e:
        logging.warning(f"Inversion failed ({e}); posting TRUE prediction.")
        return pred


def _normalize_prediction(pred: Dict[str, Any], payload_odds: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(pred)
    pick = out.get("prediction")
    if pick not in ("Over", "Under"):
        pick = "Over"
        out["prediction"] = pick

    p_over = out.get("prob_over")
    try:
        p_over = float(p_over) if p_over is not None else None
    except Exception:
        p_over = None
    p_over = _clip(p_over, 0.01, 0.99) if p_over is not None else None
    out["prob_over"] = p_over

    conf = out.get("confidence")
    if conf is None and p_over is not None:
        conf = p_over if pick == "Over" else (1.0 - p_over)
    try:
        conf = float(conf) if conf is not None else None
    except Exception:
        conf = None
    conf = _clip(conf, 0.05, 0.95) if conf is not None else 0.50
    out["confidence"] = conf

    chosen_odds = out.get("odds")
    try:
        chosen_odds = float(chosen_odds) if chosen_odds is not None else None
    except Exception:
        chosen_odds = None
    if chosen_odds is None:
        chosen_odds = payload_odds.get("over_2_5") if pick == "Over" else payload_odds.get("under_2_5")
        try:
            chosen_odds = float(chosen_odds) if chosen_odds is not None else None
        except Exception:
            chosen_odds = None
    out["odds"] = chosen_odds

    return out


def _log_scoring_debug(fixture_id: int, pred: Dict[str, Any]) -> None:
    if not SCORING_DEBUG:
        return
    sig = pred.get("signals") or {}
    weights = pred.get("weights") or {}
    priors = pred.get("priors") or {}
    logging.info(
        "SCORING | fid=%s pick=%s conf=%.3f prob_over=%s odds=%s edge=%s | signals=%s | weights=%s | priors=%s",
        fixture_id,
        pred.get("prediction"),
        float(pred.get("confidence") or 0),
        pred.get("prob_over"),
        pred.get("odds"),
        pred.get("edge"),
        {k: sig.get(k) for k in ["form_tempo", "form_rates", "season_base", "injuries", "h2h", "weighted_total"]},
        weights,
        priors,
    )


# --------------------------------------------------------------------
# Core
# --------------------------------------------------------------------
def _process_fixture(fx_raw: Dict[str, Any]) -> None:
    fixture_id = (fx_raw.get("fixture") or {}).get("id")
    if not fixture_id:
        logging.warning("Skipping fixture without fixture_id.")
        return

    # 1) Enrich the fixture (form, season, injuries, lineups, OU/BTTS markets)
    fx_enriched = enrich_fixture(fx_raw, preferred_bookmaker=PREFERRED_BOOK)

    # 2) Upsert the match row
    try:
        ok = insert_match(fx_enriched)
        if not ok:
            logging.warning(f"⚠️ insert_match returned False for fixture {fixture_id}")
    except Exception as e:
        logging.error(f"❌ insert_match failed for fixture {fixture_id}: {e}")

    # 3) Choose odds: Overtime first (by date+names), else API-Football
    odds_block = _get_best_ou25_odds_overtime_first(fx_enriched, preferred_bookmaker=PREFERRED_BOOK)

    # Build payload with chosen odds
    payload = _build_llm_payload_from_enriched(fx_enriched, odds_block.get("source") or ODDS_SOURCE)
    payload["odds"] = {
        "over_2_5": odds_block.get("over_2_5"),
        "under_2_5": odds_block.get("under_2_5"),
        "source": odds_block.get("source") or ODDS_SOURCE,
    }

    # Proceed unless BOTH sides are missing
    ou = payload.get("odds") or {}
    if ou.get("over_2_5") is None and ou.get("under_2_5") is None:
        logging.info(f"ℹ️ No OU2.5 prices for fixture {fixture_id}; skipping prediction.")
        return

    # 4) Predict
    try:
        pred = get_prediction(payload) or {}
    except Exception as e:
        logging.error(f"❌ get_prediction() failed for fixture {fixture_id}: {e}")
        return

    if not pred:
        logging.info(f"ℹ️ Predictor returned empty for fixture {fixture_id}; skipping.")
        return

    pred = dict(pred)
    pred.setdefault("fixture_id", fixture_id)
    pred.setdefault("market", "over_2_5")
    pred["is_overtime_odds"] = bool(odds_block.get("is_overtime_odds"))

    # 5) Normalize + debug
    pred = _normalize_prediction(pred, payload.get("odds") or {})
    _log_scoring_debug(fixture_id, pred)

    # 6) Optional inversion (disabled by default)
    final_pred = _maybe_invert(pred, payload.get("odds") or {})

    # 7) Store prediction
    try:
        count, msg = insert_value_predictions(final_pred, odds_source=(odds_block.get("source") or ODDS_SOURCE))
        if count:
            logging.info(
                f"✅ Stored prediction {fixture_id}: "
                f"{final_pred.get('prediction')} @ {final_pred.get('odds')} | conf={final_pred.get('confidence')} "
                f"| src={odds_block.get('source')} | {msg}"
            )
        else:
            logging.info(f"⛔ Skipped {fixture_id}: {msg}")
    except Exception as e:
        logging.error(f"❌ insert_value_predictions failed for fixture {fixture_id}: {e}")


def _process_fixtures_for_date(date_str: str) -> None:
    try:
        fixtures: List[Dict[str, Any]] = fetch_fixtures(date_str) or []
    except Exception as e:
        logging.error(f"❌ fetch_fixtures failed for {date_str}: {e}")
        return

    if not fixtures:
        logging.info(f"ℹ️ No fixtures for {date_str}.")
        return

    for fx in fixtures:
        try:
            _process_fixture(fx)
        except Exception as e:
            fid = (fx.get("fixture") or {}).get("id")
            logging.error(f"❌ Unhandled error on fixture {fid}: {e}")


def _parse_fixture_ids_from_argv(argv: List[str]) -> List[int]:
    ids: List[int] = []
    for x in argv:
        try:
            ids.append(int(x))
        except Exception:
            logging.warning(f"Skipping non-integer fixture id arg: {x}")
    return ids


def _fetch_single_fixture_stub(fixture_id: int) -> Dict[str, Any]:
    """
    Minimal fixture stub for CLI runs with explicit IDs.
    """
    try:
        odds = get_match_odds(fixture_id) or {}
    except Exception:
        odds = {}
    return {
        "fixture": {"id": fixture_id},
        "teams": {},
        "league": {},
        "odds": odds,
    }


# --------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------
if __name__ == "__main__":
    argv_ids = _parse_fixture_ids_from_argv(sys.argv[1:])

    if argv_ids:
        for fid in argv_ids:
            fx = _fetch_single_fixture_stub(fid)
            _process_fixture(fx)
    else:
        # Process today + N days ahead (default 2)
        for d in range(0, LOOKAHEAD_DAYS + 1):
            ds = _today_str_brussels() if d == 0 else _date_str_brussels_days_ahead(d)
            logging.info(f"▶ Processing fixtures for {ds}")
            _process_fixtures_for_date(ds)

    # Settle today & yesterday only (never settle future dates)
    if _HAS_SETTLER:
        try:
            today_ds = _today_str_brussels()
            yday_ds = _date_str_brussels_days_ago(1)
            settled_today = settle_date(today_ds)
            settled_yday = settle_date(yday_ds)
            logging.info(f"✅ Settled {settled_today} results for {today_ds}")
            logging.info(f"✅ Settled {settled_yday} results for {yday_ds}")
        except Exception as e:
            logging.warning(f"Result settlement failed: {e}")
    else:
        logging.info("Result settlement skipped (module not available).")
