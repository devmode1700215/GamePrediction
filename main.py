# main.py
import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ----------------------------
# Config
# ----------------------------
ODDS_SOURCE = os.getenv("ODDS_SOURCE", "apifootball")
PREFERRED_BOOK = os.getenv("PREF_BOOK", "Bwin")
INVERT_PREDICTIONS = os.getenv("INVERT_PREDICTIONS", "false").lower() in ("1", "true", "yes", "y")

# ----------------------------
# Repo utils
# ----------------------------
from utils.get_football_data import (
    fetch_fixtures,
    get_head_to_head,
    get_match_odds,
    enrich_fixture,
)
from utils.insert_match import insert_match
from utils.insert_value_predictions import insert_value_predictions
from utils.get_prediction import get_prediction

# Optional inverter; guarded by env flag
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


# ----------------------------
# Helpers
# ----------------------------
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


def _choose_ou_odds(fx: Dict[str, Any], fixture_id: int) -> Dict[str, Optional[float]]:
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


def _build_llm_payload_from_enriched(
    fx_enriched: Dict[str, Any],
    odds_src: str,
) -> Dict[str, Any]:
    """
    Build the INPUT payload for get_prediction(...) with richer context.
    """
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

    # Odds
    ou = _choose_ou_odds(fx_enriched, fixture_id)

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
        "home_team": {
            "id": home.get("id"),
            "name": home.get("name"),
        },
        "away_team": {
            "id": away.get("id"),
            "name": away.get("name"),
        },
        "head_to_head": h2h,
        "odds": {
            "over_2_5": ou.get("over_2_5"),
            "under_2_5": ou.get("under_2_5"),
            "source": odds_src,
        },
        # New richer blocks for the LLM to use:
        "recent_form": {
            "home": fx_enriched.get("recent_form_home"),
            "away": fx_enriched.get("recent_form_away"),
        },
        "season_context": {
            "home": fx_enriched.get("season_stats_home"),
            "away": fx_enriched.get("season_stats_away"),
        },
        "injuries": {
            "home": fx_enriched.get("injuries_home"),
            "away": fx_enriched.get("injuries_away"),
        },
        "lineups": {
            "home": fx_enriched.get("lineup_home"),
            "away": fx_enriched.get("lineup_away"),
        },
    }
    return payload


def _maybe_invert(pred: Dict[str, Any], odds_raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Only invert if INVERT_PREDICTIONS=true and inverter is available.
    Otherwise return the original (TRUE) prediction unchanged.
    """
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


# ----------------------------
# Core processing
# ----------------------------
def _process_fixture(fx_raw: Dict[str, Any]) -> None:
    """
    Enrich -> insert match -> predict -> (optionally invert) -> store prediction.
    """
    fixture_id = (fx_raw.get("fixture") or {}).get("id")
    if not fixture_id:
        logging.warning("Skipping fixture without fixture_id.")
        return

    # 1) Enrich the fixture with last-5 form, season context, injuries, lineups, and OU/BTTS markets
    fx_enriched = enrich_fixture(fx_raw, preferred_bookmaker=PREFERRED_BOOK)

    # 2) Upsert the match row (persists enriched JSONB blocks into matches table)
    try:
        ok = insert_match(fx_enriched)
        if not ok:
            logging.warning(f"⚠️ insert_match returned False for fixture {fixture_id}")
    except Exception as e:
        logging.error(f"❌ insert_match failed for fixture {fixture_id}: {e}")

    # 3) Build LLM payload with richer context and odds
    payload = _build_llm_payload_from_enriched(fx_enriched, ODDS_SOURCE)

    # Require Over 2.5 price to proceed (model relies on market prior)
    if payload.get("odds", {}).get("over_2_5") is None:
        logging.info(f"ℹ️ No Over 2.5 price for fixture {fixture_id}; skipping prediction.")
        return

    # 4) True model prediction
    try:
        pred = get_prediction(payload) or {}
    except Exception as e:
        logging.error(f"❌ get_prediction() failed for fixture {fixture_id}: {e}")
        return

    if not pred:
        logging.info(f"ℹ️ Predictor returned empty for fixture {fixture_id}; skipping.")
        return

    # Ensure minimal fields exist
    pred = dict(pred)
    pred.setdefault("fixture_id", fixture_id)
    pred.setdefault("market", "over_2_5")

    # 5) Optional inversion (disabled by default)
    final_pred = _maybe_invert(pred, payload.get("odds") or {})

    # 6) Store prediction in value_predictions
    try:
        count, msg = insert_value_predictions(final_pred, odds_source=ODDS_SOURCE)
        if count:
            logging.info(
                f"✅ Stored prediction {fixture_id}: "
                f"{final_pred.get('prediction')} @ {final_pred.get('odds')} | {msg}"
            )
        else:
            logging.info(f"⛔ Skipped {fixture_id}: {msg}")
    except Exception as e:
        logging.error(f"❌ insert_value_predictions failed for fixture {fixture_id}: {e}")


def _process_fixtures_for_date(date_str: str) -> None:
    """Fetch all fixtures for date_str and process them."""
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
    Minimal fixture stub so insert_match() doesn't crash when running by fixture id.
    For CLI runs against explicit IDs. Odds added via get_match_odds.
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


# ----------------------------
# Entrypoint
# ----------------------------
if __name__ == "__main__":
    argv_ids = _parse_fixture_ids_from_argv(sys.argv[1:])

    if argv_ids:
        # Process explicit fixture IDs
        for fid in argv_ids:
            fx = _fetch_single_fixture_stub(fid)
            _process_fixture(fx)
    else:
        # Default: process today's fixtures (Europe/Brussels)
        today_str = _today_str_brussels()
        _process_fixtures_for_date(today_str)

    # --- Result settlement (today & yesterday, Europe/Brussels) ---
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
