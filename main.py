# main.py
import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# ---------------------------------
# Logging
# ---------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ---------------------------------
# Config
# ---------------------------------
APP_TZ = os.getenv("APP_TZ", "UTC")          # What "today/yesterday" mean for fetch/settlement
LOOKAHEAD_DAYS = int(os.getenv("LOOKAHEAD_DAYS", "1"))  # today + 1 day ahead (default)
PREFERRED_BOOK = os.getenv("PREF_BOOK", "Bwin")
INVERT_PREDICTIONS = os.getenv("INVERT_PREDICTIONS", "false").lower() in ("1", "true", "yes", "y")
SCORING_DEBUG = os.getenv("SCORING_DEBUG", "false").lower() in ("1", "true", "yes", "y")

# ---------------------------------
# Repo utils
# ---------------------------------
from utils.get_football_data import (
    fetch_fixtures,
    get_head_to_head,
    get_match_odds,
    enrich_fixture,
)
from utils.insert_match import insert_match
from utils.insert_value_predictions import insert_value_predictions
from utils.get_prediction import get_prediction

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


# ---------------------------------
# Time helpers (APP_TZ)
# ---------------------------------
def _now_app() -> datetime:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(APP_TZ))
    except Exception:
        return datetime.utcnow()

def _today_str_app() -> str:
    return _now_app().strftime("%Y-%m-%d")

def _date_str_app_days_ago(days: int) -> str:
    return (_now_app() - timedelta(days=days)).strftime("%Y-%m-%d")

def _date_str_app_days_ahead(days: int) -> str:
    return (_now_app() + timedelta(days=days)).strftime("%Y-%m-%d")


# ---------------------------------
# Odds helper (API-Football only)
# ---------------------------------
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
    odds_flat = get_match_odds(fixture_id, preferred_bookmaker=PREFERRED_BOOK) or {}
    return {"over_2_5": odds_flat.get("over_2_5"), "under_2_5": odds_flat.get("under_2_5")}


# ---------------------------------
# LLM payload
# ---------------------------------
def _build_llm_payload_from_enriched(
    fx_enriched: Dict[str, Any],
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

    # H2H (last 3) — done here to keep enrich light/reusable
    h2h = []
    try:
        hid, aid = home.get("id"), away.get("id")
        if hid and aid:
            h2h = get_head_to_head(hid, aid, limit=3)
    except Exception:
        h2h = []

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
        "odds": {"over_2_5": None, "under_2_5": None, "source": "apifootball"},
        "recent_form": {"home": fx_enriched.get("recent_form_home"),
                        "away": fx_enriched.get("recent_form_away")},
        "season_context": {"home": fx_enriched.get("season_stats_home"),
                           "away": fx_enriched.get("season_stats_away")},
        "injuries": {"home": fx_enriched.get("injuries_home"),
                     "away": fx_enriched.get("injuries_away")},
        "lineups": {"home": fx_enriched.get("lineup_home"),
                    "away": fx_enriched.get("lineup_away")},
    }
    return payload


def _log_scoring_debug(fixture_id: int, pred: Dict[str, Any]) -> None:
    if not SCORING_DEBUG:
        return
    sig = pred.get("signals") or {}
    weights = pred.get("weights") or {}
    priors = pred.get("priors") or {}
    logging.info(
        "SCORING | fid=%s pick=%s conf=%s prob_over=%s odds=%s edge=%s | signals=%s | weights=%s | priors=%s",
        fixture_id,
        pred.get("prediction"),
        pred.get("confidence"),
        pred.get("prob_over"),
        pred.get("odds"),
        pred.get("edge"),
        {k: sig.get(k) for k in ["form_tempo", "form_rates", "season_base", "injuries", "h2h", "weighted_total"]},
        weights,
        priors,
    )


# ---------------------------------
# Inversion (optional)
# ---------------------------------
def _maybe_invert(pred: Dict[str, Any], odds_raw: Dict[str, Any]) -> Dict[str, Any]:
    if not INVERT_PREDICTIONS:
        logging.info("Inversion disabled: posting TRUE model prediction.")
        return pred
    if not _HAS_INVERTER:
        logging.warning("Inversion requested but inverter not available. Posting TRUE prediction.")
        return pred
    try:
        flipped = invert_ou25_prediction(dict(pred), odds_raw=odds_raw, src="apifootball")
        logging.info("Inversion enabled: posting INVERTED prediction.")
        return flipped
    except Exception as e:
        logging.warning(f"Inversion failed ({e}); posting TRUE prediction.")
        return pred


# ---------------------------------
# Core
# ---------------------------------
def _process_fixture(fx_raw: Dict[str, Any]) -> None:
    fixture_id = (fx_raw.get("fixture") or {}).get("id")
    if not fixture_id:
        logging.warning("Skipping fixture without fixture_id.")
        return

    # 1) Enrich with form/xG, injuries, OU/BTTS markets
    fx_enriched = enrich_fixture(fx_raw, preferred_bookmaker=PREFERRED_BOOK)

    # 2) Upsert the match row
    try:
        ok = insert_match(fx_enriched)
        if not ok:
            logging.warning(f"⚠️ insert_match returned False for fixture {fixture_id}")
    except Exception as e:
        logging.error(f"❌ insert_match failed for fixture {fixture_id}: {e}")

    # 3) Build payload + attach OU2.5 odds
    payload = _build_llm_payload_from_enriched(fx_enriched)
    ou = _choose_ou_odds(fx_enriched, fixture_id)
    payload["odds"] = {"over_2_5": ou.get("over_2_5"), "under_2_5": ou.get("under_2_5"), "source": "apifootball"}

    if payload["odds"]["over_2_5"] is None and payload["odds"]["under_2_5"] is None:
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

    if SCORING_DEBUG:
        _log_scoring_debug(fixture_id, pred)

    # 5) Optional inversion
    final_pred = _maybe_invert(pred, payload.get("odds") or {})

    # 6) Store prediction
    try:
        count, msg = insert_value_predictions(final_pred, odds_source="apifootball")
        if count:
            logging.info(
                f"✅ Stored prediction {fixture_id}: "
                f"{final_pred.get('prediction')} @ {final_pred.get('odds')} | conf={final_pred.get('confidence')} | {msg}"
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
    """Minimal stub for CLI runs with explicit IDs."""
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


# ---------------------------------
# Entrypoint
# ---------------------------------
if __name__ == "__main__":
    argv_ids = _parse_fixture_ids_from_argv(sys.argv[1:])

    if argv_ids:
        for fid in argv_ids:
            fx = _fetch_single_fixture_stub(fid)
            _process_fixture(fx)
    else:
        # Process today + LOOKAHEAD_DAYS ahead
        for d in range(0, LOOKAHEAD_DAYS + 1):
            ds = _today_str_app() if d == 0 else _date_str_app_days_ahead(d)
            logging.info(f"▶ Processing fixtures for {ds}")
            _process_fixtures_for_date(ds)

    # Settle only today & yesterday
    if _HAS_SETTLER:
        try:
            today_ds = _today_str_app()
            yday_ds = _date_str_app_days_ago(1)
            settled_today = settle_date(today_ds)
            settled_yday = settle_date(yday_ds)
            logging.info(f"✅ Settled {settled_today} results for {today_ds}")
            logging.info(f"✅ Settled {settled_yday} results for {yday_ds}")
        except Exception as e:
            logging.warning(f"Result settlement failed: {e}")
    else:
        logging.info("Result settlement skipped (module not available).")
