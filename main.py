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
# Repo modules already in your project
# ----------------------------
from utils.get_football_data import fetch_fixtures, get_head_to_head
from utils.cached_football_data import get_match_odds
from utils.insert_match import insert_match
from utils.insert_value_predictions import insert_value_predictions
from utils.get_prediction import get_prediction

# Optional: inverter present, but we’ll guard it behind an env flag
try:
    from utils.value_inversion import invert_ou25_prediction
    _HAS_INVERTER = True
except Exception:
    _HAS_INVERTER = False

# Optional result settlement (will be skipped if the module isn't present)
try:
    from utils.settle_results import settle_date
    _HAS_SETTLER = True
except Exception as _e:
    logging.info(f"Result settlement module not found (utils.settle_results): {_e}")
    _HAS_SETTLER = False


# ----------------------------
# Config
# ----------------------------
ODDS_SOURCE = os.getenv("ODDS_SOURCE", "apifootball")
INVERT_PREDICTIONS = os.getenv("INVERT_PREDICTIONS", "false").lower() in ("1", "true", "yes", "y")


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


def _build_match_payload_from_fixture(
    fx: Dict[str, Any],
    odds_src: str,
    odds: Dict[str, Any]
) -> Dict[str, Any]:
    """Build the payload expected by utils.get_prediction.get_prediction()."""
    fixture = fx.get("fixture", {}) or {}
    league = fx.get("league", {}) or {}
    teams = fx.get("teams", {}) or {}
    venue = fixture.get("venue", {}) or {}

    home = teams.get("home", {}) or {}
    away = teams.get("away", {}) or {}

    payload = {
        "fixture_id": fixture.get("id"),
        "date": fixture.get("date"),
        "league": {
            "id": league.get("id"),
            "name": league.get("name"),
            "country": league.get("country"),
            "season": league.get("season"),
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
        "head_to_head": None,  # filled later
        "odds": {
            "over_2_5": odds.get("over_2_5"),
            "under_2_5": odds.get("under_2_5"),
            "source": odds_src,
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
        logging.warning("Inversion requested but utils.value_inversion is missing. Posting TRUE prediction.")
        return pred
    try:
        flipped = invert_ou25_prediction(dict(pred), odds_raw=odds_raw, src=ODDS_SOURCE)
        logging.info("Inversion enabled: posting INVERTED prediction.")
        return flipped
    except Exception as e:
        logging.warning(f"Inversion failed ({e}); posting TRUE prediction.")
        return pred


def _process_fixture(fx: Dict[str, Any]) -> None:
    """Process a single fixture end-to-end (insert match, predict, optional invert, store)."""
    fixture_id = (fx.get("fixture") or {}).get("id")
    if not fixture_id:
        logging.warning("Skipping fixture without fixture_id.")
        return

    # 1) Upsert the match row (your function accepts API-Football-style fixture dicts)
    try:
        ok = insert_match(fx)
        if not ok:
            logging.warning(f"⚠️ insert_match returned False for fixture {fixture_id}")
    except Exception as e:
        logging.error(f"❌ insert_match failed for fixture {fixture_id}: {e}")

    # 2) Odds (cached wrapper)
    try:
        odds_raw = get_match_odds(fixture_id) or {}
    except Exception as e:
        logging.warning(f"⚠️ get_match_odds failed for fixture {fixture_id}: {e}")
        odds_raw = {}

    # 3) H2H (optional) and payload for predictor
    try:
        teams = fx.get("teams") or {}
        home_id = (teams.get("home") or {}).get("id")
        away_id = (teams.get("away") or {}).get("id")
        h2h = get_head_to_head(home_id, away_id, limit=3) if (home_id and away_id) else []
    except Exception:
        h2h = []

    payload = _build_match_payload_from_fixture(fx, ODDS_SOURCE, odds_raw)
    payload["head_to_head"] = h2h

    # Need an Over 2.5 price for predictor; if missing, skip prediction for this fixture
    if payload.get("odds", {}).get("over_2_5") is None:
        logging.info(f"ℹ️ No Over 2.5 price for fixture {fixture_id}; skipping prediction.")
        return

    # 4) Run your predictor (true model output)
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

    # 5) Optionally invert (now DISABLED by default)
    final_pred = _maybe_invert(pred, odds_raw)

    # 6) Store prediction
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
    If you add a fetch-by-id later, plug it here.
    """
    try:
        odds = get_match_odds(fixture_id) or {}
    except Exception:
        odds = {}
    return {"fixture": {"id": fixture_id}, "teams": {}, "league": {}, "odds": odds}


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
