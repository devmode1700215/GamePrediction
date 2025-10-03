# main.py
import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
APP_TZ = os.getenv("APP_TZ", "UTC")              # e.g. "America/Los_Angeles"
LOOKAHEAD_DAYS = int(os.getenv("LOOKAHEAD_DAYS", "1"))  # process today + N days ahead

ODDS_SOURCE = os.getenv("ODDS_SOURCE", "apifootball")
PREFERRED_BOOK = os.getenv("PREF_BOOK", "Bwin")
INVERT_PREDICTIONS = os.getenv("INVERT_PREDICTIONS", "false").lower() in ("1", "true", "yes", "y")

# -----------------------------------------------------------------------------
# Repo utils
# -----------------------------------------------------------------------------
from utils.get_football_data import (
    fetch_fixtures,
    get_head_to_head,
    get_match_odds,
)

# enrich_fixture is optional across branches — make it safe
try:
    from utils.get_football_data import enrich_fixture as _enrich_fixture
    def enrich_fixture_safe(fx_raw: Dict[str, Any], preferred_bookmaker: str = PREFERRED_BOOK) -> Dict[str, Any]:
        try:
            return _enrich_fixture(fx_raw, preferred_bookmaker=preferred_bookmaker) or fx_raw
        except Exception as e:
            fid = (fx_raw.get("fixture") or {}).get("id")
            logging.warning(f"enrich_fixture failed for fixture {fid}: {e}")
            return fx_raw
    _HAS_ENRICH = True
except Exception:
    logging.info("utils.get_football_data.enrich_fixture not found; proceeding without enrichment.")
    def enrich_fixture_safe(fx_raw: Dict[str, Any], preferred_bookmaker: str = PREFERRED_BOOK) -> Dict[str, Any]:
        # Minimal passthrough; attach OU2.5 odds so pipeline still runs
        fid = (fx_raw.get("fixture") or {}).get("id")
        try:
            odds = get_match_odds(int(fid)) if fid else {}
        except Exception:
            odds = {}
        out = dict(fx_raw)
        out["odds"] = odds
        return out
    _HAS_ENRICH = False

from utils.insert_match import insert_match
from utils.insert_value_predictions import insert_value_predictions
from utils.get_prediction import get_prediction  # <- your weighted engine lives here

# Supabase client (for "exists?" checks)
try:
    from utils.supabaseClient import supabase
    _HAS_SUPABASE = True
except Exception as e:
    logging.warning(f"Could not import utils.supabaseClient: {e}")
    supabase = None
    _HAS_SUPABASE = False

# Optional inverter
try:
    from utils.value_inversion import invert_ou25_prediction
    _HAS_INVERTER = True
except Exception:
    _HAS_INVERTER = False

# Result settlement
try:
    from utils.settle_results import settle_date
    _HAS_SETTLER = True
except Exception as _e:
    logging.info(f"Result settlement module not found (utils.settle_results): {_e}")
    _HAS_SETTLER = False


# -----------------------------------------------------------------------------
# Time helpers
# -----------------------------------------------------------------------------
def _now_app():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(APP_TZ))
    except Exception:
        return datetime.utcnow()

def _date_str_app_days_ago(days: int) -> str:
    return (_now_app() - timedelta(days=days)).strftime("%Y-%m-%d")

def _date_str_app_days_ahead(days: int) -> str:
    return (_now_app() + timedelta(days=days)).strftime("%Y-%m-%d")

def _today_str_app() -> str:
    return _now_app().strftime("%Y-%m-%d")


# -----------------------------------------------------------------------------
# Odds selector for OU2.5 (prefer enriched market if present)
# -----------------------------------------------------------------------------
def _choose_ou_odds(fx: Dict[str, Any], fixture_id: Optional[int]) -> Dict[str, Optional[float]]:
    ou = {}
    try:
        ou = fx.get("ou25_market") or {}
        if not isinstance(ou, dict):
            ou = {}
    except Exception:
        ou = {}

    over_ = ou.get("over")
    under_ = ou.get("under")
    if over_ is not None or under_ is not None:
        return {"over_2_5": over_, "under_2_5": under_}

    try:
        odds_flat = get_match_odds(int(fixture_id)) if fixture_id else {}
    except Exception:
        odds_flat = {}

    return {"over_2_5": odds_flat.get("over_2_5"), "under_2_5": odds_flat.get("under_2_5")}


# -----------------------------------------------------------------------------
# Pre-checks to reduce duplicate work
# -----------------------------------------------------------------------------
def _prediction_exists(fid: int, market: str = "over_2_5") -> bool:
    """
    Returns True if a value_predictions row exists for (fixture_id, market).
    Keeps API/compute low by skipping already-processed fixtures.
    """
    if not _HAS_SUPABASE:
        return False
    try:
        res = (
            supabase.table("value_predictions")
            .select("id")
            .eq("fixture_id", fid)
            .eq("market", market)
            .limit(1)
            .execute()
        )
        rows = getattr(res, "data", None) or []
        return len(rows) > 0
    except Exception as e:
        logging.info(f"exists-check failed for {fid}: {e}")
        return False


# -----------------------------------------------------------------------------
# Build prediction payload (matches your predictor)
# -----------------------------------------------------------------------------
def _build_payload_from_enriched(
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


# -----------------------------------------------------------------------------
# Core processing
# -----------------------------------------------------------------------------
def _process_fixture(fx_raw: Dict[str, Any]) -> None:
    fixture_id = (fx_raw.get("fixture") or {}).get("id")
    if not fixture_id:
        logging.warning("Skipping fixture without fixture_id.")
        return

    # Skip if we already have a prediction for this fixture/market
    if _prediction_exists(fixture_id, "over_2_5"):
        logging.info(f"⏭  Skip fixture {fixture_id}: value_predictions already has OU2.5")
        return

    # 1) Enrich (if available)
    fx_enriched = enrich_fixture_safe(fx_raw, preferred_bookmaker=PREFERRED_BOOK)

    # 2) Upsert the match row (persists enriched JSONB blocks into matches table)
    try:
        ok = insert_match(fx_enriched)
        if not ok:
            logging.warning(f"⚠️ insert_match returned False for fixture {fixture_id}")
    except Exception as e:
        logging.error(f"❌ insert_match failed for fixture {fixture_id}: {e}")

    # 3) Build payload with richer context and odds
    payload = _build_payload_from_enriched(fx_enriched, ODDS_SOURCE)

    # Require at least one OU2.5 price to proceed
    if payload.get("odds", {}).get("over_2_5") is None and payload.get("odds", {}).get("under_2_5") is None:
        logging.info(f"ℹ️ No OU2.5 prices for fixture {fixture_id}; skipping prediction.")
        return

    # 4) Model prediction
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

    # 5) Optional inversion (disabled by default)
    final_pred = _maybe_invert(pred, payload.get("odds") or {})

    # 6) Store prediction in value_predictions
    try:
        count, msg = insert_value_predictions(final_pred, odds_source=ODDS_SOURCE)
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
    logging.info(f"▶ Processing fixtures for {date_str}")
    try:
        fixtures: List[Dict[str, Any]] = fetch_fixtures(date_str) or []
        logging.info(f"📅 {date_str}: fetched {len(fixtures)} fixtures")
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


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    argv_ids = _parse_fixture_ids_from_argv(sys.argv[1:])

    if argv_ids:
        for fid in argv_ids:
            fx = _fetch_single_fixture_stub(fid)
            _process_fixture(fx)
    else:
        # Process TODAY + LOOKAHEAD_DAYS ahead (in APP_TZ)
        run_days = [0] + list(range(1, max(0, LOOKAHEAD_DAYS) + 1))
        for d in run_days:
            ds = _date_str_app_days_ahead(d)
            _process_fixtures_for_date(ds)

    # --- Result settlement (today, yesterday, day-2) ---
    if _HAS_SETTLER:
        try:
            d0 = _today_str_app()
            d1 = _date_str_app_days_ago(1)
            d2 = _date_str_app_days_ago(2)
            n0 = settle_date(d0)
            n1 = settle_date(d1)
            n2 = settle_date(d2)
            logging.info(f"✅ Settled {n0} results for {d0}")
            logging.info(f"✅ Settled {n1} results for {d1}")
            logging.info(f"✅ Settled {n2} results for {d2}")
        except Exception as e:
            logging.warning(f"Result settlement failed: {e}")
    else:
        logging.info("Result settlement skipped (module not available).")
