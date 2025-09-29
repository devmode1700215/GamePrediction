# main.py
import os
import sys
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ----------------------------
# Repo modules (present in your codebase)
# ----------------------------
from utils.get_football_data import fetch_fixtures, get_head_to_head
from utils.cached_football_data import get_match_odds
from utils.insert_match import insert_match
from utils.insert_value_predictions import insert_value_predictions
from utils.get_prediction import get_prediction
from utils.value_inversion import invert_ou25_prediction


# ----------------------------
# Helpers
# ----------------------------
def _today_str_europe_brussels() -> str:
    """Return today's date string (YYYY-MM-DD) for Europe/Brussels."""
    # Render dynos are UTC; fixtures are requested by calendar date string.
    # We'll use the current UTC date unless you want to shift by TZ; API-Football expects YYYY-MM-DD.
    # If you need explicit Brussels date: compute offset separately; for now, use UTC calendar date.
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _build_match_payload_from_fixture(fx: Dict[str, Any], odds_src: str, odds: Dict[str, Any]) -> Dict[str, Any]:
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
            "source": odds_src,
        },
        # You can add more fields later; get_prediction() only needs the above.
    }
    return payload


def _process_fixture(fx: Dict[str, Any], *, odds_source: str = "apifootball") -> None:
    """Process a single fixture end-to-end."""
    fixture_id = (fx.get("fixture") or {}).get("id")
    if not fixture_id:
        logging.warning("Skipping fixture without fixture_id.")
        return

    # 1) Insert/Upsert the match row (your insert_match expects the raw API-Football fixture dict)
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

    # 3) Build payload for the LLM predictor
    try:
        teams = fx.get("teams") or {}
        home_id = (teams.get("home") or {}).get("id")
        away_id = (teams.get("away") or {}).get("id")
        h2h = get_head_to_head(home_id, away_id, limit=3) if (home_id and away_id) else []
    except Exception:
        h2h = []

    payload = _build_match_payload_from_fixture(fx, odds_source, odds_raw)
    payload["head_to_head"] = h2h

    # Guard: need an Over 2.5 price for the predictor
    if payload.get("odds", {}).get("over_2_5") is None:
        logging.info(f"ℹ️ No Over 2.5 price for fixture {fixture_id}; skipping prediction.")
        return

    # 4) Run your predictor (utils/get_prediction.py)
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

    # 5) Invert pick and swap to counterpart odds when available
    try:
        pred = invert_ou25_prediction(pred, odds_raw=odds_raw, src=odds_source)
    except Exception as e:
        logging.warning(f"⚠️ Inversion failed for fixture {fixture_id}: {e}")

    # 6) Store value prediction (your insert_value_predictions handles gating/validation)
    try:
        count, msg = insert_value_predictions(pred, odds_source=odds_source)
        if count:
            logging.info(
                f"✅ Stored inverted value prediction {fixture_id}: "
                f"{pred.get('prediction')} @ {pred.get('odds')} | {msg}"
            )
        else:
            logging.info(f"⛔ Skipped {fixture_id}: {msg}")
    except Exception as e:
        logging.error(f"❌ insert_value_predictions failed for fixture {fixture_id}: {e}")


def _process_fixtures_for_date(date_str: str, *, odds_source: str = "apifootball") -> None:
    fixtures = []
    try:
        fixtures = fetch_fixtures(date_str) or []
    except Exception as e:
        logging.error(f"❌ fetch_fixtures failed for {date_str}: {e}")
        return

    if not fixtures:
        logging.info(f"ℹ️ No fixtures for {date_str}.")
        return

    for fx in fixtures:
        try:
            _process_fixture(fx, odds_source=odds_source)
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


def _fetch_single_fixture(fixture_id: int) -> Optional[Dict[str, Any]]:
    """Fetch a single fixture by id using fetch_fixtures’ shape (API-Football format)."""
    # API-Football supports ?id=; but we already have fetch_fixtures(date).
    # We’ll do a minimal fallback: try to get odds + build a stub if needed.
    # Better: if you have a fetch_fixture_by_id() in get_football_data.py, plug it here.
    try:
        # Try to reconstruct a minimal fixture shape so insert_match() works gracefully.
        odds = get_match_odds(fixture_id) or {}
        return {"fixture": {"id": fixture_id}, "teams": {}, "league": {}, "odds": odds}
    except Exception:
        return {"fixture": {"id": fixture_id}, "teams": {}, "league": {}}


if __name__ == "__main__":
    odds_src = os.getenv("ODDS_SOURCE", "apifootball")
    argv_ids = _parse_fixture_ids_from_argv(sys.argv[1:])

    if argv_ids:
        # Process explicit fixture IDs (e.g., invoked by a job with known IDs)
        for fid in argv_ids:
            fx = _fetch_single_fixture(fid) or {"fixture": {"id": fid}}
            _process_fixture(fx, odds_source=odds_src)
        sys.exit(0)

    # Default: process today's fixtures
    date_str = _today_str_europe_brussels()
    _process_fixtures_for_date(date_str, odds_source=odds_src)
