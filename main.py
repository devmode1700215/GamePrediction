# main.py  (only two additions marked >>>)
import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

ODDS_SOURCE = os.getenv("ODDS_SOURCE", "apifootball")
PREFERRED_BOOK = os.getenv("PREF_BOOK", "Bwin")
INVERT_PREDICTIONS = os.getenv("INVERT_PREDICTIONS", "false").lower() in ("1", "true", "yes", "y")
SCORING_DEBUG = os.getenv("SCORING_DEBUG", "true").lower() in ("1","true","yes","y")

from utils.get_football_data import (
    fetch_fixtures,
    get_head_to_head,
    enrich_fixture,
)
from utils.odds_sources import get_ou25_best
from utils.insert_match import insert_match
from utils.insert_value_predictions import insert_value_predictions
from utils.get_prediction import get_prediction
from utils.staking import compute_stake_pct  # >>> NEW

try:
    from utils.value_inversion import invert_ou25_prediction
    _HAS_INVERTER = True
except Exception:
    _HAS_INVERTER = False

try:
    from utils.settle_results import settle_date
    _HAS_SETTLER = True
except Exception as _e:
    logging.info(f"Result settlement module not found (utils.settle_results): {_e}")
    _HAS_SETTLER = False

# ... (unchanged helpers omitted for brevity)

def _process_fixture(fx_raw: Dict[str, Any]) -> None:
    fixture_id = (fx_raw.get("fixture") or {}).get("id")
    if not fixture_id:
        logging.warning("Skipping fixture without fixture_id.")
        return

    fx_enriched = enrich_fixture(fx_raw, preferred_bookmaker=PREFERRED_BOOK)
    try:
        ok = insert_match(fx_enriched)
        if not ok:
            logging.warning(f"⚠️ insert_match returned False for fixture {fixture_id}")
    except Exception as e:
        logging.error(f"❌ insert_match failed for fixture {fixture_id}: {e}")

    odds_block = get_ou25_best(fixture_id, preferred_bookmaker=PREFERRED_BOOK)
    if odds_block.get("over_2_5") is None and odds_block.get("under_2_5") is None:
        logging.info(f"ℹ️ No OU2.5 prices for fixture {fixture_id}; skipping prediction.")
        return

    payload = _build_llm_payload_from_enriched(
        fx_enriched,
        odds_block,
        odds_src=(odds_block.get("source") or ODDS_SOURCE),
    )

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

    # Normalize + debug (existing code)
    pred = _normalize_prediction(pred, payload.get("odds") or {})
    _log_scoring_debug(fixture_id, pred)

    # Optional inversion (kept off by default)
    final_pred = _maybe_invert(pred, payload.get("odds") or {})

    # >>> NEW: compute stake_pct from scoring/edge/odds and attach
    final_pred["stake_pct"] = compute_stake_pct(final_pred)

    # Save
    try:
        count, msg = insert_value_predictions(final_pred, odds_source=(odds_block.get("source") or ODDS_SOURCE))
        if count:
            logging.info(
                f"✅ Stored prediction {fixture_id}: {final_pred.get('prediction')} @ {final_pred.get('odds')} "
                f"| conf={final_pred.get('confidence')} | edge={final_pred.get('edge')} | stake={final_pred.get('stake_pct')} "
                f"| src={odds_block.get('source')} | {msg}"
            )
        else:
            logging.info(f"⛔ Skipped {fixture_id}: {msg}")
    except Exception as e:
        logging.error(f"❌ insert_value_predictions failed for fixture {fixture_id}: {e}")
