# main.py
import os
import sys
import logging
from typing import Any, Dict, List, Optional

# ----------------------------
# Path bootstrapping (Render)
# ----------------------------
HERE = os.path.dirname(os.path.abspath(__file__))
CANDIDATE_ROOTS = [
    HERE,
    os.path.dirname(HERE),                     # project/
    os.path.join(os.path.dirname(HERE), "src"),# project/src
]
for p in CANDIDATE_ROOTS:
    if p not in sys.path:
        sys.path.insert(0, p)

# ---------- Logging ----------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ----------------------------
# Imports with robust fallbacks
# ----------------------------

# Supabase client (used by DB fallbacks)
try:
    from utils.supabase_client import supabase
except Exception as e:
    supabase = None
    logging.warning(f"Could not import utils.supabase_client: {e}")

# DB inserts: try package import; if it fails, define light wrappers here
try:
    from db.inserts import insert_match as _insert_match_pkg, insert_value_predictions as _insert_value_predictions_pkg
    def insert_match(row: Dict[str, Any]):
        return _insert_match_pkg(row)
    def insert_value_predictions(row: Dict[str, Any], odds_source: str = ""):
        return _insert_value_predictions_pkg(row, odds_source=odds_source)
except Exception as e:
    logging.warning(f"db.inserts not found; using local Supabase wrappers. ({e})")
    if supabase is None:
        raise ImportError(
            "Cannot load db.inserts or supabase client. "
            "Ensure utils/supabase_client.py exists or fix import paths."
        )
    # Fallback upserts (adjust table names/keys if needed)
    def insert_match(row: Dict[str, Any]):
        return supabase.table("matches").upsert(row, on_conflict="fixture_id").execute()
    def insert_value_predictions(row: Dict[str, Any], odds_source: str = ""):
        payload = dict(row)
        if odds_source:
            payload["odds_source"] = odds_source
        return supabase.table("value_predictions").upsert(payload, on_conflict="fixture_id").execute()

# Cached football data; fallback to raw getters if cached module missing
try:
    from utils.cached_football_data import (
        get_match_odds,
        get_team_position,
        get_team_form_and_goals,
        get_recent_goals,
        get_team_injuries,
        get_head_to_head,
    )
except Exception as e:
    logging.warning(f"utils.cached_football_data not found; falling back to utils.get_football_data. ({e})")
    from utils.get_football_data import (
        get_match_odds,
        get_team_position,
        get_team_form_and_goals,
        get_recent_goals,
        get_team_injuries,
        get_head_to_head,
    )

# Inversion helper
try:
    from utils.value_inversion import invert_ou25_prediction
except Exception as e:
    logging.warning(f"utils.value_inversion not found; attempting utils.invert. ({e})")
    try:
        from utils.invert import invert_ou25_prediction  # ultra-minimal inverter
    except Exception as e2:
        raise ImportError(
            "No inversion helper found. Add utils/value_inversion.py or utils/invert.py"
        ) from e2

# Optional, auto-discover project-specific builders
def _maybe_import(pred_module: str, fn_name: str):
    try:
        m = __import__(pred_module, fromlist=[fn_name])
        return getattr(m, fn_name)
    except Exception:
        return None

def _maybe_import_prediction_builder():
    return (
        _maybe_import("services.prediction", "generate_prediction_for_fixture")
        or _maybe_import("model", "generate_prediction_for_fixture")
        or _maybe_import("predict", "generate_prediction_for_fixture")
    )

def _maybe_import_match_builder():
    return (
        _maybe_import("builders.match_row", "build_match_row")
        or _maybe_import("services.match_builder", "build_match_row")
        or _maybe_import("match_builder", "build_match_row")
    )

def _maybe_import_fixtures_provider():
    return (
        _maybe_import("providers.fixtures", "get_today_fixture_ids")
        or _maybe_import("services.fixtures", "get_today_fixture_ids")
        or _maybe_import("fixtures", "get_today_fixture_ids")
    )

# =============================
# Core helpers
# =============================

def upsert_match_row(fixture_id: int, match_row: Dict[str, Any]) -> None:
    """
    Upsert the match row without a pre-read existence check.
    Assumes insert_match(...) uses upsert semantics in DB.
    """
    try:
        insert_match(match_row)
        logging.info(f"📝 Upserted match {fixture_id}")
    except Exception as e:
        logging.error(f"❌ Error upserting match {fixture_id}: {e}")

def _normalize_prediction_shape(pred: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(pred or {})
    if "market" not in out or not out["market"]:
        out["market"] = "over_2_5"
    if "prediction" not in out:
        raise ValueError("Prediction dict missing required 'prediction' key.")
    if "fixture_id" not in out:
        raise ValueError("Prediction dict missing required 'fixture_id' key.")
    return out

def store_value_prediction(
    prediction: Dict[str, Any],
    odds_raw: Optional[Dict[str, Any]],
    src: str,
) -> Optional[Any]:
    """
    Flips OU2.5 prediction to its exact opposite and writes it with the
    correct counterpart odds when available.
    Also sets edge=abs(edge) and po_value=True to pass value filters.
    """
    pred = _normalize_prediction_shape(prediction)
    pred = invert_ou25_prediction(pred, odds_raw=odds_raw, src=src)
    try:
        res = insert_value_predictions(pred, odds_source=src)
        logging.info(
            f"✅ Stored inverted value prediction for fixture {pred.get('fixture_id')} "
            f"({pred.get('market')} -> {pred.get('prediction')} @ {pred.get('odds')})"
        )
        return res
    except Exception as e:
        logging.error(f"❌ Error inserting value prediction: {e}")
        return None

# =============================
# Fixture processing
# =============================

def process_fixture(
    fixture_id: int,
    odds_source: str = "apifootball",
    build_match_row_fn=None,
    generate_prediction_fn=None,
) -> None:
    logging.info(f"--- Processing fixture {fixture_id} ---")

    # 1) Odds (cached if cached_football_data is present)
    try:
        odds_raw = get_match_odds(fixture_id) or {}
    except Exception as e:
        logging.warning(f"Could not fetch odds for {fixture_id}: {e}")
        odds_raw = {}

    # 2) Build/Upsert match row
    match_row: Dict[str, Any] = {"fixture_id": fixture_id}
    if build_match_row_fn:
        try:
            match_row = build_match_row_fn(fixture_id=fixture_id, odds=odds_raw)
        except TypeError:
            match_row = build_match_row_fn(fixture_id)
        except Exception as e:
            logging.warning(f"Match row builder failed for {fixture_id}: {e}; using minimal row.")
    upsert_match_row(fixture_id, match_row)

    # 3) Predict
    if not generate_prediction_fn:
        logging.warning("No prediction builder found; skipping prediction insert.")
        return
    try:
        try:
            prediction = generate_prediction_fn(fixture_id=fixture_id, odds_raw=odds_raw)
        except TypeError:
            prediction = generate_prediction_fn(fixture_id)
    except Exception as e:
        logging.error(f"Prediction function failed for {fixture_id}: {e}")
        return
    if not prediction:
        logging.info(f"No prediction returned for {fixture_id}; skipping.")
        return

    prediction = dict(prediction)
    prediction.setdefault("fixture_id", fixture_id)

    if prediction.get("odds") is None:
        over_price = odds_raw.get("over_2_5")
        if over_price is not None:
            try:
                prediction["odds"] = float(over_price)
            except Exception:
                pass

    # 4) Store inverted value prediction
    store_value_prediction(prediction, odds_raw=odds_raw, src=odds_source)

def process_fixtures(fixture_ids: List[int], odds_source: str = "apifootball") -> None:
    if not fixture_ids:
        logging.info("No fixtures to process.")
        return

    build_match_row_fn = _maybe_import_match_builder()
    generate_prediction_fn = _maybe_import_prediction_builder()

    for fid in fixture_ids:
        try:
            process_fixture(
                fixture_id=fid,
                odds_source=odds_source,
                build_match_row_fn=build_match_row_fn,
                generate_prediction_fn=generate_prediction_fn,
            )
        except Exception as e:
            logging.error(f"Unhandled error on fixture {fid}: {e}")

# =============================
# Entrypoint
# =============================

def _parse_fixture_ids_from_argv(argv: List[str]) -> List[int]:
    out = []
    for x in argv:
        try:
            out.append(int(x))
        except Exception:
            logging.warning(f"Skipping non-integer fixture id arg: {x}")
    return out

def _maybe_get_today_fixtures() -> List[int]:
    fn = _maybe_import_fixtures_provider()
    if not fn:
        return []
    try:
        return list(fn() or [])
    except Exception as e:
        logging.error(f"Failed to fetch today's fixtures: {e}")
        return []

if __name__ == "__main__":
    cli_fixture_ids = _parse_fixture_ids_from_argv(sys.argv[1:])
    if cli_fixture_ids:
        process_fixtures(cli_fixture_ids, odds_source=os.getenv("ODDS_SOURCE", "apifootball"))
        sys.exit(0)

    today_ids = _maybe_get_today_fixtures()
    if today_ids:
        process_fixtures(today_ids, odds_source=os.getenv("ODDS_SOURCE", "apifootball"))
        sys.exit(0)

    logging.info("No fixtures provided and no provider found. Nothing to do.")
