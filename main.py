# main.py
import os
import sys
import logging
from typing import Any, Dict, List, Optional

# ---------- Logging ----------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ---------- Project imports ----------
# Supabase + DB writers
from db.inserts import insert_match, insert_value_predictions

# Cached wrappers around your football data getters
from utils.cached_football_data import (
    get_match_odds,
    get_team_position,
    get_team_form_and_goals,
    get_recent_goals,
    get_team_injuries,
    get_head_to_head,
)

# Inversion helper for OU2.5 (Over/Under flip with counterpart odds)
from utils.value_inversion import invert_ou25_prediction


# =============================
# Optional dynamic integrations
# =============================

def _maybe_import_prediction_builder():
    """
    Try several likely locations for a project-specific prediction function.
    The function should look like:
        def generate_prediction_for_fixture(fixture_id: int, odds_raw: dict) -> dict: ...
    and return a dict containing at minimum:
        {
          "fixture_id": int,
          "market": "over_2_5",
          "prediction": "Over" or "Under",
          "odds": float,              # price for the original side (Over or Under)
          "edge": float,              # optional
          "stake": float,             # optional
          "confidence": float,        # optional
          "rationale": str            # optional
        }
    """
    candidates = [
        ("services.prediction", "generate_prediction_for_fixture"),
        ("model", "generate_prediction_for_fixture"),
        ("predict", "generate_prediction_for_fixture"),
    ]
    for mod, fn in candidates:
        try:
            module = __import__(mod, fromlist=[fn])
            return getattr(module, fn)
        except Exception:
            continue
    return None


def _maybe_import_match_builder():
    """
    Try several likely locations for a function that builds the match row for DB.
    Expected shape is project-specific; we pass it straight to insert_match(...).
    """
    candidates = [
        ("builders.match_row", "build_match_row"),
        ("services.match_builder", "build_match_row"),
        ("match_builder", "build_match_row"),
    ]
    for mod, fn in candidates:
        try:
            module = __import__(mod, fromlist=[fn])
            return getattr(module, fn)
        except Exception:
            continue
    return None


def _maybe_import_fixtures_provider():
    """
    Optional: if your project has a helper to fetch today's fixtures, we’ll use it.
    Should return List[int] of fixture IDs.
    """
    candidates = [
        ("providers.fixtures", "get_today_fixture_ids"),
        ("services.fixtures", "get_today_fixture_ids"),
        ("fixtures", "get_today_fixture_ids"),
    ]
    for mod, fn in candidates:
        try:
            module = __import__(mod, fromlist=[fn])
            return getattr(module, fn)
        except Exception:
            continue
    return None


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
    """
    Make sure essential keys exist and are valid.
    """
    out = dict(pred or {})
    if "market" not in out or not out["market"]:
        out["market"] = "over_2_5"  # default market used by your model
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
    # 🔒 normalize inputs
    pred = _normalize_prediction_shape(prediction)

    # 🔄 flip the pick & align price
    pred = invert_ou25_prediction(pred, odds_raw=odds_raw, src=src)

    # Write
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
    """
    Process a single fixture:
      - Upsert match row (no pre-read)
      - Fetch odds once (cached)
      - Generate model prediction
      - Invert OU2.5 pick and store with counterpart odds
    """
    logging.info(f"--- Processing fixture {fixture_id} ---")

    # 1) Odds (cached)
    odds_raw = get_match_odds(fixture_id) or {}
    logging.debug(f"odds_raw for {fixture_id}: {odds_raw}")

    # 2) Build match row (if project provides a builder)
    match_row: Dict[str, Any] = {"fixture_id": fixture_id}
    if build_match_row_fn:
        try:
            # Your builder likely needs more context; pass what you have
            match_row = build_match_row_fn(fixture_id=fixture_id, odds=odds_raw)
        except TypeError:
            # Fallback call signature
            match_row = build_match_row_fn(fixture_id)
        except Exception as e:
            logging.warning(f"⚠️ match_row builder failed, using minimal row. Err: {e}")

    upsert_match_row(fixture_id, match_row)

    # 3) Generate model prediction (project function)
    if not generate_prediction_fn:
        logging.warning("⚠️ No prediction builder found; skipping prediction insert.")
        return

    try:
        # Try calling with rich context first; then degrade
        try:
            prediction = generate_prediction_fn(fixture_id=fixture_id, odds_raw=odds_raw)
        except TypeError:
            prediction = generate_prediction_fn(fixture_id)
    except Exception as e:
        logging.error(f"❌ Prediction function failed for {fixture_id}: {e}")
        return

    if not prediction:
        logging.info(f"ℹ️ No prediction returned for {fixture_id}; skipping.")
        return

    # Ensure core fields exist
    prediction = dict(prediction)
    prediction.setdefault("fixture_id", fixture_id)

    if prediction.get("odds") is None:
        # Fallback: if your model didn't set odds, take Over price as base
        over_price = odds_raw.get("over_2_5")
        if over_price is not None:
            try:
                prediction["odds"] = float(over_price)
            except Exception:
                pass

    # 4) Store inverted value prediction
    store_value_prediction(prediction, odds_raw=odds_raw, src=odds_source)


def process_fixtures(fixture_ids: List[int], odds_source: str = "apifootball") -> None:
    """
    Batch process a list of fixtures.
    """
    if not fixture_ids:
        logging.info("No fixtures to process.")
        return

    # Try to wire in your project-specific builders if present
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
            logging.error(f"❌ Unhandled error on fixture {fid}: {e}")


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


if __name__ == "__main__":
    # 1) If fixture IDs are provided on the CLI, use them
    cli_fixture_ids = _parse_fixture_ids_from_argv(sys.argv[1:])
    if cli_fixture_ids:
        process_fixtures(cli_fixture_ids, odds_source=os.getenv("ODDS_SOURCE", "apifootball"))
        sys.exit(0)

    # 2) Otherwise, attempt to import a provider for today's fixtures (optional)
    get_today_fixture_ids = _maybe_import_fixtures_provider()
    if get_today_fixture_ids:
        try:
            today_ids = get_today_fixture_ids()
            process_fixtures(today_ids, odds_source=os.getenv("ODDS_SOURCE", "apifootball"))
            sys.exit(0)
        except Exception as e:
            logging.error(f"Failed to fetch today's fixtures: {e}")

    logging.info("No fixtures provided and no provider found. Nothing to do.")
