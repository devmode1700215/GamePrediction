# utils/get_prediction.py
# -*- coding: utf-8 -*-
import json
import os
import logging
from typing import Dict, Any, Optional
from openai import OpenAI
from dotenv import load_dotenv

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# OpenAI client
# ---------------------------------------------------------------------
load_dotenv()
api_key = os.getenv("OPENAI_KEY")
if not api_key:
    logger.error("ERROR: Missing OPENAI_KEY environment variable")
    raise ValueError("OPENAI_KEY environment variable is required")

client = OpenAI(api_key=api_key)

# ---------------------------------------------------------------------
# Load system prompt from prompt.txt
# ---------------------------------------------------------------------
try:
    with open("prompt.txt", "r", encoding="utf-8") as f:
        prompt = f.read()
    if not prompt.strip():
        raise ValueError("Prompt file is empty")
    logger.info("✅ Prompt loaded successfully")
except FileNotFoundError:
    logger.error("ERROR: prompt.txt file not found")
    raise
except Exception as e:
    logger.error(f"ERROR: Failed to load prompt.txt: {e}")
    raise

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _prune_markets_with_null_odds(match_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a slimmed match_data where odds with null/non-numeric values
    are dropped entirely. Keeps only markets with real numeric odds.
    """
    md = dict(match_data or {})
    raw_odds = md.get("odds") or {}
    cleaned: Dict[str, float] = {}
    if isinstance(raw_odds, dict):
        for k, v in raw_odds.items():
            try:
                if v is None:
                    continue
                cleaned[k] = float(v)
            except (TypeError, ValueError):
                # non-numeric or bad value -> skip
                continue
    md["odds"] = cleaned
    return md

def _sanitize_odds_from_input(prediction_data: Dict[str, Any], match_data_with_clean_odds: Dict[str, Any]) -> Dict[str, Any]:
    """
    Force each market's 'odds' in prediction_data to match the input odds.
    If a market has no input odds, drop it from predictions entirely.
    This prevents GPT from inventing odds (e.g., 1.8 when input was null).
    """
    preds = prediction_data.get("predictions") or {}
    in_odds = (match_data_with_clean_odds or {}).get("odds") or {}

    # If there's nothing to keep, zero out predictions.
    if not isinstance(preds, dict):
        prediction_data["predictions"] = {}
        return prediction_data

    fixed: Dict[str, Any] = {}
    for market, block in preds.items():
        if not isinstance(block, dict):
            continue

        # Use same key name by default. (Adjust mapping here if you alias keys.)
        input_key = market

        raw = in_odds.get(input_key)
        if raw is None:
            # No numeric odds present for this market in input → drop it.
            continue

        try:
            real_odds = float(raw)
        except (TypeError, ValueError):
            continue

        nb = dict(block)
        nb["odds"] = real_odds  # overwrite whatever GPT sent
        fixed[market] = nb

    prediction_data["predictions"] = fixed
    return prediction_data

def validate_prediction_response(prediction_data: Dict[str, Any]):
    """
    Validate structure without forcing a specific market (e.g., over_2_5).
    We only validate markets that exist after sanitization.
    """
    if not isinstance(prediction_data, dict):
        return False, "Response is not a valid JSON object"

    if "fixture_id" not in prediction_data:
        return False, "Missing fixture_id in response"

    predictions = prediction_data.get("predictions")
    if not isinstance(predictions, dict):
        return False, "Missing predictions object"

    # Per-market required fields (only for markets that exist)
    required_fields = [
        "prediction",
        "confidence",
        "implied_odds_pct",
        "edge",
        "po_value",
        "odds",
        "bankroll_pct",
        "rationale",
    ]

    for market, block in predictions.items():
        if not isinstance(block, dict):
            return False, f"Prediction block for {market} must be an object"
        missing = [f for f in required_fields if f not in block]
        if missing:
            return False, f"Missing fields for {market}: {missing}"

    return True, None

# ---------------------------------------------------------------------
# OpenAI call
# ---------------------------------------------------------------------
def call_gpt4o(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Call GPT-4o and return parsed JSON or None."""
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": json.dumps(match_data)}
            ],
            max_completion_tokens=1000
        )

        if not response or not getattr(response, "choices", None):
            logger.error("❌ Empty response from GPT-4o")
            return None

        content = response.choices[0].message.content
        if not content:
            logger.error("❌ Empty content in API response from GPT-4o")
            return None

        logger.info(f"📝 Raw API response from GPT-4o: {content[:200]}...")

        try:
            prediction_data = json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON decode error from GPT-4o: {e}")
            logger.error(f"Raw content: {content}")
            return None

        return prediction_data

    except Exception as e:
        logger.error(f"❌ Error calling GPT-4o: {e}")
        return None

# ---------------------------------------------------------------------
# Public entry
# ---------------------------------------------------------------------
def get_prediction(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    fixture_id = match_data.get("fixture_id")
    logger.info(f"🔍 Requesting prediction for fixture {fixture_id}")

    # 1) Prune markets with null/missing odds (saves tokens and avoids invented odds)
    slim = _prune_markets_with_null_odds(match_data)

    # 2) If we have no usable markets after pruning, skip GPT entirely
    if not slim.get("odds"):
        logger.info(f"🪙 Skipping GPT for {fixture_id}: no markets with odds.")
        return {"fixture_id": fixture_id, "predictions": {}}

    # 3) Call GPT
    prediction_data = call_gpt4o(slim)
    if not prediction_data:
        logger.error(f"❌ Failed to get prediction for fixture {fixture_id}")
        return None

    # 4) Enforce “never invent odds”: overwrite from slim input, drop unavailable markets
    prediction_data = _sanitize_odds_from_input(prediction_data, slim)

    # 5) Validate final structure (only the markets that remain)
    is_valid, error_msg = validate_prediction_response(prediction_data)
    if not is_valid:
        logger.error(f"❌ Invalid prediction response for fixture {fixture_id}: {error_msg}")
        return None

    logger.info(f"✅ Successfully generated prediction for fixture {fixture_id}")
    return prediction_data

# ---------------------------------------------------------------------
# Minimal smoke test
# ---------------------------------------------------------------------
if __name__ == "__main__":
    dummy_match = {
        "fixture_id": 123456,
        "teams": {"home": "A", "away": "B"},
        # simulate mixed odds; some nulls, some real
        "odds": {"over_2_5": 1.72, "btts_yes": None, "home_win": "2.15"}
    }
    result = get_prediction(dummy_match)
    print(json.dumps(result or {"error": "no output"}, ensure_ascii=False))
