# utils/get_prediction.py
# -*- coding: utf-8 -*-
import json
import os
import logging
from typing import Dict, Any, Tuple

from openai import OpenAI
from dotenv import load_dotenv

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# OpenAI client & system prompt
# ------------------------------------------------------------------------------
load_dotenv()
api_key = os.getenv("OPENAI_KEY")
if not api_key:
    logger.error("ERROR: Missing OPENAI_KEY environment variable")
    raise ValueError("OPENAI_KEY environment variable is required")

client = OpenAI(api_key=api_key)

# Load system prompt from prompt.txt
try:
    with open("prompt.txt", "r", encoding="utf-8") as f:
        PROMPT = f.read()
    if not PROMPT.strip():
        raise ValueError("Prompt file is empty")
    logger.info("✅ Prompt loaded successfully")
except FileNotFoundError:
    logger.error("ERROR: prompt.txt file not found")
    raise
except Exception as e:
    logger.error(f"ERROR: Failed to load prompt.txt: {e}")
    raise

# ------------------------------------------------------------------------------
# Odds keys we care about (mapping prediction market -> odds key in match_data)
# ------------------------------------------------------------------------------
ODDS_KEYS_MAP: Dict[str, str] = {
    "over_2_5": "over_2_5",
    "under_2_5": "under_2_5",
    "home_win": "home_win",
    "away_win": "away_win",
    "draw": "draw",
    "btts_yes": "btts_yes",
    "btts_no": "btts_no",
}

TRACKED_ODDS_KEYS = set(ODDS_KEYS_MAP.values())


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def _has_any_market_odds(match_data: Dict[str, Any]) -> Tuple[bool, Dict[str, float]]:
    """
    Returns (has_any, odds_dict) where odds_dict is the authoritative odds mapping
    from match_data['odds'] filtered to the keys we track.
    """
    odds = (match_data or {}).get("odds") or {}
    if not isinstance(odds, dict):
        return False, {}

    filtered = {}
    for k in TRACKED_ODDS_KEYS:
        v = odds.get(k)
        try:
            filtered[k] = float(v) if v is not None else None
        except (TypeError, ValueError):
            filtered[k] = None

    has_any = any(v is not None for v in filtered.values())
    return has_any, filtered


def validate_prediction_response(prediction_data: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate the structure of model output."""
    if not isinstance(prediction_data, dict):
        return False, "Response is not a valid JSON object"

    if "fixture_id" not in prediction_data:
        return False, "Missing fixture_id in response"

    predictions = prediction_data.get("predictions")
    if not isinstance(predictions, dict):
        return False, "Missing predictions object"

    # We require at least one market object present
    if not any(isinstance(v, dict) for v in predictions.values()):
        return False, "No prediction markets found"

    # We only strictly validate one well-known market's fields to keep flexibility
    sample = next((v for v in predictions.values() if isinstance(v, dict)), None)
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
    if sample is None or any(f not in sample for f in required_fields):
        return False, "Missing fields in at least one market"

    return True, ""


def _pin_odds_from_match(prediction_data: Dict[str, Any],
                         authoritative_odds: Dict[str, float]) -> Dict[str, Any]:
    """
    Overwrite any GPT-returned odds with the odds we fetched for the fixture.
    """
    preds = prediction_data.get("predictions") or {}
    replaced = []

    for market, details in preds.items():
        if not isinstance(details, dict):
            continue
        key = ODDS_KEYS_MAP.get(market)
        if not key:
            continue

        fixed = authoritative_odds.get(key)
        if fixed is not None:
            if details.get("odds") != fixed:
                replaced.append((market, details.get("odds"), fixed))
            details["odds"] = fixed  # enforce authoritative odds

    if replaced:
        logger.info(
            "🔧 Odds pinned for fixture %s: %s",
            prediction_data.get("fixture_id"),
            "; ".join([f"{m}:{old}→{new}" for (m, old, new) in replaced]),
        )
    return prediction_data


def _call_gpt4o(match_data: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    Call GPT-4o and return parsed JSON or None on failure.
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": PROMPT},
                {"role": "user", "content": json.dumps(match_data)},
            ],
            max_completion_tokens=1000,
        )

        if not response or not getattr(response, "choices", None):
            logger.error("❌ Empty response from GPT-4o")
            return None

        content = response.choices[0].message.content
        if not content:
            logger.error("❌ Empty content in GPT-4o response")
            return None

        logger.info(f"📝 Raw API response from GPT-4o: {content[:200]}...")
        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON decode error from GPT-4o: {e}")
            logger.error(f"Raw content: {content}")
            return None

    except Exception as e:
        logger.error(f"❌ Error calling GPT-4o: {e}")
        return None


# ------------------------------------------------------------------------------
# Public entry-point
# ------------------------------------------------------------------------------

def get_prediction(match_data: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    - Ensures there is at least one usable market odd before calling GPT
    - Calls GPT, validates structure
    - Pins odds with authoritative odds from match_data
    - Returns the final, cleaned prediction dict (or None)
    """
    fixture_id = (match_data or {}).get("fixture_id")
    logger.info(f"🔍 Requesting prediction for fixture {fixture_id}")

    has_any, authoritative_odds = _has_any_market_odds(match_data)
    if not has_any:
        logger.info("🪙 Skipping GPT for %s: no markets with odds.", fixture_id)
        return None

    prediction_data = _call_gpt4o(match_data)
    if prediction_data is None:
        logger.error(f"❌ Failed to get prediction for fixture {fixture_id}")
        return None

    ok, msg = validate_prediction_response(prediction_data)
    if not ok:
        logger.error(f"❌ Invalid prediction response for fixture {fixture_id}: {msg}")
        return None

    # Overwrite model-returned odds with authoritative odds from the API
    prediction_data = _pin_odds_from_match(prediction_data, authoritative_odds)

    logger.info(f"✅ Successfully generated prediction for fixture {fixture_id}")
    return prediction_data


# ------------------------------------------------------------------------------
# Smoke test
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    dummy_match = {
        "fixture_id": 123456,
        "teams": {"home": "A", "away": "B"},
        "odds": {"over_2_5": 1.72, "under_2_5": 2.1, "home_win": None},
    }
    out = get_prediction(dummy_match)
    print(json.dumps(out or {"error": "no output"}, ensure_ascii=False))
