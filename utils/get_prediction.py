# utils/get_prediction.py
# -*- coding: utf-8 -*-
import json
import os
import logging
from openai import OpenAI
from dotenv import load_dotenv

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- OpenAI client ----------
load_dotenv()
api_key = os.getenv("OPENAI_KEY")
if not api_key:
    logger.error("ERROR: Missing OPENAI_KEY environment variable")
    raise ValueError("OPENAI_KEY environment variable is required")

client = OpenAI(api_key=api_key)

# ---------- Load system prompt ----------
try:
    with open("prompt.txt", "r", encoding="utf-8") as f:
        prompt = f.read()
    if not prompt.strip():
        raise ValueError("Prompt file is empty")
    logger.info("✅ Prompt loaded successfully")
except FileNotFoundError:
    logger.error("❌ ERROR: prompt.txt file not found")
    raise
except Exception as e:
    logger.error(f"❌ ERROR: Failed to load prompt.txt: {e}")
    raise

# ---------- Validation ----------
def validate_prediction_response(prediction_data):
    """Validate the structure of prediction response."""
    if not isinstance(prediction_data, dict):
        return False, "Response is not a valid JSON object"

    if "fixture_id" not in prediction_data:
        return False, "Missing fixture_id in response"

    predictions = prediction_data.get("predictions")
    if not isinstance(predictions, dict):
        return False, "Missing predictions object"

    over_2_5 = predictions.get("over_2_5")
    if not isinstance(over_2_5, dict):
        return False, "Missing over_2_5 prediction"

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
    missing = [f for f in required_fields if f not in over_2_5]
    if missing:
        return False, f"Missing prediction fields: {missing}"

    return True, None

# ---------- Core ----------
def get_prediction(match_data):
    """
    Get prediction from OpenAI with comprehensive error handling.
    Expects match_data to be a dict that includes 'fixture_id' at minimum.
    Returns dict on success, or None on failure.
    """
    fixture_id = match_data.get("fixture_id")
    try:
        logger.info(f"🤖 Requesting prediction for fixture {fixture_id}")

        response = client.chat.completions.create(
            model="gpt-5-mini",
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": json.dumps(match_data)},
            ],
            max_completion_tokens=1000,
            request_timeout=30.0,
        )

        if not response or not getattr(response, "choices", None):
            logger.error(f"❌ Empty response from OpenAI API for fixture {fixture_id}")
            return None

        content = response.choices[0].message.content
        if not content:
            logger.error(f"❌ Empty content in API response for fixture {fixture_id}")
            return None

        logger.info(f"📝 Raw API response for {fixture_id}: {content[:200]}...")

        # Parse JSON safely
        try:
            prediction_data = json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON decode error for fixture {fixture_id}: {e}")
            logger.error(f"❌ Raw content: {content}")
            return None

        # Validate structure
        is_valid, error_msg = validate_prediction_response(prediction_data)
        if not is_valid:
            logger.error(f"❌ Invalid prediction response for fixture {fixture_id}: {error_msg}")
            logger.error(f"❌ Response data: {prediction_data}")
            return None

        logger.info(f"✅ Successfully generated prediction for fixture {fixture_id}")
        return prediction_data

    except Exception as e:
        logger.error(f"❌ Unexpected error getting prediction for fixture {fixture_id if fixture_id else 'unknown'}: {e}")
        return None

# ---------- Smoke test ----------
if __name__ == "__main__":
    # Minimal smoke test to verify syntax and runtime on Render
    dummy_match = {
        "fixture_id": 123456,
        "teams": {"home": "A", "away": "B"},
        "odds": {"over_2_5": 1.72},
    }
    result = get_prediction(dummy_match)
    print(json.dumps(result or {"error": "no output"}, ensure_ascii=False))
