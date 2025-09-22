# utils/get_prediction.py
# -*- coding: utf-8 -*-
import os
import json
import time
import logging
from typing import Any, Dict, Tuple, Optional

from dotenv import load_dotenv
from openai import OpenAI
from openai._exceptions import OpenAIError, RateLimitError, APIConnectionError

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --- Config -------------------------------------------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # standard name
MODEL_NAME = os.getenv("MODEL_NAME", "gpt-5-mini")  # allow override
MAX_RETRIES = int(os.getenv("OPENAI_MAX_RETRIES", "3"))
RETRY_BACKOFF_SEC = float(os.getenv("OPENAI_RETRY_BACKOFF_SEC", "2.0"))

# hard guards for your insertion logic
MIN_ODDS = float(os.getenv("PREDICTION_MIN_ODDS", "1.6"))
MAX_ODDS = float(os.getenv("PREDICTION_MAX_ODDS", "2.3"))

if not OPENAI_API_KEY:
    raise RuntimeError("Missing OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)

# Response schema the model must follow (Structured Outputs)
# Ref: https://platform.openai.com/docs/guides/structured-outputs
PREDICTION_SCHEMA = {
    "name": "MatchPredictions",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "fixture_id": {"type": "integer"},
            "predictions": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "one_x_two": {  # 1X2 market
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "prediction": {"type": "string", "enum": ["Home", "Draw", "Away"]},
                            "confidence": {"type": "number", "minimum": 0, "maximum": 100},
                            "edge": {"type": "number"},
                            "po_value": {"type": "boolean"},
                            "odds": {"type": "number"},
                            "bankroll_pct": {"type": "number", "minimum": 0, "maximum": 10},
                            "rationale": {"type": "string"}
                        },
                        "required": ["prediction", "confidence", "edge", "po_value", "odds", "bankroll_pct", "rationale"]
                    },
                    "btts": {  # both teams to score
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "prediction": {"type": "string", "enum": ["Yes", "No"]},
                            "confidence": {"type": "number", "minimum": 0, "maximum": 100},
                            "edge": {"type": "number"},
                            "po_value": {"type": "boolean"},
                            "odds": {"type": "number"},
                            "bankroll_pct": {"type": "number", "minimum": 0, "maximum": 10},
                            "rationale": {"type": "string"}
                        },
                        "required": ["prediction", "confidence", "edge", "po_value", "odds", "bankroll_pct", "rationale"]
                    },
                    "over_2_5": {  # over/under line fixed to 2.5 as per project
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "prediction": {"type": "string", "enum": ["Over", "Under"]},
                            "confidence": {"type": "number", "minimum": 0, "maximum": 100},
                            "edge": {"type": "number"},
                            "po_value": {"type": "boolean"},
                            "odds": {"type": "number"},
                            "bankroll_pct": {"type": "number", "minimum": 0, "maximum": 10},
                            "rationale": {"type": "string"}
                        },
                        "required": ["prediction", "confidence", "edge", "po_value", "odds", "bankroll_pct", "rationale"]
                    }
                },
                "required": ["one_x_two", "btts", "over_2_5"]
            }
        },
        "required": ["fixture_id", "predictions"]
    },
    "strict": True
}

SYSTEM_INSTRUCTIONS = (
    "You are an expert football betting analyst. "
    "Given structured match data (teams, form, injuries, H2H, league, venue, odds), "
    "evaluate three markets (1X2, BTTS, Over/Under 2.5). "
    "Return calibrated probabilities -> edge vs. listed odds -> value determination. "
    "Only mark po_value true when expected value is positive and odds are within the supplied range. "
    "Keep bankroll_pct modest (e.g., 0.25–1.5) unless edge is exceptional. "
    "Return ONLY the JSON matching the schema; no extra commentary."
)

def _validate_prediction_block(name: str, block: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    required = ["prediction", "confidence", "edge", "po_value", "odds", "bankroll_pct", "rationale"]
    missing = [k for k in required if k not in block]
    if missing:
        return False, f"{name}: missing {missing}"
    if not (MIN_ODDS <= float(block["odds"]) <= MAX_ODDS):
        # We still return it; insertion layer can decide to skip. Just log here.
        logger.warning("⚠️ %s odds out of range: %.3f (allowed %.2f–%.2f)",
                       name, float(block["odds"]), MIN_ODDS, MAX_ODDS)
    return True, None

def _validate_full_response(data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    if "fixture_id" not in data or "predictions" not in data:
        return False, "Missing top-level fields"
    preds = data["predictions"]
    for k in ["one_x_two", "btts", "over_2_5"]:
        if k not in preds:
            return False, f"Missing market {k}"
        ok, err = _validate_prediction_block(k, preds[k])
        if not ok:
            return False, err
    return True, None

def _call_model(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calls GPT-5 mini with Structured Outputs and returns a parsed dict.
    Uses Responses API; falls back to text parsing if the SDK shape changes.
    """
    # Build user content
    user_content = [
        {"type": "text", "text": "Match data JSON follows."},
        {"type": "input_text", "text": json.dumps(payload, ensure_ascii=False)}
    ]

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = client.responses.create(
                model=MODEL_NAME,
                # Responses API accepts either `input` or `messages`; we use `input` blocks.
                input=[
                    {"role": "system", "content": [{"type": "text", "text": SYSTEM_INSTRUCTIONS}]},
                    {"role": "user", "content": user_content},
                ],
                response_format={"type": "json_schema", "json_schema": PREDICTION_SCHEMA},
                temperature=0.2,
                max_output_tokens=900,
                # New GPT-5 controls (optional; harmless if ignored by older SDKs)
                verbosity="low",                   # 'low'|'medium'|'high'  (controls answer length) 
                reasoning={"effort": "medium"},    # 'minimal'|'low'|'medium'|'high'
            )

            # Prefer parsed output if SDK provides it; else parse text
            parsed = None
            try:
                # Some SDKs expose a convenience accessor
                parsed = getattr(resp, "output_parsed", None)
            except Exception:
                parsed = None

            if parsed is None:
                # Generic path: combine text items
                text = ""
                for item in getattr(resp, "output", []):
                    for c in item.get("content", []):
                        if c.get("type") == "output_text":
                            text += c.get("text", "")
                if not text:
                    # Some SDKs nest content differently; fall back to str(resp)
                    text = getattr(resp, "output_text", "") or str(resp)
                parsed = json.loads(text)

            return parsed

        except (RateLimitError, APIConnectionError) as e:
            last_err = e
            sleep_for = RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
            logger.warning("OpenAI transient error (%s). Retry %d/%d in %.1fs",
                           type(e).__name__, attempt, MAX_RETRIES, sleep_for)
            time.sleep(sleep_for)
        except OpenAIError as e:
            # Non-retryable API error
            logger.error("OpenAI API error: %s", e)
            raise
        except Exception as e:
            last_err = e
            logger.warning("Model call failed (attempt %d/%d): %s", attempt, MAX_RETRIES, e)
            time.sleep(RETRY_BACKOFF_SEC)

    # Exhausted retries
    raise RuntimeError(f"Failed to get model response after {MAX_RETRIES} attempts: {last_err}")

def get_prediction(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Public entry point used by main.py
    - match_data should contain everything you already store (fixture_id, teams, odds, form, injuries, h2h, league, venue).
    - Returns structured dict matching PREDICTION_SCHEMA or None if invalid.
    """
    try:
        # Provide odds bounds to the model so it won’t mark PO if outside range
        payload = {
            "fixture_id": match_data.get("fixture_id"),
            "teams": match_data.get("teams"),
            "odds": match_data.get("odds"),
            "form": match_data.get("form"),
            "injuries": match_data.get("injuries"),
            "head_to_head": match_data.get("head_to_head"),
            "league": match_data.get("league"),
            "venue": match_data.get("venue"),
            "constraints": {
                "min_odds": MIN_ODDS,
                "max_odds": MAX_ODDS
            }
        }

        result = _call_model(payload)
        ok, err = _validate_full_response(result)
        if not ok:
            logger.error("❌ Invalid prediction shape: %s", err)
            return None

        # Final sanity logs (do not mutate)
        fixture_id = result["fixture_id"]
        logger.info("✅ Prediction ready for fixture %s", fixture_id)
        return result

    except Exception as e:
        logger.error("❌ Failed to get prediction for fixture %s: %s",
                     match_data.get("fixture_id"), e)
        return None

if __name__ == "__main__":
    # Smoke test with minimal stub
    dummy_match = {
        "fixture_id": 123456,
        "teams": {"home": "A FC", "away": "B United"},
        "odds": {"one_x_two": {"Home": 2.05, "Draw": 3.30, "Away": 3.60},
                 "btts": {"Yes": 1.95, "No": 1.85},
                 "over_2_5": 1.72},
        "form": {"home": {}, "away": {}},
        "injuries": {"home": [], "away": []},
        "head_to_head": [],
        "league": {"name": "Test League", "country": "TL", "round": "R1"},
        "venue": "Test Stadium"
    }
    out = get_prediction(dummy_match)
    print(json.dumps(out or {"error": "no output"}, ensure_ascii=False, indent=2))
