# utils/get_prediction.py
# -*- coding: utf-8 -*-

import os
import json
import time
import logging
import inspect
from typing import Any, Dict, Tuple, Optional

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL_NAME = os.getenv("MODEL_NAME", "gpt-5-mini")
MAX_RETRIES = int(os.getenv("OPENAI_MAX_RETRIES", "3"))
RETRY_BACKOFF_SEC = float(os.getenv("OPENAI_RETRY_BACKOFF_SEC", "2.0"))

# guardrails used by your insertion logic
MIN_ODDS = float(os.getenv("PREDICTION_MIN_ODDS", "1.6"))
MAX_ODDS = float(os.getenv("PREDICTION_MAX_ODDS", "2.3"))

if not OPENAI_API_KEY:
    raise RuntimeError("Missing OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)

# ------------------------------------------------------------------------------
# Structured Output Schema
# ------------------------------------------------------------------------------
SCHEMA_NAME = "MatchPredictions"
PREDICTION_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "fixture_id": {"type": "integer"},
        "predictions": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "one_x_two": {
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
                "btts": {
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
                "over_2_5": {
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
}
STRICT_OUTPUT = True

SYSTEM_INSTRUCTIONS = (
    "You are an expert football betting analyst. "
    "Given structured match data (teams, form, injuries, H2H, league, venue, odds), "
    "evaluate three markets (1X2, BTTS, Over/Under 2.5). "
    "Calibrate probabilities, compute edge vs. listed odds, and set po_value true only for positive EV "
    "within the provided odds range. Keep bankroll_pct modest (0.25–1.5) unless edge is exceptional. "
    "Return ONLY JSON conforming to the provided schema."
)

# ------------------------------------------------------------------------------
# Validation helpers
# ------------------------------------------------------------------------------
def _validate_prediction_block(name: str, block: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    required = ["prediction", "confidence", "edge", "po_value", "odds", "bankroll_pct", "rationale"]
    missing = [k for k in required if k not in block]
    if missing:
        return False, f"{name}: missing {missing}"
    try:
        if not (MIN_ODDS <= float(block["odds"]) <= MAX_ODDS):
            logger.warning("⚠️ %s odds out of range: %.3f (allowed %.2f–%.2f)",
                           name, float(block["odds"]), MIN_ODDS, MAX_ODDS)
    except Exception:
        logger.warning("⚠️ %s has non-numeric odds: %r", name, block.get("odds"))
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

# ------------------------------------------------------------------------------
# Responses API compatibility helpers
# ------------------------------------------------------------------------------
def _supports_arg(fn, name: str) -> bool:
    try:
        return name in inspect.signature(fn).parameters
    except Exception:
        return False

def _responses_tokens_kw() -> str:
    """Return the correct tokens kw: 'max_output_tokens' (new) or 'max_tokens' (older)."""
    return "max_output_tokens" if _supports_arg(client.responses.create, "max_output_tokens") else "max_tokens"

def _parse_responses_text(resp) -> str:
    """
    Robustly extract plain text from a Responses API response, handling both
    dict-shaped payloads and SDK object instances (e.g., ResponseReasoningItem).
    We ignore non-text items (reasoning traces, etc.).
    """
    # 1) Prefer direct aggregate, if exposed by the SDK
    txt = getattr(resp, "output_text", None)
    if isinstance(txt, str) and txt.strip():
        return txt

    # 2) Walk the structured output list
    text_parts = []
    output_list = getattr(resp, "output", None)

    if output_list is None:
        # Some SDKs expose a top-level 'message' with content; last resort:
        # try to stringify any available 'content' field.
        msg = getattr(resp, "message", None)
        content = getattr(msg, "content", None) if msg is not None else None
        if isinstance(content, str):
            return content
        return ""

    for item in output_list:
        # item can be a dict or an SDK object
        if isinstance(item, dict):
            content = item.get("content", []) or []
        else:
            content = getattr(item, "content", []) or []

        # content is a list of chunks; each chunk can be dict or object
        for chunk in content:
            # detect the chunk type
            if isinstance(chunk, dict):
                ctype = chunk.get("type")
                ctext = chunk.get("text", "")
            else:
                ctype = getattr(chunk, "type", None)
                ctext = getattr(chunk, "text", "")

            # We only aggregate textual outputs
            if ctype in ("output_text", "summary_text") and isinstance(ctext, str):
                text_parts.append(ctext)

    return "".join(text_parts)


# ------------------------------------------------------------------------------
# Model call (with compat shim across SDKs)
# ------------------------------------------------------------------------------
def _call_model(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calls GPT-5 mini with best-available method:
      A) Responses API + text.format (JSON schema w/ name+schema+strict)
      B) Responses API + response_format (older)
      C) Chat Completions fallback
    NOTE: temperature/top_p omitted because many GPT-5/Reasoning models reject them.
    """
    # Use input_text for all content parts
    msg_system = {"role": "system", "content": [{"type": "input_text", "text": SYSTEM_INSTRUCTIONS}]}
    msg_user = {
        "role": "user",
        "content": [
            {"type": "input_text", "text": "Match data JSON follows."},
            {"type": "input_text", "text": json.dumps(payload, ensure_ascii=False)},
        ],
    }

    last_err: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # ---- Path A: Responses with text.format ----
            tokens_kw = _responses_tokens_kw()
            kwargs = {
                "model": MODEL_NAME,
                "input": [msg_system, msg_user],
                "text": {
                    "format": {
                        "type": "json_schema",
                        "name": SCHEMA_NAME,
                        "schema": PREDICTION_JSON_SCHEMA,
                        "strict": STRICT_OUTPUT,
                    }
                },
                tokens_kw: 900,
            }
            resp = client.responses.create(**kwargs)

            parsed = getattr(resp, "output_parsed", None)
            if parsed is not None:
                return parsed

            text_out = _parse_responses_text(resp)
            return json.loads(text_out)

        except TypeError as e_a:
            # If Path A kw args aren't supported, try Path B
            last_err = e_a
            try:
                tokens_kw = _responses_tokens_kw()
                kwargs = {
                    "model": MODEL_NAME,
                    "input": [msg_system, msg_user],
                    "response_format": {
                        "type": "json_schema",
                        "json_schema": {
                            "name": SCHEMA_NAME,
                            "schema": PREDICTION_JSON_SCHEMA,
                            "strict": STRICT_OUTPUT,
                        },
                    },
                    tokens_kw: 900,
                }
                resp = client.responses.create(**kwargs)

                parsed = getattr(resp, "output_parsed", None)
                if parsed is not None:
                    return parsed

                text_out = _parse_responses_text(resp)
                return json.loads(text_out)

            except TypeError as e_b:
                # Path C: Chat Completions fallback
                last_err = e_b
                cc = client.chat.completions.create(
                    model=MODEL_NAME,
                    messages=[
                        {"role": "system", "content": SYSTEM_INSTRUCTIONS},
                        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                    ],
                    response_format={
                        "type": "json_schema",
                        "json_schema": {
                            "name": SCHEMA_NAME,
                            "schema": PREDICTION_JSON_SCHEMA,
                            "strict": STRICT_OUTPUT,
                        },
                    },
                    max_tokens=900,
                )
                text = cc.choices[0].message.content
                return json.loads(text)

        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES:
                sleep_for = RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
                logger.warning(
                    "Model call failed (attempt %d/%d): %s. Retrying in %.1fs",
                    attempt, MAX_RETRIES, e, sleep_for
                )
                time.sleep(sleep_for)
            else:
                break

    raise RuntimeError(f"Failed to get model response after {MAX_RETRIES} attempts: {last_err}")

# ------------------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------------------
def get_prediction(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Entry point used by main.py
    Returns structured dict matching PREDICTION_JSON_SCHEMA or None if invalid.
    """
    try:
        payload = {
            "fixture_id": match_data.get("fixture_id"),
            "teams": match_data.get("teams"),
            "odds": match_data.get("odds"),
            "form": match_data.get("form"),
            "injuries": match_data.get("injuries"),
            "head_to_head": match_data.get("head_to_head"),
            "league": match_data.get("league"),
            "venue": match_data.get("venue"),
            "constraints": {"min_odds": MIN_ODDS, "max_odds": MAX_ODDS},
        }

        result = _call_model(payload)
        ok, err = _validate_full_response(result)
        if not ok:
            logger.error("❌ Invalid prediction shape: %s", err)
            return None

        logger.info("✅ Prediction ready for fixture %s", result.get("fixture_id"))
        return result

    except Exception as e:
        logger.error("❌ Failed to get prediction for fixture %s: %s",
                     match_data.get("fixture_id"), e)
        return None

# ------------------------------------------------------------------------------
# Smoke test
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    dummy_match = {
        "fixture_id": 123456,
        "teams": {"home": "A FC", "away": "B United"},
        "odds": {
            "one_x_two": {"Home": 2.05, "Draw": 3.30, "Away": 3.60},
            "btts": {"Yes": 1.95, "No": 1.85},
            "over_2_5": 1.72
        },
        "form": {"home": {}, "away": {}},
        "injuries": {"home": [], "away": []},
        "head_to_head": [],
        "league": {"name": "Test League", "country": "TL", "round": "R1"},
        "venue": "Test Stadium"
    }
    out = get_prediction(dummy_match)
    print(json.dumps(out or {"error": "no output"}, ensure_ascii=False, indent=2))
