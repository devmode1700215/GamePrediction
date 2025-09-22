# utils/get_prediction.py
# -*- coding: utf-8 -*-

import os
import json
import time
import logging
import inspect
from typing import Any, Dict, Tuple, Optional, Iterable

from dotenv import load_dotenv
from openai import OpenAI

# ------------------------------------------------------------------------------
# Boot & logging
# ------------------------------------------------------------------------------
load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ------------------------------------------------------------------------------
# Config (env-driven)
# ------------------------------------------------------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL_NAME = os.getenv("MODEL_NAME", "gpt-5-mini")
MAX_RETRIES = int(os.getenv("OPENAI_MAX_RETRIES", "3"))
RETRY_BACKOFF_SEC = float(os.getenv("OPENAI_RETRY_BACKOFF_SEC", "2.0"))

# Odds guardrails (used by your insertion layer, but we also warn here)
MIN_ODDS = float(os.getenv("PREDICTION_MIN_ODDS", "1.6"))
MAX_ODDS = float(os.getenv("PREDICTION_MAX_ODDS", "2.3"))

if not OPENAI_API_KEY:
    raise RuntimeError("Missing OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)

# ------------------------------------------------------------------------------
# Structured Output Schema (JSON Schema for model)
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
# Responses API helpers (robust parsing across SDK versions)
# ------------------------------------------------------------------------------
def _supports_arg(fn, name: str) -> bool:
    try:
        return name in inspect.signature(fn).parameters
    except Exception:
        return False

def _responses_tokens_kw() -> str:
    """Return the correct tokens kw: 'max_output_tokens' (new) or 'max_tokens' (older)."""
    return "max_output_tokens" if _supports_arg(client.responses.create, "max_output_tokens") else "max_tokens"

def _iter_all_values(obj: Any) -> Iterable[Any]:
    """Depth-first iterator over nested values of dict/list/SDK objects."""
    if isinstance(obj, dict):
        for v in obj.values():
            yield v
            yield from _iter_all_values(v)
    elif isinstance(obj, list):
        for v in obj:
            yield v
            yield from _iter_all_values(v)
    else:
        # SDK objects: try model_dump()/dict()
        for attr in ("model_dump", "dict"):
            try:
                if hasattr(obj, attr):
                    res = getattr(obj, attr)()
                    yield res
                    yield from _iter_all_values(res)
                    return
            except Exception:
                pass
        # Also probe common attributes
        for name in ("content", "output", "parsed", "text"):
            try:
                v = getattr(obj, name, None)
                if v is not None:
                    yield v
                    yield from _iter_all_values(v)
            except Exception:
                pass

def _deep_find_prediction_obj(obj: Any, require_full: bool = True) -> Optional[Dict[str, Any]]:
    """
    Find the first dict that matches the expected shape.
    If require_full=True, require all three markets to be present.
    """
    def is_full_schema(d: Dict[str, Any]) -> bool:
        if not (isinstance(d, dict) and "fixture_id" in d and "predictions" in d and isinstance(d["predictions"], dict)):
            return False
        preds = d["predictions"]
        return all(k in preds for k in ("one_x_two", "btts", "over_2_5"))

    if isinstance(obj, dict):
        if (is_full_schema(obj) if require_full else ("fixture_id" in obj and "predictions" in obj)):
            return obj

    try:
        for v in _iter_all_values(obj):
            if isinstance(v, dict):
                if require_full:
                    if is_full_schema(v): return v
                else:
                    if "fixture_id" in v and "predictions" in v: return v
    except Exception:
        pass
    return None

def _extract_parsed_from_responses(resp) -> Optional[Dict[str, Any]]:
    """
    Pull parsed JSON directly from Responses API objects if present,
    falling back to a deep search through a dumped structure.
    """
    parsed = getattr(resp, "output_parsed", None)
    if parsed is not None:
        return parsed

    output_list = getattr(resp, "output", None)
    if output_list:
        for item in output_list:
            content = item.get("content", []) if isinstance(item, dict) else getattr(item, "content", []) or []
            for chunk in content:
                maybe = chunk.get("parsed", None) if isinstance(chunk, dict) else getattr(chunk, "parsed", None)
                if isinstance(maybe, (dict, list)):
                    if isinstance(maybe, dict):
                        return maybe
                    for x in maybe:
                        if isinstance(x, dict) and "fixture_id" in x and "predictions" in x:
                            return x

    try:
        if hasattr(resp, "model_dump"):
            dumped = resp.model_dump()
        elif hasattr(resp, "model_dump_json"):
            dumped = json.loads(resp.model_dump_json())
        else:
            dumped = json.loads(json.dumps(resp, default=lambda o: getattr(o, "__dict__", str(o))))
    except Exception:
        dumped = None

    if dumped is not None:
        found = _deep_find_prediction_obj(dumped, require_full=True) or _deep_find_prediction_obj(dumped, require_full=False)
        if found:
            return found
    return None

def _parse_responses_text(resp) -> str:
    """
    Extract text from a Responses response, handling SDK objects.
    Aggregates only textual outputs; ignores reasoning traces.
    """
    txt = getattr(resp, "output_text", None)
    if isinstance(txt, str) and txt.strip():
        return txt

    text_parts = []
    output_list = getattr(resp, "output", None)
    if output_list is None:
        msg = getattr(resp, "message", None)
        content = getattr(msg, "content", None) if msg is not None else None
        if isinstance(content, str):
            return content
        return ""

    for item in output_list:
        content = item.get("content", []) if isinstance(item, dict) else getattr(item, "content", []) or []
        for chunk in content:
            ctype = chunk.get("type") if isinstance(chunk, dict) else getattr(chunk, "type", None)
            ctext = chunk.get("text", "") if isinstance(chunk, dict) else getattr(chunk, "text", "")
            if ctype in ("output_text", "summary_text") and isinstance(ctext, str):
                text_parts.append(ctext)
    return "".join(text_parts)

def _extract_json_snippet(s: str) -> str:
    """As a last resort, pull the largest {...} block from a string."""
    if not isinstance(s, str) or not s:
        return ""
    start = s.find("{"); end = s.rfind("}")
    return s[start:end+1] if (start != -1 and end != -1 and end > start) else s.strip()

# ------------------------------------------------------------------------------
# Partial repair helpers (if model returns a partial object)
# ------------------------------------------------------------------------------
def _missing_markets(data: Dict[str, Any]) -> set:
    want = {"one_x_two", "btts", "over_2_5"}
    have = set(data.get("predictions", {}).keys()) if isinstance(data.get("predictions"), dict) else set()
    return want - have

def _repair_missing_markets(payload: Dict[str, Any], partial: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Ask the model to fill only the missing markets and return the full object.
    If uncertain, the model should set po_value=false and bankroll_pct=0.
    """
    missing = _missing_markets(partial)
    if not missing:
        return partial

    repair_instr = (
        "You previously returned a partial prediction object. "
        f"The following markets are missing: {', '.join(sorted(missing))}. "
        "Return the FULL object again, with ALL THREE markets present. "
        "If uncertain for any market, set po_value=false and bankroll_pct=0 (with a brief rationale). "
        "Do not change fixture_id. Output must match the JSON schema exactly."
    )

    msg_system = {"role": "system", "content": [{"type": "input_text", "text": SYSTEM_INSTRUCTIONS}]}
    msg_user = {
        "role": "user",
        "content": [
            {"type": "input_text", "text": repair_instr},
            {"type": "input_text", "text": "Original match payload:"},
            {"type": "input_text", "text": json.dumps(payload, ensure_ascii=False)},
            {"type": "input_text", "text": "Your previous (partial) result:"},
            {"type": "input_text", "text": json.dumps(partial, ensure_ascii=False)},
        ],
    }

    # Prefer text.format; fall back to response_format; then chat
    try:
        tokens_kw = _responses_tokens_kw()
        resp = client.responses.create(
            model=MODEL_NAME,
            input=[msg_system, msg_user],
            text={"format": {"type": "json_schema", "name": SCHEMA_NAME, "schema": PREDICTION_JSON_SCHEMA, "strict": STRICT_OUTPUT}},
            **{tokens_kw: 900},
        )
        fixed = _extract_parsed_from_responses(resp)
        if fixed: return fixed
        text_out = _parse_responses_text(resp)
        if text_out.strip(): return json.loads(text_out)
    except Exception:
        pass

    try:
        tokens_kw = _responses_tokens_kw()
        resp = client.responses.create(
            model=MODEL_NAME,
            input=[msg_system, msg_user],
            response_format={"type": "json_schema", "json_schema": {"name": SCHEMA_NAME, "schema": PREDICTION_JSON_SCHEMA, "strict": STRICT_OUTPUT}},
            **{tokens_kw: 900},
        )
        fixed = _extract_parsed_from_responses(resp)
        if fixed: return fixed
        text_out = _parse_responses_text(resp)
        if text_out.strip(): return json.loads(text_out)
    except Exception:
        pass

    try:
        cc = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_INSTRUCTIONS},
                {"role": "user", "content": repair_instr + "\n\nOriginal payload:\n" + json.dumps(payload, ensure_ascii=False) + "\n\nPartial result:\n" + json.dumps(partial, ensure_ascii=False)},
            ],
            response_format={"type": "json_schema", "json_schema": {"name": SCHEMA_NAME, "schema": PREDICTION_JSON_SCHEMA, "strict": STRICT_OUTPUT}},
            max_tokens=900,
        )
        text = cc.choices[0].message.content
        json_text = _extract_json_snippet(text)
        if json_text: return json.loads(json_text)
    except Exception:
        pass

    return None

# ------------------------------------------------------------------------------
# Model call (with compat shim across SDKs)
# ------------------------------------------------------------------------------
def _call_model(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calls GPT-5 mini with best-available method:
      A) Responses API + text.format (JSON schema w/ name+schema+strict)
         (if opaque output, gracefully fall through to B/C)
      B) Responses API + response_format (older)
      C) Chat Completions fallback (with/without response_format)
    """
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
        # ---- A) Responses + text.format ------------------------------------------------
        try:
            tokens_kw = _responses_tokens_kw()
            resp = client.responses.create(
                model=MODEL_NAME,
                input=[msg_system, msg_user],
                text={"format": {"type": "json_schema", "name": SCHEMA_NAME, "schema": PREDICTION_JSON_SCHEMA, "strict": STRICT_OUTPUT}},
                **{tokens_kw: 900},
            )

            parsed = _extract_parsed_from_responses(resp)
            if parsed is not None: return parsed

            text_out = _parse_responses_text(resp)
            if text_out.strip(): return json.loads(text_out)

            logger.debug("Responses text.format returned no text; trying response_format...")
        except Exception as e_a:
            last_err = e_a
            logger.debug("Path A failed: %s", e_a)

        # ---- B) Responses + response_format --------------------------------------------
        try:
            tokens_kw = _responses_tokens_kw()
            resp = client.responses.create(
                model=MODEL_NAME,
                input=[msg_system, msg_user],
                response_format={"type": "json_schema", "json_schema": {"name": SCHEMA_NAME, "schema": PREDICTION_JSON_SCHEMA, "strict": STRICT_OUTPUT}},
                **{tokens_kw: 900},
            )

            parsed = _extract_parsed_from_responses(resp)
            if parsed is not None: return parsed

            text_out = _parse_responses_text(resp)
            if text_out.strip(): return json.loads(text_out)

            logger.debug("Responses response_format returned no text; trying Chat Completions...")
        except Exception as e_b:
            last_err = e_b
            logger.debug("Path B failed: %s", e_b)

        # ---- C1) Chat Completions with response_format ---------------------------------
        try:
            cc = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": SYSTEM_INSTRUCTIONS},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                response_format={"type": "json_schema", "json_schema": {"name": SCHEMA_NAME, "schema": PREDICTION_JSON_SCHEMA, "strict": STRICT_OUTPUT}},
                max_tokens=900,
            )
            text = cc.choices[0].message.content
            json_text = _extract_json_snippet(text)
            if json_text: return json.loads(json_text)
        except TypeError as e_c1:
            last_err = e_c1
            logger.debug("Chat Completions response_format not supported: %s", e_c1)
        except Exception as e_c2:
            last_err = e_c2
            logger.debug("Chat Completions with response_format failed: %s", e_c2)

        # ---- C2) Oldest Chat Completions path (instruction-only JSON) ------------------
        try:
            sys_instr = SYSTEM_INSTRUCTIONS + " IMPORTANT: Reply ONLY with JSON that matches the schema keys."
            cc = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": sys_instr},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                max_tokens=900,
            )
            text = cc.choices[0].message.content
            json_text = _extract_json_snippet(text)
            if json_text: return json.loads(json_text)
        except Exception as e_c3:
            last_err = e_c3
            logger.debug("Chat Completions fallback failed: %s", e_c3)

        # Retry w/ backoff
        if attempt < MAX_RETRIES:
            sleep_for = RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
            logger.warning("Model call failed (attempt %d/%d): %s. Retrying in %.1fs", attempt, MAX_RETRIES, last_err, sleep_for)
            time.sleep(sleep_for)
        else:
            break

    raise RuntimeError(f"Failed to get model response after {MAX_RETRIES} attempts: {last_err}")

# ------------------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------------------
def get_prediction(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Entry point used by main.py.
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

        # If the model returned a partial object, attempt a one-shot repair
        missing = _missing_markets(result)
        if missing:
            logger.warning("⚠️ Partial prediction detected; missing markets: %s. Attempting repair...", ", ".join(sorted(missing)))
            repaired = _repair_missing_markets(payload, result)
            if repaired:
                result = repaired

        ok, err = _validate_full_response(result)
        if not ok:
            logger.error("❌ Invalid prediction shape: %s", err)
            return None

        logger.info("✅ Prediction ready for fixture %s", result.get("fixture_id"))
        return result

    except Exception as e:
        logger.error("❌ Failed to get prediction for fixture %s: %s", match_data.get("fixture_id"), e)
        return None

# ------------------------------------------------------------------------------
# Smoke test (run: python utils/get_prediction.py)
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
