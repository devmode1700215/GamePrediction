# utils/get_prediction.py
# -*- coding: utf-8 -*-
"""
Environment variables you can set (Render → Environment or .env):

OPENAI_API_KEY=sk-...
MODEL_NAME=gpt-5-nano           # default in code
OU25_USE_LLM=1                  # 1 to try the model; 0 to skip straight to local engine
OU25_ALLOW_CHAT=0               # 1 to allow chat-completions fallback (off by default)

# Guardrails & local engine tuning
PREDICTION_MIN_ODDS=1.60        # informational guardrail; we still return a record if outside
PREDICTION_MAX_ODDS=2.30
OU25_EDGE_THRESHOLD=0.02        # >= 2% EV to call it value
OU25_MAX_STAKE_PCT=1.5          # cap (percent of bankroll)
OU25_EDGE_TO_STAKE_COEF=0.20    # stake% ≈ coef * (edge * 100)
OU25_DEFAULT_VIG=0.05           # assumed overround if we can’t infer

# Optional: if your DB layer only inserts when po_value=true,
# you can force "insert anyway" behavior by treating even tiny positive EV as value.
OU25_ALWAYS_VALUE=0             # set to 1 to mark po_value=true for EV >= 0 (careful!)
"""

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
MODEL_NAME = os.getenv("MODEL_NAME", "gpt-5-nano")
MAX_RETRIES = int(os.getenv("OPENAI_MAX_RETRIES", "2"))            # keep retries short
RETRY_BACKOFF_SEC = float(os.getenv("OPENAI_RETRY_BACKOFF_SEC", "2.0"))

USE_LLM = os.getenv("OU25_USE_LLM", "1") == "1"
ALLOW_CHAT = os.getenv("OU25_ALLOW_CHAT", "0") == "1"

# Guardrails (informational)
MIN_ODDS = float(os.getenv("PREDICTION_MIN_ODDS", "1.6"))
MAX_ODDS = float(os.getenv("PREDICTION_MAX_ODDS", "2.3"))

# Local engine tuning
EDGE_THRESHOLD = float(os.getenv("OU25_EDGE_THRESHOLD", "0.02"))
MAX_STAKE = float(os.getenv("OU25_MAX_STAKE_PCT", "1.5"))
EDGE_TO_STAKE_COEF = float(os.getenv("OU25_EDGE_TO_STAKE_COEF", "0.20"))
DEFAULT_VIG = float(os.getenv("OU25_DEFAULT_VIG", "0.05"))
ALWAYS_VALUE = os.getenv("OU25_ALWAYS_VALUE", "0") == "1"

if not OPENAI_API_KEY:
    raise RuntimeError("Missing OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)

# ------------------------------------------------------------------------------
# Structured Output Schema — ONLY Over/Under 2.5
# ------------------------------------------------------------------------------
SCHEMA_NAME = "OverUnder25Prediction"
PREDICTION_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "fixture_id": {"type": "integer"},
        "predictions": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
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
            "required": ["over_2_5"]
        }
    },
    "required": ["fixture_id", "predictions"]
}
# Important: strict=False reduces "empty output" issues on some stacks.
STRICT_OUTPUT = False

SYSTEM_INSTRUCTIONS = (
    "You are an expert football betting analyst. "
    "Given structured match data (teams, form, injuries, H2H, league, venue, odds), "
    "evaluate ONLY the Over/Under 2.5 goals market. "
    f"Use odds guardrails of {MIN_ODDS}–{MAX_ODDS}. "
    "Calibrate probability; compute edge vs listed odds; set po_value true only for positive EV; "
    "stake modestly (0.25–1.5% unless edge exceptional). "
    "Return ONLY JSON matching the provided schema."
)

# ------------------------------------------------------------------------------
# Validation helpers
# ------------------------------------------------------------------------------
def _validate_prediction_block(block: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    req = ["prediction", "confidence", "edge", "po_value", "odds", "bankroll_pct", "rationale"]
    miss = [k for k in req if k not in block]
    if miss:
        return False, f"over_2_5: missing {miss}"
    try:
        if not (MIN_ODDS <= float(block["odds"]) <= MAX_ODDS):
            logger.info("ℹ️ over_2_5 odds outside preferred range: %.3f (%.2f–%.2f)",
                        float(block["odds"]), MIN_ODDS, MAX_ODDS)
    except Exception:
        logger.info("ℹ️ over_2_5 odds not numeric: %r", block.get("odds"))
    return True, None

def _validate_full_response(data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    if "fixture_id" not in data or "predictions" not in data:
        return False, "Missing top-level fields"
    preds = data["predictions"]
    if "over_2_5" not in preds:
        return False, "Missing market over_2_5"
    ok, err = _validate_prediction_block(preds["over_2_5"])
    return (ok, err)

# ------------------------------------------------------------------------------
# SDK capability introspection
# ------------------------------------------------------------------------------
def _supports_arg(fn, name: str) -> bool:
    try:
        return name in inspect.signature(fn).parameters
    except Exception:
        return False

def _responses_tokens_kw() -> str:
    return "max_output_tokens" if _supports_arg(client.responses.create, "max_output_tokens") else "max_tokens"

def _responses_supports_param(name: str) -> bool:
    try:
        return name in inspect.signature(client.responses.create).parameters
    except Exception:
        return False

def _chat_tokens_kwargs(n: int = 600) -> dict:
    try:
        if "max_completion_tokens" in inspect.signature(client.chat.completions.create).parameters:
            return {"max_completion_tokens": n}
    except Exception:
        pass
    return {"max_tokens": n}

# ------------------------------------------------------------------------------
# Traversal/extraction helpers
# ------------------------------------------------------------------------------
def _iter_all_values(obj: Any) -> Iterable[Any]:
    if isinstance(obj, dict):
        for v in obj.values():
            yield v
            yield from _iter_all_values(v)
    elif isinstance(obj, list):
        for v in obj:
            yield v
            yield from _iter_all_values(v)
    else:
        for attr in ("model_dump", "dict"):
            try:
                if hasattr(obj, attr):
                    res = getattr(obj, attr)()
                    yield res
                    yield from _iter_all_values(res)
                    return
            except Exception:
                pass
        for name in ("content", "output", "parsed", "text"):
            try:
                v = getattr(obj, name, None)
                if v is not None:
                    yield v
                    yield from _iter_all_values(v)
            except Exception:
                pass

def _deep_find_prediction_obj(obj: Any) -> Optional[Dict[str, Any]]:
    if isinstance(obj, dict):
        if "fixture_id" in obj and isinstance(obj.get("predictions"), dict) and "over_2_5" in obj["predictions"]:
            return obj
    try:
        for v in _iter_all_values(obj):
            if isinstance(v, dict) and "fixture_id" in v:
                preds = v.get("predictions")
                if isinstance(preds, dict) and "over_2_5" in preds:
                    return v
    except Exception:
        pass
    return None

def _extract_parsed_from_responses(resp) -> Optional[Dict[str, Any]]:
    parsed = getattr(resp, "output_parsed", None)
    if isinstance(parsed, dict):
        return parsed

    output = getattr(resp, "output", None)
    if output:
        for item in output:
            content = item.get("content", []) if isinstance(item, dict) else getattr(item, "content", []) or []
            for chunk in content:
                maybe = chunk.get("parsed", None) if isinstance(chunk, dict) else getattr(chunk, "parsed", None)
                if isinstance(maybe, dict):
                    return maybe

    try:
        dumped = resp.model_dump() if hasattr(resp, "model_dump") else json.loads(
            resp.model_dump_json() if hasattr(resp, "model_dump_json")
            else json.dumps(resp, default=lambda o: getattr(o, "__dict__", str(o)))
        )
        found = _deep_find_prediction_obj(dumped)
        if found:
            return found
    except Exception:
        pass
    return None

def _parse_responses_text(resp) -> str:
    txt = getattr(resp, "output_text", None)
    if isinstance(txt, str) and txt.strip():
        return txt

    output = getattr(resp, "output", None)
    parts: list[str] = []
    if output:
        for item in output:
            content = item.get("content", []) if isinstance(item, dict) else getattr(item, "content", []) or []
            for chunk in content:
                t = chunk.get("type") if isinstance(chunk, dict) else getattr(chunk, "type", None)
                s = chunk.get("text", "") if isinstance(chunk, dict) else getattr(chunk, "text", "")
                if t in ("output_text", "summary_text") and isinstance(s, str):
                    parts.append(s)
    return "".join(parts)

def _extract_json_snippet(s: str) -> str:
    if not isinstance(s, str) or not s:
        return ""
    a = s.find("{"); b = s.rfind("}")
    return s[a:b+1] if (a != -1 and b != -1 and b > a) else s.strip()

# ------------------------------------------------------------------------------
# Local engine (deterministic OU 2.5 from odds & light signals)
# ------------------------------------------------------------------------------
def _safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def _p_from_odds(odds: Optional[float]) -> Optional[float]:
    o = _safe_float(odds)
    if not o or o <= 1.0:
        return None
    return max(1.0 / o, 1e-6)

def _infer_margin_from_1x2(one_x_two: Optional[Dict[str, Any]]) -> float:
    if not isinstance(one_x_two, dict):
        return DEFAULT_VIG
    invs = []
    for k in ("Home", "Draw", "Away"):
        v = _safe_float(one_x_two.get(k))
        if v and v > 1.0:
            invs.append(1.0 / v)
    if len(invs) >= 2:
        margin = max(sum(invs) - 1.0, 0.0)
        return float(min(max(margin, 0.0), 0.12)) or DEFAULT_VIG
    return DEFAULT_VIG

def _local_over25(payload: Dict[str, Any]) -> Dict[str, Any]:
    odds = payload.get("odds") or {}
    over_price = _safe_float(odds.get("over_2_5"))
    if not over_price:
        # Minimal safe result when feed lacks a price
        over_price = MIN_ODDS

    p_mkt = _p_from_odds(over_price) or 0.5
    margin = _infer_margin_from_1x2(odds.get("one_x_two"))
    p_fair = max(min(p_mkt - 0.5 * margin, 0.975), 0.025)

    # Tilts
    btts_yes = _safe_float((odds.get("btts") or {}).get("Yes"))
    p_btts = _p_from_odds(btts_yes) if btts_yes else None
    tilt_btts = ((p_btts - 0.5) * 0.25) if (p_btts is not None) else 0.0

    one_x_two = odds.get("one_x_two") if isinstance(odds.get("one_x_two"), dict) else {}
    fav_price = min([v for v in (_safe_float(one_x_two.get("Home")),
                                 _safe_float(one_x_two.get("Away"))) if v], default=None)
    tilt_fav = 0.0
    if fav_price:
        if fav_price <= 1.55: tilt_fav += 0.04
        elif fav_price <= 1.75: tilt_fav += 0.02
        elif fav_price >= 2.60: tilt_fav -= 0.02

    p_over = max(min(p_fair + tilt_btts + tilt_fav, 0.975), 0.025)
    ev = p_over * over_price - 1.0

    # Value rule (optionally "always value" if EV >= 0 and env says so)
    po_value = (ev >= EDGE_THRESHOLD and MIN_ODDS <= over_price <= MAX_ODDS) or (ALWAYS_VALUE and ev >= 0.0)

    # Confidence & stake
    diff = abs(p_over - p_mkt)
    confidence = int(max(35.0, min(90.0, 55.0 + 300.0 * diff)))

    stake_pct = 0.0
    if po_value:
        stake_pct = min(MAX_STAKE, max(0.25, EDGE_TO_STAKE_COEF * (ev * 100.0)))

    rationale = [
        f"Base p_over≈{p_mkt:.3f}, fair adj≈{p_fair:.3f}",
        f"BTTS tilt≈{tilt_btts:+.3f}" if p_btts is not None else "BTTS tilt≈0.000",
        f"Fav tilt≈{tilt_fav:+.3f}",
        f"Final p_over≈{p_over:.3f}, EV≈{ev:.3f}"
    ]

    return {
        "fixture_id": payload.get("fixture_id", 0),
        "predictions": {
            "over_2_5": {
                "prediction": "Over",              # using Over price
                "confidence": confidence,
                "edge": round(ev, 4),
                "po_value": bool(po_value),
                "odds": over_price,
                "bankroll_pct": round(stake_pct, 3),
                "rationale": "; ".join(rationale)
            }
        }
    }

# ------------------------------------------------------------------------------
# Model call (Responses first; quick exit; optional Chat fallback)
# ------------------------------------------------------------------------------
def _call_model(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not USE_LLM:
        return None

    has_text = _responses_supports_param("text")
    has_resp_format = _responses_supports_param("response_format")
    has_instr = _responses_supports_param("instructions")
    tokens_kw = _responses_tokens_kw()

    user_blob = "Match data JSON follows.\n" + json.dumps(payload, ensure_ascii=False)

    # 1) Responses: instructions + text.format (most reliable when supported)
    if has_text and has_instr:
        try:
            resp = client.responses.create(
                model=MODEL_NAME,
                instructions=SYSTEM_INSTRUCTIONS,
                input=[{"role": "user", "content": [{"type": "input_text", "text": user_blob}]}],
                text={"format": {"type": "json_schema", "name": SCHEMA_NAME,
                                 "schema": PREDICTION_JSON_SCHEMA, "strict": STRICT_OUTPUT}},
                **{tokens_kw: 600},
            )
            parsed = _extract_parsed_from_responses(resp)
            if isinstance(parsed, dict):
                return parsed
            text_out = _parse_responses_text(resp)
            if text_out.strip():
                return json.loads(_extract_json_snippet(text_out))
            logger.warning("Responses (instructions+text.format) returned empty output.")
        except Exception as e:
            logger.debug("Responses (instructions+text.format) err: %s", e)

    # 2) Responses: messages + text.format
    if has_text:
        try:
            resp = client.responses.create(
                model=MODEL_NAME,
                input=[
                    {"role": "system", "content": [{"type": "input_text", "text": SYSTEM_INSTRUCTIONS}]},
                    {"role": "user",   "content": [{"type": "input_text", "text": user_blob}]},
                ],
                text={"format": {"type": "json_schema", "name": SCHEMA_NAME,
                                 "schema": PREDICTION_JSON_SCHEMA, "strict": STRICT_OUTPUT}},
                **{tokens_kw: 600},
            )
            parsed = _extract_parsed_from_responses(resp)
            if isinstance(parsed, dict):
                return parsed
            text_out = _parse_responses_text(resp)
            if text_out.strip():
                return json.loads(_extract_json_snippet(text_out))
            logger.warning("Responses (messages+text.format) returned empty output.")
        except Exception as e:
            logger.debug("Responses (messages+text.format) err: %s", e)

    # 3) Responses: legacy response_format
    if has_resp_format:
        try:
            resp = client.responses.create(
                model=MODEL_NAME,
                input=[
                    {"role": "system", "content": [{"type": "input_text", "text": SYSTEM_INSTRUCTIONS}]},
                    {"role": "user",   "content": [{"type": "input_text", "text": user_blob}]},
                ],
                response_format={"type": "json_schema",
                                 "json_schema": {"name": SCHEMA_NAME,
                                                 "schema": PREDICTION_JSON_SCHEMA,
                                                 "strict": STRICT_OUTPUT}},
                **{tokens_kw: 600},
            )
            parsed = _extract_parsed_from_responses(resp)
            if isinstance(parsed, dict):
                return parsed
            text_out = _parse_responses_text(resp)
            if text_out.strip():
                return json.loads(_extract_json_snippet(text_out))
            logger.warning("Responses (response_format) returned empty output.")
        except Exception as e:
            logger.debug("Responses (response_format) err: %s", e)

    # 4) Responses: no schema/format — last attempt to coax any JSON
    try:
        raw_sys = SYSTEM_INSTRUCTIONS + " IMPORTANT: Reply ONLY with valid JSON for the required keys."
        resp = client.responses.create(
            model=MODEL_NAME,
            input=[
                {"role": "system", "content": [{"type": "input_text", "text": raw_sys}]},
                {"role": "user",   "content": [{"type": "input_text", "text": user_blob}]},
            ],
            **{tokens_kw: 500},
        )
        text_out = _parse_responses_text(resp)
        if text_out.strip():
            return json.loads(_extract_json_snippet(text_out))
        logger.warning("Responses (no-format) returned empty output.")
    except Exception as e:
        logger.debug("Responses (no-format) err: %s", e)

    # Optional: Chat fallback if explicitly allowed
    if ALLOW_CHAT:
        try:
            cc = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": SYSTEM_INSTRUCTIONS},
                    {"role": "user", "content": user_blob},
                ],
                response_format={"type": "json_schema",
                                 "json_schema": {"name": SCHEMA_NAME,
                                                 "schema": PREDICTION_JSON_SCHEMA,
                                                 "strict": STRICT_OUTPUT}},
                **_chat_tokens_kwargs(500),
            )
            # Prefer parsed if SDK provides it
            choices = getattr(cc, "choices", []) or []
            for ch in choices:
                msg = getattr(ch, "message", None)
                if msg is None:
                    continue
                parsed = getattr(msg, "parsed", None)
                if isinstance(parsed, dict):
                    return parsed
                content = getattr(msg, "content", None)
                if isinstance(content, str) and content.strip():
                    return json.loads(_extract_json_snippet(content))
            logger.warning("Chat with response_format returned empty output.")
        except Exception as e:
            logger.debug("Chat err: %s", e)

    # Give up → caller will use local engine immediately
    return None

# ------------------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------------------
def get_prediction(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Entry point used by main.py.
    Returns an object with ONLY the over_2_5 market.
    """
    try:
        # Robust fixture_id
        fixture_id = match_data.get("fixture_id")
        try:
            fixture_id = int(fixture_id)
        except Exception:
            fixture_id = 0

        payload = {
            "fixture_id": fixture_id,
            "teams": match_data.get("teams"),
            "odds": match_data.get("odds"),
            "form": match_data.get("form"),
            "injuries": match_data.get("injuries"),
            "head_to_head": match_data.get("head_to_head"),
            "league": match_data.get("league"),
            "venue": match_data.get("venue"),
            "constraints": {"min_odds": MIN_ODDS, "max_odds": MAX_ODDS},
        }

        # 1) Try model (fast, few attempts)
        result = _call_model(payload)

        # 2) If model returned nothing/invalid, compute locally (deterministic)
        if not isinstance(result, dict):
            result = _local_over25(payload)

        # Ensure shape & validity
        if not isinstance(result.get("fixture_id"), int):
            result["fixture_id"] = fixture_id
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
# Smoke test
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    dummy = {
        "fixture_id": 123456,
        "teams": {"home": "A FC", "away": "B United"},
        "odds": {
            "one_x_two": {"Home": 1.72, "Draw": 3.8, "Away": 4.5},
            "btts": {"Yes": 1.80, "No": 2.00},
            "over_2_5": 1.72
        },
        "form": {"home": {}, "away": {}},
        "injuries": {"home": [], "away": []},
        "head_to_head": [],
        "league": {"name": "Test League", "country": "TL", "round": "R1"},
        "venue": "Test Stadium"
    }
    out = get_prediction(dummy)
    print(json.dumps(out or {"error": "no output"}, ensure_ascii=False, indent=2))
