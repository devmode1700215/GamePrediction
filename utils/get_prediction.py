# utils/get_prediction.py
# -*- coding: utf-8 -*-

import os
import json
import time
import math
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
# Default to gpt-5-nano
MODEL_NAME = os.getenv("MODEL_NAME", "gpt-5-nano")
MAX_RETRIES = int(os.getenv("OPENAI_MAX_RETRIES", "3"))
RETRY_BACKOFF_SEC = float(os.getenv("OPENAI_RETRY_BACKOFF_SEC", "2.0"))

# Odds guardrails (we only warn if outside)
MIN_ODDS = float(os.getenv("PREDICTION_MIN_ODDS", "1.6"))
MAX_ODDS = float(os.getenv("PREDICTION_MAX_ODDS", "2.3"))

# Local engine tuning
EDGE_THRESHOLD = float(os.getenv("OU25_EDGE_THRESHOLD", "0.02"))     # >=2% EV -> po_value true
MAX_STAKE = float(os.getenv("OU25_MAX_STAKE_PCT", "1.5"))            # cap stake% (kelly-lite)
EDGE_TO_STAKE_COEF = float(os.getenv("OU25_EDGE_TO_STAKE_COEF", "0.20"))
DEFAULT_VIG = float(os.getenv("OU25_DEFAULT_VIG", "0.05"))           # assumed market overround

# Flip to 0 to skip LLM entirely
USE_LLM = os.getenv("OU25_USE_LLM", "1") == "1"

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
STRICT_OUTPUT = True

SYSTEM_INSTRUCTIONS = (
    "You are an expert football betting analyst. "
    "Given structured match data (teams, form, injuries, H2H, league, venue, odds), "
    "evaluate ONLY the Over/Under 2.5 goals market. "
    f"Use odds guardrails of {MIN_ODDS}–{MAX_ODDS}. "
    "Calibrate probabilities, compute edge vs. listed odds, set po_value true only for positive EV, "
    "and keep bankroll_pct modest (0.25–1.5) unless edge is exceptional. "
    "Return ONLY JSON conforming to the provided schema."
)

# ------------------------------------------------------------------------------
# Validation helpers
# ------------------------------------------------------------------------------
def _validate_prediction_block(block: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    required = ["prediction", "confidence", "edge", "po_value", "odds", "bankroll_pct", "rationale"]
    missing = [k for k in required if k not in block]
    if missing:
        return False, f"over_2_5: missing {missing}"
    try:
        if not (MIN_ODDS <= float(block["odds"]) <= MAX_ODDS):
            logger.info("ℹ️ over_2_5 odds out of preferred range: %.3f (allowed %.2f–%.2f)",
                        float(block["odds"]), MIN_ODDS, MAX_ODDS)
    except Exception:
        logger.info("ℹ️ over_2_5 has non-numeric odds: %r", block.get("odds"))
    return True, None

def _validate_full_response(data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    if "fixture_id" not in data or "predictions" not in data:
        return False, "Missing top-level fields"
    preds = data["predictions"]
    if "over_2_5" not in preds:
        return False, "Missing market over_2_5"
    ok, err = _validate_prediction_block(preds["over_2_5"])
    if not ok:
        return False, err
    return True, None

# ------------------------------------------------------------------------------
# SDK capability introspection
# ------------------------------------------------------------------------------
def _supports_arg(fn, name: str) -> bool:
    try:
        return name in inspect.signature(fn).parameters
    except Exception:
        return False

def _responses_tokens_kw() -> str:
    # Newer: max_output_tokens; older: max_tokens
    return "max_output_tokens" if _supports_arg(client.responses.create, "max_output_tokens") else "max_tokens"

def _responses_supports_param(name: str) -> bool:
    try:
        return name in inspect.signature(client.responses.create).parameters
    except Exception:
        return False

def _chat_tokens_kwargs(n: int = 700) -> dict:
    # Newer Chat: max_completion_tokens; older: max_tokens
    try:
        if "max_completion_tokens" in inspect.signature(client.chat.completions.create).parameters:
            return {"max_completion_tokens": n}
    except Exception:
        pass
    return {"max_tokens": n}

# ------------------------------------------------------------------------------
# Generic traversal/extraction
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
            if isinstance(v, dict):
                if "fixture_id" in v and isinstance(v.get("predictions"), dict) and "over_2_5" in v["predictions"]:
                    return v
    except Exception:
        pass
    return None

def _extract_parsed_from_responses(resp) -> Optional[Dict[str, Any]]:
    parsed = getattr(resp, "output_parsed", None)
    if parsed is not None:
        return parsed

    output_list = getattr(resp, "output", None)
    if output_list:
        for item in output_list:
            content = item.get("content", []) if isinstance(item, dict) else getattr(item, "content", []) or []
            for chunk in content:
                maybe = chunk.get("parsed", None) if isinstance(chunk, dict) else getattr(chunk, "parsed", None)
                if isinstance(maybe, dict):
                    return maybe
                if isinstance(maybe, list):
                    for x in maybe:
                        if isinstance(x, dict) and "fixture_id" in x and "predictions" in x:
                            return x

    try:
        dumped = resp.model_dump() if hasattr(resp, "model_dump") else json.loads(
            resp.model_dump_json() if hasattr(resp, "model_dump_json") else json.dumps(resp, default=lambda o: getattr(o, "__dict__", str(o)))
        )
    except Exception:
        dumped = None

    if dumped is not None:
        found = _deep_find_prediction_obj(dumped)
        if found:
            return found
    return None

def _parse_responses_text(resp) -> str:
    txt = getattr(resp, "output_text", None)
    if isinstance(txt, str) and txt.strip():
        return txt

    parts = []
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
                parts.append(ctext)
    return "".join(parts)

def _extract_json_snippet(s: str) -> str:
    if not isinstance(s, str) or not s:
        return ""
    start = s.find("{"); end = s.rfind("}")
    return s[start:end+1] if (start != -1 and end != -1 and end > start) else s.strip()

def _extract_from_chat(cc) -> Optional[Dict[str, Any]]:
    try:
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
                try:
                    return json.loads(_extract_json_snippet(content))
                except Exception:
                    continue
    except Exception:
        pass
    return None

# ------------------------------------------------------------------------------
# Backstop + Local OU2.5 engine
# ------------------------------------------------------------------------------
def _default_stub_over25(payload: Dict[str, Any], note: str = "Stub filled due to missing market; no value.") -> Dict[str, Any]:
    def _num(x):
        try:
            return float(x)
        except Exception:
            return None
    odds = _num((payload.get("odds") or {}).get("over_2_5")) or MIN_ODDS
    return {
        "prediction": "Over",
        "confidence": 0,
        "edge": 0,
        "po_value": False,
        "odds": odds,
        "bankroll_pct": 0,
        "rationale": note
    }

def _apply_backstop_if_missing(payload: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(data)
    preds = dict(data.get("predictions") or {})
    if "over_2_5" not in preds:
        logger.info("ℹ️ Backstop fill for missing market: over_2_5")
        preds["over_2_5"] = _default_stub_over25(payload)
    data["predictions"] = preds
    return data

def _safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def _market_prob_from_odds(odds: Optional[float]) -> Optional[float]:
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

def _local_over25_estimate(payload: Dict[str, Any]) -> Dict[str, Any]:
    odds_blob = payload.get("odds") or {}
    over_price = _safe_float(odds_blob.get("over_2_5"), default=None)
    if not over_price:
        return _default_stub_over25(payload, note="No Over 2.5 price available; produced safe stub.")

    p_mkt_over = _market_prob_from_odds(over_price) or 0.5
    margin_guess = _infer_margin_from_1x2(odds_blob.get("one_x_two"))
    p_fair_over = max(min(p_mkt_over - margin_guess * 0.5, 0.975), 0.025)

    btts_yes = None
    btts_blob = odds_blob.get("btts")
    if isinstance(btts_blob, dict):
        btts_yes = _safe_float(btts_blob.get("Yes"))
    p_btts = _market_prob_from_odds(btts_yes) if btts_yes else None
    tilt_btts = ((p_btts - 0.5) * 0.25) if (p_btts is not None) else 0.0

    one_x_two = odds_blob.get("one_x_two") if isinstance(odds_blob.get("one_x_two"), dict) else {}
    fav_price = min([v for v in (_safe_float(one_x_two.get("Home")),
                                 _safe_float(one_x_two.get("Away"))) if v], default=None)
    tilt_fav = 0.0
    if fav_price:
        if fav_price <= 1.55:
            tilt_fav += 0.04
        elif fav_price <= 1.75:
            tilt_fav += 0.02
        elif fav_price >= 2.60:
            tilt_fav -= 0.02

    p_over = max(min(p_fair_over + tilt_btts + tilt_fav, 0.975), 0.025)
    ev = p_over * over_price - 1.0
    po_value = bool(ev >= EDGE_THRESHOLD and MIN_ODDS <= over_price <= MAX_ODDS)

    diff_from_mkt = abs(p_over - p_mkt_over)
    confidence = int(max(35.0, min(90.0, 55.0 + 300.0 * diff_from_mkt)))

    stake_pct = 0.0
    if po_value:
        stake_pct = min(MAX_STAKE, max(0.25, EDGE_TO_STAKE_COEF * (ev * 100.0)))

    rationale_bits = []
    rationale_bits.append(f"Base p_over from price≈{p_mkt_over:.3f}, fair adj≈{p_fair_over:.3f}")
    if p_btts is not None:
        rationale_bits.append(f"BTTS tilt≈{tilt_btts:+.3f} (BTTS Yes p≈{p_btts:.2f})")
    if fav_price:
        rationale_bits.append(f"Fav tilt≈{tilt_fav:+.3f} (fav price {fav_price:.2f})")
    rationale_bits.append(f"Final p_over≈{p_over:.3f}, EV≈{ev:.3f}")

    return {
        "fixture_id": payload.get("fixture_id", 0),
        "predictions": {
            "over_2_5": {
                "prediction": "Over",
                "confidence": confidence,
                "edge": round(ev, 4),
                "po_value": po_value,
                "odds": over_price,
                "bankroll_pct": round(stake_pct, 3),
                "rationale": "; ".join(rationale_bits)
            }
        }
    }

# ------------------------------------------------------------------------------
# MODEL CALL (Responses first; Chat as last resort) – works with gpt-5-nano
# ------------------------------------------------------------------------------
def _call_model(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Try Responses API once using the most compatible shapes.
    If we still get no usable output, return None so the caller
    uses the local OU2.5 engine immediately (no Chat fallback).
    """
    if not USE_LLM:
        return None

    has_text_format = _responses_supports_param("text")
    has_resp_format = _responses_supports_param("response_format")
    has_instructions = _responses_supports_param("instructions")
    tokens_kw = _responses_tokens_kw()

    instructions_str = SYSTEM_INSTRUCTIONS
    user_blob = "Match data JSON follows.\n" + json.dumps(payload, ensure_ascii=False)

    msg_system = {"role": "system", "content": [{"type": "input_text", "text": SYSTEM_INSTRUCTIONS}]}
    msg_user   = {"role": "user",   "content": [{"type": "input_text", "text": user_blob}]}

    # ---- 1) Responses + instructions + text.format (preferred) ----
    if has_text_format and has_instructions:
        try:
            resp = client.responses.create(
                model=MODEL_NAME,
                instructions=instructions_str,
                input=[{"role": "user", "content": [{"type": "input_text", "text": user_blob}]}],
                text={"format": {"type": "json_schema", "name": SCHEMA_NAME, "schema": PREDICTION_JSON_SCHEMA, "strict": STRICT_OUTPUT}},
                **{tokens_kw: 600},
            )
            parsed = _extract_parsed_from_responses(resp)
            if parsed is not None:
                return parsed
            text_out = _parse_responses_text(resp)
            if text_out.strip():
                return json.loads(text_out)
            logger.warning("Responses (instructions+text.format) returned empty output.")
        except Exception as e:
            logger.debug("Responses (instructions+text.format) err: %s", e)

    # ---- 2) Responses + messages + text.format (second best) ----
    if has_text_format:
        try:
            resp = client.responses.create(
                model=MODEL_NAME,
                input=[msg_system, msg_user],
                text={"format": {"type": "json_schema", "name": SCHEMA_NAME, "schema": PREDICTION_JSON_SCHEMA, "strict": STRICT_OUTPUT}},
                **{tokens_kw: 600},
            )
            parsed = _extract_parsed_from_responses(resp)
            if parsed is not None:
                return parsed
            text_out = _parse_responses_text(resp)
            if text_out.strip():
                return json.loads(text_out)
            logger.warning("Responses (messages+text.format) returned empty output.")
        except Exception as e:
            logger.debug("Responses (messages+text.format) err: %s", e)

    # ---- 3) Responses + legacy response_format (last try) ----
    if has_resp_format:
        try:
            resp = client.responses.create(
                model=MODEL_NAME,
                input=[msg_system, msg_user],
                response_format={"type": "json_schema", "json_schema": {"name": SCHEMA_NAME, "schema": PREDICTION_JSON_SCHEMA, "strict": STRICT_OUTPUT}},
                **{tokens_kw: 600},
            )
            parsed = _extract_parsed_from_responses(resp)
            if parsed is not None:
                return parsed
            text_out = _parse_responses_text(resp)
            if text_out.strip():
                return json.loads(text_out)
            logger.warning("Responses (response_format) returned empty output.")
        except Exception as e:
            logger.debug("Responses (response_format) err: %s", e)

    # Nothing useful — let caller use the local engine
    return None


# ------------------------------------------------------------------------------
# Public API (ONLY Over/Under 2.5)
# ------------------------------------------------------------------------------
def get_prediction(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Entry point used by main.py.
    Returns an object with ONLY the over_2_5 market.
    """
    try:
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

        # 1) Try model (gpt-5-nano)
        model_result = _call_model(payload)

        # 2) If model silent/invalid, use local engine (deterministic)
        result = model_result if isinstance(model_result, dict) else _local_over25_estimate(payload)

        # Ensure correct fixture_id & market
        if not isinstance(result.get("fixture_id"), int):
            result["fixture_id"] = fixture_id
        result = _apply_backstop_if_missing(payload, result)

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
    out = get_prediction(dummy_match)
    print(json.dumps(out or {"error": "no output"}, ensure_ascii=False, indent=2))
