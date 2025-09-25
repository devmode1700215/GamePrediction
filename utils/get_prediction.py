# utils/get_prediction.py
# -*- coding: utf-8 -*-

import os
import json
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# ------------------------------------------------------------------------------
# Config (env-driven)
# ------------------------------------------------------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("Missing OPENAI_API_KEY (or OPENAI_KEY)")

# Primary model first (safe default), then fallback model if 400/empty.
MODEL_NAME = os.getenv("MODEL_NAME", "gpt-4o-mini")
MODEL_FALLBACK = os.getenv("MODEL_FALLBACK", "gpt-5-nano")

# Local engine tuning (only used when the model returns nothing)
PREDICTION_MIN_ODDS = float(os.getenv("PREDICTION_MIN_ODDS", "1.6"))   # informational guardrail
PREDICTION_MAX_ODDS = float(os.getenv("PREDICTION_MAX_ODDS", "2.3"))
OU25_EDGE_THRESHOLD = float(os.getenv("OU25_EDGE_THRESHOLD", "0.02"))  # >=2% EV → value
OU25_MAX_STAKE_PCT = float(os.getenv("OU25_MAX_STAKE_PCT", "1.5"))
OU25_EDGE_TO_STAKE_COEF = float(os.getenv("OU25_EDGE_TO_STAKE_COEF", "0.20"))
OU25_DEFAULT_VIG = float(os.getenv("OU25_DEFAULT_VIG", "0.05"))
OU25_ALWAYS_VALUE = os.getenv("OU25_ALWAYS_VALUE", "0") == "1"         # if True: EV>=0 counts as value

client = OpenAI(api_key=OPENAI_API_KEY)

# Load your existing prompt (same as the version that used to work)
PROMPT_PATH = (Path(__file__).resolve().parents[1] / "prompt.txt")
if PROMPT_PATH.exists():
    PROMPT = PROMPT_PATH.read_text(encoding="utf-8")
else:
    # Minimal system prompt if prompt.txt is missing
    PROMPT = (
        "You are an expert football betting analyst. "
        "Given JSON match data, return ONLY JSON with a 'predictions' object. "
        "If multiple markets are discussed, still include 'over_2_5' with "
        "prediction, confidence (0-100), edge, po_value, odds, bankroll_pct, rationale."
    )

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _extract_json_snippet(s: str) -> str:
    if not isinstance(s, str):
        return ""
    a = s.find("{")
    b = s.rfind("}")
    return s[a:b+1] if (a != -1 and b != -1 and b > a) else s.strip()

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
        return OU25_DEFAULT_VIG
    invs = []
    for k in ("Home", "Draw", "Away"):
        v = _safe_float(one_x_two.get(k))
        if v and v > 1.0:
            invs.append(1.0 / v)
    if len(invs) >= 2:
        margin = max(sum(invs) - 1.0, 0.0)
        # cap between 0% and 12% to avoid silly inputs
        return float(min(max(margin, 0.0), 0.12)) or OU25_DEFAULT_VIG
    return OU25_DEFAULT_VIG

def _local_over25(match_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deterministic OU2.5 prediction from prices + light tilts.
    Used if the model returns nothing or errors.
    """
    fx = match_data.get("fixture_id") or 0
    odds_blob = match_data.get("odds") or {}

    over_price = _safe_float(odds_blob.get("over_2_5"))
    if not over_price or over_price <= 1.0:
        over_price = max(PREDICTION_MIN_ODDS, 1.60)  # safe default

    p_mkt = _p_from_odds(over_price) or 0.5
    margin = _infer_margin_from_1x2(odds_blob.get("one_x_two"))
    p_fair = max(min(p_mkt - 0.5 * margin, 0.975), 0.025)

    # Tilts from BTTS and 1X2 fave
    btts_yes = _safe_float((odds_blob.get("btts") or {}).get("Yes"))
    p_btts = _p_from_odds(btts_yes) if btts_yes else None
    tilt_btts = ((p_btts - 0.5) * 0.25) if (p_btts is not None) else 0.0

    one_x_two = odds_blob.get("one_x_two") if isinstance(odds_blob.get("one_x_two"), dict) else {}
    fav_price = min([v for v in (_safe_float(one_x_two.get("Home")),
                                 _safe_float(one_x_two.get("Away"))) if v], default=None)
    tilt_fav = 0.0
    if fav_price:
        if fav_price <= 1.55: tilt_fav += 0.04
        elif fav_price <= 1.75: tilt_fav += 0.02
        elif fav_price >= 2.60: tilt_fav -= 0.02

    p_over = max(min(p_fair + tilt_btts + tilt_fav, 0.975), 0.025)
    ev = p_over * over_price - 1.0

    # Value rule
    in_range = (PREDICTION_MIN_ODDS <= over_price <= PREDICTION_MAX_ODDS)
    po_value = (ev >= OU25_EDGE_THRESHOLD and in_range) or (OU25_ALWAYS_VALUE and ev >= 0.0)

    # Confidence & stake
    diff = abs(p_over - p_mkt)
    confidence = int(max(35.0, min(90.0, 55.0 + 300.0 * diff)))  # 0.1 diff => +30

    stake_pct = 0.0
    if po_value:
        stake_pct = min(OU25_MAX_STAKE_PCT, max(0.25, OU25_EDGE_TO_STAKE_COEF * (ev * 100.0)))

    rationale = [
        f"Base p_over≈{p_mkt:.3f}, fair adj≈{p_fair:.3f}",
        f"BTTS tilt≈{tilt_btts:+.3f}" if p_btts is not None else "BTTS tilt≈0.000",
        f"Fav tilt≈{tilt_fav:+.3f}",
        f"Final p_over≈{p_over:.3f}, EV≈{ev:.3f}",
        f"Odds range check: {over_price:.2f} in [{PREDICTION_MIN_ODDS:.2f},{PREDICTION_MAX_ODDS:.2f}]"
    ]

    return {
        "fixture_id": int(fx) if str(fx).isdigit() else 0,
        "predictions": {
            "over_2_5": {
                "prediction": "Over",
                "confidence": confidence,
                "edge": round(ev, 4),
                "po_value": bool(po_value),
                "odds": over_price,
                "bankroll_pct": round(stake_pct, 3),
                "rationale": "; ".join(rationale)
            }
        }
    }

def _chat_tokens_kwargs(n: int = 700) -> dict:
    """
    Newer SDKs use max_completion_tokens; older use max_tokens.
    We’ll pass only the one that exists to avoid 400s.
    """
    try:
        sig = getattr(client.chat.completions, "create").__func__.__code__.co_varnames  # type: ignore[attr-defined]
        if "max_completion_tokens" in sig:
            return {"max_completion_tokens": n}
    except Exception:
        pass
    return {"max_tokens": n}

def _call_chat_once(model: str, system_prompt: str, match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Single Chat Completions attempt, no temperature, no response_format.
    Returns parsed JSON or None on failure/empty.
    """
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(match_data, ensure_ascii=False)},
            ],
            **_chat_tokens_kwargs(800),
        )
        content = resp.choices[0].message.content or ""
        snippet = _extract_json_snippet(content)
        if not snippet:
            return None
        data = json.loads(snippet)

        # Keep ONLY over_2_5 if present
        if isinstance(data, dict) and isinstance(data.get("predictions"), dict):
            if "over_2_5" in data["predictions"]:
                data["predictions"] = {"over_2_5": data["predictions"]["over_2_5"]}
        return data
    except Exception:
        return None

# ------------------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------------------
def get_prediction(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Returns a dict with ONLY the over_2_5 market.
    Flow:
      1) Chat with MODEL_NAME (no schema), parse JSON.
      2) If empty/400/etc., chat with MODEL_FALLBACK.
      3) If still nothing, produce local OU2.5 prediction.
    """
    # Try primary, then fallback
    data = _call_chat_once(MODEL_NAME, PROMPT, match_data)
    if not isinstance(data, dict):
        data = _call_chat_once(MODEL_FALLBACK, PROMPT, match_data)

    # Last resort: deterministic local output
    if not isinstance(data, dict):
        data = _local_over25(match_data)

    # Ensure minimal shape
    if not isinstance(data.get("fixture_id"), int):
        try:
            data["fixture_id"] = int(match_data.get("fixture_id") or 0)
        except Exception:
            data["fixture_id"] = 0

    # Validate essential keys (light)
    preds = data.get("predictions") or {}
    ou = preds.get("over_2_5")
    required = ("prediction", "confidence", "edge", "po_value", "odds", "bankroll_pct", "rationale")
    if not isinstance(ou, dict) or any(k not in ou for k in required):
        # patch with local estimate if the model missed fields
        local = _local_over25(match_data)
        data["predictions"]["over_2_5"] = local["predictions"]["over_2_5"]

    return data

# ------------------------------------------------------------------------------
# Smoke test
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    dummy = {
        "fixture_id": 123456,
        "teams": {"home": "A FC", "away": "B United"},
        "odds": {
            "one_x_two": {"Home": 1.80, "Draw": 3.80, "Away": 4.50},
            "btts": {"Yes": 1.85, "No": 1.95},
            "over_2_5": 1.72
        },
        "form": {"home": {}, "away": {}},
        "injuries": {"home": [], "away": []},
        "head_to_head": [],
        "league": {"name": "Test League", "country": "TL", "round": "R1"},
        "venue": "Test Stadium"
    }
    print(json.dumps(get_prediction(dummy), ensure_ascii=False, indent=2))
