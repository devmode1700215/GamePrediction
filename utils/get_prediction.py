# utils/get_prediction.py
import os, json, math, statistics
from typing import Any, Dict, List, Tuple
from openai import OpenAI

MODEL_NAME = os.getenv("MODEL_NAME", "gpt-4o")
OPENAI_KEY = os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_KEY")
PRED_SAMPLES = max(1, int(os.getenv("PRED_SAMPLES", "2")))      # 1–3 is plenty
PRED_TEMP = float(os.getenv("PRED_TEMP", "0.2"))                 # low temperature for stability
EDGE_MIN = float(os.getenv("EDGE_MIN", "0.05"))                  # 5% by default (you asked for this)
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.25"))      # 25% Kelly

_client = None
def _client_lazy() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI(api_key=OPENAI_KEY)
    return _client

def _msg_system() -> Dict[str, str]:
    return {
        "role": "system",
        "content": (
            "You are a disciplined football total-goals forecaster. "
            "Given structured match context and a market price for Over 2.5, "
            "estimate calibrated probabilities. Respond with ONLY compact JSON. "
            "No explanations. Numbers only."
        ),
    }

def _msg_user(payload: Dict[str, Any]) -> Dict[str, str]:
    return {
        "role": "user",
        "content": json.dumps({
            "task": "over_under_2_5",
            "match": {
                "fixture_id": payload.get("fixture_id"),
                "date": payload.get("date"),
                "league": payload.get("league"),
                "venue": payload.get("venue"),
                "home_team": payload.get("home_team"),
                "away_team": payload.get("away_team"),
            },
            "head_to_head": payload.get("head_to_head"),
            "price": { "over_2_5": payload.get("odds", {}).get("over_2_5"), "source": payload.get("odds", {}).get("source") },
            "requirements": {
                "market": "over_2_5",
                "return_json_schema": {
                    "market": "over_2_5",
                    "p_over_2_5": "float in [0,1]",
                    "p_under_2_5": "float in [0,1] (≈ 1 - p_over_2_5)",
                    "prediction": "'Over' or 'Under'",
                    "confidence_pct": "0..100"
                }
            }
        }, ensure_ascii=False)
    }

def _one_call(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Single JSON-mode chat call. Returns parsed dict with required fields or raises."""
    resp = _client_lazy().chat.completions.create(
        model=MODEL_NAME,
        temperature=PRED_TEMP,
        response_format={"type": "json_object"},
        messages=[_msg_system(), _msg_user(payload)],
    )
    content = resp.choices[0].message.content or "{}"
    data = json.loads(content)
    # basic shape guard
    out = {
        "market": "over_2_5",
        "p_over_2_5": float(data.get("p_over_2_5", 0.5)),
        "p_under_2_5": float(data.get("p_under_2_5", max(0.0, 1.0 - float(data.get("p_over_2_5", 0.5))))),
        "prediction": data.get("prediction") or ("Over" if float(data.get("p_over_2_5", 0.5)) >= 0.5 else "Under"),
        "confidence_pct": float(data.get("confidence_pct", 70)),
    }
    # clamp
    out["p_over_2_5"] = max(0.0, min(1.0, out["p_over_2_5"]))
    out["p_under_2_5"] = max(0.0, min(1.0, out["p_under_2_5"]))
    return out

def _kelly(p: float, odds: float) -> float:
    """Full Kelly fraction for even-odds-style bet on 'Over'. b = odds-1."""
    b = max(0.0, odds - 1.0)
    if b <= 0 or p <= 0 or p >= 1:
        return 0.0
    q = 1.0 - p
    f = (b * p - q) / b
    return max(0.0, f)

def get_prediction(match_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns a normalized dict:
    {
      fixture_id, market, prediction, p_over_2_5, p_under_2_5,
      confidence_pct, edge, stake_pct, odds, po_value
    }
    Edge and stake are computed here for consistency.
    """
    price = match_payload.get("odds", {}).get("over_2_5")
    if price is None:
        # caller should prefilter odds; still guard
        return {}

    samples: List[Dict[str, Any]] = []
    for _ in range(PRED_SAMPLES):
        try:
            samples.append(_one_call(match_payload))
        except Exception:
            continue

    if not samples:
        return {}

    # aggregate by median for robustness
    p_over_list = [s["p_over_2_5"] for s in samples]
    conf_list = [s.get("confidence_pct", 70.0) for s in samples]
    p_over = statistics.median(p_over_list)
    conf = statistics.median(conf_list)
    span = max(p_over_list) - min(p_over_list)

    # OPTIONAL stability penalty on confidence (small)
    conf_eff = max(0.0, min(100.0, conf * (1.0 - min(0.5, span))))  # reduce up to 50% if unstable

    # Compute edge and stake
    edge = p_over * float(price) - 1.0
    k_full = _kelly(p_over, float(price))
    stake_pct = round(100.0 * KELLY_FRACTION * k_full, 2)

    # Final pick
    pick = "Over" if p_over >= 0.5 else "Under"
    po_value = (edge >= EDGE_MIN) and (pick == "Over")  # we only bet Over 2.5 in current pipeline

    return {
        "fixture_id": match_payload.get("fixture_id"),
        "market": "over_2_5",
        "prediction": pick,
        "p_over_2_5": round(p_over, 4),
        "p_under_2_5": round(1.0 - p_over, 4),
        "confidence_pct": round(conf_eff, 1),
        "edge": round(edge, 4),
        "stake_pct": stake_pct,
        "odds": float(price),
        "po_value": bool(po_value),
        # (optional) keep for debugging
        "samples": len(samples),
        "span": round(span, 4),
    }
