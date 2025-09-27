# utils/get_prediction.py
import os, json, statistics
from typing import Any, Dict, List
from openai import OpenAI

# Model & sampling
MODEL_NAME   = os.getenv("MODEL_NAME", "gpt-4o")
OPENAI_KEY   = os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_KEY")
PRED_SAMPLES = max(1, int(os.getenv("PRED_SAMPLES", "1")))  # default 1 to mirror past behavior
PRED_TEMP    = float(os.getenv("PRED_TEMP", "0.2"))

_client = None
def _client_lazy() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI(api_key=OPENAI_KEY)
    return _client

# === PROFITABLE PROMPT — with odds-range logic REMOVED ===
PROMPT = """You are a professional football betting analyst.

Output rules:
1. Return only valid JSON in the format below.
2. No extra text, no explanations, no markdown, no quotes outside JSON.
3. Internally validate your JSON before sending. If it’s invalid, regenerate silently until valid.
4. Do not include trailing commas or any other non-JSON elements.
5. Always follow the schema exactly.

Final output format:
{
  "fixture_id": [integer],
  "predictions": {
    "over_2_5": {
      "prediction": "Over" | "Under",
      "confidence": [number 0–100],
      "implied_odds_pct": [decimal],
      "edge": [decimal],
      "po_value": true | false,
      "odds": [decimal],
      "bankroll_pct": [decimal 0–10],
      "rationale": "[short reason, max 6 words]"
    }
  }
}

Task:
You will receive JSON data for one football match. Predict the Over/Under 2.5 Goals market using the rules below.

1. prediction: "Over" or "Under"
2. confidence: Weighted score from the table below
3. implied_odds_pct = (1 / odds) × 100
4. edge = confidence% − implied_odds_pct
5. po_value = true if edge > 0, else false
6. Use the provided odds as the market price; do not reject based on odds range.
7. bankroll_pct (Kelly stake %) = (((confidence_decimal × odds − 1) / (odds − 1)) / 8) × 100
8. Cap bankroll_pct at 5% (do not exceed)
9. rationale = one short reason (max 6 words)

Weighted signals:
Recent Form = 20%
League Position Gap = 15%
Expected Goals (xG) = 15%
Recent Goals For/Against = 10%
Head-to-Head Performance = 10%
Key Injuries/Suspensions = 20%
Odds Alignment = 10%

Ratings: Strong = 1.2 × weight, Medium = 1.0 × weight, Weak = 0.8 × weight.
Normalize total to 0–100%.
"""

def _msg_system() -> Dict[str, str]:
    return {"role": "system", "content": PROMPT}

def _msg_user(payload: Dict[str, Any]) -> Dict[str, str]:
    # Pass structured match pack; price contains the odds we actually use.
    pack = {
        "fixture_id": payload.get("fixture_id"),
        "match": {
            "date": payload.get("date"),
            "league": payload.get("league"),
            "venue": payload.get("venue"),
            "home_team": payload.get("home_team"),
            "away_team": payload.get("away_team"),
        },
        "head_to_head": payload.get("head_to_head"),
        "odds": {
            "over_2_5": payload.get("odds", {}).get("over_2_5"),
            "source": payload.get("odds", {}).get("source"),
        }
    }
    return {"role": "user", "content": json.dumps(pack, ensure_ascii=False)}

def _one_call(payload: Dict[str, Any]) -> Dict[str, Any]:
    """One JSON-mode call. Returns parsed profitable-shape dict."""
    resp = _client_lazy().chat.completions.create(
        model=MODEL_NAME,
        temperature=PRED_TEMP,
        response_format={"type": "json_object"},
        messages=[_msg_system(), _msg_user(payload)],
    )
    content = resp.choices[0].message.content or "{}"
    data = json.loads(content)

    # Expected shape:
    # { "fixture_id": 123, "predictions": { "over_2_5": { ... } } }
    fid = data.get("fixture_id")
    over = ((data.get("predictions") or {}).get("over_2_5")) or {}

    # Safety coercions
    def _f(x):
        try:
            return float(x)
        except Exception:
            return None

    # Force odds to the exact price we sent (prevents drift)
    price = payload.get("odds", {}).get("over_2_5")
    if price is None:
        return {}

    prediction   = over.get("prediction") or ("Over" if _f(over.get("confidence")) and _f(over.get("confidence")) >= 50 else "Under")
    confidence   = _f(over.get("confidence"))  # 0..100
    bankroll_pct = _f(over.get("bankroll_pct"))
    rationale    = over.get("rationale")

    # Recompute implied & edge from the price we actually used
    implied_odds_pct = 100.0 / float(price)
    if confidence is None:
        confidence = 50.0  # neutral backstop
    edge = confidence - implied_odds_pct  # percent points

    # Apply caps from the spec (rule 8)
    if bankroll_pct is None:
        bankroll_pct = 0.0
    bankroll_pct = max(0.0, min(bankroll_pct, 5.0))

    po_value = bool(edge > 0)

    return {
        "fixture_id": fid or payload.get("fixture_id"),
        "market": "over_2_5",
        "prediction": prediction,
        "confidence_pct": round(confidence, 1),
        "implied_odds_pct": round(implied_odds_pct, 2),
        "edge": round(edge, 2),                # percent points (e.g., 2.37)
        "po_value": po_value,
        "stake_pct": round(bankroll_pct, 2),   # percent of bankroll (0..5)
        "odds": float(price),
        "rationale": rationale,
    }

def get_prediction(match_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Aggregates N samples (median) but defaults to a single call to mirror past behavior.
    Returns a normalized dict ready for insert_value_predictions.
    """
    price = match_payload.get("odds", {}).get("over_2_5")
    if price is None:
        return {}

    samples: List[Dict[str, Any]] = []
    for _ in range(PRED_SAMPLES):
        try:
            res = _one_call(match_payload)
            if res:
                samples.append(res)
        except Exception:
            continue

    if not samples:
        return {}

    if len(samples) == 1:
        return samples[0]

    # Aggregate by median of confidence & edge; majority vote on prediction
    confs = [s["confidence_pct"] for s in samples if s.get("confidence_pct") is not None]
    edges = [s["edge"] for s in samples if s.get("edge") is not None]
    preds = [s.get("prediction") for s in samples]

    from collections import Counter
    pred = Counter(preds).most_common(1)[0][0] if preds else "Over"
    conf = statistics.median(confs) if confs else 50.0
    edg  = statistics.median(edges) if edges else (conf - 100.0/float(price))

    # Use first rationale (short)
    rationale = samples[0].get("rationale")

    out = samples[0].copy()
    out.update({
        "prediction": pred,
        "confidence_pct": round(conf, 1),
        "edge": round(edg, 2),
        "rationale": rationale,
        "po_value": bool(edg > 0 and pred == "Over"),
    })
    return out
