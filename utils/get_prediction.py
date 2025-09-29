# utils/get_prediction.py
import os
import json
import logging
from typing import Any, Dict

from utils.scoring import score_ou25

# Optional minimal LLM for rationale only
_USE_LLM = os.getenv("USE_LLM_RATIONALE", "false").lower() in ("1","true","yes","y")
try:
    import openai
    openai.api_key = os.getenv("OPENAI_API_KEY", "")
    _MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    if not openai.api_key:
        _USE_LLM = False
except Exception:
    openai = None
    _MODEL = None
    _USE_LLM = False

def _rationale_llm(payload: Dict[str, Any], scored: Dict[str, Any]) -> list[str]:
    """
    Ask the model to produce 2–3 factual bullets referencing fields in payload/scored.
    Short prompt = low tokens. If anything fails, return [].
    """
    if not _USE_LLM or openai is None or not _MODEL:
        return []

    prompt = {
        "task": "Write 2–3 short bullets explaining the OU2.5 pick. Be factual, reference recent_form (gf_avg/ga_avg, ou25_rate), season_context (goals_for_pg/goals_against_pg), injuries (positions), and the market odds. No hedging.",
        "payload_fields_used": ["recent_form","season_context","injuries","odds","head_to_head"],
        "pick": scored.get("prediction"),
        "confidence": scored.get("confidence"),
        "odds": scored.get("odds"),
        "signals": scored.get("signals"),
        "priors": scored.get("priors"),
    }
    try:
        resp = openai.chat.completions.create(
            model=_MODEL,
            temperature=float(os.getenv("LLM_TEMPERATURE", "0.0")),
            messages=[
                {"role": "system", "content": "Output JSON list of 2-3 strings. No preamble."},
                {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)}
            ],
        )
        txt = resp.choices[0].message.content.strip()
        # Try parse JSON list; fallback to single-line split
        bullets = json.loads(txt)
        if isinstance(bullets, list):
            return [str(x) for x in bullets[:3]]
        return []
    except Exception as e:
        logging.info(f"Rationale LLM skipped ({e})")
        return []

def get_prediction(payload: Dict[str, Any]) -> Dict[str, Any]:
    scored = score_ou25(payload)
    # Optionally add rationale
    bullets = _rationale_llm(payload, scored)
    if bullets:
        scored["rationale"] = bullets
    return scored
