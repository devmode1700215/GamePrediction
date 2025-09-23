# utils/get_prediction.py
import json, os
from pathlib import Path
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# Accept either var name
_api_key = os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_KEY")
if not _api_key:
    raise RuntimeError("Missing OPENAI_API_KEY (or OPENAI_KEY)")

MODEL_NAME = os.getenv("MODEL_NAME", "gpt-4o-mini")  # or "gpt-5-nano"
client = OpenAI(api_key=_api_key)

# Load prompt relative to repo root to avoid CWD issues
PROMPT_PATH = (Path(__file__).resolve().parents[1] / "prompt.txt")
with open(PROMPT_PATH, "r", encoding="utf-8") as f:
    PROMPT = f.read()

def _extract_json_snippet(s: str) -> str:
    if not isinstance(s, str):
        return ""
    a = s.find("{"); b = s.rfind("}")
    return s[a:b+1] if (a != -1 and b != -1 and b > a) else s.strip()

def _fallback_over25(match_data: dict) -> dict:
    """Minimal safe OU2.5 in case parsing fails."""
    fx = match_data.get("fixture_id") or 0
    over = None
    try:
        over = float((match_data.get("odds") or {}).get("over_2_5"))
    except Exception:
        pass
    if not over or over <= 1.0:
        over = 1.60  # sensible default
    # simple implied prob/EV
    p = max(1.0 / over, 1e-6)
    ev = p * over - 1.0
    return {
        "fixture_id": int(fx) if str(fx).isdigit() else 0,
        "predictions": {
            "over_2_5": {
                "prediction": "Over",
                "confidence": 55,
                "edge": round(ev, 4),
                "po_value": ev >= 0.01,         # treat ≥1% EV as value
                "odds": over,
                "bankroll_pct": 0.5 if ev >= 0.01 else 0.0,
                "rationale": "Fallback from price; simple implied prob and EV."
            }
        }
    }

def get_prediction(match_data: dict) -> dict | None:
    # If you only want OU2.5, you can add a one-liner after parsing to keep just that market.
    try:
        resp = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": PROMPT},
                {"role": "user", "content": json.dumps(match_data, ensure_ascii=False)},
            ],
            temperature=0.2,
        )
        content = resp.choices[0].message.content or ""
        snippet = _extract_json_snippet(content)
        data = json.loads(snippet)

        # If you want ONLY OU2.5, prune here:
        if isinstance(data, dict) and isinstance(data.get("predictions"), dict) and "over_2_5" in data["predictions"]:
            data["predictions"] = {"over_2_5": data["predictions"]["over_2_5"]}

        return data
    except Exception as e:
        # Last resort: return a minimal OU2.5 so your pipeline keeps going
        return _fallback_over25(match_data)
