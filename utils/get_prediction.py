# utils/get_prediction.py
import logging
import math
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- helpers ----------

def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip().lower() == "none":
            return None
        return float(x)
    except Exception:
        return None

def _to_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip().lower() == "none":
            return None
        if isinstance(x, str) and "." in x:
            f = _to_float(x)
            return int(round(f)) if f is not None else None
        return int(x)
    except Exception:
        return None

def _clean_goals_list(values: Any) -> List[int]:
    out: List[int] = []
    if not isinstance(values, list):
        return out
    for v in values:
        iv = _to_int(v)
        if iv is not None and iv >= 0:
            out.append(iv)
    return out

def _mean(lst: List[float]) -> Optional[float]:
    return (sum(lst) / len(lst)) if lst else None

def _poisson_pmf(k: int, lam: float) -> float:
    if lam <= 0:
        return 0.0 if k > 0 else 1.0
    try:
        return math.exp(-lam) * (lam ** k) / math.factorial(k)
    except Exception:
        return 0.0

def _prob_over25_from_lambdas(lh: float, la: float) -> float:
    lam = max(0.0, lh) + max(0.0, la)
    p0 = _poisson_pmf(0, lam)
    p1 = _poisson_pmf(1, lam)
    p2 = _poisson_pmf(2, lam)
    return max(0.0, min(1.0, 1.0 - (p0 + p1 + p2)))

def _implied_pct_from_odds(odds: float) -> Optional[float]:
    if odds is None or odds <= 1.0:
        return None
    return 100.0 / odds

def _stake_pct_from_edge(edge_pct: float) -> float:
    raw = 0.5 * max(0.0, edge_pct)  # 0.5% per edge point
    return max(0.5, min(3.0, raw))

# ---------- core ----------

def _build_over_under_prediction(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    odds_block = match_data.get("odds") or {}
    over_odds = _to_float(odds_block.get("over_2_5"))
    under_odds = _to_float(odds_block.get("under_2_5"))

    if over_odds is None and under_odds is None:
        logger.info("🪙 Skipping math prediction: no over/under odds available.")
        return None

    home = (match_data.get("home_team") or {})
    away = (match_data.get("away_team") or {})
    home_recent = _clean_goals_list(home.get("recent_goals"))
    away_recent = _clean_goals_list(away.get("recent_goals"))

    if not home_recent and not away_recent:
        logger.info("📉 Skipping math prediction: no recent goals data found.")
        return None

    home_avg = _mean(home_recent) if home_recent else 1.2
    away_avg = _mean(away_recent) if away_recent else 1.2
    if home_avg is None or away_avg is None:
        return None

    p_over = _prob_over25_from_lambdas(home_avg, away_avg)
    p_under = 1.0 - p_over

    best = None
    if over_odds is not None:
        imp_over = _implied_pct_from_odds(over_odds)
        if imp_over is not None:
            over_pct = p_over * 100.0
            over_edge = over_pct - imp_over
            best = ("Over", over_odds, over_pct, over_edge)

    if under_odds is not None:
        imp_under = _implied_pct_from_odds(under_odds)
        if imp_under is not None:
            under_pct = p_under * 100.0
            under_edge = under_pct - imp_under
            if best is None or under_edge > best[3]:
                best = ("Under", under_odds, under_pct, under_edge)

    if best is None:
        return None

    side, side_odds, side_prob_pct, edge_pct = best
    po_value = edge_pct >= 0.0

    return {
        "prediction": side,
        "confidence": round(float(side_prob_pct), 2),
        "implied_odds_pct": round(float(_implied_pct_from_odds(side_odds) or 0.0), 2),
        "edge": round(float(edge_pct), 2),
        "po_value": po_value,
        "odds": round(float(side_odds), 2),
        "bankroll_pct": round(_stake_pct_from_edge(edge_pct), 2),
        "rationale": (
            f"Poisson on recent goals (home_avg={home_avg:.2f}, away_avg={away_avg:.2f}); "
            f"prob_over={p_over:.3f}, prob_under={p_under:.3f}."
        ),
    }

# ---------- public API ----------

def get_prediction(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Math-only predictor (no AI).
    Returns {"fixture_id": ..., "predictions": {"over_2_5": {...}}} or None.
    """
    fixture_id = match_data.get("fixture_id")
    if not fixture_id:
        logger.info("Skipping prediction: missing fixture_id.")
        return None

    ou = _build_over_under_prediction(match_data)
    if not ou:
        return None

    return {"fixture_id": fixture_id, "predictions": {"over_2_5": ou}}

# Tiny self-test (harmless in prod)
if __name__ == "__main__":
    demo = {
        "fixture_id": 123,
        "home_team": {"recent_goals": [1, 2, 2, "None", 3]},
        "away_team": {"recent_goals": [0, 1, None, 2, 1]},
        "odds": {"over_2_5": 2.1, "under_2_5": 1.75},
    }
    print(get_prediction(demo))
