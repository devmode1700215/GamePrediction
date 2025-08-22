# utils/get_prediction.py
import logging
import math
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------
# Helpers
# ---------------------------

def _to_float(x: Any) -> Optional[float]:
    """Robust float cast: handles None and 'None' strings."""
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip().lower() == "none":
            return None
        return float(x)
    except Exception:
        return None

def _to_int(x: Any) -> Optional[int]:
    """Robust int cast: handles None and 'None' strings and numeric strings."""
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip().lower() == "none":
            return None
        # Accept floats like "1.0" by converting to float first, then int if integral
        if isinstance(x, str) and "." in x:
            f = _to_float(x)
            if f is None:
                return None
            return int(round(f))
        return int(x)
    except Exception:
        return None

def _clean_goals_list(values: Any) -> List[int]:
    """Sanitize a 'recent_goals' array into integers, dropping bad entries."""
    out: List[int] = []
    if not isinstance(values, list):
        return out
    for v in values:
        iv = _to_int(v)
        if iv is not None and iv >= 0:
            out.append(iv)
    return out

def _mean(lst: List[float]) -> Optional[float]:
    if not lst:
        return None
    return sum(lst) / len(lst)

def _poisson_pmf(k: int, lam: float) -> float:
    # P(X=k) = e^{-λ} λ^k / k!
    if lam <= 0:
        return 0.0 if k > 0 else 1.0
    try:
        return math.exp(-lam) * (lam ** k) / math.factorial(k)
    except Exception:
        return 0.0

def _prob_over25_from_lambdas(lam_home: float, lam_away: float) -> float:
    """
    If goals are Poisson(λh) + Poisson(λa) => Poisson(λ = λh + λa).
    P(Total > 2.5) = 1 - [P(0) + P(1) + P(2)]
    """
    lam_total = max(0.0, lam_home) + max(0.0, lam_away)
    p0 = _poisson_pmf(0, lam_total)
    p1 = _poisson_pmf(1, lam_total)
    p2 = _poisson_pmf(2, lam_total)
    return max(0.0, min(1.0, 1.0 - (p0 + p1 + p2)))

def _implied_pct_from_odds(odds: float) -> Optional[float]:
    if odds is None or odds <= 1.0:
        return None
    return 100.0 / odds

def _stake_pct_from_edge(edge_pct: float) -> float:
    """
    Simple stake sizing: 0.5% per 1% of edge, clamped to [0.5%, 3%].
    """
    raw = 0.5 * max(0.0, edge_pct)  # 0.5% per edge point
    return max(0.5, min(3.0, raw))

# ---------------------------
# Core predictor
# ---------------------------

def _build_over_under_prediction(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Build an Over/Under 2.5 prediction using:
      - recent goals arrays (home/away)
      - Poisson model on total goals
      - market odds from match_data['odds']
    Returns a dict for 'over_2_5' if we can compute, else None.
    """
    odds_block = match_data.get("odds") or {}
    over_odds = _to_float(odds_block.get("over_2_5"))
    under_odds = _to_float(odds_block.get("under_2_5"))

    # We need at least one side of the OU market to proceed
    if over_odds is None and under_odds is None:
        logger.info("🪙 Skipping math prediction: no over/under odds available.")
        return None

    # Pull and sanitize recent goals
    home_team = (match_data.get("home_team") or {})
    away_team = (match_data.get("away_team") or {})
    home_recent = _clean_goals_list(home_team.get("recent_goals"))
    away_recent = _clean_goals_list(away_team.get("recent_goals"))

    # If we truly have nothing, we can't model
    if not home_recent and not away_recent:
        logger.info("📉 Skipping math prediction: no recent goals data found.")
        return None

    # Use means; if one side missing, fallback to a small league-average proxy (1.2 goals)
    home_avg = _mean(home_recent) if home_recent else 1.2
    away_avg = _mean(away_recent) if away_recent else 1.2

    # Tiny guards
    if home_avg is None or away_avg is None:
        logger.info("📉 Skipping math prediction: recent goals means unavailable.")
        return None

    # Poisson model for total goals > 2.5
    p_over = _prob_over25_from_lambdas(home_avg, away_avg)
    p_under = 1.0 - p_over

    # Choose the side with better edge (only if odds for that side exists)
    best = None

    if over_odds is not None:
        implied_over = _implied_pct_from_odds(over_odds)
        if implied_over is not None:
            over_pct = p_over * 100.0
            over_edge = over_pct - implied_over
            best = ("Over", over_odds, over_pct, over_edge)

    if under_odds is not None:
        implied_under = _implied_pct_from_odds(under_odds)
        if implied_under is not None:
            under_pct = p_under * 100.0
            under_edge = under_pct - implied_under
            if best is None or under_edge > best[3]:
                best = ("Under", under_odds, under_pct, under_edge)

    if best is None:
        logger.info("📉 Skipping math prediction: no valid OU side with usable odds.")
        return None

    side, side_odds, side_prob_pct, edge_pct = best
    po_value = edge_pct >= 0.0  # profitable overlay if our prob% >= market implied%

    return {
        "prediction": side,
        "confidence": round(float(side_prob_pct), 2),   # as percent
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

# ---------------------------
# Public API
# ---------------------------

def get_prediction(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Math-only predictor (no AI). Returns:
    {
      "fixture_id": ...,
      "predictions": {
         "over_2_5": { ... }   # only present if we could compute
      }
    }
    or None if nothing computable for this fixture.
    """
    try:
        fixture_id = match_data.get("fixture_id")
        if not fixture_id:
            logger.info("Skipping prediction: missing fixture_id.")
            return None

        # Build OU prediction if possible
        ou = _build_over_under_prediction(match_data)
        if not ou:
            return None

        return {
            "fixture_id": fixture_id,
            "predictions": {
                "over_2_5": ou
            }
        }
    except Exception as e:
        logger.error(f"❌ Error in math prediction: {e}")
        return None
