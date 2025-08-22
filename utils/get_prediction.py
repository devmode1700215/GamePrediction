# utils/get_prediction.py
# -*- coding: utf-8 -*-
import logging
import json
import math
from statistics import mean

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONF_MIN = 70.0  # threshold in % (still used for filtering if you want)

def poisson_prob(lmbda: float, k: int) -> float:
    """Poisson probability for k events given mean λ."""
    try:
        return (math.exp(-lmbda) * (lmbda ** k)) / math.factorial(k)
    except Exception:
        return 0.0

def expected_goals(goals: list[int]) -> float:
    """Return average goals scored from recent matches."""
    if not goals:
        return 1.2  # fallback
    return mean(goals)

def calculate_over25_prob(home_goals, away_goals, h2h_scores) -> float:
    """
    Estimate probability of Over 2.5 using Poisson model + H2H.
    """
    home_avg = expected_goals(home_goals)
    away_avg = expected_goals(away_goals)

    lmbda_home = home_avg or 1.2
    lmbda_away = away_avg or 1.0

    # probability distribution of total goals 0–5
    probs = {}
    for gh in range(0, 6):
        for ga in range(0, 6):
            p = poisson_prob(lmbda_home, gh) * poisson_prob(lmbda_away, ga)
            total = gh + ga
            probs[total] = probs.get(total, 0) + p

    prob_over25 = sum(p for g, p in probs.items() if g >= 3)

    # small adjustment from head-to-head (if many were high scoring)
    if h2h_scores:
        over_games = sum(1 for s in h2h_scores if (int(s.split("-")[0]) + int(s.split("-")[1])) >= 3)
        ratio = over_games / len(h2h_scores)
        prob_over25 = (prob_over25 * 0.7) + (ratio * 0.3)

    return prob_over25 * 100  # %

def get_prediction(match_data: dict) -> dict | None:
    """
    Pure math prediction — replaces GPT.
    match_data must include: fixture_id, odds, home_team.recent_goals, away_team.recent_goals, head_to_head.
    """
    try:
        fixture_id = match_data.get("fixture_id")
        odds = match_data.get("odds") or {}
        home_goals = match_data.get("home_team", {}).get("recent_goals", []) or []
        away_goals = match_data.get("away_team", {}).get("recent_goals", []) or []
        h2h = [h.get("score") for h in (match_data.get("head_to_head") or []) if h.get("score")]

        # Calculate probability for Over 2.5
        prob_over25 = calculate_over25_prob(home_goals, away_goals, h2h)
        prob_under25 = 100 - prob_over25

        # Extract market odds
        odd_over = odds.get("over_2_5")
        odd_under = odds.get("under_2_5")

        predictions = {}

        # --- Over 2.5 ---
        if odd_over:
            implied = 100 / odd_over
            edge = prob_over25 - implied
            predictions["over_2_5"] = {
                "prediction": "Over",
                "confidence": round(prob_over25, 2),
                "implied_odds_pct": round(implied, 2),
                "edge": round(edge, 2),
                "po_value": edge > 0,
                "odds": odd_over,
                "bankroll_pct": round(max(edge, 0) / 10, 2),  # simple Kelly fraction
                "rationale": f"Based on avg goals ({mean(home_goals or [1]):.2f} vs {mean(away_goals or [1]):.2f}) "
                             f"and H2H trend, Over 2.5 has {prob_over25:.1f}% probability vs implied {implied:.1f}%."
            }

        # --- Under 2.5 ---
        if odd_under:
            implied = 100 / odd_under
            edge = prob_under25 - implied
            predictions["under_2_5"] = {
                "prediction": "Under",
                "confidence": round(prob_under25, 2),
                "implied_odds_pct": round(implied, 2),
                "edge": round(edge, 2),
                "po_value": edge > 0,
                "odds": odd_under,
                "bankroll_pct": round(max(edge, 0) / 10, 2),
                "rationale": f"Based on avg goals and H2H, Under 2.5 has {prob_under25:.1f}% probability vs implied {implied:.1f}%."
            }

        return {
            "fixture_id": fixture_id,
            "predictions": predictions
        }

    except Exception as e:
        logger.error(f"❌ Error in math prediction: {e}")
        return None


if __name__ == "__main__":
    # quick test
    dummy = {
        "fixture_id": 123,
        "home_team": {"recent_goals": [2, 1, 3, 0, 2]},
        "away_team": {"recent_goals": [1, 2, 0, 1, 1]},
        "odds": {"over_2_5": 2.0, "under_2_5": 1.8},
        "head_to_head": [{"score": "2-1"}, {"score": "1-0"}, {"score": "3-2"}]
    }
    result = get_prediction(dummy)
    print(json.dumps(result, indent=2))
