# utils/scoring.py
# Deterministic OU2.5 scoring with vig removal and clear tunables.
import os
import math
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# -----------------------------
# Env tunables (sane defaults)
# -----------------------------
# Weights of signals to build a *model* probability delta away from the market prior.
W_FORM_TEMPO   = float(os.getenv("SC_W_FORM_TEMPO", "30"))   # pace: goals/game in recent form
W_FORM_RATES   = float(os.getenv("SC_W_FORM_RATES", "20"))   # OU hit rates in recent form
W_SEASON_BASE  = float(os.getenv("SC_W_SEASON_BASE", "20"))  # season-level xG/tempo baseline
W_INJURIES     = float(os.getenv("SC_W_INJURIES", "20"))     # injury drag/boost
W_H2H          = float(os.getenv("SC_W_H2H", "10"))          # tiny, avoid overfitting h2h

# Controls how far to move off the market prior (shrinkage to prior).
K_FACTOR       = float(os.getenv("SC_K_FACTOR", "0.20"))     # 0.0 = ignore market; 1.0 = stick to market

# Filters for posting
MIN_CONF       = float(os.getenv("SC_MIN_CONF", "0.0"))      # confidence in [0,1]; 0.7 = 70%
MIN_EDGE       = float(os.getenv("SC_MIN_EDGE", "0.0"))      # edge in decimal return, e.g., 0.01 = +1%

# Odds sanity: ignore OU prices outside this range
ODDS_MIN       = float(os.getenv("SC_ODDS_MIN", "1.3"))
ODDS_MAX       = float(os.getenv("SC_ODDS_MAX", "3.5"))

def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _clip01(x: float) -> float:
    return max(0.0, min(1.0, x))

def _implied(odds: Optional[float]) -> Optional[float]:
    """Raw implied prob (with vig) from decimal odds."""
    o = _to_float(odds)
    if o is None or o <= 1.0:
        return None
    return 1.0 / o

def _fair_from_two_sides(p_over_raw: Optional[float], p_under_raw: Optional[float]) -> Optional[float]:
    """
    Remove vig using two sides: fair p_over = p_over_raw / (p_over_raw + p_under_raw).
    Each p_*_raw is 1/odds.
    """
    if p_over_raw is None:
        return None
    if p_under_raw is None:
        # fallback: assume ~5% book margin; deflate a bit
        # If only one side, reduce by 3% absolute toward 0.5 as a mild de-vig.
        return _clip01(0.5 + (p_over_raw - 0.5) * 0.94)
    s = p_over_raw + p_under_raw
    if s <= 0:
        return None
    return _clip01(p_over_raw / s)

def _recent_form_signal(team_block: Any) -> Dict[str, float]:
    """
    Expecting shape like {"form": "W-W-D-L-W", "xg_for_avg": 1.45}
    Return tempo-ish measures in a normalized [-1, +1] space.
    """
    if not isinstance(team_block, dict):
        return {"tempo": 0.0, "rate": 0.0}
    xg = _to_float(team_block.get("xg_for_avg"))
    # Map xG ~ [0.7, 2.2] to tempo signal [-0.6, +0.6] softly.
    if xg is None:
        tempo = 0.0
    else:
        tempo = (xg - 1.45) / 1.5  # ~center 1.45 goals per team per game
        tempo = max(-0.6, min(0.6, tempo))
    # OU hit rate unavailable here -> keep 0; if you store ou25_rate, use it:
    rate = 0.0
    return {"tempo": tempo, "rate": rate}

def _injury_signal(team_injuries: Any) -> float:
    """
    Simple injury drag: more items -> negative; capped.
    If positions were present, weight FWD/MID downgrades more.
    """
    if not isinstance(team_injuries, list):
        return 0.0
    n = len(team_injuries)
    # map 0..6+ to 0 .. -0.25
    drag = -min(0.25, n * 0.05)
    return drag

def _h2h_signal(h2h: Any) -> float:
    """
    Tiny nudge based on recent H2H total goals vs 2.5 line.
    """
    if not isinstance(h2h, list) or not h2h:
        return 0.0
    over_cnt = 0
    tot = 0
    for m in h2h[:3]:
        s = (m or {}).get("score", "0-0")
        try:
            a, b = s.split("-")
            goals = int(a) + int(b)
            tot += 1
            if goals >= 3:
                over_cnt += 1
        except Exception:
            continue
    if tot == 0:
        return 0.0
    rate = over_cnt / tot  # 0..1
    # convert to [-0.15, +0.15]
    return max(-0.15, min(0.15, (rate - 0.5) * 0.3 * 2))

def _blend_with_market(p_model_raw: float, p_market_fair: float) -> float:
    """Shrink model toward market prior via K_FACTOR."""
    return _clip01(K_FACTOR * p_market_fair + (1.0 - K_FACTOR) * p_model_raw)

def _conf_from_distance(p: float, p_market: float) -> float:
    """
    Confidence as how far we are from indecision once prior-adjusted.
    Map |p-0.5| to [0,1]; optionally weight by distance from market.
    """
    base = min(1.0, abs(p - 0.5) * 2.0)  # 0 at 0.5, 1 near 0/1
    market_gap = abs(p - p_market)       # how much we deviate from market after shrinkage
    return _clip01(0.5 * base + 0.5 * min(1.0, market_gap * 4.0))

def _ev_edge(p: float, odds: float) -> float:
    """
    Expected return edge on decimal odds: EV = p*odds - 1.
    Example: p=0.55, odds=1.90 -> 0.045 (4.5%).
    """
    return p * odds - 1.0

def score_ou25(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deterministic scorer for OU2.5 with vig removal and clear logging.
    Returns:
      {
        prediction: "Over"/"Under",
        market: "over_2_5",
        odds: float,
        prob_over: float,
        confidence: float [0..1],
        confidence_pct: float [0..100],
        edge: float (decimal),
        po_value: bool,
        signals: {...}, weights: {...}, priors: {...}
      }
    """
    odds = (payload.get("odds") or {})
    o_over  = _to_float(odds.get("over_2_5"))
    o_under = _to_float(odds.get("under_2_5"))

    # Guard odds
    valid_over  = (o_over is not None)  and (ODDS_MIN <= o_over  <= ODDS_MAX)
    valid_under = (o_under is not None) and (ODDS_MIN <= o_under <= ODDS_MAX)
    if not valid_over and not valid_under:
        return {}

    # Market-implied (raw)
    p_over_raw  = _implied(o_over)  if valid_over  else None
    p_under_raw = _implied(o_under) if valid_under else None

    # Fair (de-vig) market prior
    p_market_over = _fair_from_two_sides(p_over_raw, p_under_raw)
    if p_market_over is None:
        # last resort: use raw implied over
        p_market_over = p_over_raw if p_over_raw is not None else 0.5

    # ----------------- build model signals -----------------
    rf = payload.get("recent_form") or {}
    sc = payload.get("season_context") or {}
    inj = payload.get("injuries") or {}
    h2h = payload.get("head_to_head") or []

    home_rf = rf.get("home")
    away_rf = rf.get("away")
    s_home = sc.get("home")
    s_away = sc.get("away")
    inj_home = inj.get("home")
    inj_away = inj.get("away")

    # recent form tempo/rate
    rfh = _recent_form_signal(home_rf)
    rfa = _recent_form_signal(away_rf)
    tempo_sig = (rfh["tempo"] + rfa["tempo"]) / 2.0
    rate_sig  = (rfh["rate"]  + rfa["rate"])  / 2.0

    # season baseline: if you track per-team goals_for_pg/against_pg, add them here.
    # For now we lean on recent xG already captured in tempo_sig → make season_base a mild anchor.
    season_base = 0.0

    # injuries: negative drag on goals
    inj_sig = (_injury_signal(inj_home) + _injury_signal(inj_away))  # negative is under-lean

    # small h2h nudge
    h2h_sig = _h2h_signal(h2h)

    # Weighted sum (to a delta in [-1,+1])
    w_sum = (W_FORM_TEMPO * tempo_sig +
             W_FORM_RATES * rate_sig +
             W_SEASON_BASE * season_base +
             W_INJURIES * inj_sig +
             W_H2H * h2h_sig)

    w_total = max(1.0, (W_FORM_TEMPO + W_FORM_RATES + W_SEASON_BASE + W_INJURIES + W_H2H))
    # Model raw over-prob before shrink: move 0.5 by scaled delta
    delta = max(-0.45, min(0.45, w_sum / (w_total * 1.2)))  # cap to avoid extremes
    p_model_raw = _clip01(0.5 + delta)

    # Shrink toward fair market prior
    p_over = _blend_with_market(p_model_raw, p_market_over)

    # Decide side & odds used for EV
    if valid_over and (p_over >= 0.5 or not valid_under):
        side = "Over"
        o_used = o_over
        prob_for_ev = p_over
    else:
        side = "Under"
        o_used = o_under if valid_under else (o_over or 1.0)
        prob_for_ev = 1.0 - p_over

    edge = _ev_edge(prob_for_ev, o_used)
    conf = _conf_from_distance(p_over, p_market_over)
    conf_pct = round(conf * 100.0, 1)

    po_value = (edge >= MIN_EDGE) and (conf >= MIN_CONF)

    scored = {
        "prediction": side,
        "market": "over_2_5",
        "odds": o_used,
        "prob_over": round(p_over, 4),
        "confidence": round(conf, 4),
        "confidence_pct": round(conf_pct, 1),
        "edge": round(edge, 4),
        "po_value": po_value,
        "signals": {
            "form_tempo": round(tempo_sig, 3),
            "form_rates": round(rate_sig, 3),
            "season_base": round(season_base, 3),
            "injuries": round(inj_sig, 3),
            "h2h": round(h2h_sig, 3),
            "weighted_total": round(w_sum / w_total, 3),
        },
        "weights": {
            "form_tempo": W_FORM_TEMPO,
            "form_rates": W_FORM_RATES,
            "season_base": W_SEASON_BASE,
            "injuries": W_INJURIES,
            "h2h": W_H2H,
            "k_factor": K_FACTOR,
        },
        "priors": {
            "p_over_raw": round(p_over_raw, 4) if p_over_raw is not None else None,
            "p_under_raw": round(p_under_raw, 4) if p_under_raw is not None else None,
            "p_market_over": round(p_market_over, 4),
        },
    }

    # If you want to enforce posting thresholds here, uncomment:
    # if not po_value:
    #     scored["skip_reason"] = f"MIN_EDGE/CONF not met ({edge:.4f}, {conf:.2f})"

    # Optional debug log (readable one-liner)
    if os.getenv("SCORING_DEBUG", "false").lower() in ("1","true","yes","y"):
        logger.info(
            "SCORING | fid=%s pick=%s conf=%.3f prob_over=%.4f odds=%.2f edge=%.4f | signals=%s | weights=%s | priors=%s",
            payload.get("fixture_id"),
            scored["prediction"],
            scored["confidence"],
            scored["prob_over"],
            _to_float(scored["odds"]) or -1,
            scored["edge"],
            scored["signals"],
            scored["weights"],
            scored["priors"],
        )

    return scored
