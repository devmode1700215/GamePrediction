# utils/scoring.py
from __future__ import annotations
from typing import Any, Dict, Optional

def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _map_lin(x: Optional[float], lo: float, hi: float) -> float:
    if x is None: return 0.0
    if x <= lo: return -1.0
    if x >= hi: return 1.0
    return 2.0 * (x - lo) / (hi - lo) - 1.0

def _avg2(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None: return None
    return (float(a) + float(b)) / 2.0

def _to_float(x: Any) -> Optional[float]:
    try: return float(x)
    except Exception: return None

def _implied_prob(odds: Optional[float]) -> Optional[float]:
    if odds is None: return None
    if odds <= 1.0: return None
    return 1.0 / odds

def score_ou25(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deterministic scoring for OU2.5 using weighted signals.
    Expects payload built in main.py (_build_llm_payload_from_enriched).
    Returns a dict ready to store (prediction, confidence, edge, signals, weights, priors).
    """
    odds = (payload.get("odds") or {})
    over = _to_float(odds.get("over_2_5"))
    under = _to_float(odds.get("under_2_5"))
    p_mkt_over = _implied_prob(over) or 0.50

    recent = payload.get("recent_form") or {}
    r_home = recent.get("home") or {}
    r_away = recent.get("away") or {}

    season = payload.get("season_context") or {}
    s_home = season.get("home") or {}
    s_away = season.get("away") or {}

    injuries = payload.get("injuries") or {}
    inj_home = injuries.get("home") or []
    inj_away = injuries.get("away") or []

    # Weights (sum 100)
    W_form_tempo = 30
    W_form_rates = 20
    W_season_base = 20
    W_injuries    = 20
    W_h2h         = 10
    k_factor      = 0.20

    # 1) form_tempo
    gfga_h = (r_home.get("gf_avg"), r_home.get("ga_avg"))
    gfga_a = (r_away.get("gf_avg"), r_away.get("ga_avg"))
    sum_h = None if (gfga_h[0] is None or gfga_h[1] is None) else float(gfga_h[0]) + float(gfga_h[1])
    sum_a = None if (gfga_a[0] is None or gfga_a[1] is None) else float(gfga_a[0]) + float(gfga_a[1])
    avg_goals_recent = _avg2(sum_h, sum_a)
    s_form_tempo = _map_lin(avg_goals_recent, lo=2.0, hi=3.4)

    # 2) form_rates
    ou_mean = _avg2(r_home.get("ou25_rate"), r_away.get("ou25_rate"))
    btts_mean = _avg2(r_home.get("btts_rate"), r_away.get("btts_rate"))
    if ou_mean is None: ou_mean = 0.5
    if btts_mean is None: btts_mean = 0.5
    s_form_rates = 0.7*(2*ou_mean - 1.0) + 0.3*(2*btts_mean - 1.0)
    s_form_rates = _clip(s_form_rates, -1.0, 1.0)

    # 3) season_base
    gspg_h = None
    gspg_a = None
    if s_home.get("goals_for_pg") is not None and s_home.get("goals_against_pg") is not None:
        gspg_h = float(s_home["goals_for_pg"]) + float(s_home["goals_against_pg"])
    if s_away.get("goals_for_pg") is not None and s_away.get("goals_against_pg") is not None:
        gspg_a = float(s_away["goals_for_pg"]) + float(s_away["goals_against_pg"])
    season_goals = _avg2(gspg_h, gspg_a)
    s_season_base = _map_lin(season_goals, lo=2.1, hi=3.2) if season_goals is not None else 0.0

    # 4) injuries
    def _inj_delta(lst):
        if not isinstance(lst, list): return 0.0
        delta = 0.0
        count_att = 0
        count_def = 0
        for it in lst:
            pos = (it or {}).get("position") or ""
            status = (it or {}).get("status") or ""
            if str(status).lower() not in ("out", "injury", "suspended", "doubtful"):
                continue
            p = pos.upper()
            if p in ("ST","CF","RW","LW","W","AM","SS"):
                if count_att < 2:
                    delta -= 0.15
                    count_att += 1
            elif p in ("GK","CB","RCB","LCB"):
                if count_def < 2:
                    delta += 0.15
                    count_def += 1
        return _clip(delta, -0.6, 0.6)

    s_injuries = _inj_delta(inj_home) + _inj_delta(inj_away)
    s_injuries = _clip(s_injuries, -0.6, 0.6)

    # 5) h2h
    h2h = payload.get("head_to_head") or []
    over_cnt = 0
    n = 0
    for m in h2h[:3]:
        score = (m.get("score") or "")
        try:
            parts = score.split("-")
            gh = int(parts[0]); ga = int(parts[1])
            over_cnt += 1 if (gh + ga) > 2 else 0
            n += 1
        except Exception:
            continue
    over_ratio = (over_cnt / n) if n else 0.5
    s_h2h = (2*over_ratio - 1.0) * 0.5

    # Weighted total
    s_total = (
        W_form_tempo * s_form_tempo +
        W_form_rates * s_form_rates +
        W_season_base * s_season_base +
        W_injuries    * s_injuries +
        W_h2h         * s_h2h
    ) / 100.0
    s_total = _clip(s_total, -1.0, 1.0)

    # Market prior + bounded delta
    p_over = _clip(p_mkt_over + k_factor * s_total, 0.05, 0.95)
    prediction = "Over" if p_over >= 0.5 else "Under"
    confidence = p_over if prediction == "Over" else (1.0 - p_over)

    chosen_odds = over if prediction == "Over" else under
    implied = _implied_prob(chosen_odds)
    edge = (confidence - implied) if (implied is not None) else None
    edge = round(edge, 4) if edge is not None else None

    return {
        "fixture_id": payload.get("fixture_id"),
        "market": "over_2_5",
        "prediction": prediction,
        "prob_over": round(p_over, 4),
        "confidence": round(confidence, 4),
        "odds": chosen_odds,
        "edge": edge,
        "stake_units": 1,
        "rationale": [],  # filled by LLM optionally
        "signals": {
            "form_tempo": round(s_form_tempo, 3),
            "form_rates": round(s_form_rates, 3),
            "season_base": round(s_season_base, 3),
            "injuries": round(s_injuries, 3),
            "h2h": round(s_h2h, 3),
            "weighted_total": round(s_total, 3),
        },
        "weights": {
            "form_tempo": W_form_tempo,
            "form_rates": W_form_rates,
            "season_base": W_season_base,
            "injuries": W_injuries,
            "h2h": W_h2h,
            "k_factor": k_factor,
        },
        "priors": { "p_market_over": round(p_mkt_over, 4) }
    }
