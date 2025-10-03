# utils/scoring.py  — "Classic" profile: light market shrink, larger signals
import os
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)
DEBUG = os.getenv("SCORING_DEBUG", "false").lower() in ("1","true","yes","y")

# ---------- Tunables (env overrides) ----------
# Prior shrink (keep small as before)
K_FACTOR       = float(os.getenv("SC_K_FACTOR", "0.20"))   # 0.2 ~ your previous behavior

# Posting gates (let DB filter negatives; keep here permissive)
MIN_EDGE       = float(os.getenv("SC_MIN_EDGE", "0.00"))
MIN_CONF       = float(os.getenv("SC_MIN_CONF", "0.00"))

# Odds sanity for OU2.5
ODDS_MIN       = float(os.getenv("SC_ODDS_MIN", "1.35"))
ODDS_MAX       = float(os.getenv("SC_ODDS_MAX", "3.60"))

# Classic weights (match your log)
W_FORM_TEMPO   = float(os.getenv("SC_W_FORM_TEMPO", "30"))
W_FORM_RATES   = float(os.getenv("SC_W_FORM_RATES", "20"))
W_SEASON_BASE  = float(os.getenv("SC_W_SEASON_BASE", "20"))
W_INJURIES     = float(os.getenv("SC_W_INJURIES", "20"))
W_H2H          = float(os.getenv("SC_W_H2H", "10"))

# Safety clamps
DELTA_CAP      = float(os.getenv("SC_DELTA_CAP", "0.25"))  # max move around 0.5 before shrink
EDGE_CAP       = float(os.getenv("SC_EDGE_CAP", "0.20"))   # ±20%

def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _clip01(x: float) -> float:
    return max(0.0, min(1.0, x))

def _implied(odds: Optional[float]) -> Optional[float]:
    o = _to_float(odds)
    if o is None or o <= 1.0:
        return None
    return 1.0 / o

def _fair_two_sided(p_over_raw: Optional[float], p_under_raw: Optional[float]) -> Optional[float]:
    if p_over_raw is None and p_under_raw is None:
        return None
    if p_under_raw is None:
        # mild devig if only one side available
        return _clip01(0.5 + (p_over_raw - 0.5) * 0.94) if p_over_raw is not None else 0.5
    if p_over_raw is None:
        # symmetric
        po = 1.0 - p_under_raw
        return _clip01(0.5 + (po - 0.5) * 0.94)
    s = p_over_raw + p_under_raw
    if s <= 0: return None
    return _clip01(p_over_raw / s)

def _tempo_signal(team: Any) -> float:
    if not isinstance(team, dict): return 0.0
    # Prefer xG; fallback to gf_avg if you store it
    xg = _to_float(team.get("xg_for_avg"))
    if xg is None:
        gf = _to_float(team.get("gf_avg"))
        if gf is None: return 0.0
        xg = gf
    # map around 1.45; compress to [-0.35, +0.35]
    s = (xg - 1.45) / 1.4
    return max(-0.35, min(0.35, s))

def _form_rate_signal(team: Any) -> float:
    if not isinstance(team, dict): return 0.0
    r = _to_float(team.get("ou25_rate"))
    if r is None: return 0.0
    # center at 0.5 → [-0.25, +0.25]
    return max(-0.25, min(0.25, (r - 0.5) * 0.5 * 2))

def _season_base_signal(home: Any, away: Any) -> float:
    # Use season goals per game if available; small anchor
    def _gpg(t):
        if not isinstance(t, dict): return None
        gfor  = _to_float(t.get("goals_for_pg"))
        gag   = _to_float(t.get("goals_against_pg"))
        if gfor is None and gag is None: return None
        gfor = gfor or 0.0; gag = gag or 0.0
        return gfor + gag
    h = _gpg(home); a = _gpg(away)
    if h is None and a is None: return 0.0
    g = ( (h or 0.0) + (a or 0.0) ) / 2.0
    # center around ~2.6 goals/game → map to [-0.20,+0.20]
    return max(-0.20, min(0.20, (g - 2.6) / 2.0))

def _injury_signal(inj: Any) -> float:
    if not isinstance(inj, list): return 0.0
    n = len(inj)
    # classic was mild: each injury ≈ -0.02 capped at -0.20
    return -min(0.20, n * 0.02)

def _h2h_signal(h2h: Any) -> float:
    if not isinstance(h2h, list) or not h2h: return 0.0
    over = 0; tot = 0
    for m in h2h[:3]:
        try:
            a,b = (m.get("score") or "0-0").split("-")
            if int(a)+int(b) >= 3: over += 1
            tot += 1
        except Exception:
            continue
    if tot == 0: return 0.0
    rate = over / tot
    # very small: [-0.10, +0.10]
    return max(-0.10, min(0.10, (rate - 0.5) * 0.2 * 2))

def _blend_market(p_model: float, p_market: float) -> float:
    return _clip01(K_FACTOR * p_market + (1.0 - K_FACTOR) * p_model)

def _confidence(p_over: float, p_mkt: float) -> float:
    base = min(1.0, abs(p_over - 0.5) * 2.0)
    gap  = min(1.0, abs(p_over - p_mkt) * 4.0)
    return _clip01(0.5 * base + 0.5 * gap)

def _ev(prob: float, odds: float) -> float:
    return prob * odds - 1.0

def _cap_edge(e: float) -> float:
    if e >  EDGE_CAP: return EDGE_CAP
    if e < -EDGE_CAP: return -EDGE_CAP
    return e

def score_ou25(payload: Dict[str, Any]) -> Dict[str, Any]:
    odds = payload.get("odds") or {}
    o_over  = _to_float(odds.get("over_2_5"))
    o_under = _to_float(odds.get("under_2_5"))
    valid_over  = (o_over  is not None) and (ODDS_MIN <= o_over  <= ODDS_MAX)
    valid_under = (o_under is not None) and (ODDS_MIN <= o_under <= ODDS_MAX)
    if not valid_over and not valid_under:
        return {}

    p_over_raw  = _implied(o_over)  if valid_over  else None
    p_under_raw = _implied(o_under) if valid_under else None
    p_mkt_over  = _fair_two_sided(p_over_raw, p_under_raw) or 0.5

    rf = payload.get("recent_form") or {}
    sc = payload.get("season_context") or {}
    inj= payload.get("injuries") or {}
    h2h= payload.get("head_to_head") or []

    tempo   = (_tempo_signal(rf.get("home")) + _tempo_signal(rf.get("away"))) / 2.0
    rates   = (_form_rate_signal(rf.get("home")) + _form_rate_signal(rf.get("away"))) / 2.0
    season  = _season_base_signal(sc.get("home"), sc.get("away"))
    inj_sig = _injury_signal(inj.get("home")) + _injury_signal(inj.get("away"))
    h2h_sig = _h2h_signal(h2h)

    w_sum = (W_FORM_TEMPO * tempo +
             W_FORM_RATES * rates +
             W_SEASON_BASE * season +
             W_INJURIES * inj_sig +
             W_H2H * h2h)

    w_total = max(1.0, W_FORM_TEMPO + W_FORM_RATES + W_SEASON_BASE + W_INJURIES + W_H2H)
    delta   = max(-DELTA_CAP, min(DELTA_CAP, w_sum / w_total))
    p_model_over = _clip01(0.5 + delta)

    # Light shrink to market (as before)
    p_over = _blend_market(p_model_over, p_mkt_over)

    # EV for both sides and choose best
    ev_over  = _cap_edge(_ev(p_over, o_over))   if valid_over  else None
    ev_under = _cap_edge(_ev(1.0 - p_over, o_under)) if valid_under else None

    if ev_over is not None and ev_under is not None:
        pick   = "Over" if ev_over >= ev_under else "Under"
        odds_u = o_over if pick == "Over" else o_under
        edge   = ev_over if pick == "Over" else ev_under
    elif ev_over is not None:
        pick, odds_u, edge = "Over",  o_over,  ev_over
    elif ev_under is not None:
        pick, odds_u, edge = "Under", o_under, ev_under
    else:
        return {}

    conf = _confidence(p_over, p_mkt_over)
    po_value = (edge >= MIN_EDGE) and (conf >= MIN_CONF)

    out = {
        "prediction": pick,
        "market": "over_2_5",
        "odds": round(odds_u, 2),
        "prob_over": round(p_over, 4),
        "confidence": round(conf, 4),
        "confidence_pct": round(conf * 100.0, 1),
        "edge": round(edge, 4),
        "po_value": po_value,
        "signals": {
            "form_tempo": round(tempo, 3),
            "form_rates": round(rates, 3),
            "season_base": round(season, 3),
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
            "p_market_over": round(p_mkt_over, 4),
        },
    }

    if DEBUG:
        logger.info(
            "SCORING | fid=%s pick=%s conf=%.4f prob_over=%.4f odds=%.2f edge=%.4f | signals=%s | weights=%s | priors=%s",
            payload.get("fixture_id"), pick, out["confidence"], out["prob_over"],
            out["odds"], out["edge"], out["signals"], out["weights"], out["priors"],
        )
    return out
