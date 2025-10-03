# utils/get_prediction.py
# Restores the original weighted system (pre-scoring.py):
# - Signals: form_tempo (30), form_rates (20), season_base (20), injuries (20), h2h (10)
# - Weighted_total = sum(w_i * s_i) / sum(weights)
# - p_model = 0.5 + SLOPE * weighted_total  (SLOPE≈0.70 to match your old logs)
# - Shrink to market prior with k_factor ≈ 0.2 (light shrink)
# - Pick side by higher EV: edge = p*odds - 1
# - Confidence = prob of chosen side (mirrors old logs where conf≈prob_over for Over picks)
# - Robust parsing (no string*float crashes)

import os
import json
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# -----------------------------
# Env tunables (match old behavior)
# -----------------------------
W_FORM_TEMPO   = float(os.getenv("SC_W_FORM_TEMPO", "30"))
W_FORM_RATES   = float(os.getenv("SC_W_FORM_RATES", "20"))
W_SEASON_BASE  = float(os.getenv("SC_W_SEASON_BASE", "20"))
W_INJURIES     = float(os.getenv("SC_W_INJURIES", "20"))
W_H2H          = float(os.getenv("SC_W_H2H", "10"))

K_FACTOR       = float(os.getenv("SC_K_FACTOR", "0.20"))     # light shrink to market (old: ~0.2)
SLOPE          = float(os.getenv("SC_SLOPE", "0.70"))        # maps weighted_total -> p_model delta
DELTA_CAP      = float(os.getenv("SC_DELTA_CAP", "0.35"))    # cap p_model move around 0.5 (old behavior allowed ~0.25–0.35)

ODDS_MIN       = float(os.getenv("SC_ODDS_MIN", "1.35"))
ODDS_MAX       = float(os.getenv("SC_ODDS_MAX", "3.60"))

MIN_EDGE       = float(os.getenv("SC_MIN_EDGE", "0.00"))     # posting gates (keep permissive; you filter at DB)
MIN_CONF       = float(os.getenv("SC_MIN_CONF", "0.00"))     # 0..1

DEBUG          = os.getenv("SCORING_DEBUG", "false").lower() in ("1","true","yes","y")

# Optional: short LLM rationale (unchanged)
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

# -----------------------------
# Helpers
# -----------------------------
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
    """Fair Over prob removing vig; if one side is missing, mild devig toward 0.5."""
    if p_over_raw is None and p_under_raw is None:
        return None
    if p_under_raw is None and p_over_raw is not None:
        return _clip01(0.5 + (p_over_raw - 0.5) * 0.94)
    if p_over_raw is None and p_under_raw is not None:
        po = 1.0 - p_under_raw
        return _clip01(0.5 + (po - 0.5) * 0.94)
    s = p_over_raw + p_under_raw
    if s <= 0:
        return None
    return _clip01(p_over_raw / s)

def _ev(prob: float, odds: float) -> float:
    return prob * odds - 1.0

# ----- signals (mirror old shapes) -------------------------------------------
def _form_tempo_sig(team: Any) -> float:
    """
    From recent form: prefer xg_for_avg (or xg), fallback gf_avg.
    Map around 1.45; compress to roughly [-0.35, +0.35].
    """
    if not isinstance(team, dict): return 0.0
    xg = _to_float(team.get("xg_for_avg"))
    if xg is None:
        xg = _to_float(team.get("xg"))
    if xg is None:
        xg = _to_float(team.get("gf_avg"))
    if xg is None:
        return 0.0
    s = (xg - 1.45) / 1.4
    return max(-0.35, min(0.35, s))

def _form_rates_sig(team: Any) -> float:
    """
    OU2.5 hit rate if you stored it in recent form (ou25_rate in [0..1]).
    Translate around 0.5 to roughly [-0.25, +0.25].
    """
    if not isinstance(team, dict): return 0.0
    r = _to_float(team.get("ou25_rate"))
    if r is None: return 0.0
    return max(-0.25, min(0.25, (r - 0.5) * 0.5 * 2))

def _season_base_sig(home: Any, away: Any) -> float:
    """
    Old system often acted like a discrete anchor. We reconstruct it:
    Compute average season goals per game = (GF+GA)/2 for each team.
    If avg >= 2.8 => +1.0 ; if <= 2.4 => -1.0 ; else 0.0.
    """
    def _gpg(t):
        if not isinstance(t, dict): return None
        gf = _to_float(t.get("goals_for_pg"))
        ga = _to_float(t.get("goals_against_pg"))
        if gf is None and ga is None: return None
        gf = gf or 0.0; ga = ga or 0.0
        return gf + ga
    h = _gpg(home); a = _gpg(away)
    if h is None and a is None: return 0.0
    g = ((h or 0.0) + (a or 0.0)) / 2.0
    if g >= 2.8: return 1.0
    if g <= 2.4: return -1.0
    return 0.0

def _injuries_sig(inj_home: Any, inj_away: Any) -> float:
    """
    Net injury drag (old shape): each injury ≈ -0.02, capped at -0.20 per team.
    Sum of both teams (=> in [-0.40, 0.0]), but old logs often landed near 0.0.
    """
    def _drag(lst):
        if not isinstance(lst, list): return 0.0
        n = len(lst)
        return -min(0.20, n * 0.02)
    return _drag(inj_home) + _drag(inj_away)

def _h2h_sig(h2h: Any) -> float:
    """
    Rate over last up to 3 meetings: (over_rate - 0.5) -> e.g., 1/3 over => -0.167 (matches your old logs).
    """
    if not isinstance(h2h, list) or not h2h:
        return 0.0
    over = 0; tot = 0
    for m in h2h[:3]:
        try:
            score = str((m or {}).get("score") or "0-0")
            a, b = score.split("-")
            if int(a) + int(b) >= 3:
                over += 1
            tot += 1
        except Exception:
            continue
    if tot == 0:
        return 0.0
    rate = over / tot
    return rate - 0.5  # in [-0.5, +0.5], typically [-0.333, +0.333] for 3 games

# ----- rationale (optional, short) -------------------------------------------
def _rationale_llm(payload: Dict[str, Any], pick: str, p_over: float, odds_used: float, edge: float, signals: Dict[str, float]) -> list[str]:
    if not _USE_LLM or openai is None or not _MODEL:
        return []
    prompt = {
        "task": "2 short bullets explaining the OU2.5 pick. Be factual.",
        "fixture_id": payload.get("fixture_id"),
        "pick": pick,
        "prob_over": round(p_over, 4),
        "odds_used": odds_used,
        "edge": round(edge, 4),
        "signals": signals,
        "odds_block": payload.get("odds"),
    }
    try:
        resp = openai.chat.completions.create(
            model=_MODEL,
            temperature=float(os.getenv("LLM_TEMPERATURE", "0.0")),
            messages=[
                {"role": "system", "content": "Output a JSON list of 2 short strings. No preamble."},
                {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)}
            ],
        )
        txt = resp.choices[0].message.content.strip()
        bullets = json.loads(txt)
        if isinstance(bullets, list):
            return [str(x) for x in bullets[:2]]
        return []
    except Exception as e:
        logger.info(f"Rationale LLM skipped ({e})")
        return []

# -----------------------------
# Main (classic weighted engine)
# -----------------------------
def get_prediction(payload: Dict[str, Any]) -> Dict[str, Any]:
    odds = payload.get("odds") or {}
    o_over  = _to_float(odds.get("over_2_5"))
    o_under = _to_float(odds.get("under_2_5"))

    valid_over  = (o_over  is not None) and (ODDS_MIN <= o_over  <= ODDS_MAX)
    valid_under = (o_under is not None) and (ODDS_MIN <= o_under <= ODDS_MAX)
    if not valid_over and not valid_under:
        return {}

    # Market prior (fair / de-vig)
    p_over_raw  = _implied(o_over)  if valid_over  else None
    p_under_raw = _implied(o_under) if valid_under else None
    p_market_over = _fair_two_sided(p_over_raw, p_under_raw) or 0.5

    rf = payload.get("recent_form") or {}
    sc = payload.get("season_context") or {}
    inj = payload.get("injuries") or {}
    h2h = payload.get("head_to_head") or []

    # Signals (exactly like before)
    tempo   = (_form_tempo_sig(rf.get("home")) + _form_tempo_sig(rf.get("away"))) / 2.0
    rates   = (_form_rates_sig(rf.get("home")) + _form_rates_sig(rf.get("away"))) / 2.0
    season  = _season_base_sig(sc.get("home"), sc.get("away"))
    inj_sig = _injuries_sig(inj.get("home"), inj.get("away"))
    h2h_sig = _h2h_sig(h2h)

    # Weighted total (sum(w_i*sig_i)/sum_w) — matches your old "weighted_total" logs
    w_sum = (W_FORM_TEMPO * tempo +
             W_FORM_RATES * rates +
             W_SEASON_BASE * season +
             W_INJURIES * inj_sig +
             W_H2H * h2h_sig)
    w_total = max(1.0, W_FORM_TEMPO + W_FORM_RATES + W_SEASON_BASE + W_INJURIES + W_H2H)
    weighted_total = w_sum / w_total  # e.g. 0.324 like your old log

    # Map to p_model
    delta = max(-DELTA_CAP, min(DELTA_CAP, SLOPE * weighted_total))
    p_model_over = _clip01(0.5 + delta)

    # Light shrink to market prior (k≈0.2)
    p_over = _clip01(K_FACTOR * p_market_over + (1.0 - K_FACTOR) * p_model_over)

    # EV both sides; pick higher EV (old behavior)
    ev_over  = _ev(p_over, o_over)   if valid_over  else None
    ev_under = _ev(1.0 - p_over, o_under) if valid_under else None

    if ev_over is not None and ev_under is not None:
        if ev_over >= ev_under:
            pick, odds_used, edge = "Over", o_over, ev_over
            conf = p_over
        else:
            pick, odds_used, edge = "Under", o_under, ev_under
            conf = 1.0 - p_over
    elif ev_over is not None:
        pick, odds_used, edge = "Over", o_over, ev_over
        conf = p_over
    elif ev_under is not None:
        pick, odds_used, edge = "Under", o_under, ev_under
        conf = 1.0 - p_over
    else:
        return {}

    conf = _clip01(conf)
    po_value = (edge >= MIN_EDGE) and (conf >= MIN_CONF)

    out = {
        "fixture_id": payload.get("fixture_id"),
        "market": "over_2_5",
        "prediction": pick,
        "odds": round(float(odds_used), 2),
        "prob_over": round(p_over, 4),
        "confidence": round(conf, 4),                     # mirrors your old logs (conf ≈ side prob)
        "confidence_pct": round(conf * 100.0, 1),
        "edge": round(edge, 4),
        "po_value": bool(po_value),
        "signals": {
            "form_tempo": round(tempo, 3),
            "form_rates": round(rates, 3),
            "season_base": round(season, 3),              # can be -1.0, 0.0, +1.0 like before
            "injuries": round(inj_sig, 3),
            "h2h": round(h2h_sig, 3),
            "weighted_total": round(weighted_total, 3),   # expect values like 0.324
        },
        "weights": {
            "form_tempo": W_FORM_TEMPO,
            "form_rates": W_FORM_RATES,
            "season_base": W_SEASON_BASE,
            "injuries": W_INJURIES,
            "h2h": W_H2H,
            "k_factor": K_FACTOR,
            "slope": SLOPE,
        },
        "priors": {
            "p_over_raw": round(p_over_raw, 4) if p_over_raw is not None else None,
            "p_under_raw": round(p_under_raw, 4) if p_under_raw is not None else None,
            "p_market_over": round(p_market_over, 4),
        },
    }

    if DEBUG:
        logger.info(
            "SCORING(classic) | fid=%s pick=%s conf=%.4f prob_over=%.4f odds=%.2f edge=%.4f | "
            "signals=%s | weights=%s | priors=%s",
            out["fixture_id"], pick, out["confidence"], out["prob_over"],
            out["odds"], out["edge"], out["signals"], out["weights"], out["priors"]
        )

    # Optional rationale
    bullets = _rationale_llm(payload, pick, p_over, float(odds_used), float(edge), out["signals"])
    if bullets:
        out["rationale"] = bullets

    return out
