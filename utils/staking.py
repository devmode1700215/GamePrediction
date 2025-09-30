# utils/staking.py
from __future__ import annotations
import os
from typing import Any, Dict, Optional

def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _clip01(x: float) -> float:
    return max(0.0, min(1.0, x))

def _implied_prob(d: Optional[float]) -> Optional[float]:
    if d is None: return None
    if d <= 1.0: return None
    return 1.0 / d

def _kelly_fraction(p: float, d: float) -> float:
    """
    Kelly for decimal odds d and success prob p.
    f* = (d*p - (1-p)) / (d - 1)
    """
    if d is None or d <= 1.0: 
        return 0.0
    f = (d * p - (1.0 - p)) / (d - 1.0)
    return max(0.0, f)

def compute_stake_pct(pred: Dict[str, Any]) -> float:
    """
    Returns stake as a fraction of bankroll (0..1).
    Safe, deterministic, and aligned with your scoring signals.
    Expects pred dict from get_prediction() + normalization in main.py.
    """
    # ---- Inputs from prediction ----
    pick = pred.get("prediction")
    p = _to_float(pred.get("confidence")) or 0.5
    d = _to_float(pred.get("odds"))
    edge = _to_float(pred.get("edge"))  # probability delta (e.g., 0.06 = +6 pp)
    signals = pred.get("signals") or {}
    s_total = signals.get("weighted_total")
    try:
        s_total = float(s_total)
    except Exception:
        s_total = 0.0

    # ---- Environment knobs ----
    KELLY_SCALER   = float(os.getenv("KELLY_SCALER", "0.5"))     # 0..1, default half-Kelly
    MAX_STAKE_PCT  = float(os.getenv("MAX_STAKE_PCT", "0.02"))   # cap (default 2% bankroll)
    EDGE_FLOOR     = float(os.getenv("EDGE_FLOOR", "0.01"))      # 1 pp edge gets full factor
    SOURCE_QUALITY_OT = float(os.getenv("SOURCE_QUALITY_OVERTIME", "1.0"))
    SOURCE_QUALITY_DEF = float(os.getenv("SOURCE_QUALITY_DEFAULT", "0.90"))

    # ---- Bail-outs ----
    if d is None or d <= 1.0:
        return 0.0
    if p <= 0.5 and edge is not None and edge <= 0:
        # no value and underdog confidence: don't stake
        return 0.0

    # ---- Kelly base ----
    k = _kelly_fraction(p, d)

    # ---- Alignment factor (0.5..1.0) ----
    if pick == "Over":
        align = max(0.0, s_total)
    else:
        align = max(0.0, -s_total)
    q = 0.5 + 0.5 * _clip01(align)   # 0.5 (weak) to 1.0 (strong & aligned)

    # ---- Edge factor (0..1) ----
    if edge is None:
        # derive from p vs implied prob to be safe
        imp = _implied_prob(d) or 0.0
        edge = p - imp if pick == "Over" else (1.0 - p) - (1.0 - imp)
    edge_factor = 0.0 if edge <= 0 else min(1.0, edge / max(1e-9, EDGE_FLOOR))

    # ---- Source quality boost (small) ----
    src_q = SOURCE_QUALITY_OT if pred.get("is_overtime_odds") else SOURCE_QUALITY_DEF

    # ---- Final fraction ----
    f = k * KELLY_SCALER * q * edge_factor * src_q
    f = max(0.0, min(MAX_STAKE_PCT, f))
    # Round to 4 decimals for storage
    return round(f, 4)
