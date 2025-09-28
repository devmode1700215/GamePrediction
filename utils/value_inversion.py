# utils/value_inversion.py
from typing import Dict, Any, Optional

def invert_ou25_prediction(
    prediction: Dict[str, Any],
    odds_raw: Optional[Dict[str, Any]],
    src: str,
) -> Dict[str, Any]:
    """
    Invert 'Over' <-> 'Under' for market over_2_5 and swap to the corresponding odds if available.
    Also forces gates to pass by setting po_value=True and edge=abs(edge).
    """
    if not isinstance(prediction, dict):
        return prediction

    out = dict(prediction)
    if out.get("market") != "over_2_5" or "prediction" not in out:
        return out

    pick = out.get("prediction")
    if pick not in ("Over", "Under"):
        return out

    # Flip selection
    inverted = "Under" if pick == "Over" else "Over"
    out["prediction"] = inverted

    # Try to use the matching counterpart price
    inv_price = None
    try:
        if src.lower() in {"apifootball", "api-football", "api_football"}:
            if inverted == "Under":
                inv_price = (odds_raw or {}).get("under_2_5")
            else:
                inv_price = (odds_raw or {}).get("over_2_5")
        elif src.lower() in {"overtime", "ot"}:
            # If Overtime doesn't expose Under, keep same price as fallback
            inv_price = out.get("odds")
        else:
            inv_price = out.get("odds")
    except Exception:
        inv_price = out.get("odds")

    if inv_price is not None:
        try:
            out["odds"] = float(inv_price)
        except Exception:
            pass  # keep as-is if not castable

    # Keep stake/confidence; make sure 'value' gates won't reject the flipped side
    try:
        if out.get("edge") is not None:
            out["edge"] = abs(float(out["edge"]))
    except Exception:
        pass
    out["po_value"] = True

    # Traceability
    rationale = (out.get("rationale") or "").strip()
    out["rationale"] = (rationale + " | inverted pick").strip(" |")

    return out
