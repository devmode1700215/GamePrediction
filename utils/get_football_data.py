def get_match_odds(fixture_id):
    url = f"{BASE_URL}/odds?fixture={fixture_id}"
    http = safe_get(url, headers=HEADERS)
    if http is None:
        # network/api issue: return empty odds
        return {
            "home_win": None, "draw": None, "away_win": None,
            "btts_yes": None, "btts_no": None,
            "over_2_5": None, "under_2_5": None,
        }

    try:
        payload = http.json()
    except Exception:
        return {
            "home_win": None, "draw": None, "away_win": None,
            "btts_yes": None, "btts_no": None,
            "over_2_5": None, "under_2_5": None,
        }

    # API-Sports structure: response -> [ { bookmakers: [ { name, bets: [...] } ] } ... ]
    entries = payload.get("response", []) or []

    odds = {
        "home_win": None, "draw": None, "away_win": None,
        "btts_yes": None, "btts_no": None,
        "over_2_5": None, "under_2_5": None,
    }

    PREFERRED = ["Pinnacle", "bet365", "Bwin", "William Hill", "Unibet", "Betfair"]

    def extract_from_bookmaker(bm, out):
        """Fill any available markets from this bookmaker into out dict."""
        bets = bm.get("bets", []) or []
        for market in bets:
            mname = (market.get("name") or "").strip().lower()
            vals = market.get("values", []) or []

            if mname == "match winner":
                for v in vals:
                    val = (v.get("value") or "").strip().lower()
                    odd = v.get("odd")
                    try:
                        oddf = float(odd)
                    except (TypeError, ValueError):
                        continue
                    if val == "home":
                        out["home_win"] = out["home_win"] or oddf
                    elif val == "draw":
                        out["draw"] = out["draw"] or oddf
                    elif val == "away":
                        out["away_win"] = out["away_win"] or oddf

            elif mname in ("goals over/under", "over/under"):
                for v in vals:
                    val = (v.get("value") or "").strip().lower()
                    odd = v.get("odd")
                    try:
                        oddf = float(odd)
                    except (TypeError, ValueError):
                        continue
                    if val == "over 2.5":
                        out["over_2_5"] = out["over_2_5"] or oddf
                    elif val == "under 2.5":
                        out["under_2_5"] = out["under_2_5"] or oddf

            elif mname in ("both teams to score", "btts", "both teams score"):
                for v in vals:
                    val = (v.get("value") or "").strip().lower()
                    odd = v.get("odd")
                    try:
                        oddf = float(odd)
                    except (TypeError, ValueError):
                        continue
                    if val == "yes":
                        out["btts_yes"] = out["btts_yes"] or oddf
                    elif val == "no":
                        out["btts_no"] = out["btts_no"] or oddf

    # Pass 1: try preferred bookmakers, in order
    for entry in entries:
        for bm in entry.get("bookmakers", []) or []:
            if bm.get("name") in PREFERRED:
                tmp = odds.copy()
                extract_from_bookmaker(bm, tmp)
                if any(tmp.values()):
                    odds = tmp
                    return odds  # got something from a preferred bookie

    # Pass 2: fall back to first bookmaker that has any prices
    for entry in entries:
        for bm in entry.get("bookmakers", []) or []:
            tmp = odds.copy()
            extract_from_bookmaker(bm, tmp)
            if any(tmp.values()):
                odds = tmp
                return odds

    # Nothing found anywhere
    return odds
