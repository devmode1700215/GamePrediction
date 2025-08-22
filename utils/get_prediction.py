# utils/recent_form.py
from typing import List, Dict, Any
from utils.supabaseClient import supabase

def _score_from_row(row: Dict[str, Any], team_name: str) -> int | None:
    """
    Returns the goals scored by team_name in this fixture, or None if unknown.
    Supports:
      - Separate 'results' table (preferred)
      - Optional 'results' JSON on matches (fallback)
    """
    is_home = row.get("home_team") == team_name
    is_away = row.get("away_team") == team_name

    # Prefer explicit scores from results table if present
    if "score_home" in row and "score_away" in row:
        if is_home:
            return row["score_home"]
        if is_away:
            return row["score_away"]

    # Fallback: matches.results JSON with keys score_home/score_away
    res = row.get("results")
    if isinstance(res, dict):
        sh = res.get("score_home")
        sa = res.get("score_away")
        if is_home and isinstance(sh, (int, float)):
            return int(sh)
        if is_away and isinstance(sa, (int, float)):
            return int(sa)

    return None

def fetch_recent_goals(team_name: str, limit: int = 5) -> List[int]:
    """
    Get recent 'limit' finished fixtures involving team_name
    and return a list of goals scored by that team in each.
    Looks for scores in the 'results' table; falls back to matches.results JSON if you have it.
    """
    if not team_name:
        return []

    # 1) Find recent fixture_ids from matches (team appears home or away), newest first
    m = (
        supabase.table("matches")
        .select("fixture_id, date, home_team, away_team")
        .or_(f"home_team.eq.{team_name},away_team.eq.{team_name}")
        .order("date", desc=True)
        .limit(50)  # grab a buffer; we’ll filter by finished ones below
        .execute()
    ).data or []

    if not m:
        return []

    fixture_ids = [row["fixture_id"] for row in m if row.get("fixture_id")]

    # 2) Pull results for those fixtures (if you have a 'results' table)
    results_map: Dict[int, Dict[str, Any]] = {}
    if fixture_ids:
        # chunk in batches of 500 to be safe
        CHUNK = 500
        for i in range(0, len(fixture_ids), CHUNK):
            chunk = fixture_ids[i:i+CHUNK]
            r = (
                supabase.table("results")
                .select("fixture_id, score_home, score_away")
                .in_("fixture_id", chunk)
                .execute()
            ).data or []
            for row in r:
                results_map[row["fixture_id"]] = row

    # 3) Compose rows with scores (prefer 'results'; optionally attach matches.results if you store it)
    enriched: List[Dict[str, Any]] = []
    # If your matches table also stores a results JSON column, pull it (optional)
    # To keep the selection light we didn’t include matches.results above. If you need it, re-query per fixture.

    for row in m:
        fid = row.get("fixture_id")
        # try results table first
        if fid in results_map:
            enriched.append({
                **row,
                "score_home": results_map[fid]["score_home"],
                "score_away": results_map[fid]["score_away"],
                "results": None,  # not used when we have explicit scores
            })
        else:
            # OPTIONAL: if you do have a 'results' JSON on matches, you can fetch it fixture-by-fixture or
            # expand the initial select to include it. For performance, we keep it simple:
            pass

    # 4) Take the most recent 'limit' fixtures with known scores and convert to goals-for
    goals: List[int] = []
    for row in enriched:
        g = _score_from_row(row, team_name)
        if g is not None:
            goals.append(g)
        if len(goals) >= limit:
            break

    return goals
