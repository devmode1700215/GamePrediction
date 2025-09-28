# utils/cached_football_data.py
from functools import lru_cache
from typing import Optional, Dict, Any, List

# Import the originals
from .get_football_data import (
    get_match_odds as _get_match_odds,
    get_team_position as _get_team_position,
    get_team_form_and_goals as _get_team_form_and_goals,
    get_recent_goals as _get_recent_goals,
    get_team_injuries as _get_team_injuries,
    get_head_to_head as _get_head_to_head,
)

@lru_cache(maxsize=1024)
def get_match_odds(fixture_id: int) -> Dict[str, Any]:
    return _get_match_odds(fixture_id)

@lru_cache(maxsize=4096)
def get_team_position(team_id: int, league_id: int, season: int) -> Optional[int]:
    return _get_team_position(team_id, league_id, season)

@lru_cache(maxsize=4096)
def get_team_form_and_goals(team_id: int, league_id: int, season: int):
    return _get_team_form_and_goals(team_id, league_id, season)

@lru_cache(maxsize=4096)
def get_recent_goals(team_id: int) -> Dict[str, Any]:
    return _get_recent_goals(team_id)

@lru_cache(maxsize=4096)
def get_team_injuries(team_id: int, season: int) -> List[Dict[str, Any]]:
    return _get_team_injuries(team_id, season)

@lru_cache(maxsize=4096)
def get_head_to_head(home_id: int, away_id: int) -> List[Dict[str, Any]]:
    return _get_head_to_head(home_id, away_id)

