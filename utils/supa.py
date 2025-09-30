# utils/supa.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Union

# Reuse your existing Supabase client (adjust the import if your client lives elsewhere)
try:
    from utils.supabase_client import supabase  # <-- common location in your repo
except Exception as e:
    raise ImportError(
        "Could not import Supabase client. Ensure you have utils/supabase_client.py "
        "that exposes `supabase`."
    ) from e


Rows = Union[Dict[str, Any], List[Dict[str, Any]]]


def _as_list(rows: Rows) -> List[Dict[str, Any]]:
    if rows is None:
        return []
    if isinstance(rows, dict):
        return [rows]
    return list(rows)


def postgrest_upsert(
    table: str,
    rows: Rows,
    on_conflict: Optional[Union[str, Sequence[str]]] = None,
    ignore_duplicates: bool = False,
) -> Any:
    """
    Minimal wrapper expected by your utils.insert_match module.
    Works with supabase-py v1/v2:
      - v2: upsert(rows, on_conflict=..., ignore_duplicates=...)
      - v1: upsert(rows) without kwargs
    """
    payload = _as_list(rows)
    q = supabase.table(table)

    # Try modern signature first
    try:
        if on_conflict is not None:
            res = q.upsert(payload, on_conflict=on_conflict, ignore_duplicates=ignore_duplicates).execute()
        else:
            res = q.upsert(payload, ignore_duplicates=ignore_duplicates).execute()
        return res
    except TypeError:
        # Fallback for older clients that don't support kwargs
        res = q.upsert(payload).execute()
        return res


# (Optional) tiny helpers if other modules expect them; harmless to keep:

def postgrest_insert(table: str, rows: Rows) -> Any:
    payload = _as_list(rows)
    return supabase.table(table).insert(payload).execute()


def postgrest_select(table: str, columns: str = "*", **filters: Any) -> Any:
    q = supabase.table(table).select(columns)
    for k, v in filters.items():
        q = q.eq(k, v)
    return q.execute()
  
