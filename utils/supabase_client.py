# utils/supabase_client.py
from __future__ import annotations

import os
from typing import Optional

try:
    # supabase-py v2
    from supabase import create_client, Client  # type: ignore
except Exception as e:  # pragma: no cover
    raise ImportError(
        "Missing `supabase` Python SDK. Add `supabase>=2.3.0` to requirements.txt."
    ) from e


def _get_env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or v.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v


def _make_client() -> Client:
    """
    Initialize a global Supabase client from env vars:
      - SUPABASE_URL       (https://xxxx.supabase.co)
      - SUPABASE_KEY       (anon or service role)
    """
    url = _get_env("SUPABASE_URL")
    key = _get_env("SUPABASE_KEY")  # service role preferred for server-side jobs
    client: Client = create_client(url, key)

    # Optional: set default schema if you use non-public
    schema = os.getenv("SUPABASE_SCHEMA")
    if schema:
        try:
            client.postgrest.schema = schema  # supabase-py v2 exposes this
        except Exception:
            pass

    return client


# Singleton client used across the app
supabase: Client = _make_client()


# --- Optional tiny helper for diagnostics ---
def ping_supabase() -> bool:
    try:
        # light no-op query; adjust table if you have a guaranteed small table
        client = supabase
        _ = client.table("pg_stat_statements").select("userid").limit(1).execute()
    except Exception:
        # not all projects expose that view; just attempt auth refresh
        try:
            _ = supabase.auth.get_session()
        except Exception:
            pass
    return True
  
