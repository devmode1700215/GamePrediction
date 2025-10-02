# utils/safe_get.py
from __future__ import annotations
import logging
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse

import requests

logger = logging.getLogger(__name__)

def _merge_query(url: str, params: Optional[Dict[str, Any]]) -> str:
    """
    Merge params into the URL's query string. Values that are None are dropped.
    This lets us support both styles:
      safe_get("https://x/api?date=2025-10-02", params={"sport":"soccer"})
      safe_get("https://x/api", params={"date":"2025-10-02","sport":"soccer"})
    """
    if not params:
        return url
    try:
        clean = {k: v for k, v in params.items() if v is not None and v != ""}
        if not clean:
            return url
        parts = urlparse(url)
        current = dict(parse_qsl(parts.query, keep_blank_values=True))
        current.update({str(k): str(v) for k, v in clean.items()})
        new_query = urlencode(current)
        new_url = urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))
        return new_url
    except Exception:
        # If anything goes wrong, just fall back to original URL
        return url

def safe_get(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    *,
    params: Optional[Dict[str, Any]] = None,   # NEW: support params kwarg
    timeout: int = 20,
    retries: int = 2,
    backoff: float = 0.7,
) -> Optional[requests.Response]:
    """
    Robust GET with optional query params, retry on 429/5xx, and logging.
    Returns `requests.Response` or None if all retries fail.
    """
    # Merge params into URL if provided (keeps backward compatibility)
    url = _merge_query(url, params)

    attempt = 0
    last_err: Optional[Exception] = None
    while attempt <= retries:
        try:
            resp = requests.get(url, headers=headers or {}, timeout=timeout)
            sc = resp.status_code

            if sc == 429 or 500 <= sc < 600:
                # Backoff and retry
                wait = backoff * (2 ** attempt)
                logger.info(f"[safe_get] {sc} from {url} — retrying in {wait:.1f}s (attempt {attempt+1}/{retries})")
                time.sleep(wait)
                attempt += 1
                continue

            # 2xx / 4xx (non-retriable) return as-is
            return resp

        except requests.RequestException as e:
            last_err = e
            wait = backoff * (2 ** attempt)
            logger.info(f"[safe_get] network error on {url}: {e} — retrying in {wait:.1f}s (attempt {attempt+1}/{retries})")
            time.sleep(wait)
            attempt += 1

    logger.error(f"[safe_get] failed after {retries+1} attempts on {url}: {last_err}")
    return None
