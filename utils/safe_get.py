# utils/safe_get.py
import time
import random
import requests


def safe_get(
    url,
    headers=None,
    retries=6,           # ↓ fewer retries
    timeout=12,
    backoff_base=0.8,    # seconds
    backoff_factor=2.0,  # exponential
    max_sleep=30,        # cap per wait
):
    """
    GET with exponential backoff + jitter and gentle 429 handling.
    Returns `requests.Response` or None.
    """
    sleep_s = backoff_base

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)

            # 429 – rate limited
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep = min(float(retry_after), max_sleep)
                    except ValueError:
                        sleep = min(sleep_s, max_sleep)
                else:
                    sleep = min(sleep_s, max_sleep)

                time.sleep(sleep + random.random())
                sleep_s = min(sleep_s * backoff_factor, max_sleep)
                continue

            # Transient server errors -> retry with backoff
            if 500 <= resp.status_code < 600:
                time.sleep(min(sleep_s, max_sleep) + random.random())
                sleep_s = min(sleep_s * backoff_factor, max_sleep)
                continue

            # Some providers return JSON error envelopes
            try:
                data = resp.json()
                if isinstance(data, dict):
                    errs = data.get("errors") or {}
                    if isinstance(errs, dict) and "requests" in errs:
                        time.sleep(min(sleep_s, max_sleep) + random.random())
                        sleep_s = min(sleep_s * backoff_factor, max_sleep)
                        continue
            except ValueError:
                pass  # non-JSON body is fine

            resp.raise_for_status()
            return resp

        except requests.exceptions.RequestException:
            if attempt == retries:
                return None
            time.sleep(min(sleep_s, max_sleep) + random.random())
            sleep_s = min(sleep_s * backoff_factor, max_sleep)

    return None
