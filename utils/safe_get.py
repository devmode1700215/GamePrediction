import sys
import time
import requests


def safe_get(url, headers=None, retries=20, delay=10, timeout=10):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()

            # Check if API returned a rate-limit error
            try:
                data = response.json()
                if 'errors' in data and 'requests' in data['errors']:
                    print("🚫 API rate limit reached. Exiting program.")
                    sys.exit()  # Stop entire script
            except ValueError:
                # Response is not JSON – skip this check
                pass

            return response

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                print("❌ All retries failed.")
                return None