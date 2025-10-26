import requests
import time
import random
import os
from datetime import datetime, timedelta
from threading import Lock

BASE_URL = "https://api.tfl.gov.uk"
DATA_DIR = os.path.join(os.path.dirname(__file__), "../data")

# -----------------------------
# Rate Limiter
# -----------------------------
class RateLimiter:
    """Simple rate limiter for TfL API: max 50 requests per minute."""
    def __init__(self, max_calls=50, period=60):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = Lock()

    def wait(self):
        with self.lock:
            now = datetime.now()
            # Remove timestamps older than period
            self.calls = [t for t in self.calls if (now - t).total_seconds() < self.period]
            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now - self.calls[0]).total_seconds()
                if sleep_time > 0:
                    time.sleep(sleep_time)
            self.calls.append(datetime.now())

# Shared rate limiter instance
rate_limiter = RateLimiter(max_calls=49, period=60)

# -----------------------------
# Utilities
# -----------------------------
def ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def timestamped_filename(prefix, ext="csv"):
    ensure_data_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(DATA_DIR, f"{prefix}_{timestamp}.{ext}")

def safe_request(url, retries=3, backoff=2):
    """Make a GET request with retry, rate limiting, and basic 429 handling."""
    for attempt in range(retries):
        try:
            # Enforce TfL rate limit
            rate_limiter.wait()
            # Small random jitter
            time.sleep(random.uniform(0.05, 0.2))

            res = requests.get(url, timeout=10)

            # Handle rate limiting response from TfL
            if res.status_code == 429:
                retry_after = int(res.headers.get("Retry-After", backoff))
                print(f"⚠️ Rate limit hit. Retrying after {retry_after}s...")
                time.sleep(retry_after)
                continue

            res.raise_for_status()
            return res.json()

        except Exception as e:
            print(f"⚠️ Attempt {attempt + 1}/{retries} failed for {url}: {e}")
            time.sleep(backoff * (attempt + 1))
    return None

def get_bus_line_ids():
    url = f"{BASE_URL}/Line/Mode/bus"
    data = safe_request(url)
    if not data:
        return []
    return [line["id"] for line in data]
