import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

import sys, os
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PIPELINE_DIR = os.path.join(ROOT_DIR, "pipelines")

for path in [ROOT_DIR, PIPELINE_DIR]:
    if path not in sys.path:
        sys.path.append(path)

from pipelines.tfl_utils import RateLimiter, safe_request, get_bus_line_ids, BASE_URL, timestamped_filename

# Override shared rate limiter for 40 calls/min
rate_limiter = RateLimiter(max_calls=40, period=60)

def fetch_line_status(line_id):
    """Fetch bus line status details for a given line."""
    url = f"{BASE_URL}/Line/{line_id}/Status"
    data = safe_request(url)
    if not data:
        return []

    line_info = data[0]
    created = line_info.get("created")
    modified = line_info.get("modified")

    records = []
    for status in line_info.get("lineStatuses", []):
        validity = status.get("validityPeriods", [])
        from_date = validity[0].get("fromDate") if validity else None
        to_date = validity[0].get("toDate") if validity else None

        records.append({
            "lineId": line_info.get("id"),
            "lineName": line_info.get("name"),
            "statusSeverity": status.get("statusSeverity"),
            "statusSeverityDescription": status.get("statusSeverityDescription"),
            "reason": status.get("reason"),
            "fromDate": from_date,
            "toDate": to_date,
            "created": created,
            "modified": modified
        })

    return records


def fetch_all_bus_statuses(max_workers=5, save_csv=True):
    """Fetch status for all TfL bus lines (40 req/min limit)."""
    ids = get_bus_line_ids()
    all_records = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_line_status, lid): lid for lid in ids}
        for future in as_completed(futures):
            line_id = futures[future]
            try:
                result = future.result()
                all_records.extend(result)
            except Exception as e:
                print(f"⚠️ Error fetching {line_id}: {e}")

    df = pd.DataFrame(all_records)
    if save_csv:
        file_path = timestamped_filename("bus_status")
        df.to_csv(file_path, index=False)
        print(f"✅ Saved {len(df)} records to {file_path}")
        return file_path

    return df


if __name__ == "__main__":
    fetch_all_bus_statuses()
