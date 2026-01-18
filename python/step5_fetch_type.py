import csv
import time
import random
from datetime import date
from typing import List, Tuple, Optional

import requests
import pyodbc
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================
# CONFIG
# ============================
SERVER_NAME = "YASH"
DATABASE_NAME = "GovSpendingDB"

YEARS_ALLOWED = list(range(2018, 2026))

USASPENDING_GEO_URL = "https://api.usaspending.gov/api/v2/search/spending_by_geography/"

# Resume controls (optional)
START_FROM_AGENCY = "Federal Permitting Improvement Steering Council"
START_FROM_YEAR = 2019

# Output log
FAILED_LOG_PATH = "failed_calls.csv"

# Valid state codes (US + territories + military)
VALID_STATE_CODES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM",
    "NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA",
    "WV","WI","WY",
    "PR","GU","VI","AS","MP",
    "AA","AE","AP"
}

# Retry behavior
MAX_ATTEMPTS = 6
BASE_SLEEP = 1.5  # seconds
REQUEST_DELAY_MIN = 0.25
REQUEST_DELAY_MAX = 0.55

# If an agency/year fails hard, sleep more before continuing
HARD_FAIL_COOLDOWN_MIN = 6
HARD_FAIL_COOLDOWN_MAX = 12


# ============================
# SQL CONNECTION
# ============================
def connect_sql_server():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE_NAME};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


# ============================
# REQUESTS SESSION (RETRY + POOL)
# ============================
def build_session() -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({
        "User-Agent": "gov-spending-explorer/1.0 (Yash; academic project)",
        "Accept": "application/json"
    })
    return session


# ============================
# FY DATE RANGE
# ============================
def fiscal_year_date_range(fy: int):
    start_date = date(fy - 1, 10, 1).isoformat()
    end_date = date(fy, 9, 30).isoformat()
    return start_date, end_date


# ============================
# FAILURE LOGGING
# ============================
def init_failed_log():
    # Create file with header if it doesn't exist
    try:
        with open(FAILED_LOG_PATH, "r", newline="", encoding="utf-8") as _:
            pass
    except FileNotFoundError:
        with open(FAILED_LOG_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["agency_name", "fy", "error"])


def log_failure(agency_name: str, fy: int, err: str):
    with open(FAILED_LOG_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([agency_name, fy, err[:300]])


# ============================
# API CALL (manual retry loop)
# ============================
def fetch_state_spend_place_of_performance(
    session: requests.Session,
    agency_name: str,
    fy: int
) -> List[Tuple[str, float]]:
    start_date, end_date = fiscal_year_date_range(fy)

    payload = {
        "filters": {
            "agencies": [
                {"type": "awarding", "tier": "subtier", "name": agency_name}
            ],
            "time_period": [{"start_date": start_date, "end_date": end_date}]
        },
        "scope": "place_of_performance",
        "geo_layer": "state"
    }

    last_err: Optional[Exception] = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = session.post(USASPENDING_GEO_URL, json=payload, timeout=90)

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else (BASE_SLEEP * attempt)
                time.sleep(sleep_s + random.uniform(0, 0.8))
                continue

            if resp.status_code != 200:
                last_err = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
                time.sleep((BASE_SLEEP * attempt) + random.uniform(0, 1.0))
                continue

            data = resp.json()
            rows = data.get("results", [])

            cleaned = []
            for row in rows:
                code = row.get("shape_code")
                amt = row.get("aggregated_amount")

                if not code or amt is None:
                    continue

                code = code.strip().upper()
                if code not in VALID_STATE_CODES:
                    continue

                cleaned.append((code, float(amt)))

            return cleaned

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            last_err = e
            time.sleep((BASE_SLEEP * attempt) + random.uniform(0, 1.2))

    raise requests.exceptions.ConnectionError(
        f"Failed after {MAX_ATTEMPTS} attempts: {agency_name} FY{fy}. Last error: {last_err}"
    )


# ============================
# MAIN LOAD
# ============================
def load_place_of_performance_state_data():
    init_failed_log()
    session = build_session()

    conn = connect_sql_server()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT agency_id, agency_name
        FROM dim_agency
        ORDER BY agency_name;
    """)
    agencies = cursor.fetchall()
    print(f"Agencies found: {len(agencies)}")

    # Resume logic
    started = (START_FROM_AGENCY == "")
    total_inserted = 0

    for agency_id, agency_name in agencies:
        if not started:
            if agency_name == START_FROM_AGENCY:
                started = True
            else:
                continue

        for fy in YEARS_ALLOWED:
            if fy < START_FROM_YEAR:
                continue

            # small pacing to reduce disconnects
            time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

            # Delete only this slice
            cursor.execute("""
                DELETE FROM fact_spend_agency_state_year
                WHERE agency_id = ? AND year = ?;
            """, (agency_id, fy))
            conn.commit()

            try:
                state_rows = fetch_state_spend_place_of_performance(session, agency_name, fy)
            except Exception as e:
                msg = str(e)
                print(f"[ERROR] {agency_name} FY{fy}: {msg}")
                log_failure(agency_name, fy, msg)

                # Cool down so the next calls don't also die
                time.sleep(random.uniform(HARD_FAIL_COOLDOWN_MIN, HARD_FAIL_COOLDOWN_MAX))
                continue  # IMPORTANT: continue, don't crash the whole run

            inserted = 0
            for state_code, amount in state_rows:
                try:
                    cursor.execute("""
                        INSERT INTO fact_spend_agency_state_year (year, agency_id, state_code, amount)
                        VALUES (?, ?, ?, ?);
                    """, (fy, agency_id, state_code, amount))
                    inserted += 1
                except pyodbc.IntegrityError:
                    # FK skip (should be rare)
                    continue

            conn.commit()
            total_inserted += inserted
            print(f"{agency_name} FY{fy}: inserted {inserted} state rows")

    cursor.close()
    conn.close()

    print("=" * 60)
    print(f"LOAD COMPLETE. Total rows inserted: {total_inserted}")
    print(f"Failures (if any) logged to: {FAILED_LOG_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    load_place_of_performance_state_data()
