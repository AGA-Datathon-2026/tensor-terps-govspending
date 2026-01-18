import requests
import pyodbc

TOPTIER_CODE = "247"
AGENCY_ID = 1

SERVER_NAME = "YASH"
DATABASE_NAME = "GovSpendingDB"

URL = f"https://api.usaspending.gov/api/v2/agency/{TOPTIER_CODE}/budgetary_resources/"

YEARS_ALLOWED = set(range(2018, 2026))  # 2018–2025

def connect_sql_server():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE_NAME};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def fetch_and_clean_yearly_spend():
    response = requests.get(URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    rows = data.get("agency_data_by_year", [])

    cleaned = []
    for r in rows:
        year = r.get("fiscal_year")
        amount = r.get("agency_total_obligated")

        # Rule 1: only years 2018–2025
        if year not in YEARS_ALLOWED:
            continue

        # Rule 2: skip nulls
        if amount is None:
            continue

        cleaned.append((year, float(amount)))

    return cleaned

def load_fact_rows(rows):
    conn = connect_sql_server()
    cursor = conn.cursor()

    # For this ONE agency, delete existing rows first (safe for reruns)
    cursor.execute("DELETE FROM fact_spend_agency_year WHERE agency_id = ?", (AGENCY_ID,))

    # Insert rows
    for (year, amount) in rows:
        cursor.execute(
            "INSERT INTO fact_spend_agency_year (year, agency_id, amount) VALUES (?, ?, ?)",
            (year, AGENCY_ID, amount)
        )

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    rows = fetch_and_clean_yearly_spend()
    print("Clean rows (year, amount):", rows)

    load_fact_rows(rows)
    print(f"Loaded {len(rows)} rows for agency_id={AGENCY_ID} (toptier_code={TOPTIER_CODE}).")
