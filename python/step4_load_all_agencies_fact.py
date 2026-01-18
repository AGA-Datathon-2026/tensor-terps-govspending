import requests
import pyodbc

SERVER_NAME = "YASH"
DATABASE_NAME = "GovSpendingDB"
YEARS_ALLOWED = set(range(2018, 2026))

def connect_sql_server():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE_NAME};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def fetch_yearly_spend_for_agency(toptier_code):
    url = f"https://api.usaspending.gov/api/v2/agency/{toptier_code}/budgetary_resources/"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("agency_data_by_year", [])

def clean_yearly_rows(rows):
    cleaned = []
    for r in rows:
        year = r.get("fiscal_year")
        amount = r.get("agency_total_obligated")

        if year not in YEARS_ALLOWED:
            continue
        if amount is None:
            continue

        cleaned.append((year, float(amount)))
    return cleaned

def load_all_agencies():
    conn = connect_sql_server()
    cursor = conn.cursor()

    # 1) Get all agencies
    cursor.execute("SELECT agency_id, toptier_code FROM dim_agency")
    agencies = cursor.fetchall()

    print(f"Loading data for {len(agencies)} agencies...")

    for agency_id, toptier_code in agencies:
        yearly_raw = fetch_yearly_spend_for_agency(toptier_code)
        yearly_clean = clean_yearly_rows(yearly_raw)

        # Clear existing data for this agency
        cursor.execute(
            "DELETE FROM fact_spend_agency_year WHERE agency_id = ?",
            (agency_id,)
        )

        # Insert clean rows
        for year, amount in yearly_clean:
            cursor.execute(
                "INSERT INTO fact_spend_agency_year (year, agency_id, amount) VALUES (?, ?, ?)",
                (year, agency_id, amount)
            )

        print(f"Agency {toptier_code}: inserted {len(yearly_clean)} rows")

    conn.commit()
    cursor.close()
    conn.close()
    print("Finished loading all agencies.")

if __name__ == "__main__":
    load_all_agencies()
