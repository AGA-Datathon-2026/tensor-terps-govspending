import requests
import pyodbc

# 1) --- API SETTINGS ---
API_URL = "https://api.usaspending.gov/api/v2/references/toptier_agencies/"

# 2) --- SQL SERVER SETTINGS (Windows Authentication) ---
# This is the "Server name" you saw in SSMS: YASH
SERVER_NAME = "YASH"
DATABASE_NAME = "GovSpendingDB"

def fetch_agencies():
    """
    Calls the USAspending API and returns a list of agency dictionaries.
    """
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()  # if status != 200, this throws an error
    data = response.json()

    # API may return {"results": [...]} or directly a list
    agencies = data.get("results", data)
    return agencies

def connect_sql_server():
    """
    Creates a connection to SQL Server using Windows Authentication.
    """
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE_NAME};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def load_dim_agency(agencies):
    """
    Inserts agencies into dim_agency safely (no duplicates).
    """
    conn = connect_sql_server()
    cursor = conn.cursor()

    inserted = 0

    for agency in agencies:
        toptier_code = agency.get("toptier_code")
        agency_name = agency.get("agency_name")

        # Skip if something is missing (just being safe)
        if not toptier_code or not agency_name:
            continue

        # Insert ONLY if this toptier_code does not already exist
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dim_agency WHERE toptier_code = ?)
            BEGIN
                INSERT INTO dim_agency (toptier_code, agency_name)
                VALUES (?, ?)
            END
        """, (toptier_code, toptier_code, agency_name))

        inserted += 1

    conn.commit()  # saves all inserts
    cursor.close()
    conn.close()

    print(f"Finished. Attempted inserts: {inserted}")

if __name__ == "__main__":
    agencies = fetch_agencies()
    print(f"Fetched {len(agencies)} agencies from API.")
    load_dim_agency(agencies)
