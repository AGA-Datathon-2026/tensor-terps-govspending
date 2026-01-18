import requests
import json

TOPTIER_CODE = "247"

URL = f"https://api.usaspending.gov/api/v2/agency/{TOPTIER_CODE}/budgetary_resources/"

response = requests.get(URL, timeout=30)

print("Status code:", response.status_code)

data = response.json()

# Print top-level keys so we understand structure
print("\nTop-level keys in response:")
print(list(data.keys()))

# Print a small sample of yearly data
yearly_data = data.get("agency_data_by_year", [])

print("\nNumber of yearly records:", len(yearly_data))

print("\nFirst yearly record:")
print(json.dumps(yearly_data[0], indent=2))
