import requests

URL = "https://api.usaspending.gov/api/v2/references/toptier_agencies/"

response = requests.get(URL, timeout=30)

print("Status code:", response.status_code)   # 200 means success

data = response.json()  # convert response body (JSON) into Python dict/list

# Some APIs return {"results": [...]}, others return a list directly.
agencies = data.get("results", data)

print("Type of agencies:", type(agencies))
print("Number of agencies:", len(agencies))

print("\nFirst agency record (sample):")
print(agencies[0])

print("\nKeys inside one agency record:")
print(list(agencies[0].keys()))
