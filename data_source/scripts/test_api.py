"""Quick sanity test for the mock orders API."""
import os, time, requests
from dotenv import load_dotenv

load_dotenv()
BASE  = "http://localhost:8080"
TOKEN = os.getenv("MOCK_API_TOKEN", "retailpulse-dev-token")
H     = {"Authorization": f"Bearer {TOKEN}"}

# 1. health
print(requests.get(f"{BASE}/health").json())

# 2. paginated list
r = requests.get(f"{BASE}/api/v1/orders", params={"limit": 5, "offset": 0}, headers=H)
print(r.status_code, r.json()["total"], len(r.json()["data"]))

# 3. incremental
since = "2026-01-01T00:00:00Z"
r = requests.get(f"{BASE}/api/v1/orders", params={"updated_since": since, "limit": 3}, headers=H)
print("incremental:", r.status_code, len(r.json().get("data", [])))

# 4. trigger rate limit
codes = []
for _ in range(70):
    codes.append(requests.get(f"{BASE}/api/v1/orders", headers=H).status_code)
print("429s seen:", codes.count(429))