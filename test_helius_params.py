import requests
import time

API_KEY = "7f406621-4d98-4763-9895-d4277117e403"
TOKEN = "5q8oqdPgc5EJNso4C7k9j8HdUDeN7x3KnE6QJiTupump"

# Test 1: Get latest signature
url = f"https://api.helius.xyz/v0/addresses/{TOKEN}/transactions"
resp = requests.get(url, params={"api-key": API_KEY, "type": "SWAP", "limit": 1})
latest_sig = resp.json()[0]["signature"]
print(f"Latest signature: {latest_sig}")

# Test 2: Try 'after' parameter (get txs AFTER this signature)
print("\nTesting 'after' parameter...")
resp2 = requests.get(url, params={"api-key": API_KEY, "type": "SWAP", "after": latest_sig, "limit": 10})
print(f"Status: {resp2.status_code}")
print(f"Result: {resp2.text[:500]}")

# Test 3: Try 'since' parameter (timestamp-based)
print("\nTesting 'since' parameter...")
timestamp = int(time.time()) - 300  # 5 minutes ago
resp3 = requests.get(url, params={"api-key": API_KEY, "type": "SWAP", "since": timestamp, "limit": 10})
print(f"Status: {resp3.status_code}")
print(f"Result: {resp3.text[:500]}")