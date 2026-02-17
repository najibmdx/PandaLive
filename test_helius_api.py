#!/usr/bin/env python3
"""
Quick Helius API test to diagnose polling issues.
"""
import os
import requests
import time

# Your API key
HELIUS_API_KEY = "7f406621-4d98-4763-9895-d4277117e403"

# Token that's hanging
TOKEN_CA = "E5HEAjf13rwKBfgPQgtsoQM5Q8JroiqeEf9DLDGDpump"

print("="*80)
print("HELIUS API DIAGNOSTIC TEST")
print("="*80)
print(f"API Key: {HELIUS_API_KEY[:20]}...")
print(f"Token: {TOKEN_CA}")
print()

# Test 1: Check API key validity
print("[TEST 1] Checking API key validity...")
url = f"https://api.helius.xyz/v0/addresses/{TOKEN_CA}/transactions"
params = {
    "api-key": HELIUS_API_KEY,
    "limit": 10
}

try:
    print(f"Requesting: {url}")
    print(f"Params: {params}")
    
    response = requests.get(url, params=params, timeout=30)
    
    print(f"Status Code: {response.status_code}")
    print(f"Response Headers: {dict(response.headers)}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ SUCCESS - Received {len(data)} transactions")
        
        if len(data) > 0:
            print("\nFirst transaction sample:")
            tx = data[0]
            print(f"  Signature: {tx.get('signature', 'N/A')[:20]}...")
            print(f"  Timestamp: {tx.get('timestamp', 'N/A')}")
            print(f"  Type: {tx.get('type', 'N/A')}")
        else:
            print("⚠️  No transactions found for this token")
            print("   This could mean:")
            print("   - Token is very new/inactive")
            print("   - Token address is wrong")
            print("   - Helius hasn't indexed it yet")
    
    elif response.status_code == 401:
        print("❌ AUTHENTICATION FAILED")
        print("   Your API key is invalid or expired")
        print(f"   Response: {response.text[:200]}")
    
    elif response.status_code == 429:
        print("❌ RATE LIMIT HIT")
        print("   You're being rate limited by Helius")
        print(f"   Response: {response.text[:200]}")
    
    else:
        print(f"❌ ERROR {response.status_code}")
        print(f"   Response: {response.text[:200]}")

except requests.exceptions.Timeout:
    print("❌ TIMEOUT - Helius took too long to respond")
    print("   Network issue or Helius is down")

except requests.exceptions.ConnectionError as e:
    print("❌ CONNECTION ERROR")
    print(f"   {e}")
    print("   Check your internet connection")

except Exception as e:
    print(f"❌ UNEXPECTED ERROR: {type(e).__name__}")
    print(f"   {e}")

print()
print("="*80)
print("DIAGNOSTIC COMPLETE")
print("="*80)
