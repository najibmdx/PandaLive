#!/usr/bin/env python3
"""
PANDA LIVE PIPELINE DIAGNOSTIC
Traces every step from Helius → Display to find where the failure is.
"""

import sys
import os

# Add PANDA Live to path
sys.path.insert(0, '.')

from panda_live.integrations.helius_client import HeliusClient
from panda_live.core.whale_detection import WhaleDetector
from panda_live.core.time_windows import TimeWindowManager
from panda_live.models.wallet_state import WalletState

print("="*80)
print("PANDA LIVE PIPELINE DIAGNOSTIC")
print("="*80)

# Configuration
API_KEY = os.environ.get("HELIUS_API_KEY", "7f406621-4d98-4763-9895-d4277117e403")
TOKEN_CA = "5q8oqdPgc5EJNso4C7k9j8HdUDeN7x3KnE6QJiTupump"

print(f"\nToken: {TOKEN_CA}")
print(f"API Key: {API_KEY[:20]}...")

# Step 1: Test Helius Client
print("\n" + "="*80)
print("STEP 1: HELIUS CLIENT")
print("="*80)

helius = HeliusClient(api_key=API_KEY)
flows = helius.poll_and_parse(TOKEN_CA)

print(f"✓ Helius returned {len(flows)} FlowEvents")

if len(flows) == 0:
    print("❌ FAILURE: No flows returned from Helius!")
    print("   This is the root cause - nothing to process downstream")
    sys.exit(1)

# Show sample flows
print(f"\nSample flows (first 3):")
for i, flow in enumerate(flows[:3]):
    print(f"  [{i+1}] Wallet: {flow.wallet[:8]}... | {flow.direction.upper()} | {flow.amount_sol:.2f} SOL | Time: {flow.timestamp}")

# Step 2: Test Whale Detection
print("\n" + "="*80)
print("STEP 2: WHALE DETECTION")
print("="*80)

whale_detector = WhaleDetector()
time_window_mgr = TimeWindowManager()

whale_count = 0
test_wallet = WalletState(address=flows[0].wallet)

for flow in flows[:10]:  # Test first 10
    time_window_mgr.add_flow(test_wallet, flow)
    whale_events = whale_detector.check_thresholds(test_wallet, flow)
    whale_count += len(whale_events)
    
    if whale_events:
        print(f"✓ Flow {flow.amount_sol:.2f} SOL → Triggered {len(whale_events)} whale event(s)")
        for we in whale_events:
            print(f"  - {we.event_type}: {we.amount_sol:.2f} SOL")

if whale_count == 0:
    print("⚠️  WARNING: No whale events triggered!")
    print("   Possible reasons:")
    print("   - Flows too small (< 10 SOL single, < 25 SOL 5min, < 50 SOL 15min)")
    print("   - Whale thresholds already fired (latched)")
else:
    print(f"\n✓ Total whale events: {whale_count}")

# Step 3: Check Whale Thresholds
print("\n" + "="*80)
print("STEP 3: WHALE THRESHOLD ANALYSIS")
print("="*80)

from panda_live.config.thresholds import (
    WHALE_SINGLE_TX_SOL,
    WHALE_CUM_5MIN_SOL,
    WHALE_CUM_15MIN_SOL
)

print(f"Configured thresholds:")
print(f"  - Single TX: {WHALE_SINGLE_TX_SOL} SOL")
print(f"  - 5min cumulative: {WHALE_CUM_5MIN_SOL} SOL")
print(f"  - 15min cumulative: {WHALE_CUM_15MIN_SOL} SOL")

# Analyze actual flow amounts
flow_amounts = [f.amount_sol for f in flows]
print(f"\nFlow statistics from {len(flows)} flows:")
print(f"  - Max flow: {max(flow_amounts):.2f} SOL")
print(f"  - Min flow: {min(flow_amounts):.2f} SOL")
print(f"  - Average: {sum(flow_amounts)/len(flow_amounts):.2f} SOL")

flows_above_10 = [f for f in flow_amounts if f >= WHALE_SINGLE_TX_SOL]
print(f"  - Flows ≥ {WHALE_SINGLE_TX_SOL} SOL: {len(flows_above_10)} ({len(flows_above_10)/len(flows)*100:.1f}%)")

if len(flows_above_10) == 0:
    print("\n❌ CRITICAL: NO flows meet whale threshold!")
    print(f"   Largest flow is {max(flow_amounts):.2f} SOL, but threshold is {WHALE_SINGLE_TX_SOL} SOL")
    print("   This explains why no whale events are triggered")

# Step 4: Check Signature Deduplication
print("\n" + "="*80)
print("STEP 4: SIGNATURE DEDUPLICATION CHECK")
print("="*80)

signatures = [f.signature for f in flows]
unique_sigs = set(signatures)

print(f"Total flows: {len(flows)}")
print(f"Unique signatures: {len(unique_sigs)}")
print(f"Duplicates: {len(flows) - len(unique_sigs)}")

if len(unique_sigs) < len(flows):
    print("⚠️  WARNING: Duplicate signatures detected in single poll!")

# Step 5: Check Timestamps
print("\n" + "="*80)
print("STEP 5: TIMESTAMP ANALYSIS")
print("="*80)

timestamps = [f.timestamp for f in flows]
min_ts = min(timestamps)
max_ts = max(timestamps)
time_range = max_ts - min_ts

import time as time_module
current_time = int(time_module.time())
age_of_newest = current_time - max_ts

print(f"Timestamp range:")
print(f"  - Oldest: {min_ts} ({(current_time - min_ts)}s ago)")
print(f"  - Newest: {max_ts} ({age_of_newest}s ago)")
print(f"  - Span: {time_range}s ({time_range/60:.1f} minutes)")

if age_of_newest > 300:
    print(f"\n⚠️  WARNING: Newest transaction is {age_of_newest}s ({age_of_newest/60:.1f} min) old!")
    print("   Token may have gone quiet, or Helius data is stale")

# Step 6: Summary
print("\n" + "="*80)
print("DIAGNOSTIC SUMMARY")
print("="*80)

issues_found = []

if len(flows) == 0:
    issues_found.append("❌ Helius returning 0 flows")
if whale_count == 0:
    issues_found.append("❌ No whale events triggered")
if len(flows_above_10) == 0:
    issues_found.append("❌ No flows meet whale threshold")
if age_of_newest > 60:
    issues_found.append(f"⚠️  Newest data is {age_of_newest}s old")

if issues_found:
    print("ISSUES FOUND:")
    for issue in issues_found:
        print(f"  {issue}")
else:
    print("✓ No obvious issues detected")
    print("  Problem may be in state machine or display rendering")

print("\n" + "="*80)
