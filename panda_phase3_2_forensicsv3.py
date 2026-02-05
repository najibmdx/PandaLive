#!/usr/bin/env python3
"""
panda_phase3_2_forensicsv3.py (READ-ONLY)

Goal:
Compare whale_events in DB vs recomputed events from wallet_token_flow.
- Use window values: '24h', '7d', 'lifetime' (matching DB schema)
- Keep event_type strings canonical
- Use INTEGER epoch seconds for event_time (no datetime objects in keys)
- Phase 3.2 thresholds in lamports

Expected outcome: common > 0 after fixes
"""

import sqlite3
import sys
import argparse
from collections import defaultdict
from typing import Dict, Tuple, Set

# Canonical event type strings
EVENT_WHALE_TX_BUY = "WHALE_TX_BUY"
EVENT_WHALE_TX_SELL = "WHALE_TX_SELL"
EVENT_WHALE_CUM_24H_BUY = "WHALE_CUM_24H_BUY"
EVENT_WHALE_CUM_24H_SELL = "WHALE_CUM_24H_SELL"
EVENT_WHALE_CUM_7D_BUY = "WHALE_CUM_7D_BUY"
EVENT_WHALE_CUM_7D_SELL = "WHALE_CUM_7D_SELL"

# Window values matching DB schema
WINDOW_24H = "24h"
WINDOW_7D = "7d"
WINDOW_LIFETIME = "lifetime"

# Phase 3.2 thresholds (in lamports)
WHALE_SINGLE_TX_THRESHOLD = 10_000_000_000  # 10 SOL
WHALE_CUM_24H_THRESHOLD = 50_000_000_000    # 50 SOL
WHALE_CUM_7D_THRESHOLD = 200_000_000_000    # 200 SOL

# Time windows in seconds
WINDOW_24H_SECONDS = 86400   # 24 hours
WINDOW_7D_SECONDS = 604800   # 7 days


def discover_schema(conn: sqlite3.Connection):
    """
    Discover and print the whale_events table schema.
    """
    print("=" * 80)
    print("SCHEMA DISCOVERY: whale_events")
    print("=" * 80)
    
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='whale_events'")
    if not cursor.fetchone():
        print("\n✗ ERROR: Table 'whale_events' does not exist in database")
        print("  Available tables:")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = cursor.fetchall()
        if tables:
            for table in tables:
                print(f"    - {table[0]}")
        else:
            print("    (no tables found)")
        raise ValueError("Required table 'whale_events' not found")
    
    cursor.execute("PRAGMA table_info(whale_events)")
    columns = cursor.fetchall()
    
    if not columns:
        print("\n✗ ERROR: Table 'whale_events' has no columns (corrupted?)")
        raise ValueError("Table 'whale_events' appears to be corrupted")
    
    print("\nColumns:")
    for col in columns:
        print(f"  {col[1]:20s} {col[2]:15s} {'NOT NULL' if col[3] else ''} {'PK' if col[5] else ''}")
    
    # SANITY CHECK: Show distinct window values in DB
    print("\n" + "=" * 80)
    print("SANITY CHECK: Distinct window values in whale_events")
    print("=" * 80)
    cursor.execute("SELECT DISTINCT window FROM whale_events ORDER BY window")
    windows = cursor.fetchall()
    print(f"\nFound {len(windows)} distinct window value(s):")
    for w in windows:
        cursor.execute("SELECT COUNT(*) FROM whale_events WHERE window = ?", (w[0],))
        result = cursor.fetchone()
        count = result[0] if result else 0
        print(f"  '{w[0]}': {count:,} events")
    print()


def load_baseline_events(conn: sqlite3.Connection) -> Dict[Tuple, Tuple[int, int]]:
    """
    Load baseline whale_events from DB.
    Key: (wallet, window, event_type, event_time_int, flow_ref)
    Value: (sol_amount_lamports, supporting_flow_count)
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT wallet, window, event_type, event_time, flow_ref, sol_amount_lamports, supporting_flow_count
        FROM whale_events
        ORDER BY event_time, wallet, event_type
    """)
    
    events = {}
    min_event_time = None
    max_event_time = None
    
    for row in cursor.fetchall():
        wallet, window, event_type, event_time_int, flow_ref, amount, supp_count = row
        
        # Track min/max for sanity check
        if min_event_time is None or event_time_int < min_event_time:
            min_event_time = event_time_int
        if max_event_time is None or event_time_int > max_event_time:
            max_event_time = event_time_int
        
        # Key uses INTEGER epoch seconds directly (no datetime conversion)
        key = (wallet, window, event_type, event_time_int, flow_ref)
        events[key] = (amount, supp_count)
    
    # Sanity check: show epoch range
    if min_event_time is not None:
        from datetime import datetime
        print(f"Event time range: {min_event_time} → {max_event_time} "
              f"({datetime.utcfromtimestamp(min_event_time).strftime('%Y-%m-%d %H:%M:%S')} → "
              f"{datetime.utcfromtimestamp(max_event_time).strftime('%Y-%m-%d %H:%M:%S')} UTC)")
    
    return events


def recompute_whale_events(conn: sqlite3.Connection) -> Dict[Tuple, Tuple[int, int]]:
    """
    Recompute whale events from wallet_token_flow using Phase 3.2 thresholds.
    
    Window values:
    - Single-tx events -> window = 'lifetime'
    - Cumulative 24h events -> window = '24h'
    - Cumulative 7d events -> window = '7d'
    
    Thresholds (lamports):
    - Single TX: 10,000,000,000 (10 SOL)
    - Cum 24h: 50,000,000,000 (50 SOL)
    - Cum 7d: 200,000,000,000 (200 SOL)
    """
    cursor = conn.cursor()
    
    # Check if wallet_token_flow table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='wallet_token_flow'")
    if not cursor.fetchone():
        print("\n✗ ERROR: Table 'wallet_token_flow' does not exist in database")
        raise ValueError("Required table 'wallet_token_flow' not found")
    
    # Load all flows from wallet_token_flow
    cursor.execute("""
        SELECT scan_wallet, block_time, sol_direction, sol_amount_lamports, signature
        FROM wallet_token_flow
        ORDER BY scan_wallet, block_time, rowid
    """)
    
    flows = []
    for row in cursor.fetchall():
        wallet, block_time, direction, amount_lamports, signature = row
        
        # Normalize direction: accept 'buy'/'sell' case-insensitive
        direction_norm = direction.lower()
        if direction_norm not in ['buy', 'sell']:
            continue
        
        # Convert to uppercase for consistency with event types
        direction_upper = direction_norm.upper()
        
        flows.append({
            'wallet': wallet,
            'block_time': block_time,  # INTEGER epoch seconds
            'direction': direction_upper,
            'amount_lamports': abs(amount_lamports),
            'signature': signature
        })
    
    print(f"\nLoaded {len(flows):,} flows from wallet_token_flow")
    
    events = {}
    
    # Group flows by wallet for cumulative calculations
    flows_by_wallet = defaultdict(list)
    for flow in flows:
        flows_by_wallet[flow['wallet']].append(flow)
    
    # Process each wallet
    for wallet, wallet_flows in flows_by_wallet.items():
        # Flows are already sorted by block_time, rowid from query
        
        for i, anchor_flow in enumerate(wallet_flows):
            T = anchor_flow['block_time']  # event_time
            S = anchor_flow['signature']   # flow_ref
            
            # Single-tx whale events
            if anchor_flow['amount_lamports'] >= WHALE_SINGLE_TX_THRESHOLD:
                event_type = EVENT_WHALE_TX_BUY if anchor_flow['direction'] == 'BUY' else EVENT_WHALE_TX_SELL
                key = (wallet, WINDOW_LIFETIME, event_type, T, S)
                events[key] = (anchor_flow['amount_lamports'], 1)
            
            # Cumulative 24h events
            cutoff_24h = T - WINDOW_24H_SECONDS
            buy_sum_24h = 0
            sell_sum_24h = 0
            buy_count_24h = 0
            sell_count_24h = 0
            
            # Sum all flows in [T-86400, T] inclusive
            for prev_flow in wallet_flows[:i+1]:
                if prev_flow['block_time'] >= cutoff_24h:
                    if prev_flow['direction'] == 'BUY':
                        buy_sum_24h += prev_flow['amount_lamports']
                        buy_count_24h += 1
                    else:
                        sell_sum_24h += prev_flow['amount_lamports']
                        sell_count_24h += 1
            
            if buy_sum_24h >= WHALE_CUM_24H_THRESHOLD:
                key = (wallet, WINDOW_24H, EVENT_WHALE_CUM_24H_BUY, T, S)
                events[key] = (buy_sum_24h, buy_count_24h)
            
            if sell_sum_24h >= WHALE_CUM_24H_THRESHOLD:
                key = (wallet, WINDOW_24H, EVENT_WHALE_CUM_24H_SELL, T, S)
                events[key] = (sell_sum_24h, sell_count_24h)
            
            # Cumulative 7d events
            cutoff_7d = T - WINDOW_7D_SECONDS
            buy_sum_7d = 0
            sell_sum_7d = 0
            buy_count_7d = 0
            sell_count_7d = 0
            
            # Sum all flows in [T-604800, T] inclusive
            for prev_flow in wallet_flows[:i+1]:
                if prev_flow['block_time'] >= cutoff_7d:
                    if prev_flow['direction'] == 'BUY':
                        buy_sum_7d += prev_flow['amount_lamports']
                        buy_count_7d += 1
                    else:
                        sell_sum_7d += prev_flow['amount_lamports']
                        sell_count_7d += 1
            
            if buy_sum_7d >= WHALE_CUM_7D_THRESHOLD:
                key = (wallet, WINDOW_7D, EVENT_WHALE_CUM_7D_BUY, T, S)
                events[key] = (buy_sum_7d, buy_count_7d)
            
            if sell_sum_7d >= WHALE_CUM_7D_THRESHOLD:
                key = (wallet, WINDOW_7D, EVENT_WHALE_CUM_7D_SELL, T, S)
                events[key] = (sell_sum_7d, sell_count_7d)
    
    return events


def analyze_differences(baseline: Dict, recomputed: Dict):
    """
    Analyze differences between baseline and recomputed events.
    """
    baseline_keys = set(baseline.keys())
    recomputed_keys = set(recomputed.keys())
    
    common = baseline_keys & recomputed_keys
    missing = baseline_keys - recomputed_keys
    phantom = recomputed_keys - baseline_keys
    
    print("=" * 80)
    print("EVENT COUNT COMPARISON")
    print("=" * 80)
    print(f"Baseline (DB) events:    {len(baseline):,}")
    print(f"Recomputed events:       {len(recomputed):,}")
    print(f"Common events:           {len(common):,}")
    print(f"Missing from recomputed: {len(missing):,}")
    print(f"Phantom (extra) events:  {len(phantom):,}")
    print()
    
    # If still no common events, show example keys
    if len(common) == 0:
        print("=" * 80)
        print("DEBUG: Still no common events! Showing example keys:")
        print("=" * 80)
        
        print("\nTop 10 BASELINE keys:")
        for i, key in enumerate(sorted(baseline_keys)[:10]):
            wallet, window, event_type, event_time, flow_ref = key
            amount, count = baseline[key]
            flow_ref_display = flow_ref if len(flow_ref) <= 40 else flow_ref[:40] + "..."
            print(f"{i+1}. wallet={wallet[:16]}... window='{window}' type={event_type}")
            print(f"    time={event_time} flow_ref={flow_ref_display}")
            print(f"    amount={amount:,} count={count}")
        
        print("\nTop 10 RECOMPUTED keys:")
        for i, key in enumerate(sorted(recomputed_keys)[:10]):
            wallet, window, event_type, event_time, flow_ref = key
            amount, count = recomputed[key]
            flow_ref_display = flow_ref if len(flow_ref) <= 40 else flow_ref[:40] + "..."
            print(f"{i+1}. wallet={wallet[:16]}... window='{window}' type={event_type}")
            print(f"    time={event_time} flow_ref={flow_ref_display}")
            print(f"    amount={amount:,} count={count}")
        print()
    
    # Sample missing events
    if missing:
        print("=" * 80)
        print(f"SAMPLE MISSING EVENTS (first 10 of {len(missing):,})")
        print("=" * 80)
        for i, key in enumerate(sorted(missing)[:10]):
            wallet, window, event_type, event_time, flow_ref = key
            amount, count = baseline[key]
            print(f"{i+1}. {event_type:25s} wallet={wallet[:16]}... window='{window}' "
                  f"time={event_time} amount={amount:,} count={count}")
        print()
    
    # Sample phantom events
    if phantom:
        print("=" * 80)
        print(f"SAMPLE PHANTOM EVENTS (first 10 of {len(phantom):,})")
        print("=" * 80)
        for i, key in enumerate(sorted(phantom)[:10]):
            wallet, window, event_type, event_time, flow_ref = key
            amount, count = recomputed[key]
            print(f"{i+1}. {event_type:25s} wallet={wallet[:16]}... window='{window}' "
                  f"time={event_time} amount={amount:,} count={count}")
        print()
    
    # Amount mismatches
    amount_mismatches = []
    count_mismatches = []
    
    for key in common:
        base_amount, base_count = baseline[key]
        recomp_amount, recomp_count = recomputed[key]
        
        if base_amount != recomp_amount:
            amount_mismatches.append((key, base_amount, recomp_amount))
        
        if base_count != recomp_count:
            count_mismatches.append((key, base_count, recomp_count))
    
    if amount_mismatches:
        print("=" * 80)
        print(f"AMOUNT MISMATCHES (first 10 of {len(amount_mismatches):,})")
        print("=" * 80)
        for i, (key, base_amt, recomp_amt) in enumerate(amount_mismatches[:10]):
            wallet, window, event_type, event_time, flow_ref = key
            diff = recomp_amt - base_amt
            print(f"{i+1}. {event_type:25s} wallet={wallet[:16]}... window='{window}'")
            print(f"    Baseline: {base_amt:,} lamports  Recomputed: {recomp_amt:,} lamports  Diff: {diff:+,}")
        print()
    
    if count_mismatches:
        print("=" * 80)
        print(f"COUNT MISMATCHES (first 10 of {len(count_mismatches):,})")
        print("=" * 80)
        for i, (key, base_cnt, recomp_cnt) in enumerate(count_mismatches[:10]):
            wallet, window, event_type, event_time, flow_ref = key
            diff = recomp_cnt - base_cnt
            print(f"{i+1}. {event_type:25s} wallet={wallet[:16]}... window='{window}'")
            print(f"    Baseline: {base_cnt}  Recomputed: {recomp_cnt}  Diff: {diff:+d}")
        print()
    
    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total common events:      {len(common):,}")
    print(f"Amount mismatches:        {len(amount_mismatches):,}")
    print(f"Count mismatches:         {len(count_mismatches):,}")
    
    # Calculate events with any mismatch (avoiding double-counting)
    events_with_mismatches = set()
    for key, _, _ in amount_mismatches:
        events_with_mismatches.add(key)
    for key, _, _ in count_mismatches:
        events_with_mismatches.add(key)
    
    perfect_matches = len(common) - len(events_with_mismatches)
    print(f"Events with any mismatch: {len(events_with_mismatches):,}")
    print(f"Perfect matches:          {perfect_matches:,}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description='Forensic analysis of whale events: compare DB vs recomputed'
    )
    parser.add_argument(
        '--db',
        required=True,
        help='Path to SQLite database file (e.g., masterwalletsdb.db)'
    )
    args = parser.parse_args()
    
    print("panda_phase3_2_forensicsv3.py - Forensic Analysis")
    print("=" * 80)
    print("Goal: Compare whale_events (DB) vs recomputed (wallet_token_flow)")
    print("=" * 80)
    print(f"Database: {args.db}")
    print()
    
    conn = None
    try:
        conn = sqlite3.connect(args.db)
    except sqlite3.OperationalError as e:
        print(f"ERROR: Cannot open database file: {args.db}")
        print(f"Details: {e}")
        print("\nPlease check:")
        print("  1. File exists")
        print("  2. Path is correct")
        print("  3. You have read permissions")
        return 1
    
    try:
        # Discover schema and sanity check window values
        discover_schema(conn)
        
        # Load baseline events
        print("=" * 80)
        print("LOADING BASELINE EVENTS")
        print("=" * 80)
        baseline = load_baseline_events(conn)
        print(f"Loaded {len(baseline):,} baseline events from whale_events table")
        print()
        
        # Recompute events
        print("=" * 80)
        print("RECOMPUTING WHALE EVENTS")
        print("=" * 80)
        recomputed = recompute_whale_events(conn)
        print(f"Recomputed {len(recomputed):,} whale events")
        print()
        
        # Analyze differences
        analyze_differences(baseline, recomputed)
    
    except ValueError as e:
        print(f"\n✗ ERROR: {e}")
        return 1
    except sqlite3.OperationalError as e:
        print(f"\n✗ DATABASE ERROR: {e}")
        return 1
    except Exception as e:
        print(f"\n✗ UNEXPECTED ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        if conn is not None:
            conn.close()
    
    print("=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print("Expectation: common > 0 if thresholds and logic match")
    print()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
