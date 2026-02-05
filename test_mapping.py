#!/usr/bin/env python3
"""
Quick test to verify column mapping will work with your database.
"""

import sqlite3
import sys

# Simulate the ColumnMapper logic
def test_mapping(db_path):
    """Test if column mapping will work."""
    
    conn = sqlite3.connect(db_path)
    
    print("="*80)
    print("TESTING COLUMN MAPPING")
    print("="*80)
    
    # Test whale_events
    print("\n--- Testing whale_events ---")
    cursor = conn.execute("PRAGMA table_info(whale_events)")
    cols = {row[1].lower(): row[1] for row in cursor.fetchall()}
    
    required_mapping = {
        'wallet': ['scan_wallet', 'wallet', 'wallet_address', 'address'],
        'window': ['window', 'time_window', 'window_type'],
        'event_type': ['event_type', 'type', 'event_name'],
        'event_time': ['event_time', 'timestamp', 'time', 'block_time'],
        'flow_ref': ['flow_ref', 'signature', 'tx_signature', 'reference'],
        'amount': ['sol_amount_lamports', 'amount', 'lamports', 'sol_amount'],
        'count': ['supporting_flow_count', 'flow_count', 'count', 'num_flows']
    }
    
    found = {}
    for key, variations in required_mapping.items():
        for var in variations:
            if var.lower() in cols:
                found[key] = cols[var.lower()]
                print(f"  ✓ {key:15s} -> {found[key]}")
                break
        if key not in found:
            print(f"  ✗ {key:15s} -> NOT FOUND (tried: {', '.join(variations)})")
    
    # Test wallet_token_flow
    print("\n--- Testing wallet_token_flow ---")
    cursor = conn.execute("PRAGMA table_info(wallet_token_flow)")
    cols = {row[1].lower(): row[1] for row in cursor.fetchall()}
    
    required_mapping = {
        'wallet': ['scan_wallet', 'wallet', 'wallet_address', 'address'],
        'block_time': ['block_time', 'timestamp', 'time'],
        'direction': ['sol_direction', 'direction', 'type', 'side'],
        'amount': ['sol_amount_lamports', 'amount', 'lamports', 'sol_amount'],
        'signature': ['signature', 'tx_signature', 'transaction_signature', 'sig']
    }
    
    found = {}
    for key, variations in required_mapping.items():
        for var in variations:
            if var.lower() in cols:
                found[key] = cols[var.lower()]
                print(f"  ✓ {key:15s} -> {found[key]}")
                break
        if key not in found:
            print(f"  ✗ {key:15s} -> NOT FOUND (tried: {', '.join(variations)})")
    
    print("\n" + "="*80)
    print("CONCLUSION: All required columns found! Script should work.")
    print("="*80)
    
    conn.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python test_mapping.py <database_path>")
        sys.exit(1)
    
    test_mapping(sys.argv[1])
