#!/usr/bin/env python3
"""
PANDA v4 — Phase 2.7 BUILD SCRIPT (REBUILD)
Generates wallet_token_flow table from swaps and spl_transfers_v2
"""

import sqlite3
import argparse
import sys
import os
from typing import Dict


def print_header(msg: str):
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")


def validate_and_build(db_path: str) -> bool:
    """
    Rebuild wallet_token_flow table and validate.
    Returns True if PASS, False if FAIL.
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
    except sqlite3.Error as e:
        print(f"✗ ERROR: Failed to connect to database: {e}")
        return False
    
    print_header("PHASE 2.7: WALLET TOKEN FLOW BUILD")
    print(f"Database: {db_path}\n")
    
    # Verify source tables exist
    print("Checking source tables...")
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('swaps', 'spl_transfers_v2')")
    existing_tables = {row['name'] for row in cur.fetchall()}
    
    if 'swaps' not in existing_tables:
        print("✗ ERROR: Source table 'swaps' does not exist")
        conn.close()
        return False
    if 'spl_transfers_v2' not in existing_tables:
        print("✗ ERROR: Source table 'spl_transfers_v2' does not exist")
        conn.close()
        return False
    
    print("✓ Source tables verified: swaps, spl_transfers_v2")
    
    # Test write permissions
    try:
        cur.execute("CREATE TEMP TABLE _write_test (id INTEGER)")
        cur.execute("DROP TABLE _write_test")
        conn.commit()
    except sqlite3.Error as e:
        print(f"✗ ERROR: Database is not writable: {e}")
        conn.close()
        return False
    
    # ============================================================
    # STEP 0: REBUILD TABLE
    # ============================================================
    print_header("STEP 0: REBUILD TABLE")
    
    cur.execute("DROP TABLE IF EXISTS wallet_token_flow")
    print("✓ Dropped existing wallet_token_flow table")
    
    cur.execute("""
        CREATE TABLE wallet_token_flow (
            flow_id INTEGER PRIMARY KEY,
            src TEXT NOT NULL,
            scan_wallet TEXT NOT NULL,
            signature TEXT NOT NULL,
            block_time INTEGER NOT NULL,
            mint TEXT NOT NULL,
            direction TEXT NOT NULL,
            amount_raw INTEGER NOT NULL,
            counterparty TEXT,
            dex TEXT,
            has_sol_leg INTEGER,
            sol_direction TEXT,
            sol_amount_lamports INTEGER
        )
    """)
    print("✓ Created wallet_token_flow table")
    
    # Create indexes
    cur.execute("CREATE INDEX idx_wtf_wallet_time ON wallet_token_flow(scan_wallet, block_time)")
    cur.execute("CREATE INDEX idx_wtf_mint_time ON wallet_token_flow(mint, block_time)")
    cur.execute("CREATE INDEX idx_wtf_signature ON wallet_token_flow(signature)")
    cur.execute("CREATE INDEX idx_wtf_wallet_mint ON wallet_token_flow(scan_wallet, mint)")
    print("✓ Created indexes")
    
    conn.commit()
    
    # ============================================================
    # STEP A: INSERT FROM SWAPS
    # ============================================================
    print_header("STEP A: INSERT FROM SWAPS")
    
    # Count total swaps
    cur.execute("SELECT COUNT(*) as cnt FROM swaps")
    swaps_total = cur.fetchone()['cnt']
    print(f"Total swaps in source table: {swaps_total:,}")
    
    # Track rejection reasons (individual condition violations - may overlap)
    rejections: Dict[str, int] = {
        'null_scan_wallet': 0,
        'null_signature': 0,
        'null_block_time': 0,
        'null_token_mint': 0,
        'null_token_amount_raw': 0,
        'token_amount_raw_lte_0': 0,
        'has_sol_leg_not_truthy': 0,
        'sol_direction_invalid': 0
    }
    
    # Count rejections (note: these can overlap - same row may violate multiple conditions)
    cur.execute("SELECT COUNT(*) as cnt FROM swaps WHERE scan_wallet IS NULL")
    rejections['null_scan_wallet'] = cur.fetchone()['cnt']
    
    cur.execute("SELECT COUNT(*) as cnt FROM swaps WHERE signature IS NULL")
    rejections['null_signature'] = cur.fetchone()['cnt']
    
    cur.execute("SELECT COUNT(*) as cnt FROM swaps WHERE block_time IS NULL")
    rejections['null_block_time'] = cur.fetchone()['cnt']
    
    cur.execute("SELECT COUNT(*) as cnt FROM swaps WHERE token_mint IS NULL")
    rejections['null_token_mint'] = cur.fetchone()['cnt']
    
    cur.execute("SELECT COUNT(*) as cnt FROM swaps WHERE token_amount_raw IS NULL")
    rejections['null_token_amount_raw'] = cur.fetchone()['cnt']
    
    cur.execute("SELECT COUNT(*) as cnt FROM swaps WHERE token_amount_raw IS NOT NULL AND token_amount_raw <= 0")
    rejections['token_amount_raw_lte_0'] = cur.fetchone()['cnt']
    
    cur.execute("SELECT COUNT(*) as cnt FROM swaps WHERE has_sol_leg IS NULL OR has_sol_leg != 1")
    rejections['has_sol_leg_not_truthy'] = cur.fetchone()['cnt']
    
    cur.execute("SELECT COUNT(*) as cnt FROM swaps WHERE sol_direction IS NULL OR sol_direction NOT IN ('in', 'out')")
    rejections['sol_direction_invalid'] = cur.fetchone()['cnt']
    
    # Count actual unique rejected rows (NOT inserted)
    cur.execute("""
        SELECT COUNT(*) as cnt FROM swaps
        WHERE NOT (
            scan_wallet IS NOT NULL
            AND signature IS NOT NULL
            AND block_time IS NOT NULL
            AND token_mint IS NOT NULL
            AND token_amount_raw IS NOT NULL
            AND token_amount_raw > 0
            AND has_sol_leg = 1
            AND sol_direction IN ('in', 'out')
        )
    """)
    swaps_rejected_unique = cur.fetchone()['cnt']
    
    # Insert valid swaps
    cur.execute("""
        INSERT INTO wallet_token_flow (
            src, scan_wallet, signature, block_time, mint, direction, amount_raw,
            counterparty, dex, has_sol_leg, sol_direction, sol_amount_lamports
        )
        SELECT
            'swap' as src,
            scan_wallet,
            signature,
            block_time,
            token_mint as mint,
            CASE
                WHEN sol_direction = 'out' THEN 'in'
                WHEN sol_direction = 'in' THEN 'out'
            END as direction,
            token_amount_raw as amount_raw,
            NULL as counterparty,
            dex,
            has_sol_leg,
            sol_direction,
            sol_amount_lamports
        FROM swaps
        WHERE
            scan_wallet IS NOT NULL
            AND signature IS NOT NULL
            AND block_time IS NOT NULL
            AND token_mint IS NOT NULL
            AND token_amount_raw IS NOT NULL
            AND token_amount_raw > 0
            AND has_sol_leg = 1
            AND sol_direction IN ('in', 'out')
    """)
    swaps_inserted = cur.rowcount
    conn.commit()
    
    print(f"\nSwaps inserted: {swaps_inserted:,}")
    print(f"\nSwaps rejected: {swaps_rejected_unique:,} unique rows")
    print(f"Rejection reasons (may overlap):")
    for reason, count in rejections.items():
        if count > 0:
            print(f"  - {reason}: {count:,}")
    
    # Sanity check
    if swaps_inserted + swaps_rejected_unique != swaps_total:
        print(f"\n⚠ WARNING: Count mismatch! inserted({swaps_inserted}) + rejected({swaps_rejected_unique}) != total({swaps_total})")
    
    # ============================================================
    # STEP B: INSERT FROM SPL_TRANSFERS_V2 (INFLOW ONLY)
    # ============================================================
    print_header("STEP B: INSERT FROM SPL_TRANSFERS_V2 (INFLOW ONLY)")
    
    # Count total transfers
    cur.execute("SELECT COUNT(*) as cnt FROM spl_transfers_v2")
    transfers_total = cur.fetchone()['cnt']
    print(f"Total transfers in source table: {transfers_total:,}")
    
    # Count inflow candidates
    cur.execute("""
        SELECT COUNT(*) as cnt 
        FROM spl_transfers_v2 
        WHERE to_addr = scan_wallet
    """)
    transfers_inflow_candidates = cur.fetchone()['cnt']
    print(f"Inflow candidates (to_addr == scan_wallet): {transfers_inflow_candidates:,}")
    
    # Insert inflow transfers
    cur.execute("""
        INSERT INTO wallet_token_flow (
            src, scan_wallet, signature, block_time, mint, direction, amount_raw,
            counterparty, dex, has_sol_leg, sol_direction, sol_amount_lamports
        )
        SELECT
            'transfer' as src,
            scan_wallet,
            signature,
            block_time,
            mint,
            'in' as direction,
            amount_raw,
            from_addr as counterparty,
            NULL as dex,
            NULL as has_sol_leg,
            NULL as sol_direction,
            NULL as sol_amount_lamports
        FROM spl_transfers_v2
        WHERE
            to_addr = scan_wallet
            AND mint IS NOT NULL
            AND amount_raw IS NOT NULL
            AND amount_raw > 0
            AND signature IS NOT NULL
            AND block_time IS NOT NULL
            AND scan_wallet IS NOT NULL
    """)
    transfers_inserted = cur.rowcount
    conn.commit()
    
    print(f"Transfers inserted: {transfers_inserted:,}")
    
    # ============================================================
    # VALIDATIONS
    # ============================================================
    print_header("VALIDATIONS")
    
    # Total rows
    cur.execute("SELECT COUNT(*) as cnt FROM wallet_token_flow")
    total_rows = cur.fetchone()['cnt']
    print(f"Total wallet_token_flow rows: {total_rows:,}")
    
    # Count by direction
    cur.execute("SELECT COUNT(*) as cnt FROM wallet_token_flow WHERE direction = 'in'")
    count_in = cur.fetchone()['cnt']
    
    cur.execute("SELECT COUNT(*) as cnt FROM wallet_token_flow WHERE direction = 'out'")
    count_out = cur.fetchone()['cnt']
    
    print(f"  - Direction 'in': {count_in:,}")
    print(f"  - Direction 'out': {count_out:,}")
    
    # CRITICAL ASSERTION: All 'out' rows must be from swaps
    cur.execute("""
        SELECT COUNT(*) as cnt 
        FROM wallet_token_flow 
        WHERE direction = 'out' AND src != 'swap'
    """)
    invalid_out_rows = cur.fetchone()['cnt']
    
    print(f"\n✓ ASSERTION: All 'out' rows have src='swap'")
    if invalid_out_rows > 0:
        print(f"  ✗ FAIL: Found {invalid_out_rows} 'out' rows with src != 'swap'")
        return False
    else:
        print(f"  ✓ PASS: 0 invalid 'out' rows")
    
    # ASSERTION: swaps_inserted > 0
    print(f"\n✓ ASSERTION: swaps_inserted > 0")
    if swaps_inserted <= 0:
        print(f"  ✗ FAIL: swaps_inserted = {swaps_inserted}")
        return False
    else:
        print(f"  ✓ PASS: swaps_inserted = {swaps_inserted:,}")
    
    # ASSERTION: transfers_inserted >= 0
    print(f"\n✓ ASSERTION: transfers_inserted >= 0")
    if transfers_inserted < 0:
        print(f"  ✗ FAIL: transfers_inserted = {transfers_inserted}")
        return False
    else:
        print(f"  ✓ PASS: transfers_inserted = {transfers_inserted:,}")
    
    # Verify totals match
    expected_total = swaps_inserted + transfers_inserted
    print(f"\n✓ VERIFICATION: Row count matches inserts")
    if total_rows != expected_total:
        print(f"  ✗ FAIL: total_rows={total_rows} != expected={expected_total}")
        return False
    else:
        print(f"  ✓ PASS: {total_rows:,} = {swaps_inserted:,} + {transfers_inserted:,}")
    
    # Verify direction breakdown makes sense
    print(f"\n✓ VERIFICATION: Direction breakdown")
    # All transfers are 'in', swaps can be 'in' or 'out'
    # So count_in should be >= transfers_inserted
    # And count_out should equal the number of 'out' swaps
    cur.execute("SELECT COUNT(*) as cnt FROM wallet_token_flow WHERE src = 'swap' AND direction = 'in'")
    swap_in_count = cur.fetchone()['cnt']
    
    cur.execute("SELECT COUNT(*) as cnt FROM wallet_token_flow WHERE src = 'swap' AND direction = 'out'")
    swap_out_count = cur.fetchone()['cnt']
    
    expected_count_in = transfers_inserted + swap_in_count
    expected_count_out = swap_out_count
    
    if count_in != expected_count_in:
        print(f"  ✗ FAIL: count_in={count_in} != expected={expected_count_in} (transfers={transfers_inserted} + swap_in={swap_in_count})")
        return False
    
    if count_out != expected_count_out:
        print(f"  ✗ FAIL: count_out={count_out} != expected={expected_count_out} (swap_out={swap_out_count})")
        return False
    
    print(f"  ✓ PASS: count_in={count_in:,} (transfers={transfers_inserted:,} + swap_in={swap_in_count:,})")
    print(f"  ✓ PASS: count_out={count_out:,} (swap_out={swap_out_count:,})")

    
    conn.close()
    return True


def main():
    parser = argparse.ArgumentParser(
        description='PANDA v4 Phase 2.7: Build wallet_token_flow table'
    )
    parser.add_argument(
        '--db',
        type=str,
        default='masterwalletsdb.db',
        help='Path to SQLite database (default: masterwalletsdb.db)'
    )
    
    args = parser.parse_args()
    
    # Check if database file exists
    if not os.path.exists(args.db):
        print_header("ERROR")
        print(f"✗ Database file not found: {args.db}")
        print(f"✗ PHASE_2_7_WALLET_TOKEN_FLOW_BUILD = FAIL")
        print("="*60)
        sys.exit(1)
    
    try:
        success = validate_and_build(args.db)
        
        print_header("FINAL RESULT")
        if success:
            print("✓ PHASE_2_7_WALLET_TOKEN_FLOW_BUILD = PASS")
            print("="*60)
            sys.exit(0)
        else:
            print("✗ PHASE_2_7_WALLET_TOKEN_FLOW_BUILD = FAIL")
            print("="*60)
            sys.exit(1)
            
    except Exception as e:
        print_header("ERROR")
        print(f"✗ Build failed with exception: {e}")
        print(f"✗ PHASE_2_7_WALLET_TOKEN_FLOW_BUILD = FAIL")
        print("="*60)
        sys.exit(1)


if __name__ == '__main__':
    main()
