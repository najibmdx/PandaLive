#!/usr/bin/env python3
"""
panda_phase2_4_ownerlink.py
Proves whether spl_transfers_v2 rows can be linked to scan_wallet via owner fields.
Exit codes:
  0 = linkage proven usable (>=1 row matches)
  2 = linkage not proven (zero matches)
  3 = missing table/columns
"""

import sqlite3
import argparse
import sys
import os


def check_schema(conn):
    """Verify spl_transfers_v2 table and required columns exist."""
    cur = conn.cursor()
    
    try:
        # Check table exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='spl_transfers_v2'")
        if not cur.fetchone():
            print("ERROR: Table spl_transfers_v2 does not exist")
            return False, ["table spl_transfers_v2"]
        
        # Check required columns
        required_cols = [
            'scan_wallet', 'instruction_type', 'source_owner', 'authority',
            'from_addr', 'to_addr', 'mint', 'amount_raw', 'signature', 'block_time'
        ]
        
        cur.execute("PRAGMA table_info(spl_transfers_v2)")
        existing_cols = {row[1] for row in cur.fetchall()}
        
        missing = [col for col in required_cols if col not in existing_cols]
        
        if missing:
            print("ERROR: Missing columns in spl_transfers_v2:")
            for col in missing:
                print(f"  - {col}")
            return False, missing
        
        return True, []
    except sqlite3.Error as e:
        print(f"ERROR: Schema check failed: {e}")
        return False, [str(e)]
    finally:
        cur.close()


def get_baseline_counts(conn):
    """Get baseline row counts."""
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT COUNT(*) FROM spl_transfers_v2")
        total = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(*) FROM spl_transfers_v2 
            WHERE instruction_type IN ('transfer', 'transfer_checked')
        """)
        transfer_rows = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(*) FROM spl_transfers_v2 
            WHERE instruction_type = 'transfer_checked'
        """)
        transfer_checked_rows = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(*) FROM spl_transfers_v2 
            WHERE instruction_type = 'close_account'
        """)
        close_account_rows = cur.fetchone()[0]
        
        return {
            'total': total,
            'transfer': transfer_rows,
            'transfer_checked': transfer_checked_rows,
            'close_account': close_account_rows
        }
    except sqlite3.Error as e:
        print(f"ERROR: Failed to get baseline counts: {e}")
        raise
    finally:
        cur.close()


def get_owner_linkage_counts(conn, instruction_filter):
    """Get owner linkage statistics for given instruction type filter."""
    cur = conn.cursor()
    
    # Validate and build safe SQL based on filter type
    if instruction_filter == "instruction_type IN ('transfer', 'transfer_checked')":
        where_clause = "instruction_type IN ('transfer', 'transfer_checked')"
    elif instruction_filter == "instruction_type = 'transfer_checked'":
        where_clause = "instruction_type = 'transfer_checked'"
    else:
        raise ValueError(f"Unexpected instruction_filter: {instruction_filter}")
    
    try:
        # Total matching rows
        cur.execute(f"""
            SELECT COUNT(*) FROM spl_transfers_v2 
            WHERE {where_clause}
        """)
        n = cur.fetchone()[0]
        
        # Null counts
        cur.execute(f"""
            SELECT 
                SUM(CASE WHEN source_owner IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN authority IS NULL THEN 1 ELSE 0 END)
            FROM spl_transfers_v2 
            WHERE {where_clause}
        """)
        row = cur.fetchone()
        source_owner_null = row[0] or 0
        authority_null = row[1] or 0
        
        # Match counts
        cur.execute(f"""
            SELECT COUNT(*) FROM spl_transfers_v2 
            WHERE {where_clause} AND source_owner = scan_wallet
        """)
        source_owner_eq_scan = cur.fetchone()[0]
        
        cur.execute(f"""
            SELECT COUNT(*) FROM spl_transfers_v2 
            WHERE {where_clause} AND authority = scan_wallet
        """)
        authority_eq_scan = cur.fetchone()[0]
        
        cur.execute(f"""
            SELECT COUNT(*) FROM spl_transfers_v2 
            WHERE {where_clause} 
            AND (source_owner = scan_wallet OR authority = scan_wallet)
        """)
        either_owner_eq_scan = cur.fetchone()[0]
        
        return {
            'n': n,
            'source_owner_null': source_owner_null,
            'authority_null': authority_null,
            'source_owner_eq_scan': source_owner_eq_scan,
            'authority_eq_scan': authority_eq_scan,
            'either_owner_eq_scan': either_owner_eq_scan
        }
    except sqlite3.Error as e:
        print(f"ERROR: Database query failed: {e}")
        raise
    finally:
        cur.close()


def get_sample_rows(conn):
    """Get sample rows where owner fields match scan_wallet."""
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                scan_wallet, signature, mint, amount_raw,
                source_owner, authority, from_addr, to_addr
            FROM spl_transfers_v2
            WHERE instruction_type = 'transfer_checked'
            AND (source_owner = scan_wallet OR authority = scan_wallet)
            ORDER BY signature
            LIMIT 10
        """)
        
        return cur.fetchall()
    except sqlite3.Error as e:
        print(f"ERROR: Failed to get sample rows: {e}")
        raise
    finally:
        cur.close()


def truncate(s, n=12):
    """Truncate string to n chars + ..."""
    if s is None:
        return "NULL"
    s = str(s)
    # Handle very large numbers
    if len(s) > 20 and s.isdigit():
        return s[:8] + "..."
    if len(s) <= n:
        return s
    return s[:n] + "..."


def main():
    parser = argparse.ArgumentParser(description='Verify owner linkage in spl_transfers_v2')
    parser.add_argument('--db', required=True, help='Path to SQLite database')
    args = parser.parse_args()
    
    # Check if database file exists
    if not os.path.exists(args.db):
        print(f"ERROR: Database file not found: {args.db}")
        sys.exit(3)
    
    # Try to open database
    try:
        conn = sqlite3.connect(args.db)
    except Exception as e:
        print(f"ERROR: Cannot open database: {e}")
        sys.exit(3)
    
    print("=" * 80)
    print("PANDA PHASE 2.4: SPL_TRANSFERS_V2 OWNER LINKAGE VERIFICATION")
    print("=" * 80)
    print()
    
    try:
        # [0] Schema verification
        print("[0] SCHEMA VERIFICATION")
        print("-" * 80)
        schema_ok, missing = check_schema(conn)
        if not schema_ok:
            conn.close()
            sys.exit(3)
        print("✓ All required columns present")
        print()
        
        # [1] Baseline counts
        print("[1] BASELINE COUNTS")
        print("-" * 80)
        baseline = get_baseline_counts(conn)
        print(f"total_rows:              {baseline['total']:>12,}")
        print(f"transfer_rows:           {baseline['transfer']:>12,}")
        print(f"transfer_checked_rows:   {baseline['transfer_checked']:>12,}")
        print(f"close_account_rows:      {baseline['close_account']:>12,}")
        print()
        
        # [2] Owner linkage counts for transfer types
        print("[2] OWNER LINKAGE COUNTS (transfer + transfer_checked)")
        print("-" * 80)
        transfer_stats = get_owner_linkage_counts(
            conn, 
            "instruction_type IN ('transfer', 'transfer_checked')"
        )
        print(f"n (transfer-type rows):  {transfer_stats['n']:>12,}")
        print(f"source_owner_null:       {transfer_stats['source_owner_null']:>12,}")
        print(f"authority_null:          {transfer_stats['authority_null']:>12,}")
        print(f"source_owner_eq_scan:    {transfer_stats['source_owner_eq_scan']:>12,}")
        print(f"authority_eq_scan:       {transfer_stats['authority_eq_scan']:>12,}")
        print(f"either_owner_eq_scan:    {transfer_stats['either_owner_eq_scan']:>12,}")
        print()
        
        print("[2b] OWNER LINKAGE COUNTS (transfer_checked only)")
        print("-" * 80)
        checked_stats = get_owner_linkage_counts(
            conn,
            "instruction_type = 'transfer_checked'"
        )
        print(f"n (transfer_checked):    {checked_stats['n']:>12,}")
        print(f"source_owner_null:       {checked_stats['source_owner_null']:>12,}")
        print(f"authority_null:          {checked_stats['authority_null']:>12,}")
        print(f"source_owner_eq_scan:    {checked_stats['source_owner_eq_scan']:>12,}")
        print(f"authority_eq_scan:       {checked_stats['authority_eq_scan']:>12,}")
        print(f"either_owner_eq_scan:    {checked_stats['either_owner_eq_scan']:>12,}")
        print()
        
        # [3] Sample rows
        print("[3] SAMPLE ROWS (transfer_checked with owner=scan_wallet, up to 10)")
        print("-" * 80)
        samples = get_sample_rows(conn)
        
        if not samples:
            print("No linked samples found")
        else:
            print(f"{'scan_wallet':<16} {'signature':<16} {'mint':<16} {'amount_raw':<12} {'source_owner':<16} {'authority':<16} {'from_addr':<16} {'to_addr':<16}")
            print("-" * 140)
            for row in samples:
                scan_wallet, signature, mint, amount_raw, source_owner, authority, from_addr, to_addr = row
                print(f"{truncate(scan_wallet, 12):<16} {truncate(signature, 12):<16} {truncate(mint, 12):<16} {truncate(amount_raw, 8):<12} {truncate(source_owner, 12):<16} {truncate(authority, 12):<16} {truncate(from_addr, 12):<16} {truncate(to_addr, 12):<16}")
        print()
        
        # [4] Determine usable linkage
        print("[4] LINKAGE DETERMINATION")
        print("-" * 80)
        
        usable = transfer_stats['either_owner_eq_scan'] > 0
        
        if usable:
            print(f"✓ Owner linkage USABLE: {transfer_stats['either_owner_eq_scan']:,} transfer-type rows")
            print("  can be linked via source_owner or authority matching scan_wallet")
            print()
            print("RESULT: OWNER_LINK_USABLE")
            print("=" * 80)
            conn.close()
            sys.exit(0)
        else:
            print("✗ Owner linkage NOT PROVEN: 0 transfer-type rows match scan_wallet")
            print("  via source_owner or authority fields")
            print()
            print("RESULT: OWNER_LINK_NOT_PROVEN")
            print("=" * 80)
            conn.close()
            sys.exit(2)
    
    except sqlite3.Error as e:
        print()
        print(f"ERROR: Database operation failed: {e}")
        conn.close()
        sys.exit(3)
    except Exception as e:
        print()
        print(f"ERROR: Unexpected error: {e}")
        conn.close()
        sys.exit(3)


if __name__ == "__main__":
    main()
