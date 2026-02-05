#!/usr/bin/env python3
"""
PANDA v4 Phase 2.7 ACCEPTANCE INSPECTOR v2
Validates wallet_token_flow against swaps with strict provenance and integrity checks.
"""

import sqlite3
import json
import os
import argparse
import csv
from datetime import datetime
from typing import Dict, List, Tuple, Any


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    """Discover columns for a table using PRAGMA table_info."""
    # Validate table name to prevent SQL injection
    allowed_tables = {'swaps', 'wallet_token_flow'}
    if table_name not in allowed_tables:
        raise ValueError(f"Invalid table name: {table_name}")
    
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    rows = cursor.fetchall()
    if not rows:
        return []
    return [row[1] for row in rows]


def validate_required_columns(conn: sqlite3.Connection) -> None:
    """Validate required columns exist in both tables."""
    required_swaps = {
        'scan_wallet', 'signature', 'block_time', 'sol_direction',
        'token_mint', 'token_amount_raw'
    }
    required_flow = {
        'scan_wallet', 'signature', 'block_time', 'sol_direction',
        'token_mint', 'token_amount_raw', 'flow_direction'
    }
    
    swaps_cols = set(get_table_columns(conn, 'swaps'))
    flow_cols = set(get_table_columns(conn, 'wallet_token_flow'))
    
    if not swaps_cols:
        raise ValueError("Table 'swaps' does not exist or has no columns")
    if not flow_cols:
        raise ValueError("Table 'wallet_token_flow' does not exist or has no columns")
    
    missing_swaps = required_swaps - swaps_cols
    missing_flow = required_flow - flow_cols
    
    if missing_swaps:
        raise ValueError(f"Missing required columns in 'swaps': {missing_swaps}")
    if missing_flow:
        raise ValueError(f"Missing required columns in 'wallet_token_flow': {missing_flow}")


def count_qualifying_swaps(conn: sqlite3.Connection) -> int:
    """Count swaps matching the Phase 2.7 filter criteria."""
    query = """
    SELECT COUNT(*) FROM swaps
    WHERE sol_direction IN ('buy', 'sell')
      AND token_mint IS NOT NULL AND token_mint != ''
      AND token_amount_raw IS NOT NULL AND token_amount_raw > 0
      AND scan_wallet IS NOT NULL AND scan_wallet != ''
      AND signature IS NOT NULL AND signature != ''
      AND block_time IS NOT NULL
    """
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchone()[0]


def count_flow_rows(conn: sqlite3.Connection) -> Dict[str, int]:
    """Count total and directional flow rows."""
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM wallet_token_flow")
    total = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM wallet_token_flow WHERE flow_direction = 'in'")
    in_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM wallet_token_flow WHERE flow_direction = 'out'")
    out_count = cursor.fetchone()[0]
    
    return {'total': total, 'in': in_count, 'out': out_count}


def check_domain_integrity(conn: sqlite3.Connection) -> List[str]:
    """Validate domain constraints on wallet_token_flow."""
    errors = []
    cursor = conn.cursor()
    
    # Check flow_direction domain
    cursor.execute("""
        SELECT COUNT(*) FROM wallet_token_flow
        WHERE flow_direction NOT IN ('in', 'out') OR flow_direction IS NULL
    """)
    if cursor.fetchone()[0] > 0:
        errors.append("Invalid flow_direction values found (not 'in' or 'out')")
    
    # Check sol_direction domain
    cursor.execute("""
        SELECT COUNT(*) FROM wallet_token_flow
        WHERE sol_direction NOT IN ('buy', 'sell') OR sol_direction IS NULL
    """)
    if cursor.fetchone()[0] > 0:
        errors.append("Invalid sol_direction values found (not 'buy' or 'sell')")
    
    # Check token_amount_raw > 0
    cursor.execute("""
        SELECT COUNT(*) FROM wallet_token_flow
        WHERE token_amount_raw IS NULL OR token_amount_raw <= 0
    """)
    if cursor.fetchone()[0] > 0:
        errors.append("Invalid token_amount_raw values found (NULL or <= 0)")
    
    # Check non-empty token_mint
    cursor.execute("""
        SELECT COUNT(*) FROM wallet_token_flow
        WHERE token_mint IS NULL OR token_mint = ''
    """)
    if cursor.fetchone()[0] > 0:
        errors.append("Empty or NULL token_mint values found")
    
    # Check non-empty scan_wallet
    cursor.execute("""
        SELECT COUNT(*) FROM wallet_token_flow
        WHERE scan_wallet IS NULL OR scan_wallet = ''
    """)
    if cursor.fetchone()[0] > 0:
        errors.append("Empty or NULL scan_wallet values found")
    
    # Check non-empty signature
    cursor.execute("""
        SELECT COUNT(*) FROM wallet_token_flow
        WHERE signature IS NULL OR signature = ''
    """)
    if cursor.fetchone()[0] > 0:
        errors.append("Empty or NULL signature values found")
    
    return errors


def find_orphans(conn: sqlite3.Connection) -> Tuple[int, List[Dict]]:
    """Find wallet_token_flow rows with no matching swaps row on strong join key."""
    query = """
    SELECT 
        f.signature,
        f.scan_wallet,
        f.token_mint,
        f.sol_direction,
        f.token_amount_raw,
        f.flow_direction,
        f.block_time
    FROM wallet_token_flow f
    LEFT JOIN swaps s ON (
        f.signature = s.signature
        AND f.scan_wallet = s.scan_wallet
        AND f.token_mint = s.token_mint
        AND f.sol_direction = s.sol_direction
        AND CAST(f.token_amount_raw AS INTEGER) = CAST(s.token_amount_raw AS INTEGER)
        AND s.sol_direction IN ('buy', 'sell')
        AND s.token_mint IS NOT NULL AND s.token_mint != ''
        AND s.token_amount_raw IS NOT NULL AND s.token_amount_raw > 0
        AND s.scan_wallet IS NOT NULL AND s.scan_wallet != ''
        AND s.signature IS NOT NULL AND s.signature != ''
        AND s.block_time IS NOT NULL
    )
    WHERE s.signature IS NULL
    LIMIT 200
    """
    cursor = conn.cursor()
    cursor.execute(query)
    samples = []
    for row in cursor.fetchall():
        samples.append({
            'signature': row[0],
            'scan_wallet': row[1],
            'token_mint': row[2],
            'sol_direction': row[3],
            'token_amount_raw': row[4],
            'flow_direction': row[5],
            'block_time': row[6]
        })
    
    # Count total orphans
    count_query = """
    SELECT COUNT(*)
    FROM wallet_token_flow f
    LEFT JOIN swaps s ON (
        f.signature = s.signature
        AND f.scan_wallet = s.scan_wallet
        AND f.token_mint = s.token_mint
        AND f.sol_direction = s.sol_direction
        AND CAST(f.token_amount_raw AS INTEGER) = CAST(s.token_amount_raw AS INTEGER)
        AND s.sol_direction IN ('buy', 'sell')
        AND s.token_mint IS NOT NULL AND s.token_mint != ''
        AND s.token_amount_raw IS NOT NULL AND s.token_amount_raw > 0
        AND s.scan_wallet IS NOT NULL AND s.scan_wallet != ''
        AND s.signature IS NOT NULL AND s.signature != ''
        AND s.block_time IS NOT NULL
    )
    WHERE s.signature IS NULL
    """
    cursor.execute(count_query)
    count = cursor.fetchone()[0]
    
    return count, samples


def find_mapping_mismatches(conn: sqlite3.Connection) -> Tuple[int, List[Dict]]:
    """Find rows where direction mapping is incorrect."""
    query = """
    SELECT 
        f.signature,
        f.scan_wallet,
        f.token_mint,
        s.sol_direction,
        f.flow_direction,
        f.token_amount_raw,
        f.block_time
    FROM wallet_token_flow f
    INNER JOIN swaps s ON (
        f.signature = s.signature
        AND f.scan_wallet = s.scan_wallet
        AND f.token_mint = s.token_mint
        AND f.sol_direction = s.sol_direction
        AND CAST(f.token_amount_raw AS INTEGER) = CAST(s.token_amount_raw AS INTEGER)
    )
    WHERE s.sol_direction IN ('buy', 'sell')
      AND s.token_mint IS NOT NULL AND s.token_mint != ''
      AND s.token_amount_raw IS NOT NULL AND s.token_amount_raw > 0
      AND s.scan_wallet IS NOT NULL AND s.scan_wallet != ''
      AND s.signature IS NOT NULL AND s.signature != ''
      AND s.block_time IS NOT NULL
      AND NOT (
        (s.sol_direction = 'buy' AND f.flow_direction = 'in')
        OR (s.sol_direction = 'sell' AND f.flow_direction = 'out')
      )
    LIMIT 200
    """
    cursor = conn.cursor()
    cursor.execute(query)
    samples = []
    for row in cursor.fetchall():
        samples.append({
            'signature': row[0],
            'scan_wallet': row[1],
            'token_mint': row[2],
            'sol_direction': row[3],
            'flow_direction': row[4],
            'token_amount_raw': row[5],
            'block_time': row[6]
        })
    
    # Count total mismatches
    count_query = """
    SELECT COUNT(*)
    FROM wallet_token_flow f
    INNER JOIN swaps s ON (
        f.signature = s.signature
        AND f.scan_wallet = s.scan_wallet
        AND f.token_mint = s.token_mint
        AND f.sol_direction = s.sol_direction
        AND CAST(f.token_amount_raw AS INTEGER) = CAST(s.token_amount_raw AS INTEGER)
    )
    WHERE s.sol_direction IN ('buy', 'sell')
      AND s.token_mint IS NOT NULL AND s.token_mint != ''
      AND s.token_amount_raw IS NOT NULL AND s.token_amount_raw > 0
      AND s.scan_wallet IS NOT NULL AND s.scan_wallet != ''
      AND s.signature IS NOT NULL AND s.signature != ''
      AND s.block_time IS NOT NULL
      AND NOT (
        (s.sol_direction = 'buy' AND f.flow_direction = 'in')
        OR (s.sol_direction = 'sell' AND f.flow_direction = 'out')
      )
    """
    cursor.execute(count_query)
    count = cursor.fetchone()[0]
    
    return count, samples


def find_duplicates(conn: sqlite3.Connection) -> Dict[str, List[Dict]]:
    """Find duplicate groups in both tables."""
    # Duplicates in wallet_token_flow
    flow_dup_query = """
    SELECT 
        signature,
        scan_wallet,
        token_mint,
        flow_direction,
        COUNT(*) as dup_count
    FROM wallet_token_flow
    GROUP BY signature, scan_wallet, token_mint, flow_direction
    HAVING COUNT(*) > 1
    ORDER BY dup_count DESC
    LIMIT 100
    """
    
    # Duplicates in swaps
    swaps_dup_query = """
    SELECT 
        signature,
        scan_wallet,
        token_mint,
        sol_direction,
        token_amount_raw,
        COUNT(*) as dup_count
    FROM swaps
    WHERE sol_direction IN ('buy', 'sell')
      AND token_mint IS NOT NULL AND token_mint != ''
      AND token_amount_raw IS NOT NULL AND token_amount_raw > 0
      AND scan_wallet IS NOT NULL AND scan_wallet != ''
      AND signature IS NOT NULL AND signature != ''
      AND block_time IS NOT NULL
    GROUP BY signature, scan_wallet, token_mint, sol_direction, token_amount_raw
    HAVING COUNT(*) > 1
    ORDER BY dup_count DESC
    LIMIT 100
    """
    
    cursor = conn.cursor()
    
    cursor.execute(flow_dup_query)
    flow_dups = []
    for row in cursor.fetchall():
        flow_dups.append({
            'signature': row[0],
            'scan_wallet': row[1],
            'token_mint': row[2],
            'flow_direction': row[3],
            'count': row[4]
        })
    
    cursor.execute(swaps_dup_query)
    swaps_dups = []
    for row in cursor.fetchall():
        swaps_dups.append({
            'signature': row[0],
            'scan_wallet': row[1],
            'token_mint': row[2],
            'sol_direction': row[3],
            'token_amount_raw': row[4],
            'count': row[5]
        })
    
    return {'flow_duplicates': flow_dups, 'swaps_duplicates': swaps_dups}


def write_tsv(filepath: str, headers: List[str], rows: List[Dict]) -> None:
    """Write TSV file with headers and data."""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter='\t', extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description='PANDA Phase 2.7 Acceptance Inspector v2')
    parser.add_argument('--db', required=True, help='Path to database file')
    parser.add_argument('--outdir', default='exports_phase2_7_accept_v2', help='Output directory')
    parser.add_argument('--strict', type=int, default=1, help='Strict mode (1=yes, 0=no)')
    args = parser.parse_args()
    
    start_time = datetime.now()
    
    try:
        # Create output directory
        os.makedirs(args.outdir, exist_ok=True)
        
        # Connect to database
        if not os.path.exists(args.db):
            print(f"ERROR: Database file not found: {args.db}")
            return 1
        
        conn = sqlite3.connect(args.db)
        
    except Exception as e:
        print(f"ERROR: Failed to initialize: {e}")
        return 1
    
    print("=" * 80)
    print("PANDA v4 Phase 2.7 ACCEPTANCE INSPECTOR v2")
    print("=" * 80)
    print(f"Database: {args.db}")
    print(f"Output directory: {args.outdir}")
    print(f"Strict mode: {'ON' if args.strict else 'OFF'}")
    print(f"Started: {start_time.isoformat()}")
    print()
    
    try:
        # Step A: Schema discovery and validation
        print("STEP A: Schema Discovery and Validation")
        print("-" * 80)
        try:
            validate_required_columns(conn)
            print("✓ All required columns present in both tables")
        except ValueError as e:
            print(f"✗ SCHEMA VALIDATION FAILED: {e}")
            conn.close()
            return 1
        print()
    
        # Step B: Count qualifying swaps
        print("STEP B: Counting Qualifying Swaps")
        print("-" * 80)
        qualifying_swaps = count_qualifying_swaps(conn)
        total_swaps_query = "SELECT COUNT(*) FROM swaps"
        cursor = conn.cursor()
        cursor.execute(total_swaps_query)
        total_swaps = cursor.fetchone()[0]
        print(f"Total swaps: {total_swaps:,}")
        print(f"Qualifying swaps (Phase 2.7 filter): {qualifying_swaps:,}")
        print()
        
        # Step C: Core acceptance checks
        print("STEP C: Core Acceptance Checks")
        print("-" * 80)
        
        # C1: Presence check
        print("C1: Presence Check")
        flow_counts = count_flow_rows(conn)
        print(f"  wallet_token_flow total rows: {flow_counts['total']:,}")
        print(f"  wallet_token_flow 'in' rows: {flow_counts['in']:,}")
        print(f"  wallet_token_flow 'out' rows: {flow_counts['out']:,}")
        
        if flow_counts['total'] == 0 and qualifying_swaps > 0:
            print("  ✗ FAIL: No rows in wallet_token_flow but qualifying swaps exist")
            conn.close()
            return 1
        print("  ✓ PASS: Presence check")
        print()
        
        # C2: Domain integrity
        print("C2: Domain Integrity Check")
        domain_errors = check_domain_integrity(conn)
        if domain_errors:
            print("  ✗ FAIL: Domain integrity violations:")
            for error in domain_errors:
                print(f"    - {error}")
            conn.close()
            return 1
        print("  ✓ PASS: All domain constraints satisfied")
        print()
        
        # C3: Strong provenance (orphan check)
        print("C3: Strong Provenance Check")
        orphan_count, orphan_samples = find_orphans(conn)
        print(f"  Orphan rows (no swaps match): {orphan_count:,}")
        if orphan_count > 0:
            print("  ✗ FAIL: Orphan rows found (wallet_token_flow rows without swaps provenance)")
        else:
            print("  ✓ PASS: No orphan rows")
        print()
        
        # C4: Direction mapping validation
        print("C4: Direction Mapping Validation")
        mismatch_count, mismatch_samples = find_mapping_mismatches(conn)
        print(f"  Mapping mismatches: {mismatch_count:,}")
        if mismatch_count > 0:
            print("  ✗ FAIL: Direction mapping violations found")
        else:
            print("  ✓ PASS: All direction mappings correct")
        print()
        
        # C5: Strict parity check
        print("C5: Strict Parity Check")
        duplicates = find_duplicates(conn)
        print(f"  Qualifying swaps count: {qualifying_swaps:,}")
        print(f"  wallet_token_flow count: {flow_counts['total']:,}")
        print(f"  Difference: {flow_counts['total'] - qualifying_swaps:,}")
        print(f"  Duplicate groups in wallet_token_flow: {len(duplicates['flow_duplicates']):,}")
        print(f"  Duplicate groups in swaps: {len(duplicates['swaps_duplicates']):,}")
        
        parity_pass = flow_counts['total'] == qualifying_swaps
        if args.strict and not parity_pass:
            print("  ✗ FAIL: Row count mismatch (strict mode)")
        elif not args.strict and not parity_pass:
            print("  ⚠ WARN: Row count mismatch (non-strict mode)")
        else:
            print("  ✓ PASS: Exact parity")
        print()
        
        # Determine overall pass/fail
        overall_pass = (
            orphan_count == 0 and
            mismatch_count == 0 and
            len(domain_errors) == 0 and
            (parity_pass or not args.strict)
        )
        
        # Step D: Export artifacts
        print("STEP D: Exporting Artifacts")
        print("-" * 80)
        
        # D1: Summary JSON
        summary = {
            'timestamp': start_time.isoformat(),
            'database': args.db,
            'strict_mode': bool(args.strict),
            'counts': {
                'total_swaps': total_swaps,
                'qualifying_swaps': qualifying_swaps,
                'wallet_token_flow_total': flow_counts['total'],
                'wallet_token_flow_in': flow_counts['in'],
                'wallet_token_flow_out': flow_counts['out'],
                'orphans': orphan_count,
                'mapping_mismatches': mismatch_count,
                'flow_duplicate_groups': len(duplicates['flow_duplicates']),
                'swaps_duplicate_groups': len(duplicates['swaps_duplicates'])
            },
            'pass': overall_pass,
            'failures': []
        }
        
        if orphan_count > 0:
            summary['failures'].append('Orphan rows found')
        if mismatch_count > 0:
            summary['failures'].append('Direction mapping mismatches found')
        if domain_errors:
            summary['failures'].extend(domain_errors)
        if args.strict and not parity_pass:
            summary['failures'].append('Strict parity check failed')
        
        summary_path = os.path.join(args.outdir, 'accept_summary.json')
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"  ✓ {summary_path}")
        
        # D2: Qualifying swaps count
        swaps_count_path = os.path.join(args.outdir, 'swaps_qualifying_count.tsv')
        write_tsv(swaps_count_path, ['count'], [{'count': qualifying_swaps}])
        print(f"  ✓ {swaps_count_path}")
        
        # D3: Flow counts
        flow_counts_path = os.path.join(args.outdir, 'flow_counts.tsv')
        write_tsv(flow_counts_path, ['type', 'count'], [
            {'type': 'total', 'count': flow_counts['total']},
            {'type': 'in', 'count': flow_counts['in']},
            {'type': 'out', 'count': flow_counts['out']}
        ])
        print(f"  ✓ {flow_counts_path}")
        
        # D4: Orphan samples
        orphan_path = os.path.join(args.outdir, 'orphan_samples.tsv')
        write_tsv(orphan_path, 
                  ['signature', 'scan_wallet', 'token_mint', 'sol_direction', 
                   'token_amount_raw', 'flow_direction', 'block_time'],
                  orphan_samples)
        print(f"  ✓ {orphan_path} ({len(orphan_samples)} samples)")
        
        # D5: Mapping mismatch samples
        mismatch_path = os.path.join(args.outdir, 'mapping_mismatch_samples.tsv')
        write_tsv(mismatch_path,
                  ['signature', 'scan_wallet', 'token_mint', 'sol_direction',
                   'flow_direction', 'token_amount_raw', 'block_time'],
                  mismatch_samples)
        print(f"  ✓ {mismatch_path} ({len(mismatch_samples)} samples)")
        
        # D6: Duplicates report
        dup_path = os.path.join(args.outdir, 'duplicates_report.tsv')
        dup_rows = []
        for dup in duplicates['flow_duplicates']:
            dup_rows.append({
                'table': 'wallet_token_flow',
                'signature': dup['signature'],
                'scan_wallet': dup['scan_wallet'],
                'token_mint': dup['token_mint'],
                'sol_direction': '',
                'flow_direction': dup['flow_direction'],
                'token_amount_raw': '',
                'count': dup['count']
            })
        for dup in duplicates['swaps_duplicates']:
            dup_rows.append({
                'table': 'swaps',
                'signature': dup['signature'],
                'scan_wallet': dup['scan_wallet'],
                'token_mint': dup['token_mint'],
                'sol_direction': dup['sol_direction'],
                'flow_direction': '',
                'token_amount_raw': dup['token_amount_raw'],
                'count': dup['count']
            })
        write_tsv(dup_path,
                  ['table', 'signature', 'scan_wallet', 'token_mint', 'sol_direction',
                   'flow_direction', 'token_amount_raw', 'count'],
                  dup_rows)
        print(f"  ✓ {dup_path} ({len(dup_rows)} duplicate groups)")
        print()
        
        # Final summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("=" * 80)
        print("FINAL RESULT")
        print("=" * 80)
        if overall_pass:
            print("✓✓✓ ACCEPTANCE: PASS ✓✓✓")
            print("Phase 2.7 output is ACCEPTED")
        else:
            print("✗✗✗ ACCEPTANCE: FAIL ✗✗✗")
            print("Phase 2.7 output is REJECTED")
            print()
            print("Failure reasons:")
            for failure in summary['failures']:
                print(f"  - {failure}")
        print()
        print(f"Duration: {duration:.2f} seconds")
        print(f"Artifacts exported to: {args.outdir}")
        print("=" * 80)
    
    except Exception as e:
        print()
        print("=" * 80)
        print("FATAL ERROR")
        print("=" * 80)
        print(f"An unexpected error occurred: {e}")
        print()
        import traceback
        traceback.print_exc()
        return 1
    finally:
        conn.close()
    
    return 0 if overall_pass else 1


if __name__ == '__main__':
    exit(main())
