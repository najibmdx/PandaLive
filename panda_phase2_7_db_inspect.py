#!/usr/bin/env python3
"""
PANDA V4 Phase 2.7 DB Inspector: Evidence-based flow buildability analysis.
Provides hard YES/NO verdicts before any wallet_token_flow construction.
"""

import sqlite3
import argparse
import os
import json
import time
from collections import defaultdict


def ensure_outdir(outdir):
    """Create output directory if it doesn't exist."""
    if not os.path.exists(outdir):
        os.makedirs(outdir)
        print(f"[INFO] Created output directory: {outdir}")
    else:
        print(f"[INFO] Using output directory: {outdir}")


def inspect_spl_transfers_v2(cursor, outdir):
    """
    Inspect spl_transfers_v2 table semantics and buildability.
    Returns: dict with inspection results
    """
    print("\n" + "="*70)
    print("INSPECTING: spl_transfers_v2")
    print("="*70)
    
    results = {
        'table_exists': False,
        'total_rows': 0,
        'min_block_time': None,
        'max_block_time': None,
        'match_rates': {},
        'owner_fields': [],
        'verdict': 'NO',
        'verdict_reason': 'Not analyzed'
    }
    
    # Check if table exists
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='spl_transfers_v2'
    """)
    if not cursor.fetchone():
        print("[WARN] spl_transfers_v2 table does not exist")
        results['verdict_reason'] = 'Table does not exist'
        return results
    
    results['table_exists'] = True
    
    # Get table schema
    cursor.execute("PRAGMA table_info(spl_transfers_v2)")
    columns = [row[1] for row in cursor.fetchall()]
    print(f"[INFO] Columns: {', '.join(columns)}")
    
    # Basic counts
    cursor.execute("SELECT COUNT(*) FROM spl_transfers_v2")
    total_rows = cursor.fetchone()[0]
    results['total_rows'] = total_rows
    print(f"[INFO] Total rows: {total_rows:,}")
    
    if total_rows == 0:
        results['verdict_reason'] = 'Table is empty'
        return results
    
    # Block time range
    cursor.execute("""
        SELECT MIN(block_time), MAX(block_time) 
        FROM spl_transfers_v2 
        WHERE block_time IS NOT NULL
    """)
    min_bt, max_bt = cursor.fetchone()
    results['min_block_time'] = min_bt
    results['max_block_time'] = max_bt
    print(f"[INFO] Block time range: {min_bt} to {max_bt}")
    
    # Match rate analysis
    print("\n[ANALYZING] Match rates for direction attribution...")
    
    match_counts = {}
    
    # scan_wallet == from_addr
    if 'from_addr' in columns:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM spl_transfers_v2 
            WHERE scan_wallet IS NOT NULL 
              AND from_addr IS NOT NULL
              AND scan_wallet = from_addr
        """)
        match_counts['scan_wallet_eq_from_addr'] = cursor.fetchone()[0]
    
    # scan_wallet == to_addr
    if 'to_addr' in columns:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM spl_transfers_v2 
            WHERE scan_wallet IS NOT NULL 
              AND to_addr IS NOT NULL
              AND scan_wallet = to_addr
        """)
        match_counts['scan_wallet_eq_to_addr'] = cursor.fetchone()[0]
    
    # scan_wallet == source_owner
    if 'source_owner' in columns:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM spl_transfers_v2 
            WHERE scan_wallet IS NOT NULL 
              AND source_owner IS NOT NULL
              AND scan_wallet = source_owner
        """)
        match_counts['scan_wallet_eq_source_owner'] = cursor.fetchone()[0]
    
    # scan_wallet == authority
    if 'authority' in columns:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM spl_transfers_v2 
            WHERE scan_wallet IS NOT NULL 
              AND authority IS NOT NULL
              AND scan_wallet = authority
        """)
        match_counts['scan_wallet_eq_authority'] = cursor.fetchone()[0]
    
    results['match_rates'] = match_counts
    
    print("\nMatch Rate Summary:")
    for key, count in match_counts.items():
        pct = (count / total_rows * 100) if total_rows > 0 else 0
        print(f"  {key}: {count:,} ({pct:.2f}%)")
    
    # Search for destination owner fields
    print("\n[SEARCHING] Owner-related columns...")
    owner_fields = []
    for col in columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['owner', 'dest', 'destination']):
            owner_fields.append(col)
    
    results['owner_fields'] = owner_fields
    if owner_fields:
        print(f"[INFO] Found owner-related columns: {', '.join(owner_fields)}")
    else:
        print("[WARN] No owner-related columns found")
    
    # Check for destination_owner or dest_owner specifically
    has_dest_owner = any(col in columns for col in ['destination_owner', 'dest_owner'])
    
    # Sample non-matching rows to understand addresses
    print("\n[SAMPLING] Non-matching rows (ORDER BY block_time DESC LIMIT 50)...")
    cursor.execute("""
        SELECT scan_wallet, from_addr, to_addr, source_owner, authority, mint
        FROM spl_transfers_v2
        WHERE scan_wallet IS NOT NULL
          AND scan_wallet != COALESCE(from_addr, '')
          AND scan_wallet != COALESCE(to_addr, '')
        ORDER BY block_time DESC
        LIMIT 50
    """)
    samples = cursor.fetchall()
    
    # Write samples to TSV
    samples_path = os.path.join(outdir, 'transfers_samples.tsv')
    with open(samples_path, 'w', encoding='utf-8') as f:
        f.write("scan_wallet\tfrom_addr\tto_addr\tsource_owner\tauthority\tmint\n")
        for row in samples:
            f.write('\t'.join(str(x) if x else '' for x in row) + '\n')
    print(f"[OK] Wrote {len(samples)} sample rows to {samples_path}")
    
    # Distinct count analysis
    if samples:
        distinct_from = len(set(row[1] for row in samples if row[1]))
        distinct_to = len(set(row[2] for row in samples if row[2]))
        print(f"[INFO] Sample distinct from_addr: {distinct_from}, to_addr: {distinct_to}")
    
    # Write match rates to TSV
    match_rates_path = os.path.join(outdir, 'transfers_match_rates.tsv')
    with open(match_rates_path, 'w', encoding='utf-8') as f:
        f.write("field_comparison\tcount\tpercentage\n")
        for key, count in match_counts.items():
            pct = (count / total_rows * 100) if total_rows > 0 else 0
            f.write(f"{key}\t{count}\t{pct:.2f}\n")
    print(f"[OK] Wrote match rates to {match_rates_path}")
    
    # VERDICT LOGIC
    print("\n" + "="*70)
    print("VERDICT: TRANSFERS_FLOW_BUILDABLE")
    print("="*70)
    
    outflow_viable = match_counts.get('scan_wallet_eq_source_owner', 0) > 0
    inflow_viable = has_dest_owner or match_counts.get('scan_wallet_eq_to_addr', 0) > 0
    
    if not outflow_viable and not inflow_viable:
        results['verdict'] = 'NO'
        results['verdict_reason'] = 'No viable direction attribution: source_owner never matches scan_wallet AND no destination_owner field exists AND to_addr never matches scan_wallet'
        print(f"[VERDICT] NO")
        print(f"[REASON] {results['verdict_reason']}")
    elif not outflow_viable:
        results['verdict'] = 'PARTIAL'
        results['verdict_reason'] = 'Only inflow viable (source_owner never matches scan_wallet but destination owner or to_addr available)'
        print(f"[VERDICT] PARTIAL (inflow only)")
        print(f"[REASON] {results['verdict_reason']}")
    elif not inflow_viable:
        results['verdict'] = 'PARTIAL'
        results['verdict_reason'] = 'Only outflow viable (source_owner matches but no destination_owner field and to_addr never matches)'
        print(f"[VERDICT] PARTIAL (outflow only)")
        print(f"[REASON] {results['verdict_reason']}")
    else:
        results['verdict'] = 'YES'
        results['verdict_reason'] = f'Both directions viable: source_owner matches {match_counts.get("scan_wallet_eq_source_owner", 0)} rows, ' + \
                                   ('destination_owner exists' if has_dest_owner else f'to_addr matches {match_counts.get("scan_wallet_eq_to_addr", 0)} rows')
        print(f"[VERDICT] YES")
        print(f"[REASON] {results['verdict_reason']}")
    
    print("="*70)
    
    return results


def inspect_swaps_table(cursor, outdir):
    """
    Inspect swaps table viability for wallet_token_flow.
    Returns: dict with inspection results
    """
    print("\n" + "="*70)
    print("INSPECTING: swaps")
    print("="*70)
    
    results = {
        'table_exists': False,
        'total_rows': 0,
        'window_counts': {},
        'qualifying_pct': 0.0,
        'verdict': 'NO',
        'verdict_reason': 'Not analyzed'
    }
    
    # Check if table exists
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='swaps'
    """)
    if not cursor.fetchone():
        print("[WARN] swaps table does not exist")
        results['verdict_reason'] = 'Table does not exist'
        return results
    
    results['table_exists'] = True
    
    # Basic counts
    cursor.execute("SELECT COUNT(*) FROM swaps")
    total_rows = cursor.fetchone()[0]
    results['total_rows'] = total_rows
    print(f"[INFO] Total rows: {total_rows:,}")
    
    if total_rows == 0:
        results['verdict_reason'] = 'Table is empty'
        return results
    
    # Qualifying rows (for wallet_token_flow)
    cursor.execute("""
        SELECT COUNT(*) 
        FROM swaps
        WHERE block_time IS NOT NULL
          AND scan_wallet IS NOT NULL
          AND token_mint IS NOT NULL
          AND token_amount_raw IS NOT NULL
          AND sol_direction IN ('buy', 'sell')
    """)
    qualifying_rows = cursor.fetchone()[0]
    qualifying_pct = (qualifying_rows / total_rows * 100) if total_rows > 0 else 0
    results['qualifying_pct'] = qualifying_pct
    
    print(f"[INFO] Qualifying rows: {qualifying_rows:,} ({qualifying_pct:.2f}%)")
    
    # Try to get cohorts window anchors
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='cohorts'
    """)
    has_cohorts = cursor.fetchone() is not None
    
    window_counts = {}
    
    if has_cohorts:
        print("[INFO] cohorts table found - analyzing per window...")
        
        # Detect cohorts columns
        cursor.execute("PRAGMA table_info(cohorts)")
        cohorts_cols = [row[1] for row in cursor.fetchall()]
        
        window_col = 'window_kind' if 'window_kind' in cohorts_cols else 'window' if 'window' in cohorts_cols else None
        start_col = 'window_start_ts' if 'window_start_ts' in cohorts_cols else 'start_ts' if 'start_ts' in cohorts_cols else None
        end_col = 'window_end_ts' if 'window_end_ts' in cohorts_cols else 'end_ts' if 'end_ts' in cohorts_cols else None
        
        if window_col and start_col and end_col:
            # Get window anchors
            cursor.execute(f"""
                SELECT DISTINCT {window_col}, {start_col}, {end_col}
                FROM cohorts
                WHERE {window_col} IN ('24h', '7d', 'lifetime')
            """)
            windows = cursor.fetchall()
            
            for kind, start, end in windows:
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM swaps
                    WHERE block_time IS NOT NULL
                      AND block_time >= ?
                      AND block_time <= ?
                      AND scan_wallet IS NOT NULL
                      AND token_mint IS NOT NULL
                      AND token_amount_raw IS NOT NULL
                      AND sol_direction IN ('buy', 'sell')
                """, (start, end))
                count = cursor.fetchone()[0]
                window_counts[kind] = {'start': start, 'end': end, 'count': count}
                print(f"  Window '{kind}': {count:,} qualifying rows")
    
    results['window_counts'] = window_counts
    
    # Write window counts to TSV
    if window_counts:
        window_path = os.path.join(outdir, 'swaps_window_counts.tsv')
        with open(window_path, 'w', encoding='utf-8') as f:
            f.write("window_kind\tstart_ts\tend_ts\tqualifying_rows\n")
            for kind in ['24h', '7d', 'lifetime']:
                if kind in window_counts:
                    wc = window_counts[kind]
                    f.write(f"{kind}\t{wc['start']}\t{wc['end']}\t{wc['count']}\n")
        print(f"[OK] Wrote window counts to {window_path}")
    
    # VERDICT LOGIC
    print("\n" + "="*70)
    print("VERDICT: SWAPS_FLOW_BUILDABLE")
    print("="*70)
    
    if qualifying_rows == 0:
        results['verdict'] = 'NO'
        results['verdict_reason'] = 'No qualifying rows found (missing block_time, token_mint, token_amount_raw, or valid sol_direction)'
        print(f"[VERDICT] NO")
        print(f"[REASON] {results['verdict_reason']}")
    elif window_counts and all(wc['count'] > 0 for wc in window_counts.values()):
        results['verdict'] = 'YES'
        results['verdict_reason'] = f'All windows have qualifying rows: {qualifying_rows:,} total across {len(window_counts)} windows'
        print(f"[VERDICT] YES")
        print(f"[REASON] {results['verdict_reason']}")
    elif window_counts:
        zero_windows = [k for k, v in window_counts.items() if v['count'] == 0]
        results['verdict'] = 'PARTIAL'
        results['verdict_reason'] = f'Some windows have zero rows: {", ".join(zero_windows)}'
        print(f"[VERDICT] PARTIAL")
        print(f"[REASON] {results['verdict_reason']}")
    else:
        results['verdict'] = 'YES'
        results['verdict_reason'] = f'{qualifying_rows:,} qualifying rows found (windows not analyzed - no cohorts table)'
        print(f"[VERDICT] YES (global)")
        print(f"[REASON] {results['verdict_reason']}")
    
    print("="*70)
    
    return results


def run_inspection(db_path, outdir):
    """Main inspection function."""
    start_time = time.time()
    
    print("="*70)
    print("PANDA Phase 2.7 DB INSPECTOR")
    print("="*70)
    print(f"Database: {db_path}")
    print(f"Output directory: {outdir}")
    print("="*70)
    
    if not os.path.exists(db_path):
        print(f"[ERROR] Database not found: {db_path}")
        return 1
    
    ensure_outdir(outdir)
    
    conn = sqlite3.connect(db_path, timeout=30.0)
    cursor = conn.cursor()
    
    try:
        # Inspect spl_transfers_v2
        transfers_results = inspect_spl_transfers_v2(cursor, outdir)
        
        # Inspect swaps
        swaps_results = inspect_swaps_table(cursor, outdir)
        
        # Final summary
        summary = {
            'inspection_timestamp': int(time.time()),
            'database': db_path,
            'transfers': {
                'buildable': transfers_results['verdict'],
                'reason': transfers_results['verdict_reason'],
                'total_rows': transfers_results['total_rows'],
                'match_rates': transfers_results['match_rates']
            },
            'swaps': {
                'buildable': swaps_results['verdict'],
                'reason': swaps_results['verdict_reason'],
                'total_rows': swaps_results['total_rows'],
                'qualifying_pct': swaps_results['qualifying_pct']
            }
        }
        
        # Write summary JSON
        summary_path = os.path.join(outdir, 'inspect_summary.json')
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
        print(f"\n[OK] Wrote inspection summary to {summary_path}")
        
        # Print final verdict
        print("\n" + "="*70)
        print("FINAL VERDICTS")
        print("="*70)
        print(f"TRANSFERS_FLOW_BUILDABLE: {transfers_results['verdict']}")
        print(f"  Reason: {transfers_results['verdict_reason']}")
        print()
        print(f"SWAPS_FLOW_BUILDABLE: {swaps_results['verdict']}")
        print(f"  Reason: {swaps_results['verdict_reason']}")
        print("="*70)
        
        # Recommendations
        print("\nRECOMMENDATIONS:")
        if transfers_results['verdict'] == 'YES':
            print("  ✓ Use panda_phase2_7_tokenflow_transfers.py for transfer-based flows")
        elif transfers_results['verdict'] == 'PARTIAL':
            print("  ⚠ Transfer flows possible but limited (see reason above)")
        else:
            print("  ✗ Transfer flows NOT VIABLE - missing required owner fields")
        
        if swaps_results['verdict'] in ('YES', 'PARTIAL'):
            print("  ✓ Use panda_phase2_7_tokenflowCohorts.py for swap-based flows")
        else:
            print("  ✗ Swap flows NOT VIABLE")
        
        elapsed = time.time() - start_time
        print(f"\n[DONE] Inspection completed in {elapsed:.2f} seconds")
        
        return 0
        
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        cursor.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='PANDA Phase 2.7 DB Inspector: Evidence-based buildability analysis'
    )
    parser.add_argument(
        '--db',
        required=True,
        help='Path to SQLite database'
    )
    parser.add_argument(
        '--outdir',
        default='exports_phase2_7_inspect',
        help='Output directory for inspection files (default: exports_phase2_7_inspect)'
    )
    
    args = parser.parse_args()
    
    exit_code = run_inspection(args.db, args.outdir)
    exit(exit_code)


if __name__ == '__main__':
    main()
