#!/usr/bin/env python3
"""
panda_phase2_7_tokenflow.py

PANDA v4 Phase 2.7: Token Flow Table Builder
Builds wallet_token_flow table from swaps table with deterministic, idempotent logic.
"""

import sqlite3
import argparse
import sys
import time
import json
from pathlib import Path
from typing import Dict, List, Tuple


def check_swaps_schema(conn: sqlite3.Connection) -> Dict[str, str]:
    """Inspect swaps table schema and verify required columns exist."""
    cursor = conn.cursor()
    
    # Get schema
    cursor.execute("PRAGMA table_info(swaps)")
    columns = {row[1]: row[2] for row in cursor.fetchall()}
    
    if not columns:
        print("ERROR: swaps table does not exist or has no columns", file=sys.stderr)
        sys.exit(1)
    
    # Required columns
    required = {
        'scan_wallet', 'signature', 'block_time', 'dex',
        'sol_direction', 'sol_amount_lamports', 'token_mint', 'token_amount_raw'
    }
    
    missing = required - set(columns.keys())
    if missing:
        print(f"ERROR: swaps table missing required columns: {missing}", file=sys.stderr)
        print(f"Available columns: {list(columns.keys())}", file=sys.stderr)
        sys.exit(1)
    
    print(f"✓ Schema validation passed. Found {len(columns)} columns in swaps table.")
    return columns


def get_qualifying_swaps(conn: sqlite3.Connection) -> Tuple[List[Tuple], Dict[str, int]]:
    """
    Fetch qualifying swaps rows and return them with filter statistics.
    """
    cursor = conn.cursor()
    
    # Get total count
    cursor.execute("SELECT COUNT(*) FROM swaps")
    total_swaps = cursor.fetchone()[0]
    
    # Build query with all filters
    query = """
    SELECT 
        signature,
        scan_wallet,
        block_time,
        dex,
        token_mint,
        token_amount_raw,
        sol_direction,
        sol_amount_lamports
    FROM swaps
    WHERE 
        sol_direction IN ('buy', 'sell')
        AND token_mint IS NOT NULL 
        AND token_mint != ''
        AND token_amount_raw IS NOT NULL 
        AND token_amount_raw > 0
        AND scan_wallet IS NOT NULL 
        AND scan_wallet != ''
        AND signature IS NOT NULL 
        AND signature != ''
        AND block_time IS NOT NULL
    """
    
    cursor.execute(query)
    qualifying_rows = cursor.fetchall()
    
    # Calculate filter stats
    stats = {
        'total_swaps': total_swaps,
        'qualifying_rows': len(qualifying_rows),
        'filtered_out': total_swaps - len(qualifying_rows)
    }
    
    # Get breakdown of filtered rows
    cursor.execute("SELECT COUNT(*) FROM swaps WHERE sol_direction NOT IN ('buy', 'sell') OR sol_direction IS NULL")
    stats['invalid_sol_direction'] = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM swaps WHERE token_mint IS NULL OR token_mint = ''")
    stats['missing_token_mint'] = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM swaps WHERE token_amount_raw IS NULL OR token_amount_raw <= 0")
    stats['invalid_token_amount'] = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM swaps WHERE scan_wallet IS NULL OR scan_wallet = ''")
    stats['missing_scan_wallet'] = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM swaps WHERE signature IS NULL OR signature = ''")
    stats['missing_signature'] = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM swaps WHERE block_time IS NULL")
    stats['missing_block_time'] = cursor.fetchone()[0]
    
    return qualifying_rows, stats


def create_wallet_token_flow_table(conn: sqlite3.Connection, mode: str):
    """Create or recreate wallet_token_flow table."""
    cursor = conn.cursor()
    
    if mode == 'replace':
        cursor.execute("DROP TABLE IF EXISTS wallet_token_flow")
        print("✓ Dropped existing wallet_token_flow table (replace mode)")
    
    create_sql = """
    CREATE TABLE IF NOT EXISTS wallet_token_flow (
        signature TEXT NOT NULL,
        scan_wallet TEXT NOT NULL,
        block_time INTEGER NOT NULL,
        dex TEXT,
        token_mint TEXT NOT NULL,
        token_amount_raw INTEGER NOT NULL,
        flow_direction TEXT NOT NULL,
        sol_direction TEXT NOT NULL,
        sol_amount_lamports INTEGER,
        source_table TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (signature, scan_wallet, token_mint, flow_direction)
    )
    """
    
    cursor.execute(create_sql)
    conn.commit()
    print("✓ Created wallet_token_flow table")


def transform_and_insert(conn: sqlite3.Connection, swaps_rows: List[Tuple], mode: str) -> Dict[str, int]:
    """
    Transform swaps rows to token flow records and insert.
    Returns insertion statistics.
    """
    cursor = conn.cursor()
    created_at = int(time.time())
    
    # Transform rows: map sol_direction to flow_direction
    flow_records = []
    for row in swaps_rows:
        signature, scan_wallet, block_time, dex, token_mint, token_amount_raw, sol_direction, sol_amount_lamports = row
        
        # Map sol_direction to flow_direction
        flow_direction = 'in' if sol_direction == 'buy' else 'out'
        
        flow_records.append((
            signature,
            scan_wallet,
            block_time,
            dex,
            token_mint,
            token_amount_raw,
            flow_direction,
            sol_direction,
            sol_amount_lamports,
            'swaps',
            created_at
        ))
    
    # Insert with conflict handling
    insert_sql = """
    INSERT OR IGNORE INTO wallet_token_flow (
        signature, scan_wallet, block_time, dex, token_mint, 
        token_amount_raw, flow_direction, sol_direction, 
        sol_amount_lamports, source_table, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    cursor.executemany(insert_sql, flow_records)
    conn.commit()
    
    # Get actual count - executemany rowcount is unreliable
    cursor.execute("SELECT COUNT(*) FROM wallet_token_flow WHERE created_at = ?", (created_at,))
    rows_inserted = cursor.fetchone()[0]
    
    stats = {
        'records_transformed': len(flow_records),
        'rows_inserted': rows_inserted,
        'duplicates_ignored': len(flow_records) - rows_inserted
    }
    
    return stats


def validate_output(conn: sqlite3.Connection, expected_qualifying: int) -> bool:
    """Run validation checks on wallet_token_flow table."""
    cursor = conn.cursor()
    
    print("\n=== VALIDATION CHECKS ===")
    all_passed = True
    
    # Check 1: Row count > 0
    cursor.execute("SELECT COUNT(*) FROM wallet_token_flow")
    row_count = cursor.fetchone()[0]
    
    if expected_qualifying > 0 and row_count == 0:
        print("✗ FAIL: wallet_token_flow has 0 rows but expected > 0")
        all_passed = False
    else:
        print(f"✓ Row count check passed: {row_count} rows")
    
    # Check 2: No NULLs in NOT NULL columns
    null_checks = [
        ('signature', 'signature IS NULL'),
        ('scan_wallet', 'scan_wallet IS NULL'),
        ('block_time', 'block_time IS NULL'),
        ('token_mint', 'token_mint IS NULL'),
        ('token_amount_raw', 'token_amount_raw IS NULL'),
        ('flow_direction', 'flow_direction IS NULL'),
        ('sol_direction', 'sol_direction IS NULL'),
        ('source_table', 'source_table IS NULL'),
        ('created_at', 'created_at IS NULL')
    ]
    
    for col_name, condition in null_checks:
        cursor.execute(f"SELECT COUNT(*) FROM wallet_token_flow WHERE {condition}")
        null_count = cursor.fetchone()[0]
        if null_count > 0:
            print(f"✗ FAIL: {col_name} has {null_count} NULL values")
            all_passed = False
    
    if all_passed:
        print("✓ NULL constraint checks passed")
    
    # Check 3: flow_direction only 'in' or 'out'
    cursor.execute("SELECT COUNT(*) FROM wallet_token_flow WHERE flow_direction NOT IN ('in', 'out')")
    invalid_flow = cursor.fetchone()[0]
    if invalid_flow > 0:
        print(f"✗ FAIL: {invalid_flow} rows have invalid flow_direction")
        all_passed = False
    else:
        print("✓ flow_direction values valid")
    
    # Check 4: sol_direction only 'buy' or 'sell'
    cursor.execute("SELECT COUNT(*) FROM wallet_token_flow WHERE sol_direction NOT IN ('buy', 'sell')")
    invalid_sol = cursor.fetchone()[0]
    if invalid_sol > 0:
        print(f"✗ FAIL: {invalid_sol} rows have invalid sol_direction")
        all_passed = False
    else:
        print("✓ sol_direction values valid")
    
    # Check 5: All signatures exist in swaps
    cursor.execute("""
        SELECT COUNT(*) FROM wallet_token_flow wtf
        WHERE NOT EXISTS (SELECT 1 FROM swaps s WHERE s.signature = wtf.signature)
    """)
    orphan_sigs = cursor.fetchone()[0]
    if orphan_sigs > 0:
        print(f"✗ FAIL: {orphan_sigs} signatures don't exist in swaps")
        all_passed = False
    else:
        print("✓ All signatures exist in swaps table")
    
    return all_passed


def generate_summary_stats(conn: sqlite3.Connection) -> Dict:
    """Generate comprehensive statistics for output."""
    cursor = conn.cursor()
    
    stats = {}
    
    # Total rows
    cursor.execute("SELECT COUNT(*) FROM wallet_token_flow")
    stats['total_rows'] = cursor.fetchone()[0]
    
    # Distinct wallets
    cursor.execute("SELECT COUNT(DISTINCT scan_wallet) FROM wallet_token_flow")
    stats['distinct_wallets'] = cursor.fetchone()[0]
    
    # Distinct token mints
    cursor.execute("SELECT COUNT(DISTINCT token_mint) FROM wallet_token_flow")
    stats['distinct_tokens'] = cursor.fetchone()[0]
    
    # Block time range
    cursor.execute("SELECT MIN(block_time), MAX(block_time) FROM wallet_token_flow")
    min_time, max_time = cursor.fetchone()
    stats['min_block_time'] = min_time
    stats['max_block_time'] = max_time
    
    # Flow direction counts
    cursor.execute("SELECT flow_direction, COUNT(*) FROM wallet_token_flow GROUP BY flow_direction")
    flow_counts = dict(cursor.fetchall())
    stats['flow_in_count'] = flow_counts.get('in', 0)
    stats['flow_out_count'] = flow_counts.get('out', 0)
    
    # Sol direction counts
    cursor.execute("SELECT sol_direction, COUNT(*) FROM wallet_token_flow GROUP BY sol_direction")
    sol_counts = dict(cursor.fetchall())
    stats['sol_buy_count'] = sol_counts.get('buy', 0)
    stats['sol_sell_count'] = sol_counts.get('sell', 0)
    
    return stats


def export_summaries(conn: sqlite3.Connection, output_dir: Path, build_stats: Dict):
    """Export TSV summaries and JSON build report."""
    output_dir.mkdir(exist_ok=True)
    cursor = conn.cursor()
    
    # 1. Counts by wallet
    cursor.execute("""
        SELECT 
            scan_wallet,
            COUNT(*) as total_rows,
            SUM(CASE WHEN flow_direction = 'in' THEN 1 ELSE 0 END) as in_rows,
            SUM(CASE WHEN flow_direction = 'out' THEN 1 ELSE 0 END) as out_rows
        FROM wallet_token_flow
        GROUP BY scan_wallet
        ORDER BY total_rows DESC
    """)
    
    with open(output_dir / 'wallet_token_flow_counts_by_wallet.tsv', 'w') as f:
        f.write("scan_wallet\trows\tin_rows\tout_rows\n")
        for row in cursor.fetchall():
            f.write(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\n")
    
    # 2. Counts by mint
    cursor.execute("""
        SELECT 
            token_mint,
            COUNT(*) as total_rows,
            SUM(CASE WHEN flow_direction = 'in' THEN 1 ELSE 0 END) as in_rows,
            SUM(CASE WHEN flow_direction = 'out' THEN 1 ELSE 0 END) as out_rows
        FROM wallet_token_flow
        GROUP BY token_mint
        ORDER BY total_rows DESC
    """)
    
    with open(output_dir / 'wallet_token_flow_counts_by_mint.tsv', 'w') as f:
        f.write("token_mint\trows\tin_rows\tout_rows\n")
        for row in cursor.fetchall():
            f.write(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\n")
    
    # 3. Time range
    cursor.execute("""
        SELECT MIN(block_time), MAX(block_time), COUNT(*)
        FROM wallet_token_flow
    """)
    
    with open(output_dir / 'wallet_token_flow_time_range.tsv', 'w') as f:
        f.write("min_block_time\tmax_block_time\trows\n")
        row = cursor.fetchone()
        f.write(f"{row[0]}\t{row[1]}\t{row[2]}\n")
    
    # 4. JSON summary
    with open(output_dir / 'build_summary.json', 'w') as f:
        json.dump(build_stats, f, indent=2)
    
    print(f"\n✓ Exported summaries to {output_dir}/")


def main():
    parser = argparse.ArgumentParser(description='PANDA Phase 2.7: Build wallet_token_flow table')
    parser.add_argument('--db', required=True, help='Path to masterwalletsdb.db')
    parser.add_argument('--mode', default='replace', choices=['replace', 'upsert'],
                        help='Build mode (default: replace)')
    
    args = parser.parse_args()
    
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"ERROR: Database file not found: {db_path}", file=sys.stderr)
        sys.exit(1)
    
    print(f"=== PANDA Phase 2.7: Token Flow Builder ===")
    print(f"Database: {db_path}")
    print(f"Mode: {args.mode}\n")
    
    # Connect
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    
    try:
        # Step 1: Verify schema
        print("Step 1: Schema Discovery")
        check_swaps_schema(conn)
        
        # Step 2: Get qualifying swaps
        print("\nStep 2: Fetching Qualifying Swaps")
        swaps_rows, filter_stats = get_qualifying_swaps(conn)
        
        print(f"  Total swaps rows: {filter_stats['total_swaps']:,}")
        print(f"  Qualifying rows: {filter_stats['qualifying_rows']:,}")
        print(f"  Filtered out: {filter_stats['filtered_out']:,}")
        print(f"    - Invalid sol_direction: {filter_stats['invalid_sol_direction']:,}")
        print(f"    - Missing token_mint: {filter_stats['missing_token_mint']:,}")
        print(f"    - Invalid token_amount: {filter_stats['invalid_token_amount']:,}")
        print(f"    - Missing scan_wallet: {filter_stats['missing_scan_wallet']:,}")
        print(f"    - Missing signature: {filter_stats['missing_signature']:,}")
        print(f"    - Missing block_time: {filter_stats['missing_block_time']:,}")
        
        # Step 3: Create table
        print("\nStep 3: Creating Table")
        create_wallet_token_flow_table(conn, args.mode)
        
        # Step 4: Transform and insert
        print("\nStep 4: Transforming and Inserting Records")
        insert_stats = transform_and_insert(conn, swaps_rows, args.mode)
        
        print(f"  Records transformed: {insert_stats['records_transformed']:,}")
        print(f"  Rows inserted: {insert_stats['rows_inserted']:,}")
        print(f"  Duplicates ignored: {insert_stats['duplicates_ignored']:,}")
        
        # Step 5: Generate stats
        print("\nStep 5: Generating Statistics")
        summary_stats = generate_summary_stats(conn)
        
        print(f"  Total rows in wallet_token_flow: {summary_stats['total_rows']:,}")
        print(f"  Distinct wallets: {summary_stats['distinct_wallets']:,}")
        print(f"  Distinct tokens: {summary_stats['distinct_tokens']:,}")
        print(f"  Block time range: {summary_stats['min_block_time']} to {summary_stats['max_block_time']}")
        print(f"  Flow direction - IN: {summary_stats['flow_in_count']:,}, OUT: {summary_stats['flow_out_count']:,}")
        print(f"  Sol direction - BUY: {summary_stats['sol_buy_count']:,}, SELL: {summary_stats['sol_sell_count']:,}")
        
        # Step 6: Validation
        validation_passed = validate_output(conn, filter_stats['qualifying_rows'])
        
        if not validation_passed:
            print("\n✗ VALIDATION FAILED", file=sys.stderr)
            sys.exit(1)
        
        print("\n✓ All validation checks passed")
        
        # Step 7: Export summaries
        output_dir = Path('exports_phase2_7_build')
        
        build_stats = {
            'timestamp': int(time.time()),
            'database': str(db_path),
            'mode': args.mode,
            'filter_stats': filter_stats,
            'insert_stats': insert_stats,
            'summary_stats': summary_stats,
            'validation_passed': validation_passed
        }
        
        export_summaries(conn, output_dir, build_stats)
        
        print("\n=== BUILD COMPLETE ===")
        print("✓ wallet_token_flow table successfully built and validated")
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        conn.close()


if __name__ == '__main__':
    main()
