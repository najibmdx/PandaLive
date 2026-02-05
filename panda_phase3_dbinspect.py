#!/usr/bin/env python3
"""
PANDA v4 Phase 3 Database Inspector
Read-only SQLite inspection with deterministic artifact generation.
"""

import sqlite3
import os
import json
import hashlib
import argparse
import csv
import sys
from datetime import datetime, timezone


def quote_identifier(identifier):
    """Quote SQL identifier to prevent injection. Doubles internal quotes."""
    # SQLite uses double quotes for identifiers
    return '"' + identifier.replace('"', '""') + '"'


def sha256_file(filepath):
    """Compute SHA256 hash of file."""
    h = hashlib.sha256()
    with open(filepath, 'rb') as f:
        chunk = f.read(8192)
        while chunk:
            h.update(chunk)
            chunk = f.read(8192)
    return h.hexdigest()


def get_file_mtime_utc(filepath):
    """Get file modification time as UTC ISO string."""
    mtime = os.path.getmtime(filepath)
    dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
    return dt.isoformat()


def get_now_utc_timestamp():
    """Get current UTC timestamp in seconds since epoch."""
    return int(datetime.now(timezone.utc).timestamp())


def write_tsv(filepath, headers, rows):
    """Write deterministic TSV file."""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)


def list_all_tables(conn):
    """Get sorted list of all tables."""
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    return [row[0] for row in cursor.fetchall()]


def get_row_count(conn, table_name):
    """Get row count for a table, return 0 if table doesn't exist."""
    try:
        quoted_table = quote_identifier(table_name)
        cursor = conn.execute(f"SELECT COUNT(*) FROM {quoted_table}")
        return cursor.fetchone()[0]
    except sqlite3.OperationalError:
        return 0


def table_exists(conn, table_name):
    """Check if table exists."""
    cursor = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return cursor.fetchone()[0] > 0


def get_table_info(conn, table_name):
    """Get PRAGMA table_info results."""
    quoted_table = quote_identifier(table_name)
    cursor = conn.execute(f"PRAGMA table_info({quoted_table})")
    return cursor.fetchall()


def get_all_columns_index(conn, tables):
    """Build index of all columns across all tables."""
    index = []
    for table in sorted(tables):
        try:
            info = get_table_info(conn, table)
            for row in info:
                # row: (cid, name, type, notnull, dflt_value, pk)
                index.append((table, row[1], row[2]))
        except sqlite3.OperationalError:
            pass
    return index


def column_exists(conn, table_name, column_substring):
    """Check if any column in table contains substring (case-insensitive)."""
    try:
        info = get_table_info(conn, table_name)
        for row in info:
            col_name = row[1].lower()
            if column_substring.lower() in col_name:
                return True, row[1]
    except sqlite3.OperationalError:
        pass
    return False, None


def find_exact_or_contains(conn, table_name, candidates):
    """Find first matching column from candidates list."""
    try:
        info = get_table_info(conn, table_name)
        col_names = [row[1] for row in info]
        col_names_lower = [c.lower() for c in col_names]
        
        for candidate in candidates:
            # Exact match first
            if candidate.lower() in col_names_lower:
                idx = col_names_lower.index(candidate.lower())
                return col_names[idx]
            # Contains match
            for actual in col_names:
                if candidate.lower() in actual.lower():
                    return actual
        return None
    except sqlite3.OperationalError:
        return None


def inspect_block_0(conn, db_path, summary):
    """[0] SNAPSHOT INTEGRITY"""
    print("\n" + "="*70)
    print("[0] SNAPSHOT INTEGRITY")
    print("="*70)
    
    # File metadata
    file_size = os.path.getsize(db_path)
    mtime = get_file_mtime_utc(db_path)
    sha256 = sha256_file(db_path)
    
    print(f"Database: {db_path}")
    print(f"Size: {file_size:,} bytes")
    print(f"Modified: {mtime}")
    print(f"SHA256: {sha256}")
    
    summary['db_file_path'] = db_path
    summary['file_size_bytes'] = file_size
    summary['mtime_utc_iso'] = mtime
    summary['sha256'] = sha256
    
    # List all tables
    all_tables = list_all_tables(conn)
    print(f"\nFound {len(all_tables)} tables:")
    for t in all_tables:
        print(f"  - {t}")
    
    summary['all_tables'] = all_tables
    
    # Key table row counts
    key_tables = [
        'swaps', 'wallet_token_flow', 'wallets', 'wallet_features',
        'wallet_clusters', 'wallet_edges', 'cohorts', 'cohort_members',
        'whale_states', 'whale_events', 'recycling_flags'
    ]
    
    row_counts = {}
    print("\nKey table row counts:")
    for table in key_tables:
        count = get_row_count(conn, table)
        row_counts[table] = count
        status = "✓" if count > 0 else "✗"
        print(f"  {status} {table}: {count:,}")
    
    summary['row_counts'] = row_counts
    
    # PASS criteria
    swaps_ok = row_counts.get('swaps', 0) > 0
    wtf_ok = row_counts.get('wallet_token_flow', 0) > 0
    passed = swaps_ok and wtf_ok
    
    print(f"\n[0] RESULT: {'PASS' if passed else 'FAIL'}")
    if not passed:
        print(f"  swaps exists and has rows: {swaps_ok}")
        print(f"  wallet_token_flow exists and has rows: {wtf_ok}")
    
    summary['inspections']['block_0_snapshot_integrity'] = {
        'pass': passed,
        'swaps_exists_nonempty': swaps_ok,
        'wallet_token_flow_exists_nonempty': wtf_ok
    }
    
    return passed


def inspect_block_1(conn, outdir, summary):
    """[1] DEPENDENCY MAP (TABLES + SCHEMA)"""
    print("\n" + "="*70)
    print("[1] DEPENDENCY MAP (TABLES + SCHEMA)")
    print("="*70)
    
    all_tables = list_all_tables(conn)
    
    # Expected tables
    expected = [
        'swaps', 'wallet_token_flow', 'wallets', 'wallet_features',
        'wallet_clusters', 'wallet_edges', 'cohorts', 'cohort_members',
        'whale_states', 'whale_events', 'recycling_flags'
    ]
    
    # phase3_tables.tsv
    table_rows = []
    for table in sorted(expected):
        exists = table_exists(conn, table)
        count = get_row_count(conn, table) if exists else 0
        notes = "present" if exists else "missing"
        table_rows.append([table, 1 if exists else 0, count, notes])
    
    tables_tsv = os.path.join(outdir, 'phase3_tables.tsv')
    write_tsv(tables_tsv, ['table_name', 'exists', 'row_count', 'notes'], table_rows)
    print(f"✓ Wrote {tables_tsv}")
    
    # phase3_schema_wallet_token_flow.tsv
    wtf_schema_rows = []
    wtf_schema_ok = False
    if table_exists(conn, 'wallet_token_flow'):
        info = get_table_info(conn, 'wallet_token_flow')
        for row in info:
            wtf_schema_rows.append(list(row))
        wtf_schema_ok = True
        print(f"✓ wallet_token_flow schema: {len(wtf_schema_rows)} columns")
    else:
        print("✗ wallet_token_flow table not found")
    
    wtf_schema_tsv = os.path.join(outdir, 'phase3_schema_wallet_token_flow.tsv')
    write_tsv(
        wtf_schema_tsv,
        ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        wtf_schema_rows
    )
    print(f"✓ Wrote {wtf_schema_tsv}")
    
    # phase3_schema_swaps.tsv
    swaps_schema_rows = []
    swaps_schema_ok = False
    if table_exists(conn, 'swaps'):
        info = get_table_info(conn, 'swaps')
        for row in info:
            swaps_schema_rows.append(list(row))
        swaps_schema_ok = True
        print(f"✓ swaps schema: {len(swaps_schema_rows)} columns")
    else:
        print("✗ swaps table not found")
    
    swaps_schema_tsv = os.path.join(outdir, 'phase3_schema_swaps.tsv')
    write_tsv(
        swaps_schema_tsv,
        ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'],
        swaps_schema_rows
    )
    print(f"✓ Wrote {swaps_schema_tsv}")
    
    # phase3_columns_index.tsv
    columns_index = get_all_columns_index(conn, all_tables)
    columns_tsv = os.path.join(outdir, 'phase3_columns_index.tsv')
    write_tsv(columns_tsv, ['table_name', 'column_name', 'type'], columns_index)
    print(f"✓ Wrote {columns_tsv} ({len(columns_index)} columns)")
    
    passed = wtf_schema_ok and swaps_schema_ok
    print(f"\n[1] RESULT: {'PASS' if passed else 'FAIL'}")
    
    summary['inspections']['block_1_dependency_map'] = {
        'pass': passed,
        'wallet_token_flow_schema_ok': wtf_schema_ok,
        'swaps_schema_ok': swaps_schema_ok,
        'total_columns_indexed': len(columns_index)
    }
    
    return passed


def inspect_block_2(conn, summary):
    """[2] wallet_token_flow SEMANTICS CONFIRMATION"""
    print("\n" + "="*70)
    print("[2] wallet_token_flow SEMANTICS CONFIRMATION")
    print("="*70)
    
    if not table_exists(conn, 'wallet_token_flow'):
        print("✗ wallet_token_flow table not found")
        summary['inspections']['block_2_semantics'] = {
            'pass': False,
            'table_exists': False
        }
        return False
    
    # Required semantic columns
    required = ['scan_wallet', 'token_mint', 'signature']
    optional = [
        'block_time', 'direction', 'sol_direction',
        'sol_amount', 'sol_amount_raw', 'sol_amount_lamports',
        'token_amount', 'token_amount_raw'
    ]
    
    semantic_map = {}
    
    print("\nRequired columns:")
    for col in required:
        exists, actual = column_exists(conn, 'wallet_token_flow', col)
        semantic_map[col] = {'exists': exists, 'actual_name': actual}
        status = "✓" if exists else "✗"
        print(f"  {status} {col}: {actual if exists else 'NOT FOUND'}")
    
    print("\nOptional columns:")
    for col in optional:
        exists, actual = column_exists(conn, 'wallet_token_flow', col)
        semantic_map[col] = {'exists': exists, 'actual_name': actual}
        status = "✓" if exists else "✗"
        print(f"  {status} {col}: {actual if exists else 'NOT FOUND'}")
    
    summary['semantic_map'] = semantic_map
    
    # PASS if all required columns exist
    passed = all(semantic_map[col]['exists'] for col in required)
    
    print(f"\n[2] RESULT: {'PASS' if passed else 'FAIL'}")
    if not passed:
        missing = [col for col in required if not semantic_map[col]['exists']]
        print(f"  Missing required columns: {missing}")
    
    summary['inspections']['block_2_semantics'] = {
        'pass': passed,
        'table_exists': True,
        'required_columns_found': sum(1 for col in required if semantic_map[col]['exists']),
        'required_columns_total': len(required)
    }
    
    return passed


def inspect_block_3(conn, outdir, summary):
    """[3] WINDOWABILITY AUDIT"""
    print("\n" + "="*70)
    print("[3] WINDOWABILITY AUDIT")
    print("="*70)
    
    now_utc = get_now_utc_timestamp()
    future_threshold = now_utc + 86400
    
    audit_rows = []
    
    for table in ['wallet_token_flow', 'swaps']:
        if not table_exists(conn, table):
            print(f"✗ {table} not found, skipping")
            continue
        
        print(f"\n{table}:")
        
        # Find time columns
        time_col = find_exact_or_contains(conn, table, ['block_time'])
        if not time_col:
            # Find any column with 'time'
            info = get_table_info(conn, table)
            for row in info:
                if 'time' in row[1].lower():
                    time_col = row[1]
                    break
        
        if not time_col:
            print(f"  ✗ No time column found")
            continue
        
        print(f"  Time column: {time_col}")
        
        # Analyze time column
        try:
            quoted_col = quote_identifier(time_col)
            quoted_table = quote_identifier(table)
            cursor = conn.execute(f"""
                SELECT 
                    MIN({quoted_col}) as min_val,
                    MAX({quoted_col}) as max_val,
                    SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END) as null_count,
                    SUM(CASE WHEN {quoted_col} IS NOT NULL THEN 1 ELSE 0 END) as non_null_count,
                    SUM(CASE WHEN {quoted_col} <= 0 OR {quoted_col} > {future_threshold} THEN 1 ELSE 0 END) as outlier_count
                FROM {quoted_table}
            """)
            result = cursor.fetchone()
            
            min_val, max_val, null_count, non_null_count, outlier_count = result
            
            audit_rows.append([
                time_col, min_val, max_val, null_count, non_null_count, outlier_count
            ])
            
            print(f"    Min: {min_val}")
            print(f"    Max: {max_val}")
            print(f"    Null: {null_count:,}")
            print(f"    Non-null: {non_null_count:,}")
            print(f"    Outliers: {outlier_count:,}")
            
        except sqlite3.OperationalError as e:
            print(f"  ✗ Error analyzing {time_col}: {e}")
    
    # Write audit results
    audit_tsv = os.path.join(outdir, 'phase3_time_audit_wallet_token_flow.tsv')
    write_tsv(
        audit_tsv,
        ['time_column', 'min_value', 'max_value', 'null_count', 'non_null_count', 'outlier_count'],
        audit_rows
    )
    print(f"\n✓ Wrote {audit_tsv}")
    
    # PASS if at least one non-null time column with no outliers
    passed = any(
        row[4] > 0 and row[5] == 0  # non_null_count > 0 and outlier_count == 0
        for row in audit_rows
    )
    
    print(f"\n[3] RESULT: {'PASS' if passed else 'FAIL'}")
    
    summary['inspections']['block_3_windowability'] = {
        'pass': passed,
        'time_columns_analyzed': len(audit_rows),
        'has_valid_time_column': passed
    }
    summary['timestamp_audit'] = [
        {
            'time_column': row[0],
            'min_value': row[1],
            'max_value': row[2],
            'null_count': row[3],
            'non_null_count': row[4],
            'outlier_count': row[5]
        }
        for row in audit_rows
    ]
    
    return passed


def inspect_block_4(conn, outdir, summary):
    """[4] CARDINALITY / UNIQUENESS CHECKS"""
    print("\n" + "="*70)
    print("[4] CARDINALITY / UNIQUENESS CHECKS")
    print("="*70)
    
    uniqueness_rows = []
    all_passed = True
    
    # Check 1: swaps by signature
    if table_exists(conn, 'swaps'):
        print("\nChecking swaps for duplicate signatures...")
        try:
            cursor = conn.execute("""
                SELECT signature, COUNT(*) as cnt
                FROM swaps
                GROUP BY signature
                HAVING cnt > 1
                ORDER BY cnt DESC, signature
                LIMIT 5
            """)
            duplicates = cursor.fetchall()
            dup_count = len(duplicates)
            
            sample_key = duplicates[0][0] if duplicates else None
            uniqueness_rows.append([
                'swaps_signature_dupes', 'swaps', 'signature', dup_count, sample_key
            ])
            
            if dup_count > 0:
                all_passed = False
                print(f"  ✗ Found {dup_count} duplicate signature groups")
                for sig, cnt in duplicates[:5]:
                    print(f"    {sig}: {cnt} occurrences")
            else:
                print(f"  ✓ No duplicate signatures")
        except sqlite3.OperationalError as e:
            print(f"  ✗ Error: {e}")
            uniqueness_rows.append(['swaps_signature_dupes', 'swaps', 'signature', -1, 'ERROR'])
    
    # Check 2: wallet_token_flow composite key
    if table_exists(conn, 'wallet_token_flow'):
        print("\nChecking wallet_token_flow for duplicate composite keys...")
        
        # Build composite key from available columns
        key_parts = []
        semantic_map = summary.get('semantic_map', {})
        
        for col_name in ['signature', 'scan_wallet', 'token_mint']:
            if semantic_map.get(col_name, {}).get('exists'):
                actual = semantic_map[col_name]['actual_name']
                key_parts.append(actual)
        
        # Add direction if exists
        for dir_col in ['direction', 'sol_direction']:
            if semantic_map.get(dir_col, {}).get('exists'):
                actual = semantic_map[dir_col]['actual_name']
                key_parts.append(actual)
                break
        
        # Add sol_amount if exists
        for amt_col in ['sol_amount', 'sol_amount_raw', 'sol_amount_lamports']:
            if semantic_map.get(amt_col, {}).get('exists'):
                actual = semantic_map[amt_col]['actual_name']
                key_parts.append(actual)
                break
        
        if len(key_parts) >= 3:
            # Quote each identifier for SQL safety
            quoted_parts = [quote_identifier(col) for col in key_parts]
            key_expr = ', '.join(quoted_parts)
            key_expr_display = ', '.join(key_parts)
            print(f"  Key: ({key_expr_display})")
            
            try:
                # Count duplicate groups
                cursor = conn.execute(f"""
                    SELECT {key_expr}, COUNT(*) as cnt
                    FROM wallet_token_flow
                    GROUP BY {key_expr}
                    HAVING cnt > 1
                    ORDER BY cnt DESC
                    LIMIT 5
                """)
                duplicates = cursor.fetchall()
                dup_count = len(duplicates)
                
                sample_key = str(duplicates[0][:-1]) if duplicates else None
                uniqueness_rows.append([
                    'wtf_composite_dupes', 'wallet_token_flow', key_expr_display, dup_count, sample_key
                ])
                
                if dup_count > 0:
                    all_passed = False
                    print(f"  ✗ Found {dup_count} duplicate composite key groups")
                    for dup in duplicates[:3]:
                        print(f"    {dup}")
                else:
                    print(f"  ✓ No duplicate composite keys")
            except sqlite3.OperationalError as e:
                print(f"  ✗ Error: {e}")
                uniqueness_rows.append(['wtf_composite_dupes', 'wallet_token_flow', key_expr_display, -1, 'ERROR'])
        else:
            print(f"  ✗ Insufficient columns for composite key check")
    
    # Write uniqueness results
    uniqueness_tsv = os.path.join(outdir, 'phase3_uniqueness_checks.tsv')
    write_tsv(
        uniqueness_tsv,
        ['check_name', 'table_name', 'key_expression', 'duplicate_group_count', 'sample_key'],
        uniqueness_rows
    )
    print(f"\n✓ Wrote {uniqueness_tsv}")
    
    print(f"\n[4] RESULT: {'PASS' if all_passed else 'FAIL'}")
    
    summary['inspections']['block_4_uniqueness'] = {
        'pass': all_passed,
        'checks_performed': len(uniqueness_rows)
    }
    summary['duplication_counts'] = [
        {
            'check_name': row[0],
            'table_name': row[1],
            'key_expression': row[2],
            'duplicate_group_count': row[3],
            'sample_key': row[4]
        }
        for row in uniqueness_rows
    ]
    
    return all_passed


def inspect_block_5(conn, summary):
    """[5] UNIT & TYPE SANITY"""
    print("\n" + "="*70)
    print("[5] UNIT & TYPE SANITY (WHALE-THRESHOLD RISK)")
    print("="*70)
    
    all_tables = list_all_tables(conn)
    
    usd_keywords = ['usd', 'price', 'value', 'dollar']
    findings = []
    
    for table in all_tables:
        try:
            info = get_table_info(conn, table)
            for row in info:
                col_name = row[1]
                col_type = row[2]
                col_lower = col_name.lower()
                
                for keyword in usd_keywords:
                    if keyword in col_lower:
                        findings.append({
                            'table': table,
                            'column': col_name,
                            'type': col_type,
                            'keyword': keyword
                        })
                        print(f"  ⚠ {table}.{col_name} ({col_type}) - contains '{keyword}'")
        except sqlite3.OperationalError:
            pass
    
    has_usd_columns = len(findings) > 0
    
    print(f"\nTotal USD-related columns found: {len(findings)}")
    print(f"ANY USD-denominated column exists: {has_usd_columns}")
    
    summary['inspections']['block_5_unit_sanity'] = {
        'pass': True,  # Always pass (informational)
        'usd_columns_found': len(findings),
        'has_any_usd_column': has_usd_columns
    }
    summary['unit_sanity_findings'] = findings
    
    print(f"\n[5] RESULT: PASS (informational)")
    
    return True


def inspect_block_6(conn, outdir, summary):
    """[6] PHASE-3 TABLE CONTAMINATION CHECK"""
    print("\n" + "="*70)
    print("[6] PHASE-3 TABLE CONTAMINATION CHECK")
    print("="*70)
    
    phase3_tables = [
        'wallet_features', 'whale_states', 'whale_events',
        'wallet_clusters', 'wallet_edges'
    ]
    
    suspicious_keywords = [
        'sniper', 'smart', 'alpha', 'regime', 'stage',
        'score', 'signal', 'confidence'
    ]
    
    contamination_rows = []
    all_clean = True
    
    for table in phase3_tables:
        if not table_exists(conn, table):
            print(f"\n{table}: does not exist (OK)")
            continue
        
        row_count = get_row_count(conn, table)
        print(f"\n{table}: {row_count:,} rows")
        
        try:
            info = get_table_info(conn, table)
            suspicious_cols = []
            
            for col_row in info:
                col_name = col_row[1]
                col_lower = col_name.lower()
                
                for keyword in suspicious_keywords:
                    if keyword in col_lower:
                        suspicious_cols.append({
                            'column': col_name,
                            'keyword': keyword
                        })
                        print(f"  ⚠ SUSPICIOUS: {col_name} (contains '{keyword}')")
            
            if row_count > 0 or suspicious_cols:
                all_clean = False
                for sus in suspicious_cols:
                    contamination_rows.append([
                        table, row_count, sus['column'], f"contains '{sus['keyword']}'"
                    ])
                if row_count > 0 and not suspicious_cols:
                    contamination_rows.append([
                        table, row_count, 'N/A', 'table has rows but no suspicious columns'
                    ])
            else:
                print(f"  ✓ Empty and no suspicious columns")
                
        except sqlite3.OperationalError as e:
            print(f"  ✗ Error: {e}")
    
    # Write contamination results
    contamination_tsv = os.path.join(outdir, 'phase3_contamination_scan.tsv')
    write_tsv(
        contamination_tsv,
        ['table_name', 'row_count', 'suspicious_column', 'reason'],
        contamination_rows
    )
    print(f"\n✓ Wrote {contamination_tsv}")
    
    if not all_clean:
        print("\n⚠ CONTAMINATION DETECTED - RECOMMEND NUKE-AND-REBUILD")
    
    print(f"\n[6] RESULT: {'PASS' if all_clean else 'FAIL'}")
    
    summary['inspections']['block_6_contamination'] = {
        'pass': all_clean,
        'contaminated_tables': len(contamination_rows)
    }
    summary['contamination_findings'] = [
        {
            'table_name': row[0],
            'row_count': row[1],
            'suspicious_column': row[2],
            'reason': row[3]
        }
        for row in contamination_rows
    ]
    
    return all_clean


def main():
    parser = argparse.ArgumentParser(
        description='PANDA v4 Phase 3 Database Inspector (READ-ONLY)'
    )
    parser.add_argument('--db', required=True, help='Path to SQLite database file')
    parser.add_argument('--outdir', required=True, help='Output directory for artifacts')
    
    args = parser.parse_args()
    
    db_path = args.db
    outdir = args.outdir
    
    # Validate inputs
    if not os.path.isfile(db_path):
        print(f"ERROR: Database file not found: {db_path}", file=sys.stderr)
        sys.exit(1)
    
    os.makedirs(outdir, exist_ok=True)
    
    print("="*70)
    print("PANDA v4 PHASE 3 DATABASE INSPECTOR")
    print("="*70)
    print(f"Database: {db_path}")
    print(f"Output: {outdir}")
    
    # Initialize summary
    summary = {
        'inspections': {},
        'detected_semantic_columns': [],
        'timestamp_audit': [],
        'duplication_counts': [],
        'unit_sanity_findings': [],
        'contamination_findings': []
    }
    
    # Connect to database (read-only)
    conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    
    try:
        # Run all inspection blocks
        results = []
        results.append(inspect_block_0(conn, db_path, summary))
        results.append(inspect_block_1(conn, outdir, summary))
        results.append(inspect_block_2(conn, summary))
        results.append(inspect_block_3(conn, outdir, summary))
        results.append(inspect_block_4(conn, outdir, summary))
        results.append(inspect_block_5(conn, summary))
        results.append(inspect_block_6(conn, outdir, summary))
        
        # Overall result
        overall_pass = all(results)
        summary['OVERALL_PASS'] = overall_pass
        
        # Write summary JSON
        summary_json = os.path.join(outdir, 'phase3_inspect_pack.summary.json')
        with open(summary_json, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, sort_keys=True)
        
        print("\n" + "="*70)
        print("INSPECTION COMPLETE")
        print("="*70)
        print(f"Overall result: {'PASS' if overall_pass else 'FAIL'}")
        print(f"\nSummary written to: {summary_json}")
        
        if not overall_pass:
            print("\nFailed inspections:")
            for i, passed in enumerate(results):
                if not passed:
                    print(f"  - Block {i}")
        
        conn.close()
        
        sys.exit(0 if overall_pass else 1)
        
    except Exception as e:
        print(f"\nFATAL ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        conn.close()
        sys.exit(1)


if __name__ == '__main__':
    main()
