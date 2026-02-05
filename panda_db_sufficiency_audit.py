#!/usr/bin/env python3
"""
panda_db_sufficiency_audit.py

STRICT PASS/FAIL audit for SQLite DB sufficiency under PANDA v4 zero-guesswork doctrine.
Validates whether the database is sufficient for "data-proven edge mining".
"""

import argparse
import sqlite3
import sys
import hashlib


# STRICT THRESHOLDS
RAW_JSON_MIN_PCT = 0.95
TIME_ANCHOR_MIN_PCT = 0.99
MAX_GAP_HOURS = 6
STRICT_DEDUPE_TOLERANCE = 0.0  # Zero tolerance for duplicates


class AuditResult:
    def __init__(self, name):
        self.name = name
        self.passed = False
        self.evidence = []
        self.warnings = []
    
    def mark_pass(self, evidence=""):
        self.passed = True
        if evidence:
            self.evidence.append(evidence)
    
    def mark_fail(self, evidence=""):
        self.passed = False
        if evidence:
            self.evidence.append(evidence)
    
    def add_warning(self, warning):
        self.warnings.append(warning)
    
    def status(self):
        return "PASS" if self.passed else "FAIL"


class DBSufficiencyAuditor:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        self.results = {}
        self.evidence = {}
        
    def connect(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            return True
        except Exception as e:
            print(f"ERROR: Cannot connect to database: {e}")
            return False
    
    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def get_table_columns(self, table_name):
        """Get list of columns for a table."""
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        return [row[1] for row in cursor.fetchall()]
    
    def table_exists(self, table_name):
        """Check if a table exists."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        return cursor.fetchone() is not None
    
    def audit_1_tables_exist(self):
        """Requirement 1: Tables exist (files, swaps, spl_transfers)."""
        result = AuditResult("Tables Exist")
        required_tables = ['files', 'swaps', 'spl_transfers']
        
        missing = []
        for table in required_tables:
            if not self.table_exists(table):
                missing.append(table)
        
        if missing:
            result.mark_fail(f"Missing tables: {', '.join(missing)}")
        else:
            result.mark_pass(f"All required tables present: {', '.join(required_tables)}")
        
        self.results['1_tables_exist'] = result
    
    def audit_2_row_counts(self):
        """Requirement 2: Row counts per table."""
        cursor = self.conn.cursor()
        
        tables = ['files', 'swaps', 'spl_transfers']
        counts = {}
        
        for table in tables:
            if self.table_exists(table):
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                counts[table] = count
            else:
                counts[table] = 0
        
        self.evidence['row_counts'] = counts
    
    def audit_3_raw_truth_availability(self):
        """
        PATCH A: RAW TRUTH AVAILABILITY - strict gate.
        Must have raw_json >= 95% in at least one table.
        """
        result = AuditResult("Raw Truth Availability")
        cursor = self.conn.cursor()
        
        raw_stats = {}
        has_sufficient_raw = False
        
        # Check swaps.raw_json
        if self.table_exists('swaps'):
            cols = self.get_table_columns('swaps')
            if 'raw_json' in cols:
                cursor.execute("SELECT COUNT(*) FROM swaps")
                total = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM swaps WHERE raw_json IS NOT NULL AND raw_json != ''")
                non_null = cursor.fetchone()[0]
                
                pct = (non_null / total) if total > 0 else 0.0
                raw_stats['swaps'] = {
                    'total': total,
                    'non_null': non_null,
                    'pct': pct,
                    'has_column': True
                }
                if total > 0 and pct >= RAW_JSON_MIN_PCT:
                    has_sufficient_raw = True
        
        # Check spl_transfers.raw_json
        if self.table_exists('spl_transfers'):
            cols = self.get_table_columns('spl_transfers')
            if 'raw_json' in cols:
                cursor.execute("SELECT COUNT(*) FROM spl_transfers")
                total = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM spl_transfers WHERE raw_json IS NOT NULL AND raw_json != ''")
                non_null = cursor.fetchone()[0]
                
                pct = (non_null / total) if total > 0 else 0.0
                raw_stats['spl_transfers'] = {
                    'total': total,
                    'non_null': non_null,
                    'pct': pct,
                    'has_column': True
                }
                if total > 0 and pct >= RAW_JSON_MIN_PCT:
                    has_sufficient_raw = True
        
        self.evidence['raw_truth'] = raw_stats
        
        # STRICT EVALUATION
        if not raw_stats:
            result.mark_fail(f"Neither swaps nor spl_transfers has raw_json column")
        elif not has_sufficient_raw:
            evidence_parts = []
            for table, stats in raw_stats.items():
                if stats['total'] == 0:
                    evidence_parts.append(
                        f"{table}: 0 rows (empty table with raw_json column)"
                    )
                else:
                    evidence_parts.append(
                        f"{table}: {stats['non_null']}/{stats['total']} "
                        f"({stats['pct']*100:.2f}% < {RAW_JSON_MIN_PCT*100:.0f}% required)"
                    )
            result.mark_fail("; ".join(evidence_parts))
        else:
            evidence_parts = []
            for table, stats in raw_stats.items():
                status = "✓" if stats['total'] > 0 and stats['pct'] >= RAW_JSON_MIN_PCT else "✗"
                evidence_parts.append(
                    f"{table}: {stats['non_null']}/{stats['total']} "
                    f"({stats['pct']*100:.2f}%) {status}"
                )
            result.mark_pass("; ".join(evidence_parts))
        
        self.results['3_raw_truth'] = result
    
    def audit_4_time_anchor(self):
        """
        PATCH B: TIME ANCHOR - strict chain-time requirement.
        Must have block_time OR slot >= 99% non-null. No ingested_at fallback.
        """
        result = AuditResult("Time Anchor (Chain-Time)")
        cursor = self.conn.cursor()
        
        time_stats = {}
        all_valid = True
        
        # Check swaps
        if self.table_exists('swaps'):
            cols = self.get_table_columns('swaps')
            
            cursor.execute(f"SELECT COUNT(*) FROM swaps")
            total = cursor.fetchone()[0]
            
            # Empty table is invalid
            if total == 0:
                time_stats['swaps'] = {
                    'field': 'NONE',
                    'total': 0,
                    'valid': False
                }
                all_valid = False
            else:
                time_field = None
                if 'block_time' in cols:
                    time_field = 'block_time'
                elif 'slot' in cols:
                    time_field = 'slot'
                
                if time_field:
                    cursor.execute(f"SELECT COUNT(*) FROM swaps WHERE {time_field} IS NOT NULL")
                    non_null = cursor.fetchone()[0]
                    
                    pct = (non_null / total)
                    
                    # Get min/max
                    cursor.execute(f"SELECT MIN({time_field}), MAX({time_field}) FROM swaps WHERE {time_field} IS NOT NULL")
                    row = cursor.fetchone()
                    min_time, max_time = row[0], row[1]
                    
                    time_stats['swaps'] = {
                        'field': time_field,
                        'total': total,
                        'non_null': non_null,
                        'pct': pct,
                        'min': min_time,
                        'max': max_time,
                        'valid': pct >= TIME_ANCHOR_MIN_PCT
                    }
                    
                    if pct < TIME_ANCHOR_MIN_PCT:
                        all_valid = False
                else:
                    time_stats['swaps'] = {
                        'field': 'NONE',
                        'total': total,
                        'valid': False
                    }
                    all_valid = False
        
        # Check spl_transfers
        if self.table_exists('spl_transfers'):
            cols = self.get_table_columns('spl_transfers')
            
            cursor.execute(f"SELECT COUNT(*) FROM spl_transfers")
            total = cursor.fetchone()[0]
            
            # Empty table is invalid
            if total == 0:
                time_stats['spl_transfers'] = {
                    'field': 'NONE',
                    'total': 0,
                    'valid': False
                }
                all_valid = False
            else:
                time_field = None
                if 'block_time' in cols:
                    time_field = 'block_time'
                elif 'slot' in cols:
                    time_field = 'slot'
                
                if time_field:
                    cursor.execute(f"SELECT COUNT(*) FROM spl_transfers WHERE {time_field} IS NOT NULL")
                    non_null = cursor.fetchone()[0]
                    
                    pct = (non_null / total)
                    
                    # Get min/max
                    cursor.execute(f"SELECT MIN({time_field}), MAX({time_field}) FROM spl_transfers WHERE {time_field} IS NOT NULL")
                    row = cursor.fetchone()
                    min_time, max_time = row[0], row[1]
                    
                    time_stats['spl_transfers'] = {
                        'field': time_field,
                        'total': total,
                        'non_null': non_null,
                        'pct': pct,
                        'min': min_time,
                        'max': max_time,
                        'valid': pct >= TIME_ANCHOR_MIN_PCT
                    }
                    
                    if pct < TIME_ANCHOR_MIN_PCT:
                        all_valid = False
                else:
                    time_stats['spl_transfers'] = {
                        'field': 'NONE',
                        'total': total,
                        'valid': False
                    }
                    all_valid = False
        
        self.evidence['time_stats'] = time_stats
        
        # STRICT EVALUATION
        if not all_valid:
            evidence_parts = []
            for table, stats in time_stats.items():
                if stats.get('total') == 0:
                    evidence_parts.append(f"{table}: 0 rows (empty table)")
                elif stats['field'] == 'NONE':
                    evidence_parts.append(f"{table}: NO chain-time field (block_time/slot)")
                elif not stats['valid']:
                    evidence_parts.append(
                        f"{table}.{stats['field']}: {stats['pct']*100:.2f}% < {TIME_ANCHOR_MIN_PCT*100:.0f}% required"
                    )
            result.mark_fail("; ".join(evidence_parts))
        else:
            evidence_parts = []
            for table, stats in time_stats.items():
                if stats['field'] != 'NONE' and stats.get('total', 0) > 0:
                    evidence_parts.append(
                        f"{table}.{stats['field']}: {stats['pct']*100:.2f}% non-null ✓"
                    )
            result.mark_pass("; ".join(evidence_parts))
        
        self.results['4_time_anchor'] = result
    
    def audit_5_key_field_completeness(self):
        """
        PATCH 4: KEY FIELD COMPLETENESS - strict NULL-key rate gates.
        All key fields must be 100% non-null for zero-guesswork identity.
        """
        result = AuditResult("Key Field Completeness")
        cursor = self.conn.cursor()
        
        key_stats = {}
        all_complete = True
        missing_columns = []
        
        # Check swaps key fields: scan_wallet, signature
        if self.table_exists('swaps'):
            cols = self.get_table_columns('swaps')
            
            required_swaps = ['scan_wallet', 'signature']
            missing_swaps = [col for col in required_swaps if col not in cols]
            
            if missing_swaps:
                missing_columns.append(f"swaps missing columns: {', '.join(missing_swaps)}")
                all_complete = False
            else:
                cursor.execute("SELECT COUNT(*) FROM swaps")
                total = cursor.fetchone()[0]
                
                swaps_fields = {}
                for field in required_swaps:
                    cursor.execute(f"SELECT COUNT(*) FROM swaps WHERE {field} IS NOT NULL")
                    non_null = cursor.fetchone()[0]
                    pct = (non_null / total) if total > 0 else 0.0
                    swaps_fields[field] = {
                        'total': total,
                        'non_null': non_null,
                        'pct': pct,
                        'complete': pct == 1.0
                    }
                    if pct < 1.0:
                        all_complete = False
                
                key_stats['swaps'] = swaps_fields
        
        # Check spl_transfers key fields
        if self.table_exists('spl_transfers'):
            cols = self.get_table_columns('spl_transfers')
            
            required_spl = ['scan_wallet', 'signature', 'mint', 'direction', 'amount', 'from_addr', 'to_addr']
            missing_spl = [col for col in required_spl if col not in cols]
            
            if missing_spl:
                missing_columns.append(f"spl_transfers missing columns: {', '.join(missing_spl)}")
                all_complete = False
            else:
                cursor.execute("SELECT COUNT(*) FROM spl_transfers")
                total = cursor.fetchone()[0]
                
                spl_fields = {}
                for field in required_spl:
                    cursor.execute(f"SELECT COUNT(*) FROM spl_transfers WHERE {field} IS NOT NULL")
                    non_null = cursor.fetchone()[0]
                    pct = (non_null / total) if total > 0 else 0.0
                    spl_fields[field] = {
                        'total': total,
                        'non_null': non_null,
                        'pct': pct,
                        'complete': pct == 1.0
                    }
                    if pct < 1.0:
                        all_complete = False
                
                key_stats['spl_transfers'] = spl_fields
        
        self.evidence['key_stats'] = key_stats
        
        # STRICT EVALUATION - 100% completeness required
        if missing_columns:
            result.mark_fail("; ".join(missing_columns))
        elif not all_complete:
            evidence_parts = []
            for table, fields in key_stats.items():
                for field, stats in fields.items():
                    if not stats['complete']:
                        null_count = stats['total'] - stats['non_null']
                        evidence_parts.append(
                            f"{table}.{field}: {null_count} NULLs ({(1.0-stats['pct'])*100:.4f}%)"
                        )
            result.mark_fail("; ".join(evidence_parts))
        else:
            evidence_parts = []
            for table, fields in key_stats.items():
                complete_fields = [f for f, s in fields.items() if s['complete']]
                if complete_fields:
                    evidence_parts.append(f"{table}: {len(complete_fields)} key fields 100% complete")
            result.mark_pass("; ".join(evidence_parts))
        
        self.results['5_key_completeness'] = result
    
    def audit_6_coverage_gaps(self):
        """
        PATCH C: COVERAGE GAP MEASUREMENT - real gap test (report-only).
        Compute actual gaps between consecutive events per wallet.
        """
        cursor = self.conn.cursor()
        
        gap_analysis = {}
        
        # Get time field from evidence
        time_stats = self.evidence.get('time_stats', {})
        
        # Analyze swaps
        if self.table_exists('swaps') and 'swaps' in time_stats:
            stats = time_stats['swaps']
            if stats.get('field') and stats['field'] != 'NONE':
                time_field = stats['field']
                cols = self.get_table_columns('swaps')
                
                if 'scan_wallet' in cols:
                    # Get all wallets
                    cursor.execute(f"""
                        SELECT DISTINCT scan_wallet
                        FROM swaps
                        WHERE scan_wallet IS NOT NULL AND {time_field} IS NOT NULL
                    """)
                    wallets = [row[0] for row in cursor.fetchall()]
                    
                    for wallet in wallets:
                        # Get ordered events for this wallet
                        cursor.execute(f"""
                            SELECT {time_field}
                            FROM swaps
                            WHERE scan_wallet = ? AND {time_field} IS NOT NULL
                            ORDER BY {time_field}
                        """, (wallet,))
                        
                        times = [float(row[0]) for row in cursor.fetchall()]
                        
                        if len(times) >= 2:
                            # Compute gaps
                            gaps = [times[i+1] - times[i] for i in range(len(times)-1)]
                            max_gap = max(gaps) if gaps else 0
                            
                            if wallet not in gap_analysis:
                                gap_analysis[wallet] = {}
                            
                            gap_analysis[wallet]['swaps'] = {
                                'event_count': len(times),
                                'min_time': times[0],
                                'max_time': times[-1],
                                'max_gap_seconds': max_gap
                            }
        
        # Analyze spl_transfers
        if self.table_exists('spl_transfers') and 'spl_transfers' in time_stats:
            stats = time_stats['spl_transfers']
            if stats.get('field') and stats['field'] != 'NONE':
                time_field = stats['field']
                cols = self.get_table_columns('spl_transfers')
                
                if 'scan_wallet' in cols:
                    # Get all wallets
                    cursor.execute(f"""
                        SELECT DISTINCT scan_wallet
                        FROM spl_transfers
                        WHERE scan_wallet IS NOT NULL AND {time_field} IS NOT NULL
                    """)
                    wallets = [row[0] for row in cursor.fetchall()]
                    
                    for wallet in wallets:
                        # Get ordered events for this wallet
                        cursor.execute(f"""
                            SELECT {time_field}
                            FROM spl_transfers
                            WHERE scan_wallet = ? AND {time_field} IS NOT NULL
                            ORDER BY {time_field}
                        """, (wallet,))
                        
                        times = [float(row[0]) for row in cursor.fetchall()]
                        
                        if len(times) >= 2:
                            # Compute gaps
                            gaps = [times[i+1] - times[i] for i in range(len(times)-1)]
                            max_gap = max(gaps) if gaps else 0
                            
                            if wallet not in gap_analysis:
                                gap_analysis[wallet] = {}
                            
                            gap_analysis[wallet]['spl_transfers'] = {
                                'event_count': len(times),
                                'min_time': times[0],
                                'max_time': times[-1],
                                'max_gap_seconds': max_gap
                            }
        
        # Find wallets with gaps > 6 hours
        max_gap_threshold = MAX_GAP_HOURS * 3600
        wallets_with_gaps = []
        
        for wallet, data in gap_analysis.items():
            for table_name, stats in data.items():
                if stats['max_gap_seconds'] > max_gap_threshold:
                    wallets_with_gaps.append({
                        'wallet': wallet,
                        'table': table_name,
                        'max_gap_hours': stats['max_gap_seconds'] / 3600,
                        'event_count': stats['event_count']
                    })
        
        # Sort by max_gap descending
        wallets_with_gaps.sort(key=lambda x: x['max_gap_hours'], reverse=True)
        
        self.evidence['gap_analysis'] = {
            'total_wallets': len(gap_analysis),
            'wallets_with_gaps': len(set(w['wallet'] for w in wallets_with_gaps)),
            'top_gaps': wallets_with_gaps[:20]
        }
    
    def audit_7_dedupe_strict(self):
        """
        PATCH D: DEDUPE - enforce stable minimum keys, zero tolerance.
        Compute duplicates over FULL table and report null key exclusions.
        swaps: group by (scan_wallet, signature) - ZERO duplicates allowed.
        spl_transfers: group by (scan_wallet, signature, mint, direction, amount, from_addr, to_addr) - ZERO allowed.
        """
        result = AuditResult("Dedupe (Strict)")
        cursor = self.conn.cursor()
        
        dedupe_stats = {}
        all_clean = True
        
        # Check swaps - STRICT: group by (scan_wallet, signature)
        if self.table_exists('swaps'):
            cols = self.get_table_columns('swaps')
            
            if 'scan_wallet' in cols and 'signature' in cols:
                cursor.execute("SELECT COUNT(*) FROM swaps")
                total = cursor.fetchone()[0]
                
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM swaps
                    WHERE scan_wallet IS NOT NULL AND signature IS NOT NULL
                """)
                total_non_null = cursor.fetchone()[0]
                
                cursor.execute("""
                    SELECT COUNT(DISTINCT scan_wallet || '|' || signature)
                    FROM swaps
                    WHERE scan_wallet IS NOT NULL AND signature IS NOT NULL
                """)
                distinct = cursor.fetchone()[0]
                
                dup_rows = total_non_null - distinct
                dup_pct = (dup_rows / total) if total > 0 else 0
                null_excluded = total - total_non_null
                
                dedupe_stats['swaps'] = {
                    'total': total,
                    'non_null_keys': total_non_null,
                    'null_excluded': null_excluded,
                    'distinct': distinct,
                    'dup_rows': dup_rows,
                    'dup_pct': dup_pct,
                    'clean': dup_rows == 0
                }
                
                if dup_rows > 0:
                    all_clean = False
        
        # Check spl_transfers - group by full key
        if self.table_exists('spl_transfers'):
            cols = self.get_table_columns('spl_transfers')
            required_cols = ['scan_wallet', 'signature', 'mint', 'direction',
                           'amount', 'from_addr', 'to_addr']
            
            if all(col in cols for col in required_cols):
                cursor.execute("SELECT COUNT(*) FROM spl_transfers")
                total = cursor.fetchone()[0]
                
                # Require ALL key fields to be non-null (full-key defensive)
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM spl_transfers
                    WHERE scan_wallet IS NOT NULL 
                      AND signature IS NOT NULL
                      AND mint IS NOT NULL
                      AND direction IS NOT NULL
                      AND amount IS NOT NULL
                      AND from_addr IS NOT NULL
                      AND to_addr IS NOT NULL
                """)
                total_non_null = cursor.fetchone()[0]
                
                # DISTINCT also requires all fields non-null
                cursor.execute("""
                    SELECT COUNT(DISTINCT 
                        scan_wallet || '|' || signature || '|' || mint || '|' || 
                        direction || '|' || amount || '|' || from_addr || '|' || to_addr
                    )
                    FROM spl_transfers
                    WHERE scan_wallet IS NOT NULL 
                      AND signature IS NOT NULL
                      AND mint IS NOT NULL
                      AND direction IS NOT NULL
                      AND amount IS NOT NULL
                      AND from_addr IS NOT NULL
                      AND to_addr IS NOT NULL
                """)
                distinct = cursor.fetchone()[0]
                
                dup_rows = total_non_null - distinct
                dup_pct = (dup_rows / total) if total > 0 else 0
                null_excluded = total - total_non_null
                
                dedupe_stats['spl_transfers'] = {
                    'total': total,
                    'non_null_keys': total_non_null,
                    'null_excluded': null_excluded,
                    'distinct': distinct,
                    'dup_rows': dup_rows,
                    'dup_pct': dup_pct,
                    'clean': dup_rows == 0
                }
                
                if dup_rows > 0:
                    all_clean = False
        
        self.evidence['dedupe_stats'] = dedupe_stats
        
        # STRICT EVALUATION - ZERO tolerance
        if not all_clean:
            evidence_parts = []
            for table, stats in dedupe_stats.items():
                if not stats['clean']:
                    evidence_parts.append(
                        f"{table}: {stats['dup_rows']} duplicates "
                        f"({stats['dup_pct']*100:.4f}%) ✗"
                    )
            result.mark_fail("; ".join(evidence_parts))
        else:
            evidence_parts = []
            for table, stats in dedupe_stats.items():
                evidence_parts.append(
                    f"{table}: {stats['distinct']}/{stats['total']} distinct (0 duplicates) ✓"
                )
            result.mark_pass("; ".join(evidence_parts))
        
        self.results['7_dedupe'] = result
    
    def audit_8_replay_friendliness(self):
        """
        PATCH E: REPLAY FRIENDLINESS - meaningful test.
        Export top 1000 rows deterministically, hash, reconnect, repeat, compare.
        """
        result = AuditResult("Replay Friendliness")
        
        def table_exists_in_conn(conn, table_name):
            """Check if table exists using given connection."""
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            return cursor.fetchone() is not None
        
        def get_table_columns_in_conn(conn, table_name):
            """Get columns using given connection."""
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            return [row[1] for row in cursor.fetchall()]
        
        def create_deterministic_export(conn):
            cursor = conn.cursor()
            export_parts = []
            exported_rows = 0  # Track whether any rows were exported
            
            # Export from swaps
            if table_exists_in_conn(conn, 'swaps'):
                cols = get_table_columns_in_conn(conn, 'swaps')
                time_field = 'block_time' if 'block_time' in cols else ('slot' if 'slot' in cols else None)
                
                select_cols = ['scan_wallet', 'signature', 'token_mint', 'in_mint', 'out_mint', 
                             'in_amount_raw', 'out_amount_raw']
                if time_field:
                    select_cols.append(time_field)
                
                # Only select columns that exist
                actual_cols = [c for c in select_cols if c in cols]
                
                # Fail if table exists but no required export columns
                if not actual_cols:
                    raise ValueError("swaps: missing required export columns")
                
                # Determine order columns
                order_cols_candidates = ['scan_wallet', 'signature', 'token_mint', 
                                        'in_mint', 'out_mint', 'in_amount_raw', 
                                        'out_amount_raw']
                order_cols_list = [c for c in order_cols_candidates if c in cols]
                
                if not order_cols_list:
                    raise ValueError("swaps: no ordering columns available for deterministic export")
                
                cols_str = ', '.join(actual_cols)
                order_cols = ', '.join(order_cols_list)
                
                cursor.execute(f"""
                    SELECT {cols_str}
                    FROM swaps
                    ORDER BY {order_cols}
                    LIMIT 1000
                """)
                
                for row in cursor.fetchall():
                    export_parts.append('swaps:' + '|'.join(str(v) if v is not None else '' for v in row))
                    exported_rows += 1
            
            # Export from spl_transfers
            if table_exists_in_conn(conn, 'spl_transfers'):
                cols = get_table_columns_in_conn(conn, 'spl_transfers')
                time_field = 'block_time' if 'block_time' in cols else ('slot' if 'slot' in cols else None)
                
                select_cols = ['scan_wallet', 'signature', 'mint', 'direction', 'amount', 
                             'from_addr', 'to_addr']
                if time_field:
                    select_cols.append(time_field)
                
                # Only select columns that exist
                actual_cols = [c for c in select_cols if c in cols]
                
                # Fail if table exists but no required export columns
                if not actual_cols:
                    raise ValueError("spl_transfers: missing required export columns")
                
                # Determine order columns
                order_cols_candidates = ['scan_wallet', 'signature', 'mint', 
                                        'direction', 'amount', 'from_addr', 
                                        'to_addr']
                order_cols_list = [c for c in order_cols_candidates if c in cols]
                
                if not order_cols_list:
                    raise ValueError("spl_transfers: no ordering columns available for deterministic export")
                
                cols_str = ', '.join(actual_cols)
                order_cols = ', '.join(order_cols_list)
                
                cursor.execute(f"""
                    SELECT {cols_str}
                    FROM spl_transfers
                    ORDER BY {order_cols}
                    LIMIT 1000
                """)
                
                for row in cursor.fetchall():
                    export_parts.append('spl_transfers:' + '|'.join(str(v) if v is not None else '' for v in row))
                    exported_rows += 1
            
            # Fail if no rows were exported
            if exported_rows == 0:
                raise ValueError("Replay export is empty (0 rows exported) — cannot prove replay determinism")
            
            combined = "\n".join(export_parts)
            return hashlib.sha256(combined.encode()).hexdigest()
        
        # First export with current connection
        try:
            hash1 = create_deterministic_export(self.conn)
        except ValueError as e:
            result.mark_fail(f"Cannot create deterministic export: {e}")
            self.results['8_replay'] = result
            return
        except Exception as e:
            result.mark_fail(f"Export error: {e}")
            self.results['8_replay'] = result
            return
        
        # Close and reconnect
        self.close()
        if not self.connect():
            result.mark_fail("Cannot reconnect for replay test")
            self.results['8_replay'] = result
            return
        
        # Second export with new connection
        try:
            hash2 = create_deterministic_export(self.conn)
        except ValueError as e:
            result.mark_fail(f"Cannot create deterministic export on replay: {e}")
            self.results['8_replay'] = result
            return
        except Exception as e:
            result.mark_fail(f"Replay export error: {e}")
            self.results['8_replay'] = result
            return
        
        self.evidence['replay_hashes'] = {
            'hash1': hash1,
            'hash2': hash2,
            'match': hash1 == hash2
        }
        
        if hash1 == hash2:
            result.mark_pass(f"Deterministic (hash: {hash1[:16]}...)")
        else:
            result.mark_fail(f"Non-deterministic (hash1: {hash1[:16]}..., hash2: {hash2[:16]}...)")
        
        self.results['8_replay'] = result
    
    def audit_9_cross_token_linkage(self):
        """Cross-token linkage (informational)."""
        cursor = self.conn.cursor()
        
        token_stats = {}
        
        # Check swaps.token_mint
        if self.table_exists('swaps'):
            cols = self.get_table_columns('swaps')
            if 'token_mint' in cols:
                cursor.execute(
                    "SELECT COUNT(DISTINCT token_mint) FROM swaps WHERE token_mint IS NOT NULL"
                )
                count = cursor.fetchone()[0]
                token_stats['swaps_token_mint'] = count
        
        # Check spl_transfers.mint
        if self.table_exists('spl_transfers'):
            cols = self.get_table_columns('spl_transfers')
            if 'mint' in cols:
                cursor.execute(
                    "SELECT COUNT(DISTINCT mint) FROM spl_transfers WHERE mint IS NOT NULL"
                )
                count = cursor.fetchone()[0]
                token_stats['spl_transfers_mint'] = count
        
        self.evidence['token_stats'] = token_stats
    
    def run_audit(self):
        """Run all audit checks."""
        if not self.connect():
            return False
        
        try:
            self.audit_1_tables_exist()
            
            # Only proceed if tables exist
            if not self.results['1_tables_exist'].passed:
                return True
            
            self.audit_2_row_counts()
            self.audit_3_raw_truth_availability()
            self.audit_4_time_anchor()
            self.audit_5_key_field_completeness()  # NEW decisive requirement
            self.audit_6_coverage_gaps()  # Report-only
            self.audit_7_dedupe_strict()
            self.audit_8_replay_friendliness()
            self.audit_9_cross_token_linkage()  # Informational
            
            return True
        except Exception as e:
            print(f"ERROR during audit: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            self.close()
    
    def print_report(self):
        """Print the audit report."""
        print("=" * 80)
        print(f"PANDA v4 DATABASE SUFFICIENCY AUDIT")
        print(f"Zero-Guesswork Doctrine: Data-Proven Edge Mining")
        print(f"Database: {self.db_path}")
        print("=" * 80)
        print()
        
        # Print requirements table
        print("REQUIREMENTS CHECK:")
        print("-" * 80)
        
        # Decisive requirements (affect PASS/FAIL)
        decisive_requirements = [
            ('1_tables_exist', '1. Tables Exist'),
            ('3_raw_truth', '2. Raw Truth Availability (>= 95%)'),
            ('4_time_anchor', '3. Time Anchor (Chain-Time >= 99%)'),
            ('5_key_completeness', '4. Key Field Completeness (100%)'),
            ('7_dedupe', '5. Dedupe (Strict, Zero Tolerance)'),
            ('8_replay', '6. Replay Friendliness'),
        ]
        
        for key, name in decisive_requirements:
            if key in self.results:
                r = self.results[key]
                status = r.status()
                print(f"{name:50} {status:6}")
                
                for evidence in r.evidence:
                    print(f"  └─ {evidence}")
                
                for warning in r.warnings:
                    print(f"  ⚠  {warning}")
        
        print("-" * 80)
        print()
        
        # Print evidence section
        print("EVIDENCE DETAILS:")
        print("-" * 80)
        
        # Row counts
        if 'row_counts' in self.evidence:
            print("Row Counts:")
            for table, count in self.evidence['row_counts'].items():
                print(f"  {table}: {count:,}")
            print()
        
        # Raw truth stats
        if 'raw_truth' in self.evidence and self.evidence['raw_truth']:
            print(f"Raw Truth (raw_json, threshold: {RAW_JSON_MIN_PCT*100:.0f}%):")
            for table, stats in self.evidence['raw_truth'].items():
                status = "✓ PASS" if stats['pct'] >= RAW_JSON_MIN_PCT else "✗ FAIL"
                print(f"  {table}: {stats['non_null']:,}/{stats['total']:,} "
                      f"({stats['pct']*100:.2f}%) {status}")
            print()
        
        # Time anchor stats
        if 'time_stats' in self.evidence and self.evidence['time_stats']:
            print(f"Time Anchor (chain-time, threshold: {TIME_ANCHOR_MIN_PCT*100:.0f}%):")
            for table, stats in self.evidence['time_stats'].items():
                if stats['field'] == 'NONE':
                    print(f"  {table}: NO chain-time field ✗ FAIL")
                else:
                    status = "✓ PASS" if stats.get('valid', False) else "✗ FAIL"
                    print(f"  {table}.{stats['field']}: {stats['pct']*100:.2f}% non-null {status}")
                    if 'min' in stats and 'max' in stats:
                        print(f"    Range: {stats['min']} to {stats['max']}")
            print()
        
        # Key field completeness stats
        if 'key_stats' in self.evidence and self.evidence['key_stats']:
            print("Key Field Completeness (threshold: 100%):")
            for table, fields in self.evidence['key_stats'].items():
                print(f"  {table}:")
                for field, stats in fields.items():
                    status = "✓ PASS" if stats['complete'] else "✗ FAIL"
                    null_count = stats['total'] - stats['non_null']
                    print(f"    {field}: {stats['non_null']:,}/{stats['total']:,} "
                          f"({stats['pct']*100:.2f}%, {null_count} NULLs) {status}")
            print()
        
        # Gap analysis (report-only)
        if 'gap_analysis' in self.evidence:
            gap_data = self.evidence['gap_analysis']
            print(f"Coverage Gaps (threshold: {MAX_GAP_HOURS}h, REPORT-ONLY):")
            print(f"  Total wallets analyzed: {gap_data['total_wallets']}")
            pct_affected = (gap_data['wallets_with_gaps']/gap_data['total_wallets']*100) if gap_data['total_wallets'] > 0 else 0
            print(f"  Wallets with gaps > {MAX_GAP_HOURS}h: {gap_data['wallets_with_gaps']} "
                  f"({pct_affected:.1f}%)")
            
            if gap_data['top_gaps']:
                print(f"  Top gaps (showing up to 20):")
                for item in gap_data['top_gaps'][:20]:
                    print(f"    {item['wallet'][:12]}... in {item['table']}: "
                          f"{item['max_gap_hours']:.1f}h gap ({item['event_count']} events)")
            print()
        
        # Dedupe stats
        if 'dedupe_stats' in self.evidence and self.evidence['dedupe_stats']:
            print("Deduplication Analysis (STRICT, zero tolerance):")
            for table, stats in self.evidence['dedupe_stats'].items():
                status = "✓ PASS" if stats['clean'] else "✗ FAIL"
                print(f"  {table}:")
                print(f"    Total rows: {stats['total']:,}")
                print(f"    Non-null key rows: {stats['non_null_keys']:,}")
                print(f"    Null key excluded: {stats['null_excluded']:,}")
                print(f"    Distinct: {stats['distinct']:,}")
                print(f"    Duplicate rows: {stats['dup_rows']:,} ({stats['dup_pct']*100:.4f}%) {status}")
            print()
        
        # Replay hashes
        if 'replay_hashes' in self.evidence:
            hashes = self.evidence['replay_hashes']
            status = "✓ PASS" if hashes['match'] else "✗ FAIL"
            print("Replay Friendliness:")
            print(f"  Hash 1: {hashes['hash1']}")
            print(f"  Hash 2: {hashes['hash2']}")
            print(f"  Match: {hashes['match']} {status}")
            print()
        
        # Token stats (informational)
        if 'token_stats' in self.evidence and self.evidence['token_stats']:
            print("Token Diversity (informational):")
            for key, count in self.evidence['token_stats'].items():
                print(f"  {key}: {count:,} distinct tokens")
            print()
        
        print("-" * 80)
        print()
        
        # Overall result - only decisive checks
        decisive_keys = ['1_tables_exist', '3_raw_truth', '4_time_anchor', '5_key_completeness', '7_dedupe', '8_replay']
        all_passed = all(
            self.results[k].passed for k in decisive_keys if k in self.results
        )
        
        if all_passed:
            print("✓ OVERALL: PASS")
            print("  Database is SUFFICIENT for data-proven edge mining under PANDA v4.")
        else:
            print("✗ OVERALL: FAIL")
            print("  Database is NOT SUFFICIENT for data-proven edge mining under PANDA v4.")
            print()
            print("  Failed requirements:")
            for key in decisive_keys:
                if key in self.results and not self.results[key].passed:
                    name = dict(decisive_requirements).get(key, key)
                    print(f"    • {name}")
        
        print("=" * 80)
        
        return all_passed


def main():
    parser = argparse.ArgumentParser(
        description='STRICT audit for SQLite DB sufficiency under PANDA v4 zero-guesswork doctrine'
    )
    parser.add_argument(
        '--db',
        required=True,
        help='Path to SQLite database (e.g., masterwalletsdb.db)'
    )
    
    args = parser.parse_args()
    
    auditor = DBSufficiencyAuditor(args.db)
    
    if not auditor.run_audit():
        sys.exit(2)
    
    all_passed = auditor.print_report()
    
    sys.exit(0 if all_passed else 2)


if __name__ == '__main__':
    main()
