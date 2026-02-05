#!/usr/bin/env python3
import sqlite3
import argparse
import sys

def main():
    parser = argparse.ArgumentParser(description='Re-check Phase 2.4 inflow semantics for spl_transfers_v2')
    parser.add_argument('--db', required=True, help='Path to SQLite database file')
    args = parser.parse_args()
    
    conn = None
    try:
        conn = sqlite3.connect(args.db)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        print("=" * 80)
        print("PHASE 2.4 RECHECK: spl_transfers_v2 Inflow Semantics Analysis")
        print("=" * 80)
        print()
        
        # 1) Verify table and columns exist
        print("[1] TABLE AND COLUMN VERIFICATION")
        print("-" * 80)
        
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='spl_transfers_v2'")
        if not cur.fetchone():
            print("ERROR: Table 'spl_transfers_v2' does not exist")
            conn.close()
            sys.exit(3)
        
        required_cols = ['instruction_type', 'scan_wallet', 'mint', 'amount_raw', 
                        'from_addr', 'to_addr', 'signature', 'block_time']
        cur.execute("PRAGMA table_info(spl_transfers_v2)")
        existing_cols = {row['name'] for row in cur.fetchall()}
        missing_cols = [col for col in required_cols if col not in existing_cols]
        
        if missing_cols:
            print(f"ERROR: Missing required columns: {', '.join(missing_cols)}")
            conn.close()
            sys.exit(3)
        
        print(f"✓ Table exists with all required columns: {', '.join(required_cols)}")
        print()
        
        # 2) Instruction type distribution (top 10)
        print("[2] INSTRUCTION TYPE DISTRIBUTION (Top 10)")
        print("-" * 80)
        cur.execute("""
            SELECT instruction_type, COUNT(*) as c
            FROM spl_transfers_v2
            GROUP BY instruction_type
            ORDER BY c DESC
            LIMIT 10
        """)
        for row in cur.fetchall():
            itype = row['instruction_type'] if row['instruction_type'] else '(NULL)'
            print(f"  {itype:30s} {row['c']:>10,}")
        print()
        
        # 3) Naive inflow stats (old failing definition)
        print("[3] NAIVE INFLOW STATS (to_addr = scan_wallet)")
        print("-" * 80)
        
        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN mint IS NULL THEN 1 ELSE 0 END) as mint_null,
                   SUM(CASE WHEN amount_raw IS NULL THEN 1 ELSE 0 END) as amount_null
            FROM spl_transfers_v2
            WHERE to_addr = scan_wallet
        """)
        naive_stats = cur.fetchone()
        naive_total = naive_stats['total'] if naive_stats['total'] else 0
        naive_mint_null = naive_stats['mint_null'] if naive_stats['mint_null'] else 0
        naive_amount_null = naive_stats['amount_null'] if naive_stats['amount_null'] else 0
        
        print(f"  Total rows:        {naive_total:>10,}")
        print(f"  mint IS NULL:      {naive_mint_null:>10,}")
        print(f"  amount_raw IS NULL:{naive_amount_null:>10,}")
        print()
        
        print("  Instruction type breakdown for naive inflow (Top 10):")
        cur.execute("""
            SELECT instruction_type, COUNT(*) as c
            FROM spl_transfers_v2
            WHERE to_addr = scan_wallet
            GROUP BY instruction_type
            ORDER BY c DESC
            LIMIT 10
        """)
        for row in cur.fetchall():
            itype = row['instruction_type'] if row['instruction_type'] else '(NULL)'
            print(f"    {itype:30s} {row['c']:>10,}")
        print()
        
        # 4) Real token transfer inflow stats
        print("[4] REAL TOKEN TRANSFER INFLOW STATS")
        print("-" * 80)
        transfer_types = ('transfer', 'transfer_checked')
        print(f"  Using instruction_type IN {transfer_types}")
        print()
        
        # Predicate A: to_addr = scan_wallet
        print("  [A] Predicate: to_addr = scan_wallet")
        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN mint IS NULL THEN 1 ELSE 0 END) as mint_null,
                   SUM(CASE WHEN amount_raw IS NULL THEN 1 ELSE 0 END) as amount_null,
                   SUM(CASE WHEN amount_raw IS NOT NULL AND 
                        (CASE WHEN amount_raw GLOB '*[^0-9]*' THEN NULL ELSE CAST(amount_raw AS INTEGER) END) IS NOT NULL AND
                        CAST(amount_raw AS INTEGER) <= 0 THEN 1 ELSE 0 END) as amount_le_0,
                   SUM(CASE WHEN amount_raw IS NOT NULL AND 
                        amount_raw GLOB '*[^0-9]*' THEN 1 ELSE 0 END) as non_castable
            FROM spl_transfers_v2
            WHERE instruction_type IN ('transfer', 'transfer_checked')
              AND to_addr = scan_wallet
        """)
        pred_a = cur.fetchone()
        pred_a_total = pred_a['total'] if pred_a['total'] else 0
        pred_a_mint_null = pred_a['mint_null'] if pred_a['mint_null'] else 0
        pred_a_amount_null = pred_a['amount_null'] if pred_a['amount_null'] else 0
        pred_a_amount_le_0 = pred_a['amount_le_0'] if pred_a['amount_le_0'] else 0
        pred_a_non_castable = pred_a['non_castable'] if pred_a['non_castable'] else 0
        
        print(f"      Total:              {pred_a_total:>10,}")
        print(f"      mint IS NULL:       {pred_a_mint_null:>10,}")
        print(f"      amount_raw IS NULL: {pred_a_amount_null:>10,}")
        print(f"      amount_raw <= 0:    {pred_a_amount_le_0:>10,}")
        print(f"      non-castable amount:{pred_a_non_castable:>10,}")
        print()
        
        # Predicate B: scan_wallet in either endpoint
        print("  [B] Predicate: scan_wallet in (from_addr OR to_addr)")
        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN mint IS NULL THEN 1 ELSE 0 END) as mint_null,
                   SUM(CASE WHEN amount_raw IS NULL THEN 1 ELSE 0 END) as amount_null,
                   SUM(CASE WHEN amount_raw IS NOT NULL AND 
                        (CASE WHEN amount_raw GLOB '*[^0-9]*' THEN NULL ELSE CAST(amount_raw AS INTEGER) END) IS NOT NULL AND
                        CAST(amount_raw AS INTEGER) <= 0 THEN 1 ELSE 0 END) as amount_le_0,
                   SUM(CASE WHEN amount_raw IS NOT NULL AND 
                        amount_raw GLOB '*[^0-9]*' THEN 1 ELSE 0 END) as non_castable
            FROM spl_transfers_v2
            WHERE instruction_type IN ('transfer', 'transfer_checked')
              AND (from_addr = scan_wallet OR to_addr = scan_wallet)
        """)
        pred_b = cur.fetchone()
        pred_b_total = pred_b['total'] if pred_b['total'] else 0
        pred_b_mint_null = pred_b['mint_null'] if pred_b['mint_null'] else 0
        pred_b_amount_null = pred_b['amount_null'] if pred_b['amount_null'] else 0
        pred_b_amount_le_0 = pred_b['amount_le_0'] if pred_b['amount_le_0'] else 0
        pred_b_non_castable = pred_b['non_castable'] if pred_b['non_castable'] else 0
        
        print(f"      Total:              {pred_b_total:>10,}")
        print(f"      mint IS NULL:       {pred_b_mint_null:>10,}")
        print(f"      amount_raw IS NULL: {pred_b_amount_null:>10,}")
        print(f"      amount_raw <= 0:    {pred_b_amount_le_0:>10,}")
        print(f"      non-castable amount:{pred_b_non_castable:>10,}")
        print()
        
        # 5) close_account contamination check
        print("[5] CLOSE_ACCOUNT CONTAMINATION CHECK")
        print("-" * 80)
        
        cur.execute("""
            SELECT COUNT(*) as total_close
            FROM spl_transfers_v2
            WHERE to_addr = scan_wallet
              AND instruction_type = 'close_account'
        """)
        close_result = cur.fetchone()
        close_count = close_result['total_close'] if close_result['total_close'] else 0
        
        pct_close = (close_count / naive_total * 100) if naive_total > 0 else 0.0
        
        print(f"  Naive inflow subset (to_addr=scan_wallet) total: {naive_total:>10,}")
        print(f"  close_account rows in that subset:           {close_count:>10,}")
        print(f"  Percentage close_account:                    {pct_close:>9.2f}%")
        print()
        
        # Decision logic
        print("=" * 80)
        print("DECISION LOGIC")
        print("=" * 80)
        print()
        
        # Check if old subset dominated by close_account (>=80%)
        close_dominated = pct_close >= 80.0
        
        # Check if transfer_types subset (predicate B) is clean
        transfer_clean = (pred_b_total > 0 and 
                         pred_b_mint_null == 0 and 
                         pred_b_amount_null == 0 and
                         pred_b_amount_le_0 == 0 and
                         pred_b_non_castable == 0)
        
        # Check if transfer_types has issues
        transfer_has_issues = (pred_b_total > 0 and 
                              (pred_b_mint_null > 0 or 
                               pred_b_amount_null > 0 or 
                               pred_b_amount_le_0 > 0 or
                               pred_b_non_castable > 0))
        
        transfer_empty = (pred_b_total == 0)
        
        print(f"  close_account dominated (>=80%): {close_dominated}")
        print(f"  transfer_types clean (predicate B): {transfer_clean}")
        print(f"  transfer_types has issues: {transfer_has_issues}")
        print(f"  transfer_types empty: {transfer_empty}")
        print()
        
        if close_dominated and transfer_clean:
            print("CONCLUSION:")
            print("  The naive inflow definition (to_addr=scan_wallet) was contaminated by")
            print("  close_account instructions (>=80% of that subset). The actual transfer")
            print("  instructions (transfer/transfer_checked) have clean mint and amount_raw")
            print("  when using scan_wallet in endpoints. The prior Phase 2.4 FAIL was caused")
            print("  by an inflow-definition bug, not a decode issue.")
            print()
            print("RESULT: OK_INFLOW_DEFINITION_BUG")
            conn.close()
            sys.exit(0)
        elif transfer_has_issues:
            print("CONCLUSION:")
            print("  Real token transfer instructions (transfer/transfer_checked) still have")
            print("  missing or invalid mint/amount_raw values. This indicates a genuine decode")
            print("  issue that blocks Phase 2.4 validation.")
            print()
            print("RESULT: FAIL_TRANSFER_DECODE")
            conn.close()
            sys.exit(2)
        elif transfer_empty:
            print("CONCLUSION:")
            print("  No transfer-type rows match the predicates. Cannot validate inflow via")
            print("  endpoints. This is inconclusive and still blocking.")
            print()
            print("RESULT: INCONCLUSIVE_BLOCKING")
            conn.close()
            sys.exit(2)
        else:
            print("CONCLUSION:")
            print("  The situation is ambiguous. Transfer types exist but are clean, yet")
            print("  close_account does not dominate the naive subset. Treating as inconclusive.")
            print()
            print("RESULT: INCONCLUSIVE_BLOCKING")
            conn.close()
            sys.exit(2)
        
    except sqlite3.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        if conn:
            conn.close()
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        if conn:
            conn.close()
        sys.exit(1)

if __name__ == "__main__":
    main()
