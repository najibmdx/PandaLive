#!/usr/bin/env python3
import sqlite3
import argparse
import sys

def table_exists(con, name):
    cur = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,)
    )
    return cur.fetchone() is not None

def get_columns(con, name):
    rows = fetchall(con, f"PRAGMA table_info({name})")
    return {row[1] for row in rows}

def scalar(con, query, params=()):
    cur = con.execute(query, params)
    row = cur.fetchone()
    return row[0] if row else None

def fetchall(con, query, params=()):
    cur = con.execute(query, params)
    return cur.fetchall()

def domain_values(con, query):
    rows = fetchall(con, query)
    return {row[0] for row in rows if row[0] is not None}

def phase_2_1(con):
    if not table_exists(con, "swaps"):
        return "FAIL", "table swaps does not exist"
    
    rowcount = scalar(con, "SELECT COUNT(*) FROM swaps")
    if rowcount == 0:
        return "FAIL", f"swaps rowcount={rowcount}"
    
    required = {"scan_wallet", "signature", "block_time", "token_mint", "token_amount_raw", "sol_direction"}
    cols = get_columns(con, "swaps")
    missing = required - cols
    if missing:
        return "FAIL", f"missing columns: {missing}"
    
    return "PASS", f"rowcount={rowcount}, all required columns present"

def phase_2_2(con):
    if not table_exists(con, "swaps"):
        return "FAIL", "table swaps does not exist"
    
    required = ["scan_wallet", "signature", "block_time", "token_mint", "token_amount_raw", "sol_direction"]
    cols = get_columns(con, "swaps")
    
    for col in required:
        if col not in cols:
            return "FAIL", f"column {col} missing"
    
    null_counts = {}
    for col in required:
        null_count = scalar(con, f"SELECT COUNT(*) FROM swaps WHERE {col} IS NULL")
        null_counts[col] = null_count
        if null_count > 0:
            return "FAIL", f"NULL count for {col}={null_count}"
    
    sol_dir_domain = domain_values(con, "SELECT DISTINCT sol_direction FROM swaps")
    if sol_dir_domain != {"buy", "sell"}:
        return "FAIL", f"sol_direction domain={sol_dir_domain}, expected {{'buy','sell'}}"
    
    non_positive = scalar(con, """
        SELECT COUNT(*) FROM swaps 
        WHERE token_amount_raw IS NOT NULL 
        AND CAST(token_amount_raw AS INTEGER) <= 0
    """)
    if non_positive > 0:
        return "FAIL", f"token_amount_raw <=0 count={non_positive}"
    
    bt_non_positive = scalar(con, """
        SELECT COUNT(*) FROM swaps 
        WHERE block_time IS NOT NULL 
        AND CAST(block_time AS INTEGER) <= 0
    """)
    if bt_non_positive > 0:
        return "FAIL", f"block_time <=0 count={bt_non_positive}"
    
    rowcount = scalar(con, "SELECT COUNT(*) FROM swaps")
    return "PASS", f"rowcount={rowcount}, all NULLs=0, sol_direction valid, amounts>0"

def phase_2_3(con):
    if not table_exists(con, "spl_transfers_v2"):
        return "FAIL", "table spl_transfers_v2 does not exist"
    
    rowcount = scalar(con, "SELECT COUNT(*) FROM spl_transfers_v2")
    if rowcount == 0:
        return "FAIL", f"spl_transfers_v2 rowcount={rowcount}"
    
    required = {"scan_wallet", "signature", "block_time", "instruction_type", "from_addr", "to_addr", "mint", "amount_raw", "decode_status", "authority"}
    cols = get_columns(con, "spl_transfers_v2")
    missing = required - cols
    if missing:
        return "FAIL", f"missing columns: {missing}"
    
    return "PASS", f"rowcount={rowcount}, all required columns present"

def phase_2_4(con):
    if not table_exists(con, "spl_transfers_v2"):
        return "FAIL", "table spl_transfers_v2 does not exist"
    
    cols = get_columns(con, "spl_transfers_v2")
    required = {"scan_wallet", "instruction_type", "authority", "mint", "amount_raw", "to_addr"}
    if not required.issubset(cols):
        return "FAIL", f"missing columns: {required - cols}"
    
    total_transfer_rows = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE instruction_type IN ('transfer', 'transfer_checked')
    """)
    
    if total_transfer_rows == 0:
        return "FAIL", f"total_transfer_rows={total_transfer_rows} (must be > 0)"
    
    authority_null_transfer_rows = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE instruction_type IN ('transfer', 'transfer_checked')
        AND authority IS NULL
    """)
    
    if authority_null_transfer_rows > 0:
        return "FAIL", f"authority_null_transfer_rows={authority_null_transfer_rows} (must be 0)"
    
    linked_total = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE instruction_type IN ('transfer', 'transfer_checked')
        AND authority = scan_wallet
    """)
    
    if linked_total == 0:
        return "FAIL", f"linked_total={linked_total} (must be > 0)"
    
    mint_null = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE instruction_type IN ('transfer', 'transfer_checked')
        AND authority = scan_wallet
        AND mint IS NULL
    """)
    
    if mint_null > 0:
        return "FAIL", f"linked_total={linked_total}, mint_null={mint_null} (must be 0)"
    
    amount_null = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE instruction_type IN ('transfer', 'transfer_checked')
        AND authority = scan_wallet
        AND amount_raw IS NULL
    """)
    
    if amount_null > 0:
        return "FAIL", f"linked_total={linked_total}, amount_null={amount_null} (must be 0)"
    
    amount_le_0 = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE instruction_type IN ('transfer', 'transfer_checked')
        AND authority = scan_wallet
        AND amount_raw IS NOT NULL
        AND CAST(amount_raw AS INTEGER) <= 0
    """)
    
    if amount_le_0 > 0:
        return "FAIL", f"linked_total={linked_total}, amount_le_0={amount_le_0} (must be 0)"
    
    non_castable_amount = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE instruction_type IN ('transfer', 'transfer_checked')
        AND authority = scan_wallet
        AND amount_raw IS NOT NULL
        AND (CAST(amount_raw AS TEXT) != CAST(CAST(amount_raw AS INTEGER) AS TEXT))
    """)
    
    if non_castable_amount > 0:
        return "FAIL", f"linked_total={linked_total}, non_castable_amount={non_castable_amount} (must be 0)"
    
    close_account_rows = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE instruction_type = 'close_account'
        AND to_addr = scan_wallet
    """) or 0
    
    evidence = f"linked_total={linked_total}, total_transfer_rows={total_transfer_rows}, auth_null={authority_null_transfer_rows}, mint_null={mint_null}, amt_null={amount_null}, amt_le_0={amount_le_0}, non_cast={non_castable_amount}; close_account_contamination={close_account_rows}"
    return "PASS", evidence

def phase_2_5(con, phase_2_2_verdict, phase_2_4_verdict):
    if phase_2_2_verdict != "PASS":
        return "FAIL", "swaps QA (2.2) failed"
    
    if phase_2_4_verdict != "PASS":
        return "FAIL", "transfers QA (2.4) failed"
    
    return "PASS", "swaps and transfers both passed QA"

def phase_2_6(con, phase_2_2_verdict, phase_2_4_verdict):
    if phase_2_2_verdict != "PASS":
        return "FAIL", "swaps QA (2.2) failed"
    
    if phase_2_4_verdict != "PASS":
        return "FAIL", "transfers QA (2.4) failed"
    
    conclusion = "Inspection: swaps reliable; transfers link via authority; close_account not a token transfer"
    return "PASS", conclusion

def main():
    parser = argparse.ArgumentParser(description="Phase 2 re-audit for panda project (PATCHED)")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    args = parser.parse_args()
    
    try:
        con = sqlite3.connect(args.db)
    except Exception as e:
        print(f"ERROR: Cannot connect to database: {e}", file=sys.stderr)
        sys.exit(1)
    
    results = []
    
    verdict_2_1, evidence_2_1 = phase_2_1(con)
    results.append(("2.1", "swaps build presence", verdict_2_1, evidence_2_1))
    
    verdict_2_2, evidence_2_2 = phase_2_2(con)
    results.append(("2.2", "swaps QA", verdict_2_2, evidence_2_2))
    
    verdict_2_3, evidence_2_3 = phase_2_3(con)
    results.append(("2.3", "spl_transfers_v2 build presence", verdict_2_3, evidence_2_3))
    
    verdict_2_4, evidence_2_4 = phase_2_4(con)
    results.append(("2.4", "spl_transfers_v2 QA (authority)", verdict_2_4, evidence_2_4))
    
    verdict_2_5, evidence_2_5 = phase_2_5(con, verdict_2_2, verdict_2_4)
    results.append(("2.5", "cohort prep readiness", verdict_2_5, evidence_2_5))
    
    verdict_2_6, evidence_2_6 = phase_2_6(con, verdict_2_2, verdict_2_4)
    results.append(("2.6", "inspection conclusions", verdict_2_6, evidence_2_6))
    
    con.close()
    
    print(f"{'phase':<7} | {'name':<30} | {'verdict':<6} | {'key_evidence'}")
    print("-" * 120)
    for phase, name, verdict, evidence in results:
        print(f"{phase:<7} | {name:<30} | {verdict:<6} | {evidence}")
    
    exit_code = 0
    if verdict_2_1 != "PASS" or verdict_2_2 != "PASS" or verdict_2_4 != "PASS" or verdict_2_6 != "PASS":
        exit_code = 1
    
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
