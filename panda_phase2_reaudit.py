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
    cur = con.execute(f"PRAGMA table_info({name})")
    return {row[1] for row in cur.fetchall()}

def scalar(con, query, params=()):
    cur = con.execute(query, params)
    row = cur.fetchone()
    return row[0] if row else None

def domain_values(con, query):
    cur = con.execute(query)
    return {row[0] for row in cur.fetchall() if row[0] is not None}

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
    
    required = {"scan_wallet", "signature", "block_time", "from_addr", "to_addr", "mint", "amount_raw", "decode_status"}
    cols = get_columns(con, "spl_transfers_v2")
    missing = required - cols
    if missing:
        return "FAIL", f"missing columns: {missing}"
    
    return "PASS", f"rowcount={rowcount}, all required columns present"

def phase_2_4(con):
    if not table_exists(con, "spl_transfers_v2"):
        return "FAIL", "table spl_transfers_v2 does not exist"
    
    cols = get_columns(con, "spl_transfers_v2")
    required = {"scan_wallet", "to_addr", "mint", "amount_raw", "signature", "block_time", "decode_status"}
    if not required.issubset(cols):
        return "FAIL", f"missing columns: {required - cols}"
    
    total_inflow = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE to_addr = scan_wallet
    """)
    
    mint_null_inflow = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE to_addr = scan_wallet AND mint IS NULL
    """)
    
    amt_null_inflow = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE to_addr = scan_wallet AND amount_raw IS NULL
    """)
    
    sig_null_inflow = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE to_addr = scan_wallet AND signature IS NULL
    """)
    
    bt_null_inflow = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE to_addr = scan_wallet AND block_time IS NULL
    """)
    
    sw_null_inflow = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE to_addr = scan_wallet AND scan_wallet IS NULL
    """)
    
    amt_le_0_inflow = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE to_addr = scan_wallet 
        AND amount_raw IS NOT NULL 
        AND CAST(amount_raw AS INTEGER) <= 0
    """)
    
    decode_ok = scalar(con, "SELECT COUNT(*) FROM spl_transfers_v2 WHERE decode_status = 'ok'") or 0
    decode_unsupported = scalar(con, "SELECT COUNT(*) FROM spl_transfers_v2 WHERE decode_status = 'unsupported_ix'") or 0
    
    decode_dist = f"decode: ok={decode_ok}, unsup={decode_unsupported}"
    
    if sig_null_inflow > 0 or bt_null_inflow > 0 or sw_null_inflow > 0:
        return "FAIL", f"inflow={total_inflow}, sig_null={sig_null_inflow}, bt_null={bt_null_inflow}, sw_null={sw_null_inflow}; {decode_dist}"
    
    if total_inflow > 0:
        if mint_null_inflow > 0 or amt_null_inflow > 0 or amt_le_0_inflow > 0:
            return "FAIL", f"inflow={total_inflow}, mint_null={mint_null_inflow}, amt_null={amt_null_inflow}, amt_le_0={amt_le_0_inflow}; {decode_dist}"
    
    return "PASS", f"inflow={total_inflow}, all critical NULLs=0, amounts>0; {decode_dist}"

def phase_2_5(con, phase_2_2_verdict, phase_2_4_verdict):
    if phase_2_2_verdict != "PASS":
        return "FAIL", "swaps QA (2.2) failed"
    
    if phase_2_4_verdict == "PASS":
        return "PASS", "swaps QA passed, transfers usable"
    else:
        return "WARN", "swaps-only ready (transfers not usable)"

def phase_2_6(con, phase_2_2_verdict):
    if phase_2_2_verdict != "PASS":
        return "FAIL", "swaps QA (2.2) failed"
    
    if not table_exists(con, "spl_transfers_v2"):
        return "PASS", "swaps reliable; transfers table absent (status: not usable)"
    
    cols = get_columns(con, "spl_transfers_v2")
    if "to_addr" not in cols or "scan_wallet" not in cols:
        return "PASS", "swaps reliable; transfers missing cols (status: not usable)"
    
    total_inflow = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE to_addr = scan_wallet
    """)
    
    if total_inflow == 0:
        return "PASS", "swaps reliable; inflow=0 (status: transfers empty)"
    
    mint_null_inflow = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE to_addr = scan_wallet AND mint IS NULL
    """)
    
    amt_null_inflow = scalar(con, """
        SELECT COUNT(*) FROM spl_transfers_v2 
        WHERE to_addr = scan_wallet AND amount_raw IS NULL
    """)
    
    if mint_null_inflow > 0 or amt_null_inflow > 0:
        return "PASS", f"swaps reliable; inflow={total_inflow}, mint_null={mint_null_inflow}, amt_null={amt_null_inflow} (status: not usable)"
    
    return "PASS", f"swaps reliable; inflow={total_inflow} (status: fully usable)"

def main():
    parser = argparse.ArgumentParser(description="Phase 2 re-audit for panda project")
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
    results.append(("2.4", "spl_transfers_v2 QA (inflow)", verdict_2_4, evidence_2_4))
    
    verdict_2_5, evidence_2_5 = phase_2_5(con, verdict_2_2, verdict_2_4)
    results.append(("2.5", "cohort prep readiness", verdict_2_5, evidence_2_5))
    
    verdict_2_6, evidence_2_6 = phase_2_6(con, verdict_2_2)
    results.append(("2.6", "inspection conclusions", verdict_2_6, evidence_2_6))
    
    con.close()
    
    print(f"{'phase':<7} | {'name':<30} | {'verdict':<6} | {'key_evidence'}")
    print("-" * 120)
    for phase, name, verdict, evidence in results:
        print(f"{phase:<7} | {name:<30} | {verdict:<6} | {evidence}")
    
    exit_code = 0
    if verdict_2_1 != "PASS" or verdict_2_2 != "PASS" or verdict_2_6 != "PASS":
        exit_code = 1
    
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
