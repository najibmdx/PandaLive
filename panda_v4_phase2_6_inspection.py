#!/usr/bin/env python3
import sqlite3
import sys
import os

DB_PATH = "masterwalletsdb.db"

def verify_schema(cur):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('swaps', 'spl_transfers_v2')")
    tables = [row[0] for row in cur.fetchall()]
    if 'swaps' not in tables:
        raise ValueError("table swaps does not exist")
    if 'spl_transfers_v2' not in tables:
        raise ValueError("table spl_transfers_v2 does not exist")

def main():
    if not os.path.exists(DB_PATH):
        print(f"ERROR: {DB_PATH} not found", file=sys.stderr)
        sys.exit(1)
    
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA query_only = ON")
        cur = conn.cursor()
        
        verify_schema(cur)
        
        print("=== SECTION A - SWAPS ===")
        
        cur.execute("SELECT COUNT(*) FROM swaps")
        total_swaps = cur.fetchone()[0]
        print(f"{total_swaps}")
        
        cur.execute("SELECT COUNT(*) FROM swaps WHERE scan_wallet IS NOT NULL")
        swaps_with_wallet = cur.fetchone()[0]
        print(f"{swaps_with_wallet}")
        
        cur.execute("SELECT COUNT(*) FROM swaps WHERE token_mint IS NOT NULL")
        swaps_with_token = cur.fetchone()[0]
        print(f"{swaps_with_token}")
        
        cur.execute("""
            SELECT COUNT(*) FROM swaps 
            WHERE scan_wallet IS NOT NULL AND token_mint IS NOT NULL
        """)
        attributable_swaps = cur.fetchone()[0]
        inflow_attr = attributable_swaps
        print(f"{inflow_attr}")
        
        outflow_attr = attributable_swaps
        print(f"{outflow_attr}")
        
        pct_bidirectional = (inflow_attr / total_swaps * 100) if total_swaps > 0 else 0
        print(f"{pct_bidirectional:.2f}")
        
        verdict = "YES" if inflow_attr > 0 and outflow_attr > 0 else "NO"
        print(f"SWAPS_FLOW_BUILDABLE = {verdict}")
        
        print("\n=== SECTION B - SPL_TRANSFERS_V2 ===")
        
        cur.execute("SELECT COUNT(*) FROM spl_transfers_v2")
        total_transfers = cur.fetchone()[0]
        print(f"{total_transfers}")
        
        cur.execute("""
            SELECT COUNT(*) FROM spl_transfers_v2 
            WHERE scan_wallet IS NOT NULL AND to_addr IS NOT NULL AND scan_wallet = to_addr
        """)
        to_count = cur.fetchone()[0]
        print(f"{to_count}")
        
        cur.execute("""
            SELECT COUNT(*) FROM spl_transfers_v2 
            WHERE scan_wallet IS NOT NULL AND from_addr IS NOT NULL AND scan_wallet = from_addr
        """)
        from_count = cur.fetchone()[0]
        print(f"{from_count}")
        
        cur.execute("""
            SELECT COUNT(*) FROM spl_transfers_v2 
            WHERE scan_wallet IS NOT NULL AND authority IS NOT NULL AND scan_wallet = authority
        """)
        auth_count = cur.fetchone()[0]
        print(f"{auth_count}")
        
        pct_inflow = (to_count / total_transfers * 100) if total_transfers > 0 else 0
        print(f"{pct_inflow:.2f}")
        
        pct_outflow = (from_count / total_transfers * 100) if total_transfers > 0 else 0
        print(f"{pct_outflow:.2f}")
        
        inflow_verdict = "YES" if to_count > 0 else "NO"
        print(f"TRANSFER_INFLOW_BUILDABLE = {inflow_verdict}")
        
        outflow_verdict = "YES" if from_count > 0 else "NO"
        print(f"TRANSFER_OUTFLOW_BUILDABLE = {outflow_verdict}")
        
        print("\n=== SECTION C - CROSS SOURCE ===")
        
        cur.execute("""
            SELECT COUNT(DISTINCT s.token_mint) FROM swaps s
            LEFT JOIN spl_transfers_v2 t ON s.token_mint = t.mint
            WHERE s.token_mint IS NOT NULL AND t.mint IS NULL
        """)
        tokens_swaps_only = cur.fetchone()[0]
        print(f"{tokens_swaps_only}")
        
        cur.execute("""
            SELECT COUNT(DISTINCT t.mint) FROM spl_transfers_v2 t
            LEFT JOIN swaps s ON t.mint = s.token_mint
            WHERE t.mint IS NOT NULL AND s.token_mint IS NULL
        """)
        tokens_transfers_only = cur.fetchone()[0]
        print(f"{tokens_transfers_only}")
        
        cur.execute("""
            SELECT COUNT(DISTINCT s.token_mint) FROM swaps s
            INNER JOIN spl_transfers_v2 t ON s.token_mint = t.mint
            WHERE s.token_mint IS NOT NULL
        """)
        tokens_both = cur.fetchone()[0]
        print(f"{tokens_both}")
        
        cur.execute("""
            SELECT COUNT(DISTINCT s.scan_wallet) FROM swaps s
            LEFT JOIN spl_transfers_v2 t ON s.scan_wallet = t.scan_wallet
            WHERE s.scan_wallet IS NOT NULL AND t.scan_wallet IS NULL
        """)
        wallets_swaps_only = cur.fetchone()[0]
        print(f"{wallets_swaps_only}")
        
        cur.execute("""
            SELECT COUNT(DISTINCT t.scan_wallet) FROM spl_transfers_v2 t
            LEFT JOIN swaps s ON t.scan_wallet = s.scan_wallet
            WHERE t.scan_wallet IS NOT NULL AND s.scan_wallet IS NULL
        """)
        wallets_transfers_only = cur.fetchone()[0]
        print(f"{wallets_transfers_only}")
        
        cur.execute("""
            SELECT COUNT(DISTINCT s.scan_wallet) FROM swaps s
            INNER JOIN spl_transfers_v2 t ON s.scan_wallet = t.scan_wallet
            WHERE s.scan_wallet IS NOT NULL
        """)
        wallets_both = cur.fetchone()[0]
        print(f"{wallets_both}")
        
        print("\n=== SECTION D - INVARIANTS ===")
        
        if total_swaps > 0 and swaps_with_wallet == total_swaps:
            print("INVARIANT: all swaps contain scan_wallet")
        
        if total_swaps > 0 and swaps_with_token == total_swaps:
            print("INVARIANT: all swaps contain token_mint")
        
        if total_transfers > 0 and to_count > 0 and from_count > 0:
            print("INVARIANT: scan_wallet appears as both sender and receiver")
        
        if tokens_both > 0:
            print("INVARIANT: token overlap exists between swaps and transfers")
        
        if wallets_both > 0:
            print("INVARIANT: wallet overlap exists between swaps and transfers")
        
        if total_swaps > 0 and abs(pct_bidirectional - 100.0) < 0.01:
            print("INVARIANT: bidirectional flow is complete")
        
        if pct_inflow > 0 and pct_outflow > 0:
            print("INVARIANT: transfers contain both inflow and outflow attribution")
        
        conn.close()
        
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except sqlite3.Error as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
