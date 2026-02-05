#!/usr/bin/env python3
import sqlite3
import argparse
import json
import sys
from collections import Counter

def main():
    parser = argparse.ArgumentParser(description='Inspect spl_transfers_v2 endpoints')
    parser.add_argument('--db', required=True, help='Path to SQLite database')
    args = parser.parse_args()

    try:
        conn = sqlite3.connect(args.db)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        print("=" * 80)
        print("SPL_TRANSFERS_V2 ENDPOINT INSPECTION REPORT")
        print("=" * 80)

        # [0] Verify required tables and columns
        print("\n[0] SCHEMA VERIFICATION")
        print("-" * 80)
        
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='spl_transfers_v2'")
        if not cur.fetchone():
            print("ERROR: Table spl_transfers_v2 does not exist")
            sys.exit(3)
        
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tx'")
        if not cur.fetchone():
            print("ERROR: Table tx does not exist")
            sys.exit(3)

        cur.execute("PRAGMA table_info(spl_transfers_v2)")
        spl_cols = {row['name'] for row in cur.fetchall()}
        required_spl = {'scan_wallet', 'signature', 'instruction_type', 'from_addr', 'to_addr', 'mint', 'amount_raw'}
        missing_spl = required_spl - spl_cols
        if missing_spl:
            print(f"ERROR: spl_transfers_v2 missing required columns: {missing_spl}")
            sys.exit(3)

        cur.execute("PRAGMA table_info(tx)")
        tx_cols = {row['name'] for row in cur.fetchall()}
        required_tx = {'signature', 'raw_json'}
        missing_tx = required_tx - tx_cols
        if missing_tx:
            print(f"ERROR: tx missing required columns: {missing_tx}")
            sys.exit(3)

        print("✓ Required tables and columns present")

        # [1] Print column list and detect owner fields
        print("\n[1] SPL_TRANSFERS_V2 SCHEMA & OWNER FIELD DETECTION")
        print("-" * 80)
        
        cur.execute("PRAGMA table_info(spl_transfers_v2)")
        all_columns = [row['name'] for row in cur.fetchall()]
        print(f"Columns ({len(all_columns)}): {', '.join(all_columns)}")
        
        owner_keywords = {'owner', 'from_owner', 'to_owner', 'from_user', 'to_user', 
                         'fromuseraccount', 'touseraccount', 'authority', 
                         'source_owner', 'destination_owner'}
        detected_owner_cols = [col for col in all_columns if col.lower() in owner_keywords]
        HAS_OWNER_FIELDS = len(detected_owner_cols) > 0
        
        if HAS_OWNER_FIELDS:
            print(f"\n✓ OWNER-ISH FIELDS DETECTED: {detected_owner_cols}")
        else:
            print("\n✗ No owner-ish fields detected")

        # [2] Endpoint string-shape heuristics
        print("\n[2] ENDPOINT STRING-SHAPE HEURISTICS")
        print("-" * 80)
        
        # Check if we have transfer types
        cur.execute("SELECT DISTINCT instruction_type FROM spl_transfers_v2 LIMIT 100")
        all_types = [row[0] for row in cur.fetchall() if row[0]]
        has_transfer_types = any(t in ['transfer', 'transfer_checked'] for t in all_types if t)
        
        if has_transfer_types:
            filter_clause = "WHERE instruction_type IN ('transfer', 'transfer_checked') AND from_addr IS NOT NULL AND to_addr IS NOT NULL"
            print("Analyzing rows with instruction_type IN ('transfer', 'transfer_checked') and non-NULL endpoints")
        else:
            filter_clause = "WHERE from_addr IS NOT NULL AND to_addr IS NOT NULL"
            print("WARNING: No 'transfer' or 'transfer_checked' types found. Analyzing ALL rows with non-NULL endpoints.")
        
        cur.execute(f"SELECT COUNT(*) FROM spl_transfers_v2 {filter_clause}")
        total_rows = cur.fetchone()[0]
        print(f"Total rows considered: {total_rows}")
        
        if total_rows == 0:
            print("ERROR: No rows to analyze")
            sys.exit(1)
        
        cur.execute(f"""
            SELECT 
                MIN(LENGTH(from_addr)) as from_min,
                AVG(LENGTH(from_addr)) as from_avg,
                MAX(LENGTH(from_addr)) as from_max,
                MIN(LENGTH(to_addr)) as to_min,
                AVG(LENGTH(to_addr)) as to_avg,
                MAX(LENGTH(to_addr)) as to_max
            FROM spl_transfers_v2 {filter_clause}
        """)
        lens = cur.fetchone()
        print(f"\nfrom_addr length: min={lens[0]}, avg={lens[1]:.1f}, max={lens[2]}")
        print(f"to_addr length:   min={lens[3]}, avg={lens[4]:.1f}, max={lens[5]}")
        
        cur.execute(f"""
            SELECT COUNT(*) FROM spl_transfers_v2 
            {filter_clause}
            AND LENGTH(from_addr) BETWEEN 32 AND 44 
            AND LENGTH(to_addr) BETWEEN 32 AND 44
        """)
        wallet_like = cur.fetchone()[0]
        print(f"\nRows where both endpoints length 32-44 (wallet-like): {wallet_like} ({100*wallet_like/total_rows:.1f}%)")
        
        cur.execute(f"""
            SELECT COUNT(*) FROM spl_transfers_v2 
            {filter_clause}
            AND LENGTH(from_addr) >= 43 AND LENGTH(to_addr) >= 43
        """)
        token_like = cur.fetchone()[0]
        print(f"Rows where both endpoints length >= 43 (token-account-like): {token_like} ({100*token_like/total_rows:.1f}%)")
        
        cur.execute(f"""
            SELECT COUNT(*) FROM spl_transfers_v2 
            {filter_clause}
            AND from_addr = scan_wallet
        """)
        from_eq_scan = cur.fetchone()[0]
        print(f"\nRows where from_addr = scan_wallet: {from_eq_scan} ({100*from_eq_scan/total_rows:.1f}%)")
        
        cur.execute(f"""
            SELECT COUNT(*) FROM spl_transfers_v2 
            {filter_clause}
            AND to_addr = scan_wallet
        """)
        to_eq_scan = cur.fetchone()[0]
        print(f"Rows where to_addr = scan_wallet: {to_eq_scan} ({100*to_eq_scan/total_rows:.1f}%)")
        
        # Sample rows
        print("\n5 sample transfer_checked rows:")
        cur.execute("""
            SELECT scan_wallet, signature, mint, amount_raw, from_addr, to_addr
            FROM spl_transfers_v2
            WHERE instruction_type = 'transfer_checked'
            LIMIT 5
        """)
        samples = cur.fetchall()
        if samples:
            for s in samples:
                scan = s[0][:12] if s[0] else 'NULL'
                sig = s[1][:12] if s[1] else 'NULL'
                mint = s[2][:12] if s[2] else 'NULL'
                amt = s[3] if s[3] is not None else 'NULL'
                frm = s[4][:12] if s[4] else 'NULL'
                to = s[5][:12] if s[5] else 'NULL'
                print(f"  scan={scan}... sig={sig}... mint={mint}... amt={amt} from={frm}... to={to}...")
        else:
            print("  (No transfer_checked rows found)")

        # [3] Join-based evidence
        print("\n[3] JOIN-BASED EVIDENCE")
        print("-" * 80)
        
        transfer_types = ['transfer', 'transfer_checked']
        placeholders = ','.join('?' * len(transfer_types))
        cur.execute(f"""
            SELECT COUNT(*) FROM spl_transfers_v2
            WHERE instruction_type IN ({placeholders})
            AND (from_addr = scan_wallet OR to_addr = scan_wallet)
            AND from_addr IS NOT NULL AND to_addr IS NOT NULL
        """, transfer_types)
        transfer_matches = cur.fetchone()[0]
        print(f"Rows where instruction_type IN ('transfer','transfer_checked') AND (from_addr=scan_wallet OR to_addr=scan_wallet): {transfer_matches}")
        
        cur.execute("""
            SELECT COUNT(*) FROM spl_transfers_v2
            WHERE instruction_type = 'close_account' 
            AND to_addr = scan_wallet
            AND to_addr IS NOT NULL
        """)
        close_matches = cur.fetchone()[0]
        print(f"Rows where instruction_type='close_account' AND to_addr=scan_wallet: {close_matches}")
        print("(Note: close_account is NOT a token transfer, so to_addr linkage is not transfer-related)")

        # [4] Raw JSON sampling
        print("\n[4] RAW_JSON SAMPLING")
        print("-" * 80)
        
        sample_type = 'transfer_checked'
        cur.execute("SELECT COUNT(*) FROM spl_transfers_v2 WHERE instruction_type = ?", (sample_type,))
        if cur.fetchone()[0] == 0:
            sample_type = 'transfer'
            print(f"No transfer_checked rows; using instruction_type='transfer' for sampling")
        else:
            print(f"Using instruction_type='transfer_checked' for sampling")
        
        cur.execute(f"""
            SELECT s.signature, s.scan_wallet, s.from_addr, s.to_addr
            FROM spl_transfers_v2 s
            WHERE s.instruction_type = ?
            AND s.from_addr IS NOT NULL 
            AND s.to_addr IS NOT NULL
            ORDER BY s.signature
            LIMIT 50
        """, (sample_type,))
        sample_rows = cur.fetchall()
        
        if len(sample_rows) == 0:
            print(f"WARNING: No {sample_type} rows found for sampling")
            samples_with_tokenTransfers = 0
            userAccount_matches_scan_wallet = 0
            tokenAccount_matches_from_to = 0
        else:
            print(f"Sampling {len(sample_rows)} signatures...")
            
            samples_with_tokenTransfers = 0
            userAccount_matches_scan_wallet = 0
            tokenAccount_matches_from_to = 0
            
            for row in sample_rows:
                sig = row[0]
                scan_wallet = row[1]
                from_addr = row[2]
                to_addr = row[3]
                
                cur.execute("SELECT raw_json FROM tx WHERE signature = ?", (sig,))
                tx_row = cur.fetchone()
                if not tx_row or not tx_row[0]:
                    continue
                
                try:
                    data = json.loads(tx_row[0])
                except:
                    continue
                
                # Search for token transfer info in various places
                token_transfers = []
                for key_path in [
                    ['tokenTransfers'],
                    ['parsedTokenTransfers'],
                    ['events', 'tokenTransfers']
                ]:
                    obj = data
                    for k in key_path:
                        obj = obj.get(k) if isinstance(obj, dict) else None
                        if obj is None:
                            break
                    if isinstance(obj, list):
                        token_transfers.extend(obj)
                
                if not token_transfers:
                    continue
                
                samples_with_tokenTransfers += 1
                
                for tt in token_transfers:
                    # Check for userAccount fields
                    from_user = tt.get('fromUserAccount') or tt.get('source_owner')
                    to_user = tt.get('toUserAccount') or tt.get('destination_owner')
                    
                    if from_user == scan_wallet or to_user == scan_wallet:
                        userAccount_matches_scan_wallet += 1
                    
                    # Check for tokenAccount fields
                    from_token = tt.get('fromTokenAccount')
                    to_token = tt.get('toTokenAccount')
                    
                    if (from_token == from_addr or to_token == to_addr or 
                        from_token == to_addr or to_token == from_addr):
                        tokenAccount_matches_from_to += 1
        
        print(f"\nSampling results (N={len(sample_rows)}):")
        print(f"  Signatures with tokenTransfers data: {samples_with_tokenTransfers}")
        print(f"  UserAccount matches scan_wallet: {userAccount_matches_scan_wallet}")
        print(f"  TokenAccount matches from_addr/to_addr: {tokenAccount_matches_from_to}")

        # [5] Determination
        print("\n[5] DETERMINATION")
        print("=" * 80)
        
        endpoints_eq_scan_pct = 100 * (from_eq_scan + to_eq_scan) / total_rows if total_rows > 0 else 0
        
        if HAS_OWNER_FIELDS:
            result = "HAS_OWNER_FIELDS"
            print(f"✓ Owner-ish fields detected: {detected_owner_cols}")
            print(f"  => Decoder likely already separates owner from token account")
        elif tokenAccount_matches_from_to > 0 and userAccount_matches_scan_wallet > 0 and endpoints_eq_scan_pct < 1.0:
            result = "ENDPOINTS_LOOK_LIKE_TOKEN_ACCOUNTS"
            print(f"✓ Raw JSON evidence:")
            print(f"  - TokenAccount fields match from_addr/to_addr: {tokenAccount_matches_from_to} occurrences")
            print(f"  - UserAccount fields match scan_wallet: {userAccount_matches_scan_wallet} occurrences")
            print(f"  - Endpoints rarely equal scan_wallet directly ({endpoints_eq_scan_pct:.1f}%)")
            print(f"  => from_addr/to_addr are TOKEN ACCOUNTS, not wallets")
        elif endpoints_eq_scan_pct >= 1.0:
            result = "ENDPOINTS_LOOK_LIKE_WALLETS"
            print(f"✓ Endpoints frequently equal scan_wallet:")
            print(f"  - from_addr=scan_wallet: {from_eq_scan} ({100*from_eq_scan/total_rows:.1f}%)")
            print(f"  - to_addr=scan_wallet: {to_eq_scan} ({100*to_eq_scan/total_rows:.1f}%)")
            print(f"  => from_addr/to_addr are WALLET addresses")
        else:
            result = "NEEDS_DECODER_CHANGE"
            print(f"✗ Insufficient evidence to determine endpoint type:")
            print(f"  - No owner fields in schema")
            print(f"  - Limited/no raw JSON evidence")
            print(f"  - Endpoints rarely match scan_wallet ({endpoints_eq_scan_pct:.1f}%)")
            print(f"  => Cannot reliably link spl_transfers_v2 to scan_wallet")
            print(f"  => Decoder may need to extract owner/authority fields")
        
        print(f"\nRESULT: {result}")
        print("=" * 80)
        
        conn.close()
        sys.exit(0)
        
    except sqlite3.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
