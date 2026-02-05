#!/usr/bin/env python3
"""
PANDA v4 Phase 3.1 - Wallet Feature Matrix Builder
REPLACE-MODE: Deletes all existing rows before insert.
Source: wallet_token_flow only (SOL lamports).
Windows: 24h, 7d, lifetime.
"""

import sqlite3
import argparse
import sys
from datetime import datetime, timezone


def main():
    parser = argparse.ArgumentParser(
        description="PANDA Phase 3.1: Build wallet_features table (REPLACE-MODE)"
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to masterwalletsdb.db"
    )
    args = parser.parse_args()

    print("=" * 70)
    print("PANDA v4 Phase 3.1 - Wallet Feature Matrix Builder")
    print("MODE: REPLACE (DELETE + INSERT)")
    print(f"START TIME: {datetime.now(timezone.utc).isoformat()}Z")
    print("=" * 70)

    try:
        # 1) CONNECT
        conn = sqlite3.connect(args.db)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Validate required tables exist
        cur.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name IN ('wallet_token_flow', 'wallet_features')
        """)
        tables = {row[0] for row in cur.fetchall()}
        if 'wallet_token_flow' not in tables:
            raise RuntimeError("Missing required table: wallet_token_flow")
        if 'wallet_features' not in tables:
            raise RuntimeError("Missing required table: wallet_features")

        # 2) PREP - Get current UTC timestamp
        created_at_utc = int(datetime.now(timezone.utc).timestamp())
        
        # Define windows with their time filters
        windows = {
            '24h': created_at_utc - 86400,
            '7d': created_at_utc - 604800,
            'lifetime': None
        }

        # 3) CLEAR TARGET
        print("\n[CLEAR] Deleting all existing rows from wallet_features...")
        cur.execute("DELETE FROM wallet_features")
        deleted_count = cur.rowcount
        print(f"[CLEAR] Deleted {deleted_count} existing rows")

        # 4) AGGREGATION PER WINDOW
        total_inserted = 0
        
        for window_name in ['24h', '7d', 'lifetime']:
            print(f"\n[WINDOW: {window_name}] Processing...")
            
            time_threshold = windows[window_name]
            
            # Build query based on window
            if time_threshold is not None:
                query = """
                    SELECT 
                        scan_wallet,
                        COUNT(*) as tx_count_total,
                        SUM(ABS(sol_amount_lamports)) as sol_volume_total
                    FROM wallet_token_flow
                    WHERE block_time >= ?
                    GROUP BY scan_wallet
                """
                cur.execute(query, (time_threshold,))
            else:
                # lifetime: no time filter
                query = """
                    SELECT 
                        scan_wallet,
                        COUNT(*) as tx_count_total,
                        SUM(ABS(sol_amount_lamports)) as sol_volume_total
                    FROM wallet_token_flow
                    GROUP BY scan_wallet
                """
                cur.execute(query)
            
            rows = cur.fetchall()
            
            if len(rows) == 0:
                # Check if source table truly has no qualifying rows
                if time_threshold is not None:
                    cur.execute(
                        "SELECT COUNT(*) FROM wallet_token_flow WHERE block_time >= ?",
                        (time_threshold,)
                    )
                else:
                    cur.execute("SELECT COUNT(*) FROM wallet_token_flow")
                
                source_count = cur.fetchone()[0]
                if source_count == 0:
                    print(f"[WINDOW: {window_name}] No source rows - skipping (valid)")
                    continue
                else:
                    raise RuntimeError(
                        f"Aggregation returned 0 wallets but source has {source_count} rows"
                    )
            
            # Insert aggregated rows
            insert_data = []
            total_volume = 0
            
            for row in rows:
                scan_wallet = row['scan_wallet']
                tx_count = row['tx_count_total']
                sol_volume = int(row['sol_volume_total']) if row['sol_volume_total'] is not None else 0
                
                # 6) POST-BUILD CHECKS (inline)
                if sol_volume < 0:
                    raise RuntimeError(
                        f"Invalid sol_volume_total ({sol_volume}) for {scan_wallet}"
                    )
                if tx_count <= 0:
                    raise RuntimeError(
                        f"Invalid tx_count_total ({tx_count}) for {scan_wallet}"
                    )
                
                insert_data.append((
                    scan_wallet,
                    window_name,
                    tx_count,
                    sol_volume,
                    created_at_utc
                ))
                total_volume += sol_volume
            
            # Batch insert
            cur.executemany("""
                INSERT INTO wallet_features 
                (scan_wallet, window, tx_count_total, sol_volume_total, created_at_utc)
                VALUES (?, ?, ?, ?, ?)
            """, insert_data)
            
            inserted_count = len(insert_data)
            total_inserted += inserted_count
            
            print(f"[WINDOW: {window_name}] Source rows: {sum(r['tx_count_total'] for r in rows)}")
            print(f"[WINDOW: {window_name}] Distinct wallets inserted: {inserted_count}")
            print(f"[WINDOW: {window_name}] Total SOL volume (lamports): {total_volume:,}")
        
        # 6) POST-BUILD CHECKS - Verify uniqueness
        print("\n[VERIFY] Checking for duplicate (scan_wallet, window) pairs...")
        cur.execute("""
            SELECT scan_wallet, window, COUNT(*) as cnt
            FROM wallet_features
            GROUP BY scan_wallet, window
            HAVING cnt > 1
        """)
        duplicates = cur.fetchall()
        if duplicates:
            raise RuntimeError(
                f"Found {len(duplicates)} duplicate (scan_wallet, window) pairs"
            )
        print("[VERIFY] No duplicates found")
        
        # Final row count
        cur.execute("SELECT COUNT(*) FROM wallet_features")
        final_count = cur.fetchone()[0]
        
        print("\n" + "=" * 70)
        print(f"[SUCCESS] Total rows inserted: {total_inserted}")
        print(f"[SUCCESS] Final wallet_features row count: {final_count}")
        print(f"[SUCCESS] Build completed at: {datetime.now(timezone.utc).isoformat()}Z")
        print("=" * 70)
        
        # 8) COMMIT & EXIT
        conn.commit()
        conn.close()
        
        sys.exit(0)
        
    except Exception as e:
        print("\n" + "=" * 70, file=sys.stderr)
        print(f"[ERROR] Build failed: {e}", file=sys.stderr)
        print("=" * 70, file=sys.stderr)
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
