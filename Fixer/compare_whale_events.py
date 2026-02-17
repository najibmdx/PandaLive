#!/usr/bin/env python3
"""
Compare database whale_events vs streaming detector output to find differences.
"""

import sqlite3
from collections import defaultdict

def analyze_differences(db_path):
    """Analyze what's different between DB and streaming results."""
    
    conn = sqlite3.connect(db_path)
    
    print("="*80)
    print("WHALE EVENTS COMPARISON ANALYSIS")
    print("="*80)
    
    # Get DB event breakdown by type
    print("\n[1] DATABASE EVENTS BY TYPE")
    print("-"*80)
    
    cursor = conn.execute("""
        SELECT event_type, COUNT(*) as cnt
        FROM whale_events
        GROUP BY event_type
        ORDER BY event_type
    """)
    
    db_counts = {}
    for event_type, count in cursor.fetchall():
        db_counts[event_type] = count
        print(f"{event_type:25s}: {count:,}")
    
    print(f"\nTotal DB events: {sum(db_counts.values()):,}")
    
    # Streaming counts (from output)
    streaming_counts = {
        'WHALE_CUM_24H_BUY': 153508,
        'WHALE_CUM_24H_SELL': 97541,
        'WHALE_CUM_7D_BUY': 137611,
        'WHALE_CUM_7D_SELL': 87025,
        'WHALE_TX_BUY': 4031,
        'WHALE_TX_SELL': 3159
    }
    
    print("\n[2] STREAMING EVENTS BY TYPE")
    print("-"*80)
    for event_type, count in sorted(streaming_counts.items()):
        print(f"{event_type:25s}: {count:,}")
    
    print(f"\nTotal streaming events: {sum(streaming_counts.values()):,}")
    
    # Compare
    print("\n[3] DIFFERENCES BY EVENT TYPE")
    print("-"*80)
    print(f"{'Event Type':<25s} {'DB':>12s} {'Stream':>12s} {'Diff':>12s} {'%':>8s}")
    print("-"*80)
    
    for event_type in sorted(db_counts.keys()):
        db_val = db_counts.get(event_type, 0)
        stream_val = streaming_counts.get(event_type, 0)
        diff = db_val - stream_val
        pct = (diff / db_val * 100) if db_val > 0 else 0
        
        print(f"{event_type:<25s} {db_val:>12,} {stream_val:>12,} {diff:>+12,} {pct:>7.1f}%")
    
    print("-"*80)
    total_db = sum(db_counts.values())
    total_stream = sum(streaming_counts.values())
    total_diff = total_db - total_stream
    total_pct = (total_diff / total_db * 100) if total_db > 0 else 0
    print(f"{'TOTAL':<25s} {total_db:>12,} {total_stream:>12,} {total_diff:>+12,} {total_pct:>7.1f}%")
    
    # Check for duplicate events at same timestamp
    print("\n[4] DUPLICATE TIMESTAMP ANALYSIS")
    print("-"*80)
    
    cursor = conn.execute("""
        SELECT wallet, window, event_type, event_time, COUNT(*) as cnt
        FROM whale_events
        GROUP BY wallet, window, event_type, event_time
        HAVING cnt > 1
        ORDER BY cnt DESC
        LIMIT 20
    """)
    
    dupes = cursor.fetchall()
    
    if dupes:
        print(f"Found {len(dupes)} cases of duplicate (wallet, window, event_type, event_time)")
        print("\nTop 20 duplicates:")
        print(f"{'Wallet':<48s} {'Window':<10s} {'Type':<25s} {'Time':>12s} {'Count':>6s}")
        print("-"*80)
        for wallet, window, event_type, event_time, cnt in dupes[:20]:
            wallet_short = wallet[:45] + "..." if len(wallet) > 45 else wallet
            print(f"{wallet_short:<48s} {window:<10s} {event_type:<25s} {event_time:>12,} {cnt:>6,}")
        
        # Count total duplicates
        cursor = conn.execute("""
            SELECT SUM(cnt - 1) as total_dupes
            FROM (
                SELECT COUNT(*) as cnt
                FROM whale_events
                GROUP BY wallet, window, event_type, event_time
                HAVING cnt > 1
            )
        """)
        total_dupes = cursor.fetchone()[0]
        print(f"\nTotal duplicate events: {total_dupes:,}")
        print(f"This explains {total_dupes:,} of the {total_diff:,} difference ({total_dupes/total_diff*100:.1f}%)")
    else:
        print("No duplicate (wallet, window, event_type, event_time) found")
    
    # Check for multiple events at same timestamp with different flow_ref
    print("\n[5] MULTIPLE FLOW_REF AT SAME TIMESTAMP")
    print("-"*80)
    
    cursor = conn.execute("""
        SELECT wallet, window, event_type, event_time, COUNT(DISTINCT flow_ref) as ref_count
        FROM whale_events
        GROUP BY wallet, window, event_type, event_time
        HAVING ref_count > 1
        ORDER BY ref_count DESC
        LIMIT 10
    """)
    
    multi_ref = cursor.fetchall()
    
    if multi_ref:
        print(f"Found cases where same (wallet, window, event_type, event_time) has multiple flow_refs")
        print("\nTop 10 cases:")
        print(f"{'Wallet':<48s} {'Window':<10s} {'Type':<25s} {'Time':>12s} {'Refs':>6s}")
        print("-"*80)
        for wallet, window, event_type, event_time, ref_count in multi_ref:
            wallet_short = wallet[:45] + "..." if len(wallet) > 45 else wallet
            print(f"{wallet_short:<48s} {window:<10s} {event_type:<25s} {event_time:>12,} {ref_count:>6,}")
        
        # Get example
        example = multi_ref[0]
        print(f"\nExample: {example[0][:20]}... at time {example[3]}")
        cursor = conn.execute("""
            SELECT flow_ref, sol_amount_lamports, supporting_flow_count
            FROM whale_events
            WHERE wallet = ? AND window = ? AND event_type = ? AND event_time = ?
            ORDER BY flow_ref
        """, example[:4])
        
        print("All events at this timestamp:")
        for flow_ref, amount, count in cursor.fetchall():
            print(f"  flow_ref={flow_ref[:16]}... amount={amount:,} count={count}")
    else:
        print("No multiple flow_refs at same timestamp found")
    
    # Check window distribution
    print("\n[6] EVENTS PER WALLET DISTRIBUTION")
    print("-"*80)
    
    cursor = conn.execute("""
        SELECT 
            CASE 
                WHEN cnt <= 100 THEN '1-100'
                WHEN cnt <= 500 THEN '101-500'
                WHEN cnt <= 1000 THEN '501-1000'
                WHEN cnt <= 5000 THEN '1001-5000'
                ELSE '5000+'
            END as bucket,
            COUNT(*) as wallet_count,
            SUM(cnt) as total_events
        FROM (
            SELECT wallet, COUNT(*) as cnt
            FROM whale_events
            GROUP BY wallet
        )
        GROUP BY bucket
        ORDER BY MIN(cnt)
    """)
    
    print(f"{'Events/Wallet':<15s} {'Wallets':>10s} {'Total Events':>15s} {'Avg/Wallet':>12s}")
    print("-"*80)
    for bucket, wallet_count, total_events in cursor.fetchall():
        avg = total_events / wallet_count if wallet_count > 0 else 0
        print(f"{bucket:<15s} {wallet_count:>10,} {total_events:>15,} {avg:>12.1f}")
    
    conn.close()
    
    print("\n" + "="*80)
    print("CONCLUSIONS")
    print("="*80)
    print("""
The 12% difference likely comes from:
1. Multiple emissions at same timestamp (if duplicates found above)
2. Different anchor time selection (EACH_FLOW vs EACH_UNIQUE_TIME)
3. Same-timestamp handling (include all vs stop after first)

The streaming detector uses STRICT semantics:
- One event per (wallet, window, event_type, event_time)
- Stops after first threshold crossing at each timestamp

If your DB has duplicates, that explains the difference.
If your DB emits at EVERY flow (even same timestamp), that also explains it.
""")

if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print("Usage: python compare_whale_events.py <database.db>")
        sys.exit(1)
    
    analyze_differences(sys.argv[1])
