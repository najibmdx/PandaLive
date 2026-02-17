#!/usr/bin/env python3
"""
Quick diagnostic to understand PANDA data scale and performance implications.
"""
import sqlite3
import sys

def analyze_database(db_path):
    """Analyze database to understand scale and cap implications."""
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
    except Exception as e:
        print(f"ERROR: Cannot connect to {db_path}: {e}")
        return
    
    print("="*80)
    print("PANDA DATA SCALE ANALYSIS")
    print("="*80)
    
    # 1. Overall table sizes
    print("\n[1] TABLE ROW COUNTS")
    print("-" * 80)
    
    tables = ['whale_events', 'wallet_token_flow', 'wallets']
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table:30s}: {count:,} rows")
        except:
            print(f"{table:30s}: (not found)")
    
    # 2. Distinct wallets
    print("\n[2] DISTINCT WALLETS")
    print("-" * 80)
    
    try:
        cursor.execute("SELECT COUNT(DISTINCT wallet) FROM whale_events")
        whale_wallets = cursor.fetchone()[0]
        print(f"Distinct wallets in whale_events: {whale_wallets:,}")
    except:
        whale_wallets = 0
        print("Could not count whale_events wallets")
    
    try:
        cursor.execute("SELECT COUNT(DISTINCT scan_wallet) FROM wallet_token_flow")
        flow_wallets = cursor.fetchone()[0]
        print(f"Distinct wallets in wallet_token_flow: {flow_wallets:,}")
    except:
        flow_wallets = 0
        print("Could not count wallet_token_flow wallets")
    
    # 3. Whale events breakdown
    print("\n[3] WHALE EVENTS BREAKDOWN")
    print("-" * 80)
    
    try:
        cursor.execute("""
            SELECT window, event_type, COUNT(*) as cnt
            FROM whale_events
            GROUP BY window, event_type
            ORDER BY window, event_type
        """)
        for row in cursor.fetchall():
            print(f"{row[0]:10s} {row[1]:25s}: {row[2]:,}")
    except Exception as e:
        print(f"Could not analyze whale_events: {e}")
    
    # 4. Cap impact analysis
    print("\n[4] WALLET CAP IMPACT ANALYSIS")
    print("-" * 80)
    
    try:
        cursor.execute("""
            SELECT wallet, COUNT(*) as event_count
            FROM whale_events
            GROUP BY wallet
            ORDER BY event_count DESC
            LIMIT 50
        """)
        
        wallet_counts = cursor.fetchall()
        
        if wallet_counts:
            print(f"\nTop 50 wallets by event count:")
            print(f"{'Wallet':<50s} {'Events':>10s}")
            print("-" * 80)
            for i, (wallet, count) in enumerate(wallet_counts[:10], 1):
                print(f"{wallet[:47]:<50s} {count:>10,}")
            
            # Calculate coverage at different cap levels
            caps_to_test = [15, 50, 100, 500, 1000, 5000, 10000]
            total_events = sum(c[1] for c in wallet_counts)
            
            # Get all wallet event counts
            cursor.execute("""
                SELECT wallet, COUNT(*) as event_count
                FROM whale_events
                GROUP BY wallet
                ORDER BY event_count DESC
            """)
            all_wallet_counts = cursor.fetchall()
            
            print(f"\nCoverage at different wallet caps:")
            print(f"{'Cap':>10s} {'Wallets':>10s} {'Events':>12s} {'Coverage':>10s}")
            print("-" * 80)
            
            for cap in caps_to_test:
                if cap <= len(all_wallet_counts):
                    events_covered = sum(c[1] for c in all_wallet_counts[:cap])
                    coverage_pct = (events_covered / total_events * 100) if total_events > 0 else 0
                    print(f"{cap:>10,} {cap:>10,} {events_covered:>12,} {coverage_pct:>9.1f}%")
                else:
                    print(f"{cap:>10,} {len(all_wallet_counts):>10,} {total_events:>12,} {100.0:>9.1f}%")
                    break
                    
    except Exception as e:
        print(f"Could not analyze wallet distribution: {e}")
    
    # 5. Performance estimate
    print("\n[5] PERFORMANCE IMPLICATIONS")
    print("-" * 80)
    
    if whale_wallets > 0:
        print(f"\nCurrent state:")
        print(f"  - {whale_wallets:,} distinct wallets in whale_events")
        print(f"  - {flow_wallets:,} distinct wallets in wallet_token_flow")
        
        print(f"\nWith --fast-wallets 15:")
        print(f"  - Only first 15 wallets processed")
        print(f"  - {whale_wallets - 15:,} wallets ignored ({((whale_wallets-15)/whale_wallets*100):.1f}% of data)")
        
        print(f"\nWith --fast-wallets 1000:")
        if whale_wallets > 1000:
            print(f"  - Only first 1000 wallets processed")
            print(f"  - {whale_wallets - 1000:,} wallets ignored ({((whale_wallets-1000)/whale_wallets*100):.1f}% of data)")
        else:
            print(f"  - All wallets processed (cap not hit)")
        
        print(f"\nWith no cap:")
        print(f"  - All {whale_wallets:,} wallets processed")
        print(f"  - 100% coverage")
    
    print("\n" + "="*80)
    print("RECOMMENDATION")
    print("="*80)
    
    if whale_wallets > 0:
        if whale_wallets <= 100:
            print("\nYour database has relatively few wallets (<100).")
            print("→ REMOVE THE CAP entirely. Performance impact will be minimal.")
            print("  Command: python script.py --db <db> (no --limit-wallets)")
        elif whale_wallets <= 1000:
            print(f"\nYour database has {whale_wallets} wallets.")
            print("→ SET A HIGH CAP (--fast-wallets 10000 or remove it)")
            print("  Performance should still be acceptable.")
        else:
            print(f"\nYour database has {whale_wallets:,} wallets.")
            print("→ You have OPTIONS:")
            print("  1. Remove cap for full accuracy (may be slow)")
            print("  2. Use high cap (e.g., --fast-wallets 5000) for good coverage")
            print("  3. Keep low cap for speed (accept incomplete results)")
            print("\nCheck the coverage table above to decide.")
    
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python diagnose_panda_scale.py <database.db>")
        sys.exit(1)
    
    analyze_database(sys.argv[1])
