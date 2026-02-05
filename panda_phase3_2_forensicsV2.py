#!/usr/bin/env python3
"""
panda_phase3_2_forensicsV2.py
READ-ONLY forensic recomputation of whale_events with FIXED event_type ENUMs.

ONLY CHANGE: Fix event_type strings to match whale_events.event_type exactly:
  - WHALE_TX_BUY, WHALE_TX_SELL
  - WHALE_CUM_24H_BUY, WHALE_CUM_24H_SELL
  - WHALE_CUM_7D_BUY, WHALE_CUM_7D_SELL

NO writes, NO temp tables, NO inserts/updates/deletes.
"""

import argparse
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta

# ============================================================================
# CANONICAL EVENT TYPE ENUMS (CRITICAL FIX)
# ============================================================================
EVT_TX_BUY = "WHALE_TX_BUY"
EVT_TX_SELL = "WHALE_TX_SELL"
EVT_C24_BUY = "WHALE_CUM_24H_BUY"
EVT_C24_SELL = "WHALE_CUM_24H_SELL"
EVT_C7_BUY = "WHALE_CUM_7D_BUY"
EVT_C7_SELL = "WHALE_CUM_7D_SELL"

ALL_CANONICAL_EVENT_TYPES = {
    EVT_TX_BUY, EVT_TX_SELL,
    EVT_C24_BUY, EVT_C24_SELL,
    EVT_C7_BUY, EVT_C7_SELL
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="READ-ONLY whale_events forensics with fixed event_type ENUMs"
    )
    parser.add_argument("--db", required=True, help="Path to masterwalletsdb.db")
    parser.add_argument("--t-tx", type=int, default=10_000_000_000,
                        help="Single-tx threshold (lamports)")
    parser.add_argument("--t-cum-24h", type=int, default=50_000_000_000,
                        help="Cumulative 24h threshold (lamports)")
    parser.add_argument("--t-cum-7d", type=int, default=200_000_000_000,
                        help="Cumulative 7d threshold (lamports)")
    parser.add_argument("--sample-n", type=int, default=20,
                        help="Number of samples to show for missing/phantom events")
    return parser.parse_args()


def discover_whale_events_schema(conn):
    """
    Discover exact column names for whale_events table.
    Returns: (wallet_col, window_col, type_col, time_col, ref_col, amt_col, count_col)
    """
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(whale_events)")
    cols = {row[1].lower(): row[1] for row in cur.fetchall()}
    
    # Wallet column
    wallet_col = None
    for candidate in ["scan_wallet", "wallet", "wallet_address"]:
        if candidate.lower() in cols:
            wallet_col = cols[candidate.lower()]
            break
    
    # Window column
    window_col = None
    for candidate in ["time_window", "window", "window_type"]:
        if candidate.lower() in cols:
            window_col = cols[candidate.lower()]
            break
    
    # Event type column
    type_col = None
    for candidate in ["event_type", "type", "evt_type"]:
        if candidate.lower() in cols:
            type_col = cols[candidate.lower()]
            break
    
    # Event time column
    time_col = None
    for candidate in ["event_time", "time", "timestamp"]:
        if candidate.lower() in cols:
            time_col = cols[candidate.lower()]
            break
    
    # Flow reference column
    ref_col = None
    for candidate in ["flow_reference", "signature", "tx_signature", "flow_ref"]:
        if candidate.lower() in cols:
            ref_col = cols[candidate.lower()]
            break
    
    # Amount column
    amt_col = None
    for candidate in ["sol_amount_lamports", "amount", "sol_amount"]:
        if candidate.lower() in cols:
            amt_col = cols[candidate.lower()]
            break
    
    # Supporting flow count column
    count_col = None
    for candidate in ["supporting_flow_count", "flow_count", "count"]:
        if candidate.lower() in cols:
            count_col = cols[candidate.lower()]
            break
    
    if not all([wallet_col, window_col, type_col, time_col, ref_col, amt_col, count_col]):
        print("ERROR: Could not discover all required columns in whale_events")
        print(f"Found: wallet={wallet_col}, window={window_col}, type={type_col}, "
              f"time={time_col}, ref={ref_col}, amt={amt_col}, count={count_col}")
        sys.exit(1)
    
    return wallet_col, window_col, type_col, time_col, ref_col, amt_col, count_col


def discover_wallet_token_flow_schema(conn):
    """
    Discover exact column names for wallet_token_flow.
    Returns: (wallet_col, time_col, dir_col, amt_col, flow_ref_col)
    """
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(wallet_token_flow)")
    cols = {row[1].lower(): row[1] for row in cur.fetchall()}
    
    # Wallet column
    wallet_col = None
    for candidate in ["scan_wallet", "wallet", "wallet_address"]:
        if candidate.lower() in cols:
            wallet_col = cols[candidate.lower()]
            break
    
    # Time column
    time_col = None
    for candidate in ["block_time", "time", "timestamp"]:
        if candidate.lower() in cols:
            time_col = cols[candidate.lower()]
            break
    
    # Direction column
    dir_col = None
    for candidate in ["sol_direction", "direction", "flow_direction"]:
        if candidate.lower() in cols:
            dir_col = cols[candidate.lower()]
            break
    
    # Amount column
    amt_col = None
    for candidate in ["sol_amount_lamports", "amount", "sol_amount"]:
        if candidate.lower() in cols:
            amt_col = cols[candidate.lower()]
            break
    
    # Flow reference column
    flow_ref_col = None
    for candidate in ["signature", "tx_signature", "flow_id"]:
        if candidate.lower() in cols:
            flow_ref_col = cols[candidate.lower()]
            break
    
    if not all([wallet_col, time_col, dir_col, amt_col, flow_ref_col]):
        print("ERROR: Could not discover all required columns in wallet_token_flow")
        print(f"Found: wallet={wallet_col}, time={time_col}, dir={dir_col}, "
              f"amt={amt_col}, flow_ref={flow_ref_col}")
        sys.exit(1)
    
    return wallet_col, time_col, dir_col, amt_col, flow_ref_col


def normalize_direction(direction_value):
    """Normalize direction to 'BUY' or 'SELL'"""
    if direction_value is None:
        return None
    d = str(direction_value).upper().strip()
    if d in ["BUY", "IN", "INFLOW", "RECEIVE", "RECEIVED"]:
        return "BUY"
    elif d in ["SELL", "OUT", "OUTFLOW", "SEND", "SENT"]:
        return "SELL"
    return None


def parse_timestamp(time_val):
    """
    Parse timestamp to datetime object, handling both string and numeric formats.
    Returns naive datetime for consistency in comparisons.
    """
    if time_val is None:
        return None
    
    try:
        if isinstance(time_val, str):
            # Handle ISO format with optional timezone
            dt = datetime.fromisoformat(time_val.replace('Z', '+00:00'))
            # Convert to naive datetime for consistent comparison
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            return dt
        else:
            # Assume Unix timestamp
            return datetime.fromtimestamp(time_val)
    except (ValueError, TypeError, OSError):
        return None


def sanity_check_event_types(conn, type_col):
    """
    Read distinct event_type values from whale_events and verify they match canonical set.
    Uses discovered column name for event_type.
    """
    print("\n" + "="*80)
    print("SANITY CHECK: Verifying event_type values in whale_events")
    print("="*80)
    
    cur = conn.cursor()
    cur.execute(f"SELECT DISTINCT {type_col} FROM whale_events")
    db_event_types = {row[0] for row in cur.fetchall()}
    
    print(f"\nDistinct event_type values in whale_events ({len(db_event_types)}):")
    for evt in sorted(db_event_types):
        print(f"  - {evt}")
    
    print(f"\nCanonical event_type values expected ({len(ALL_CANONICAL_EVENT_TYPES)}):")
    for evt in sorted(ALL_CANONICAL_EVENT_TYPES):
        print(f"  - {evt}")
    
    if not db_event_types.issubset(ALL_CANONICAL_EVENT_TYPES):
        unexpected = db_event_types - ALL_CANONICAL_EVENT_TYPES
        print(f"\n❌ FAIL: Found unexpected event_type values in database:")
        for evt in sorted(unexpected):
            print(f"  - {evt}")
        print("\nDatabase event_types do not match canonical set. Exiting.")
        sys.exit(1)
    
    print("\n✓ PASS: All database event_types are valid and canonical.")
    print("="*80)


def load_whale_events(conn, whale_schema):
    """
    Load all whale_events from database using discovered schema.
    Returns dict: {(wallet, window, event_type, event_time, flow_ref): (sol_amount, flow_count)}
    """
    wallet_col, window_col, type_col, time_col, ref_col, amt_col, count_col = whale_schema
    
    cur = conn.cursor()
    query = f"""
        SELECT {wallet_col}, {window_col}, {type_col}, {time_col}, 
               {ref_col}, {amt_col}, {count_col}
        FROM whale_events
    """
    cur.execute(query)
    
    db_events = {}
    for row in cur.fetchall():
        wallet, window, evt_type, evt_time, flow_ref, amt, count = row
        # Parse event_time to ensure consistent datetime comparison with recomputed events
        parsed_time = parse_timestamp(evt_time)
        if parsed_time is None:
            # Skip events with invalid timestamps
            continue
        key = (wallet, window, evt_type, parsed_time, flow_ref)
        db_events[key] = (amt, count)
    
    return db_events


def recompute_whale_events(conn, t_tx, t_cum_24h, t_cum_7d, 
                           wallet_col, time_col, dir_col, amt_col, flow_ref_col):
    """
    Recompute whale events from wallet_token_flow using CANONICAL event_type strings.
    Returns dict: {(wallet, window, event_type, event_time, flow_ref): (sol_amount, flow_count)}
    """
    cur = conn.cursor()
    
    # Load all flows
    query = f"""
        SELECT {wallet_col}, {time_col}, {dir_col}, {amt_col}, {flow_ref_col}
        FROM wallet_token_flow
        ORDER BY {wallet_col}, {time_col}
    """
    cur.execute(query)
    
    flows = []
    for row in cur.fetchall():
        wallet, time_val, dir_val, amt_val, flow_ref = row
        direction = normalize_direction(dir_val)
        if direction is None:
            continue
        
        try:
            amount = int(amt_val) if amt_val is not None else 0
        except (ValueError, TypeError):
            amount = 0
        
        if amount <= 0:
            continue
        
        # Parse time with improved handling
        flow_time = parse_timestamp(time_val)
        if flow_time is None:
            continue
        
        flows.append({
            'wallet': wallet,
            'time': flow_time,
            'direction': direction,
            'amount': amount,
            'flow_ref': flow_ref
        })
    
    # Recompute events
    recomputed = {}
    
    # Variant A: Single-tx events
    for flow in flows:
        if flow['amount'] >= t_tx:
            evt_type = EVT_TX_BUY if flow['direction'] == 'BUY' else EVT_TX_SELL
            key = (flow['wallet'], 'SINGLE_TX', evt_type, flow['time'], flow['flow_ref'])
            recomputed[key] = (flow['amount'], 1)
    
    # Variant B & C: Cumulative events (24h and 7d)
    wallet_flows = defaultdict(list)
    for flow in flows:
        wallet_flows[flow['wallet']].append(flow)
    
    for wallet, wflows in wallet_flows.items():
        wflows.sort(key=lambda f: f['time'])
        
        for i, anchor_flow in enumerate(wflows):
            # 24h window
            window_end_24h = anchor_flow['time']
            window_start_24h = window_end_24h - timedelta(hours=24)
            
            buy_sum_24h = 0
            sell_sum_24h = 0
            buy_count_24h = 0
            sell_count_24h = 0
            
            for f in wflows[:i+1]:
                if window_start_24h <= f['time'] <= window_end_24h:
                    if f['direction'] == 'BUY':
                        buy_sum_24h += f['amount']
                        buy_count_24h += 1
                    else:
                        sell_sum_24h += f['amount']
                        sell_count_24h += 1
            
            if buy_sum_24h >= t_cum_24h:
                key = (wallet, 'CUM_24H', EVT_C24_BUY, anchor_flow['time'], anchor_flow['flow_ref'])
                recomputed[key] = (buy_sum_24h, buy_count_24h)
            
            if sell_sum_24h >= t_cum_24h:
                key = (wallet, 'CUM_24H', EVT_C24_SELL, anchor_flow['time'], anchor_flow['flow_ref'])
                recomputed[key] = (sell_sum_24h, sell_count_24h)
            
            # 7d window
            window_end_7d = anchor_flow['time']
            window_start_7d = window_end_7d - timedelta(days=7)
            
            buy_sum_7d = 0
            sell_sum_7d = 0
            buy_count_7d = 0
            sell_count_7d = 0
            
            for f in wflows[:i+1]:
                if window_start_7d <= f['time'] <= window_end_7d:
                    if f['direction'] == 'BUY':
                        buy_sum_7d += f['amount']
                        buy_count_7d += 1
                    else:
                        sell_sum_7d += f['amount']
                        sell_count_7d += 1
            
            if buy_sum_7d >= t_cum_7d:
                key = (wallet, 'CUM_7D', EVT_C7_BUY, anchor_flow['time'], anchor_flow['flow_ref'])
                recomputed[key] = (buy_sum_7d, buy_count_7d)
            
            if sell_sum_7d >= t_cum_7d:
                key = (wallet, 'CUM_7D', EVT_C7_SELL, anchor_flow['time'], anchor_flow['flow_ref'])
                recomputed[key] = (sell_sum_7d, sell_count_7d)
    
    return recomputed


def print_section(title, content=None):
    """Print a formatted section header"""
    print("\n" + "="*80)
    print(f"SECTION: {title}")
    print("="*80)
    if content:
        print(content)


def compare_events(db_events, recomputed_events, sample_n):
    """Compare database events with recomputed events"""
    
    db_keys = set(db_events.keys())
    recomp_keys = set(recomputed_events.keys())
    
    missing = recomp_keys - db_keys
    phantom = db_keys - recomp_keys
    common = db_keys & recomp_keys
    
    print_section("0. BASELINE STATISTICS")
    print(f"Database events:    {len(db_keys):,}")
    print(f"Recomputed events:  {len(recomp_keys):,}")
    print(f"Common events:      {len(common):,}")
    print(f"Missing events:     {len(missing):,}")
    print(f"Phantom events:     {len(phantom):,}")
    
    print_section("1. MISSING EVENTS (in recomputed, not in DB)")
    if missing:
        print(f"Showing up to {sample_n} samples:")
        for key in sorted(missing)[:sample_n]:
            wallet, window, evt_type, evt_time, flow_ref = key
            amt, count = recomputed_events[key]
            print(f"  Wallet: {wallet}, Window: {window}, Type: {evt_type}")
            print(f"    Time: {evt_time}, Flow: {flow_ref}")
            print(f"    Amount: {amt:,} lamports, Count: {count}")
    else:
        print("  None")
    
    print_section("2. PHANTOM EVENTS (in DB, not in recomputed)")
    if phantom:
        print(f"Showing up to {sample_n} samples:")
        for key in sorted(phantom)[:sample_n]:
            wallet, window, evt_type, evt_time, flow_ref = key
            amt, count = db_events[key]
            print(f"  Wallet: {wallet}, Window: {window}, Type: {evt_type}")
            print(f"    Time: {evt_time}, Flow: {flow_ref}")
            print(f"    Amount: {amt:,} lamports, Count: {count}")
    else:
        print("  None")
    
    print_section("3. AMOUNT MISMATCHES (common events with different amounts)")
    amt_mismatches = []
    for key in common:
        db_amt, db_count = db_events[key]
        recomp_amt, recomp_count = recomputed_events[key]
        if db_amt != recomp_amt:
            amt_mismatches.append((key, db_amt, recomp_amt))
    
    if amt_mismatches:
        print(f"Found {len(amt_mismatches):,} amount mismatches")
        print(f"Showing up to {sample_n} samples:")
        for key, db_amt, recomp_amt in amt_mismatches[:sample_n]:
            wallet, window, evt_type, evt_time, flow_ref = key
            print(f"  Wallet: {wallet}, Window: {window}, Type: {evt_type}")
            print(f"    Time: {evt_time}, Flow: {flow_ref}")
            print(f"    DB amount: {db_amt:,}, Recomputed: {recomp_amt:,}")
    else:
        print("  None")
    
    print_section("4. COUNT MISMATCHES (common events with different flow counts)")
    count_mismatches = []
    for key in common:
        db_amt, db_count = db_events[key]
        recomp_amt, recomp_count = recomputed_events[key]
        if db_count != recomp_count:
            count_mismatches.append((key, db_count, recomp_count))
    
    if count_mismatches:
        print(f"Found {len(count_mismatches):,} count mismatches")
        print(f"Showing up to {sample_n} samples:")
        for key, db_count, recomp_count in count_mismatches[:sample_n]:
            wallet, window, evt_type, evt_time, flow_ref = key
            print(f"  Wallet: {wallet}, Window: {window}, Type: {evt_type}")
            print(f"    Time: {evt_time}, Flow: {flow_ref}")
            print(f"    DB count: {db_count}, Recomputed: {recomp_count}")
    else:
        print("  None")
    
    return {
        'missing': len(missing),
        'phantom': len(phantom),
        'amt_mismatch': len(amt_mismatches),
        'count_mismatch': len(count_mismatches)
    }


def rank_variants(stats):
    """Rank variants A/B/C/D by error magnitude"""
    
    print_section("5. VARIANT RANKING")
    
    variants = [
        ('A', stats['missing'], "Recomputed has more events (missing from DB)"),
        ('B', stats['phantom'], "DB has more events (phantom in DB)"),
        ('C', stats['amt_mismatch'], "Amount mismatches"),
        ('D', stats['count_mismatch'], "Flow count mismatches")
    ]
    
    variants.sort(key=lambda x: x[1], reverse=True)
    
    print("\nVariants ranked by error magnitude (highest to lowest):")
    print("-" * 80)
    print(f"{'Rank':<6} {'Variant':<10} {'Error Count':<15} {'Description'}")
    print("-" * 80)
    
    for rank, (variant, count, desc) in enumerate(variants, 1):
        print(f"{rank:<6} {variant:<10} {count:<15,} {desc}")
    
    total_errors = sum(v[1] for v in variants)
    print("-" * 80)
    print(f"{'TOTAL':<16} {total_errors:<15,}")
    print("="*80)
    
    return variants[0][0]  # Return top variant


def main():
    args = parse_args()
    
    print("="*80)
    print("WHALE EVENTS FORENSICS v2 - READ-ONLY ANALYSIS")
    print("="*80)
    print(f"Database: {args.db}")
    print(f"Thresholds:")
    print(f"  Single TX:  {args.t_tx:,} lamports")
    print(f"  Cum 24h:    {args.t_cum_24h:,} lamports")
    print(f"  Cum 7d:     {args.t_cum_7d:,} lamports")
    print(f"Sample size: {args.sample_n}")
    
    # Connect to database
    try:
        conn = sqlite3.connect(args.db)
    except Exception as e:
        print(f"\nERROR: Cannot connect to database: {e}")
        sys.exit(1)
    
    # Discover whale_events schema FIRST (needed for sanity check)
    whale_schema = discover_whale_events_schema(conn)
    wallet_col_we, window_col, type_col, time_col_we, ref_col, amt_col_we, count_col = whale_schema
    print(f"\nDiscovered whale_events schema:")
    print(f"  Wallet:      {wallet_col_we}")
    print(f"  Window:      {window_col}")
    print(f"  Event type:  {type_col}")
    print(f"  Event time:  {time_col_we}")
    print(f"  Flow ref:    {ref_col}")
    print(f"  Amount:      {amt_col_we}")
    print(f"  Flow count:  {count_col}")
    
    # Sanity check event types (using discovered column name)
    sanity_check_event_types(conn, type_col)
    
    # Discover wallet_token_flow schema
    wallet_col, time_col, dir_col, amt_col, flow_ref_col = discover_wallet_token_flow_schema(conn)
    print(f"\nDiscovered wallet_token_flow schema:")
    print(f"  Wallet:    {wallet_col}")
    print(f"  Time:      {time_col}")
    print(f"  Direction: {dir_col}")
    print(f"  Amount:    {amt_col}")
    print(f"  Flow ref:  {flow_ref_col}")
    
    # Load database events
    print("\nLoading whale_events from database...")
    db_events = load_whale_events(conn, whale_schema)
    print(f"Loaded {len(db_events):,} events from database")
    
    # Recompute events
    print("\nRecomputing whale events from wallet_token_flow...")
    recomputed_events = recompute_whale_events(
        conn, args.t_tx, args.t_cum_24h, args.t_cum_7d,
        wallet_col, time_col, dir_col, amt_col, flow_ref_col
    )
    print(f"Recomputed {len(recomputed_events):,} events")
    
    # Compare and analyze
    stats = compare_events(db_events, recomputed_events, args.sample_n)
    
    # Rank variants
    top_variant = rank_variants(stats)
    
    # Conclusion
    total_errors = sum(stats.values())
    if total_errors == 0:
        conclusion = "PERFECT MATCH - Database and recomputation are identical"
    else:
        conclusion = f"PRIMARY DISCREPANCY: Variant {top_variant}"
    
    print(f"\nCONCLUSION: {conclusion}")
    
    conn.close()


if __name__ == "__main__":
    main()
