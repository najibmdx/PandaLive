#!/usr/bin/env python3
"""
panda_phase3_2_forensics.py

PANDA v4 Phase 3.2: Forensic analysis of whale_events acceptance parity mismatch.
READ-ONLY diagnostic script - no mutations to database.

Diagnoses A4 parity differences between actual whale_events and recomputation
from wallet_token_flow by testing multiple ordering variants and flow_ref rules.
"""

import sqlite3
import argparse
import sys
from collections import defaultdict
from typing import Dict, List, Tuple, Set, Optional, Any


# === Configuration ===
DEFAULT_T_TX = 10_000_000_000
DEFAULT_T_CUM_24H = 50_000_000_000
DEFAULT_T_CUM_7D = 200_000_000_000
DEFAULT_SAMPLE_N = 20

WINDOW_24H_SECS = 86400
WINDOW_7D_SECS = 604800

# Event type enums
EVENT_TX_BUY = "TX_BUY"
EVENT_TX_SELL = "TX_SELL"
EVENT_CUM_24H_BUY = "CUM_24H_BUY"
EVENT_CUM_24H_SELL = "CUM_24H_SELL"
EVENT_CUM_7D_BUY = "CUM_7D_BUY"
EVENT_CUM_7D_SELL = "CUM_7D_SELL"

# Direction normalization
BUY_DIRECTIONS = {'buy', 'in', 'receive', 'received'}
SELL_DIRECTIONS = {'sell', 'out', 'sent', 'send'}


# === Helper Functions ===

def normalize_direction(raw_direction: str) -> Optional[str]:
    """Normalize direction to 'buy' or 'sell', or None if unknown."""
    if raw_direction is None:
        return None
    normalized = raw_direction.lower().strip()
    if normalized in BUY_DIRECTIONS:
        return 'buy'
    elif normalized in SELL_DIRECTIONS:
        return 'sell'
    return None


def discover_schema(conn: sqlite3.Connection) -> Dict[str, str]:
    """
    Discover wallet_token_flow schema by examining PRAGMA table_info.
    Returns dict with keys: wallet_col, time_col, dir_col, amt_col, flow_ref_col (or None).
    """
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(wallet_token_flow)")
    columns = [row[1] for row in cursor.fetchall()]
    
    schema = {}
    
    # Wallet column
    wallet_candidates = ['wallet', 'wallet_address', 'scan_wallet']
    schema['wallet_col'] = next((c for c in wallet_candidates if c in columns), None)
    
    # Time column
    time_candidates = ['event_time', 'block_time', 'flow_time', 'timestamp']
    schema['time_col'] = next((c for c in time_candidates if c in columns), None)
    
    # Direction column
    dir_candidates = ['sol_direction', 'direction']
    schema['dir_col'] = next((c for c in dir_candidates if c in columns), None)
    
    # Amount column
    amt_candidates = ['sol_amount_lamports', 'amount_lamports', 'lamports', 'sol_lamports']
    schema['amt_col'] = next((c for c in amt_candidates if c in columns), None)
    
    # Flow reference column
    flow_ref_candidates = ['flow_ref', 'signature', 'flow_id', 'hash', 'tx_signature']
    schema['flow_ref_col'] = next((c for c in flow_ref_candidates if c in columns), None)
    
    return schema


def load_actual_events(conn: sqlite3.Connection) -> Tuple[Dict, Dict]:
    """
    Load actual whale_events from database.
    Returns:
        - events_dict: {(wallet, window, event_type, event_time, flow_ref): (amount, count)}
        - metadata: various counts for baseline reporting
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT wallet, window, event_type, event_time, sol_amount_lamports, 
               supporting_flow_count, flow_ref
        FROM whale_events
    """)
    
    events_dict = {}
    metadata = {
        'total': 0,
        'by_window_type': defaultdict(int),
        'flow_ref_null': 0,
        'flow_ref_empty': 0,
        'flow_ref_nonempty': 0,
        'wallets_by_window': defaultdict(set),
        'flow_ref_null_examples': [],
        'flow_ref_empty_examples': []
    }
    
    for row in cursor.fetchall():
        wallet, window, event_type, event_time, amount, count, flow_ref = row
        
        # Track original flow_ref state before normalization
        if flow_ref is None:
            metadata['flow_ref_null'] += 1
            if len(metadata['flow_ref_null_examples']) < 5:
                metadata['flow_ref_null_examples'].append((wallet, window, event_type, event_time))
        elif flow_ref == '':
            metadata['flow_ref_empty'] += 1
            if len(metadata['flow_ref_empty_examples']) < 5:
                metadata['flow_ref_empty_examples'].append((wallet, window, event_type, event_time))
        else:
            metadata['flow_ref_nonempty'] += 1
        
        # Normalize flow_ref for key matching
        normalized_ref = '' if flow_ref is None else str(flow_ref)
        
        key = (wallet, window, event_type, event_time, normalized_ref)
        events_dict[key] = (amount, count)
        
        metadata['total'] += 1
        metadata['by_window_type'][(window, event_type)] += 1
        metadata['wallets_by_window'][window].add(wallet)
    
    return events_dict, metadata


def load_flows_ordered(conn: sqlite3.Connection, schema: Dict, ordering: str, 
                       focus_wallet: Optional[str] = None) -> List[Tuple]:
    """
    Load wallet_token_flow ordered by the specified variant.
    
    Ordering variants:
        'A': wallet, time, normalized_flow_ref
        'B': wallet, time, rowid
        'C': time, rowid (global)
        'D': wallet, time, normalized_flow_ref, rowid
    
    Returns list of tuples: (wallet, time, direction, amount, normalized_flow_ref, rowid)
    """
    wallet_col = schema['wallet_col']
    time_col = schema['time_col']
    dir_col = schema['dir_col']
    amt_col = schema['amt_col']
    flow_ref_col = schema['flow_ref_col']
    
    if flow_ref_col:
        flow_ref_expr = f"COALESCE(CAST({flow_ref_col} AS TEXT), '')"
    else:
        # No flow_ref column exists. Use empty string for SQL ordering.
        # This means variants A and D will effectively order by wallet/time only,
        # with rowid as implicit tie-breaker (same as variant B).
        # We'll construct "rowid:N" refs in Python for event keys.
        flow_ref_expr = "''"
    
    select_clause = f"""
        SELECT {wallet_col}, {time_col}, {dir_col}, {amt_col}, 
               {flow_ref_expr} AS normalized_flow_ref, rowid
        FROM wallet_token_flow
    """
    
    where_clause = ""
    if focus_wallet:
        where_clause = f" WHERE {wallet_col} = ?"
    
    # Use the actual expression in ORDER BY for better compatibility
    if ordering == 'A':
        order_clause = f" ORDER BY {wallet_col}, {time_col}, {flow_ref_expr}"
    elif ordering == 'B':
        order_clause = f" ORDER BY {wallet_col}, {time_col}, rowid"
    elif ordering == 'C':
        order_clause = f" ORDER BY {time_col}, rowid"
    elif ordering == 'D':
        order_clause = f" ORDER BY {wallet_col}, {time_col}, {flow_ref_expr}, rowid"
    else:
        raise ValueError(f"Unknown ordering: {ordering}")
    
    query = select_clause + where_clause + order_clause
    
    cursor = conn.cursor()
    if focus_wallet:
        cursor.execute(query, (focus_wallet,))
    else:
        cursor.execute(query)
    
    flows = []
    for row in cursor.fetchall():
        wallet, time, raw_dir, raw_amt, norm_ref, rowid = row
        
        direction = normalize_direction(raw_dir)
        if direction is None:
            continue
        
        if raw_amt is None or raw_amt <= 0:
            continue
        
        amount = abs(int(raw_amt))
        
        # Fallback flow_ref if not available
        if not flow_ref_col:
            norm_ref = f"rowid:{rowid}"
        
        flows.append((wallet, time, direction, amount, norm_ref, rowid))
    
    return flows


def recompute_events(flows: List[Tuple], t_tx: int, t_cum_24h: int, t_cum_7d: int) -> Dict:
    """
    Recompute expected whale_events from ordered flows.
    
    Returns dict: {(wallet, window, event_type, event_time, flow_ref): (amount, count)}
    """
    events = {}
    
    # Per-wallet cumulative state
    wallet_state_24h = defaultdict(lambda: {'buy': [], 'sell': []})
    wallet_state_7d = defaultdict(lambda: {'buy': [], 'sell': []})
    
    for wallet, time, direction, amount, flow_ref, rowid in flows:
        # Single-tx events
        if amount >= t_tx:
            event_type = EVENT_TX_BUY if direction == 'buy' else EVENT_TX_SELL
            key = (wallet, 'lifetime', event_type, time, flow_ref)
            events[key] = (amount, 1)
        
        # Cumulative events
        for window_name, window_secs, threshold, state in [
            ('24h', WINDOW_24H_SECS, t_cum_24h, wallet_state_24h),
            ('7d', WINDOW_7D_SECS, t_cum_7d, wallet_state_7d)
        ]:
            # Get current state for this wallet/direction
            wallet_dir_state = state[wallet][direction]
            
            # Expire old entries - filter in place
            expired_filtered = [(t, a, r) for t, a, r in wallet_dir_state 
                               if (time - t) <= window_secs]
            
            # Add current flow
            expired_filtered.append((time, amount, flow_ref))
            
            # Update state
            state[wallet][direction] = expired_filtered
            
            # Check threshold
            sum_after = sum(a for _, a, _ in expired_filtered)
            if sum_after >= threshold:
                event_type = (EVENT_CUM_24H_BUY if window_name == '24h' else EVENT_CUM_7D_BUY) \
                             if direction == 'buy' else \
                             (EVENT_CUM_24H_SELL if window_name == '24h' else EVENT_CUM_7D_SELL)
                
                key = (wallet, window_name, event_type, time, flow_ref)
                events[key] = (sum_after, len(expired_filtered))
    
    return events


def compare_events(actual: Dict, expected: Dict, sample_n: int) -> Dict:
    """
    Compare actual vs expected events.
    
    Returns dict with mismatch details.
    """
    actual_keys = set(actual.keys())
    expected_keys = set(expected.keys())
    
    phantoms = actual_keys - expected_keys
    missing = expected_keys - actual_keys
    common = actual_keys & expected_keys
    
    amount_mismatches = []
    count_mismatches = []
    
    for key in common:
        actual_amt, actual_cnt = actual[key]
        expected_amt, expected_cnt = expected[key]
        
        if actual_amt != expected_amt:
            amount_mismatches.append((key, expected_amt, actual_amt))
        
        if actual_cnt != expected_cnt:
            count_mismatches.append((key, expected_cnt, actual_cnt))
    
    return {
        'phantoms': sorted(list(phantoms))[:sample_n],
        'phantom_count': len(phantoms),
        'missing': sorted(list(missing))[:sample_n],
        'missing_count': len(missing),
        'amount_mismatches': amount_mismatches[:sample_n],
        'amount_mismatch_count': len(amount_mismatches),
        'count_mismatches': count_mismatches[:sample_n],
        'count_mismatch_count': len(count_mismatches),
        'total_mismatch': len(phantoms) + len(missing) + len(amount_mismatches) + len(count_mismatches)
    }


def analyze_ties(conn: sqlite3.Connection, schema: Dict, mismatch_samples: List, 
                 sample_n: int) -> None:
    """
    Analyze timestamp ties for wallets involved in mismatches.
    """
    wallet_col = schema['wallet_col']
    time_col = schema['time_col']
    dir_col = schema['dir_col']
    amt_col = schema['amt_col']
    flow_ref_col = schema['flow_ref_col']
    
    # Collect unique (wallet, event_time) pairs
    wallet_time_pairs = set()
    for key in mismatch_samples:
        wallet, window, event_type, event_time, flow_ref = key
        wallet_time_pairs.add((wallet, event_time))
    
    wallet_time_pairs = sorted(list(wallet_time_pairs))[:sample_n]
    
    print(f"\n=== SECTION 3: Tie Analysis ===")
    print(f"Analyzing up to {len(wallet_time_pairs)} (wallet, event_time) pairs involved in mismatches:\n")
    
    cursor = conn.cursor()
    
    for wallet, event_time in wallet_time_pairs:
        print(f"\nWallet: {wallet}, Event Time: {event_time}")
        
        # Count flows at exact timestamp
        cursor.execute(f"""
            SELECT COUNT(*) FROM wallet_token_flow
            WHERE {wallet_col} = ? AND {time_col} = ?
        """, (wallet, event_time))
        exact_count = cursor.fetchone()[0]
        print(f"  Flows at exact timestamp: {exact_count}")
        
        # Show first 10 flows at this timestamp
        if flow_ref_col:
            flow_ref_expr = f"COALESCE(CAST({flow_ref_col} AS TEXT), '')"
        else:
            flow_ref_expr = "''"
        
        cursor.execute(f"""
            SELECT {flow_ref_expr}, {amt_col}, {dir_col}, rowid
            FROM wallet_token_flow
            WHERE {wallet_col} = ? AND {time_col} = ?
            ORDER BY rowid
            LIMIT 10
        """, (wallet, event_time))
        
        flows_at_time = cursor.fetchall()
        if flows_at_time:
            print(f"  First {len(flows_at_time)} flows:")
            for flow_ref, amt, direction, rowid in flows_at_time:
                print(f"    flow_ref={flow_ref}, amount={amt}, direction={direction}, rowid={rowid}")
        
        # Count flows in 24h window before this timestamp
        cursor.execute(f"""
            SELECT COUNT(*) FROM wallet_token_flow
            WHERE {wallet_col} = ? 
              AND {time_col} >= ? AND {time_col} <= ?
        """, (wallet, event_time - WINDOW_24H_SECS, event_time))
        window_24h_count = cursor.fetchone()[0]
        print(f"  Flows in 24h window [time-86400, time]: {window_24h_count}")
        
        # Count flows in 7d window before this timestamp
        cursor.execute(f"""
            SELECT COUNT(*) FROM wallet_token_flow
            WHERE {wallet_col} = ? 
              AND {time_col} >= ? AND {time_col} <= ?
        """, (wallet, event_time - WINDOW_7D_SECS, event_time))
        window_7d_count = cursor.fetchone()[0]
        print(f"  Flows in 7d window [time-604800, time]: {window_7d_count}")


def analyze_flow_ref_normalization(conn: sqlite3.Connection, schema: Dict) -> None:
    """
    Analyze flow_ref normalization in wallet_token_flow.
    """
    print(f"\n=== SECTION 4: Flow_ref Normalization Evidence ===")
    
    flow_ref_col = schema['flow_ref_col']
    
    if not flow_ref_col:
        print("No flow_ref column found in wallet_token_flow.")
        print("All flow_refs are fallback 'rowid:<rowid>' format.\n")
        return
    
    cursor = conn.cursor()
    
    # Count NULL flow_refs
    cursor.execute(f"""
        SELECT COUNT(*) FROM wallet_token_flow
        WHERE {flow_ref_col} IS NULL
    """)
    null_count = cursor.fetchone()[0]
    
    # Count empty string flow_refs
    cursor.execute(f"""
        SELECT COUNT(*) FROM wallet_token_flow
        WHERE {flow_ref_col} = ''
    """)
    empty_count = cursor.fetchone()[0]
    
    print(f"Flow_ref column: {flow_ref_col}")
    print(f"  NULL values: {null_count}")
    print(f"  Empty string values: {empty_count}")
    
    # Show examples of NULL
    if null_count > 0:
        cursor.execute(f"""
            SELECT rowid, {schema['time_col']}
            FROM wallet_token_flow
            WHERE {flow_ref_col} IS NULL
            LIMIT 5
        """)
        null_examples = cursor.fetchall()
        print(f"\n  First 5 NULL flow_ref examples:")
        for rowid, time in null_examples:
            print(f"    rowid={rowid}, time={time}")
    
    # Show examples of empty string
    if empty_count > 0:
        cursor.execute(f"""
            SELECT rowid, {schema['time_col']}
            FROM wallet_token_flow
            WHERE {flow_ref_col} = ''
            LIMIT 5
        """)
        empty_examples = cursor.fetchall()
        print(f"\n  First 5 empty string flow_ref examples:")
        for rowid, time in empty_examples:
            print(f"    rowid={rowid}, time={time}")
    
    print()


def main():
    parser = argparse.ArgumentParser(description='PANDA Phase 3.2 Forensics')
    parser.add_argument('--db', required=True, help='Path to masterwalletsdb.db')
    parser.add_argument('--t-tx', type=int, default=DEFAULT_T_TX, 
                       help='Single-tx threshold (lamports)')
    parser.add_argument('--t-cum-24h', type=int, default=DEFAULT_T_CUM_24H,
                       help='24h cumulative threshold (lamports)')
    parser.add_argument('--t-cum-7d', type=int, default=DEFAULT_T_CUM_7D,
                       help='7d cumulative threshold (lamports)')
    parser.add_argument('--sample-n', type=int, default=DEFAULT_SAMPLE_N,
                       help='Number of samples to show')
    parser.add_argument('--wallet', help='Focus on specific wallet')
    parser.add_argument('--event-time', type=int, help='Focus on specific event time')
    parser.add_argument('--window', choices=['24h', '7d', 'lifetime'], help='Focus window')
    parser.add_argument('--event-type', help='Focus event type')
    
    args = parser.parse_args()
    
    # Connect to database
    try:
        conn = sqlite3.connect(args.db)
        print(f"Connected to database: {args.db}\n")
    except Exception as e:
        print(f"ERROR: Could not connect to database: {e}", file=sys.stderr)
        return 1
    
    # Discover schema
    try:
        schema = discover_schema(conn)
    except Exception as e:
        print(f"ERROR: Could not access wallet_token_flow table: {e}", file=sys.stderr)
        return 1
    
    print("=== Schema Discovery ===")
    for key, value in schema.items():
        print(f"  {key}: {value}")
    print()
    
    # Validate schema
    if not all([schema['wallet_col'], schema['time_col'], schema['dir_col'], schema['amt_col']]):
        print("ERROR: Could not discover required columns in wallet_token_flow", file=sys.stderr)
        return 1
    
    # === SECTION 0: Baseline counts ===
    print("=== SECTION 0: Baseline Counts ===")
    try:
        all_actual_events, metadata = load_actual_events(conn)
    except Exception as e:
        print(f"ERROR: Could not load whale_events table: {e}", file=sys.stderr)
        return 1
    
    # Apply focus filters if provided
    actual_events = all_actual_events
    if args.wallet or args.event_time or args.window or args.event_type:
        filtered_events = {}
        for key, value in all_actual_events.items():
            wallet, window, event_type, event_time, flow_ref = key
            if args.wallet and wallet != args.wallet:
                continue
            if args.event_time and event_time != args.event_time:
                continue
            if args.window and window != args.window:
                continue
            if args.event_type and event_type != args.event_type:
                continue
            filtered_events[key] = value
        
        actual_events = filtered_events
        print(f"\nFocus filters applied:")
        if args.wallet:
            print(f"  Wallet: {args.wallet}")
        if args.event_time:
            print(f"  Event time: {args.event_time}")
        if args.window:
            print(f"  Window: {args.window}")
        if args.event_type:
            print(f"  Event type: {args.event_type}")
        print(f"  Filtered to {len(actual_events)} events (from {len(all_actual_events)} total)\n")
    
    print(f"Total whale_events: {metadata['total']}")
    print(f"\nWhale_events by (window, event_type):")
    for (window, event_type), count in sorted(metadata['by_window_type'].items()):
        print(f"  {window:8s} {event_type:15s}: {count:6d}")
    
    print(f"\nFlow_ref distribution in whale_events:")
    print(f"  NULL: {metadata['flow_ref_null']}")
    print(f"  Empty string: {metadata['flow_ref_empty']}")
    print(f"  Non-empty: {metadata['flow_ref_nonempty']}")
    
    if metadata['flow_ref_null_examples']:
        print(f"\n  First {len(metadata['flow_ref_null_examples'])} NULL flow_ref examples:")
        for wallet, window, event_type, event_time in metadata['flow_ref_null_examples']:
            print(f"    wallet={wallet}, window={window}, type={event_type}, time={event_time}")
    
    if metadata['flow_ref_empty_examples']:
        print(f"\n  First {len(metadata['flow_ref_empty_examples'])} empty flow_ref examples:")
        for wallet, window, event_type, event_time in metadata['flow_ref_empty_examples']:
            print(f"    wallet={wallet}, window={window}, type={event_type}, time={event_time}")
    
    print(f"\nDistinct wallets by window:")
    for window in sorted(metadata['wallets_by_window'].keys()):
        print(f"  {window}: {len(metadata['wallets_by_window'][window])}")
    
    # === SECTION 1 & 2: Recompute and compare ===
    print("\n=== SECTION 1 & 2: Recomputation Variants ===")
    
    variants = {
        'A': 'wallet, time, normalized_flow_ref',
        'B': 'wallet, time, rowid',
        'C': 'time, rowid',
        'D': 'wallet, time, normalized_flow_ref, rowid'
    }
    
    variant_results = {}
    
    for variant_id, variant_desc in variants.items():
        print(f"\nVariant {variant_id}: ORDER BY {variant_desc}")
        
        # Load flows in this ordering
        flows = load_flows_ordered(conn, schema, variant_id, args.wallet)
        print(f"  Loaded {len(flows)} valid flows")
        
        # Recompute events
        expected_events = recompute_events(flows, args.t_tx, args.t_cum_24h, args.t_cum_7d)
        print(f"  Expected events: {len(expected_events)}")
        
        # Compare
        comparison = compare_events(actual_events, expected_events, args.sample_n)
        variant_results[variant_id] = {
            'expected': expected_events,
            'comparison': comparison
        }
        
        print(f"  Phantoms (actual - expected): {comparison['phantom_count']}")
        print(f"  Missing (expected - actual): {comparison['missing_count']}")
        print(f"  Amount mismatches: {comparison['amount_mismatch_count']}")
        print(f"  Count mismatches: {comparison['count_mismatch_count']}")
        print(f"  Total mismatches: {comparison['total_mismatch']}")
    
    # Rank variants
    print("\n=== Variant Ranking ===")
    print(f"{'Variant':<10} {'Phantoms':<10} {'Missing':<10} {'Amt Mismatch':<13} {'Cnt Mismatch':<13} {'Total':<10}")
    print("-" * 76)
    
    ranked = sorted(variant_results.items(), 
                   key=lambda x: x[1]['comparison']['total_mismatch'])
    
    for variant_id, result in ranked:
        cmp = result['comparison']
        print(f"{variant_id:<10} {cmp['phantom_count']:<10} {cmp['missing_count']:<10} "
              f"{cmp['amount_mismatch_count']:<13} {cmp['count_mismatch_count']:<13} "
              f"{cmp['total_mismatch']:<10}")
    
    # Best variant
    best_variant_id = ranked[0][0]
    best_comparison = ranked[0][1]['comparison']
    
    print(f"\n=== Best Variant: {best_variant_id} ===")
    print(f"ORDER BY {variants[best_variant_id]}")
    print(f"Total mismatches: {best_comparison['total_mismatch']}\n")
    
    # Show detailed samples for best variant
    if best_comparison['phantoms']:
        print(f"First {len(best_comparison['phantoms'])} phantom events (in actual, not in expected):")
        for key in best_comparison['phantoms']:
            wallet, window, event_type, event_time, flow_ref = key
            actual_amt, actual_cnt = actual_events[key]
            print(f"  wallet={wallet}, window={window}, type={event_type}, "
                  f"time={event_time}, flow_ref={flow_ref}")
            print(f"    actual: amount={actual_amt}, count={actual_cnt}")
        print()
    
    if best_comparison['missing']:
        print(f"First {len(best_comparison['missing'])} missing events (in expected, not in actual):")
        for key in best_comparison['missing']:
            wallet, window, event_type, event_time, flow_ref = key
            expected_amt, expected_cnt = ranked[0][1]['expected'][key]
            print(f"  wallet={wallet}, window={window}, type={event_type}, "
                  f"time={event_time}, flow_ref={flow_ref}")
            print(f"    expected: amount={expected_amt}, count={expected_cnt}")
        print()
    
    if best_comparison['amount_mismatches']:
        print(f"First {len(best_comparison['amount_mismatches'])} amount mismatches:")
        for key, expected_amt, actual_amt in best_comparison['amount_mismatches']:
            wallet, window, event_type, event_time, flow_ref = key
            print(f"  wallet={wallet}, window={window}, type={event_type}, "
                  f"time={event_time}, flow_ref={flow_ref}")
            print(f"    expected: {expected_amt}, actual: {actual_amt}")
        print()
    
    if best_comparison['count_mismatches']:
        print(f"First {len(best_comparison['count_mismatches'])} count mismatches:")
        for key, expected_cnt, actual_cnt in best_comparison['count_mismatches']:
            wallet, window, event_type, event_time, flow_ref = key
            print(f"  wallet={wallet}, window={window}, type={event_type}, "
                  f"time={event_time}, flow_ref={flow_ref}")
            print(f"    expected: {expected_cnt}, actual: {actual_cnt}")
        print()
    
    # === SECTION 3: Tie analysis ===
    all_mismatch_keys = (best_comparison['phantoms'] + 
                         best_comparison['missing'] + 
                         [k for k, _, _ in best_comparison['amount_mismatches']] +
                         [k for k, _, _ in best_comparison['count_mismatches']])
    
    if all_mismatch_keys:
        analyze_ties(conn, schema, all_mismatch_keys, args.sample_n)
    
    # === SECTION 4: Flow_ref normalization ===
    analyze_flow_ref_normalization(conn, schema)
    
    # === SECTION 5: Conclusion ===
    print("=== SECTION 5: Conclusion ===\n")
    
    best_total = best_comparison['total_mismatch']
    
    if best_total == 0:
        print(f"CONCLUSION: Perfect parity achieved with variant {best_variant_id}. "
              "No further action needed.")
    elif best_total < 100:
        # Check if ordering helps significantly
        worst_total = ranked[-1][1]['comparison']['total_mismatch']
        if worst_total - best_total > 50:
            print(f"CONCLUSION: Acceptance mismatch is due to ordering/tie-break differences; "
                  f"align acceptance ordering to variant {best_variant_id} "
                  f"(ORDER BY {variants[best_variant_id]}).")
        else:
            # Check flow_ref issues
            if metadata['flow_ref_null'] + metadata['flow_ref_empty'] > 0:
                print(f"CONCLUSION: Acceptance mismatch is due to flow_ref normalization differences; "
                      f"align acceptance flow_ref rule to handle NULL/empty values consistently.")
            else:
                print(f"CONCLUSION: Builder and recomputation disagree beyond ordering/ref normalization; "
                      f"inspect builder logic filters (direction/amount inclusion) next.")
    else:
        # Significant mismatches remain
        if metadata['flow_ref_null'] + metadata['flow_ref_empty'] > best_total * 0.5:
            print(f"CONCLUSION: Acceptance mismatch is due to flow_ref normalization differences; "
                  f"align acceptance flow_ref rule to handle NULL/empty values consistently.")
        else:
            print(f"CONCLUSION: Builder and recomputation disagree beyond ordering/ref normalization; "
                  f"inspect builder logic filters (direction/amount inclusion) next.")
    
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
