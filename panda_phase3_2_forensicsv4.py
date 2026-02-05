#!/usr/bin/env python3
"""
panda_phase3_2_forensicsv4_latch.py

READ-ONLY forensic analysis to identify exact logic mismatch between whale_events (DB)
and recomputation from wallet_token_flow.

Tests:
  (A) continuous emission (current behavior)
  (B) latched emission (state transition emission)
  (C) window boundary variants (inclusive/exclusive cutoffs)
  (D) flow_ref definition variants for cumulative events (anchor vs triggering signature)

Hard rules:
  - READ ONLY: no INSERT/UPDATE/DELETE, no temp tables
  - Deterministic output
  - Auto-discover column names
  - Use exact DB values for windows and event_types
"""

import sqlite3
import argparse
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Set, Optional
import sys

# Constants
SINGLE_TX_THRESHOLD = 10_000_000_000  # 10 SOL
CUM_24H_THRESHOLD = 50_000_000_000    # 50 SOL
CUM_7D_THRESHOLD = 200_000_000_000    # 200 SOL

WINDOW_SECONDS = {
    '24h': 24 * 3600,
    '7d': 7 * 24 * 3600,
    'lifetime': None
}

CANONICAL_EVENT_TYPES = {
    'WHALE_TX_BUY',
    'WHALE_TX_SELL',
    'WHALE_CUM_24H_BUY',
    'WHALE_CUM_24H_SELL',
    'WHALE_CUM_7D_BUY',
    'WHALE_CUM_7D_SELL'
}


class ColumnMapper:
    """Auto-discover and map column names with flexible matching."""
    
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.whale_events_cols = {}
        self.wallet_flow_cols = {}
        
    def discover_whale_events(self) -> bool:
        """Discover whale_events column names with flexible matching."""
        cursor = self.conn.execute("PRAGMA table_info(whale_events)")
        cols = {row[1].lower(): row[1] for row in cursor.fetchall()}  # Store original case
        
        # Define required columns with possible variations
        required_mapping = {
            'wallet': ['scan_wallet', 'wallet', 'wallet_address', 'address'],
            'window': ['window', 'time_window', 'window_type'],
            'event_type': ['event_type', 'type', 'event_name'],
            'event_time': ['event_time', 'timestamp', 'time', 'block_time'],
            'flow_ref': ['flow_ref', 'signature', 'tx_signature', 'reference'],
            'amount': ['sol_amount_lamports', 'amount', 'lamports', 'sol_amount'],
            'count': ['supporting_flow_count', 'flow_count', 'count', 'num_flows']
        }
        
        found_cols = {}
        missing = []
        
        for key, variations in required_mapping.items():
            found = False
            for var in variations:
                if var.lower() in cols:
                    found_cols[key] = cols[var.lower()]  # Use actual column name
                    found = True
                    break
            
            if not found:
                missing.append(f"{key} (tried: {', '.join(variations)})")
        
        if missing:
            print(f"ERROR: whale_events missing required columns:")
            for m in missing:
                print(f"  - {m}")
            print(f"\nAvailable columns: {', '.join(cols.values())}")
            return False
        
        self.whale_events_cols = found_cols
        print(f"✓ whale_events columns mapped:")
        for key, col in found_cols.items():
            print(f"  {key:15s} -> {col}")
        
        return True
    
    def discover_wallet_token_flow(self) -> bool:
        """Discover wallet_token_flow column names with flexible matching."""
        cursor = self.conn.execute("PRAGMA table_info(wallet_token_flow)")
        cols = {row[1].lower(): row[1] for row in cursor.fetchall()}
        
        required_mapping = {
            'wallet': ['scan_wallet', 'wallet', 'wallet_address', 'address'],
            'block_time': ['block_time', 'timestamp', 'time'],
            'direction': ['sol_direction', 'direction', 'type', 'side'],
            'amount': ['sol_amount_lamports', 'amount', 'lamports', 'sol_amount'],
            'signature': ['signature', 'tx_signature', 'transaction_signature', 'sig']
        }
        
        found_cols = {}
        missing = []
        
        for key, variations in required_mapping.items():
            found = False
            for var in variations:
                if var.lower() in cols:
                    found_cols[key] = cols[var.lower()]
                    found = True
                    break
            
            if not found:
                missing.append(f"{key} (tried: {', '.join(variations)})")
        
        if missing:
            print(f"ERROR: wallet_token_flow missing required columns:")
            for m in missing:
                print(f"  - {m}")
            print(f"\nAvailable columns: {', '.join(cols.values())}")
            return False
        
        self.wallet_flow_cols = found_cols
        print(f"✓ wallet_token_flow columns mapped:")
        for key, col in found_cols.items():
            print(f"  {key:15s} -> {col}")
        
        return True


class FlowEvent:
    """Represents a single wallet token flow."""
    
    def __init__(self, wallet: str, block_time: int, direction: str, 
                 amount_lamports: int, signature: str):
        self.wallet = wallet if wallet else ''
        self.block_time = block_time
        self.direction = direction.upper() if direction else ''  # Normalize to BUY/SELL
        self.amount_lamports = abs(amount_lamports) if amount_lamports else 0
        self.signature = signature if signature else ''
    
    def __repr__(self):
        return f"Flow({self.direction} {self.amount_lamports} @ {self.block_time})"


class WhaleEvent:
    """Represents a whale event (baseline or recomputed)."""
    
    def __init__(self, wallet: str, window: str, event_type: str, 
                 event_time: int, flow_ref: str, amount: int, count: int):
        self.wallet = wallet if wallet else ''
        self.window = window if window else ''
        self.event_type = event_type if event_type else ''
        self.event_time = event_time if event_time else 0
        self.flow_ref = flow_ref if flow_ref else ''
        self.amount = amount if amount else 0
        self.count = count if count else 0
    
    def key(self) -> Tuple:
        """Return tuple key for comparison."""
        return (self.wallet, self.window, self.event_type, 
                self.event_time, self.flow_ref)
    
    def __repr__(self):
        return f"Whale({self.event_type} {self.window} @ {self.event_time})"


def load_baseline_events(conn: sqlite3.Connection, mapper: ColumnMapper) -> Dict[Tuple, WhaleEvent]:
    """Load baseline whale_events from DB."""
    print("\n" + "="*80)
    print("LOADING BASELINE WHALE_EVENTS")
    print("="*80)
    
    cols = mapper.whale_events_cols
    query = f"""
        SELECT {cols['wallet']}, {cols['window']}, {cols['event_type']}, 
               {cols['event_time']}, {cols['flow_ref']},
               {cols['amount']}, {cols['count']}
        FROM whale_events
        ORDER BY {cols['wallet']}, {cols['event_time']}, {cols['window']}, {cols['event_type']}
    """
    
    cursor = conn.execute(query)
    events = {}
    
    for row in cursor:
        event = WhaleEvent(
            wallet=row[0],
            window=row[1],
            event_type=row[2],
            event_time=row[3],
            flow_ref=row[4],
            amount=row[5],
            count=row[6]
        )
        events[event.key()] = event
    
    # Print statistics
    print(f"Total baseline events: {len(events)}")
    
    # Count by (window, event_type)
    counts = defaultdict(int)
    windows = set()
    min_time = float('inf')
    max_time = 0
    
    for event in events.values():
        counts[(event.window, event.event_type)] += 1
        windows.add(event.window)
        min_time = min(min_time, event.event_time)
        max_time = max(max_time, event.event_time)
    
    print("\nCounts by (window, event_type):")
    for (window, event_type), count in sorted(counts.items()):
        print(f"  {window:10s} {event_type:25s} : {count:6d}")
    
    print(f"\nDistinct windows: {sorted(windows)}")
    if min_time != float('inf'):
        print(f"Event time range: {min_time} to {max_time}")
    else:
        print("Event time range: (no events)")
    
    return events


def load_wallet_flows(conn: sqlite3.Connection, mapper: ColumnMapper) -> Dict[str, List[FlowEvent]]:
    """Load wallet token flows ordered by wallet, time, rowid."""
    print("\n" + "="*80)
    print("LOADING WALLET_TOKEN_FLOW")
    print("="*80)
    
    cols = mapper.wallet_flow_cols
    query = f"""
        SELECT {cols['wallet']}, {cols['block_time']}, {cols['direction']}, 
               {cols['amount']}, {cols['signature']}
        FROM wallet_token_flow
        ORDER BY {cols['wallet']}, {cols['block_time']}, rowid
    """
    
    cursor = conn.execute(query)
    flows_by_wallet = defaultdict(list)
    total_flows = 0
    
    for row in cursor:
        wallet, block_time, direction, amount, signature = row
        
        # Normalize direction
        direction = direction.upper() if direction else ''
        if direction not in ('BUY', 'SELL'):
            continue
        
        flow = FlowEvent(wallet, block_time, direction, amount, signature)
        flows_by_wallet[wallet].append(flow)
        total_flows += 1
    
    print(f"Total flows loaded: {total_flows}")
    print(f"Distinct wallets: {len(flows_by_wallet)}")
    
    return flows_by_wallet


def get_flows_in_window(flows: List[FlowEvent], anchor_idx: int, 
                       window_seconds: int, boundary_variant: str) -> List[FlowEvent]:
    """Get flows within window relative to anchor, in chronological order."""
    anchor_time = flows[anchor_idx].block_time
    start_time = anchor_time - window_seconds
    
    result = []
    for i in range(len(flows)):
        flow = flows[i]
        
        if boundary_variant == 'W1':
            # Inclusive: >= start_time AND <= anchor_time
            if flow.block_time >= start_time and flow.block_time <= anchor_time:
                result.append(flow)
        elif boundary_variant == 'W2':
            # Exclusive lower: > start_time AND <= anchor_time
            if flow.block_time > start_time and flow.block_time <= anchor_time:
                result.append(flow)
    
    # Flows should already be sorted, but ensure chronological order
    # This is critical for find_triggering_signature to work correctly
    return result


def compute_continuous_events(flows_by_wallet: Dict[str, List[FlowEvent]], 
                              boundary_variant: str) -> Dict[Tuple, WhaleEvent]:
    """
    Engine E1: CONTINUOUS emission.
    For each flow (anchor), check if it triggers single-tx or cumulative events.
    """
    events = {}
    
    for wallet, flows in flows_by_wallet.items():
        for anchor_idx, anchor in enumerate(flows):
            anchor_time = anchor.block_time
            anchor_sig = anchor.signature
            
            # Single-tx events
            if anchor.amount_lamports >= SINGLE_TX_THRESHOLD:
                event_type = f"WHALE_TX_{anchor.direction}"
                event = WhaleEvent(
                    wallet=wallet,
                    window='lifetime',
                    event_type=event_type,
                    event_time=anchor_time,
                    flow_ref=anchor_sig,
                    amount=anchor.amount_lamports,
                    count=1
                )
                events[event.key()] = event
            
            # Cumulative 24h
            window_flows = get_flows_in_window(flows, anchor_idx, 
                                              WINDOW_SECONDS['24h'], boundary_variant)
            
            for direction in ['BUY', 'SELL']:
                dir_flows = [f for f in window_flows if f.direction == direction]
                total = sum(f.amount_lamports for f in dir_flows)
                
                if total >= CUM_24H_THRESHOLD:
                    event_type = f"WHALE_CUM_24H_{direction}"
                    event = WhaleEvent(
                        wallet=wallet,
                        window='24h',
                        event_type=event_type,
                        event_time=anchor_time,
                        flow_ref=anchor_sig,
                        amount=total,
                        count=len(dir_flows)
                    )
                    events[event.key()] = event
            
            # Cumulative 7d
            window_flows = get_flows_in_window(flows, anchor_idx, 
                                              WINDOW_SECONDS['7d'], boundary_variant)
            
            for direction in ['BUY', 'SELL']:
                dir_flows = [f for f in window_flows if f.direction == direction]
                total = sum(f.amount_lamports for f in dir_flows)
                
                if total >= CUM_7D_THRESHOLD:
                    event_type = f"WHALE_CUM_7D_{direction}"
                    event = WhaleEvent(
                        wallet=wallet,
                        window='7d',
                        event_type=event_type,
                        event_time=anchor_time,
                        flow_ref=anchor_sig,
                        amount=total,
                        count=len(dir_flows)
                    )
                    events[event.key()] = event
    
    return events


def compute_latched_events(flows_by_wallet: Dict[str, List[FlowEvent]], 
                          boundary_variant: str, 
                          flow_ref_variant: str) -> Dict[Tuple, WhaleEvent]:
    """
    Engine E2: LATCHED emission (state transitions).
    Emit event only when condition transitions from False to True.
    """
    events = {}
    
    for wallet, flows in flows_by_wallet.items():
        # Track previous state for each cumulative type
        prev_state = {
            ('24h', 'BUY'): False,
            ('24h', 'SELL'): False,
            ('7d', 'BUY'): False,
            ('7d', 'SELL'): False
        }
        
        for anchor_idx, anchor in enumerate(flows):
            anchor_time = anchor.block_time
            anchor_sig = anchor.signature
            
            # Single-tx events (same as continuous)
            if anchor.amount_lamports >= SINGLE_TX_THRESHOLD:
                event_type = f"WHALE_TX_{anchor.direction}"
                event = WhaleEvent(
                    wallet=wallet,
                    window='lifetime',
                    event_type=event_type,
                    event_time=anchor_time,
                    flow_ref=anchor_sig,
                    amount=anchor.amount_lamports,
                    count=1
                )
                events[event.key()] = event
            
            # Check cumulative conditions
            for window_key, window_seconds in [('24h', WINDOW_SECONDS['24h']), 
                                               ('7d', WINDOW_SECONDS['7d'])]:
                threshold = CUM_24H_THRESHOLD if window_key == '24h' else CUM_7D_THRESHOLD
                
                window_flows = get_flows_in_window(flows, anchor_idx, 
                                                   window_seconds, boundary_variant)
                
                for direction in ['BUY', 'SELL']:
                    dir_flows = [f for f in window_flows if f.direction == direction]
                    total = sum(f.amount_lamports for f in dir_flows)
                    
                    current_state = (total >= threshold)
                    prev = prev_state[(window_key, direction)]
                    
                    # Emit only on transition from False to True
                    if current_state and not prev:
                        # Determine flow_ref based on variant
                        if flow_ref_variant == 'Vref1':
                            # Anchor signature
                            ref_sig = anchor_sig
                        elif flow_ref_variant == 'Vref2':
                            # Triggering signature (first flow that crosses threshold)
                            ref_sig = find_triggering_signature(dir_flows, threshold)
                        else:
                            ref_sig = anchor_sig
                        
                        event_type = f"WHALE_CUM_{window_key.upper()}_{direction}"
                        event = WhaleEvent(
                            wallet=wallet,
                            window=window_key,
                            event_type=event_type,
                            event_time=anchor_time,
                            flow_ref=ref_sig,
                            amount=total,
                            count=len(dir_flows)
                        )
                        events[event.key()] = event
                    
                    # Update state
                    prev_state[(window_key, direction)] = current_state
    
    return events


def find_triggering_signature(flows: List[FlowEvent], threshold: int) -> str:
    """Find signature of flow that first makes running sum cross threshold."""
    running_sum = 0
    for flow in flows:
        running_sum += flow.amount_lamports
        if running_sum >= threshold:
            return flow.signature
    
    # Fallback (shouldn't happen if total >= threshold)
    return flows[-1].signature if flows else ''


def compare_events(baseline: Dict[Tuple, WhaleEvent], 
                  recomputed: Dict[Tuple, WhaleEvent]) -> Dict:
    """Compare baseline vs recomputed events."""
    baseline_keys = set(baseline.keys())
    recomputed_keys = set(recomputed.keys())
    
    common_keys = baseline_keys & recomputed_keys
    missing_keys = baseline_keys - recomputed_keys
    phantom_keys = recomputed_keys - baseline_keys
    
    amount_mismatches = set()
    count_mismatches = set()
    
    for key in common_keys:
        b_event = baseline[key]
        r_event = recomputed[key]
        
        if b_event.amount != r_event.amount:
            amount_mismatches.add(key)
        if b_event.count != r_event.count:
            count_mismatches.add(key)
    
    mismatched_keys = amount_mismatches | count_mismatches
    perfect_matches = common_keys - mismatched_keys
    
    return {
        'recomputed_total': len(recomputed),
        'common_keys': len(common_keys),
        'missing_keys': missing_keys,
        'phantom_keys': phantom_keys,
        'amount_mismatches': amount_mismatches,
        'count_mismatches': count_mismatches,
        'perfect_matches': len(perfect_matches),
        'total_errors': len(missing_keys) + len(phantom_keys) + len(mismatched_keys)
    }


def truncate_str(s: str, max_len: int = 12) -> str:
    """Truncate string for display."""
    if len(s) <= max_len:
        return s
    return s[:max_len] + '...'


def print_ranking_table(results: List[Tuple]):
    """Print ranking table of variant performance."""
    print("\n" + "="*80)
    print("VARIANT RANKING (by total errors ascending)")
    print("="*80)
    
    headers = ['Engine', 'Boundary', 'FlowRef', 'Missing', 'Phantom', 
               'Amt_Err', 'Cnt_Err', 'Total_Err', 'Perfect']
    
    print(f"{'Engine':10s} {'Bndry':8s} {'FlowRef':8s} "
          f"{'Missing':>8s} {'Phantom':>8s} {'Amt_Err':>8s} "
          f"{'Cnt_Err':>8s} {'Total':>8s} {'Perfect':>8s}")
    print("-" * 80)
    
    for variant_name, comparison in results:
        parts = variant_name.split('_')
        engine = parts[0]
        boundary = parts[1]
        flow_ref = parts[2] if len(parts) > 2 else 'Vref1'
        
        print(f"{engine:10s} {boundary:8s} {flow_ref:8s} "
              f"{len(comparison['missing_keys']):8d} "
              f"{len(comparison['phantom_keys']):8d} "
              f"{len(comparison['amount_mismatches']):8d} "
              f"{len(comparison['count_mismatches']):8d} "
              f"{comparison['total_errors']:8d} "
              f"{comparison['perfect_matches']:8d}")


def print_evidence_dumps(baseline: Dict[Tuple, WhaleEvent],
                        recomputed: Dict[Tuple, WhaleEvent],
                        comparison: Dict,
                        flows_by_wallet: Dict[str, List[FlowEvent]],
                        variant_name: str,
                        sample_size: int,
                        boundary_variant: str):
    """Print detailed evidence for best variant."""
    print("\n" + "="*80)
    print(f"EVIDENCE DUMPS FOR: {variant_name}")
    print("="*80)
    
    # Missing events
    print(f"\n--- MISSING EVENTS (first {sample_size}) ---")
    for i, key in enumerate(sorted(comparison['missing_keys'])[:sample_size]):
        event = baseline[key]
        wallet_trunc = truncate_str(event.wallet, 8)
        ref_trunc = truncate_str(event.flow_ref, 12)
        print(f"{i+1}. {wallet_trunc} | {event.window:8s} | {event.event_type:25s} | "
              f"time={event.event_time} | ref={ref_trunc} | "
              f"amt={event.amount:,} | cnt={event.count}")
    
    # Phantom events
    print(f"\n--- PHANTOM EVENTS (first {sample_size}) ---")
    for i, key in enumerate(sorted(comparison['phantom_keys'])[:sample_size]):
        event = recomputed[key]
        wallet_trunc = truncate_str(event.wallet, 8)
        ref_trunc = truncate_str(event.flow_ref, 12)
        print(f"{i+1}. {wallet_trunc} | {event.window:8s} | {event.event_type:25s} | "
              f"time={event.event_time} | ref={ref_trunc} | "
              f"amt={event.amount:,} | cnt={event.count}")
    
    # Amount mismatches
    print(f"\n--- AMOUNT MISMATCHES (first {sample_size}) ---")
    for i, key in enumerate(sorted(comparison['amount_mismatches'])[:sample_size]):
        b_event = baseline[key]
        r_event = recomputed[key]
        wallet_trunc = truncate_str(b_event.wallet, 8)
        ref_trunc = truncate_str(b_event.flow_ref, 12)
        diff = r_event.amount - b_event.amount
        print(f"{i+1}. {wallet_trunc} | {b_event.window:8s} | {b_event.event_type:25s} | "
              f"time={b_event.event_time} | ref={ref_trunc}")
        print(f"    Baseline:    amt={b_event.amount:,} cnt={b_event.count}")
        print(f"    Recomputed:  amt={r_event.amount:,} cnt={r_event.count}")
        print(f"    Diff:        amt={diff:+,}")
    
    # Count-only mismatches (amount matches but count differs)
    count_only = comparison['count_mismatches'] - comparison['amount_mismatches']
    if count_only:
        print(f"\n--- COUNT-ONLY MISMATCHES (first {min(sample_size, len(count_only))}) ---")
        for i, key in enumerate(sorted(count_only)[:sample_size]):
            b_event = baseline[key]
            r_event = recomputed[key]
            wallet_trunc = truncate_str(b_event.wallet, 8)
            ref_trunc = truncate_str(b_event.flow_ref, 12)
            diff = r_event.count - b_event.count
            print(f"{i+1}. {wallet_trunc} | {b_event.window:8s} | {b_event.event_type:25s} | "
                  f"time={b_event.event_time} | ref={ref_trunc}")
            print(f"    Baseline:    amt={b_event.amount:,} cnt={b_event.count}")
            print(f"    Recomputed:  amt={r_event.amount:,} cnt={r_event.count}")
            print(f"    Diff:        cnt={diff:+,}")
    
    # Detailed breakdown for first 5 amount mismatches
    print(f"\n--- DETAILED BREAKDOWN (first 5 amount mismatches) ---")
    for i, key in enumerate(sorted(comparison['amount_mismatches'])[:5]):
        b_event = baseline[key]
        r_event = recomputed[key]
        
        print(f"\n### Mismatch {i+1} ###")
        print(f"Wallet: {b_event.wallet}")
        print(f"Event: {b_event.event_type} @ {b_event.event_time}")
        print(f"Window: {b_event.window}")
        print(f"Baseline: amt={b_event.amount:,} cnt={b_event.count} flow_ref={truncate_str(b_event.flow_ref, 16)}")
        print(f"Recomputed: amt={r_event.amount:,} cnt={r_event.count} flow_ref={truncate_str(r_event.flow_ref, 16)}")
        if b_event.flow_ref != r_event.flow_ref:
            print(f"  WARNING: flow_ref mismatch!")
        
        # Get flows for this wallet
        flows = flows_by_wallet.get(b_event.wallet, [])
        if not flows:
            print("  (no flows found)")
            continue
        
        # Find anchor index by matching flow_ref (signature) and time
        anchor_idx = None
        for idx, flow in enumerate(flows):
            if flow.signature == b_event.flow_ref and flow.block_time == b_event.event_time:
                anchor_idx = idx
                break
        
        # Fallback: if not found by signature, try just by time (take first matching)
        if anchor_idx is None:
            for idx, flow in enumerate(flows):
                if flow.block_time == b_event.event_time:
                    anchor_idx = idx
                    break
        
        if anchor_idx is None:
            print("  (anchor flow not found)")
            continue
        
        # Get window flows
        if b_event.window == 'lifetime':
            window_flows = [flows[anchor_idx]]
        else:
            window_seconds = WINDOW_SECONDS[b_event.window]
            window_flows = get_flows_in_window(flows, anchor_idx, 
                                              window_seconds, boundary_variant)
        
        # Filter by direction
        direction = b_event.event_type.split('_')[-1]  # BUY or SELL
        dir_flows = [f for f in window_flows if f.direction == direction]
        
        print(f"\nFlows in window ({len(dir_flows)} {direction} flows):")
        print(f"  Boundary variant: {boundary_variant}")
        print(f"  Window: {b_event.window} ({WINDOW_SECONDS.get(b_event.window, 0)} seconds)")
        print(f"  Anchor time: {b_event.event_time}")
        if b_event.window != 'lifetime':
            start_time = b_event.event_time - WINDOW_SECONDS[b_event.window]
            print(f"  Window start: {start_time} ({'inclusive' if boundary_variant == 'W1' else 'exclusive'})")
        
        running_sum = 0
        threshold = (SINGLE_TX_THRESHOLD if 'TX' in b_event.event_type 
                    else (CUM_24H_THRESHOLD if '24H' in b_event.event_type 
                          else CUM_7D_THRESHOLD))
        
        for flow in dir_flows:
            running_sum += flow.amount_lamports
            crosses = " <-- CROSSES THRESHOLD" if running_sum >= threshold and (running_sum - flow.amount_lamports) < threshold else ""
            sig_trunc = truncate_str(flow.signature, 12)
            print(f"  {flow.block_time} | {flow.direction:4s} | "
                  f"{flow.amount_lamports:>15,} | sum={running_sum:>15,} | "
                  f"sig={sig_trunc}{crosses}")


def export_results(baseline: Dict[Tuple, WhaleEvent],
                  recomputed: Dict[Tuple, WhaleEvent],
                  comparison: Dict,
                  variant_name: str):
    """Export detailed results to files."""
    export_dir = Path('exports_phase3_2_forensicsv4')
    export_dir.mkdir(exist_ok=True)
    
    # Export mismatches TSV
    tsv_path = export_dir / 'best_variant_mismatches.tsv'
    with open(tsv_path, 'w') as f:
        f.write("type\twallet\twindow\tevent_type\tevent_time\tflow_ref\t"
                "baseline_amt\tbaseline_cnt\trecomp_amt\trecomp_cnt\tdiff_amt\tdiff_cnt\n")
        
        # Missing
        for key in sorted(comparison['missing_keys']):
            event = baseline[key]
            f.write(f"MISSING\t{event.wallet}\t{event.window}\t{event.event_type}\t"
                   f"{event.event_time}\t{event.flow_ref}\t"
                   f"{event.amount}\t{event.count}\t\t\t\t\n")
        
        # Phantom
        for key in sorted(comparison['phantom_keys']):
            event = recomputed[key]
            f.write(f"PHANTOM\t{event.wallet}\t{event.window}\t{event.event_type}\t"
                   f"{event.event_time}\t{event.flow_ref}\t"
                   f"\t\t{event.amount}\t{event.count}\t\t\n")
        
        # Amount mismatches
        for key in sorted(comparison['amount_mismatches']):
            b_event = baseline[key]
            r_event = recomputed[key]
            diff_amt = r_event.amount - b_event.amount
            diff_cnt = r_event.count - b_event.count
            f.write(f"AMT_MISMATCH\t{b_event.wallet}\t{b_event.window}\t{b_event.event_type}\t"
                   f"{b_event.event_time}\t{b_event.flow_ref}\t"
                   f"{b_event.amount}\t{b_event.count}\t"
                   f"{r_event.amount}\t{r_event.count}\t{diff_amt}\t{diff_cnt}\n")
        
        # Count-only mismatches (where amount matches but count differs)
        count_only_mismatches = comparison['count_mismatches'] - comparison['amount_mismatches']
        for key in sorted(count_only_mismatches):
            b_event = baseline[key]
            r_event = recomputed[key]
            diff_cnt = r_event.count - b_event.count
            f.write(f"CNT_MISMATCH\t{b_event.wallet}\t{b_event.window}\t{b_event.event_type}\t"
                   f"{b_event.event_time}\t{b_event.flow_ref}\t"
                   f"{b_event.amount}\t{b_event.count}\t"
                   f"{r_event.amount}\t{r_event.count}\t0\t{diff_cnt}\n")
    
    print(f"\nExported mismatches to: {tsv_path}")
    
    # Export summary
    summary_path = export_dir / 'best_variant_summary.txt'
    with open(summary_path, 'w') as f:
        f.write(f"Best Variant: {variant_name}\n")
        f.write(f"=" * 80 + "\n\n")
        f.write(f"Recomputed Total: {comparison['recomputed_total']}\n")
        f.write(f"Common Keys: {comparison['common_keys']}\n")
        f.write(f"Missing: {len(comparison['missing_keys'])}\n")
        f.write(f"Phantom: {len(comparison['phantom_keys'])}\n")
        f.write(f"Amount Mismatches: {len(comparison['amount_mismatches'])}\n")
        f.write(f"Count Mismatches: {len(comparison['count_mismatches'])}\n")
        f.write(f"Perfect Matches: {comparison['perfect_matches']}\n")
        f.write(f"Total Errors: {comparison['total_errors']}\n")
    
    print(f"Exported summary to: {summary_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Forensic analysis of whale_events vs wallet_token_flow recomputation'
    )
    parser.add_argument('--db', required=True, help='Path to SQLite database')
    parser.add_argument('--sample', type=int, default=20, 
                       help='Number of sample events to display')
    
    args = parser.parse_args()
    
    # Connect to database
    try:
        conn = sqlite3.connect(args.db)
        conn.row_factory = sqlite3.Row
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}")
        sys.exit(1)
    
    # Discover columns
    mapper = ColumnMapper(conn)
    if not mapper.discover_whale_events() or not mapper.discover_wallet_token_flow():
        sys.exit(1)
    
    # Load baseline
    baseline = load_baseline_events(conn, mapper)
    if not baseline:
        print("ERROR: No baseline events found")
        sys.exit(1)
    
    # Load flows
    flows_by_wallet = load_wallet_flows(conn, mapper)
    if not flows_by_wallet:
        print("ERROR: No wallet flows found")
        sys.exit(1)
    
    # Run all variant combinations
    print("\n" + "="*80)
    print("RUNNING ALL VARIANT COMBINATIONS")
    print("="*80)
    
    results = []
    recomputed_cache = {}  # Store recomputed dicts for evidence dumps
    
    # E1 (continuous) with both boundaries
    for boundary in ['W1', 'W2']:
        variant_name = f"E1_{boundary}_Vref1"
        print(f"\nRecomputing: {variant_name}...", end='', flush=True)
        recomputed = compute_continuous_events(flows_by_wallet, boundary)
        recomputed_cache[variant_name] = recomputed
        comparison = compare_events(baseline, recomputed)
        results.append((variant_name, comparison))
        print(f" Done. ({len(recomputed)} events, {comparison['total_errors']} errors)")
    
    # E2 (latched) with both boundaries and both flow_ref variants
    for boundary in ['W1', 'W2']:
        for flow_ref in ['Vref1', 'Vref2']:
            variant_name = f"E2_{boundary}_{flow_ref}"
            print(f"\nRecomputing: {variant_name}...", end='', flush=True)
            recomputed = compute_latched_events(flows_by_wallet, boundary, flow_ref)
            recomputed_cache[variant_name] = recomputed
            comparison = compare_events(baseline, recomputed)
            results.append((variant_name, comparison))
            print(f" Done. ({len(recomputed)} events, {comparison['total_errors']} errors)")
    
    # Sort by total errors
    results.sort(key=lambda x: x[1]['total_errors'])
    
    if not results:
        print("ERROR: No variant results generated")
        sys.exit(1)
    
    # Print ranking
    print_ranking_table(results)
    
    # Get best variant
    best_name, best_comparison = results[0]
    
    print(f"\n{'='*80}")
    print(f"BEST VARIANT: {best_name}")
    print(f"Total Errors: {best_comparison['total_errors']}")
    print(f"{'='*80}")
    
    # Get best recomputed from cache
    best_recomputed = recomputed_cache[best_name]
    
    # Extract boundary for evidence dumps
    parts = best_name.split('_')
    boundary = parts[1]
    
    # Print evidence dumps
    print_evidence_dumps(baseline, best_recomputed, best_comparison, 
                        flows_by_wallet, best_name, args.sample, boundary)
    
    # Export results
    export_results(baseline, best_recomputed, best_comparison, best_name)
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)
    
    conn.close()


if __name__ == '__main__':
    main()
