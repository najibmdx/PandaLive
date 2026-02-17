#!/usr/bin/env python3
"""
panda_streaming_whale_detector.py

FAST O(N) whale event detection using streaming algorithm.
Processes flows in a SINGLE PASS instead of nested loops.

This is how trading platforms do it.
"""

import sqlite3
from collections import deque, defaultdict
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import time


# Thresholds (same as original)
SINGLE_TX_THRESHOLD = 10_000_000_000   # 10 SOL
CUM_24H_THRESHOLD = 50_000_000_000     # 50 SOL
CUM_7D_THRESHOLD = 200_000_000_000     # 200 SOL

WINDOW_24H_SECONDS = 86400
WINDOW_7D_SECONDS = 604800


@dataclass
class Flow:
    """Single flow event."""
    wallet: str
    block_time: int
    direction: str  # 'BUY' or 'SELL'
    amount: int
    signature: str


@dataclass
class WhaleEvent:
    """Emitted whale event."""
    wallet: str
    window: str
    event_type: str
    event_time: int
    flow_ref: str
    amount: int
    count: int
    
    def key(self):
        return (self.wallet, self.window, self.event_type, self.event_time, self.flow_ref)


class SlidingWindow:
    """
    Efficient sliding window that maintains sum and count.
    O(1) amortized add/expire operations.
    """
    
    def __init__(self, window_seconds: int):
        self.window_seconds = window_seconds
        self.flows = deque()  # [(time, amount, signature)]
        self.total = 0
        self.count = 0
    
    def expire_old(self, current_time: int):
        """Remove flows older than window. O(k) where k = expired flows."""
        cutoff = current_time - self.window_seconds
        
        while self.flows and self.flows[0][0] < cutoff:
            old_time, old_amount, old_sig = self.flows.popleft()
            self.total -= old_amount
            self.count -= 1
    
    def add(self, flow: Flow):
        """Add flow to window. O(1)."""
        self.flows.append((flow.block_time, flow.amount, flow.signature))
        self.total += flow.amount
        self.count += 1
    
    def crosses_threshold(self, threshold: int) -> bool:
        """Check if current window sum crosses threshold. O(1)."""
        return self.total >= threshold
    
    def get_crossing_flow(self, threshold: int) -> Tuple[str, int, int]:
        """
        Find the flow that caused threshold crossing.
        Returns: (signature, amount_at_crossing, count_at_crossing)
        """
        # Walk forward through window to find first crossing point
        running_sum = 0
        running_count = 0
        
        for time, amount, sig in self.flows:
            running_sum += amount
            running_count += 1
            
            if running_sum >= threshold:
                return (sig, running_sum, running_count)
        
        # Shouldn't reach here if crosses_threshold() was true
        return (self.flows[-1][2], self.total, self.count)


class WalletDirectionState:
    """
    State for one (wallet, direction) combination.
    Tracks sliding windows and emission status.
    """
    
    def __init__(self):
        self.window_24h = SlidingWindow(WINDOW_24H_SECONDS)
        self.window_7d = SlidingWindow(WINDOW_7D_SECONDS)
        
        # Track if we've emitted for current window state
        # Reset to False when window drops below threshold
        self.emitted_24h_at_time = None
        self.emitted_7d_at_time = None
    
    def update_and_check(self, flow: Flow) -> List[WhaleEvent]:
        """
        Update state with new flow and emit events if thresholds crossed.
        Returns list of events to emit (0-2 events).
        """
        events = []
        
        # Update windows
        self.window_24h.expire_old(flow.block_time)
        self.window_7d.expire_old(flow.block_time)
        
        self.window_24h.add(flow)
        self.window_7d.add(flow)
        
        # Check 24h threshold
        if self.window_24h.crosses_threshold(CUM_24H_THRESHOLD):
            # Only emit if we haven't emitted for this anchor time yet
            if self.emitted_24h_at_time != flow.block_time:
                sig, amount, count = self.window_24h.get_crossing_flow(CUM_24H_THRESHOLD)
                
                event_type = f"WHALE_CUM_24H_{flow.direction}"
                events.append(WhaleEvent(
                    wallet=flow.wallet,
                    window='24h',
                    event_type=event_type,
                    event_time=flow.block_time,
                    flow_ref=sig,
                    amount=amount,
                    count=count
                ))
                self.emitted_24h_at_time = flow.block_time
        else:
            # Dropped below threshold - reset emission tracker
            self.emitted_24h_at_time = None
        
        # Check 7d threshold (same logic)
        if self.window_7d.crosses_threshold(CUM_7D_THRESHOLD):
            if self.emitted_7d_at_time != flow.block_time:
                sig, amount, count = self.window_7d.get_crossing_flow(CUM_7D_THRESHOLD)
                
                event_type = f"WHALE_CUM_7D_{flow.direction}"
                events.append(WhaleEvent(
                    wallet=flow.wallet,
                    window='7d',
                    event_type=event_type,
                    event_time=flow.block_time,
                    flow_ref=sig,
                    amount=amount,
                    count=count
                ))
                self.emitted_7d_at_time = flow.block_time
        else:
            self.emitted_7d_at_time = None
        
        return events


class StreamingWhaleDetector:
    """
    O(N) whale event detector using streaming algorithm.
    
    Processes flows in chronological order, maintaining sliding windows
    per (wallet, direction) and emitting events when thresholds cross.
    """
    
    def __init__(self):
        # State: (wallet, direction) -> WalletDirectionState
        self.state: Dict[Tuple[str, str], WalletDirectionState] = defaultdict(WalletDirectionState)
        
        # Emitted events
        self.events: Dict[Tuple, WhaleEvent] = {}
        
        # Statistics
        self.flows_processed = 0
        self.events_emitted = 0
    
    def process_flow(self, flow: Flow) -> List[WhaleEvent]:
        """
        Process a single flow. O(1) amortized.
        Returns events emitted for this flow.
        """
        self.flows_processed += 1
        
        events = []
        
        # Single-TX whale events (instant check, no state needed)
        if flow.amount >= SINGLE_TX_THRESHOLD:
            event_type = f"WHALE_TX_{flow.direction}"
            event = WhaleEvent(
                wallet=flow.wallet,
                window='lifetime',
                event_type=event_type,
                event_time=flow.block_time,
                flow_ref=flow.signature,
                amount=flow.amount,
                count=1
            )
            
            # Only emit if we haven't emitted this exact event
            if event.key() not in self.events:
                events.append(event)
                self.events[event.key()] = event
                self.events_emitted += 1
        
        # Cumulative events (use sliding windows)
        state_key = (flow.wallet, flow.direction)
        state = self.state[state_key]
        
        cumulative_events = state.update_and_check(flow)
        
        for event in cumulative_events:
            if event.key() not in self.events:
                events.append(event)
                self.events[event.key()] = event
                self.events_emitted += 1
        
        return events
    
    def get_all_events(self) -> List[WhaleEvent]:
        """Return all emitted events."""
        return list(self.events.values())


def load_flows_streaming(conn: sqlite3.Connection) -> List[Flow]:
    """
    Load flows in chronological order.
    CRITICAL: Sorted by time FIRST for streaming to work.
    """
    print("Loading flows from database...")
    
    query = """
        SELECT scan_wallet, block_time, sol_direction, sol_amount_lamports, signature
        FROM wallet_token_flow
        WHERE block_time IS NOT NULL
          AND sol_amount_lamports IS NOT NULL
          AND signature IS NOT NULL
        ORDER BY block_time ASC, signature ASC
    """
    
    cursor = conn.execute(query)
    flows = []
    
    for row in cursor:
        wallet, block_time, direction, amount, signature = row
        
        # Normalize direction
        direction = str(direction).upper() if direction else ''
        if direction not in ('BUY', 'SELL'):
            continue
        
        flow = Flow(
            wallet=wallet,
            block_time=block_time,
            direction=direction,
            amount=abs(amount),
            signature=signature
        )
        flows.append(flow)
    
    print(f"Loaded {len(flows):,} flows")
    return flows


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Streaming O(N) whale detector")
    parser.add_argument('--db', required=True, help="Database path")
    parser.add_argument('--output', help="Output file for events (optional)")
    args = parser.parse_args()
    
    print("="*80)
    print("STREAMING WHALE EVENT DETECTOR (O(N) Algorithm)")
    print("="*80)
    print(f"Database: {args.db}")
    print()
    
    # Connect to database
    conn = sqlite3.connect(args.db)
    
    # Load flows (sorted by time)
    start_time = time.time()
    flows = load_flows_streaming(conn)
    load_time = time.time() - start_time
    print(f"Load time: {load_time:.2f}s")
    print()
    
    # Stream through flows
    print("Processing flows (single pass)...")
    detector = StreamingWhaleDetector()
    
    start_time = time.time()
    progress_interval = max(len(flows) // 20, 1)
    
    for i, flow in enumerate(flows):
        detector.process_flow(flow)
        
        if (i + 1) % progress_interval == 0:
            pct = ((i + 1) / len(flows)) * 100
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            print(f"  {i+1:,}/{len(flows):,} ({pct:.1f}%) - {rate:,.0f} flows/sec", flush=True)
    
    process_time = time.time() - start_time
    
    print()
    print("="*80)
    print("RESULTS")
    print("="*80)
    print(f"Total flows processed: {detector.flows_processed:,}")
    print(f"Total events emitted: {detector.events_emitted:,}")
    print(f"Processing time: {process_time:.2f}s")
    print(f"Throughput: {detector.flows_processed / process_time:,.0f} flows/sec")
    print()
    
    # Event breakdown
    events_by_type = defaultdict(int)
    for event in detector.get_all_events():
        events_by_type[event.event_type] += 1
    
    print("Events by type:")
    for event_type, count in sorted(events_by_type.items()):
        print(f"  {event_type:25s}: {count:,}")
    
    # Compare with database
    print()
    print("Comparing with existing whale_events table...")
    try:
        cursor = conn.execute("SELECT COUNT(*) FROM whale_events")
        db_count = cursor.fetchone()[0]
        print(f"  Database has: {db_count:,} events")
        print(f"  Streaming detected: {detector.events_emitted:,} events")
        
        if db_count == detector.events_emitted:
            print("  ✓ MATCH!")
        else:
            diff = detector.events_emitted - db_count
            print(f"  △ Difference: {diff:+,} events")
    except:
        print("  (whale_events table not found)")
    
    # Save to file if requested
    if args.output:
        print(f"\nSaving events to {args.output}...")
        with open(args.output, 'w') as f:
            f.write("wallet,window,event_type,event_time,flow_ref,amount,count\n")
            for event in sorted(detector.get_all_events(), key=lambda e: (e.wallet, e.event_time)):
                f.write(f"{event.wallet},{event.window},{event.event_type},")
                f.write(f"{event.event_time},{event.flow_ref},{event.amount},{event.count}\n")
        print(f"  Saved {detector.events_emitted:,} events")
    
    conn.close()
    
    print()
    print("="*80)
    print(f"PERFORMANCE: {process_time:.2f}s for {detector.flows_processed:,} flows")
    print(f"This is O(N) - linear scaling with data size")
    print("="*80)


if __name__ == '__main__':
    main()
