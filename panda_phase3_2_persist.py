#!/usr/bin/env python3
"""
panda_phase3_2_recompute_persist.py

Recompute whale_events using E_PERSIST semantics:
- Anchor on every flow (wallet+direction) sorted by (block_time ASC, signature ASC)
- Inclusive windows (start/end)
- Evaluate after each flow at same block_time
- flow_ref is anchor signature
- Payload is exact rolling sum/count at anchor
- Emit at EVERY anchor meeting threshold (persistent snapshot)
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Iterable, List, Optional, Tuple

WINDOW_SECONDS = {
    "24h": 86400,
    "7d": 604800,
    "lifetime": None,
}


@dataclass
class FlowEvent:
    wallet: str
    block_time: int
    direction: str
    amount_lamports: int
    signature: str


@dataclass
class WhaleEvent:
    wallet: str
    window: str
    event_type: str
    event_time: int
    flow_ref: str
    amount: int
    count: int

    def key(self) -> Tuple[str, str, str, int, str]:
        return (self.wallet, self.window, self.event_type, self.event_time, self.flow_ref)


@dataclass
class WindowState:
    window_seconds: int
    items: Deque[Tuple[int, int]]
    running_sum: int
    running_count: int

    def expire(self, anchor_time: int) -> None:
        start_time = anchor_time - self.window_seconds
        while self.items and self.items[0][0] < start_time:
            time_val, amount = self.items.popleft()
            self.running_sum -= amount
            self.running_count -= 1

    def add(self, block_time: int, amount: int) -> None:
        self.items.append((block_time, amount))
        self.running_sum += amount
        self.running_count += 1


def fetch_wallets(conn: sqlite3.Connection, limit_wallets: Optional[int]) -> Optional[List[str]]:
    if limit_wallets is None:
        return None
    query = """
        SELECT DISTINCT scan_wallet
        FROM wallet_token_flow
        ORDER BY scan_wallet ASC
        LIMIT ?
    """
    wallets = [row[0] for row in conn.execute(query, (limit_wallets,))]
    return wallets


def load_thresholds(conn: sqlite3.Connection) -> Tuple[int, int, int]:
    query_tx = """
        SELECT MIN(sol_amount_lamports)
        FROM whale_events
        WHERE event_type LIKE 'WHALE_TX_%'
    """
    query_24h = """
        SELECT MIN(sol_amount_lamports)
        FROM whale_events
        WHERE event_type IN ('WHALE_CUM_24H_BUY', 'WHALE_CUM_24H_SELL')
    """
    query_7d = """
        SELECT MIN(sol_amount_lamports)
        FROM whale_events
        WHERE event_type IN ('WHALE_CUM_7D_BUY', 'WHALE_CUM_7D_SELL')
    """
    tx_threshold = conn.execute(query_tx).fetchone()[0]
    cum_24h_threshold = conn.execute(query_24h).fetchone()[0]
    cum_7d_threshold = conn.execute(query_7d).fetchone()[0]

    if tx_threshold is None or cum_24h_threshold is None or cum_7d_threshold is None:
        raise RuntimeError("Failed to derive thresholds from whale_events.")

    return int(tx_threshold), int(cum_24h_threshold), int(cum_7d_threshold)


def load_baseline_events(
    conn: sqlite3.Connection, wallets: Optional[Iterable[str]]
) -> Dict[Tuple[str, str, str, int, str], WhaleEvent]:
    print("\n" + "=" * 80)
    print("LOADING BASELINE WHALE_EVENTS")
    print("=" * 80)

    params: List[str] = []
    wallet_filter = ""
    if wallets is not None:
        wallet_list = list(wallets)
        if wallet_list:
            placeholders = ", ".join("?" for _ in wallet_list)
            wallet_filter = f"WHERE wallet IN ({placeholders})"
            params.extend(wallet_list)
        else:
            return {}

    query = f"""
        SELECT wallet, window, event_type, event_time, flow_ref,
               sol_amount_lamports, supporting_flow_count
        FROM whale_events
        {wallet_filter}
        ORDER BY wallet, event_time, window, event_type
    """

    events: Dict[Tuple[str, str, str, int, str], WhaleEvent] = {}
    for row in conn.execute(query, params):
        event = WhaleEvent(
            wallet=row[0],
            window=row[1],
            event_type=row[2],
            event_time=row[3],
            flow_ref=row[4],
            amount=row[5],
            count=row[6],
        )
        events[event.key()] = event

    print(f"Total baseline events: {len(events):,}")

    counts = defaultdict(int)
    for event in events.values():
        counts[(event.window, event.event_type)] += 1

    print("\nCounts by (window, event_type):")
    for (window, event_type), count in sorted(counts.items()):
        print(f"  {window:10s} {event_type:25s} : {count:6d}")

    return events


def iter_flows(
    conn: sqlite3.Connection, wallets: Optional[Iterable[str]]
) -> Iterable[FlowEvent]:
    params: List[str] = []
    wallet_filter = ""
    if wallets is not None:
        wallet_list = list(wallets)
        if wallet_list:
            placeholders = ", ".join("?" for _ in wallet_list)
            wallet_filter = f"AND scan_wallet IN ({placeholders})"
            params.extend(wallet_list)
        else:
            return []

    query = f"""
        SELECT scan_wallet, block_time, sol_direction, sol_amount_lamports, signature
        FROM wallet_token_flow
        WHERE sol_direction IN ('buy', 'sell')
          AND sol_amount_lamports > 0
          {wallet_filter}
        ORDER BY scan_wallet ASC, sol_direction ASC, block_time ASC, signature ASC
    """
    cursor = conn.execute(query, params)
    for row in cursor:
        yield FlowEvent(
            wallet=row[0],
            block_time=row[1],
            direction=row[2],
            amount_lamports=row[3],
            signature=row[4],
        )


def recompute_events(
    conn: sqlite3.Connection,
    wallets: Optional[Iterable[str]],
    limit_anchors: Optional[int],
    thresholds: Tuple[int, int, int],
) -> Dict[Tuple[str, str, str, int, str], WhaleEvent]:
    tx_threshold, cum_24h_threshold, cum_7d_threshold = thresholds
    events: Dict[Tuple[str, str, str, int, str], WhaleEvent] = {}

    current_wallet: Optional[str] = None
    current_direction: Optional[str] = None
    anchor_count = 0

    window_24h = WindowState(
        window_seconds=WINDOW_SECONDS["24h"],
        items=deque(),
        running_sum=0,
        running_count=0,
    )
    window_7d = WindowState(
        window_seconds=WINDOW_SECONDS["7d"],
        items=deque(),
        running_sum=0,
        running_count=0,
    )

    def reset_state() -> None:
        nonlocal anchor_count
        anchor_count = 0
        window_24h.items.clear()
        window_24h.running_sum = 0
        window_24h.running_count = 0
        window_7d.items.clear()
        window_7d.running_sum = 0
        window_7d.running_count = 0

    total_flows = 0
    for flow in iter_flows(conn, wallets):
        total_flows += 1
        if flow.wallet != current_wallet or flow.direction != current_direction:
            current_wallet = flow.wallet
            current_direction = flow.direction
            reset_state()

        if limit_anchors is not None and anchor_count >= limit_anchors:
            continue

        anchor_count += 1
        anchor_time = flow.block_time

        window_24h.expire(anchor_time)
        window_7d.expire(anchor_time)

        window_24h.add(flow.block_time, flow.amount_lamports)
        window_7d.add(flow.block_time, flow.amount_lamports)

        direction_upper = flow.direction.upper()

        if flow.amount_lamports >= tx_threshold:
            event_type = f"WHALE_TX_{direction_upper}"
            event = WhaleEvent(
                wallet=flow.wallet,
                window="lifetime",
                event_type=event_type,
                event_time=anchor_time,
                flow_ref=flow.signature,
                amount=flow.amount_lamports,
                count=1,
            )
            events[event.key()] = event

        if window_24h.running_sum >= cum_24h_threshold:
            event_type = f"WHALE_CUM_24H_{direction_upper}"
            event = WhaleEvent(
                wallet=flow.wallet,
                window="24h",
                event_type=event_type,
                event_time=anchor_time,
                flow_ref=flow.signature,
                amount=window_24h.running_sum,
                count=window_24h.running_count,
            )
            events[event.key()] = event

        if window_7d.running_sum >= cum_7d_threshold:
            event_type = f"WHALE_CUM_7D_{direction_upper}"
            event = WhaleEvent(
                wallet=flow.wallet,
                window="7d",
                event_type=event_type,
                event_time=anchor_time,
                flow_ref=flow.signature,
                amount=window_7d.running_sum,
                count=window_7d.running_count,
            )
            events[event.key()] = event

    print(f"Total flows processed: {total_flows:,}")
    print(f"Total recomputed events: {len(events):,}")
    return events


def compare_events(
    baseline: Dict[Tuple[str, str, str, int, str], WhaleEvent],
    recomputed: Dict[Tuple[str, str, str, int, str], WhaleEvent],
) -> Dict[str, object]:
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
        "recomputed_total": len(recomputed),
        "common_keys": len(common_keys),
        "missing_keys": missing_keys,
        "phantom_keys": phantom_keys,
        "amount_mismatches": amount_mismatches,
        "count_mismatches": count_mismatches,
        "perfect_matches": len(perfect_matches),
        "total_errors": len(missing_keys) + len(phantom_keys) + len(mismatched_keys),
    }


def counts_by_window_event(events: Dict[Tuple[str, str, str, int, str], WhaleEvent]) -> Dict[Tuple[str, str], int]:
    counts: Dict[Tuple[str, str], int] = defaultdict(int)
    for event in events.values():
        counts[(event.window, event.event_type)] += 1
    return counts


def write_tsv(path: str, rows: List[List[object]]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write("\t".join("" if value is None else str(value) for value in row) + "\n")


def write_outputs(
    outdir: str,
    baseline: Dict[Tuple[str, str, str, int, str], WhaleEvent],
    recomputed: Dict[Tuple[str, str, str, int, str], WhaleEvent],
    comparison: Dict[str, object],
) -> None:
    os.makedirs(outdir, exist_ok=True)

    missing_rows: List[List[object]] = []
    phantom_rows: List[List[object]] = []
    mismatch_rows: List[List[object]] = []

    for key in sorted(comparison["missing_keys"]):
        b_event = baseline[key]
        missing_rows.append(
            [
                b_event.wallet,
                b_event.window,
                b_event.event_type,
                b_event.event_time,
                b_event.flow_ref,
                b_event.amount,
                b_event.count,
                None,
                None,
            ]
        )

    for key in sorted(comparison["phantom_keys"]):
        r_event = recomputed[key]
        phantom_rows.append(
            [
                r_event.wallet,
                r_event.window,
                r_event.event_type,
                r_event.event_time,
                r_event.flow_ref,
                None,
                None,
                r_event.amount,
                r_event.count,
            ]
        )

    mismatch_keys = comparison["amount_mismatches"] | comparison["count_mismatches"]
    for key in sorted(mismatch_keys):
        b_event = baseline[key]
        r_event = recomputed[key]
        mismatch_rows.append(
            [
                b_event.wallet,
                b_event.window,
                b_event.event_type,
                b_event.event_time,
                b_event.flow_ref,
                b_event.amount,
                b_event.count,
                r_event.amount,
                r_event.count,
            ]
        )

    headers = [
        "wallet",
        "window",
        "event_type",
        "event_time",
        "flow_ref",
        "baseline_amount",
        "baseline_count",
        "recomputed_amount",
        "recomputed_count",
    ]

    write_tsv(os.path.join(outdir, "missing.tsv"), [headers] + missing_rows[:50])
    write_tsv(os.path.join(outdir, "phantom.tsv"), [headers] + phantom_rows[:50])
    write_tsv(os.path.join(outdir, "mismatches.tsv"), [headers] + mismatch_rows[:50])

    baseline_counts = counts_by_window_event(baseline)
    recomputed_counts = counts_by_window_event(recomputed)

    summary_path = os.path.join(outdir, "summary.txt")
    with open(summary_path, "w", encoding="utf-8") as handle:
        handle.write("STRICT RECOMPUTE COMPARISON\n")
        handle.write("=" * 80 + "\n")
        handle.write(f"Baseline events:   {len(baseline):,}\n")
        handle.write(f"Recomputed events: {comparison['recomputed_total']:,}\n")
        handle.write(f"Common keys:       {comparison['common_keys']:,}\n")
        handle.write(f"Perfect matches:   {comparison['perfect_matches']:,}\n")
        handle.write("\nERRORS:\n")
        handle.write(f"  Missing:         {len(comparison['missing_keys']):,}\n")
        handle.write(f"  Phantom:         {len(comparison['phantom_keys']):,}\n")
        handle.write(f"  Amount mismatch: {len(comparison['amount_mismatches']):,}\n")
        handle.write(f"  Count mismatch:  {len(comparison['count_mismatches']):,}\n")
        handle.write(f"  TOTAL ERRORS:    {comparison['total_errors']:,}\n")

        handle.write("\nBASELINE COUNTS BY (WINDOW, EVENT_TYPE)\n")
        handle.write("=" * 80 + "\n")
        for (window, event_type), count in sorted(baseline_counts.items()):
            handle.write(f"  {window:10s} {event_type:25s} : {count:6d}\n")

        handle.write("\nRECOMPUTED COUNTS BY (WINDOW, EVENT_TYPE)\n")
        handle.write("=" * 80 + "\n")
        for (window, event_type), count in sorted(recomputed_counts.items()):
            handle.write(f"  {window:10s} {event_type:25s} : {count:6d}\n")


def print_summary(
    baseline: Dict[Tuple[str, str, str, int, str], WhaleEvent],
    recomputed: Dict[Tuple[str, str, str, int, str], WhaleEvent],
    comparison: Dict[str, object],
) -> None:
    print("\n" + "=" * 80)
    print("STRICT RECOMPUTE COMPARISON")
    print("=" * 80)
    print(f"\nBaseline events:   {len(baseline):,}")
    print(f"Recomputed events: {comparison['recomputed_total']:,}")
    print(f"Common keys:       {comparison['common_keys']:,}")
    print(f"Perfect matches:   {comparison['perfect_matches']:,}")

    if len(baseline) > 0:
        success_rate = (comparison["perfect_matches"] / len(baseline)) * 100
        print(f"SUCCESS RATE:      {success_rate:.2f}%")

    print("\nERRORS:")
    print(f"  Missing:         {len(comparison['missing_keys']):,}")
    print(f"  Phantom:         {len(comparison['phantom_keys']):,}")
    print(f"  Amount mismatch: {len(comparison['amount_mismatches']):,}")
    print(f"  Count mismatch:  {len(comparison['count_mismatches']):,}")
    print(f"  TOTAL ERRORS:    {comparison['total_errors']:,}")

    print("\n" + "=" * 80)
    print("BASELINE COUNTS BY (WINDOW, EVENT_TYPE)")
    print("=" * 80)
    baseline_counts = counts_by_window_event(baseline)
    for (window, event_type), count in sorted(baseline_counts.items()):
        print(f"  {window:10s} {event_type:25s} : {count:6d}")

    print("\n" + "=" * 80)
    print("RECOMPUTED COUNTS BY (WINDOW, EVENT_TYPE)")
    print("=" * 80)
    recomputed_counts = counts_by_window_event(recomputed)
    for (window, event_type), count in sorted(recomputed_counts.items()):
        print(f"  {window:10s} {event_type:25s} : {count:6d}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="E_PERSIST whale event recomputation matching DB semantics"
    )
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument(
        "--limit-wallets",
        type=int,
        default=None,
        help="Only process first N wallets (sorted)",
    )
    parser.add_argument(
        "--limit-anchors",
        type=int,
        default=None,
        help="Only process first N flows per wallet-direction stream",
    )
    parser.add_argument(
        "--outdir",
        default="exports_phase3_2_recompute_persist",
        help="Output directory for comparison artifacts",
    )

    args = parser.parse_args()

    try:
        conn = sqlite3.connect(args.db)
    except Exception as exc:
        print(f"ERROR: Failed to connect to database: {exc}")
        sys.exit(1)

    wallets = fetch_wallets(conn, args.limit_wallets)

    try:
        thresholds = load_thresholds(conn)
    except Exception as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    print("\n" + "=" * 80)
    print("THRESHOLDS")
    print("=" * 80)
    print(f"MIN_TX_LAMPORTS:  {thresholds[0]:,}")
    print(f"THRESH_24H:       {thresholds[1]:,}")
    print(f"THRESH_7D:        {thresholds[2]:,}")

    baseline = load_baseline_events(conn, wallets)
    if not baseline:
        print("ERROR: No baseline events found")
        sys.exit(1)

    print("\n" + "=" * 80)
    print("RECOMPUTING WITH E_PERSIST SEMANTICS")
    print("=" * 80)
    print("Rules:")
    print("  - Sort flows by (wallet, direction, block_time ASC, signature ASC)")
    print("  - Inclusive windows (start/end)")
    print("  - Evaluate after each flow at same time")
    print("  - Emit at EVERY anchor meeting threshold (persistent)")
    print()

    recomputed = recompute_events(conn, wallets, args.limit_anchors, thresholds)
    comparison = compare_events(baseline, recomputed)
    print_summary(baseline, recomputed, comparison)
    write_outputs(args.outdir, baseline, recomputed, comparison)

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)

    conn.close()


if __name__ == "__main__":
    main()
