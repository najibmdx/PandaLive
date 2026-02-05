#!/usr/bin/env python3
"""
panda_phase3_2_flowref.py

Answer whether baseline whale_events.flow_ref values exist in wallet_token_flow.signature,
partitioning baseline rows into null/empty, present, and absent buckets, and reporting
deterministic coverage counts plus consistency checks for matched flow references.
"""

import argparse
import os
import sqlite3
import sys
from collections import Counter
from typing import Dict, Iterable, List, Optional, Tuple

DEFAULT_OUTDIR = "exports_phase3_2_flowref_coverage"
DEFAULT_LIMIT_MISSING = 5000
DEFAULT_SAMPLE = 50
LOOKUP_CHUNK_SIZE = 500
FETCH_BATCH_SIZE = 1000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check whale_events.flow_ref coverage against wallet_token_flow.signature"
    )
    parser.add_argument("--db", required=True, help="Path to sqlite database")
    parser.add_argument(
        "--outdir",
        default=DEFAULT_OUTDIR,
        help=f"Output directory (default: {DEFAULT_OUTDIR})",
    )
    parser.add_argument(
        "--limit-missing",
        type=int,
        default=DEFAULT_LIMIT_MISSING,
        help="Cap number of missing baseline rows to analyze",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=DEFAULT_SAMPLE,
        help="How many example rows to print and include in sample files",
    )
    parser.add_argument(
        "--only-window",
        choices=["24h", "7d", "lifetime"],
        help="Only include baseline rows with this whale_events.window",
    )
    parser.add_argument(
        "--only-event-type",
        help="Only include baseline rows with this whale_events.event_type",
    )
    parser.add_argument(
        "--only-wallet",
        help="Only include baseline rows with this whale_events.wallet",
    )
    return parser.parse_args()


def connect_readonly(db_path: str) -> sqlite3.Connection:
    try:
        return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    except sqlite3.OperationalError:
        return sqlite3.connect(db_path)


def discover_columns(cursor: sqlite3.Cursor, table_name: str) -> List[str]:
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cursor.fetchall()]


def validate_schema(cursor: sqlite3.Cursor) -> None:
    whale_required = [
        "wallet",
        "window",
        "event_time",
        "event_type",
        "sol_amount_lamports",
        "supporting_flow_count",
        "flow_ref",
    ]
    flow_required = [
        "signature",
        "scan_wallet",
        "block_time",
        "sol_direction",
        "sol_amount_lamports",
        "source_table",
    ]

    whale_cols = set(discover_columns(cursor, "whale_events"))
    flow_cols = set(discover_columns(cursor, "wallet_token_flow"))

    missing_whale = [col for col in whale_required if col not in whale_cols]
    missing_flow = [col for col in flow_required if col not in flow_cols]

    if missing_whale or missing_flow:
        if missing_whale:
            print("ERROR: whale_events missing required columns:", ", ".join(missing_whale))
        if missing_flow:
            print("ERROR: wallet_token_flow missing required columns:", ", ".join(missing_flow))
        sys.exit(1)


def build_filters(args: argparse.Namespace) -> Tuple[str, List[str]]:
    clauses = []
    params: List[str] = []
    if args.only_window:
        clauses.append("window = ?")
        params.append(args.only_window)
    if args.only_event_type:
        clauses.append("event_type = ?")
        params.append(args.only_event_type)
    if args.only_wallet:
        clauses.append("wallet = ?")
        params.append(args.only_wallet)
    if clauses:
        return "WHERE " + " AND ".join(clauses), params
    return "", params


def normalize_flow_ref(flow_ref: Optional[object]) -> Optional[str]:
    if flow_ref is None:
        return None
    if isinstance(flow_ref, bytes):
        flow_ref = flow_ref.decode("utf-8", errors="replace")
    if isinstance(flow_ref, str):
        stripped = flow_ref.strip()
        return stripped if stripped else None
    return str(flow_ref)


def chunked(iterable: Iterable[str], size: int) -> Iterable[List[str]]:
    batch: List[str] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def fetch_flow_rows(
    cursor: sqlite3.Cursor, signatures: List[str]
) -> Dict[str, Tuple[str, int, str, int, Optional[str]]]:
    results: Dict[str, Tuple[str, int, str, int, Optional[str]]] = {}
    if not signatures:
        return results
    for batch in chunked(signatures, LOOKUP_CHUNK_SIZE):
        placeholders = ",".join(["?"] * len(batch))
        query = (
            "SELECT signature, scan_wallet, block_time, sol_direction, "
            "sol_amount_lamports, source_table "
            "FROM wallet_token_flow "
            f"WHERE signature IN ({placeholders}) "
            "ORDER BY signature, block_time, scan_wallet, sol_amount_lamports"
        )
        cursor.execute(query, batch)
        for row in cursor.fetchall():
            signature, scan_wallet, block_time, sol_direction, sol_amount, source_table = row
            if signature not in results:
                results[signature] = (
                    scan_wallet,
                    block_time,
                    sol_direction,
                    sol_amount,
                    source_table,
                )
    return results


def infer_expected_direction(event_type: str) -> Optional[str]:
    if "_BUY" in event_type:
        return "buy"
    if "_SELL" in event_type:
        return "sell"
    return None


def format_bool(value: Optional[bool]) -> str:
    if value is None:
        return "na"
    return "true" if value else "false"


def write_tsv(path: str, header: List[str], rows: List[List[object]]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\t".join(header) + "\n")
        for row in rows:
            handle.write("\t".join("" if v is None else str(v) for v in row) + "\n")


def main() -> int:
    args = parse_args()

    conn = connect_readonly(args.db)
    try:
        cursor = conn.cursor()
        validate_schema(cursor)

        where_clause, params = build_filters(args)
        baseline_query = (
            "SELECT wallet, window, event_type, event_time, "
            "flow_ref, sol_amount_lamports, supporting_flow_count "
            "FROM whale_events "
            f"{where_clause} "
            "ORDER BY window, event_type, wallet, event_time, flow_ref"
        )
        cursor.execute(baseline_query, params)

        os.makedirs(args.outdir, exist_ok=True)

        total_rows = 0
        missing_rows = 0
        bucket_counts = {"A": 0, "B": 0, "C": 0}

        wallet_match_counts = Counter()
        time_match_counts = Counter()
        direction_match_counts = Counter()

        absent_wallet_counts = Counter()
        absent_event_type_counts = Counter()
        matched_source_table_counts = Counter()

        sample_absent: List[List[object]] = []
        sample_present: List[List[object]] = []
        sample_null: List[List[object]] = []

        flow_cache: Dict[str, Tuple[str, int, str, int, Optional[str]]] = {}

        while True:
            batch = cursor.fetchmany(FETCH_BATCH_SIZE)
            if not batch:
                break

            batch_flow_refs = []
            for row in batch:
                flow_ref = normalize_flow_ref(row[4])
                if flow_ref and flow_ref not in flow_cache:
                    batch_flow_refs.append(flow_ref)

            if batch_flow_refs:
                flow_cache.update(fetch_flow_rows(cursor, batch_flow_refs))

            for row in batch:
                wallet, window, event_type, event_time, flow_ref_raw, amount, count = row
                flow_ref = normalize_flow_ref(flow_ref_raw)
                total_rows += 1

                if not flow_ref:
                    bucket_counts["A"] += 1
                    missing_rows += 1
                    if len(sample_null) < args.sample:
                        sample_null.append(
                            [wallet, window, event_type, event_time, amount, count, flow_ref_raw]
                        )
                else:
                    flow_row = flow_cache.get(flow_ref)
                    if flow_row:
                        bucket_counts["B"] += 1
                        scan_wallet, block_time, sol_direction, sol_amount, source_table = flow_row

                        wallet_match = wallet == scan_wallet
                        time_match = event_time == block_time
                        expected_direction = infer_expected_direction(event_type)
                        if expected_direction is None:
                            direction_match = None
                        else:
                            direction_match = sol_direction == expected_direction

                        wallet_match_counts[format_bool(wallet_match)] += 1
                        time_match_counts[format_bool(time_match)] += 1
                        direction_match_counts[format_bool(direction_match)] += 1

                        matched_source_table_counts[
                            "NULL" if source_table is None else str(source_table)
                        ] += 1

                        if len(sample_present) < args.sample:
                            sample_present.append(
                                [
                                    wallet,
                                    window,
                                    event_type,
                                    event_time,
                                    amount,
                                    count,
                                    flow_ref,
                                    scan_wallet,
                                    block_time,
                                    sol_direction,
                                    sol_amount,
                                    source_table,
                                    format_bool(wallet_match),
                                    format_bool(time_match),
                                    format_bool(direction_match),
                                ]
                            )
                    else:
                        bucket_counts["C"] += 1
                        missing_rows += 1
                        absent_wallet_counts[wallet] += 1
                        absent_event_type_counts[event_type] += 1
                        if len(sample_absent) < args.sample:
                            sample_absent.append(
                                [wallet, window, event_type, event_time, amount, count, flow_ref]
                            )

                if missing_rows >= args.limit_missing:
                    break

            if missing_rows >= args.limit_missing:
                break

        summary_lines = []
        summary_lines.append("Flow Ref Coverage Summary")
        summary_lines.append("=" * 28)
        summary_lines.append(f"Total baseline rows analyzed: {total_rows}")
        summary_lines.append(
            f"A) flow_ref null/empty: {bucket_counts['A']}"
            f" ({bucket_counts['A'] / total_rows * 100:.2f}% )"
            if total_rows
            else "A) flow_ref null/empty: 0"
        )
        summary_lines.append(
            f"B) flow_ref present in wallet_token_flow: {bucket_counts['B']}"
            f" ({bucket_counts['B'] / total_rows * 100:.2f}% )"
            if total_rows
            else "B) flow_ref present in wallet_token_flow: 0"
        )
        summary_lines.append(
            f"C) flow_ref absent in wallet_token_flow: {bucket_counts['C']}"
            f" ({bucket_counts['C'] / total_rows * 100:.2f}% )"
            if total_rows
            else "C) flow_ref absent in wallet_token_flow: 0"
        )

        summary_lines.append("")
        summary_lines.append("Bucket B consistency checks")
        summary_lines.append("-" * 28)
        summary_lines.append(
            f"wallet_match true: {wallet_match_counts['true']}, false: {wallet_match_counts['false']}"
        )
        summary_lines.append(
            f"time_match true: {time_match_counts['true']}, false: {time_match_counts['false']}"
        )
        summary_lines.append(
            "direction_match true: {true}, false: {false}, na: {na}".format(
                true=direction_match_counts["true"],
                false=direction_match_counts["false"],
                na=direction_match_counts["na"],
            )
        )

        summary_lines.append("")
        summary_lines.append("Top 20 wallets by flow_ref_absent count")
        summary_lines.append("-" * 36)
        for wallet, count in absent_wallet_counts.most_common(20):
            summary_lines.append(f"{wallet}\t{count}")

        summary_lines.append("")
        summary_lines.append("Top 20 event_types by flow_ref_absent count")
        summary_lines.append("-" * 42)
        for event_type, count in absent_event_type_counts.most_common(20):
            summary_lines.append(f"{event_type}\t{count}")

        summary_lines.append("")
        summary_lines.append("Top 20 source_table among matched flow_refs")
        summary_lines.append("-" * 44)
        for source_table, count in matched_source_table_counts.most_common(20):
            summary_lines.append(f"{source_table}\t{count}")

        summary_text = "\n".join(summary_lines)
        summary_path = os.path.join(args.outdir, "coverage_summary.txt")
        with open(summary_path, "w", encoding="utf-8") as handle:
            handle.write(summary_text)

        write_tsv(
            os.path.join(args.outdir, "flowref_absent_samples.tsv"),
            [
                "wallet",
                "window",
                "event_type",
                "event_time",
                "baseline_amount",
                "baseline_count",
                "flow_ref",
            ],
            sample_absent,
        )
        write_tsv(
            os.path.join(args.outdir, "flowref_present_samples.tsv"),
            [
                "wallet",
                "window",
                "event_type",
                "event_time",
                "baseline_amount",
                "baseline_count",
                "flow_ref",
                "flow_scan_wallet",
                "flow_block_time",
                "flow_sol_direction",
                "flow_sol_amount_lamports",
                "flow_source_table",
                "wallet_match",
                "time_match",
                "direction_match",
            ],
            sample_present,
        )
        write_tsv(
            os.path.join(args.outdir, "flowref_null_samples.tsv"),
            [
                "wallet",
                "window",
                "event_type",
                "event_time",
                "baseline_amount",
                "baseline_count",
                "flow_ref",
            ],
            sample_null,
        )

        print(summary_text)

        def print_samples(title: str, rows: List[List[object]]) -> None:
            print("")
            print(title)
            print("-" * len(title))
            for row in rows[:10]:
                print("\t".join("" if v is None else str(v) for v in row))

        print_samples("Sample flow_ref null/empty (A)", sample_null)
        print_samples("Sample flow_ref present (B)", sample_present)
        print_samples("Sample flow_ref absent (C)", sample_absent)

        return 0
    except sqlite3.Error as exc:
        print(f"ERROR: {exc}")
        return 2
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
