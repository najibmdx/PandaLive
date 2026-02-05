#!/usr/bin/env python3
"""
PANDA v4 - Phase 3.2 Acceptance

Read-only acceptance checks for whale_events and whale_states.
"""

import argparse
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


EVENT_TYPES = [
    "WHALE_TX_BUY",
    "WHALE_TX_SELL",
    "WHALE_CUM_24H_BUY",
    "WHALE_CUM_24H_SELL",
    "WHALE_CUM_7D_BUY",
    "WHALE_CUM_7D_SELL",
]

WINDOWS = ["24h", "7d", "lifetime"]


@dataclass
class BlockResult:
    name: str
    passed: bool
    details: List[str]


def connect_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_table_info(conn: sqlite3.Connection, table: str) -> List[sqlite3.Row]:
    return conn.execute(f"PRAGMA table_info({table});").fetchall()


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?;",
        (table,),
    ).fetchone()
    return row is not None


def normalize_type(type_name: Optional[str]) -> str:
    return (type_name or "").strip().upper()


def print_block(result: BlockResult) -> None:
    status = "PASS" if result.passed else "FAIL"
    print(f"{result.name}: {status}")
    for line in result.details:
        print(f"  - {line}")


def fetch_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    return [row["name"] for row in fetch_table_info(conn, table)]


def first_present(columns: Iterable[str], candidates: Sequence[str]) -> Optional[str]:
    column_set = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in column_set:
            return column_set[cand.lower()]
    return None


def build_expected_event_rows(
    conn: sqlite3.Connection,
    columns: List[str],
) -> Tuple[List[sqlite3.Row], List[str], Dict[str, Optional[str]]]:
    issues: List[str] = []
    required_cols = {
        "wallet": first_present(columns, ["wallet", "scan_wallet"]),
        "window": first_present(columns, ["window"]),
        "flow_time": first_present(columns, ["flow_time", "event_time", "block_time", "tx_time", "time"]),
        "flow_ref": first_present(columns, ["flow_ref", "signature", "tx_signature", "flow_id"]),
        "direction": first_present(columns, ["sol_direction", "direction"]),
        "amount": first_present(columns, ["sol_amount_lamports", "sol_amount", "amount_lamports"]),
        "supporting_flow_count": first_present(
            columns, ["supporting_flow_count", "supporting_count", "flow_support_count"]
        ),
    }

    missing = [key for key, value in required_cols.items() if value is None]
    if missing:
        issues.append("wallet_token_flow missing columns: " + ", ".join(sorted(missing)))
        return [], issues, {}

    event_type_col = first_present(columns, ["event_type", "whale_event_type"])
    if event_type_col:
        sql = f"""
            SELECT {required_cols['wallet']} AS wallet,
                   {required_cols['window']} AS window,
                   {required_cols['flow_time']} AS event_time,
                   {event_type_col} AS event_type,
                   {required_cols['amount']} AS sol_amount_lamports,
                   {required_cols['supporting_flow_count']} AS supporting_flow_count,
                   {required_cols['flow_ref']} AS flow_ref
            FROM wallet_token_flow
            WHERE {event_type_col} IN ({','.join('?' for _ in EVENT_TYPES)})
        """
        return conn.execute(sql, EVENT_TYPES).fetchall(), issues, {}

    event_columns = {
        "WHALE_TX_BUY": first_present(columns, ["whale_tx_buy"]),
        "WHALE_TX_SELL": first_present(columns, ["whale_tx_sell"]),
        "WHALE_CUM_24H_BUY": first_present(columns, ["whale_cum_24h_buy", "whale_cum_24h_buy_lamports"]),
        "WHALE_CUM_24H_SELL": first_present(columns, ["whale_cum_24h_sell", "whale_cum_24h_sell_lamports"]),
        "WHALE_CUM_7D_BUY": first_present(columns, ["whale_cum_7d_buy", "whale_cum_7d_buy_lamports"]),
        "WHALE_CUM_7D_SELL": first_present(columns, ["whale_cum_7d_sell", "whale_cum_7d_sell_lamports"]),
    }
    if not any(event_columns.values()):
        issues.append("wallet_token_flow lacks event_type or whale event columns")
        return [], issues, event_columns

    rows: List[sqlite3.Row] = []
    for event_type, col in event_columns.items():
        if not col:
            issues.append(f"wallet_token_flow missing event column for {event_type}")
            continue
        sql = f"""
            SELECT {required_cols['wallet']} AS wallet,
                   {required_cols['window']} AS window,
                   {required_cols['flow_time']} AS event_time,
                   ? AS event_type,
                   {col} AS sol_amount_lamports,
                   {required_cols['supporting_flow_count']} AS supporting_flow_count,
                   {required_cols['flow_ref']} AS flow_ref
            FROM wallet_token_flow
            WHERE {col} IS NOT NULL AND {col} > 0
        """
        rows.extend(conn.execute(sql, (event_type,)).fetchall())
    return rows, issues, event_columns


def format_key(row: sqlite3.Row) -> str:
    return "|".join(
        [
            str(row["wallet"]),
            str(row["window"]),
            str(row["event_type"]),
            str(row["event_time"]),
            str(row["flow_ref"]),
        ]
    )


def compare_event_sets(
    actual: Sequence[sqlite3.Row],
    expected: Sequence[sqlite3.Row],
) -> Tuple[List[str], List[str]]:
    actual_keys = {format_key(row) for row in actual}
    expected_keys = {format_key(row) for row in expected}
    phantom = sorted(actual_keys - expected_keys)
    missing = sorted(expected_keys - actual_keys)
    return phantom, missing


def compute_state_aggregates(rows: Sequence[sqlite3.Row]) -> Dict[Tuple[str, str], Dict[str, int]]:
    agg: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        key = (row["wallet"], row["window"])
        event_type = row["event_type"]
        amount = int(row["sol_amount_lamports"]) if row["sol_amount_lamports"] is not None else 0
        event_time = row["event_time"]
        agg[key]["event_count"] += 1
        agg[key]["first_whale_time"] = (
            event_time
            if agg[key].get("first_whale_time") is None
            else min(agg[key]["first_whale_time"], event_time)
        )
        agg[key]["last_whale_time"] = (
            event_time
            if agg[key].get("last_whale_time") is None
            else max(agg[key]["last_whale_time"], event_time)
        )
        if event_type == "WHALE_TX_BUY":
            agg[key]["whale_tx_buy_count"] += 1
            agg[key]["whale_tx_buy_max_lamports"] = max(
                agg[key].get("whale_tx_buy_max_lamports", 0), amount
            )
        if event_type == "WHALE_TX_SELL":
            agg[key]["whale_tx_sell_count"] += 1
            agg[key]["whale_tx_sell_max_lamports"] = max(
                agg[key].get("whale_tx_sell_max_lamports", 0), amount
            )
        if event_type in {"WHALE_CUM_24H_BUY", "WHALE_CUM_7D_BUY"}:
            agg[key]["whale_cum_buy_total_lamports"] += amount
        if event_type in {"WHALE_CUM_24H_SELL", "WHALE_CUM_7D_SELL"}:
            agg[key]["whale_cum_sell_total_lamports"] += amount
    return agg


def main() -> int:
    parser = argparse.ArgumentParser(description="PANDA v4 Phase 3.2 acceptance for whale tables")
    parser.add_argument("--db", required=True, help="Path to SQLite DB")
    args = parser.parse_args()

    conn = connect_db(args.db)
    results: List[BlockResult] = []

    # A1
    details: List[str] = []
    expected_whale_events = [
        ("wallet", "TEXT"),
        ("window", "TEXT"),
        ("event_time", "INTEGER"),
        ("event_type", "TEXT"),
        ("sol_amount_lamports", "INTEGER"),
        ("supporting_flow_count", "INTEGER"),
        ("flow_ref", "TEXT"),
        ("created_at", "INTEGER"),
    ]
    expected_whale_states = [
        ("wallet", "TEXT"),
        ("window", "TEXT"),
        ("whale_tx_buy_count", "INTEGER"),
        ("whale_tx_sell_count", "INTEGER"),
        ("whale_tx_buy_max_lamports", "INTEGER"),
        ("whale_tx_sell_max_lamports", "INTEGER"),
        ("whale_cum_buy_total_lamports", "INTEGER"),
        ("whale_cum_sell_total_lamports", "INTEGER"),
        ("first_whale_time", "INTEGER"),
        ("last_whale_time", "INTEGER"),
        ("created_at", "INTEGER"),
    ]
    a1_pass = True
    for table, expected in (
        ("whale_events", expected_whale_events),
        ("whale_states", expected_whale_states),
    ):
        if not table_exists(conn, table):
            a1_pass = False
            details.append(f"missing table: {table}")
            continue
        actual_info = fetch_table_info(conn, table)
        actual_cols = [(row["name"], normalize_type(row["type"])) for row in actual_info]
        expected_cols = [(name, normalize_type(tp)) for name, tp in expected]
        if actual_cols != expected_cols:
            a1_pass = False
            details.append(f"schema mismatch for {table}: expected {expected_cols}, got {actual_cols}")
    results.append(BlockResult("A1", a1_pass, details))

    # A2
    details = []
    a2_pass = True
    if table_exists(conn, "whale_events"):
        window_bad = conn.execute(
            "SELECT COUNT(*) FROM whale_events WHERE window NOT IN ('24h','7d','lifetime');"
        ).fetchone()[0]
        event_type_bad = conn.execute(
            f"SELECT COUNT(*) FROM whale_events WHERE event_type NOT IN ({','.join('?' for _ in EVENT_TYPES)});",
            EVENT_TYPES,
        ).fetchone()[0]
        null_bad = conn.execute(
            """
            SELECT COUNT(*) FROM whale_events
            WHERE wallet IS NULL OR window IS NULL OR event_time IS NULL
               OR event_type IS NULL OR sol_amount_lamports IS NULL
               OR supporting_flow_count IS NULL OR created_at IS NULL
            """
        ).fetchone()[0]
        amount_bad = conn.execute(
            "SELECT COUNT(*) FROM whale_events WHERE sol_amount_lamports < 0;"
        ).fetchone()[0]
        support_bad = conn.execute(
            "SELECT COUNT(*) FROM whale_events WHERE supporting_flow_count < 1;"
        ).fetchone()[0]
        if window_bad:
            a2_pass = False
            details.append(f"invalid window values in whale_events: {window_bad}")
        if event_type_bad:
            a2_pass = False
            details.append(f"invalid event_type values in whale_events: {event_type_bad}")
        if null_bad:
            a2_pass = False
            details.append(f"nulls in required whale_events columns: {null_bad}")
        if amount_bad:
            a2_pass = False
            details.append(f"negative sol_amount_lamports in whale_events: {amount_bad}")
        if support_bad:
            a2_pass = False
            details.append(f"supporting_flow_count < 1 in whale_events: {support_bad}")

    if table_exists(conn, "whale_states"):
        window_bad = conn.execute(
            "SELECT COUNT(*) FROM whale_states WHERE window NOT IN ('24h','7d','lifetime');"
        ).fetchone()[0]
        null_bad = conn.execute(
            """
            SELECT COUNT(*) FROM whale_states
            WHERE wallet IS NULL OR window IS NULL OR whale_tx_buy_count IS NULL
               OR whale_tx_sell_count IS NULL OR whale_tx_buy_max_lamports IS NULL
               OR whale_tx_sell_max_lamports IS NULL OR whale_cum_buy_total_lamports IS NULL
               OR whale_cum_sell_total_lamports IS NULL OR created_at IS NULL
            """
        ).fetchone()[0]
        if window_bad:
            a2_pass = False
            details.append(f"invalid window values in whale_states: {window_bad}")
        if null_bad:
            a2_pass = False
            details.append(f"nulls in required whale_states columns: {null_bad}")

    results.append(BlockResult("A2", a2_pass, details))

    # A3
    details = []
    a3_pass = True
    if table_exists(conn, "whale_events"):
        dupes = conn.execute(
            """
            SELECT wallet, window, event_type, event_time, flow_ref, COUNT(*) AS cnt
            FROM whale_events
            GROUP BY wallet, window, event_type, event_time, flow_ref
            HAVING COUNT(*) > 1
            """
        ).fetchall()
        if dupes:
            a3_pass = False
            details.append("duplicate whale_events keys:")
            for row in dupes:
                details.append(
                    f"{row['wallet']}|{row['window']}|{row['event_type']}|{row['event_time']}|{row['flow_ref']} -> {row['cnt']}"
                )
    if table_exists(conn, "whale_states"):
        dupes = conn.execute(
            """
            SELECT wallet, window, COUNT(*) AS cnt
            FROM whale_states
            GROUP BY wallet, window
            HAVING COUNT(*) > 1
            """
        ).fetchall()
        if dupes:
            a3_pass = False
            details.append("duplicate whale_states keys:")
            for row in dupes:
                details.append(f"{row['wallet']}|{row['window']} -> {row['cnt']}")
    results.append(BlockResult("A3", a3_pass, details))

    # A4
    details = []
    a4_pass = True
    if not table_exists(conn, "wallet_token_flow"):
        a4_pass = False
        details.append("missing table: wallet_token_flow")
    elif not table_exists(conn, "whale_events"):
        a4_pass = False
        details.append("missing table: whale_events")
    else:
        flow_columns = fetch_columns(conn, "wallet_token_flow")
        expected_rows, issues, event_columns = build_expected_event_rows(conn, flow_columns)
        if issues:
            a4_pass = False
            details.extend(issues)
        actual_rows = conn.execute(
            """
            SELECT wallet, window, event_time, event_type, sol_amount_lamports,
                   supporting_flow_count, flow_ref
            FROM whale_events
            """
        ).fetchall()
        if expected_rows:
            phantom, missing = compare_event_sets(actual_rows, expected_rows)
            if phantom:
                a4_pass = False
                details.append("phantom events:")
                details.extend(phantom)
            if missing:
                a4_pass = False
                details.append("missing events:")
                details.extend(missing)
        # Direction + time + amount checks
        direction_col = first_present(flow_columns, ["sol_direction", "direction"])
        flow_time_col = first_present(flow_columns, ["flow_time", "event_time", "block_time", "tx_time", "time"])
        flow_ref_col = first_present(flow_columns, ["flow_ref", "signature", "tx_signature", "flow_id"])
        wallet_col = first_present(flow_columns, ["wallet", "scan_wallet"])
        window_col = first_present(flow_columns, ["window"])
        amount_col = first_present(flow_columns, ["sol_amount_lamports", "sol_amount", "amount_lamports"])
        if None in (direction_col, flow_time_col, flow_ref_col, wallet_col, window_col, amount_col):
            a4_pass = False
            details.append("wallet_token_flow missing columns for parity checks")
        else:
            select_cols = [
                "we.wallet",
                "we.window",
                "we.event_type",
                "we.event_time",
                "we.flow_ref",
                "we.sol_amount_lamports AS we_amount",
                f"wf.{direction_col} AS flow_direction",
                f"wf.{flow_time_col} AS flow_time",
                f"wf.{amount_col} AS flow_amount",
            ]
            for event_type, col in event_columns.items():
                if col:
                    select_cols.append(f"wf.{col} AS {event_type.lower()}_amount")
            join_sql = f"""
                SELECT {', '.join(select_cols)}
                FROM whale_events we
                JOIN wallet_token_flow wf
                  ON we.wallet = wf.{wallet_col}
                 AND we.window = wf.{window_col}
                 AND (
                      we.flow_ref = wf.{flow_ref_col}
                   OR (we.flow_ref IS NULL AND wf.{flow_ref_col} IS NULL)
                 )
            """
            bad_rows = []
            for row in conn.execute(join_sql).fetchall():
                direction = str(row["flow_direction"]).lower()
                if "BUY" in row["event_type"] and direction not in {"buy", "in"}:
                    bad_rows.append(
                        f"direction mismatch {row['wallet']}|{row['window']}|{row['event_type']}|{row['event_time']}|{row['flow_ref']}"
                    )
                if "SELL" in row["event_type"] and direction not in {"sell", "out"}:
                    bad_rows.append(
                        f"direction mismatch {row['wallet']}|{row['window']}|{row['event_type']}|{row['event_time']}|{row['flow_ref']}"
                    )
                if row["event_time"] != row["flow_time"]:
                    bad_rows.append(
                        f"time mismatch {row['wallet']}|{row['window']}|{row['event_type']}|{row['event_time']}|{row['flow_ref']}"
                    )
                candidate_amounts = {int(row["flow_amount"])}
                event_type = row["event_type"]
                event_key = f"{event_type.lower()}_amount"
                if event_key in row.keys() and row[event_key] is not None:
                    candidate_amounts.add(int(row[event_key]))
                if int(row["we_amount"]) not in candidate_amounts:
                    bad_rows.append(
                        f"amount mismatch {row['wallet']}|{row['window']}|{row['event_type']}|{row['event_time']}|{row['flow_ref']}"
                    )
            if bad_rows:
                a4_pass = False
                details.append("parity mismatches:")
                details.extend(bad_rows)
    results.append(BlockResult("A4", a4_pass, details))

    # A5
    details = []
    a5_pass = True
    if table_exists(conn, "whale_events") and table_exists(conn, "whale_states"):
        events = conn.execute(
            """
            SELECT wallet, window, event_type, event_time, sol_amount_lamports
            FROM whale_events
            """
        ).fetchall()
        agg = compute_state_aggregates(events)
        states = conn.execute(
            """
            SELECT wallet, window, whale_tx_buy_count, whale_tx_sell_count,
                   whale_tx_buy_max_lamports, whale_tx_sell_max_lamports,
                   whale_cum_buy_total_lamports, whale_cum_sell_total_lamports,
                   first_whale_time, last_whale_time
            FROM whale_states
            """
        ).fetchall()
        for row in states:
            key = (row["wallet"], row["window"])
            expected = agg.get(key, defaultdict(int))
            event_count = expected.get("event_count", 0)
            mismatches = []
            for col in [
                "whale_tx_buy_count",
                "whale_tx_sell_count",
                "whale_tx_buy_max_lamports",
                "whale_tx_sell_max_lamports",
                "whale_cum_buy_total_lamports",
                "whale_cum_sell_total_lamports",
                "first_whale_time",
                "last_whale_time",
            ]:
                expected_val = expected.get(col)
                actual_val = row[col]
                if expected_val is None:
                    if col in {"first_whale_time", "last_whale_time"} and event_count == 0:
                        expected_val = None
                    else:
                        expected_val = 0
                if actual_val != expected_val:
                    mismatches.append(f"{col} expected {expected_val} got {actual_val}")
            if mismatches:
                a5_pass = False
                details.append(f"state mismatch {row['wallet']}|{row['window']}: " + "; ".join(mismatches))
    else:
        a5_pass = False
        details.append("missing whale_events or whale_states table")
    results.append(BlockResult("A5", a5_pass, details))

    # A6
    details = []
    a6_pass = True
    if table_exists(conn, "whale_events") and table_exists(conn, "wallet_token_flow"):
        flow_columns = fetch_columns(conn, "wallet_token_flow")
        flow_time_col = first_present(flow_columns, ["flow_time", "event_time", "block_time", "tx_time", "time"])
        wallet_col = first_present(flow_columns, ["wallet", "scan_wallet"])
        window_col = first_present(flow_columns, ["window"])
        if None in (flow_time_col, wallet_col, window_col):
            a6_pass = False
            details.append("wallet_token_flow missing columns for window bounds")
        else:
            bounds = conn.execute(
                f"""
                SELECT {wallet_col} AS wallet, {window_col} AS window,
                       MIN({flow_time_col}) AS min_time,
                       MAX({flow_time_col}) AS max_time
                FROM wallet_token_flow
                GROUP BY {wallet_col}, {window_col}
                """
            ).fetchall()
            bound_map = {(row["wallet"], row["window"]): row for row in bounds}
            out_of_bounds = []
            for row in conn.execute(
                "SELECT wallet, window, event_time FROM whale_events"
            ).fetchall():
                bound = bound_map.get((row["wallet"], row["window"]))
                if not bound:
                    out_of_bounds.append(
                        f"missing bounds for {row['wallet']}|{row['window']} event_time {row['event_time']}"
                    )
                    continue
                if row["event_time"] < bound["min_time"] or row["event_time"] > bound["max_time"]:
                    out_of_bounds.append(
                        f"out of bounds {row['wallet']}|{row['window']} event_time {row['event_time']}"
                    )
            if out_of_bounds:
                a6_pass = False
                details.append("window boundary violations:")
                details.extend(out_of_bounds)
    else:
        a6_pass = False
        details.append("missing whale_events or wallet_token_flow table")
    results.append(BlockResult("A6", a6_pass, details))

    # A7
    details = []
    a7_pass = True
    if table_exists(conn, "whale_events"):
        sql = """
            SELECT wallet, window, event_type, COUNT(*) AS cnt, SUM(sol_amount_lamports) AS total
            FROM whale_events
            GROUP BY wallet, window, event_type
            ORDER BY wallet, window, event_type
        """
        first = conn.execute(sql).fetchall()
        second = conn.execute(sql).fetchall()
        if first != second:
            a7_pass = False
            details.append("aggregate recomputation mismatch")
    else:
        a7_pass = False
        details.append("missing whale_events table")
    results.append(BlockResult("A7", a7_pass, details))

    # Row counts per window
    if table_exists(conn, "whale_events"):
        print("Row counts whale_events:")
        for window in WINDOWS:
            count = conn.execute(
                "SELECT COUNT(*) FROM whale_events WHERE window = ?;",
                (window,),
            ).fetchone()[0]
            print(f"  {window}: {count}")
    if table_exists(conn, "whale_states"):
        print("Row counts whale_states:")
        for window in WINDOWS:
            count = conn.execute(
                "SELECT COUNT(*) FROM whale_states WHERE window = ?;",
                (window,),
            ).fetchone()[0]
            print(f"  {window}: {count}")

    for result in results:
        print_block(result)

    final_pass = all(result.passed for result in results)
    print("FINAL VERDICT:", "PASS" if final_pass else "FAIL")
    return 0 if final_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
