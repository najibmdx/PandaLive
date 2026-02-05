#!/usr/bin/env python3
"""Phase 2.2 wallet edge generator (deterministic, idempotent)."""

import argparse
import json
import sqlite3
import sys
import time
from typing import Dict, List


WINDOWS = {
    "lifetime": lambda now_ts: (0, now_ts),
    "24h": lambda now_ts: (now_ts - 86400, now_ts),
    "7d": lambda now_ts: (now_ts - 604800, now_ts),
}

REQUIRED_WALLET_EDGES_COLUMNS = {
    "src_wallet",
    "dst_wallet",
    "mint",
    "window_kind",
    "window_start_ts",
    "window_end_ts",
    "tx_count",
    "amount_raw",
    "first_seen_ts",
    "last_seen_ts",
    "updated_at",
}

REQUIRED_PHASE2_RUNS_COLUMNS = {
    "run_id",
    "component",
    "window_kind",
    "window_start_ts",
    "window_end_ts",
    "input_counts_json",
    "output_counts_json",
    "created_at",
}

PHASE2_COMPONENT = "phase2_2_wallet_edges"


def get_table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cursor = conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cursor.fetchall()]


def require_columns(conn: sqlite3.Connection, table: str, required: set) -> None:
    columns = set(get_table_columns(conn, table))
    missing = sorted(required - columns)
    if missing:
        raise RuntimeError(
            f"Missing required columns in {table}: {', '.join(missing)}"
        )


def insert_phase2_run(
    conn: sqlite3.Connection,
    window_kind: str,
    window_start: int,
    window_end: int,
    now_ts: int,
    input_counts: Dict[str, int],
    output_counts: Dict[str, int],
) -> None:
    run_id = f"{PHASE2_COMPONENT}:{window_kind}:{window_start}:{window_end}"
    conn.execute(
        """
        INSERT INTO phase2_runs (
            run_id,
            component,
            window_kind,
            window_start_ts,
            window_end_ts,
            input_counts_json,
            output_counts_json,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id) DO UPDATE SET
            input_counts_json=excluded.input_counts_json,
            output_counts_json=excluded.output_counts_json,
            created_at=excluded.created_at
        """,
        (
            run_id,
            PHASE2_COMPONENT,
            window_kind,
            window_start,
            window_end,
            json.dumps(input_counts, sort_keys=True),
            json.dumps(output_counts, sort_keys=True),
            int(now_ts),
        ),
    )


def find_column(columns: List[str], logical_name: str, candidates: List[str]) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    raise RuntimeError(
        f"Missing required column for {logical_name}. Tried: {', '.join(candidates)}. "
        f"Available columns: {', '.join(columns)}"
    )


def map_spl_transfers_columns(columns: List[str]) -> Dict[str, str]:
    return {
        "signature": find_column(
            columns,
            "signature",
            ["signature", "sig", "tx_signature", "transaction_signature"],
        ),
        "mint": find_column(
            columns,
            "mint",
            ["mint", "mint_address", "token_mint"],
        ),
        "amount_raw": find_column(
            columns,
            "amount_raw",
            [
                "amount_raw",
                "amount",
                "token_amount_raw",
                "raw_amount",
                "amount_int",
            ],
        ),
        "src_wallet": find_column(
            columns,
            "src_wallet",
            [
                "from_addr",
                "src_wallet",
                "source",
                "source_wallet",
                "src",
                "from_owner",
                "from_wallet",
                "from_address",
                "from",
            ],
        ),
        "dst_wallet": find_column(
            columns,
            "dst_wallet",
            [
                "to_addr",
                "dst_wallet",
                "destination",
                "destination_wallet",
                "dst",
                "to_owner",
                "to_wallet",
                "to_address",
                "to",
            ],
        ),
        "ix_kind": find_column(
            columns,
            "ix_kind",
            [
                "instruction_type",
                "ix_kind",
                "instruction_kind",
                "instruction_type",
                "ix_type",
                "type",
            ],
        ),
    }


def resolve_time_source(
    spl_columns: List[str],
    tx_columns: List[str],
) -> Dict[str, str]:
    if "block_time" in spl_columns:
        return {
            "time_expr": "st.block_time",
            "join_clause": "",
            "block_time_col": "block_time",
        }
    spl_signature = find_column(
        spl_columns,
        "signature",
        ["signature", "sig", "tx_signature", "transaction_signature"],
    )
    tx_cols = map_tx_columns(tx_columns)
    return {
        "time_expr": f"tx.{tx_cols['block_time']}",
        "join_clause": (
            f"INNER JOIN tx ON st.{spl_signature} = tx.{tx_cols['signature']}"
        ),
        "block_time_col": tx_cols["block_time"],
    }


def map_tx_columns(columns: List[str]) -> Dict[str, str]:
    return {
        "signature": find_column(
            columns,
            "signature",
            ["signature", "sig", "tx_signature", "transaction_signature"],
        ),
        "block_time": find_column(
            columns,
            "block_time",
            ["block_time", "block_timestamp", "slot_time", "timestamp"],
        ),
    }


def fetch_now_ts(conn: sqlite3.Connection, tx_block_time_col: str) -> int:
    cursor = conn.execute(f"SELECT MAX({tx_block_time_col}) FROM tx")
    value = cursor.fetchone()[0]
    if value is None:
        raise RuntimeError("No tx.block_time available to determine now-ts")
    return int(value)


def build_event_filters(
    spl_cols: Dict[str, str],
    time_expr: str,
) -> str:
    required = [
        spl_cols["signature"],
        spl_cols["mint"],
        spl_cols["amount_raw"],
        spl_cols["src_wallet"],
        spl_cols["dst_wallet"],
        spl_cols["ix_kind"],
    ]
    null_checks = " AND ".join([f"st.{col} IS NOT NULL" for col in required[:-1]])
    null_checks = f"{null_checks} AND {time_expr} IS NOT NULL"
    ix_kinds = "('transfer_checked','close_account')"
    return (
        f"{null_checks} AND st.{spl_cols['ix_kind']} IN {ix_kinds} "
        f"AND st.{spl_cols['src_wallet']} <> st.{spl_cols['dst_wallet']}"
    )


def get_event_counts(
    conn: sqlite3.Connection,
    window_start: int,
    window_end: int,
    spl_cols: Dict[str, str],
    time_expr: str,
    join_clause: str,
) -> Dict[str, int]:
    filters = build_event_filters(spl_cols, time_expr)
    time_filter = f"{time_expr} BETWEEN ? AND ?"
    counts_sql = f"""
        SELECT
            COUNT(*) AS transfer_like_events,
            COUNT(DISTINCT st.{spl_cols['src_wallet']}) AS distinct_src_wallets,
            COUNT(DISTINCT st.{spl_cols['dst_wallet']}) AS distinct_dst_wallets,
            COUNT(DISTINCT st.{spl_cols['mint']}) AS distinct_mints
        FROM spl_transfers_v2 st
        {join_clause}
        WHERE {time_filter}
          AND {filters}
    """
    row = conn.execute(counts_sql, (window_start, window_end)).fetchone()
    return {
        "transfer_like_events": int(row[0] or 0),
        "distinct_src_wallets": int(row[1] or 0),
        "distinct_dst_wallets": int(row[2] or 0),
        "distinct_mints": int(row[3] or 0),
    }


def get_edges_inserted(
    conn: sqlite3.Connection,
    window_kind: str,
    window_start: int,
    window_end: int,
) -> int:
    cursor = conn.execute(
        """
        SELECT COUNT(*) FROM wallet_edges
        WHERE window_kind=? AND window_start_ts=? AND window_end_ts=?
        """,
        (window_kind, window_start, window_end),
    )
    return int(cursor.fetchone()[0] or 0)


def compute_edges(
    conn: sqlite3.Connection,
    window_kind: str,
    window_start: int,
    window_end: int,
    now_ts: int,
    spl_cols: Dict[str, str],
    time_expr: str,
    join_clause: str,
) -> int:
    filters = build_event_filters(spl_cols, time_expr)
    time_filter = f"{time_expr} BETWEEN ? AND ?"
    params = (window_start, window_end)

    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "DELETE FROM wallet_edges WHERE window_kind=? AND window_start_ts=? AND window_end_ts=?",
            (window_kind, window_start, window_end),
        )

        insert_sql = f"""
            INSERT INTO wallet_edges (
                src_wallet,
                dst_wallet,
                mint,
                window_kind,
                window_start_ts,
                window_end_ts,
                tx_count,
                amount_raw,
                first_seen_ts,
                last_seen_ts,
                updated_at
            )
            SELECT
                st.{spl_cols['src_wallet']} AS src_wallet,
                st.{spl_cols['dst_wallet']} AS dst_wallet,
                st.{spl_cols['mint']} AS mint,
                ? AS window_kind,
                ? AS window_start_ts,
                ? AS window_end_ts,
                COUNT(*) AS tx_count,
                SUM(st.{spl_cols['amount_raw']}) AS amount_raw,
                MIN({time_expr}) AS first_seen_ts,
                MAX({time_expr}) AS last_seen_ts,
                ? AS updated_at
            FROM spl_transfers_v2 st
            {join_clause}
            WHERE {time_filter}
              AND {filters}
            GROUP BY st.{spl_cols['src_wallet']}, st.{spl_cols['dst_wallet']}, st.{spl_cols['mint']}
            ORDER BY st.{spl_cols['src_wallet']}, st.{spl_cols['dst_wallet']}, st.{spl_cols['mint']}
        """
        conn.execute(
            insert_sql,
            (
                window_kind,
                window_start,
                window_end,
                now_ts,
                *params,
            ),
        )

        sanity_mint_count = sanity_checks(
            conn,
            window_kind,
            window_start,
            window_end,
            spl_cols,
            time_expr,
            join_clause,
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return sanity_mint_count


def sanity_checks(
    conn: sqlite3.Connection,
    window_kind: str,
    window_start: int,
    window_end: int,
    spl_cols: Dict[str, str],
    time_expr: str,
    join_clause: str,
) -> int:
    cursor = conn.execute(
        """
        SELECT COUNT(*) FROM wallet_edges
        WHERE window_kind=? AND window_start_ts=? AND window_end_ts=?
          AND src_wallet = dst_wallet
        """,
        (window_kind, window_start, window_end),
    )
    if cursor.fetchone()[0] != 0:
        raise RuntimeError(
            f"Sanity check failed: self-edges found for window {window_kind}"
        )

    event_sum_sql = f"""
        SELECT st.{spl_cols['mint']} AS mint,
               SUM(st.{spl_cols['amount_raw']}) AS total_amount_raw
        FROM spl_transfers_v2 st
        {join_clause}
        WHERE {time_expr} BETWEEN ? AND ?
          AND {build_event_filters(spl_cols, time_expr)}
        GROUP BY st.{spl_cols['mint']}
    """
    edge_sum_sql = """
        SELECT mint, SUM(amount_raw) AS total_amount_raw
        FROM wallet_edges
        WHERE window_kind=? AND window_start_ts=? AND window_end_ts=?
        GROUP BY mint
    """

    events = {
        row[0]: int(row[1])
        for row in conn.execute(event_sum_sql, (window_start, window_end))
    }
    edges = {
        row[0]: int(row[1])
        for row in conn.execute(edge_sum_sql, (window_kind, window_start, window_end))
    }

    if events != edges:
        missing = {mint: events[mint] for mint in events.keys() - edges.keys()}
        extra = {mint: edges[mint] for mint in edges.keys() - events.keys()}
        mismatched = {
            mint: (events[mint], edges[mint])
            for mint in events.keys() & edges.keys()
            if events[mint] != edges[mint]
        }
        detail = (
            f"missing={missing}, extra={extra}, mismatched={mismatched}"
        )
        raise RuntimeError(
            f"Sanity check failed: amount mismatch for window {window_kind}: {detail}"
        )
    return len(events)


def run(db_path: str, now_ts: int | None) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        require_columns(conn, "wallet_edges", REQUIRED_WALLET_EDGES_COLUMNS)
        require_columns(conn, "phase2_runs", REQUIRED_PHASE2_RUNS_COLUMNS)

        spl_cols_list = get_table_columns(conn, "spl_transfers_v2")
        tx_cols_list = get_table_columns(conn, "tx")
        spl_cols = map_spl_transfers_columns(spl_cols_list)
        time_source = resolve_time_source(spl_cols_list, tx_cols_list)
        time_expr = time_source["time_expr"]
        join_clause = time_source["join_clause"]

        if now_ts is None:
            if time_source["block_time_col"] == "block_time" and "block_time" in spl_cols_list:
                cursor = conn.execute("SELECT MAX(block_time) FROM spl_transfers_v2")
                value = cursor.fetchone()[0]
                if value is None:
                    raise RuntimeError(
                        "No spl_transfers_v2.block_time available to determine now-ts"
                    )
                now_ts = int(value)
            else:
                now_ts = fetch_now_ts(conn, time_source["block_time_col"])

        for window_kind, fn in WINDOWS.items():
            window_start, window_end = fn(now_ts)
            counts = None
            started_at = int(time.time())
            try:
                counts = get_event_counts(
                    conn,
                    window_start,
                    window_end,
                    spl_cols,
                    time_expr,
                    join_clause,
                )
                sanity_mint_count = compute_edges(
                    conn,
                    window_kind,
                    window_start,
                    window_end,
                    now_ts,
                    spl_cols,
                    time_expr,
                    join_clause,
                )
                edges_inserted = get_edges_inserted(
                    conn, window_kind, window_start, window_end
                )
                completed_at = int(time.time())
                input_counts = {
                    "now_ts": int(now_ts),
                    f"transfer_like_events_{window_kind}": counts[
                        "transfer_like_events"
                    ],
                    f"distinct_src_wallets_{window_kind}": counts[
                        "distinct_src_wallets"
                    ],
                    f"distinct_dst_wallets_{window_kind}": counts[
                        "distinct_dst_wallets"
                    ],
                    f"distinct_mints_{window_kind}": counts["distinct_mints"],
                    "started_at": started_at,
                }
                output_counts = {
                    f"edges_inserted_{window_kind}": edges_inserted,
                    f"sanity_mint_count_{window_kind}": sanity_mint_count,
                    "status": "ok",
                    "error": None,
                    "completed_at": completed_at,
                }
                insert_phase2_run(
                    conn,
                    window_kind,
                    window_start,
                    window_end,
                    now_ts,
                    input_counts,
                    output_counts,
                )
                conn.commit()
                print(
                    f"{window_kind} start={window_start} end={window_end} "
                    f"events={counts['transfer_like_events']} "
                    f"edges={edges_inserted}"
                )
            except Exception as exc:
                completed_at = int(time.time())
                if counts is None:
                    counts = {
                        "transfer_like_events": 0,
                        "distinct_src_wallets": 0,
                        "distinct_dst_wallets": 0,
                        "distinct_mints": 0,
                    }
                input_counts = {
                    "now_ts": int(now_ts),
                    f"transfer_like_events_{window_kind}": counts[
                        "transfer_like_events"
                    ],
                    f"distinct_src_wallets_{window_kind}": counts[
                        "distinct_src_wallets"
                    ],
                    f"distinct_dst_wallets_{window_kind}": counts[
                        "distinct_dst_wallets"
                    ],
                    f"distinct_mints_{window_kind}": counts["distinct_mints"],
                    "started_at": started_at,
                }
                output_counts = {
                    f"edges_inserted_{window_kind}": 0,
                    f"sanity_mint_count_{window_kind}": 0,
                    "status": "failed",
                    "error": str(exc),
                    "completed_at": completed_at,
                }
                insert_phase2_run(
                    conn,
                    window_kind,
                    window_start,
                    window_end,
                    now_ts,
                    input_counts,
                    output_counts,
                )
                conn.commit()
                raise
    finally:
        conn.close()


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Populate Phase 2.2 wallet edges.")
    parser.add_argument("--db", required=True, help="Path to sqlite DB")
    parser.add_argument("--now-ts", type=int, default=None, help="Override NOW_TS")
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    try:
        run(args.db, args.now_ts)
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
