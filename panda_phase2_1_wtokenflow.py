#!/usr/bin/env python3
import argparse
import json
import sqlite3
import sys
import time
from typing import Dict, List, Tuple

WINDOWS = [
    ("lifetime", 0, None),
    ("24h", None, None),
    ("7d", None, None),
]

REQUIRED_WALLET_TOKEN_FLOW_COLUMNS = {
    "wallet",
    "mint",
    "window_kind",
    "window_start_ts",
    "window_end_ts",
    "in_amount_raw",
    "out_amount_raw",
    "net_amount_raw",
    "in_count",
    "out_count",
    "unique_in_counterparties",
    "unique_out_counterparties",
    "top_in_counterparty",
    "top_out_counterparty",
    "top_in_amount_raw",
    "top_out_amount_raw",
    "first_seen",
    "last_seen",
    "created_at",
    "updated_at",
}

SPL_TRANSFER_FIELD_CANDIDATES = {
    "signature": ["signature", "sig", "tx_signature", "txn_signature"],
    "mint": ["mint", "token_mint"],
    "amount_raw": ["amount_raw", "amount", "amount_int", "raw_amount"],
    "src_wallet": ["src_wallet", "source_wallet", "from_wallet", "src", "source"],
    "dst_wallet": ["dst_wallet", "destination_wallet", "to_wallet", "dst", "destination"],
    "ix_kind": ["ix_kind", "instruction_kind", "ix_type", "instruction_type", "kind"],
}

TX_REQUIRED_COLUMNS = ["signature", "block_time"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Populate Phase 2.1 wallet token flow")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--topn", type=int, default=5, help="Unused for now; top counterparty only")
    parser.add_argument("--now-ts", type=int, default=None, help="Override NOW_TS")
    return parser.parse_args()


def get_table_columns(conn: sqlite3.Connection, table: str) -> Dict[str, Dict[str, int]]:
    cursor = conn.execute(f"PRAGMA table_info({table})")
    rows = cursor.fetchall()
    if not rows:
        raise RuntimeError(f"Missing required table: {table}")
    return {row[1]: {"notnull": row[3]} for row in rows}


def require_columns(table: str, columns: Dict[str, Dict[str, int]], required: List[str]) -> None:
    missing = [col for col in required if col not in columns]
    if missing:
        raise RuntimeError(f"Missing columns in {table}: {', '.join(missing)}")


def resolve_spl_mapping(columns: Dict[str, Dict[str, int]]) -> Dict[str, str]:
    mapping = {}
    for logical_name, candidates in SPL_TRANSFER_FIELD_CANDIDATES.items():
        matches = [c for c in candidates if c in columns]
        if len(matches) == 1:
            mapping[logical_name] = matches[0]
        elif len(matches) == 0:
            raise RuntimeError(f"Unable to map required spl_transfers_v2 field: {logical_name}")
        else:
            raise RuntimeError(
                f"Ambiguous mapping for spl_transfers_v2 field {logical_name}: {', '.join(matches)}"
            )
    return mapping


def compute_now_ts(conn: sqlite3.Connection, override: int | None) -> int:
    if override is not None:
        return override
    row = conn.execute("SELECT MAX(block_time) FROM tx").fetchone()
    if row is None or row[0] is None:
        raise RuntimeError("Unable to determine NOW_TS from tx.block_time")
    return int(row[0])


def delete_existing_window(conn: sqlite3.Connection, window_kind: str, start_ts: int, end_ts: int) -> None:
    conn.execute(
        "DELETE FROM wallet_token_flow WHERE window_kind = ? AND window_start_ts = ? AND window_end_ts = ?",
        (window_kind, start_ts, end_ts),
    )


def create_temp_events(
    conn: sqlite3.Connection,
    mapping: Dict[str, str],
    window_start: int,
    window_end: int,
) -> None:
    conn.execute("DROP TABLE IF EXISTS tmp_events")
    conn.execute(
        f"""
        CREATE TEMP TABLE tmp_events AS
        SELECT
            st.{mapping['signature']} AS signature,
            st.{mapping['mint']} AS mint,
            st.{mapping['amount_raw']} AS amount_raw,
            st.{mapping['src_wallet']} AS src_wallet,
            st.{mapping['dst_wallet']} AS dst_wallet,
            st.{mapping['ix_kind']} AS ix_kind,
            tx.block_time AS block_time
        FROM spl_transfers_v2 st
        INNER JOIN tx ON st.{mapping['signature']} = tx.signature
        WHERE tx.block_time >= ? AND tx.block_time <= ?
          AND st.{mapping['ix_kind']} IN ('transfer_checked', 'close_account')
          AND st.{mapping['mint']} IS NOT NULL
          AND st.{mapping['amount_raw']} IS NOT NULL
          AND st.{mapping['src_wallet']} IS NOT NULL
          AND st.{mapping['dst_wallet']} IS NOT NULL
          AND st.{mapping['signature']} IS NOT NULL
          AND tx.block_time IS NOT NULL
        """,
        (window_start, window_end),
    )


def build_aggregates(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS tmp_inbound")
    conn.execute(
        """
        CREATE TEMP TABLE tmp_inbound AS
        SELECT
            dst_wallet AS wallet,
            mint,
            SUM(amount_raw) AS in_amount_raw,
            COUNT(*) AS in_count,
            COUNT(DISTINCT src_wallet) AS unique_in_counterparties,
            MIN(block_time) AS first_seen,
            MAX(block_time) AS last_seen
        FROM tmp_events
        GROUP BY dst_wallet, mint
        """
    )

    conn.execute("DROP TABLE IF EXISTS tmp_outbound")
    conn.execute(
        """
        CREATE TEMP TABLE tmp_outbound AS
        SELECT
            src_wallet AS wallet,
            mint,
            SUM(amount_raw) AS out_amount_raw,
            COUNT(*) AS out_count,
            COUNT(DISTINCT dst_wallet) AS unique_out_counterparties,
            MIN(block_time) AS first_seen,
            MAX(block_time) AS last_seen
        FROM tmp_events
        GROUP BY src_wallet, mint
        """
    )

    conn.execute("DROP TABLE IF EXISTS tmp_wallet_mints")
    conn.execute(
        """
        CREATE TEMP TABLE tmp_wallet_mints AS
        SELECT dst_wallet AS wallet, mint FROM tmp_events
        UNION
        SELECT src_wallet AS wallet, mint FROM tmp_events
        """
    )

    conn.execute("DROP TABLE IF EXISTS tmp_top_in")
    conn.execute(
        """
        CREATE TEMP TABLE tmp_top_in AS
        SELECT
            e.wallet,
            e.mint,
            (
                SELECT src_wallet FROM (
                    SELECT src_wallet, SUM(amount_raw) AS total
                    FROM tmp_events e2
                    WHERE e2.dst_wallet = e.wallet AND e2.mint = e.mint
                    GROUP BY src_wallet
                    ORDER BY total DESC, src_wallet ASC
                    LIMIT 1
                )
            ) AS top_in_counterparty,
            (
                SELECT total FROM (
                    SELECT src_wallet, SUM(amount_raw) AS total
                    FROM tmp_events e2
                    WHERE e2.dst_wallet = e.wallet AND e2.mint = e.mint
                    GROUP BY src_wallet
                    ORDER BY total DESC, src_wallet ASC
                    LIMIT 1
                )
            ) AS top_in_amount_raw
        FROM (
            SELECT DISTINCT dst_wallet AS wallet, mint FROM tmp_events
        ) e
        """
    )

    conn.execute("DROP TABLE IF EXISTS tmp_top_out")
    conn.execute(
        """
        CREATE TEMP TABLE tmp_top_out AS
        SELECT
            e.wallet,
            e.mint,
            (
                SELECT dst_wallet FROM (
                    SELECT dst_wallet, SUM(amount_raw) AS total
                    FROM tmp_events e2
                    WHERE e2.src_wallet = e.wallet AND e2.mint = e.mint
                    GROUP BY dst_wallet
                    ORDER BY total DESC, dst_wallet ASC
                    LIMIT 1
                )
            ) AS top_out_counterparty,
            (
                SELECT total FROM (
                    SELECT dst_wallet, SUM(amount_raw) AS total
                    FROM tmp_events e2
                    WHERE e2.src_wallet = e.wallet AND e2.mint = e.mint
                    GROUP BY dst_wallet
                    ORDER BY total DESC, dst_wallet ASC
                    LIMIT 1
                )
            ) AS top_out_amount_raw
        FROM (
            SELECT DISTINCT src_wallet AS wallet, mint FROM tmp_events
        ) e
        """
    )


def insert_window_rows(
    conn: sqlite3.Connection,
    window_kind: str,
    window_start: int,
    window_end: int,
    now_ts: int,
) -> None:
    conn.execute(
        """
        INSERT INTO wallet_token_flow (
            wallet,
            mint,
            window_kind,
            window_start_ts,
            window_end_ts,
            in_amount_raw,
            out_amount_raw,
            net_amount_raw,
            in_count,
            out_count,
            unique_in_counterparties,
            unique_out_counterparties,
            top_in_counterparty,
            top_out_counterparty,
            top_in_amount_raw,
            top_out_amount_raw,
            first_seen,
            last_seen,
            created_at,
            updated_at
        )
        SELECT
            wm.wallet,
            wm.mint,
            ?,
            ?,
            ?,
            COALESCE(i.in_amount_raw, 0),
            COALESCE(o.out_amount_raw, 0),
            COALESCE(i.in_amount_raw, 0) - COALESCE(o.out_amount_raw, 0),
            COALESCE(i.in_count, 0),
            COALESCE(o.out_count, 0),
            COALESCE(i.unique_in_counterparties, 0),
            COALESCE(o.unique_out_counterparties, 0),
            ti.top_in_counterparty,
            to2.top_out_counterparty,
            ti.top_in_amount_raw,
            to2.top_out_amount_raw,
            CASE
                WHEN i.first_seen IS NULL THEN o.first_seen
                WHEN o.first_seen IS NULL THEN i.first_seen
                WHEN i.first_seen < o.first_seen THEN i.first_seen
                ELSE o.first_seen
            END AS first_seen,
            CASE
                WHEN i.last_seen IS NULL THEN o.last_seen
                WHEN o.last_seen IS NULL THEN i.last_seen
                WHEN i.last_seen > o.last_seen THEN i.last_seen
                ELSE o.last_seen
            END AS last_seen,
            ?,
            ?
        FROM tmp_wallet_mints wm
        LEFT JOIN tmp_inbound i ON i.wallet = wm.wallet AND i.mint = wm.mint
        LEFT JOIN tmp_outbound o ON o.wallet = wm.wallet AND o.mint = wm.mint
        LEFT JOIN tmp_top_in ti ON ti.wallet = wm.wallet AND ti.mint = wm.mint
        LEFT JOIN tmp_top_out to2 ON to2.wallet = wm.wallet AND to2.mint = wm.mint
        ORDER BY wm.wallet, wm.mint
        """,
        (window_kind, window_start, window_end, now_ts, now_ts),
    )


def check_conservation(
    conn: sqlite3.Connection, window_kind: str, window_start: int, window_end: int
) -> List[Tuple[str, int]]:
    cursor = conn.execute(
        """
        SELECT mint, SUM(net_amount_raw) AS net_sum
        FROM wallet_token_flow
        WHERE window_kind = ? AND window_start_ts = ? AND window_end_ts = ?
        GROUP BY mint
        HAVING net_sum != 0
        """,
        (window_kind, window_start, window_end),
    )
    return [(row[0], row[1]) for row in cursor.fetchall()]


def collect_counts(conn: sqlite3.Connection) -> Tuple[int, int, int]:
    event_count = conn.execute("SELECT COUNT(*) FROM tmp_events").fetchone()[0]
    distinct_wallets = conn.execute("SELECT COUNT(DISTINCT wallet) FROM tmp_wallet_mints").fetchone()[0]
    distinct_mints = conn.execute("SELECT COUNT(DISTINCT mint) FROM tmp_events").fetchone()[0]
    return event_count, distinct_wallets, distinct_mints


def insert_phase2_run(
    conn: sqlite3.Connection,
    window_kind: str,
    window_start: int,
    window_end: int,
    status: str,
    error: str | None,
    input_counts: Dict[str, int],
    output_counts: Dict[str, int],
    started_at: int,
    completed_at: int,
) -> None:
    conn.execute(
        """
        INSERT INTO phase2_runs (
            component,
            window_kind,
            window_start_ts,
            window_end_ts,
            started_at,
            completed_at,
            status,
            error,
            input_counts_json,
            output_counts_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "phase2_1_wallet_token_flow",
            window_kind,
            window_start,
            window_end,
            started_at,
            completed_at,
            status,
            error,
            json.dumps(input_counts, sort_keys=True),
            json.dumps(output_counts, sort_keys=True),
        ),
    )


def main() -> int:
    args = parse_args()
    conn = None
    started_at = int(time.time())
    try:
        conn = sqlite3.connect(args.db)
        conn.execute("PRAGMA foreign_keys = ON")
        wallet_token_flow_cols = get_table_columns(conn, "wallet_token_flow")
        require_columns(
            "wallet_token_flow",
            wallet_token_flow_cols,
            list(REQUIRED_WALLET_TOKEN_FLOW_COLUMNS),
        )
        spl_cols = get_table_columns(conn, "spl_transfers_v2")
        mapping = resolve_spl_mapping(spl_cols)
        tx_cols = get_table_columns(conn, "tx")
        require_columns("tx", tx_cols, TX_REQUIRED_COLUMNS)
        now_ts = compute_now_ts(conn, args.now_ts)

        windows = []
        for kind, start, end in WINDOWS:
            if kind == "lifetime":
                window_start = 0
                window_end = now_ts
            elif kind == "24h":
                window_start = now_ts - 86400
                window_end = now_ts
            elif kind == "7d":
                window_start = now_ts - 604800
                window_end = now_ts
            else:
                raise RuntimeError(f"Unknown window kind: {kind}")
            windows.append((kind, window_start, window_end))

        input_counts: Dict[str, int] = {"now_ts": now_ts}
        output_counts: Dict[str, int] = {}
        window_event_counts: Dict[str, int] = {}
        window_rows_inserted: Dict[str, int] = {}
        conservation_ok = {}

        for window_kind, window_start, window_end in windows:
            conn.execute("BEGIN IMMEDIATE")
            try:
                delete_existing_window(conn, window_kind, window_start, window_end)
                create_temp_events(conn, mapping, window_start, window_end)
                build_aggregates(conn)
                event_count, distinct_wallets, distinct_mints = collect_counts(conn)
                window_event_counts[window_kind] = event_count
                input_counts[f"transfer_like_events_{window_kind}"] = event_count
                input_counts[f"distinct_wallets_{window_kind}"] = distinct_wallets
                input_counts[f"distinct_mints_{window_kind}"] = distinct_mints

                if event_count > 0:
                    insert_window_rows(conn, window_kind, window_start, window_end, now_ts)

                rows_inserted = conn.execute(
                    """
                    SELECT COUNT(*) FROM wallet_token_flow
                    WHERE window_kind = ? AND window_start_ts = ? AND window_end_ts = ?
                    """,
                    (window_kind, window_start, window_end),
                ).fetchone()[0]
                window_rows_inserted[window_kind] = rows_inserted
                output_counts[f"rows_inserted_{window_kind}"] = rows_inserted

                violations = check_conservation(conn, window_kind, window_start, window_end)
                if violations:
                    for mint, net_sum in violations:
                        print(f"conservation_failed window={window_kind} mint={mint} net_sum={net_sum}")
                    conn.execute("ROLLBACK")
                    raise RuntimeError(f"Conservation check failed for window {window_kind}")

                conn.execute("COMMIT")
                conservation_ok[window_kind] = len(violations) == 0
            except Exception:
                conn.execute("ROLLBACK")
                raise

        completed_at = int(time.time())
        for window_kind, window_start, window_end in windows:
            insert_phase2_run(
                conn,
                window_kind,
                window_start,
                window_end,
                "ok",
                None,
                input_counts,
                output_counts,
                started_at,
                completed_at,
            )
        conn.commit()

        print(f"NOW_TS={now_ts}")
        for window_kind, window_start, window_end in windows:
            print(
                f"window={window_kind} start={window_start} end={window_end} "
                f"events={window_event_counts.get(window_kind, 0)} "
                f"rows_inserted={window_rows_inserted.get(window_kind, 0)} "
                f"conservation_ok={conservation_ok.get(window_kind, False)}"
            )
        return 0
    except Exception as exc:  # pylint: disable=broad-except
        error_message = str(exc)
        print(error_message)
        if conn is not None:
            try:
                completed_at = int(time.time())
                for window_kind, window_start, window_end in windows if "windows" in locals() else []:
                    insert_phase2_run(
                        conn,
                        window_kind,
                        window_start,
                        window_end,
                        "failed",
                        error_message,
                        {"now_ts": args.now_ts or -1},
                        {},
                        started_at,
                        completed_at,
                    )
                conn.commit()
            except Exception:
                pass
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
