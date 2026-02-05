#!/usr/bin/env python3
"""
panda_phase3_3_transitions.py

Phase 3.3: Compress persistent whale snapshots into whale_states and whale_transitions.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sqlite3
import sys
from typing import Dict, Iterable, List, Tuple


WHALE_WINDOW_VALUES = {"24h", "7d", "lifetime"}

WHALE_EVENTS_ALIASES = {
    "wallet": ["wallet", "scan_wallet"],
    "window": ["window", "window_kind"],
    "event_time": ["event_time", "block_time"],
    "event_type": ["event_type"],
    "flow_ref": ["flow_ref", "signature"],
    "amount_lamports": ["sol_amount_lamports", "amount_lamports"],
    "supporting_flow_count": ["supporting_flow_count", "flow_count"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Phase 3.3 whale_states and whale_transitions.")
    parser.add_argument("--db", default="masterwalletsdb.db", help="Path to SQLite DB")
    parser.add_argument("--outdir", default="exports_phase3_3", help="Output directory")
    parser.add_argument(
        "--strict",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Enable strict validation (default: on)",
    )
    return parser.parse_args()


def discover_columns(cursor: sqlite3.Cursor, table: str) -> List[str]:
    cursor.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cursor.fetchall()]


def map_columns(columns: Iterable[str], alias_map: Dict[str, List[str]]) -> Dict[str, str]:
    mapped: Dict[str, str] = {}
    for semantic, aliases in alias_map.items():
        found = [alias for alias in aliases if alias in columns]
        if not found:
            raise ValueError(
                f"ERROR: whale_events missing required semantic column for '{semantic}'. "
                f"Expected one of {aliases}"
            )
        mapped[semantic] = found[0]
    return mapped


def require_wallet_token_flow_signature(columns: Iterable[str]) -> None:
    if "signature" not in columns:
        raise ValueError("ERROR: wallet_token_flow missing required column: signature")


def check_distinct_values(cursor: sqlite3.Cursor, table: str, column: str) -> List[str]:
    cursor.execute(f"SELECT DISTINCT {column} FROM {table} ORDER BY {column}")
    return [row[0] for row in cursor.fetchall()]


def validate_windows(distinct_values: List[str]) -> None:
    offenders = [value for value in distinct_values if value not in WHALE_WINDOW_VALUES]
    if offenders:
        raise ValueError(
            "ERROR: unexpected window values in whale_events: " + ", ".join(map(str, offenders))
        )


def validate_sides(distinct_values: List[str]) -> None:
    offenders = [value for value in distinct_values if value not in ("buy", "sell")]
    if offenders:
        raise ValueError("ERROR: unexpected derived side values: " + ", ".join(map(str, offenders)))


def validate_non_null(cursor: sqlite3.Cursor, table: str, column: str) -> None:
    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
    count = cursor.fetchone()[0]
    if count:
        raise ValueError(f"ERROR: {table}.{column} has {count} NULL values")


def validate_intish(cursor: sqlite3.Cursor, table: str, column: str) -> None:
    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {table}
        WHERE {column} IS NULL
           OR CAST({column} AS INTEGER) != {column}
        """
    )
    count = cursor.fetchone()[0]
    if count:
        raise ValueError(
            f"ERROR: {table}.{column} has {count} non-integer or NULL values"
        )


def validate_event_type_suffix(cursor: sqlite3.Cursor, column: str) -> None:
    cursor.execute(
        f"""
        SELECT {column}, COUNT(*) AS cnt
        FROM whale_events
        WHERE {column} IS NULL
           OR ({column} NOT LIKE '%\\_BUY' ESCAPE '\\'
           AND {column} NOT LIKE '%\\_SELL' ESCAPE '\\')
        GROUP BY {column}
        ORDER BY cnt DESC, {column}
        """
    )
    offenders = cursor.fetchall()
    if offenders:
        formatted = ", ".join(f"{row[0]}:{row[1]}" for row in offenders)
        raise ValueError(
            "ERROR: whale_events.event_type must end with _BUY or _SELL. Offenders: "
            + formatted
        )


def distinct_derived_sides(cursor: sqlite3.Cursor, column: str) -> List[str]:
    cursor.execute(
        f"""
        SELECT DISTINCT
            CASE
                WHEN {column} LIKE '%\\_BUY' ESCAPE '\\' THEN 'buy'
                WHEN {column} LIKE '%\\_SELL' ESCAPE '\\' THEN 'sell'
            END AS side
        FROM whale_events
        ORDER BY side
        """
    )
    return [row[0] for row in cursor.fetchall()]


def sqlite_supports_window_functions(cursor: sqlite3.Cursor) -> bool:
    cursor.execute("SELECT sqlite_version()")
    version = cursor.fetchone()[0]
    major, minor, patch = (int(part) for part in version.split("."))
    return (major, minor, patch) >= (3, 25, 0)


def build_normalized_cte(mapped: Dict[str, str]) -> str:
    return f"""
        WITH normalized AS (
            SELECT
                {mapped['wallet']} AS wallet,
                {mapped['window']} AS window,
                CASE
                    WHEN {mapped['event_type']} LIKE '%\\_BUY' ESCAPE '\\' THEN 'buy'
                    WHEN {mapped['event_type']} LIKE '%\\_SELL' ESCAPE '\\' THEN 'sell'
                END AS side,
                CAST({mapped['event_time']} AS INTEGER) AS event_time,
                {mapped['flow_ref']} AS flow_ref,
                CAST({mapped['amount_lamports']} AS INTEGER) AS amount_lamports,
                CAST({mapped['supporting_flow_count']} AS INTEGER) AS supporting_flow_count
            FROM whale_events
        )
    """


def build_tables(cursor: sqlite3.Cursor, mapped: Dict[str, str]) -> None:
    cursor.execute("DROP TABLE IF EXISTS whale_states")
    cursor.execute("DROP TABLE IF EXISTS whale_transitions")

    cursor.execute(
        """
        CREATE TABLE whale_states (
            wallet TEXT NOT NULL,
            window TEXT NOT NULL,
            side TEXT NOT NULL,
            asof_time INTEGER NOT NULL,
            amount_lamports INTEGER NOT NULL,
            supporting_flow_count INTEGER NOT NULL,
            flow_ref TEXT NOT NULL,
            first_seen_time INTEGER NOT NULL,
            first_seen_flow_ref TEXT NOT NULL,
            is_whale INTEGER NOT NULL,
            PRIMARY KEY (wallet, window, side)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE whale_transitions (
            wallet TEXT NOT NULL,
            window TEXT NOT NULL,
            side TEXT NOT NULL,
            transition_type TEXT NOT NULL,
            event_time INTEGER NOT NULL,
            amount_lamports INTEGER NOT NULL,
            supporting_flow_count INTEGER NOT NULL,
            flow_ref TEXT NOT NULL,
            PRIMARY KEY (wallet, window, side, transition_type)
        )
        """
    )

    normalized_cte = build_normalized_cte(mapped)
    if sqlite_supports_window_functions(cursor):
        cursor.execute(
            normalized_cte
            + """
            , first_rows AS (
                SELECT * FROM (
                    SELECT
                        wallet,
                        window,
                        side,
                        event_time,
                        flow_ref,
                        amount_lamports,
                        supporting_flow_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY wallet, window, side
                            ORDER BY event_time ASC, flow_ref ASC
                        ) AS rn
                    FROM normalized
                ) WHERE rn = 1
            ), last_rows AS (
                SELECT * FROM (
                    SELECT
                        wallet,
                        window,
                        side,
                        event_time,
                        flow_ref,
                        amount_lamports,
                        supporting_flow_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY wallet, window, side
                            ORDER BY event_time DESC, flow_ref DESC
                        ) AS rn
                    FROM normalized
                ) WHERE rn = 1
            )
            INSERT INTO whale_states (
                wallet,
                window,
                side,
                asof_time,
                amount_lamports,
                supporting_flow_count,
                flow_ref,
                first_seen_time,
                first_seen_flow_ref,
                is_whale
            )
            SELECT
                last_rows.wallet,
                last_rows.window,
                last_rows.side,
                last_rows.event_time,
                last_rows.amount_lamports,
                last_rows.supporting_flow_count,
                last_rows.flow_ref,
                first_rows.event_time,
                first_rows.flow_ref,
                1
            FROM last_rows
            JOIN first_rows
              ON last_rows.wallet = first_rows.wallet
             AND last_rows.window = first_rows.window
             AND last_rows.side = first_rows.side
            """
        )
        cursor.execute(
            normalized_cte
            + """
            , first_rows AS (
                SELECT * FROM (
                    SELECT
                        wallet,
                        window,
                        side,
                        event_time,
                        flow_ref,
                        amount_lamports,
                        supporting_flow_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY wallet, window, side
                            ORDER BY event_time ASC, flow_ref ASC
                        ) AS rn
                    FROM normalized
                ) WHERE rn = 1
            )
            INSERT INTO whale_transitions (
                wallet,
                window,
                side,
                transition_type,
                event_time,
                amount_lamports,
                supporting_flow_count,
                flow_ref
            )
            SELECT
                wallet,
                window,
                side,
                'ENTER',
                event_time,
                amount_lamports,
                supporting_flow_count,
                flow_ref
            FROM first_rows
            """
        )
    else:
        cursor.execute(
            normalized_cte
            + """
            , first_rows AS (
                SELECT n.*
                FROM normalized n
                WHERE (n.event_time, n.flow_ref) = (
                    SELECT n2.event_time, n2.flow_ref
                    FROM normalized n2
                    WHERE n2.wallet = n.wallet
                      AND n2.window = n.window
                      AND n2.side = n.side
                    ORDER BY n2.event_time ASC, n2.flow_ref ASC
                    LIMIT 1
                )
            ), last_rows AS (
                SELECT n.*
                FROM normalized n
                WHERE (n.event_time, n.flow_ref) = (
                    SELECT n2.event_time, n2.flow_ref
                    FROM normalized n2
                    WHERE n2.wallet = n.wallet
                      AND n2.window = n.window
                      AND n2.side = n.side
                    ORDER BY n2.event_time DESC, n2.flow_ref DESC
                    LIMIT 1
                )
            )
            INSERT INTO whale_states (
                wallet,
                window,
                side,
                asof_time,
                amount_lamports,
                supporting_flow_count,
                flow_ref,
                first_seen_time,
                first_seen_flow_ref,
                is_whale
            )
            SELECT
                last_rows.wallet,
                last_rows.window,
                last_rows.side,
                last_rows.event_time,
                last_rows.amount_lamports,
                last_rows.supporting_flow_count,
                last_rows.flow_ref,
                first_rows.event_time,
                first_rows.flow_ref,
                1
            FROM last_rows
            JOIN first_rows
              ON last_rows.wallet = first_rows.wallet
             AND last_rows.window = first_rows.window
             AND last_rows.side = first_rows.side
            """
        )
        cursor.execute(
            normalized_cte
            + """
            , first_rows AS (
                SELECT n.*
                FROM normalized n
                WHERE (n.event_time, n.flow_ref) = (
                    SELECT n2.event_time, n2.flow_ref
                    FROM normalized n2
                    WHERE n2.wallet = n.wallet
                      AND n2.window = n.window
                      AND n2.side = n.side
                    ORDER BY n2.event_time ASC, n2.flow_ref ASC
                    LIMIT 1
                )
            )
            INSERT INTO whale_transitions (
                wallet,
                window,
                side,
                transition_type,
                event_time,
                amount_lamports,
                supporting_flow_count,
                flow_ref
            )
            SELECT
                wallet,
                window,
                side,
                'ENTER',
                event_time,
                amount_lamports,
                supporting_flow_count,
                flow_ref
            FROM first_rows
            """
        )


def write_sample(cursor: sqlite3.Cursor, outpath: str, table: str) -> None:
    cursor.execute(
        f"SELECT * FROM {table} ORDER BY wallet, window, side LIMIT 200"
    )
    rows = cursor.fetchall()
    headers = [description[0] for description in cursor.description]
    with open(outpath, "w", encoding="utf-8") as handle:
        handle.write("\t".join(headers) + "\n")
        for row in rows:
            handle.write("\t".join(str(value) for value in row) + "\n")


def check_referential_integrity(
    cursor: sqlite3.Cursor, outdir: str, table: str, label: str
) -> Tuple[bool, int]:
    cursor.execute(
        f"""
        SELECT {table}.flow_ref
        FROM {table}
        LEFT JOIN wallet_token_flow
          ON {table}.flow_ref = wallet_token_flow.signature
        WHERE wallet_token_flow.signature IS NULL
        ORDER BY {table}.flow_ref
        """
    )
    offenders = [row[0] for row in cursor.fetchall()]
    if offenders:
        outpath = os.path.join(outdir, f"phase3_3_offenders_{label}.tsv")
        with open(outpath, "w", encoding="utf-8") as handle:
            handle.write("flow_ref\n")
            for flow_ref in offenders:
                handle.write(f"{flow_ref}\n")
        return False, len(offenders)
    return True, 0


def write_build_log(outdir: str, lines: List[str]) -> None:
    path = os.path.join(outdir, "phase3_3_build_log.txt")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def compute_digest(cursor: sqlite3.Cursor, table: str) -> str:
    cursor.execute(f"SELECT * FROM {table} ORDER BY wallet, window, side")
    digest = hashlib.sha256()
    for row in cursor.fetchall():
        digest.update("|".join(str(value) for value in row).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def count_groups(cursor: sqlite3.Cursor, mapped: Dict[str, str]) -> int:
    normalized_cte = build_normalized_cte(mapped)
    cursor.execute(
        normalized_cte
        + """
        SELECT COUNT(*) FROM (
            SELECT wallet, window, side
            FROM normalized
            GROUP BY wallet, window, side
        )
        """
    )
    return cursor.fetchone()[0]


def main() -> None:
    args = parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    conn = sqlite3.connect(args.db)
    cursor = conn.cursor()

    try:
        whale_columns = discover_columns(cursor, "whale_events")
        flow_columns = discover_columns(cursor, "wallet_token_flow")
        mapped = map_columns(whale_columns, WHALE_EVENTS_ALIASES)
        require_wallet_token_flow_signature(flow_columns)

        print("Schema mapping:", ", ".join(f"{k}={v}" for k, v in mapped.items()))

        window_values = check_distinct_values(cursor, "whale_events", mapped["window"])
        validate_windows(window_values)
        validate_event_type_suffix(cursor, mapped["event_type"])

        validate_non_null(cursor, "whale_events", mapped["event_time"])
        validate_non_null(cursor, "whale_events", mapped["event_type"])
        validate_non_null(cursor, "whale_events", mapped["flow_ref"])
        validate_non_null(cursor, "whale_events", mapped["amount_lamports"])
        validate_non_null(cursor, "whale_events", mapped["supporting_flow_count"])
        validate_intish(cursor, "whale_events", mapped["event_time"])
        validate_intish(cursor, "whale_events", mapped["amount_lamports"])
        validate_intish(cursor, "whale_events", mapped["supporting_flow_count"])

        side_values = distinct_derived_sides(cursor, mapped["event_type"])
        validate_sides(side_values)

        conn.execute("BEGIN")
        build_tables(cursor, mapped)

        write_sample(
            cursor,
            os.path.join(args.outdir, "whale_states.sample.tsv"),
            "whale_states",
        )
        write_sample(
            cursor,
            os.path.join(args.outdir, "whale_transitions.sample.tsv"),
            "whale_transitions",
        )

        integrity_states, missing_states = check_referential_integrity(
            cursor, args.outdir, "whale_states", "whale_states_flow_ref"
        )
        integrity_transitions, missing_transitions = check_referential_integrity(
            cursor, args.outdir, "whale_transitions", "whale_transitions_flow_ref"
        )

        if not integrity_states or not integrity_transitions:
            raise ValueError(
                "ERROR: Referential integrity check failed. "
                f"states_missing={missing_states}, transitions_missing={missing_transitions}"
            )

        cursor.execute("SELECT COUNT(*) FROM whale_events")
        total_events = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(DISTINCT {mapped['wallet']}) FROM whale_events")
        distinct_wallets = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM whale_states")
        states_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM whale_transitions")
        transitions_count = cursor.fetchone()[0]
        group_count = count_groups(cursor, mapped)

        digest_states = compute_digest(cursor, "whale_states")
        digest_transitions = compute_digest(cursor, "whale_transitions")

        log_lines = [
            f"total whale_events rows: {total_events}",
            f"distinct wallets: {distinct_wallets}",
            f"group count (wallet,window,side): {group_count}",
            f"whale_states rowcount: {states_count}",
            f"whale_transitions rowcount: {transitions_count}",
            "integrity: PASS",
            f"whale_states digest: {digest_states}",
            f"whale_transitions digest: {digest_transitions}",
        ]
        write_build_log(args.outdir, log_lines)

        conn.commit()
        print("Phase 3.3 build summary")
        for line in log_lines:
            print(line)
    except Exception as exc:
        conn.rollback()
        print(str(exc))
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
