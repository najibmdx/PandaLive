#!/usr/bin/env python3
"""
panda_phase3_3_acceptance.py

Phase 3.3 acceptance verification for whale_states and whale_transitions.
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
    parser = argparse.ArgumentParser(description="Phase 3.3 acceptance checks.")
    parser.add_argument("--db", default="masterwalletsdb.db", help="Path to SQLite DB")
    parser.add_argument("--outdir", default="exports_phase3_3_accept", help="Output directory")
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
                    ELSE NULL
                END AS side,
                CAST({mapped['event_time']} AS INTEGER) AS event_time,
                {mapped['flow_ref']} AS flow_ref,
                CAST({mapped['amount_lamports']} AS INTEGER) AS amount_lamports,
                CAST({mapped['supporting_flow_count']} AS INTEGER) AS supporting_flow_count
            FROM whale_events
        )
    """


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
        raise ValueError(
            "ERROR: unexpected derived side values: " + ", ".join(map(str, offenders))
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
        WITH normalized AS (
            SELECT
                CASE
                    WHEN {column} LIKE '%\\_BUY' ESCAPE '\\' THEN 'buy'
                    WHEN {column} LIKE '%\\_SELL' ESCAPE '\\' THEN 'sell'
                    ELSE NULL
                END AS side
            FROM whale_events
        )
        SELECT DISTINCT side
        FROM normalized
        ORDER BY side
        """
    )
    return [row[0] for row in cursor.fetchall()]


def compute_digest(cursor: sqlite3.Cursor, table: str) -> str:
    cursor.execute(f"SELECT * FROM {table} ORDER BY wallet, window, side")
    digest = hashlib.sha256()
    for row in cursor.fetchall():
        digest.update("|".join(str(value) for value in row).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def write_report(outdir: str, rows: List[Tuple[str, str, str]]) -> None:
    path = os.path.join(outdir, "phase3_3_acceptance_report.tsv")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("section\tverdict\tdetails\n")
        for section, verdict, details in rows:
            handle.write(f"{section}\t{verdict}\t{details}\n")


def check_duplicate_rows(cursor: sqlite3.Cursor, table: str, columns: List[str]) -> int:
    group_cols = ", ".join(columns)
    cursor.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT {group_cols}, COUNT(*) AS cnt
            FROM {table}
            GROUP BY {group_cols}
            HAVING cnt > 1
        )
        """
    )
    return cursor.fetchone()[0]


def check_referential_integrity(cursor: sqlite3.Cursor, table: str) -> int:
    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {table}
        LEFT JOIN wallet_token_flow
          ON {table}.flow_ref = wallet_token_flow.signature
        WHERE wallet_token_flow.signature IS NULL
        """
    )
    return cursor.fetchone()[0]


def count_expected_groups(cursor: sqlite3.Cursor, mapped: Dict[str, str]) -> int:
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

    report_rows: List[Tuple[str, str, str]] = []
    final_pass = True

    try:
        whale_columns = discover_columns(cursor, "whale_events")
        flow_columns = discover_columns(cursor, "wallet_token_flow")
        mapped = map_columns(whale_columns, WHALE_EVENTS_ALIASES)
        require_wallet_token_flow_signature(flow_columns)

        print("Schema mapping:", ", ".join(f"{k}={v}" for k, v in mapped.items()))

        window_values = check_distinct_values(cursor, "whale_events", mapped["window"])
        validate_windows(window_values)
        validate_event_type_suffix(cursor, mapped["event_type"])
        side_values = distinct_derived_sides(cursor, mapped["event_type"])
        if None in side_values:
            cursor.execute(
                f"""
                SELECT {mapped['event_type']}, COUNT(*) AS cnt
                FROM whale_events
                WHERE {mapped['event_type']} NOT LIKE '%\\_BUY' ESCAPE '\\'
                  AND {mapped['event_type']} NOT LIKE '%\\_SELL' ESCAPE '\\'
                GROUP BY {mapped['event_type']}
                ORDER BY cnt DESC, {mapped['event_type']}
                """
            )
            offenders = cursor.fetchall()
            formatted = ", ".join(f"{row[0]}:{row[1]}" for row in offenders)
            raise ValueError(
                "ERROR: NULL derived side values. Offending event_type values: " + formatted
            )
        validate_sides(side_values)

        expected_groups = count_expected_groups(cursor, mapped)

        cursor.execute("SELECT COUNT(*) FROM whale_states")
        states_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM whale_transitions")
        transitions_count = cursor.fetchone()[0]

        dup_states = check_duplicate_rows(cursor, "whale_states", ["wallet", "window", "side"])
        dup_transitions = check_duplicate_rows(
            cursor, "whale_transitions", ["wallet", "window", "side", "transition_type"]
        )
        cursor.execute(
            "SELECT COUNT(*) FROM whale_transitions WHERE transition_type != 'ENTER'"
        )
        non_enter = cursor.fetchone()[0]

        uniqueness_pass = (
            dup_states == 0
            and dup_transitions == 0
            and non_enter == 0
            and states_count == expected_groups
            and transitions_count == states_count
        )
        report_rows.append(
            (
                "uniqueness_and_counts",
                "PASS" if uniqueness_pass else "FAIL",
                f"dup_states={dup_states}, dup_transitions={dup_transitions}, non_enter={non_enter}, "
                f"expected_groups={expected_groups}, states={states_count}, transitions={transitions_count}",
            )
        )
        final_pass = final_pass and uniqueness_pass

        normalized_cte = build_normalized_cte(mapped)
        if sqlite_supports_window_functions(cursor):
            expected_first_last = normalized_cte + """
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
            ),
            last_rows AS (
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
            """
        else:
            expected_first_last = normalized_cte + """
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
            ),
            last_rows AS (
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
            """

        cursor.execute(
            expected_first_last
            + """
            SELECT COUNT(*)
            FROM whale_transitions wt
            JOIN first_rows fr
              ON wt.wallet = fr.wallet
             AND wt.window = fr.window
             AND wt.side = fr.side
            WHERE wt.transition_type = 'ENTER'
              AND (
                    wt.event_time != fr.event_time
                 OR wt.flow_ref != fr.flow_ref
                 OR wt.amount_lamports != fr.amount_lamports
                 OR wt.supporting_flow_count != fr.supporting_flow_count
              )
            """
        )
        transition_mismatches = cursor.fetchone()[0]

        cursor.execute(
            expected_first_last
            + """
            SELECT COUNT(*)
            FROM whale_states ws
            JOIN last_rows lr
              ON ws.wallet = lr.wallet
             AND ws.window = lr.window
             AND ws.side = lr.side
            WHERE ws.asof_time != lr.event_time
               OR ws.flow_ref != lr.flow_ref
               OR ws.amount_lamports != lr.amount_lamports
               OR ws.supporting_flow_count != lr.supporting_flow_count
            """
        )
        state_latest_mismatches = cursor.fetchone()[0]

        cursor.execute(
            expected_first_last
            + """
            SELECT COUNT(*)
            FROM whale_states ws
            JOIN first_rows fr
              ON ws.wallet = fr.wallet
             AND ws.window = fr.window
             AND ws.side = fr.side
            WHERE ws.first_seen_time != fr.event_time
               OR ws.first_seen_flow_ref != fr.flow_ref
            """
        )
        state_first_mismatches = cursor.fetchone()[0]

        time_payload_pass = (
            transition_mismatches == 0
            and state_latest_mismatches == 0
            and state_first_mismatches == 0
        )
        report_rows.append(
            (
                "time_and_payload",
                "PASS" if time_payload_pass else "FAIL",
                f"transition_mismatches={transition_mismatches}, "
                f"state_latest_mismatches={state_latest_mismatches}, "
                f"state_first_mismatches={state_first_mismatches}",
            )
        )
        final_pass = final_pass and time_payload_pass

        integrity_states = check_referential_integrity(cursor, "whale_states")
        integrity_transitions = check_referential_integrity(cursor, "whale_transitions")
        integrity_pass = integrity_states == 0 and integrity_transitions == 0
        report_rows.append(
            (
                "referential_integrity",
                "PASS" if integrity_pass else "FAIL",
                f"states_missing={integrity_states}, transitions_missing={integrity_transitions}",
            )
        )
        final_pass = final_pass and integrity_pass

        cursor.execute(
            expected_first_last
            + """
            SELECT COUNT(*) FROM first_rows
            """
        )
        recomputed_first = cursor.fetchone()[0]
        cursor.execute(
            expected_first_last
            + """
            SELECT COUNT(*) FROM last_rows
            """
        )
        recomputed_last = cursor.fetchone()[0]
        determinism_pass = recomputed_first == expected_groups and recomputed_last == expected_groups
        digest_states = compute_digest(cursor, "whale_states")
        digest_transitions = compute_digest(cursor, "whale_transitions")
        report_rows.append(
            (
                "determinism",
                "PASS" if determinism_pass else "FAIL",
                f"recomputed_first={recomputed_first}, recomputed_last={recomputed_last}, "
                f"states_digest={digest_states}, transitions_digest={digest_transitions}",
            )
        )
        final_pass = final_pass and determinism_pass

    except Exception as exc:
        report_rows.append(("error", "FAIL", str(exc)))
        final_pass = False

    write_report(args.outdir, report_rows)

    for section, verdict, details in report_rows:
        print(f"{section}: {verdict} ({details})")
    print("FINAL VERDICT:", "PASS" if final_pass else "FAIL")
    if not final_pass:
        sys.exit(1)


if __name__ == "__main__":
    main()
