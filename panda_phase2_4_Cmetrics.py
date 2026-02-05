#!/usr/bin/env python3
"""
PANDA Phase 2.4: Cohort Metrics + Dominance Scoring (deterministic, wallet-pure).
"""

import argparse
import json
import math
import sqlite3
import sys
from typing import Dict, Iterable, List, Sequence, Tuple


REQUIRED_COLUMNS = {
    "cohorts": {
        "cohort_id",
        "scope_kind",
        "window_kind",
        "window_start_ts",
        "window_end_ts",
        "member_count",
        "mint",
        "edge_density",
        "internal_flow_raw",
        "external_flow_raw",
        "cohort_score",
        "updated_at",
    },
    "cohort_members": {
        "cohort_id",
        "wallet",
        "role_hint",
        "inflow_raw",
        "outflow_raw",
        "degree_in",
        "degree_out",
    },
    "wallet_edges": {
        "window_kind",
        "window_start_ts",
        "window_end_ts",
        "mint",
        "src_wallet",
        "dst_wallet",
    },
    "wallet_token_flow": {
        "wallet",
        "mint",
        "window_kind",
        "window_start_ts",
        "window_end_ts",
    },
    "phase2_runs": {
        "run_id",
        "component",
        "window_kind",
        "window_start_ts",
        "window_end_ts",
        "input_counts_json",
        "output_counts_json",
        "created_at",
    },
}

WINDOW_KINDS = ("lifetime", "24h", "7d")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PANDA Phase 2.4 cohort metrics")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument(
        "--windows",
        default=",".join(WINDOW_KINDS),
        help="Comma-separated window kinds (default: lifetime,24h,7d)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute but do not write updates")
    parser.add_argument("--top-n", type=int, default=50, help="Top N cohorts to print")
    return parser.parse_args()


def get_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table});")
    return [row[1] for row in cur.fetchall()]


def ensure_required_columns(conn: sqlite3.Connection) -> Dict[str, List[str]]:
    existing = {}
    for table, required in REQUIRED_COLUMNS.items():
        cols = get_columns(conn, table)
        existing[table] = cols
        missing = sorted(set(required) - set(cols))
        if missing:
            print(f"ERROR: Missing required columns in {table}: {', '.join(missing)}")
            sys.exit(1)
    return existing


def fetch_windows(conn: sqlite3.Connection, window_kinds: Sequence[str]) -> List[Tuple[str, int, int]]:
    placeholders = ",".join("?" for _ in window_kinds)
    query = f"""
        SELECT DISTINCT window_kind, window_start_ts, window_end_ts
        FROM cohorts
        WHERE window_kind IN ({placeholders})
        ORDER BY window_kind, window_start_ts, window_end_ts
    """
    cur = conn.execute(query, tuple(window_kinds))
    return [(row[0], row[1], row[2]) for row in cur.fetchall()]


def coerce_windows(value: str) -> List[str]:
    parts = [v.strip() for v in value.split(",") if v.strip()]
    if not parts:
        return list(WINDOW_KINDS)
    return parts


def make_temp_member_flow(
    conn: sqlite3.Connection,
    window_kind: str,
    window_start: int,
    window_end: int,
    has_inflow: bool,
    has_outflow: bool,
) -> None:
    conn.execute("DROP TABLE IF EXISTS tmp_member_flow;")
    if has_inflow or has_outflow:
        inflow_expr = "SUM(wtf.inflow_raw)" if has_inflow else "0"
        outflow_expr = "SUM(wtf.outflow_raw)" if has_outflow else "0"
        query = f"""
            CREATE TEMP TABLE tmp_member_flow AS
            SELECT
                cm.cohort_id,
                cm.wallet,
                COALESCE({inflow_expr}, 0) AS inflow_raw,
                COALESCE({outflow_expr}, 0) AS outflow_raw
            FROM cohort_members cm
            JOIN cohorts c ON c.cohort_id = cm.cohort_id
            LEFT JOIN wallet_token_flow wtf
              ON wtf.wallet = cm.wallet
             AND wtf.window_kind = c.window_kind
             AND wtf.window_start_ts = c.window_start_ts
             AND wtf.window_end_ts = c.window_end_ts
             AND (c.mint IS NULL OR wtf.mint = c.mint)
            WHERE c.window_kind = ?
              AND c.window_start_ts = ?
              AND c.window_end_ts = ?
            GROUP BY cm.cohort_id, cm.wallet;
        """
        conn.execute(query, (window_kind, window_start, window_end))
    else:
        query = """
            CREATE TEMP TABLE tmp_member_flow AS
            SELECT
                cm.cohort_id,
                cm.wallet,
                0 AS inflow_raw,
                0 AS outflow_raw
            FROM cohort_members cm
            JOIN cohorts c ON c.cohort_id = cm.cohort_id
            WHERE c.window_kind = ?
              AND c.window_start_ts = ?
              AND c.window_end_ts = ?;
        """
        conn.execute(query, (window_kind, window_start, window_end))


def make_temp_degrees(
    conn: sqlite3.Connection,
    window_kind: str,
    window_start: int,
    window_end: int,
) -> None:
    conn.execute("DROP TABLE IF EXISTS tmp_degree_out;")
    conn.execute("DROP TABLE IF EXISTS tmp_degree_in;")
    out_query = """
        CREATE TEMP TABLE tmp_degree_out AS
        SELECT
            cm_src.cohort_id,
            cm_src.wallet,
            COUNT(*) AS degree_out
        FROM wallet_edges we
        JOIN cohort_members cm_src ON cm_src.wallet = we.src_wallet
        JOIN cohort_members cm_dst
          ON cm_dst.wallet = we.dst_wallet
         AND cm_dst.cohort_id = cm_src.cohort_id
        JOIN cohorts c ON c.cohort_id = cm_src.cohort_id
        WHERE c.window_kind = ?
          AND c.window_start_ts = ?
          AND c.window_end_ts = ?
          AND (c.mint IS NULL OR we.mint = c.mint)
        GROUP BY cm_src.cohort_id, cm_src.wallet;
    """
    in_query = """
        CREATE TEMP TABLE tmp_degree_in AS
        SELECT
            cm_dst.cohort_id,
            cm_dst.wallet,
            COUNT(*) AS degree_in
        FROM wallet_edges we
        JOIN cohort_members cm_src ON cm_src.wallet = we.src_wallet
        JOIN cohort_members cm_dst
          ON cm_dst.wallet = we.dst_wallet
         AND cm_dst.cohort_id = cm_src.cohort_id
        JOIN cohorts c ON c.cohort_id = cm_src.cohort_id
        WHERE c.window_kind = ?
          AND c.window_start_ts = ?
          AND c.window_end_ts = ?
          AND (c.mint IS NULL OR we.mint = c.mint)
        GROUP BY cm_dst.cohort_id, cm_dst.wallet;
    """
    conn.execute(out_query, (window_kind, window_start, window_end))
    conn.execute(in_query, (window_kind, window_start, window_end))


def update_members(
    conn: sqlite3.Connection,
    window_kind: str,
    window_start: int,
    window_end: int,
) -> int:
    update_query = """
        UPDATE cohort_members
        SET
            inflow_raw = COALESCE(
                (SELECT inflow_raw FROM tmp_member_flow t
                 WHERE t.cohort_id = cohort_members.cohort_id
                   AND t.wallet = cohort_members.wallet),
                0
            ),
            outflow_raw = COALESCE(
                (SELECT outflow_raw FROM tmp_member_flow t
                 WHERE t.cohort_id = cohort_members.cohort_id
                   AND t.wallet = cohort_members.wallet),
                0
            ),
            degree_out = COALESCE(
                (SELECT degree_out FROM tmp_degree_out t
                 WHERE t.cohort_id = cohort_members.cohort_id
                   AND t.wallet = cohort_members.wallet),
                0
            ),
            degree_in = COALESCE(
                (SELECT degree_in FROM tmp_degree_in t
                 WHERE t.cohort_id = cohort_members.cohort_id
                   AND t.wallet = cohort_members.wallet),
                0
            )
        WHERE cohort_id IN (
            SELECT cohort_id
            FROM cohorts
            WHERE window_kind = ?
              AND window_start_ts = ?
              AND window_end_ts = ?
        );
    """
    cur = conn.execute(update_query, (window_kind, window_start, window_end))
    return cur.rowcount


def update_role_hints(
    conn: sqlite3.Connection,
    window_kind: str,
    window_start: int,
    window_end: int,
) -> int:
    query = """
        UPDATE cohort_members
        SET role_hint = 'member'
        WHERE role_hint IS NULL
          AND cohort_id IN (
            SELECT cohort_id
            FROM cohorts
            WHERE window_kind = ?
              AND window_start_ts = ?
              AND window_end_ts = ?
              AND scope_kind = 'co_transfer_cc'
          );
    """
    cur = conn.execute(query, (window_kind, window_start, window_end))
    return cur.rowcount


def load_cohorts(
    conn: sqlite3.Connection,
    window_kind: str,
    window_start: int,
    window_end: int,
) -> List[sqlite3.Row]:
    query = """
        SELECT cohort_id, scope_kind, mint, member_count
        FROM cohorts
        WHERE window_kind = ?
          AND window_start_ts = ?
          AND window_end_ts = ?
        ORDER BY cohort_id;
    """
    cur = conn.execute(query, (window_kind, window_start, window_end))
    return cur.fetchall()


def load_internal_flow(conn: sqlite3.Connection, cohort_ids: Sequence[int]) -> Dict[int, int]:
    if not cohort_ids:
        return {}
    placeholders = ",".join("?" for _ in cohort_ids)
    query = f"""
        SELECT cohort_id, COALESCE(SUM(inflow_raw), 0) AS internal_flow_raw
        FROM cohort_members
        WHERE cohort_id IN ({placeholders})
        GROUP BY cohort_id;
    """
    cur = conn.execute(query, tuple(cohort_ids))
    return {row[0]: int(row[1]) for row in cur.fetchall()}


def load_internal_edges(
    conn: sqlite3.Connection,
    window_kind: str,
    window_start: int,
    window_end: int,
) -> Dict[int, int]:
    query = """
        SELECT cm_src.cohort_id, COUNT(*) AS edge_count
        FROM wallet_edges we
        JOIN cohort_members cm_src ON cm_src.wallet = we.src_wallet
        JOIN cohort_members cm_dst
          ON cm_dst.wallet = we.dst_wallet
         AND cm_dst.cohort_id = cm_src.cohort_id
        JOIN cohorts c ON c.cohort_id = cm_src.cohort_id
        WHERE c.window_kind = ?
          AND c.window_start_ts = ?
          AND c.window_end_ts = ?
          AND (c.mint IS NULL OR we.mint = c.mint)
        GROUP BY cm_src.cohort_id;
    """
    cur = conn.execute(query, (window_kind, window_start, window_end))
    return {row[0]: int(row[1]) for row in cur.fetchall()}


def compute_cohort_updates(
    cohorts: Iterable[sqlite3.Row],
    internal_flow: Dict[int, int],
    internal_edges: Dict[int, int],
    updated_at: int,
) -> List[Tuple[int, int, float, float, int]]:
    updates = []
    for cohort in cohorts:
        cohort_id = cohort["cohort_id"]
        scope_kind = cohort["scope_kind"]
        member_count = cohort["member_count"] or 0
        internal_flow_raw = internal_flow.get(cohort_id, 0)
        internal_edges_count = internal_edges.get(cohort_id, 0)
        edge_density = 0.0
        if scope_kind == "co_transfer_cc" and member_count > 1:
            denom = member_count * (member_count - 1)
            edge_density = internal_edges_count / float(denom)
        if internal_flow_raw <= 0 or member_count <= 0:
            cohort_score = 0.0
        else:
            cohort_score = (
                math.log1p(member_count)
                * math.log1p(internal_flow_raw)
                * (1.0 + edge_density)
            )
        updates.append(
            (
                internal_flow_raw,
                0,
                edge_density,
                cohort_score,
                updated_at,
                cohort_id,
            )
        )
    return updates


def apply_cohort_updates(conn: sqlite3.Connection, updates: Sequence[Tuple[int, int, float, float, int, int]]) -> None:
    query = """
        UPDATE cohorts
        SET internal_flow_raw = ?,
            external_flow_raw = ?,
            edge_density = ?,
            cohort_score = ?,
            updated_at = ?
        WHERE cohort_id = ?;
    """
    conn.executemany(query, updates)


def counts_for_window(conn: sqlite3.Connection, window_kind: str, window_start: int, window_end: int) -> Dict[str, int]:
    cohort_count = conn.execute(
        """
        SELECT COUNT(*) FROM cohorts
        WHERE window_kind = ? AND window_start_ts = ? AND window_end_ts = ?;
        """,
        (window_kind, window_start, window_end),
    ).fetchone()[0]
    member_count = conn.execute(
        """
        SELECT COUNT(*) FROM cohort_members
        WHERE cohort_id IN (
            SELECT cohort_id FROM cohorts
            WHERE window_kind = ? AND window_start_ts = ? AND window_end_ts = ?
        );
        """,
        (window_kind, window_start, window_end),
    ).fetchone()[0]
    edges_count = conn.execute(
        """
        SELECT COUNT(*) FROM wallet_edges
        WHERE window_kind = ? AND window_start_ts = ? AND window_end_ts = ?;
        """,
        (window_kind, window_start, window_end),
    ).fetchone()[0]
    flow_count = conn.execute(
        """
        SELECT COUNT(*) FROM wallet_token_flow
        WHERE window_kind = ? AND window_start_ts = ? AND window_end_ts = ?;
        """,
        (window_kind, window_start, window_end),
    ).fetchone()[0]
    return {
        "cohorts": int(cohort_count),
        "cohort_members": int(member_count),
        "wallet_edges": int(edges_count),
        "wallet_token_flow": int(flow_count),
    }


def insert_phase2_run(
    conn: sqlite3.Connection,
    window_kind: str,
    window_start: int,
    window_end: int,
    input_counts: Dict[str, int],
    output_counts: Dict[str, int],
    created_at: int,
) -> None:
    run_id = f"phase2_4:{window_kind}:{window_start}:{window_end}"
    query = """
        INSERT OR REPLACE INTO phase2_runs (
            run_id,
            component,
            window_kind,
            window_start_ts,
            window_end_ts,
            input_counts_json,
            output_counts_json,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """
    conn.execute(
        query,
        (
            run_id,
            "phase2_4",
            window_kind,
            window_start,
            window_end,
            json.dumps(input_counts, sort_keys=True),
            json.dumps(output_counts, sort_keys=True),
            created_at,
        ),
    )


def print_summary(
    window_kind: str,
    window_start: int,
    window_end: int,
    input_counts: Dict[str, int],
    cohorts_updated: int,
    members_updated: int,
    top_rows: List[Tuple],
) -> None:
    print(f"window={window_kind} start={window_start} end={window_end}")
    print(f"cohorts={input_counts['cohorts']} members={input_counts['cohort_members']}")
    print(f"updated cohorts={cohorts_updated} updated members={members_updated}")
    print("Top cohorts by cohort_score:")
    for row in top_rows:
        (
            cohort_id,
            scope_kind,
            mint,
            member_count,
            edge_density,
            internal_flow_raw,
            cohort_score,
        ) = row
        print(
            " ".join(
                [
                    f"cohort_id={cohort_id}",
                    f"scope_kind={scope_kind}",
                    f"mint={mint}",
                    f"member_count={member_count}",
                    f"edge_density={edge_density:.6f}",
                    f"internal_flow_raw={internal_flow_raw}",
                    f"cohort_score={cohort_score:.6f}",
                ]
            )
        )


def main() -> int:
    args = parse_args()
    window_kinds = coerce_windows(args.windows)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    ensure_required_columns(conn)

    existing_cols = get_columns(conn, "wallet_token_flow")
    has_inflow = "inflow_raw" in existing_cols
    has_outflow = "outflow_raw" in existing_cols

    windows = fetch_windows(conn, window_kinds)
    if not windows:
        print("No matching windows found in cohorts table.")
        return 0

    for window_kind, window_start, window_end in windows:
        input_counts = counts_for_window(conn, window_kind, window_start, window_end)
        cohorts = load_cohorts(conn, window_kind, window_start, window_end)
        cohort_ids = [row["cohort_id"] for row in cohorts]

        make_temp_member_flow(conn, window_kind, window_start, window_end, has_inflow, has_outflow)
        make_temp_degrees(conn, window_kind, window_start, window_end)

        members_updated = update_members(conn, window_kind, window_start, window_end)
        role_updates = update_role_hints(conn, window_kind, window_start, window_end)
        members_updated += role_updates

        internal_flow = load_internal_flow(conn, cohort_ids)
        internal_edges = load_internal_edges(conn, window_kind, window_start, window_end)
        updated_at = window_end
        updates = compute_cohort_updates(cohorts, internal_flow, internal_edges, updated_at)
        if not args.dry_run:
            apply_cohort_updates(conn, updates)

        cohorts_updated = len(updates) if not args.dry_run else len(updates)

        if not args.dry_run:
            output_counts = {
                "cohorts_updated": cohorts_updated,
                "members_updated": members_updated,
            }
            insert_phase2_run(
                conn,
                window_kind,
                window_start,
                window_end,
                input_counts,
                output_counts,
                updated_at,
            )
            conn.commit()
        else:
            conn.rollback()

        top_rows = sorted(
            [
                (
                    cohort["cohort_id"],
                    cohort["scope_kind"],
                    cohort["mint"],
                    cohort["member_count"] or 0,
                    update[2],
                    update[0],
                    update[3],
                )
                for cohort, update in zip(cohorts, updates)
            ],
            key=lambda row: (-row[6], row[0]),
        )[: args.top_n]

        print_summary(
            window_kind,
            window_start,
            window_end,
            input_counts,
            cohorts_updated,
            members_updated,
            top_rows,
        )

        conn.execute("DROP TABLE IF EXISTS tmp_member_flow;")
        conn.execute("DROP TABLE IF EXISTS tmp_degree_out;")
        conn.execute("DROP TABLE IF EXISTS tmp_degree_in;")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
