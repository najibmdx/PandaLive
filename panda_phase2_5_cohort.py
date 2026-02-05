#!/usr/bin/env python3
"""
Phase 2.5: Cohort Capital Activation & Dominance Classification.
"""

import argparse
import json
import math
import sqlite3
from typing import Dict, Iterable, List, Sequence, Tuple

REQUIRED_COHORT_COLUMNS = {
    "cohort_id",
    "member_count",
    "edge_density",
    "internal_flow_raw",
    "cohort_score",
    "window_kind",
    "window_start_ts",
    "window_end_ts",
}


def parse_windows(value: str) -> List[str]:
    windows = [item.strip() for item in value.split(",") if item.strip()]
    return windows if windows else ["lifetime", "7d", "24h"]


def fetch_table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [row[1] for row in rows]


def validate_schema(conn: sqlite3.Connection) -> None:
    columns = set(fetch_table_columns(conn, "cohorts"))
    missing = sorted(REQUIRED_COHORT_COLUMNS - columns)
    if missing:
        missing_str = ", ".join(missing)
        raise RuntimeError(f"Missing required columns in cohorts: {missing_str}")


def load_windows(
    conn: sqlite3.Connection,
    windows: Sequence[str],
) -> List[Tuple[str, int, int]]:
    placeholders = ",".join("?" for _ in windows)
    sql = f"""
        SELECT DISTINCT window_kind, window_start_ts, window_end_ts
        FROM cohorts
        WHERE window_kind IN ({placeholders})
        ORDER BY window_kind, window_start_ts, window_end_ts
    """
    rows = conn.execute(sql, list(windows)).fetchall()
    return [(row[0], int(row[1]), int(row[2])) for row in rows]


def classify_activation(
    scope_kind: str,
    member_count: int,
    edge_density: float,
    internal_flow_raw: int,
    min_flow_raw: int,
) -> str:
    if internal_flow_raw == 0:
        if member_count >= 3 and (edge_density > 0 or scope_kind == "hub_orbit"):
            return "structural_only"
        return "inactive"
    if internal_flow_raw >= min_flow_raw:
        return "capital_active"
    return "inactive"


def compute_dominance_score(member_count: int, edge_density: float, internal_flow_raw: int) -> float:
    base = member_count * (1.0 + edge_density)
    if internal_flow_raw == 0:
        return base
    return base * math.log1p(internal_flow_raw)


def fetch_cohorts_for_window(
    conn: sqlite3.Connection,
    window_kind: str,
    window_start_ts: int,
    window_end_ts: int,
) -> List[sqlite3.Row]:
    sql = """
        SELECT cohort_id,
               scope_kind,
               mint,
               member_count,
               edge_density,
               internal_flow_raw
        FROM cohorts
        WHERE window_kind = ?
          AND window_start_ts = ?
          AND window_end_ts = ?
    """
    return conn.execute(sql, [window_kind, window_start_ts, window_end_ts]).fetchall()


def update_cohort_scores(
    conn: sqlite3.Connection,
    updates: Iterable[Tuple[float, int, str, int, int, str]],
) -> int:
    sql = """
        UPDATE cohorts
        SET cohort_score = ?,
            updated_at = ?
        WHERE cohort_id = ?
          AND window_start_ts = ?
          AND window_end_ts = ?
          AND window_kind = ?
    """
    cur = conn.executemany(sql, updates)
    return cur.rowcount


def log_phase2_run(
    conn: sqlite3.Connection,
    run_id: str,
    window_kind: str,
    window_start_ts: int,
    window_end_ts: int,
    input_counts: Dict[str, int],
    output_counts: Dict[str, int],
) -> None:
    sql = """
        INSERT OR REPLACE INTO phase2_runs
            (
                run_id,
                component,
                window_kind,
                window_start_ts,
                window_end_ts,
                input_counts_json,
                output_counts_json,
                created_at
            )
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?)
    """
    conn.execute(
        sql,
        [
            run_id,
            "phase2_5",
            window_kind,
            window_start_ts,
            window_end_ts,
            json.dumps(input_counts, sort_keys=True),
            json.dumps(output_counts, sort_keys=True),
            window_end_ts,
        ],
    )


def print_window_report(
    window_kind: str,
    window_start_ts: int,
    window_end_ts: int,
    counts: Dict[str, int],
    top_rows: List[Tuple[sqlite3.Row, float]],
) -> None:
    print(f"Window: {window_kind} start={window_start_ts} end={window_end_ts}")
    print(
        "Counts: inactive={inactive} structural_only={structural_only} capital_active={capital_active}".format(
            **counts
        )
    )
    print("Top cohorts:")
    for row, score in top_rows:
        member_count = int(row[3] or 0)
        edge_density = float(row[4] or 0)
        internal_flow_raw = int(row[5] or 0)
        print(
            "  cohort_id={cohort_id} scope_kind={scope_kind} mint={mint} member_count={member_count} "
            "edge_density={edge_density:.6f} internal_flow_raw={internal_flow_raw} dominance_score={score:.6f}".format(
                cohort_id=row[0],
                scope_kind=row[1],
                mint=row[2],
                member_count=member_count,
                edge_density=edge_density,
                internal_flow_raw=internal_flow_raw,
                score=score,
            )
        )


def run_phase(
    conn: sqlite3.Connection,
    windows: Sequence[str],
    min_flow_raw: int,
    top_n: int,
) -> None:
    conn.row_factory = sqlite3.Row
    validate_schema(conn)

    window_sets = load_windows(conn, windows)
    for window_kind, window_start_ts, window_end_ts in window_sets:
        rows = fetch_cohorts_for_window(conn, window_kind, window_start_ts, window_end_ts)

        counts = {"inactive": 0, "structural_only": 0, "capital_active": 0}
        updates: List[Tuple[float, int, str, int, int, str]] = []
        scored_rows: List[Tuple[sqlite3.Row, float]] = []

        for row in rows:
            member_count = int(row[3] or 0)
            edge_density = float(row[4] or 0)
            internal_flow_raw = int(row[5] or 0)

            activation_state = classify_activation(
                str(row[1]),
                member_count,
                edge_density,
                internal_flow_raw,
                min_flow_raw,
            )
            counts[activation_state] += 1

            dominance_score = compute_dominance_score(member_count, edge_density, internal_flow_raw)
            updates.append(
                (
                    dominance_score,
                    window_end_ts,
                    str(row[0]),
                    window_start_ts,
                    window_end_ts,
                    window_kind,
                )
            )
            scored_rows.append((row, dominance_score))

        updated_count = update_cohort_scores(conn, updates)

        scored_rows.sort(key=lambda item: (-item[1], str(item[0][0])))
        top_rows = scored_rows[:top_n]
        print_window_report(window_kind, window_start_ts, window_end_ts, counts, top_rows)

        run_id = f"phase2_5:{window_kind}:{window_start_ts}:{window_end_ts}"
        input_counts = {
            "cohorts": len(rows),
            "inactive": counts["inactive"],
            "structural_only": counts["structural_only"],
            "capital_active": counts["capital_active"],
        }
        output_counts = {"updated_cohorts": updated_count}
        log_phase2_run(
            conn,
            run_id,
            window_kind,
            window_start_ts,
            window_end_ts,
            input_counts,
            output_counts,
        )

    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 2.5 Cohort Activation & Dominance Classification")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--windows", default="lifetime,7d,24h", help="Comma-delimited windows to process")
    parser.add_argument("--min-flow-raw", type=int, default=1, help="Minimum flow threshold")
    parser.add_argument("--top-n", type=int, default=30, help="Top N cohorts to print")
    args = parser.parse_args()

    windows = parse_windows(args.windows)
    if not windows:
        raise SystemExit("No windows specified")

    if args.min_flow_raw < 1:
        raise SystemExit("--min-flow-raw must be >= 1")

    with sqlite3.connect(args.db) as conn:
        try:
            run_phase(conn, windows, args.min_flow_raw, args.top_n)
        except RuntimeError as exc:
            print(str(exc))
            raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
