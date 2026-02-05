#!/usr/bin/env python3
import argparse
import csv
import os
import sqlite3
import sys
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def connect_readonly(db_path: str) -> sqlite3.Connection:
    uri = f"file:{db_path}?mode=ro"
    try:
        return sqlite3.connect(uri, uri=True)
    except sqlite3.OperationalError:
        return sqlite3.connect(db_path)


def get_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


def get_table_info(conn: sqlite3.Connection, table: str) -> List[Tuple[str, str, int, int]]:
    # name, type, notnull, pk
    rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [(r[1], r[2], r[3], r[5]) for r in rows]


def write_tsv(path: str, headers: Sequence[str], rows: Iterable[Sequence[object]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)


def table_rowcount(conn: sqlite3.Connection, table: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM '{table}'").fetchone()
    return int(row[0]) if row else 0


def column_exists(columns: Dict[str, List[str]], table: str, column: str) -> bool:
    return column in columns.get(table, [])


def check_required_columns(
    columns: Dict[str, List[str]],
    required_map: Dict[str, List[str]],
) -> List[Tuple[str, str, int]]:
    results = []
    for table, required_cols in required_map.items():
        for col in required_cols:
            exists = 1 if column_exists(columns, table, col) else 0
            results.append((table, col, exists))
    return results


def compute_time_sanity(
    conn: sqlite3.Connection, columns: Dict[str, List[str]], table: str, time_cols: List[str]
) -> List[Tuple[str, str, int, Optional[object], Optional[object]]]:
    results = []
    for col in time_cols:
        if not column_exists(columns, table, col):
            continue
        row = conn.execute(
            f"SELECT SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END), MIN({col}), MAX({col}) FROM '{table}'"
        ).fetchone()
        nulls, min_time, max_time = row
        results.append((table, col, int(nulls or 0), min_time, max_time))
    return results


def compute_value_sanity(
    conn: sqlite3.Connection, columns: Dict[str, List[str]], table: str, value_cols: List[str]
) -> List[Tuple[str, str, int, Optional[object], Optional[object]]]:
    results = []
    for col in value_cols:
        if not column_exists(columns, table, col):
            continue
        row = conn.execute(
            f"SELECT SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END), MIN({col}), MAX({col}) FROM '{table}'"
        ).fetchone()
        nulls, min_val, max_val = row
        results.append((table, col, int(nulls or 0), min_val, max_val))
    return results


def resolve_alt_mapping(columns: Dict[str, List[str]], table: str, primary: str, alternates: List[str]) -> Optional[str]:
    if column_exists(columns, table, primary):
        return None
    for alt in alternates:
        if column_exists(columns, table, alt):
            return alt
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only SQLite inspection for Phase 4 preflight.")
    parser.add_argument("--db", required=True, help="Path to SQLite database.")
    parser.add_argument("--outdir", required=True, help="Output directory for TSV exports.")
    args = parser.parse_args()

    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)

    conn = connect_readonly(args.db)
    try:
        tables = get_tables(conn)
        table_set = set(tables)

        hard_requirements = {
            "swaps_or_wallet_token_flow": ("swaps", "wallet_token_flow"),
            "whale_transitions": ("whale_transitions",),
            "wallet_edges": ("wallet_edges",),
            "spl_transfers_v2_or_swaps": ("spl_transfers_v2", "swaps"),
        }

        missing_hard = []
        for name, options in hard_requirements.items():
            if not any(opt in table_set for opt in options):
                missing_hard.append((name, options))

        # Optional table
        wallets_missing = "wallets" not in table_set

        schema_tables_rows = []
        for table in tables:
            schema_tables_rows.append((table, table_rowcount(conn, table)))

        columns: Dict[str, List[str]] = {}
        schema_columns_rows = []
        for table in tables:
            info = get_table_info(conn, table)
            columns[table] = [r[0] for r in info]
            for name, col_type, notnull, pk in info:
                schema_columns_rows.append((table, name, col_type, notnull, pk))

        required_columns = {
            "swaps": ["scan_wallet", "signature", "block_time"],
            "spl_transfers_v2": [
                "scan_wallet",
                "signature",
                "block_time",
                "event_time",
                "amount_raw",
                "mint",
            ],
            "wallet_token_flow": [
                "wallet",
                "scan_wallet",
                "block_time",
                "event_time",
                "direction",
                "mint",
                "amount_raw",
                "signature",
            ],
            "wallet_edges": [
                "window",
                "src_wallet",
                "dst_wallet",
                "wallet",
                "counterparty",
                "edge_time",
                "block_time",
            ],
            "whale_transitions": [
                "wallet",
                "window",
                "side",
                "event_time",
                "amount_lamports",
                "supporting_flow_count",
                "flow_ref",
            ],
        }

        required_columns_rows = check_required_columns(columns, required_columns)

        time_sanity_rows: List[Tuple[str, str, int, Optional[object], Optional[object]]] = []
        time_candidates = ["block_time", "event_time", "edge_time"]
        for table in [
            "swaps",
            "spl_transfers_v2",
            "wallet_token_flow",
            "wallet_edges",
            "whale_transitions",
        ]:
            if table in table_set:
                time_sanity_rows.extend(compute_time_sanity(conn, columns, table, time_candidates))

        value_sanity_rows: List[Tuple[str, str, int, Optional[object], Optional[object]]] = []
        value_targets = {
            "spl_transfers_v2": ["amount_raw"],
            "wallet_token_flow": ["amount_raw"],
            "whale_transitions": ["amount_lamports", "supporting_flow_count"],
        }
        for table, cols in value_targets.items():
            if table in table_set:
                value_sanity_rows.extend(compute_value_sanity(conn, columns, table, cols))

        write_tsv(
            os.path.join(outdir, "schema_tables.tsv"),
            ["table_name", "rowcount"],
            schema_tables_rows,
        )
        write_tsv(
            os.path.join(outdir, "schema_columns.tsv"),
            ["table_name", "column_name", "type", "notnull", "pk"],
            schema_columns_rows,
        )
        write_tsv(
            os.path.join(outdir, "phase4_required_columns_check.tsv"),
            ["table_name", "required_column", "exists"],
            required_columns_rows,
        )
        write_tsv(
            os.path.join(outdir, "time_sanity.tsv"),
            ["table_name", "time_col", "nulls", "min_time", "max_time"],
            time_sanity_rows,
        )
        write_tsv(
            os.path.join(outdir, "value_sanity.tsv"),
            ["table_name", "col", "nulls", "min_val", "max_val"],
            value_sanity_rows,
        )

        # Console report
        print("Phase 4 preflight report")
        print("========================")

        if missing_hard:
            print("Missing required tables:")
            for name, options in missing_hard:
                print(f"- {name}: needs one of {', '.join(options)}")
        else:
            print("All required tables present (per hard requirements).")

        if wallets_missing:
            print("Optional table missing: wallets")

        # Canonical max_time source
        canonical_source = None
        if column_exists(columns, "swaps", "block_time"):
            canonical_source = "swaps.block_time"
        elif column_exists(columns, "spl_transfers_v2", "block_time"):
            canonical_source = "spl_transfers_v2.block_time"
        elif column_exists(columns, "spl_transfers_v2", "event_time"):
            canonical_source = "spl_transfers_v2.event_time"
        elif column_exists(columns, "wallet_token_flow", "block_time"):
            canonical_source = "wallet_token_flow.block_time"
        elif column_exists(columns, "wallet_token_flow", "event_time"):
            canonical_source = "wallet_token_flow.event_time"
        elif column_exists(columns, "whale_transitions", "event_time"):
            canonical_source = "whale_transitions.event_time"
        elif column_exists(columns, "wallet_edges", "edge_time"):
            canonical_source = "wallet_edges.edge_time"
        elif column_exists(columns, "wallet_edges", "block_time"):
            canonical_source = "wallet_edges.block_time"

        if canonical_source:
            print(f"Canonical max_time source: {canonical_source}")
        else:
            print("Canonical max_time source: none found")

        missing_columns = [
            (table, col)
            for table, col, exists in required_columns_rows
            if table in table_set and exists == 0
        ]
        if missing_columns:
            print("Missing required columns:")
            for table, col in missing_columns:
                print(f"- {table}.{col}")
        else:
            print("No missing required columns detected in existing tables.")

        print("Recommended column mappings:")
        mappings = []
        alt_map = {
            "spl_transfers_v2": {"block_time": ["event_time"]},
            "wallet_token_flow": {"wallet": ["scan_wallet"], "block_time": ["event_time"]},
            "wallet_edges": {
                "src_wallet": ["wallet"],
                "dst_wallet": ["counterparty"],
                "edge_time": ["block_time"],
            },
        }
        for table, mapping in alt_map.items():
            if table not in table_set:
                continue
            for primary, alts in mapping.items():
                alt = resolve_alt_mapping(columns, table, primary, alts)
                if alt:
                    mappings.append(f"- {table}.{primary} -> {table}.{alt}")
        if mappings:
            for item in mappings:
                print(item)
        else:
            print("- None")

        if missing_hard:
            return 1
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
