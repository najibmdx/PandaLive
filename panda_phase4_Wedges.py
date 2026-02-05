#!/usr/bin/env python3

import argparse
import hashlib
import json
import sqlite3
import sys
import time
from pathlib import Path


WINDOWS = {
    "24h": 86400,
    "7d": 604800,
    "lifetime": None,
}


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def fetch_max_time(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT MAX(block_time) FROM swaps").fetchone()
    if row is None or row[0] is None:
        raise RuntimeError("swaps table is empty; cannot determine max_time")
    return int(row[0])


def ensure_run_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS phase4_edge_runs (
            run_id TEXT PRIMARY KEY,
            started_at INTEGER NOT NULL,
            max_time INTEGER NOT NULL,
            digest TEXT NOT NULL,
            rowcount INTEGER NOT NULL,
            code_sha256 TEXT NOT NULL
        )
        """
    )


def build_edges_for_window(
    conn: sqlite3.Connection, window: str, max_time: int
) -> None:
    duration = WINDOWS[window]
    if duration is None:
        min_time = 0
    else:
        min_time = max_time - duration
    conn.execute(
        "DELETE FROM wallet_edges WHERE window = ? AND edge_type = 'co_token_window'",
        (window,),
    )
    conn.execute("DROP TABLE IF EXISTS wallet_mints")
    conn.execute(
        """
        CREATE TEMP TABLE wallet_mints AS
        SELECT DISTINCT scan_wallet AS wallet, token_mint
        FROM wallet_token_flow
        WHERE block_time BETWEEN ? AND ?
        """,
        (min_time, max_time),
    )
    conn.execute(
        """
        WITH pair_counts AS (
            SELECT
                a.wallet AS wallet_a,
                b.wallet AS wallet_b,
                COUNT(DISTINCT a.token_mint) AS weight
            FROM wallet_mints a
            JOIN wallet_mints b
                ON a.token_mint = b.token_mint
               AND a.wallet < b.wallet
            GROUP BY a.wallet, b.wallet
        )
        INSERT INTO wallet_edges (
            src_wallet,
            dst_wallet,
            edge_type,
            weight,
            window,
            created_at_utc
        )
        SELECT wallet_a, wallet_b, 'co_token_window', weight, ?, ? FROM pair_counts
        UNION ALL
        SELECT wallet_b, wallet_a, 'co_token_window', weight, ?, ? FROM pair_counts
        """,
        (window, max_time, window, max_time),
    )
    conn.execute("DROP TABLE wallet_mints")


def check_window_stats(conn: sqlite3.Connection, window: str) -> dict:
    rowcount = conn.execute(
        "SELECT COUNT(*) FROM wallet_edges WHERE window = ?", (window,)
    ).fetchone()[0]
    if rowcount == 0:
        if window in ("7d", "lifetime"):
            raise RuntimeError(f"window {window} produced zero rows")
        print("[INFO] window 24h produced zero rows; continuing (allowed)")
        return {
            "rowcount": 0,
            "distinct_src_wallets": 0,
            "distinct_dst_wallets": 0,
            "min_weight": None,
            "max_weight": None,
            "null_violations": 0,
            "window": window,
            "distinct_src": 0,
            "distinct_dst": 0,
            "top_weights": [],
        }
    distinct_src = conn.execute(
        "SELECT COUNT(DISTINCT src_wallet) FROM wallet_edges WHERE window = ?",
        (window,),
    ).fetchone()[0]
    distinct_dst = conn.execute(
        "SELECT COUNT(DISTINCT dst_wallet) FROM wallet_edges WHERE window = ?",
        (window,),
    ).fetchone()[0]
    min_weight = conn.execute(
        "SELECT MIN(weight) FROM wallet_edges WHERE window = ?", (window,)
    ).fetchone()[0]
    max_weight = conn.execute(
        "SELECT MAX(weight) FROM wallet_edges WHERE window = ?", (window,)
    ).fetchone()[0]
    top_weights = [
        row[0]
        for row in conn.execute(
            """
            SELECT weight
            FROM wallet_edges
            WHERE window = ?
            ORDER BY weight DESC
            LIMIT 5
            """,
            (window,),
        ).fetchall()
    ]
    if distinct_src == 0 or distinct_dst == 0:
        raise RuntimeError(f"window {window} has empty src/dst set")
    if min_weight is None or min_weight < 1:
        raise RuntimeError(f"window {window} has invalid weights")
    nulls = conn.execute(
        """
        SELECT COUNT(*)
        FROM wallet_edges
        WHERE window = ?
          AND (src_wallet IS NULL
               OR dst_wallet IS NULL
               OR edge_type IS NULL
               OR weight IS NULL
               OR window IS NULL
               OR created_at_utc IS NULL)
        """,
        (window,),
    ).fetchone()[0]
    if nulls != 0:
        raise RuntimeError(f"window {window} has NULL values")
    return {
        "rowcount": rowcount,
        "distinct_src_wallets": distinct_src,
        "distinct_dst_wallets": distinct_dst,
        "min_weight": min_weight,
        "max_weight": max_weight,
        "null_violations": nulls,
        "window": window,
        "distinct_src": distinct_src,
        "distinct_dst": distinct_dst,
        "top_weights": top_weights,
    }


def export_edges(conn: sqlite3.Connection, out_path: Path) -> int:
    rows = conn.execute(
        """
        SELECT src_wallet, dst_wallet, edge_type, weight, window, created_at_utc
        FROM wallet_edges
        ORDER BY window, edge_type, src_wallet, dst_wallet
        """
    )
    rowcount = 0
    with out_path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(
                "\t".join(str(value) for value in row)
                + "\n"
            )
            rowcount += 1
    return rowcount


def main() -> int:
    parser = argparse.ArgumentParser(description="Build wallet_edges from wallet_token_flow.")
    parser.add_argument("--db", required=True, help="Path to sqlite database")
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Delete existing wallet_edges before build",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise RuntimeError(f"Database not found: {db_path}")

    script_path = Path(__file__).resolve()
    code_sha256 = sha256_file(script_path)

    out_dir = Path("exports_phase4_0_edges")
    out_dir.mkdir(parents=True, exist_ok=True)

    run_id = hashlib.sha256(f"{int(time.time())}-{code_sha256}".encode("utf-8")).hexdigest()
    started_at = int(time.time())

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        max_time = fetch_max_time(conn)
        if args.fresh:
            conn.execute("DELETE FROM wallet_edges")

        for window in WINDOWS:
            build_edges_for_window(conn, window, max_time)

        ensure_run_table(conn)
        conn.commit()

        window_stats = {}
        for window in WINDOWS:
            window_stats[window] = check_window_stats(conn, window)

        tsv_path = out_dir / "wallet_edges.tsv"
        rowcount = export_edges(conn, tsv_path)
        digest = sha256_file(tsv_path)

        conn.execute(
            """
            INSERT INTO phase4_edge_runs (
                run_id, started_at, max_time, digest, rowcount, code_sha256
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (run_id, started_at, max_time, digest, rowcount, code_sha256),
        )
        conn.commit()

    manifest = {
        "run_id": run_id,
        "max_time": max_time,
        "rowcount": rowcount,
        "digest": digest,
        "code_sha256": code_sha256,
    }
    manifest_path = out_dir / "edge_run_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    for window, stats in window_stats.items():
        print(
            f"window={window} rowcount={stats['rowcount']} "
            f"distinct_src={stats['distinct_src']} "
            f"distinct_dst={stats['distinct_dst']} "
            f"top5_weights={stats['top_weights']}"
        )
    print(f"digest={digest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
