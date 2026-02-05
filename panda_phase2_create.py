import argparse
import sqlite3
from typing import Dict, List, Optional, Tuple

# Doctrine:
# - Allowed window_kind values are EXACTLY: 'lifetime', '24h', '7d'
# - net_amount_raw = in_amount_raw - out_amount_raw is a mandatory invariant (enforced via QA)
# - top_in_counterparty / top_out_counterparty store wallet address only (no JSON or "wallet:amount")
# - Phase 2 tables are derived and may be dropped/recreated ONLY when empty and ONLY with --recreate-empty

WINDOW_KIND_VALUES = ("lifetime", "24h", "7d")

TABLES = [
    (
        "wallet_token_flow",
        """
        CREATE TABLE IF NOT EXISTS wallet_token_flow (
            wallet TEXT NOT NULL,
            mint TEXT NOT NULL,
            window_kind TEXT NOT NULL,
            window_start_ts INTEGER NOT NULL,
            window_end_ts INTEGER NOT NULL,
            in_amount_raw INTEGER NOT NULL DEFAULT 0,
            out_amount_raw INTEGER NOT NULL DEFAULT 0,
            net_amount_raw INTEGER NOT NULL DEFAULT 0,
            in_tx_count INTEGER NOT NULL DEFAULT 0,
            out_tx_count INTEGER NOT NULL DEFAULT 0,
            unique_senders INTEGER NOT NULL DEFAULT 0,
            unique_receivers INTEGER NOT NULL DEFAULT 0,
            first_seen_ts INTEGER,
            last_seen_ts INTEGER,
            top_in_counterparty TEXT,
            top_out_counterparty TEXT,
            top_in_amount_raw INTEGER,
            top_out_amount_raw INTEGER,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (wallet, mint, window_kind, window_start_ts, window_end_ts),
            CHECK (window_kind IN ('lifetime','24h','7d'))
        )
        """,
    ),
    (
        "wallet_edges",
        """
        CREATE TABLE IF NOT EXISTS wallet_edges (
            src_wallet TEXT NOT NULL,
            dst_wallet TEXT NOT NULL,
            mint TEXT NOT NULL,
            window_kind TEXT NOT NULL,
            window_start_ts INTEGER NOT NULL,
            window_end_ts INTEGER NOT NULL,
            tx_count INTEGER NOT NULL DEFAULT 0,
            amount_raw INTEGER NOT NULL DEFAULT 0,
            first_seen_ts INTEGER,
            last_seen_ts INTEGER,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (src_wallet, dst_wallet, mint, window_kind, window_start_ts, window_end_ts),
            CHECK (src_wallet <> dst_wallet),
            CHECK (window_kind IN ('lifetime','24h','7d'))
        )
        """,
    ),
    (
        "cohorts",
        """
        CREATE TABLE IF NOT EXISTS cohorts (
            cohort_id TEXT PRIMARY KEY,
            mint TEXT,
            scope_kind TEXT NOT NULL,
            window_kind TEXT NOT NULL,
            window_start_ts INTEGER NOT NULL,
            window_end_ts INTEGER NOT NULL,
            member_count INTEGER NOT NULL,
            edge_density REAL,
            internal_flow_raw INTEGER,
            external_flow_raw INTEGER,
            cohort_score REAL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            CHECK (window_kind IN ('lifetime','24h','7d'))
        )
        """,
    ),
    (
        "cohort_members",
        """
        CREATE TABLE IF NOT EXISTS cohort_members (
            cohort_id TEXT NOT NULL,
            wallet TEXT NOT NULL,
            role_hint TEXT,
            inflow_raw INTEGER,
            outflow_raw INTEGER,
            degree_in INTEGER,
            degree_out INTEGER,
            PRIMARY KEY (cohort_id, wallet),
            FOREIGN KEY (cohort_id) REFERENCES cohorts(cohort_id) ON DELETE CASCADE
        )
        """,
    ),
    (
        "recycling_flags",
        """
        CREATE TABLE IF NOT EXISTS recycling_flags (
            wallet TEXT NOT NULL,
            mint TEXT NOT NULL,
            window_kind TEXT NOT NULL,
            window_start_ts INTEGER NOT NULL,
            window_end_ts INTEGER NOT NULL,
            pattern_kind TEXT NOT NULL,
            pattern_id TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            severity REAL NOT NULL,
            first_seen_ts INTEGER,
            last_seen_ts INTEGER,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (wallet, mint, window_kind, window_start_ts, window_end_ts, pattern_kind, pattern_id),
            CHECK (window_kind IN ('lifetime','24h','7d'))
        )
        """,
    ),
    (
        "whale_events",
        """
        CREATE TABLE IF NOT EXISTS whale_events (
            event_id TEXT PRIMARY KEY,
            wallet TEXT NOT NULL,
            mint TEXT NOT NULL,
            ts INTEGER NOT NULL,
            signature TEXT,
            ix_index INTEGER,
            direction TEXT NOT NULL,
            amount_raw INTEGER NOT NULL,
            rule_kind TEXT NOT NULL,
            threshold_raw INTEGER NOT NULL,
            window_start_ts INTEGER,
            window_end_ts INTEGER,
            notes TEXT,
            created_at INTEGER NOT NULL
        )
        """,
    ),
    (
        "whale_states",
        """
        CREATE TABLE IF NOT EXISTS whale_states (
            wallet TEXT NOT NULL,
            mint TEXT NOT NULL,
            state_kind TEXT NOT NULL,
            window_kind TEXT NOT NULL,
            window_start_ts INTEGER NOT NULL,
            window_end_ts INTEGER NOT NULL,
            is_active INTEGER NOT NULL,
            entered_at INTEGER,
            last_event_at INTEGER,
            last_amount_raw INTEGER,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (wallet, mint, state_kind, window_kind, window_start_ts, window_end_ts),
            CHECK (window_kind IN ('lifetime','24h','7d'))
        )
        """,
    ),
    (
        "wallet_features",
        """
        CREATE TABLE IF NOT EXISTS wallet_features (
            wallet TEXT NOT NULL,
            window_kind TEXT NOT NULL,
            window_start_ts INTEGER NOT NULL,
            window_end_ts INTEGER NOT NULL,
            token_diversity INTEGER NOT NULL DEFAULT 0,
            transfer_tx_count INTEGER NOT NULL DEFAULT 0,
            swap_tx_count INTEGER NOT NULL DEFAULT 0,
            transfer_volume_raw INTEGER NOT NULL DEFAULT 0,
            swap_volume_sol_lamports INTEGER NOT NULL DEFAULT 0,
            unique_counterparties INTEGER NOT NULL DEFAULT 0,
            active_days INTEGER NOT NULL DEFAULT 0,
            burst_score REAL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (wallet, window_kind, window_start_ts, window_end_ts),
            CHECK (window_kind IN ('lifetime','24h','7d'))
        )
        """,
    ),
    (
        "wallet_clusters",
        """
        CREATE TABLE IF NOT EXISTS wallet_clusters (
            cluster_run_id TEXT NOT NULL,
            wallet TEXT NOT NULL,
            cluster_id INTEGER NOT NULL,
            score_to_centroid REAL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (cluster_run_id, wallet)
        )
        """,
    ),
    (
        "cluster_runs",
        """
        CREATE TABLE IF NOT EXISTS cluster_runs (
            cluster_run_id TEXT PRIMARY KEY,
            window_kind TEXT NOT NULL,
            window_start_ts INTEGER NOT NULL,
            window_end_ts INTEGER NOT NULL,
            algo TEXT NOT NULL,
            params_json TEXT NOT NULL,
            wallet_count INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            CHECK (window_kind IN ('lifetime','24h','7d'))
        )
        """,
    ),
    (
        "phase2_runs",
        """
        CREATE TABLE IF NOT EXISTS phase2_runs (
            run_id TEXT PRIMARY KEY,
            component TEXT NOT NULL,
            window_kind TEXT NOT NULL,
            window_start_ts INTEGER NOT NULL,
            window_end_ts INTEGER NOT NULL,
            input_counts_json TEXT NOT NULL,
            output_counts_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            CHECK (window_kind IN ('lifetime','24h','7d'))
        )
        """,
    ),
]

INDEXES = [
    (
        "idx_wtf_wallet_window",
        "CREATE INDEX IF NOT EXISTS idx_wtf_wallet_window ON wallet_token_flow(wallet, window_kind, window_end_ts)",
    ),
    (
        "idx_wtf_mint_window",
        "CREATE INDEX IF NOT EXISTS idx_wtf_mint_window ON wallet_token_flow(mint, window_kind, window_end_ts)",
    ),
    (
        "idx_wtf_net",
        "CREATE INDEX IF NOT EXISTS idx_wtf_net ON wallet_token_flow(window_kind, window_end_ts, net_amount_raw)",
    ),
    (
        "idx_edges_src_window",
        "CREATE INDEX IF NOT EXISTS idx_edges_src_window ON wallet_edges(src_wallet, window_kind, window_end_ts)",
    ),
    (
        "idx_edges_dst_window",
        "CREATE INDEX IF NOT EXISTS idx_edges_dst_window ON wallet_edges(dst_wallet, window_kind, window_end_ts)",
    ),
    (
        "idx_edges_mint_window",
        "CREATE INDEX IF NOT EXISTS idx_edges_mint_window ON wallet_edges(mint, window_kind, window_end_ts)",
    ),
    (
        "idx_edges_weight",
        "CREATE INDEX IF NOT EXISTS idx_edges_weight ON wallet_edges(window_kind, window_end_ts, amount_raw DESC)",
    ),
    (
        "idx_cohorts_scope",
        "CREATE INDEX IF NOT EXISTS idx_cohorts_scope ON cohorts(scope_kind, mint, window_kind, window_end_ts)",
    ),
    (
        "idx_cohorts_score",
        "CREATE INDEX IF NOT EXISTS idx_cohorts_score ON cohorts(window_kind, window_end_ts, cohort_score DESC)",
    ),
    (
        "idx_cohort_members_wallet",
        "CREATE INDEX IF NOT EXISTS idx_cohort_members_wallet ON cohort_members(wallet)",
    ),
    (
        "idx_recycle_wallet_window",
        "CREATE INDEX IF NOT EXISTS idx_recycle_wallet_window ON recycling_flags(wallet, window_kind, window_end_ts)",
    ),
    (
        "idx_recycle_mint_window",
        "CREATE INDEX IF NOT EXISTS idx_recycle_mint_window ON recycling_flags(mint, window_kind, window_end_ts)",
    ),
    (
        "idx_recycle_severity",
        "CREATE INDEX IF NOT EXISTS idx_recycle_severity ON recycling_flags(window_kind, window_end_ts, severity DESC)",
    ),
    (
        "idx_whale_events_wallet_ts",
        "CREATE INDEX IF NOT EXISTS idx_whale_events_wallet_ts ON whale_events(wallet, ts)",
    ),
    (
        "idx_whale_events_mint_ts",
        "CREATE INDEX IF NOT EXISTS idx_whale_events_mint_ts ON whale_events(mint, ts)",
    ),
    (
        "idx_whale_events_rule",
        "CREATE INDEX IF NOT EXISTS idx_whale_events_rule ON whale_events(rule_kind, ts)",
    ),
    (
        "idx_whale_states_active",
        "CREATE INDEX IF NOT EXISTS idx_whale_states_active ON whale_states(state_kind, is_active, updated_at)",
    ),
    (
        "idx_wfeat_window",
        "CREATE INDEX IF NOT EXISTS idx_wfeat_window ON wallet_features(window_kind, window_end_ts)",
    ),
    (
        "idx_wfeat_wallet_window",
        "CREATE INDEX IF NOT EXISTS idx_wfeat_wallet_window ON wallet_features(wallet, window_kind, window_end_ts)",
    ),
    (
        "idx_wclusters_cluster",
        "CREATE INDEX IF NOT EXISTS idx_wclusters_cluster ON wallet_clusters(cluster_run_id, cluster_id)",
    ),
    (
        "idx_phase2_runs_component",
        "CREATE INDEX IF NOT EXISTS idx_phase2_runs_component ON phase2_runs(component, window_kind, window_end_ts)",
    ),
]

TABLE_ORDER = [
    "wallet_token_flow",
    "wallet_edges",
    "cohorts",
    "cohort_members",
    "recycling_flags",
    "whale_events",
    "whale_states",
    "wallet_clusters",
    "wallet_features",
    "phase2_runs",
    "cluster_runs",
]

DROP_ORDER = [
    "cohort_members",
    "cohorts",
    "wallet_token_flow",
    "wallet_edges",
    "recycling_flags",
    "whale_events",
    "whale_states",
    "wallet_clusters",
    "wallet_features",
    "phase2_runs",
    "cluster_runs",
]


def object_exists(conn: sqlite3.Connection, obj_type: str, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = ? AND name = ?",
        (obj_type, name),
    ).fetchone()
    return row is not None


def table_rowcount(conn: sqlite3.Connection, table_name: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def ensure_schema(
    conn: sqlite3.Connection,
    recreate_empty: bool,
) -> Tuple[List[Dict[str, object]], List[Tuple[str, str]], List[str]]:
    table_lookup = {name: statement for name, statement in TABLES}
    table_results: List[Dict[str, object]] = []
    non_empty_tables: List[str] = []

    table_state: Dict[str, Dict[str, object]] = {}
    for name in TABLE_ORDER:
        exists = object_exists(conn, "table", name)
        rows = table_rowcount(conn, name) if exists else None
        table_state[name] = {"exists": exists, "rows": rows, "dropped": False}
        if recreate_empty and exists and rows not in (None, 0):
            non_empty_tables.append(name)

    if recreate_empty:
        conn.execute("BEGIN")
        try:
            for name in DROP_ORDER:
                state = table_state[name]
                if not state["exists"]:
                    continue
                if state["rows"] == 0:
                    conn.execute(f"DROP TABLE IF EXISTS {name}")
                    state["dropped"] = True
            conn.execute("COMMIT")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

    for name in TABLE_ORDER:
        existed_before = table_state[name]["exists"]
        dropped = table_state[name]["dropped"]
        statement = table_lookup[name]
        conn.execute(statement)
        recreated = dropped
        created = (not existed_before) or dropped
        table_results.append(
            {
                "name": name,
                "exists": existed_before,
                "rows": table_state[name]["rows"],
                "dropped": dropped,
                "recreated": recreated,
                "created": created,
            }
        )

    index_results: List[Tuple[str, str]] = []
    for name, statement in INDEXES:
        existed = object_exists(conn, "index", name)
        conn.execute(statement)
        status = "already exists" if existed else "created"
        index_results.append((name, status))

    return table_results, index_results, non_empty_tables


def format_rowcount(value: Optional[int]) -> str:
    if value is None:
        return "n/a"
    return str(value)


def main() -> int:
    parser = argparse.ArgumentParser(description="Create Phase 2 derived tables.")
    parser.add_argument("--db", required=True, help="Path to SQLite database.")
    parser.add_argument(
        "--recreate-empty",
        action="store_true",
        default=False,
        help="Drop and recreate Phase 2 tables only if they exist and are empty.",
    )
    args = parser.parse_args()

    print(f"DB: {args.db}")
    print(f"Recreate empty: {args.recreate_empty}")

    conn = sqlite3.connect(args.db)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        tables, indexes, non_empty_tables = ensure_schema(conn, args.recreate_empty)
        conn.commit()
    except sqlite3.Error as exc:
        print(f"Error: {exc}")
        return 1
    finally:
        conn.close()

    if args.recreate_empty:
        for name in non_empty_tables:
            print(f"Table {name} not empty; skipping drop")

    for table in tables:
        print(
            "Table {name}: exists={exists} rows={rows} dropped={dropped} recreated={recreated}".format(
                name=table["name"],
                exists=table["exists"],
                rows=format_rowcount(table["rows"]),
                dropped=table["dropped"],
                recreated=table["recreated"],
            )
        )

    print("Indexes:")
    for name, status in indexes:
        print(f"  {name} ({status})")

    print("Phase 2 schema ready")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
