#!/usr/bin/env python3
import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
import sys
import uuid


ALLOWED_WINDOWS = {"24h", "7d", "lifetime"}
WINDOW_TOKENS = {
    ":24h:": "24h",
    ":7d:": "7d",
    ":lifetime:": "lifetime",
}


def get_table_columns(conn, table_name):
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    return [
        {
            "name": row[1],
            "type": row[2],
            "notnull": bool(row[3]),
            "default": row[4],
            "pk": bool(row[5]),
        }
        for row in cursor.fetchall()
    ]


def require_table(conn, table_name):
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    if cursor.fetchone() is None:
        raise RuntimeError(f"Required table missing: {table_name}")


def pick_wallet_column(columns, table_name):
    names = {col["name"] for col in columns}
    if "wallet" in names:
        return "wallet"
    if "scan_wallet" in names:
        return "scan_wallet"
    if "address" in names:
        return "address"
    raise RuntimeError(
        "Cannot find wallet identifier column in "
        f"{table_name}. Available columns: {sorted(names)}"
    )


def derive_windows_from_cohort_ids(cohort_ids):
    window_map = {}
    for cohort_id in cohort_ids:
        window = None
        for token, value in WINDOW_TOKENS.items():
            if token in cohort_id:
                window = value
                break
        if window is None:
            raise RuntimeError(
                "Cannot derive window from cohort_id without explicit token "
                f"(:24h:, :7d:, :lifetime:). Offending cohort_id: {cohort_id}"
            )
        window_map[cohort_id] = window
    return window_map


def stable_hash32(cohort_id_text):
    """
    Deterministic integer hash from cohort_id text.
    Uses first 8 hex chars of SHA256 as signed int32.
    """
    digest = hashlib.sha256(cohort_id_text.encode('utf-8')).hexdigest()
    return int(digest[:8], 16)


def sha256_file(path):
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def export_wallet_clusters(conn, export_path):
    cursor = conn.execute(
        """
        SELECT scan_wallet, window, cluster_id, created_at_utc
        FROM wallet_clusters
        ORDER BY window, cluster_id, scan_wallet
        """
    )
    rows = cursor.fetchall()
    with open(export_path, "w", encoding="utf-8") as handle:
        handle.write("scan_wallet\twindow\tcluster_id\tcreated_at_utc\n")
        for row in rows:
            handle.write("\t".join("" if value is None else str(value) for value in row) + "\n")
    digest = hashlib.sha256()
    with open(export_path, "rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest(), len(rows)


def compute_membership_stats(conn):
    """
    Compute min/max memberships (distinct cluster_id count) per wallet per window.
    """
    stats = {}
    cursor = conn.execute(
        """
        SELECT window, scan_wallet, COUNT(DISTINCT cluster_id) AS membership_count
        FROM wallet_clusters
        GROUP BY window, scan_wallet
        """
    )
    for row in cursor.fetchall():
        window = row[0]
        membership_count = row[2]
        if window not in stats:
            stats[window] = {"min": membership_count, "max": membership_count}
        else:
            stats[window]["min"] = min(stats[window]["min"], membership_count)
            stats[window]["max"] = max(stats[window]["max"], membership_count)
    return stats


def ensure_correct_schema(conn):
    """
    Ensure wallet_clusters has the correct schema without UNIQUE constraint on (scan_wallet, window).
    If the table exists with wrong constraints, recreate it.
    """
    # Check if table exists
    cursor = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='wallet_clusters'"
    )
    result = cursor.fetchone()
    
    if result is None:
        # Table doesn't exist, create it
        print("Creating wallet_clusters table with correct schema...")
        conn.execute("""
            CREATE TABLE wallet_clusters (
                scan_wallet TEXT NOT NULL,
                window TEXT NOT NULL,
                cluster_id INTEGER NOT NULL,
                created_at_utc INTEGER NOT NULL,
                PRIMARY KEY (scan_wallet, window, cluster_id)
            )
        """)
        conn.commit()
        return
    
    # Table exists, check if it has the problematic UNIQUE constraint
    create_sql = result[0].upper()
    
    # Check for problematic constraints
    has_wrong_constraint = False
    
    # Check indexes for UNIQUE on (scan_wallet, window)
    cursor = conn.execute("PRAGMA index_list(wallet_clusters)")
    for idx in cursor.fetchall():
        idx_name = idx[1]
        is_unique = idx[2]
        
        if is_unique:
            cursor2 = conn.execute(f"PRAGMA index_info({idx_name})")
            idx_cols = [col[2] for col in cursor2.fetchall()]
            
            # If there's a unique index on exactly (scan_wallet, window), it's wrong
            if set(idx_cols) == {"scan_wallet", "window"}:
                has_wrong_constraint = True
                print(f"Found problematic UNIQUE constraint: index {idx_name} on (scan_wallet, window)")
                break
    
    # Also check if PRIMARY KEY is only on (scan_wallet, window)
    if "PRIMARY KEY" in create_sql and "CLUSTER_ID" not in create_sql.split("PRIMARY KEY")[1].split(")")[0]:
        # Check what columns are in the PK
        cursor = conn.execute("PRAGMA table_info(wallet_clusters)")
        pk_cols = [col[1] for col in cursor.fetchall() if col[5] > 0]
        if set(pk_cols) == {"scan_wallet", "window"}:
            has_wrong_constraint = True
            print("Found problematic PRIMARY KEY on only (scan_wallet, window)")
    
    if has_wrong_constraint:
        print("Recreating wallet_clusters table with correct schema...")
        
        # Backup existing data if any
        cursor = conn.execute("SELECT COUNT(*) FROM wallet_clusters")
        existing_count = cursor.fetchone()[0]
        
        if existing_count > 0:
            print(f"Warning: Dropping table with {existing_count} existing rows")
            response = input("Continue? (yes/no): ")
            if response.lower() != "yes":
                raise RuntimeError("User aborted due to existing data")
        
        # Drop and recreate
        conn.execute("DROP TABLE wallet_clusters")
        conn.execute("""
            CREATE TABLE wallet_clusters (
                scan_wallet TEXT NOT NULL,
                window TEXT NOT NULL,
                cluster_id INTEGER NOT NULL,
                created_at_utc INTEGER NOT NULL,
                PRIMARY KEY (scan_wallet, window, cluster_id)
            )
        """)
        conn.commit()
        print("Table recreated successfully")
    else:
        print("wallet_clusters schema is correct")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--fresh", action="store_true")
    parser.add_argument("--fix-schema", action="store_true", 
                       help="Automatically fix schema without prompting")
    args = parser.parse_args()

    if not os.path.exists(args.db):
        raise RuntimeError(f"Database file not found: {args.db}")

    conn = sqlite3.connect(args.db)
    try:
        conn.row_factory = sqlite3.Row
        require_table(conn, "cohort_members")
        require_table(conn, "cohorts")
        require_table(conn, "swaps")
        
        # Ensure correct schema (may recreate table if needed)
        # For --fix-schema, we'll handle it non-interactively
        if args.fix_schema:
            cursor = conn.execute("SELECT COUNT(*) FROM wallet_clusters")
            existing_count = cursor.fetchone()[0]
            if existing_count > 0:
                print(f"--fix-schema: Dropping wallet_clusters table with {existing_count} rows")
                conn.execute("DROP TABLE IF EXISTS wallet_clusters")
        
        ensure_correct_schema(conn)
        require_table(conn, "wallet_clusters")

        cohort_member_columns = get_table_columns(conn, "cohort_members")
        cohort_columns = get_table_columns(conn, "cohorts")
        wallet_cluster_columns = get_table_columns(conn, "wallet_clusters")

        cohort_member_wallet_col = pick_wallet_column(cohort_member_columns, "cohort_members")
        
        # Verify wallet_clusters has required columns
        wallet_cluster_column_names = {col["name"] for col in wallet_cluster_columns}
        required_wc_columns = {"scan_wallet", "window", "cluster_id", "created_at_utc"}
        missing = required_wc_columns - wallet_cluster_column_names
        if missing:
            raise RuntimeError(f"wallet_clusters missing required columns: {missing}")

        cohort_member_column_names = {col["name"] for col in cohort_member_columns}
        cohort_column_names = {col["name"] for col in cohort_columns}

        if "cohort_id" not in cohort_member_column_names:
            raise RuntimeError("cohort_members must include cohort_id column.")
        if "cohort_id" not in cohort_column_names:
            raise RuntimeError("cohorts must include cohort_id column.")

        # Determine window derivation strategy
        if "window" in cohort_column_names:
            window_map = None
        else:
            cohort_ids = [
                row["cohort_id"]
                for row in conn.execute("SELECT cohort_id FROM cohorts").fetchall()
            ]
            window_map = derive_windows_from_cohort_ids(cohort_ids)

        max_time = conn.execute("SELECT MAX(block_time) FROM swaps").fetchone()[0]
        if max_time is None:
            raise RuntimeError("swaps.block_time max_time is NULL")
        created_at_value = int(max_time)

        if args.fresh:
            conn.execute("DELETE FROM wallet_clusters")
            conn.commit()

        # Build membership rows
        # Use a set to track exact triplets and skip true duplicates
        seen_triplets = set()
        rows_to_insert = []
        duplicates_skipped_triplet = 0
        
        cursor = conn.execute(
            f"""
            SELECT cm.{cohort_member_wallet_col} AS wallet, cm.cohort_id AS cohort_id
                   {'' if window_map is not None else ', c.window AS window'}
            FROM cohort_members cm
            JOIN cohorts c ON c.cohort_id = cm.cohort_id
            ORDER BY cm.cohort_id ASC, cm.{cohort_member_wallet_col} ASC
            """
        )
        
        for row in cursor.fetchall():
            cohort_id_text = row["cohort_id"]
            wallet_value = row["wallet"]
            
            if window_map is None:
                window_value = row["window"]
            else:
                window_value = window_map[cohort_id_text]
            
            # Compute stable integer cluster_id from cohort_id text
            cluster_id_int = stable_hash32(cohort_id_text)
            
            # Create triplet key for exact duplicate detection
            triplet = (wallet_value, window_value, cluster_id_int)
            
            if triplet in seen_triplets:
                duplicates_skipped_triplet += 1
                continue
            
            seen_triplets.add(triplet)
            rows_to_insert.append({
                "scan_wallet": wallet_value,
                "window": window_value,
                "cluster_id": cluster_id_int,
                "created_at_utc": created_at_value,
            })

        if not rows_to_insert:
            raise RuntimeError("No rows derived from cohort_members/cohorts.")

        # Insert all rows
        conn.executemany(
            """
            INSERT INTO wallet_clusters (scan_wallet, window, cluster_id, created_at_utc)
            VALUES (:scan_wallet, :window, :cluster_id, :created_at_utc)
            """,
            rows_to_insert,
        )
        conn.commit()

        # Validation checks
        rowcount = conn.execute("SELECT COUNT(*) FROM wallet_clusters").fetchone()[0]
        if rowcount <= 0:
            raise RuntimeError("wallet_clusters rowcount is zero after build.")
        
        if rowcount != len(rows_to_insert):
            raise RuntimeError(
                f"Rowcount mismatch: inserted {len(rows_to_insert)}, "
                f"found {rowcount} in table."
            )

        # Check for NULLs
        null_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM wallet_clusters
            WHERE scan_wallet IS NULL OR window IS NULL 
               OR cluster_id IS NULL OR created_at_utc IS NULL
            """
        ).fetchone()[0]
        if null_count > 0:
            raise RuntimeError(f"Found {null_count} rows with NULL values in wallet_clusters.")

        # Check window values
        window_values = [
            row[0]
            for row in conn.execute("SELECT DISTINCT window FROM wallet_clusters").fetchall()
        ]
        invalid_windows = [val for val in window_values if val not in ALLOWED_WINDOWS]
        if invalid_windows:
            raise RuntimeError(f"Invalid windows in wallet_clusters: {invalid_windows}")

        # Compute membership statistics
        membership_stats = compute_membership_stats(conn)

        # Window counts
        window_counts = conn.execute(
            """
            SELECT window, COUNT(*) AS row_count
            FROM wallet_clusters
            GROUP BY window
            ORDER BY window
            """
        ).fetchall()

        # Top clusters by membership size
        top_clusters = conn.execute(
            """
            SELECT cluster_id, COUNT(*) AS member_count
            FROM wallet_clusters
            GROUP BY cluster_id
            ORDER BY member_count DESC, cluster_id ASC
            LIMIT 5
            """
        ).fetchall()

        # Export
        export_dir = "exports_phase4_0_clusters"
        os.makedirs(export_dir, exist_ok=True)
        export_path = os.path.join(export_dir, "wallet_clusters.tsv")
        digest, export_count = export_wallet_clusters(conn, export_path)
        
        if export_count != rowcount:
            raise RuntimeError(
                "Export rowcount mismatch. "
                f"wallet_clusters={rowcount}, export={export_count}"
            )

        # Record run metadata
        started_at = dt.datetime.now(dt.timezone.utc).isoformat()
        run_id = str(uuid.uuid4())
        code_sha = sha256_file(__file__)

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS phase4_cluster_runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT,
                digest TEXT,
                rowcount INTEGER,
                code_sha256 TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO phase4_cluster_runs
                (run_id, started_at, digest, rowcount, code_sha256)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run_id, started_at, digest, rowcount, code_sha),
        )
        conn.commit()

        # Write manifest
        manifest_path = os.path.join(export_dir, "cluster_run_manifest.json")
        manifest = {
            "run_id": run_id,
            "started_at": started_at,
            "db": args.db,
            "digest": digest,
            "rowcount": rowcount,
            "code_sha256": code_sha,
            "created_at_utc_used": created_at_value,
            "duplicates_skipped_triplet": duplicates_skipped_triplet,
            "window_counts": {row["window"]: row["row_count"] for row in window_counts},
            "membership_stats": membership_stats,
            "top_clusters": [
                {"cluster_id": row["cluster_id"], "member_count": row["member_count"]}
                for row in top_clusters
            ],
        }
        with open(manifest_path, "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
            handle.write("\n")

        # Console output
        print(f"total_rows: {rowcount}")
        print(f"duplicates_skipped_triplet: {duplicates_skipped_triplet}")
        print()
        print("Window counts:")
        for row in window_counts:
            print(f"  {row['window']}: {row['row_count']} rows")
        print()
        print("Membership statistics (distinct cluster_id per wallet per window):")
        for window, stats in sorted(membership_stats.items()):
            print(f"  {window}: min={stats['min']}, max={stats['max']}")
        print()
        print("Top 5 clusters by member count:")
        for row in top_clusters:
            print(f"  cluster_id={row['cluster_id']}: {row['member_count']} members")
        print()
        print(f"created_at_utc_used: {created_at_value}")
        print(f"digest: {digest}")
        print(f"run_id: {run_id}")
        print(f"Export written to: {export_path}")
        print(f"Manifest written to: {manifest_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
