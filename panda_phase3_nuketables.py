#!/usr/bin/env python3
"""
PANDA v4 Phase 3 Gate 0A: DESTRUCTIVE Table Nuke Script
========================================================

THIS SCRIPT IS INTENTIONALLY DESTRUCTIVE.

Purpose: Clean-slate Phase 3 by dropping and recreating Phase-3 tables.
         NO INSPECTION. NO MERCY. REPLACE MODE ONLY.

Target tables:
  - wallet_features
  - wallet_clusters
  - wallet_edges

THIS WILL PERMANENTLY DESTROY ALL DATA IN THESE TABLES.
"""

import sqlite3
import argparse
import sys
from datetime import datetime, timezone


# ============================================================================
# CONSTANTS
# ============================================================================

TARGET_TABLES = [
    "wallet_features",
    "wallet_clusters",
    "wallet_edges",
]

EXPECTED_CONFIRMATION = "YES_DROP_PHASE3_TABLES"


# ============================================================================
# SCHEMA DEFINITIONS (MINIMAL, CLEAN)
# ============================================================================

SCHEMA_WALLET_FEATURES = """
CREATE TABLE wallet_features (
    scan_wallet TEXT NOT NULL,
    window TEXT NOT NULL,
    tx_count_total INTEGER NOT NULL,
    sol_volume_total INTEGER NOT NULL,
    created_at_utc INTEGER NOT NULL,
    PRIMARY KEY (scan_wallet, window)
)
"""

SCHEMA_WALLET_CLUSTERS = """
CREATE TABLE wallet_clusters (
    scan_wallet TEXT NOT NULL,
    window TEXT NOT NULL,
    cluster_id INTEGER NOT NULL,
    created_at_utc INTEGER NOT NULL,
    PRIMARY KEY (scan_wallet, window)
)
"""

SCHEMA_WALLET_EDGES = """
CREATE TABLE wallet_edges (
    src_wallet TEXT NOT NULL,
    dst_wallet TEXT NOT NULL,
    edge_type TEXT NOT NULL,
    weight INTEGER NOT NULL,
    window TEXT NOT NULL,
    created_at_utc INTEGER NOT NULL,
    PRIMARY KEY (src_wallet, dst_wallet, edge_type, window)
)
"""

SCHEMAS = {
    "wallet_features": SCHEMA_WALLET_FEATURES,
    "wallet_clusters": SCHEMA_WALLET_CLUSTERS,
    "wallet_edges": SCHEMA_WALLET_EDGES,
}


# ============================================================================
# BANNER
# ============================================================================

def print_banner(db_path: str):
    """Print large, loud warning banner."""
    now_utc = datetime.now(timezone.utc).isoformat()
    
    print("=" * 80)
    print("=" * 80)
    print("||")
    print("||  ███████╗ ████████╗  ██████╗  ██████╗")
    print("||  ██╔════╝ ╚══██╔══╝ ██╔═══██╗ ██╔══██╗")
    print("||  ███████╗    ██║    ██║   ██║ ██████╔╝")
    print("||  ╚════██║    ██║    ██║   ██║ ██╔═══╝")
    print("||  ███████║    ██║    ╚██████╔╝ ██║")
    print("||  ╚══════╝    ╚═╝     ╚═════╝  ╚═╝")
    print("||")
    print("||  THIS WILL PERMANENTLY DROP AND RECREATE PHASE-3 TABLES")
    print("||")
    print("||  Database: {}".format(db_path))
    print("||  UTC Time: {}".format(now_utc))
    print("||")
    print("||  Target tables:")
    for table in TARGET_TABLES:
        print("||    - {}".format(table))
    print("||")
    print("=" * 80)
    print("=" * 80)
    print()


# ============================================================================
# SQL SAFETY
# ============================================================================

def validate_identifier(identifier: str) -> str:
    """
    Validate SQL identifier to prevent injection.
    Only allows alphanumeric and underscore (safe for table names).
    """
    if not identifier.replace('_', '').isalnum():
        raise ValueError("Invalid SQL identifier: {}".format(identifier))
    return identifier


# ============================================================================
# EXISTENCE CHECK
# ============================================================================

def check_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Check if a table exists in the database."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return cursor.fetchone() is not None


def get_row_count(conn: sqlite3.Connection, table_name: str) -> int:
    """Get row count for a table."""
    cursor = conn.cursor()
    safe_table = validate_identifier(table_name)
    cursor.execute("SELECT COUNT(*) FROM {}".format(safe_table))
    return cursor.fetchone()[0]


def print_existence_check(conn: sqlite3.Connection):
    """Check and print existence status for all target tables."""
    print("EXISTENCE CHECK")
    print("-" * 80)
    
    for table in TARGET_TABLES:
        try:
            exists = check_table_exists(conn, table)
            if exists:
                row_count = get_row_count(conn, table)
                print("  {} → EXISTS (row_count={})".format(table, row_count))
            else:
                print("  {} → SKIP (table not found)".format(table))
        except Exception as e:
            print("  {} → ERROR: {}".format(table, e))
            sys.exit(1)
    
    print()


# ============================================================================
# CONFIRMATION
# ============================================================================

def require_confirmation():
    """Require explicit user confirmation. No bypass."""
    print("CONFIRMATION REQUIRED")
    print("-" * 80)
    print("Type EXACTLY: {}".format(EXPECTED_CONFIRMATION))
    print()
    
    user_input = input("Confirm: ").strip()
    
    if user_input != EXPECTED_CONFIRMATION:
        print()
        print("ABORT: Confirmation string does not match.")
        print("Expected: {}".format(EXPECTED_CONFIRMATION))
        print("Received: {}".format(user_input))
        print()
        sys.exit(1)
    
    print()
    print("Confirmation received. Proceeding with DESTRUCTION.")
    print()


# ============================================================================
# DROP PHASE
# ============================================================================

def drop_tables(conn: sqlite3.Connection):
    """Drop all target tables that exist."""
    print("DROP PHASE")
    print("-" * 80)
    
    for table in TARGET_TABLES:
        try:
            if check_table_exists(conn, table):
                print("  Dropping {} ...".format(table))
                safe_table = validate_identifier(table)
                conn.execute("DROP TABLE {}".format(safe_table))
                print("  ✓ {} DROPPED".format(table))
            else:
                print("  {} does not exist, skipping drop".format(table))
        except Exception as e:
            print("  ERROR dropping {}: {}".format(table, e))
            sys.exit(1)
    
    try:
        conn.commit()
    except Exception as e:
        print("  ERROR committing drops: {}".format(e))
        sys.exit(1)
    
    print()


# ============================================================================
# RECREATE PHASE
# ============================================================================

def recreate_tables(conn: sqlite3.Connection):
    """Recreate all target tables with clean, minimal schemas."""
    print("RECREATE PHASE")
    print("-" * 80)
    
    for table in TARGET_TABLES:
        try:
            print("  Creating {} ...".format(table))
            schema = SCHEMAS[table]
            conn.execute(schema)
            print("  ✓ {} CREATED (empty)".format(table))
        except Exception as e:
            print("  ERROR creating {}: {}".format(table, e))
            sys.exit(1)
    
    try:
        conn.commit()
    except Exception as e:
        print("  ERROR committing creates: {}".format(e))
        sys.exit(1)
    
    print()


# ============================================================================
# POST-CHECK
# ============================================================================

def print_table_schema(conn: sqlite3.Connection, table_name: str):
    """Print table schema using PRAGMA table_info."""
    cursor = conn.cursor()
    safe_table = validate_identifier(table_name)
    cursor.execute("PRAGMA table_info({})".format(safe_table))
    rows = cursor.fetchall()
    
    print("  Schema for {}:".format(table_name))
    for row in rows:
        cid, name, dtype, notnull, default, pk = row
        print("    [{}] {} {} {}{}".format(
            cid,
            name,
            dtype,
            "NOT NULL " if notnull else "",
            "PRIMARY KEY" if pk else ""
        ))


def post_check(conn: sqlite3.Connection):
    """Verify all tables exist, are empty, and print schemas."""
    print("POST-CHECK")
    print("-" * 80)
    
    all_ok = True
    
    for table in TARGET_TABLES:
        try:
            exists = check_table_exists(conn, table)
            if not exists:
                print("  ERROR: {} does not exist after recreation".format(table))
                all_ok = False
                continue
            
            row_count = get_row_count(conn, table)
            if row_count != 0:
                print("  ERROR: {} has row_count={}, expected 0".format(table, row_count))
                all_ok = False
                continue
            
            print("  ✓ {} exists, row_count=0".format(table))
            print_table_schema(conn, table)
            print()
        except Exception as e:
            print("  ERROR checking {}: {}".format(table, e))
            all_ok = False
    
    if not all_ok:
        print("POST-CHECK FAILED")
        sys.exit(1)
    
    print("POST-CHECK PASSED")
    print()


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="DESTRUCTIVE Phase 3 table nuke script for PANDA v4"
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to SQLite database (e.g., masterwalletsdb.db)"
    )
    
    args = parser.parse_args()
    db_path = args.db
    
    # Banner
    print_banner(db_path)
    
    # Connect
    try:
        conn = sqlite3.connect(db_path)
    except Exception as e:
        print("ERROR: Failed to connect to database: {}".format(e))
        sys.exit(1)
    
    # Existence check
    print_existence_check(conn)
    
    # Confirmation
    require_confirmation()
    
    # Drop
    drop_tables(conn)
    
    # Recreate
    recreate_tables(conn)
    
    # Post-check
    post_check(conn)
    
    # Close
    conn.close()
    
    print("=" * 80)
    print("PHASE 3 TABLE NUKE COMPLETE")
    print("=" * 80)
    print()
    
    sys.exit(0)


if __name__ == "__main__":
    main()
