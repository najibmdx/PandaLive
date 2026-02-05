#!/usr/bin/env python3
"""
Phase 4.7 - Pattern coverage remediation audit (NULL pattern_id coverage).

Usage:
    python panda_phase4_7_null.py --db masterwalletsdb.db --outdir .
"""

import argparse
import hashlib
import os
import re
import sqlite3
import sys

REQUIRED_WINDOWS = ["24h", "7d", "lifetime"]
FEATURE_PREFIXES = [f"N{i}_" for i in range(1, 10)]
FEATURE_REGEX = re.compile(r"^N[1-9]_")
EXCLUDED_FEATURE_COLUMNS = {"wallet", "window", "run_id", "created_at"}


def discover_columns(cursor, table_name):
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cursor.fetchall()]


def ensure_table(cursor, table_name):
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    if cursor.fetchone() is None:
        print(f"ERROR: required table missing: {table_name}")
        sys.exit(1)


def validate_required_columns(table_name, columns, required):
    missing = [col for col in required if col not in columns]
    if missing:
        print(f"ERROR: {table_name} missing required columns: {', '.join(missing)}")
        sys.exit(1)


def validate_feature_prefixes(columns):
    missing_prefixes = []
    for prefix in FEATURE_PREFIXES:
        if not any(col.startswith(prefix) for col in columns):
            missing_prefixes.append(prefix)
    if missing_prefixes:
        present = ", ".join(columns)
        missing = ", ".join(missing_prefixes)
        print(
            "ERROR: phase4_features_norm missing required feature prefixes: "
            f"{missing}. Columns present: {present}"
        )
        sys.exit(1)


def select_feature_columns(columns):
    feature_columns = [
        col
        for col in columns
        if col not in EXCLUDED_FEATURE_COLUMNS and FEATURE_REGEX.match(col)
    ]
    return sorted(feature_columns)


def fetch_windows(cursor):
    cursor.execute("SELECT DISTINCT window FROM phase4_patterns")
    windows = sorted([row[0] for row in cursor.fetchall()])
    return windows


def validate_windows(windows):
    if sorted(windows) != sorted(REQUIRED_WINDOWS):
        print(
            "ERROR: windows must be exactly: 24h, 7d, lifetime. "
            f"Found: {', '.join(windows)}"
        )
        sys.exit(1)


def write_tsv(path, header, rows):
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\t".join(header) + "\n")
        for row in rows:
            handle.write("\t".join(row) + "\n")


def build_output1(cursor, out_path):
    query = (
        "SELECT window, "
        "COUNT(DISTINCT wallet) AS total_wallets, "
        "COUNT(DISTINCT CASE WHEN pattern_id IS NULL THEN wallet END) AS null_wallets "
        "FROM phase4_patterns "
        "GROUP BY window"
    )
    cursor.execute(query)
    results = {row[0]: row[1:] for row in cursor.fetchall()}

    rows = []
    for window in REQUIRED_WINDOWS:
        total_wallets, null_wallets = results.get(window, (0, 0))
        null_pct = 0.0
        if total_wallets:
            null_pct = null_wallets / total_wallets
        rows.append(
            [
                window,
                str(total_wallets),
                str(null_wallets),
                f"{null_pct:.4f}",
            ]
        )

    write_tsv(
        out_path,
        ["window", "total_wallets", "null_pattern_wallets", "null_pct"],
        rows,
    )
    return len(rows)


def build_output2(cursor, out_path):
    cursor.execute(
        "SELECT wallet, window, pattern_id FROM phase4_patterns ORDER BY wallet"
    )
    wallet_windows = {}
    wallet_null_any = set()
    for wallet, window, pattern_id in cursor.fetchall():
        wallet_windows.setdefault(wallet, set()).add(window)
        if pattern_id is None:
            wallet_null_any.add(wallet)

    counts = {
        "present_in_all_windows": 0,
        "missing_one_or_more_windows": 0,
    }
    required_set = set(REQUIRED_WINDOWS)
    for wallet in wallet_null_any:
        present = wallet_windows.get(wallet, set())
        if present == required_set:
            counts["present_in_all_windows"] += 1
        else:
            counts["missing_one_or_more_windows"] += 1

    rows = [
        ["present_in_all_windows", str(counts["present_in_all_windows"])],
        [
            "missing_one_or_more_windows",
            str(counts["missing_one_or_more_windows"]),
        ],
    ]

    write_tsv(out_path, ["presence_category", "wallet_count"], rows)
    return len(rows)


def build_output3(cursor, feature_columns, out_path):
    rows = []
    for window in REQUIRED_WINDOWS:
        for feature in feature_columns:
            query = (
                "SELECT "
                "SUM(CASE WHEN p.pattern_id IS NULL AND f.{feature} IS NOT NULL THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN p.pattern_id IS NULL THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN p.pattern_id IS NOT NULL AND f.{feature} IS NOT NULL THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN p.pattern_id IS NOT NULL THEN 1 ELSE 0 END) "
                "FROM phase4_patterns p "
                "JOIN phase4_features_norm f "
                "ON p.wallet = f.wallet AND p.window = f.window "
                "WHERE p.window = ?"
            ).format(feature=feature)
            cursor.execute(query, (window,))
            (
                null_non_null_count,
                null_rows,
                non_null_non_null_count,
                non_null_rows,
            ) = cursor.fetchone()

            rows.append(
                [
                    window,
                    feature,
                    str(null_non_null_count or 0),
                    str(null_rows or 0),
                    str(non_null_non_null_count or 0),
                    str(non_null_rows or 0),
                ]
            )

    write_tsv(
        out_path,
        [
            "window",
            "feature",
            "null_pattern_non_null_count",
            "null_pattern_rows",
            "non_null_pattern_non_null_count",
            "non_null_pattern_rows",
        ],
        rows,
    )
    return len(rows)


def build_digest(paths, out_path):
    digest = hashlib.sha256()
    for path in paths:
        with open(path, "rb") as handle:
            digest.update(handle.read())
    hex_digest = digest.hexdigest()
    with open(out_path, "w", encoding="utf-8") as handle:
        handle.write(hex_digest + "\n")
    return hex_digest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--outdir", required=True)
    args = parser.parse_args()

    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)

    conn = sqlite3.connect(args.db)
    cursor = conn.cursor()

    ensure_table(cursor, "phase4_patterns")
    ensure_table(cursor, "phase4_features_norm")

    patterns_columns = discover_columns(cursor, "phase4_patterns")
    features_columns = discover_columns(cursor, "phase4_features_norm")

    validate_required_columns(
        "phase4_patterns", patterns_columns, ["wallet", "window", "pattern_id"]
    )
    validate_required_columns(
        "phase4_features_norm", features_columns, ["wallet", "window"]
    )
    validate_feature_prefixes(features_columns)

    feature_columns = select_feature_columns(features_columns)

    windows = fetch_windows(cursor)
    validate_windows(windows)

    print(
        "Column mappings: "
        f"phase4_patterns={{'wallet':'wallet','window':'window','pattern_id':'pattern_id'}}, "
        f"phase4_features_norm={{'wallet':'wallet','window':'window','features':{feature_columns}}}"
    )

    output1 = os.path.join(outdir, "phase4_7_null_pattern_by_window.tsv")
    output2 = os.path.join(outdir, "phase4_7_null_pattern_by_wallet_presence.tsv")
    output3 = os.path.join(outdir, "phase4_7_null_pattern_feature_coverage.tsv")
    digest_path = os.path.join(outdir, "phase4_7_digest.txt")

    rows1 = build_output1(cursor, output1)
    print(f"Wrote {rows1} rows to {output1}")
    rows2 = build_output2(cursor, output2)
    print(f"Wrote {rows2} rows to {output2}")
    rows3 = build_output3(cursor, feature_columns, output3)
    print(f"Wrote {rows3} rows to {output3}")

    digest = build_digest([output1, output2, output3], digest_path)
    print(f"Digest written to {digest_path}: {digest}")

    conn.close()


if __name__ == "__main__":
    main()
