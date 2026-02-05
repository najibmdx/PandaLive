#!/usr/bin/env python3
import argparse
import csv
import hashlib
import os
import sqlite3
import sys


class ValidationError(Exception):
    pass


def fail(message):
    raise ValidationError(message)


def get_tables(conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    return [row[0] for row in cur.fetchall()]


def get_table_columns(conn, table):
    cur = conn.execute(f"PRAGMA table_info({table});")
    return [row[1] for row in cur.fetchall()]


def find_column(table, columns, candidates):
    lower_map = {col.lower(): col for col in columns}
    matches = []
    for candidate in candidates:
        if candidate.lower() in lower_map:
            matches.append(lower_map[candidate.lower()])
    if len(matches) == 1:
        return matches[0]
    if len(matches) == 0:
        fail(
            f"Could not find required column in {table}. "
            f"Candidates: {candidates}. Columns found: {columns}"
        )
    fail(
        f"Ambiguous column match in {table}. Candidates: {candidates}. "
        f"Matches: {matches}. Columns found: {columns}"
    )


def check_required_tables(conn, required_tables):
    tables = set(get_tables(conn))
    missing = [table for table in required_tables if table not in tables]
    if missing:
        fail(f"Missing required tables: {missing}. Tables present: {sorted(tables)}")


def count_nulls(conn, table, columns):
    null_counts = {}
    for column in columns:
        cur = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL;"
        )
        null_counts[column] = cur.fetchone()[0]
    return null_counts


def check_duplicates(conn, table, columns):
    cols = ", ".join(columns)
    cur = conn.execute(
        f"SELECT COUNT(*) FROM ("
        f"SELECT {cols}, COUNT(*) AS cnt FROM {table} "
        f"GROUP BY {cols} HAVING COUNT(*) > 1"
        f");"
    )
    return cur.fetchone()[0]


def write_tsv(path, headers, rows):
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--outdir", required=True)
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    required_tables = ["phase4_patterns", "phase4_pattern_stats", "whale_states"]
    check_required_tables(conn, required_tables)

    phase4_columns = get_table_columns(conn, "phase4_patterns")
    _ = get_table_columns(conn, "phase4_pattern_stats")
    whale_columns = get_table_columns(conn, "whale_states")

    wallet_candidates = ["wallet", "wallet_address", "address", "wallet_id"]
    window_candidates = ["window", "time_window", "window_id"]
    lens_candidates = ["lens", "lens_id"]
    pattern_candidates = ["pattern_id", "pattern", "patternid"]
    whale_is_whale_candidates = ["is_whale"]
    whale_side_candidates = ["side"]
    whale_asof_candidates = ["asof_time"]
    whale_amount_candidates = ["amount_lamports"]
    whale_supporting_flow_candidates = ["supporting_flow_count"]
    whale_flow_ref_candidates = ["flow_ref"]

    phase4_wallet_col = find_column("phase4_patterns", phase4_columns, wallet_candidates)
    phase4_window_col = find_column("phase4_patterns", phase4_columns, window_candidates)
    phase4_lens_col = find_column("phase4_patterns", phase4_columns, lens_candidates)
    phase4_pattern_col = find_column("phase4_patterns", phase4_columns, pattern_candidates)
    whale_wallet_col = find_column("whale_states", whale_columns, wallet_candidates)
    whale_window_col = find_column("whale_states", whale_columns, window_candidates)
    whale_is_whale_col = find_column("whale_states", whale_columns, whale_is_whale_candidates)
    whale_side_col = find_column("whale_states", whale_columns, whale_side_candidates)
    whale_asof_col = find_column("whale_states", whale_columns, whale_asof_candidates)
    whale_amount_col = find_column("whale_states", whale_columns, whale_amount_candidates)
    whale_supporting_flow_col = find_column(
        "whale_states", whale_columns, whale_supporting_flow_candidates
    )
    whale_flow_ref_col = find_column("whale_states", whale_columns, whale_flow_ref_candidates)

    print("Detected column mappings:")
    print(
        f" phase4_patterns: wallet={phase4_wallet_col}, window={phase4_window_col}, "
        f"lens={phase4_lens_col}, pattern_id={phase4_pattern_col}"
    )
    print(
        f" whale_states: wallet={whale_wallet_col}, window={whale_window_col}, "
        f"is_whale={whale_is_whale_col}, side={whale_side_col}"
    )

    phase4_nulls = count_nulls(
        conn,
        "phase4_patterns",
        [phase4_wallet_col, phase4_window_col, phase4_lens_col],
    )
    whale_nulls = count_nulls(
        conn,
        "whale_states",
        [whale_wallet_col, whale_window_col],
    )

    if any(count > 0 for count in phase4_nulls.values()):
        fail(f"Nulls found in phase4_patterns join keys: {phase4_nulls}")
    if any(count > 0 for count in whale_nulls.values()):
        fail(f"Nulls found in whale_states join keys: {whale_nulls}")

    cur = conn.execute(
        f"SELECT COUNT(*) AS total_rows, "
        f"SUM(CASE WHEN {phase4_pattern_col} IS NULL THEN 1 ELSE 0 END) AS null_rows, "
        f"COUNT(DISTINCT CASE WHEN {phase4_pattern_col} IS NULL THEN {phase4_wallet_col} END) "
        f"AS null_wallets "
        f"FROM phase4_patterns;"
    )
    pattern_id_stats = cur.fetchone()
    total_rows = pattern_id_stats["total_rows"]
    null_rows = pattern_id_stats["null_rows"]
    null_wallets = pattern_id_stats["null_wallets"]
    null_pct = (null_rows / total_rows * 100.0) if total_rows else 0.0
    print(
        "pattern_id_null_rows="
        f"{null_rows} pattern_id_total_rows={total_rows} "
        f"pattern_id_null_pct={null_pct:.2f}"
    )
    print(f"pattern_id_null_distinct_wallets={null_wallets}")

    phase4_dupes = check_duplicates(
        conn, "phase4_patterns", [phase4_wallet_col, phase4_window_col, phase4_lens_col]
    )
    whale_dupes = check_duplicates(
        conn, "whale_states", [whale_wallet_col, whale_window_col]
    )
    print(f"Duplicate wallet/window/lens rows in phase4_patterns: {phase4_dupes}")
    print(f"Duplicate wallet/window rows in whale_states: {whale_dupes}")
    if phase4_dupes > 0:
        fail(
            f"Wallet identity inconsistencies detected. "
            f"phase4_patterns duplicates: {phase4_dupes}"
        )

    cur = conn.execute(f"SELECT COUNT(*) AS total_rows FROM whale_states;")
    whale_total_rows = cur.fetchone()["total_rows"]
    cur = conn.execute(
        f"SELECT COUNT(*) AS unique_rows FROM ("
        f"SELECT {whale_wallet_col}, {whale_window_col} "
        f"FROM whale_states GROUP BY {whale_wallet_col}, {whale_window_col}"
        f");"
    )
    whale_unique_rows = cur.fetchone()["unique_rows"]
    cur = conn.execute(
        f"SELECT COUNT(*) AS duplicate_groups FROM ("
        f"SELECT {whale_wallet_col}, {whale_window_col}, COUNT(*) AS cnt "
        f"FROM whale_states GROUP BY {whale_wallet_col}, {whale_window_col} "
        f"HAVING COUNT(*) > 1"
        f");"
    )
    whale_duplicate_groups = cur.fetchone()["duplicate_groups"]
    whale_dropped_rows = whale_total_rows - whale_unique_rows
    print(f"whale_states_rows_total={whale_total_rows}")
    print(f"whale_states_unique_wallet_window={whale_unique_rows}")
    print(f"whale_states_duplicate_wallet_window_groups={whale_duplicate_groups}")
    print(f"whale_states_rows_dropped_by_canonicalization={whale_dropped_rows}")

    cur = conn.execute(
        f"SELECT DISTINCT {phase4_window_col} AS window FROM phase4_patterns;"
    )
    windows = sorted([row["window"] for row in cur.fetchall()])
    required_windows = ["24h", "7d", "lifetime"]
    missing_windows = [window for window in required_windows if window not in windows]
    if missing_windows:
        fail(f"Missing required windows: {missing_windows}. Windows present: {windows}")

    axis_a_rows = []
    cur = conn.execute(
        f"SELECT {phase4_wallet_col} AS wallet, {phase4_lens_col} AS lens, "
        f"{phase4_window_col} AS window, COALESCE({phase4_pattern_col}, '') AS pattern_id "
        f"FROM phase4_patterns "
        f"WHERE {phase4_window_col} IN (?, ?, ?);",
        required_windows,
    )
    data = {}
    for row in cur.fetchall():
        wallet = row["wallet"]
        lens = row["lens"]
        window = row["window"]
        pattern_id = row["pattern_id"]
        data.setdefault(lens, {}).setdefault(wallet, {})[window] = pattern_id

    for lens in sorted(data.keys()):
        for wallet in sorted(data[lens].keys()):
            pattern_24h = data[lens][wallet].get("24h", "")
            pattern_7d = data[lens][wallet].get("7d", "")
            pattern_lifetime = data[lens][wallet].get("lifetime", "")
            stable_24h_7d = (
                1 if pattern_24h != "" and pattern_7d != "" and pattern_24h == pattern_7d else 0
            )
            stable_7d_lifetime = (
                1
                if pattern_7d != "" and pattern_lifetime != "" and pattern_7d == pattern_lifetime
                else 0
            )
            axis_a_rows.append(
                [
                    wallet,
                    lens,
                    pattern_24h,
                    pattern_7d,
                    pattern_lifetime,
                    stable_24h_7d,
                    stable_7d_lifetime,
                ]
            )

    axis_a_path = os.path.join(args.outdir, "phase4_5_window_stability.tsv")
    write_tsv(
        axis_a_path,
        [
            "wallet",
            "lens",
            "pattern_24h",
            "pattern_7d",
            "pattern_lifetime",
            "stable_24h_7d",
            "stable_7d_lifetime",
        ],
        axis_a_rows,
    )

    axis_b_rows = []
    cur = conn.execute(
        f"SELECT DISTINCT {phase4_window_col} AS window, {phase4_lens_col} AS lens "
        f"FROM phase4_patterns;"
    )
    window_lens = {}
    for row in cur.fetchall():
        window_lens.setdefault(row["window"], set()).add(row["lens"])

    for window in sorted(window_lens.keys()):
        lenses = sorted(window_lens[window])
        lens_wallets = {}
        for lens in lenses:
            cur = conn.execute(
                f"SELECT DISTINCT {phase4_wallet_col} AS wallet "
                f"FROM phase4_patterns WHERE {phase4_window_col} = ? "
                f"AND {phase4_lens_col} = ?;",
                (window, lens),
            )
            lens_wallets[lens] = {row["wallet"] for row in cur.fetchall()}

        for idx, lens_a in enumerate(lenses):
            for lens_b in lenses[idx + 1 :]:
                wallets_a = lens_wallets[lens_a]
                wallets_b = lens_wallets[lens_b]
                overlap = wallets_a.intersection(wallets_b)
                union = wallets_a.union(wallets_b)
                if len(union) == 0:
                    fail(f"Union is zero for window {window} lens pair {lens_a}, {lens_b}")
                jaccard = f"{len(overlap) / len(union):.10f}"
                axis_b_rows.append(
                    [
                        window,
                        lens_a,
                        lens_b,
                        len(overlap),
                        len(union),
                        jaccard,
                    ]
                )

    axis_b_path = os.path.join(args.outdir, "phase4_5_lens_overlap.tsv")
    write_tsv(
        axis_b_path,
        [
            "window",
            "lens_a",
            "lens_b",
            "overlap_wallet_count",
            "union_wallet_count",
            "jaccard_index",
        ],
        axis_b_rows,
    )

    axis_c_rows = []
    cur = conn.execute(
        f"SELECT DISTINCT {phase4_lens_col} AS lens FROM phase4_patterns;"
    )
    lenses = {row["lens"] for row in cur.fetchall()}
    if "C" not in lenses:
        fail(f"No lens 'C' found in phase4_patterns. Lenses present: {sorted(lenses)}")

    cur = conn.execute(
        f"SELECT {phase4_window_col} AS window, "
        f"COUNT(DISTINCT COALESCE({phase4_pattern_col}, '')) AS pattern_count, "
        f"COUNT(DISTINCT {phase4_wallet_col}) AS total_wallets "
        f"FROM phase4_patterns WHERE {phase4_lens_col} = ? "
        f"GROUP BY {phase4_window_col};",
        ("C",),
    )
    for row in cur.fetchall():
        axis_c_rows.append(
            [
                row["window"],
                "C",
                row["pattern_count"],
                row["total_wallets"],
                1 if row["pattern_count"] != 1 else 0,
            ]
        )

    axis_c_rows.sort(key=lambda item: item[0])

    axis_c_path = os.path.join(args.outdir, "phase4_5_lensC_audit.tsv")
    write_tsv(
        axis_c_path,
        ["window", "lens", "pattern_count", "total_wallets", "variance_detected"],
        axis_c_rows,
    )

    axis_d_rows = []
    cur = conn.execute(
        f"WITH canonical_whale_states AS ("
        f"SELECT {whale_wallet_col} AS wallet, {whale_window_col} AS window, "
        f"{whale_is_whale_col} AS is_whale, {whale_side_col} AS side, "
        f"{whale_asof_col} AS asof_time, {whale_amount_col} AS amount_lamports, "
        f"{whale_supporting_flow_col} AS supporting_flow_count, "
        f"{whale_flow_ref_col} AS flow_ref, "
        f"ROW_NUMBER() OVER (PARTITION BY {whale_wallet_col}, {whale_window_col} "
        f"ORDER BY {whale_asof_col} DESC, {whale_amount_col} DESC, "
        f"{whale_supporting_flow_col} DESC, {whale_flow_ref_col} DESC) AS rn "
        f"FROM whale_states"
        f") "
        f"SELECT p.{phase4_window_col} AS window, "
        f"p.{phase4_lens_col} AS lens, "
        f"COALESCE(p.{phase4_pattern_col}, '') AS pattern_id, "
        f"COALESCE(w.is_whale, '') AS is_whale, "
        f"COALESCE(w.side, '') AS side, "
        f"COUNT(DISTINCT p.{phase4_wallet_col}) AS wallet_count "
        f"FROM phase4_patterns p "
        f"LEFT JOIN canonical_whale_states w "
        f"ON p.{phase4_wallet_col} = w.wallet "
        f"AND p.{phase4_window_col} = w.window "
        f"AND w.rn = 1 "
        f"GROUP BY p.{phase4_window_col}, p.{phase4_lens_col}, "
        f"COALESCE(p.{phase4_pattern_col}, ''), COALESCE(w.is_whale, ''), "
        f"COALESCE(w.side, '') "
        f"ORDER BY p.{phase4_window_col}, p.{phase4_lens_col}, "
        f"COALESCE(p.{phase4_pattern_col}, ''), COALESCE(w.is_whale, ''), "
        f"COALESCE(w.side, '');"
    )
    for row in cur.fetchall():
        axis_d_rows.append(
            [
                row["window"],
                row["lens"],
                row["pattern_id"],
                row["is_whale"],
                row["side"],
                row["wallet_count"],
            ]
        )

    axis_d_path = os.path.join(args.outdir, "phase4_5_whale_state_cross.tsv")
    write_tsv(
        axis_d_path,
        ["window", "lens", "pattern_id", "is_whale", "side", "wallet_count"],
        axis_d_rows,
    )

    digest = hashlib.sha256()
    for path in [axis_a_path, axis_b_path, axis_c_path, axis_d_path]:
        with open(path, "rb") as handle:
            digest.update(handle.read())
    digest_path = os.path.join(args.outdir, "phase4_5_digest.txt")
    with open(digest_path, "w", encoding="utf-8", newline="") as handle:
        handle.write(digest.hexdigest())
        handle.write("\n")

    print(f"Rows written: window_stability={len(axis_a_rows)}")
    print(f"Rows written: lens_overlap={len(axis_b_rows)}")
    print(f"Rows written: lensC_audit={len(axis_c_rows)}")
    print(f"Rows written: whale_state_cross={len(axis_d_rows)}")
    print("Digest written: phase4_5_digest.txt")


if __name__ == "__main__":
    try:
        main()
    except ValidationError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
