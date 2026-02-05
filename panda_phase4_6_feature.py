#!/usr/bin/env python3
import argparse
import hashlib
import os
import sqlite3
import sys
from typing import List, Tuple, Optional

import pandas as pd


REQUIRED_WINDOWS = ["24h", "7d", "lifetime"]
WINDOW_PAIRS = [
    ("24h", "7d", "24h_vs_7d"),
    ("7d", "lifetime", "7d_vs_lifetime"),
]


def log(message: str) -> None:
    print(message)


def fail(message: str) -> None:
    print(message)
    sys.exit(1)


def detect_table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None


def load_from_db(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        if not detect_table_exists(conn, "phase4_features_norm"):
            return None
        conn.execute("PRAGMA table_info(phase4_features_norm)").fetchall()
        return pd.read_sql_query("SELECT * FROM phase4_features_norm", conn)
    finally:
        conn.close()


def load_from_tsv(tsv_path: str) -> pd.DataFrame:
    if not os.path.exists(tsv_path):
        fail(f"TSV fallback not found: {tsv_path}")
    return pd.read_csv(tsv_path, sep="\t", dtype=str)


def pick_wallet_column(columns: List[str]) -> str:
    candidates = [c for c in ["wallet", "scan_wallet"] if c in columns]
    if len(candidates) != 1:
        fail(f"Wallet column ambiguous or missing. Columns: {columns}")
    return candidates[0]


def pick_window_column(df: pd.DataFrame) -> str:
    candidates = []
    for col in df.columns:
        values = set(df[col].dropna().unique().tolist())
        if set(REQUIRED_WINDOWS).issubset(values):
            candidates.append(col)
    if len(candidates) != 1:
        fail(f"Window column ambiguous or missing. Columns: {list(df.columns)}")
    return candidates[0]


def validate_windows(df: pd.DataFrame, window_col: str) -> None:
    present = sorted(set(df[window_col].dropna().unique().tolist()))
    if set(present) != set(REQUIRED_WINDOWS):
        fail(f"Missing required windows. Present: {present}")


def detect_feature_columns(
    df: pd.DataFrame, wallet_col: str, window_col: str
) -> Tuple[List[str], List[str]]:
    excluded_identifiers = {wallet_col, window_col}
    for optional_col in ["run_id", "created_at"]:
        if optional_col in df.columns:
            excluded_identifiers.add(optional_col)
    feature_cols = []
    excluded_cols = []
    for col in df.columns:
        if col in excluded_identifiers:
            continue
        series = df[col]
        if series.isnull().all():
            feature_cols.append(col)
            continue
        coerced = pd.to_numeric(series, errors="coerce")
        non_null_before = series.notna().sum()
        non_null_after = coerced.notna().sum()
        if non_null_before == non_null_after:
            feature_cols.append(col)
            df[col] = coerced
        else:
            excluded_cols.append(col)
    return feature_cols, excluded_cols


def format_float(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{value:.4f}"


def compute_coverage(df: pd.DataFrame, window_col: str, feature_cols: List[str]) -> pd.DataFrame:
    rows = []
    grouped = df.groupby(window_col, dropna=False)
    for window, group in grouped:
        rows_total = len(group)
        for feature in feature_cols:
            non_null_count = group[feature].notna().sum()
            null_count = rows_total - non_null_count
            null_pct = null_count / rows_total if rows_total else 0.0
            rows.append(
                {
                    "window": window,
                    "feature": feature,
                    "rows_total": rows_total,
                    "non_null_count": non_null_count,
                    "null_count": null_count,
                    "null_pct": format_float(null_pct),
                }
            )
    coverage = pd.DataFrame(rows)
    coverage = coverage.sort_values(by=["feature", "window"], kind="mergesort")
    return coverage


def compute_stability(
    df: pd.DataFrame, wallet_col: str, window_col: str, feature_cols: List[str]
) -> pd.DataFrame:
    rows = []
    for left_window, right_window, pair_name in WINDOW_PAIRS:
        left_df = df[df[window_col] == left_window]
        right_df = df[df[window_col] == right_window]
        merged = left_df[[wallet_col] + feature_cols].merge(
            right_df[[wallet_col] + feature_cols],
            on=wallet_col,
            how="inner",
            suffixes=("_left", "_right"),
        )
        for feature in feature_cols:
            left_values = merged[f"{feature}_left"]
            right_values = merged[f"{feature}_right"]
            mask = left_values.notna() & right_values.notna()
            diffs = (left_values[mask] - right_values[mask]).abs()
            wallets_compared = int(diffs.shape[0])
            if wallets_compared == 0:
                rows.append(
                    {
                        "feature": feature,
                        "pair": pair_name,
                        "wallets_compared": 0,
                        "mean_abs_diff": "",
                        "median_abs_diff": "",
                        "pct_equal": "",
                    }
                )
            else:
                mean_abs_diff = diffs.mean()
                median_abs_diff = diffs.median()
                pct_equal = (diffs == 0).sum() / wallets_compared
                rows.append(
                    {
                        "feature": feature,
                        "pair": pair_name,
                        "wallets_compared": wallets_compared,
                        "mean_abs_diff": format_float(mean_abs_diff),
                        "median_abs_diff": format_float(median_abs_diff),
                        "pct_equal": format_float(pct_equal),
                    }
                )
    stability = pd.DataFrame(rows)
    stability = stability.sort_values(by=["feature", "pair"], kind="mergesort")
    return stability


def compute_rankcorr(
    df: pd.DataFrame, wallet_col: str, window_col: str, feature_cols: List[str]
) -> pd.DataFrame:
    rows = []
    for left_window, right_window, pair_name in WINDOW_PAIRS:
        left_df = df[df[window_col] == left_window]
        right_df = df[df[window_col] == right_window]
        merged = left_df[[wallet_col] + feature_cols].merge(
            right_df[[wallet_col] + feature_cols],
            on=wallet_col,
            how="inner",
            suffixes=("_left", "_right"),
        )
        for feature in feature_cols:
            left_values = merged[f"{feature}_left"]
            right_values = merged[f"{feature}_right"]
            mask = left_values.notna() & right_values.notna()
            left_series = left_values[mask]
            right_series = right_values[mask]
            wallets_compared = int(left_series.shape[0])
            if wallets_compared < 3:
                rows.append(
                    {
                        "feature": feature,
                        "pair": pair_name,
                        "wallets_compared": wallets_compared,
                        "spearman_r": "",
                    }
                )
                continue
            left_rank = left_series.rank(method="average")
            right_rank = right_series.rank(method="average")
            spearman_r = left_rank.corr(right_rank)
            rows.append(
                {
                    "feature": feature,
                    "pair": pair_name,
                    "wallets_compared": wallets_compared,
                    "spearman_r": format_float(spearman_r),
                }
            )
    rankcorr = pd.DataFrame(rows)
    rankcorr = rankcorr.sort_values(by=["feature", "pair"], kind="mergesort")
    return rankcorr


def apply_run_id_filter(df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[str], int, bool]:
    if "run_id" not in df.columns:
        return df, None, 0, False
    distinct_runs = df["run_id"].dropna().unique().tolist()
    distinct_count = len(distinct_runs)
    if distinct_count <= 1:
        return df, distinct_runs[0] if distinct_runs else None, distinct_count, True
    if "created_at" not in df.columns:
        fail(
            "run_id has multiple values but created_at is missing; cannot deterministically choose."
        )
    latest_by_run = df.groupby("run_id")["created_at"].max()
    selected_run = latest_by_run.sort_values().index[-1]
    filtered = df[df["run_id"] == selected_run].copy()
    return filtered, selected_run, distinct_count, True


def write_tsv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, sep="\t", index=False, lineterminator="\n")


def write_digest(outdir: str, files: List[str]) -> None:
    hasher = hashlib.sha256()
    for name in files:
        file_path = os.path.join(outdir, name)
        with open(file_path, "rb") as handle:
            hasher.update(handle.read())
    digest_path = os.path.join(outdir, "phase4_6_digest.txt")
    with open(digest_path, "w", encoding="utf-8") as handle:
        handle.write(hasher.hexdigest() + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--outdir", required=True)
    parser.add_argument(
        "--tsv_fallback",
        default="exports_phase4_4\\phase4_features_norm.tsv",
    )
    args = parser.parse_args()

    df = None
    if os.path.exists(args.db):
        df = load_from_db(args.db)
    if df is not None:
        data_source = "DB"
    else:
        df = load_from_tsv(args.tsv_fallback)
        data_source = "TSV"

    log(f"Data source used: {data_source}")

    wallet_col = pick_wallet_column(list(df.columns))
    window_col = pick_window_column(df)

    mapping_parts = [f"wallet={wallet_col}", f"window={window_col}"]
    if "run_id" in df.columns:
        mapping_parts.append("run_id=run_id")
    if "created_at" in df.columns:
        mapping_parts.append("created_at=created_at")
    log(f"Column mappings: {', '.join(mapping_parts)}")

    validate_windows(df, window_col)

    df, selected_run_id, distinct_run_count, has_run_id = apply_run_id_filter(df)
    if has_run_id:
        log(f"distinct_run_id_count={distinct_run_count}")
        log(f"selected_run_id={selected_run_id or ''}")

    feature_cols, excluded_cols = detect_feature_columns(df, wallet_col, window_col)
    log(f"Feature columns included: {len(feature_cols)}")
    if excluded_cols:
        log(f"Feature columns excluded: {', '.join(excluded_cols)}")
    else:
        log("Feature columns excluded: ")

    coverage = compute_coverage(df, window_col, feature_cols)
    stability = compute_stability(df, wallet_col, window_col, feature_cols)
    rankcorr = compute_rankcorr(df, wallet_col, window_col, feature_cols)

    os.makedirs(args.outdir, exist_ok=True)
    coverage_path = os.path.join(args.outdir, "phase4_6_feature_coverage.tsv")
    stability_path = os.path.join(args.outdir, "phase4_6_feature_stability.tsv")
    rankcorr_path = os.path.join(args.outdir, "phase4_6_feature_rankcorr.tsv")

    write_tsv(coverage, coverage_path)
    write_tsv(stability, stability_path)
    write_tsv(rankcorr, rankcorr_path)

    write_digest(
        args.outdir,
        [
            "phase4_6_feature_coverage.tsv",
            "phase4_6_feature_stability.tsv",
            "phase4_6_feature_rankcorr.tsv",
        ],
    )

    log(f"Rows written: coverage={len(coverage)}, stability={len(stability)}, rankcorr={len(rankcorr)}")
    log("Digest written: phase4_6_digest.txt")


if __name__ == "__main__":
    main()
