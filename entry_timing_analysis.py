#!/usr/bin/env python3
"""Analyze wallet entry timing relative to token first trade from a SQLite database."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sqlite3
import statistics
from typing import Any


THRESHOLDS = [10, 30, 60, 120, 300]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Measure how early a wallet enters tokens relative to each token's first trade."
        )
    )
    parser.add_argument("--db", required=True, help="Path to SQLite database file")
    parser.add_argument("--wallet", required=True, help="Wallet address to analyze")
    parser.add_argument("--outdir", required=True, help="Output directory")
    return parser.parse_args()


def ensure_outdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def fetch_wallet_tokens(conn: sqlite3.Connection, wallet: str) -> list[str]:
    cursor = conn.execute(
        """
        SELECT DISTINCT token_mint
        FROM wallet_token_flow
        WHERE scan_wallet = ?
        ORDER BY token_mint
        """,
        (wallet,),
    )
    return [row[0] for row in cursor.fetchall() if row[0] is not None]


def compute_token_entry(
    conn: sqlite3.Connection, wallet: str, token_mint: str
) -> dict[str, Any] | None:
    global_first_trade = conn.execute(
        """
        SELECT MIN(block_time)
        FROM wallet_token_flow
        WHERE token_mint = ?
        """,
        (token_mint,),
    ).fetchone()[0]

    wallet_first_entry = conn.execute(
        """
        SELECT MIN(block_time)
        FROM wallet_token_flow
        WHERE token_mint = ?
          AND scan_wallet = ?
          AND flow_direction = 'in'
        """,
        (token_mint, wallet),
    ).fetchone()[0]

    if global_first_trade is None or wallet_first_entry is None:
        return None

    entry_delta = int(wallet_first_entry) - int(global_first_trade)
    return {
        "token_mint": token_mint,
        "global_first_trade": int(global_first_trade),
        "wallet_first_entry": int(wallet_first_entry),
        "entry_delta_seconds": int(entry_delta),
    }


def percentile_p90(values: list[int]) -> float | int | None:
    if not values:
        return None
    sorted_values = sorted(values)
    index = int(0.9 * len(sorted_values))
    if index >= len(sorted_values):
        index = len(sorted_values) - 1
    return sorted_values[index]


def compute_summary(wallet: str, deltas: list[int]) -> dict[str, Any]:
    total = len(deltas)
    summary: dict[str, Any] = {
        "wallet": wallet,
        "total_tokens_analyzed": total,
        "median_entry_delta": None,
        "avg_entry_delta": None,
        "p90_entry_delta": None,
        "min_entry_delta": None,
        "max_entry_delta": None,
    }

    for threshold in THRESHOLDS:
        summary[f"pct_within_{threshold}s"] = 0.0

    if total == 0:
        return summary

    sorted_deltas = sorted(deltas)
    summary["median_entry_delta"] = statistics.median(sorted_deltas)
    summary["avg_entry_delta"] = sum(sorted_deltas) / total
    summary["p90_entry_delta"] = percentile_p90(sorted_deltas)
    summary["min_entry_delta"] = sorted_deltas[0]
    summary["max_entry_delta"] = sorted_deltas[-1]

    for threshold in THRESHOLDS:
        within = sum(1 for value in sorted_deltas if value <= threshold)
        summary[f"pct_within_{threshold}s"] = round(within / total, 4)

    return summary


def write_distribution_csv(path: str, rows: list[dict[str, Any]]) -> None:
    ordered_rows = sorted(rows, key=lambda r: (r["entry_delta_seconds"], r["token_mint"]))
    with open(path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["token_mint", "entry_delta_seconds"])
        for row in ordered_rows:
            writer.writerow([row["token_mint"], row["entry_delta_seconds"]])


def write_summary_json(path: str, summary: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as jsonfile:
        json.dump(summary, jsonfile, indent=2, sort_keys=False)
        jsonfile.write("\n")


def file_sha256(path: str) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as infile:
        for chunk in iter(lambda: infile.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def write_log(path: str, wallet: str, total_tokens: int, median: Any, p90: Any, hashes: dict[str, str]) -> None:
    lines = [
        f"wallet: {wallet}",
        f"total tokens: {total_tokens}",
        f"median entry delta: {median}",
        f"p90 entry delta: {p90}",
        f"sha256 entry_timing_distribution.csv: {hashes['distribution']}",
        f"sha256 entry_timing_summary.json: {hashes['summary']}",
    ]
    with open(path, "w", encoding="utf-8", newline="\n") as logfile:
        logfile.write("\n".join(lines) + "\n")


def main() -> int:
    args = parse_args()
    ensure_outdir(args.outdir)

    distribution_path = os.path.join(args.outdir, "entry_timing_distribution.csv")
    summary_path = os.path.join(args.outdir, "entry_timing_summary.json")
    log_path = os.path.join(args.outdir, "run.log")

    conn = sqlite3.connect(args.db)
    try:
        wallet_tokens = fetch_wallet_tokens(conn, args.wallet)
        token_entries: list[dict[str, Any]] = []

        for token_mint in wallet_tokens:
            entry = compute_token_entry(conn, args.wallet, token_mint)
            if entry is not None:
                token_entries.append(entry)

        deltas = [int(item["entry_delta_seconds"]) for item in token_entries]
        summary = compute_summary(args.wallet, deltas)

        write_distribution_csv(distribution_path, token_entries)
        write_summary_json(summary_path, summary)

        hashes = {
            "distribution": file_sha256(distribution_path),
            "summary": file_sha256(summary_path),
        }
        write_log(
            log_path,
            wallet=args.wallet,
            total_tokens=summary["total_tokens_analyzed"],
            median=summary["median_entry_delta"],
            p90=summary["p90_entry_delta"],
            hashes=hashes,
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
