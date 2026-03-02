#!/usr/bin/env python3
"""Compute wallet position sizes as a percentage of liquidity snapshots."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
from pathlib import Path
from statistics import median

LAMPORTS_PER_SOL = 1_000_000_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze wallet token position size relative to liquidity snapshots."
    )
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--wallet", required=True, help="Wallet address to analyze")
    parser.add_argument("--outdir", required=True, help="Output directory")
    parser.add_argument(
        "--sol-price",
        required=True,
        type=float,
        help="SOL price in USD used to convert spent SOL into USD position size",
    )
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fetch_token_spend(cursor: sqlite3.Cursor, wallet: str) -> list[tuple[str, float]]:
    cursor.execute(
        """
        SELECT token_mint, SUM(sol_amount_lamports) AS total_lamports
        FROM wallet_token_flow
        WHERE scan_wallet = ? AND flow_direction = 'in'
        GROUP BY token_mint
        ORDER BY token_mint ASC
        """,
        (wallet,),
    )

    spent: list[tuple[str, float]] = []
    for token_mint, total_lamports in cursor.fetchall():
        lamports = total_lamports or 0
        total_sol_spent = lamports / LAMPORTS_PER_SOL
        if total_sol_spent > 0:
            spent.append((token_mint, total_sol_spent))
    return spent


def main() -> None:
    args = parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(args.db) as conn:
        cursor = conn.cursor()

        token_spend = fetch_token_spend(cursor, args.wallet)

        rows: list[dict[str, float | str]] = []
        for token_mint, total_sol_spent in token_spend:
            cursor.execute(
                "SELECT liquidity_usd FROM mint_liquidity WHERE mint = ?",
                (token_mint,),
            )
            rec = cursor.fetchone()
            liquidity_usd = rec[0] if rec else None

            if liquidity_usd is None or liquidity_usd <= 0:
                continue

            position_usd = total_sol_spent * args.sol_price
            position_pct = position_usd / liquidity_usd if liquidity_usd else 0.0

            rows.append(
                {
                    "token_mint": token_mint,
                    "total_sol_spent": total_sol_spent,
                    "liquidity_usd": float(liquidity_usd),
                    "position_pct": position_pct,
                }
            )

    rows.sort(key=lambda item: (item["position_pct"], item["token_mint"]))

    csv_path = outdir / "liquidity_distribution.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["token_mint", "total_sol_spent", "liquidity_usd", "position_pct"])
        for row in rows:
            writer.writerow(
                [
                    row["token_mint"],
                    f"{row['total_sol_spent']:.9f}",
                    f"{row['liquidity_usd']:.6f}",
                    f"{row['position_pct']:.12f}",
                ]
            )

    position_pcts = [float(row["position_pct"]) for row in rows]
    n = len(position_pcts)

    if n == 0:
        median_position_pct = 0.0
        p90_position_pct = 0.0
        max_position_pct = 0.0
        pct_above_1pct = 0.0
        pct_above_3pct = 0.0
        pct_above_5pct = 0.0
    else:
        sorted_pcts = sorted(position_pcts)
        p90_index = min(n - 1, int(0.9 * n))

        median_position_pct = round(float(median(position_pcts)), 6)
        p90_position_pct = round(float(sorted_pcts[p90_index]), 6)
        max_position_pct = round(float(sorted_pcts[-1]), 6)

        pct_above_1pct = round(sum(v > 0.01 for v in position_pcts) / n, 6)
        pct_above_3pct = round(sum(v > 0.03 for v in position_pcts) / n, 6)
        pct_above_5pct = round(sum(v > 0.05 for v in position_pcts) / n, 6)

    summary = {
        "wallet": args.wallet,
        "total_tokens_analyzed": n,
        "median_position_pct": median_position_pct,
        "p90_position_pct": p90_position_pct,
        "max_position_pct": max_position_pct,
        "pct_above_1pct": pct_above_1pct,
        "pct_above_3pct": pct_above_3pct,
        "pct_above_5pct": pct_above_5pct,
    }

    json_path = outdir / "liquidity_summary.json"
    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)
        handle.write("\n")

    csv_sha = sha256_file(csv_path)
    json_sha = sha256_file(json_path)

    log_path = outdir / "run.log"
    with log_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(f"wallet: {args.wallet}\n")
        handle.write(f"tokens analyzed: {n}\n")
        handle.write(f"median_position_pct: {median_position_pct}\n")
        handle.write(f"p90_position_pct: {p90_position_pct}\n")
        handle.write(f"sha256 liquidity_distribution.csv: {csv_sha}\n")
        handle.write(f"sha256 liquidity_summary.json: {json_sha}\n")


if __name__ == "__main__":
    main()
