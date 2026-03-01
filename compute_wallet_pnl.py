#!/usr/bin/env python3
"""Compute realized SOL PnL geometry for one wallet from SQLite flows."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
from pathlib import Path
from statistics import median
from typing import Dict, List

LAMPORTS_PER_SOL = 1_000_000_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute realized SOL PnL geometry for a wallet.")
    parser.add_argument("--db", required=True, help="Path to SQLite database (e.g., masterwalletsdb.db)")
    parser.add_argument("--wallet", required=True, help="Wallet address to analyze")
    parser.add_argument("--outdir", required=True, help="Output directory")
    return parser.parse_args()


def safe_std_dev(values: List[float]) -> float:
    n = len(values)
    if n == 0:
        return 0.0
    mu = sum(values) / n
    var = sum((v - mu) ** 2 for v in values) / n
    return math.sqrt(var)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fetch_valid_flows(conn: sqlite3.Connection, wallet: str) -> List[sqlite3.Row]:
    query = """
        SELECT
            wtf.signature,
            wtf.scan_wallet,
            wtf.block_time,
            wtf.dex,
            wtf.token_mint,
            wtf.token_amount_raw,
            wtf.flow_direction,
            wtf.sol_direction,
            wtf.sol_amount_lamports,
            wtf.source_table,
            wtf.created_at
        FROM wallet_token_flow AS wtf
        LEFT JOIN tx
            ON tx.signature = wtf.signature
           AND tx.scan_wallet = wtf.scan_wallet
        WHERE wtf.scan_wallet = ?
          AND (tx.err IS NULL OR tx.err = '')
        ORDER BY wtf.block_time ASC, wtf.signature ASC, wtf.token_mint ASC, wtf.created_at ASC
    """
    return conn.execute(query, (wallet,)).fetchall()


def compute_token_rows(rows: List[sqlite3.Row]) -> List[Dict[str, float]]:
    by_token: Dict[str, Dict[str, int]] = {}

    for row in rows:
        token = row["token_mint"]
        if token not in by_token:
            by_token[token] = {
                "spent_lamports": 0,
                "received_lamports": 0,
                "first_time": row["block_time"],
                "last_time": row["block_time"],
                "buy_count": 0,
                "sell_count": 0,
            }

        rec = by_token[token]
        block_time = row["block_time"]
        if block_time is not None:
            if rec["first_time"] is None or block_time < rec["first_time"]:
                rec["first_time"] = block_time
            if rec["last_time"] is None or block_time > rec["last_time"]:
                rec["last_time"] = block_time

        direction = row["flow_direction"]
        lamports = int(row["sol_amount_lamports"] or 0)

        if direction == "in":
            rec["spent_lamports"] += lamports
            rec["buy_count"] += 1
        elif direction == "out":
            rec["received_lamports"] += lamports
            rec["sell_count"] += 1

    token_rows: List[Dict[str, float]] = []
    for token in sorted(by_token):
        rec = by_token[token]
        buy_count = int(rec["buy_count"])
        if buy_count == 0:
            continue

        spent = rec["spent_lamports"] / LAMPORTS_PER_SOL
        received = rec["received_lamports"] / LAMPORTS_PER_SOL
        net = received - spent
        first_time = rec["first_time"] if rec["first_time"] is not None else 0
        last_time = rec["last_time"] if rec["last_time"] is not None else 0
        hold_seconds = max(0, int(last_time) - int(first_time))

        token_rows.append(
            {
                "token_mint": token,
                "total_sol_spent": spent,
                "total_sol_received": received,
                "net_sol": net,
                "hold_seconds": hold_seconds,
                "buy_count": buy_count,
                "sell_count": int(rec["sell_count"]),
            }
        )

    token_rows.sort(key=lambda r: r["token_mint"])
    return token_rows


def compute_summary(token_rows: List[Dict[str, float]]) -> Dict[str, float]:
    total_tokens = len(token_rows)
    net_vals = [r["net_sol"] for r in token_rows]
    hold_vals = [r["hold_seconds"] for r in token_rows]
    buy_vals = [r["buy_count"] for r in token_rows]

    positives = [v for v in net_vals if v > 0]
    negatives = [v for v in net_vals if v < 0]

    win_count = len(positives)
    loss_count = len(negatives)
    win_rate = (win_count / total_tokens) if total_tokens else 0.0

    avg_net_sol = (sum(net_vals) / total_tokens) if total_tokens else 0.0
    median_net_sol = median(net_vals) if net_vals else 0.0
    std_dev_net_sol = safe_std_dev(net_vals)
    largest_gain = max(net_vals) if net_vals else 0.0
    largest_loss = min(net_vals) if net_vals else 0.0

    median_hold_seconds = median(hold_vals) if hold_vals else 0.0
    avg_hold_seconds = (sum(hold_vals) / total_tokens) if total_tokens else 0.0
    median_buys_per_token = median(buy_vals) if buy_vals else 0.0

    tokens_with_sells = [r for r in token_rows if r["sell_count"] > 0]
    partial_exit_tokens = [r for r in tokens_with_sells if r["sell_count"] >= 2]
    partial_exit_rate = (len(partial_exit_tokens) / len(tokens_with_sells)) if tokens_with_sells else 0.0

    ranked = sorted(token_rows, key=lambda r: (r["net_sol"], r["token_mint"]), reverse=True)
    top5_sum = sum(r["net_sol"] for r in ranked[:5])
    total_positive = sum(positives)
    top5_profit_contribution = (top5_sum / total_positive) if total_positive > 0 else 0.0

    cumulative_net_sol = sum(net_vals)
    negative_abs = abs(sum(negatives))
    profit_factor = (sum(positives) / negative_abs) if negative_abs > 0 else 0.0

    return {
        "total_tokens": total_tokens,
        "win_count": win_count,
        "loss_count": loss_count,
        "win_rate": win_rate,
        "avg_net_sol": avg_net_sol,
        "median_net_sol": median_net_sol,
        "std_dev_net_sol": std_dev_net_sol,
        "largest_gain": largest_gain,
        "largest_loss": largest_loss,
        "median_hold_seconds": median_hold_seconds,
        "avg_hold_seconds": avg_hold_seconds,
        "median_buys_per_token": median_buys_per_token,
        "partial_exit_rate": partial_exit_rate,
        "top5_profit_contribution": top5_profit_contribution,
        "cumulative_net_sol": cumulative_net_sol,
        "profit_factor": profit_factor,
    }


def write_token_csv(path: Path, token_rows: List[Dict[str, float]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "token_mint",
                "total_sol_spent",
                "total_sol_received",
                "net_sol",
                "hold_seconds",
                "buy_count",
                "sell_count",
            ]
        )
        for row in token_rows:
            writer.writerow(
                [
                    row["token_mint"],
                    f"{row['total_sol_spent']:.9f}",
                    f"{row['total_sol_received']:.9f}",
                    f"{row['net_sol']:.9f}",
                    int(row["hold_seconds"]),
                    int(row["buy_count"]),
                    int(row["sell_count"]),
                ]
            )


def write_distribution_csv(path: Path, token_rows: List[Dict[str, float]]) -> None:
    rows = sorted(token_rows, key=lambda r: (r["net_sol"], r["token_mint"]), reverse=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["token_mint", "net_sol", "hold_seconds"])
        for row in rows:
            writer.writerow([row["token_mint"], f"{row['net_sol']:.9f}", int(row["hold_seconds"])])


def write_summary_json(path: Path, summary: Dict[str, float]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")


def write_run_log(path: Path, wallet: str, total_tokens: int, summary: Dict[str, float], output_paths: List[Path]) -> None:
    hashes = {p.name: sha256_file(p) for p in sorted(output_paths, key=lambda x: x.name)}
    with path.open("w", encoding="utf-8") as handle:
        handle.write(f"wallet: {wallet}\n")
        handle.write(f"total tokens analyzed: {total_tokens}\n")
        handle.write(f"cumulative net SOL: {summary['cumulative_net_sol']:.9f}\n")
        handle.write(f"win rate: {summary['win_rate']:.6f}\n")
        handle.write(f"top5 contribution %: {summary['top5_profit_contribution'] * 100:.6f}\n")
        handle.write(f"profit factor: {summary['profit_factor']:.6f}\n")
        handle.write("sha256:\n")
        for name in sorted(hashes):
            handle.write(f"  {name}: {hashes[name]}\n")


def main() -> None:
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        rows = fetch_valid_flows(conn, args.wallet)
    finally:
        conn.close()

    token_rows = compute_token_rows(rows)
    summary = compute_summary(token_rows)

    token_csv = outdir / "token_pnl.csv"
    summary_json = outdir / "pnl_summary.json"
    dist_csv = outdir / "pnl_distribution.csv"
    run_log = outdir / "run.log"

    write_token_csv(token_csv, token_rows)
    write_summary_json(summary_json, summary)
    write_distribution_csv(dist_csv, token_rows)
    write_run_log(run_log, args.wallet, len(token_rows), summary, [token_csv, summary_json, dist_csv])


if __name__ == "__main__":
    main()
