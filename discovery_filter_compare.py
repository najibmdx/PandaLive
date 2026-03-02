#!/usr/bin/env python3
"""Analyze wallet discovery filter cohorts by entry_delta."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from statistics import median
from typing import Iterable

LAMPORTS_PER_SOL = 1_000_000_000
BUCKET_ORDER = ("FAST", "MID", "SLOW")


@dataclass
class TokenRow:
    token_mint: str
    entry_delta: int
    dex_mode: str | None
    sol_spent: float
    sol_recv: float
    net_sol: float
    buy_count: int
    sell_count: int
    hold_seconds: int | None
    liquidity_usd: float | None
    lp_locked_pct: float | None
    lp_lock_flag: int | None
    primary_pool: str | None
    bucket: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare wallet discovery filter cohorts")
    parser.add_argument("--db", default="masterwalletsdb.db")
    parser.add_argument("--wallet", required=True)
    parser.add_argument("--outdir", default="out_filter")
    parser.add_argument("--fast-threshold", type=int, default=5)
    parser.add_argument("--slow-threshold", type=int, default=60)
    return parser.parse_args()


def safe_median(values: Iterable[float]) -> float | None:
    vals = list(values)
    if not vals:
        return None
    return float(median(vals))


def round6(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 6)


def choose_bucket(entry_delta: int, fast_threshold: int, slow_threshold: int) -> str:
    if entry_delta <= fast_threshold:
        return "FAST"
    if entry_delta <= slow_threshold:
        return "MID"
    return "SLOW"


def fetch_global_first_trade(conn: sqlite3.Connection) -> dict[str, int]:
    query = """
        SELECT token_mint, MIN(block_time) AS first_trade
        FROM wallet_token_flow
        WHERE token_mint IS NOT NULL AND token_mint != '' AND block_time IS NOT NULL
        GROUP BY token_mint
    """
    return {token: first_trade for token, first_trade in conn.execute(query)}


def fetch_wallet_first_in(conn: sqlite3.Connection, wallet: str) -> dict[str, int]:
    query = """
        SELECT token_mint, MIN(block_time) AS first_in
        FROM wallet_token_flow
        WHERE scan_wallet = ?
          AND flow_direction = 'in'
          AND token_mint IS NOT NULL AND token_mint != ''
          AND block_time IS NOT NULL
        GROUP BY token_mint
    """
    return {token: first_in for token, first_in in conn.execute(query, (wallet,))}


def fetch_wallet_token_stats(conn: sqlite3.Connection, wallet: str) -> dict[str, dict]:
    query = """
        SELECT
            flow.token_mint,
            flow.block_time,
            flow.flow_direction,
            flow.sol_amount_lamports,
            flow.dex
        FROM wallet_token_flow AS flow
        LEFT JOIN tx
            ON tx.signature = flow.signature
           AND tx.scan_wallet = flow.scan_wallet
        WHERE flow.scan_wallet = ?
          AND flow.token_mint IS NOT NULL AND flow.token_mint != ''
          AND (tx.err IS NULL OR tx.err = '')
    """

    stats: dict[str, dict] = {}
    for token_mint, block_time, flow_direction, sol_lamports, dex in conn.execute(query, (wallet,)):
        token = stats.setdefault(
            token_mint,
            {
                "spent_lamports": 0,
                "recv_lamports": 0,
                "buy_count": 0,
                "sell_count": 0,
                "first_time": None,
                "last_time": None,
                "dex_counter": Counter(),
            },
        )

        lamports = int(sol_lamports or 0)
        if flow_direction == "in":
            token["spent_lamports"] += lamports
            token["buy_count"] += 1
        elif flow_direction == "out":
            token["recv_lamports"] += lamports
            token["sell_count"] += 1

        if block_time is not None:
            if token["first_time"] is None or block_time < token["first_time"]:
                token["first_time"] = block_time
            if token["last_time"] is None or block_time > token["last_time"]:
                token["last_time"] = block_time

        if dex:
            token["dex_counter"][dex] += 1

    return stats


def fetch_liquidity(conn: sqlite3.Connection) -> dict[str, tuple[float | None, float | None, int | None, str | None]]:
    query = """
        SELECT mint, liquidity_usd, lp_locked_pct, lp_lock_flag, primary_pool
        FROM mint_liquidity
    """
    return {mint: (liq, lp_pct, lp_flag, pool) for mint, liq, lp_pct, lp_flag, pool in conn.execute(query)}


def mode_dex(counter: Counter) -> str | None:
    if not counter:
        return None
    best_count = max(counter.values())
    candidates = [dex for dex, cnt in counter.items() if cnt == best_count]
    return sorted(candidates)[0]


def write_token_rows_csv(path: str, rows: list[TokenRow]) -> None:
    fieldnames = [
        "token_mint",
        "entry_delta",
        "dex_mode",
        "sol_spent",
        "sol_recv",
        "net_sol",
        "buy_count",
        "sell_count",
        "hold_seconds",
        "liquidity_usd",
        "lp_locked_pct",
        "lp_lock_flag",
        "primary_pool",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "token_mint": row.token_mint,
                    "entry_delta": row.entry_delta,
                    "dex_mode": row.dex_mode,
                    "sol_spent": f"{row.sol_spent:.9f}",
                    "sol_recv": f"{row.sol_recv:.9f}",
                    "net_sol": f"{row.net_sol:.9f}",
                    "buy_count": row.buy_count,
                    "sell_count": row.sell_count,
                    "hold_seconds": row.hold_seconds,
                    "liquidity_usd": row.liquidity_usd,
                    "lp_locked_pct": row.lp_locked_pct,
                    "lp_lock_flag": row.lp_lock_flag,
                    "primary_pool": row.primary_pool,
                }
            )


def compute_bucket_summary(rows: list[TokenRow]) -> dict:
    token_count = len(rows)
    if token_count == 0:
        return {
            "token_count": 0,
            "win_rate": None,
            "median_net_sol": None,
            "avg_net_sol": None,
            "profit_factor": None,
            "median_sol_spent": None,
            "median_hold_seconds": None,
            "median_liquidity_usd": None,
            "pct_lp_locked": None,
            "top_dex_share": None,
        }

    nets = [r.net_sol for r in rows]
    wins = sum(1 for n in nets if n > 0)
    avg_net = sum(nets) / token_count
    pos_sum = sum(n for n in nets if n > 0)
    neg_sum = sum(n for n in nets if n < 0)
    profit_factor = None if math.isclose(neg_sum, 0.0, abs_tol=1e-15) else pos_sum / abs(neg_sum)

    liq_vals = [r.liquidity_usd for r in rows if r.liquidity_usd is not None and r.liquidity_usd > 0]
    hold_vals = [r.hold_seconds for r in rows if r.hold_seconds is not None]

    lp_locked_count = sum(1 for r in rows if r.lp_lock_flag == 1)

    dex_counter = Counter(r.dex_mode for r in rows if r.dex_mode)
    if dex_counter:
        top_count = max(dex_counter.values())
        top_dex = sorted([d for d, c in dex_counter.items() if c == top_count])[0]
        top_dex_share = top_count / token_count
    else:
        top_dex = None
        top_dex_share = None

    return {
        "token_count": token_count,
        "win_rate": round6(wins / token_count),
        "median_net_sol": round6(safe_median(nets)),
        "avg_net_sol": round6(avg_net),
        "profit_factor": round6(profit_factor),
        "median_sol_spent": round6(safe_median([r.sol_spent for r in rows])),
        "median_hold_seconds": round6(safe_median(hold_vals)),
        "median_liquidity_usd": round6(safe_median(liq_vals)),
        "pct_lp_locked": round6(lp_locked_count / token_count),
        "top_dex": top_dex,
        "top_dex_share": round6(top_dex_share),
    }


def write_summary_json(path: str, rows: list[TokenRow]) -> dict:
    by_bucket: dict[str, list[TokenRow]] = {b: [] for b in BUCKET_ORDER}
    for row in rows:
        by_bucket[row.bucket].append(row)

    summaries = {bucket: compute_bucket_summary(by_bucket[bucket]) for bucket in BUCKET_ORDER}
    total = len(rows)
    payload = {
        "overall": {
            "total_tokens_considered": total,
            "pct_fast": round6((len(by_bucket["FAST"]) / total) if total else None),
            "pct_mid": round6((len(by_bucket["MID"]) / total) if total else None),
            "pct_slow": round6((len(by_bucket["SLOW"]) / total) if total else None),
        },
        "buckets": summaries,
        "notes": {
            "profit_factor": "sum(positive net_sol) / abs(sum(negative net_sol)); null when no losses in bucket",
        },
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    return payload


def write_dex_breakdown_csv(path: str, rows: list[TokenRow]) -> None:
    by_bucket: dict[str, list[TokenRow]] = defaultdict(list)
    for row in rows:
        by_bucket[row.bucket].append(row)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["bucket", "dex", "count", "share"])
        writer.writeheader()
        for bucket in BUCKET_ORDER:
            bucket_rows = by_bucket.get(bucket, [])
            total = len(bucket_rows)
            dex_counts = Counter(r.dex_mode for r in bucket_rows if r.dex_mode)
            for dex in sorted(dex_counts):
                count = dex_counts[dex]
                share = (count / total) if total else 0.0
                writer.writerow(
                    {
                        "bucket": bucket,
                        "dex": dex,
                        "count": count,
                        "share": f"{round(share, 6):.6f}",
                    }
                )


def sha256_file(path: str) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            hasher.update(chunk)
    return hasher.hexdigest()


def write_run_log(path: str, wallet: str, fast: int, slow: int, rows: list[TokenRow], output_paths: list[str]) -> None:
    bucket_counts = Counter(r.bucket for r in rows)
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"wallet={wallet}\n")
        f.write(f"fast_threshold={fast}\n")
        f.write(f"slow_threshold={slow}\n")
        for bucket in BUCKET_ORDER:
            f.write(f"bucket_{bucket.lower()}={bucket_counts.get(bucket, 0)}\n")
        for out in output_paths:
            f.write(f"sha256 {os.path.basename(out)} {sha256_file(out)}\n")




def require_tables(conn: sqlite3.Connection, needed: set[str]) -> None:
    existing = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    missing = sorted(needed - existing)
    if missing:
        raise SystemExit(
            f"Missing required tables: {', '.join(missing)}. Found tables: {', '.join(sorted(existing)) or '(none)'}"
        )

def main() -> None:
    args = parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    require_tables(conn, {"wallet_token_flow", "tx", "mint_liquidity"})
    try:
        global_first = fetch_global_first_trade(conn)
        wallet_first_in = fetch_wallet_first_in(conn, args.wallet)
        wallet_stats = fetch_wallet_token_stats(conn, args.wallet)
        liq_map = fetch_liquidity(conn)
    finally:
        conn.close()

    rows: list[TokenRow] = []
    for token_mint in sorted(wallet_first_in):
        if token_mint not in global_first:
            continue
        stats = wallet_stats.get(token_mint)
        if not stats:
            continue

        entry_delta = wallet_first_in[token_mint] - global_first[token_mint]
        sol_spent = stats["spent_lamports"] / LAMPORTS_PER_SOL
        sol_recv = stats["recv_lamports"] / LAMPORTS_PER_SOL
        net_sol = sol_recv - sol_spent

        first_time = stats["first_time"]
        last_time = stats["last_time"]
        hold_seconds = (last_time - first_time) if (first_time is not None and last_time is not None) else None
        dex = mode_dex(stats["dex_counter"])

        liq = liq_map.get(token_mint, (None, None, None, None))
        bucket = choose_bucket(entry_delta, args.fast_threshold, args.slow_threshold)

        rows.append(
            TokenRow(
                token_mint=token_mint,
                entry_delta=entry_delta,
                dex_mode=dex,
                sol_spent=sol_spent,
                sol_recv=sol_recv,
                net_sol=net_sol,
                buy_count=stats["buy_count"],
                sell_count=stats["sell_count"],
                hold_seconds=hold_seconds,
                liquidity_usd=liq[0],
                lp_locked_pct=liq[1],
                lp_lock_flag=liq[2],
                primary_pool=liq[3],
                bucket=bucket,
            )
        )

    rows.sort(key=lambda r: (r.entry_delta, -r.net_sol, r.token_mint))

    token_csv = os.path.join(args.outdir, "token_filter_rows.csv")
    summary_json = os.path.join(args.outdir, "filter_summary.json")
    dex_csv = os.path.join(args.outdir, "filter_dex_breakdown.csv")
    run_log = os.path.join(args.outdir, "run.log")

    write_token_rows_csv(token_csv, rows)
    write_summary_json(summary_json, rows)
    write_dex_breakdown_csv(dex_csv, rows)
    write_run_log(run_log, args.wallet, args.fast_threshold, args.slow_threshold, rows, [token_csv, summary_json, dex_csv])


if __name__ == "__main__":
    main()
