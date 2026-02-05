#!/usr/bin/env python3
"""Deterministic profit situations extractor for Solana wallet scan DB."""

import argparse
import csv
import os
import sqlite3
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple
from collections import Counter


TIME_CANDIDATES = [
    "block_time",
    "block_timestamp",
    "timestamp",
    "ts",
    "time",
    "slot",
    "created_at",
    "updated_at",
    "ingested_at",
]

TOKEN_CANDIDATES = [
    "token_mint",
    "mint",
    "token",
    "token_address",
    "ca",
    "in_mint",
    "out_mint",
]

DIRECTION_CANDIDATES = ["sol_direction", "direction", "side", "trade_side", "trade_type"]

PROFIT_COLUMNS = [
    "balance_delta_sol",
    "sol_amount",
    "sol_amount_lamports",
    "sol_spent_lamports",
    "sol_received_lamports",
    "sol_spent",
    "sol_received",
]

EPOCH_TIME_CANDIDATES = [
    "block_time",
    "block_timestamp",
    "timestamp",
    "ts",
    "time",
    "created_at",
    "updated_at",
    "ingested_at",
]

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
WSOL_MINT = "So11111111111111111111111111111111111111112"
QUOTE_MINTS = {
    USDC_MINT,
    WSOL_MINT,
}


@dataclass
class TableInfo:
    name: str
    columns: List[str]


@dataclass
class ProfitConfig:
    table: str
    token_expr: Optional[str]
    time_col: Optional[str]
    profit_expr: Optional[str]
    profit_divisor: Optional[float]
    direction_col: Optional[str]
    profit_note: str
    token_note: str


@dataclass
class SituationRow:
    wallet_address: str
    wallet_label: str
    token: str
    realized_profit_sol: Optional[float]
    wallet_total_profit_sol: float
    trade_count: int
    first_buy_time: Optional[int]
    first_sell_time: Optional[int]
    first_seen_time: Optional[int]
    cohort_60s: Optional[int]
    cohort_300s: Optional[int]
    positive_total_wallet_profit_cohort_300s: Optional[int]
    positive_token_profit_cohort_300s: Optional[int]
    profit_source: str
    profit_note: str
    has_token_profit: bool


def list_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name ASC"
    ).fetchall()
    return [row[0] for row in rows]


def table_info(conn: sqlite3.Connection, table: str) -> TableInfo:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    columns = [row[1] for row in rows]
    return TableInfo(name=table, columns=columns)


def find_first_match(columns: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    lower_map = {col.lower(): col for col in columns}
    for candidate in candidates:
        if candidate in lower_map:
            return lower_map[candidate]
    return None


def find_matches(columns: Sequence[str], candidates: Sequence[str]) -> List[str]:
    lower_map = {col.lower(): col for col in columns}
    return [lower_map[candidate] for candidate in candidates if candidate in lower_map]


def detect_time_column(columns: Sequence[str]) -> Optional[str]:
    epoch_match = find_first_match(columns, EPOCH_TIME_CANDIDATES)
    if epoch_match:
        return epoch_match
    return find_first_match(columns, ["slot"])


def detect_direction_column(columns: Sequence[str]) -> Optional[str]:
    return find_first_match(columns, DIRECTION_CANDIDATES)


def detect_token_expr(columns: Sequence[str]) -> Tuple[Optional[str], str]:
    lower_columns = [col.lower() for col in columns]
    token_mint_col = next((col for col in columns if col.lower() == "token_mint"), None)
    if token_mint_col:
        return token_mint_col, "token_mint token leg for swaps"

    has_sol_leg_col = "has_sol_leg" if "has_sol_leg" in lower_columns else None
    sol_direction_col = next((col for col in columns if col.lower() == "sol_direction"), None)
    in_mint_col = next((col for col in columns if col.lower() == "in_mint"), None)
    out_mint_col = next((col for col in columns if col.lower() == "out_mint"), None)

    if has_sol_leg_col and sol_direction_col and in_mint_col and out_mint_col:
        expr = (
            f"CASE "
            f"WHEN {has_sol_leg_col}=1 AND LOWER({sol_direction_col})='buy' THEN {out_mint_col} "
            f"WHEN {has_sol_leg_col}=1 AND LOWER({sol_direction_col})='sell' THEN {in_mint_col} END"
        )
        return expr, "has_sol_leg token routing for swaps"

    direct = find_first_match(columns, ["token_mint", "mint", "token", "token_address", "ca"])
    if direct:
        return direct, f"direct token column '{direct}'"

    has_in = "in_mint" in [col.lower() for col in columns]
    has_out = "out_mint" in [col.lower() for col in columns]
    if has_in and has_out:
        in_col = next(col for col in columns if col.lower() == "in_mint")
        out_col = next(col for col in columns if col.lower() == "out_mint")
        in_is_sol = find_first_match(
            columns,
            ["in_is_sol", "in_is_sol_leg", "in_is_sol_token", "in_is_sol_flag"],
        )
        out_is_sol = find_first_match(
            columns,
            ["out_is_sol", "out_is_sol_leg", "out_is_sol_token", "out_is_sol_flag"],
        )
        direction = detect_direction_column(columns)
        if in_is_sol and out_is_sol:
            expr = (
                f"CASE WHEN {in_is_sol}=1 THEN {out_col} "
                f"WHEN {out_is_sol}=1 THEN {in_col} END"
            )
            return expr, "in_mint/out_mint with is_sol flags"
        if direction:
            expr = (
                f"CASE WHEN LOWER({direction})='buy' THEN {out_col} "
                f"WHEN LOWER({direction})='sell' THEN {in_col} END"
            )
            return expr, "in_mint/out_mint inferred via direction"
        return None, "in_mint/out_mint present but no direction/sol flags"

    return None, "no token column detected"


def ensure_case_else(token_expr: Optional[str]) -> Optional[str]:
    if not token_expr:
        return token_expr
    normalized = token_expr.strip().lower()
    if not normalized.startswith("case"):
        return token_expr
    if " else " in normalized:
        return token_expr
    if normalized.endswith("end"):
        end_index = token_expr.lower().rfind("end")
        return token_expr[:end_index] + " ELSE '__UNKNOWN_TOKEN__' " + token_expr[end_index:]
    return token_expr


def detect_profit_expr(columns: Sequence[str]) -> Tuple[Optional[str], Optional[float], str]:
    lower_cols = {col.lower() for col in columns}
    if "balance_delta_sol" in lower_cols:
        col = next(col for col in columns if col.lower() == "balance_delta_sol")
        return f"SUM({col})", None, "balance_delta_sol"

    has_dir = detect_direction_column(columns)
    if has_dir and "sol_amount_lamports" in lower_cols:
        col = next(col for col in columns if col.lower() == "sol_amount_lamports")
        expr = (
            f"SUM(CASE WHEN LOWER({has_dir})='sell' THEN {col} "
            f"WHEN LOWER({has_dir})='buy' THEN -{col} ELSE 0 END)"
        )
        return expr, 1_000_000_000.0, "sol_amount_lamports with direction"
    if has_dir and "sol_amount" in lower_cols:
        col = next(col for col in columns if col.lower() == "sol_amount")
        expr = (
            f"SUM(CASE WHEN LOWER({has_dir})='sell' THEN {col} "
            f"WHEN LOWER({has_dir})='buy' THEN -{col} ELSE 0 END)"
        )
        return expr, None, "sol_amount with direction"

    if "sol_received_lamports" in lower_cols and "sol_spent_lamports" in lower_cols:
        received = next(col for col in columns if col.lower() == "sol_received_lamports")
        spent = next(col for col in columns if col.lower() == "sol_spent_lamports")
        return f"SUM({received} - {spent})", 1_000_000_000.0, "sol_received_lamports - sol_spent_lamports"

    if "sol_received" in lower_cols and "sol_spent" in lower_cols:
        received = next(col for col in columns if col.lower() == "sol_received")
        spent = next(col for col in columns if col.lower() == "sol_spent")
        return f"SUM({received} - {spent})", None, "sol_received - sol_spent"

    return None, None, "no profit columns detected"


def build_profit_config(table_info_map: Dict[str, TableInfo]) -> ProfitConfig:
    swaps_config: Optional[ProfitConfig] = None
    if "swaps" in table_info_map:
        swaps_info = table_info_map["swaps"]
        token_expr, token_note = detect_token_expr(swaps_info.columns)
        time_col = detect_time_column(swaps_info.columns)
        profit_expr, divisor, profit_note = detect_profit_expr(swaps_info.columns)
        direction_col = detect_direction_column(swaps_info.columns)
        swaps_config = ProfitConfig(
            table="swaps",
            token_expr=ensure_case_else(token_expr),
            time_col=time_col,
            profit_expr=profit_expr,
            profit_divisor=divisor,
            direction_col=direction_col,
            profit_note=profit_note,
            token_note=token_note,
        )
        if token_expr and profit_expr:
            return swaps_config

    transfers_config: Optional[ProfitConfig] = None
    if "spl_transfers" in table_info_map:
        transfers_info = table_info_map["spl_transfers"]
        token_expr, token_note = detect_token_expr(transfers_info.columns)
        time_col = detect_time_column(transfers_info.columns)
        profit_expr, divisor, profit_note = detect_profit_expr(transfers_info.columns)
        direction_col = detect_direction_column(transfers_info.columns)
        transfers_config = ProfitConfig(
            table="spl_transfers",
            token_expr=ensure_case_else(token_expr),
            time_col=time_col,
            profit_expr=profit_expr,
            profit_divisor=divisor,
            direction_col=direction_col,
            profit_note=profit_note,
            token_note=token_note,
        )
        if token_expr and profit_expr:
            return transfers_config

    fallback = swaps_config or transfers_config
    if fallback and fallback.token_expr:
        return ProfitConfig(
            table=fallback.table,
            token_expr=fallback.token_expr,
            time_col=fallback.time_col,
            profit_expr=None,
            profit_divisor=None,
            direction_col=fallback.direction_col,
            profit_note="no realized profit available",
            token_note=fallback.token_note,
        )

    return ProfitConfig(
        table="",
        token_expr=None,
        time_col=None,
        profit_expr=None,
        profit_divisor=None,
        direction_col=None,
        profit_note="no realized profit available",
        token_note="no token source",
    )


def fetch_wallet_labels(conn: sqlite3.Connection) -> Dict[str, str]:
    try:
        rows = conn.execute(
            "SELECT wallet_address, wallet_label FROM wallets"
        ).fetchall()
        return {row[0]: row[1] for row in rows if row[0]}
    except sqlite3.Error:
        return {}


def fetch_wallet_profit(conn: sqlite3.Connection) -> Dict[str, float]:
    profit_map: Dict[str, float] = {}
    try:
        rows = conn.execute(
            """
            SELECT scan_wallet, SUM(balance_delta_sol) AS total_profit
            FROM tx
            WHERE err IS NULL
            GROUP BY scan_wallet
            """
        ).fetchall()
        for wallet, total_profit in rows:
            if wallet:
                profit_map[wallet] = float(total_profit or 0.0)
    except sqlite3.Error:
        return {}
    return profit_map


def build_base_query(config: ProfitConfig) -> Tuple[str, List[str]]:
    if not config.table or not config.token_expr:
        return "", []

    time_col = config.time_col or "NULL"
    direction = config.direction_col
    quote_list = ", ".join(f"'{mint}'" for mint in QUOTE_MINTS)
    select_parts = [
        f"scan_wallet AS wallet_address",
        f"{config.token_expr} AS token",
        "COUNT(*) AS trade_count",
        f"MIN({time_col}) AS first_seen_time",
    ]
    if direction and config.time_col:
        select_parts.append(
            f"MIN(CASE WHEN LOWER({direction})='buy' THEN {time_col} END) AS first_buy_time"
        )
        select_parts.append(
            f"MIN(CASE WHEN LOWER({direction})='sell' THEN {time_col} END) AS first_sell_time"
        )
    else:
        select_parts.append("NULL AS first_buy_time")
        select_parts.append("NULL AS first_sell_time")

    if config.profit_expr:
        select_parts.append(f"{config.profit_expr} AS realized_profit_raw")
    else:
        select_parts.append("NULL AS realized_profit_raw")

    sql = (
        "SELECT "
        + ", ".join(select_parts)
        + f" FROM {config.table}"
        + " WHERE scan_wallet IS NOT NULL"
    )
    if config.table == "swaps" and config.token_expr:
        sql += f" AND ({config.token_expr}) NOT IN ({quote_list})"
    sql += " GROUP BY scan_wallet, token"
    return sql, select_parts


def fetch_situations(
    conn: sqlite3.Connection,
    config: ProfitConfig,
    wallet_labels: Dict[str, str],
    wallet_profits: Dict[str, float],
) -> Tuple[List[SituationRow], int, int]:
    if not config.table or not config.token_expr:
        return [], 0, 0

    query, _ = build_base_query(config)
    excluded_quote_rows = 0
    if config.table == "swaps" and config.token_expr:
        quote_list = ", ".join(f"'{mint}'" for mint in QUOTE_MINTS)
        count_query = (
            f"SELECT COUNT(*) FROM {config.table} "
            f"WHERE scan_wallet IS NOT NULL AND ({config.token_expr}) IN ({quote_list})"
        )
        excluded_quote_rows = int(conn.execute(count_query).fetchone()[0] or 0)
    rows = conn.execute(query).fetchall()
    situations: List[SituationRow] = []
    unknown_token_rows = 0

    for row in rows:
        (
            wallet_address,
            token,
            trade_count,
            first_seen_time,
            first_buy_time,
            first_sell_time,
            realized_profit_raw,
        ) = row

        if token is None:
            token = "__UNKNOWN_TOKEN__"
            unknown_token_rows += 1
        if token in QUOTE_MINTS:
            excluded_quote_rows += 1
            continue

        realized_profit_sol = None
        if realized_profit_raw is not None:
            realized_profit_sol = float(realized_profit_raw)
            if config.profit_divisor:
                realized_profit_sol = realized_profit_sol / config.profit_divisor

        wallet_total_profit = wallet_profits.get(wallet_address, 0.0)
        has_token_profit = realized_profit_sol is not None
        situations.append(
            SituationRow(
                wallet_address=wallet_address,
                wallet_label=wallet_labels.get(wallet_address, ""),
                token=token,
                realized_profit_sol=realized_profit_sol,
                wallet_total_profit_sol=wallet_total_profit,
                trade_count=int(trade_count or 0),
                first_buy_time=first_buy_time,
                first_sell_time=first_sell_time,
                first_seen_time=first_seen_time,
                cohort_60s=None,
                cohort_300s=None,
                positive_total_wallet_profit_cohort_300s=None,
                positive_token_profit_cohort_300s=None,
                profit_source=config.table,
                profit_note=config.profit_note,
                has_token_profit=has_token_profit,
            )
        )

    unique_map: Dict[Tuple[str, str], SituationRow] = {}
    for situation in situations:
        key = (situation.wallet_address, situation.token)
        if key in unique_map:
            continue
        unique_map[key] = situation
    return list(unique_map.values()), unknown_token_rows, excluded_quote_rows


def compute_cohorts(
    situations: List[SituationRow],
    use_buy_time: bool,
    wallet_profits: Dict[str, float],
    has_token_profit: bool,
) -> None:
    token_map: Dict[str, List[Tuple[int, str, float, Optional[float]]]] = {}
    for situation in situations:
        time_value = situation.first_buy_time if use_buy_time else situation.first_seen_time
        if time_value is None:
            continue
        token_map.setdefault(situation.token, []).append(
            (
                int(time_value),
                situation.wallet_address,
                wallet_profits.get(situation.wallet_address, 0.0),
                situation.realized_profit_sol,
            )
        )

    for token, entries in token_map.items():
        entries.sort(key=lambda item: item[0])
        times = [item[0] for item in entries]
        wallet_profits_list = [item[2] for item in entries]
        token_profits_list = [item[3] for item in entries]
        wallets = [item[1] for item in entries]
        index_map = {wallet: idx for idx, wallet in enumerate(wallets)}

        for situation in situations:
            if situation.token != token:
                continue
            time_value = situation.first_buy_time if use_buy_time else situation.first_seen_time
            if time_value is None:
                continue
            idx = index_map.get(situation.wallet_address)
            if idx is None:
                continue
            lower_60 = bisect_left(times, time_value - 60)
            upper_60 = bisect_right(times, time_value + 60)
            lower_300 = bisect_left(times, time_value - 300)
            upper_300 = bisect_right(times, time_value + 300)
            situation.cohort_60s = max(upper_60 - lower_60 - 1, 0)
            situation.cohort_300s = max(upper_300 - lower_300 - 1, 0)
            positive_wallet_count = 0
            positive_token_count = 0
            for entry_idx in range(lower_300, upper_300):
                if entry_idx == idx:
                    continue
                if wallet_profits_list[entry_idx] > 0:
                    positive_wallet_count += 1
                if has_token_profit:
                    token_profit = token_profits_list[entry_idx]
                    if token_profit is not None and token_profit > 0:
                        positive_token_count += 1
            situation.positive_total_wallet_profit_cohort_300s = positive_wallet_count
            situation.positive_token_profit_cohort_300s = (
                positive_token_count if has_token_profit else None
            )


def write_tsv(path: str, rows: List[SituationRow]) -> None:
    headers = [
        "wallet_address",
        "wallet_label",
        "token",
        "realized_profit_sol",
        "wallet_total_profit_sol",
        "trade_count",
        "first_buy_time",
        "first_sell_time",
        "first_seen_time",
        "cohort_60s",
        "cohort_300s",
        "positive_total_wallet_profit_cohort_300s",
        "positive_token_profit_cohort_300s",
        "profit_source",
        "profit_note",
    ]
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(headers)
        for row in rows:
            writer.writerow(
                [
                    row.wallet_address,
                    row.wallet_label,
                    row.token,
                    "" if row.realized_profit_sol is None else f"{row.realized_profit_sol:.9f}",
                    f"{row.wallet_total_profit_sol:.9f}",
                    row.trade_count,
                    "" if row.first_buy_time is None else row.first_buy_time,
                    "" if row.first_sell_time is None else row.first_sell_time,
                    "" if row.first_seen_time is None else row.first_seen_time,
                    "" if row.cohort_60s is None else row.cohort_60s,
                    "" if row.cohort_300s is None else row.cohort_300s,
                    "" if row.positive_total_wallet_profit_cohort_300s is None else row.positive_total_wallet_profit_cohort_300s,
                    "" if row.positive_token_profit_cohort_300s is None else row.positive_token_profit_cohort_300s,
                    row.profit_source,
                    row.profit_note,
                ]
            )


def write_report(
    path: str,
    tables: List[str],
    table_info_map: Dict[str, TableInfo],
    config: ProfitConfig,
    situations: List[SituationRow],
    unknown_token_rows: int,
    excluded_quote_rows: int,
    all_count: int,
    profitable_count: int,
    cohort_label: str,
    wallet_profits: Dict[str, float],
) -> None:
    realized_non_null = [row for row in situations if row.realized_profit_sol is not None]
    absurd_rows = [
        row
        for row in situations
        if row.realized_profit_sol is not None
        and abs(row.realized_profit_sol)
        > max(abs(row.wallet_total_profit_sol) * 5, 50.0)
    ]
    abs_realized = sorted(abs(row.realized_profit_sol) for row in realized_non_null)
    median_abs_realized = None
    if abs_realized:
        mid = len(abs_realized) // 2
        if len(abs_realized) % 2 == 1:
            median_abs_realized = abs_realized[mid]
        else:
            median_abs_realized = (abs_realized[mid - 1] + abs_realized[mid]) / 2
    token_counter = Counter(row.token for row in situations)
    tokens = set(token_counter.keys())
    wallets = {row.wallet_address for row in situations}

    lines = []
    lines.append("Schema discovery")
    lines.append("=================")
    lines.append(f"Tables: {', '.join(tables)}")
    lines.append("")
    for table in tables:
        info = table_info_map[table]
        lines.append(f"Table: {table}")
        lines.append("Columns: " + ", ".join(info.columns))
        lines.append("")

    lines.append("Detection summary")
    lines.append("=================")
    lines.append(f"Profit table: {config.table or 'NONE'}")
    lines.append(f"Token expression: {config.token_expr or 'NONE'} ({config.token_note})")
    time_mode = "unknown"
    if config.time_col:
        time_mode = "slot-based" if config.time_col.lower() == "slot" else "epoch-seconds"
    lines.append(f"Time column: {config.time_col or 'NONE'} ({time_mode})")
    lines.append(f"Direction column: {config.direction_col or 'NONE'}")
    lines.append(f"Profit expression: {config.profit_expr or 'NONE'} ({config.profit_note})")
    if config.profit_expr is None:
        lines.append("token-scoped cohort profitability not available")
    lines.append("")

    lines.append("Counts")
    lines.append("======")
    lines.append(f"wallets: {len(wallets)}")
    lines.append(f"tokens_after_exclusion: {len(tokens)}")
    lines.append(f"situations_all: {all_count}")
    lines.append(f"situations_profitable: {profitable_count}")
    if situations:
        percent = (len(realized_non_null) / len(situations)) * 100
        lines.append(f"% with realized_profit_sol: {percent:.2f}%")
    lines.append(f"cohort mode: {cohort_label}")
    lines.append(f"unknown_token_rows: {unknown_token_rows}")
    lines.append(f"excluded_quote_rows: {excluded_quote_rows}")
    if unknown_token_rows > 0:
        lines.append("WARNING: found NULL token values; mapped to __UNKNOWN_TOKEN__")
    if excluded_quote_rows > 0:
        lines.append("WARNING: excluded quote-mint rows (USDC/WSOL)")
    if token_counter:
        lines.append("top_tokens_by_situations:")
        for token, count in token_counter.most_common(10):
            lines.append(f"  {token}: {count}")
    lines.append("")

    lines.append("Sanity checks")
    lines.append("=============")
    lines.append("absurd profit rows (>5x wallet total or >50 SOL absolute): " + str(len(absurd_rows)))
    for row in absurd_rows[:20]:
        lines.append(
            f"wallet={row.wallet_address} token={row.token} "
            f"realized={row.realized_profit_sol} wallet_total={row.wallet_total_profit_sol}"
        )
    if median_abs_realized is not None:
        if median_abs_realized > 1000:
            lines.append(
                "WARNING: median |realized_profit_sol| > 1000; check for lamports-vs-SOL scaling"
            )
        else:
            lines.append(f"median |realized_profit_sol|: {median_abs_realized:.6f}")
    lines.append("")

    lines.append("Wallet total profit computed from tx.balance_delta_sol (err IS NULL)")

    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract profit situations from Solana wallet scan DB")
    parser.add_argument("--db", default="masterwalletsdb.db", help="Path to sqlite DB")
    parser.add_argument("--out-dir", default=".", help="Output directory")
    args = parser.parse_args()

    db_path = args.db
    out_dir = args.out_dir

    if not os.path.exists(db_path):
        raise SystemExit(f"DB not found: {db_path}")

    os.makedirs(out_dir, exist_ok=True)
    all_path = os.path.join(out_dir, "profit_situations_all.tsv")
    profitable_path = os.path.join(out_dir, "profit_situations_profitable.tsv")
    report_path = os.path.join(out_dir, "profit_situations_report.txt")

    conn = sqlite3.connect(db_path)
    try:
        tables = list_tables(conn)
        table_info_map = {table: table_info(conn, table) for table in tables}

        wallet_labels = fetch_wallet_labels(conn)
        wallet_profits = fetch_wallet_profit(conn)

        config = build_profit_config(table_info_map)
        situations, unknown_token_rows, excluded_quote_rows = fetch_situations(
            conn, config, wallet_labels, wallet_profits
        )

        use_buy_time = config.direction_col is not None and config.time_col is not None
        cohort_label = "first_buy_time" if use_buy_time else "first_seen_time"
        compute_cohorts(
            situations,
            use_buy_time=use_buy_time,
            wallet_profits=wallet_profits,
            has_token_profit=config.profit_expr is not None,
        )

        write_tsv(all_path, situations)
        profitable_rows = [
            row for row in situations if row.realized_profit_sol is not None and row.realized_profit_sol > 0
        ]
        write_tsv(profitable_path, profitable_rows)
        write_report(
            report_path,
            tables,
            table_info_map,
            config,
            situations,
            unknown_token_rows,
            excluded_quote_rows,
            len(situations),
            len(profitable_rows),
            cohort_label,
            wallet_profits,
        )

        print("Schema map:")
        for table in tables:
            info = table_info_map[table]
            time_col = detect_time_column(info.columns)
            token_cols = find_matches(info.columns, TOKEN_CANDIDATES)
            time_mode = "unknown"
            if time_col:
                if time_col.lower() == "slot":
                    time_mode = "slot-based"
                else:
                    time_mode = "epoch-seconds"
            print(f"- {table}")
            print(f"  columns: {', '.join(info.columns)}")
            if time_col:
                print(f"  detected time column: {time_col} ({time_mode})")
            if token_cols:
                print(f"  detected token columns: {', '.join(token_cols)}")

        print(f"\nWrote {all_path}")
        print(f"Wrote {profitable_path}")
        print(f"Wrote {report_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
