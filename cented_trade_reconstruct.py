#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import sqlite3
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

LAMPORTS_PER_SOL = 1_000_000_000

TOKEN_IN_DIRS = {"in", "buy", "b"}
TOKEN_OUT_DIRS = {"out", "sell", "s"}
SOL_IN_DIRS = {"in", "sell", "s"}
SOL_OUT_DIRS = {"out", "buy", "b"}


class ReconstructionError(RuntimeError):
    pass


@dataclass
class SourceSpec:
    mode: str  # wallet_token_flow | swaps
    table: str
    wallet_col: str
    ts_col: str
    mint_col: str
    token_amount_col: str
    sol_direction_col: str
    sol_amount_col: str
    flow_direction_col: Optional[str] = None
    has_sol_leg_col: Optional[str] = None


@dataclass
class Event:
    ts: int
    mint: str
    token_delta_raw: int
    sol_delta_lamports: int
    row_id: str


@dataclass
class TradeAgg:
    mint: str
    first_ts: int
    last_ts: int
    entry_time: Optional[int] = None
    exit_time: Optional[int] = None
    entry_sol: float = 0.0
    exit_sol: float = 0.0
    buys_count: int = 0
    sells_count: int = 0


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({qident(table)})").fetchall()
    return [r[1] for r in rows]


def load_schema(conn: sqlite3.Connection) -> Dict[str, List[str]]:
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return {t[0]: table_columns(conn, t[0]) for t in tables}


def find_col(cols: Sequence[str], name: str) -> Optional[str]:
    by_lower = {c.lower(): c for c in cols}
    return by_lower.get(name.lower())


def require_cols(table: str, cols: Sequence[str], required: Sequence[str]) -> Dict[str, str]:
    mapped: Dict[str, str] = {}
    missing: List[str] = []
    for req in required:
        c = find_col(cols, req)
        if c is None:
            missing.append(req)
        else:
            mapped[req] = c
    if missing:
        raise ReconstructionError(
            f"Table {table} is present but missing required columns: {missing}. Found: {list(cols)}"
        )
    return mapped


def detect_source(schema: Dict[str, List[str]]) -> SourceSpec:
    if "wallet_token_flow" in schema:
        cols = schema["wallet_token_flow"]
        m = require_cols(
            "wallet_token_flow",
            cols,
            [
                "scan_wallet",
                "block_time",
                "token_mint",
                "token_amount_raw",
                "flow_direction",
                "sol_direction",
                "sol_amount_lamports",
            ],
        )
        return SourceSpec(
            mode="wallet_token_flow",
            table="wallet_token_flow",
            wallet_col=m["scan_wallet"],
            ts_col=m["block_time"],
            mint_col=m["token_mint"],
            token_amount_col=m["token_amount_raw"],
            sol_direction_col=m["sol_direction"],
            sol_amount_col=m["sol_amount_lamports"],
            flow_direction_col=m["flow_direction"],
        )

    if "swaps" in schema:
        cols = schema["swaps"]
        m = require_cols(
            "swaps",
            cols,
            [
                "scan_wallet",
                "block_time",
                "token_mint",
                "token_amount_raw",
                "has_sol_leg",
                "sol_direction",
                "sol_amount_lamports",
            ],
        )
        return SourceSpec(
            mode="swaps",
            table="swaps",
            wallet_col=m["scan_wallet"],
            ts_col=m["block_time"],
            mint_col=m["token_mint"],
            token_amount_col=m["token_amount_raw"],
            sol_direction_col=m["sol_direction"],
            sol_amount_col=m["sol_amount_lamports"],
            has_sol_leg_col=m["has_sol_leg"],
        )

    raise ReconstructionError(
        "Neither wallet_token_flow (primary) nor swaps (fallback) found. "
        f"Tables found: {sorted(schema.keys())}"
    )


def normalize_dir(val: object) -> Optional[str]:
    if val is None:
        return None
    return str(val).strip().lower()


def build_time_bounds(
    conn: sqlite3.Connection,
    source: SourceSpec,
    wallet: str,
    time_min: Optional[int],
    time_max: Optional[int],
    window_hours: Optional[int],
) -> Tuple[Optional[int], Optional[int]]:
    if time_min is not None or time_max is not None:
        return time_min, time_max
    if window_hours is None:
        return None, None

    q = (
        f"SELECT MAX({qident(source.ts_col)}) FROM {qident(source.table)} "
        f"WHERE {qident(source.wallet_col)} = ?"
    )
    row = conn.execute(q, (wallet,)).fetchone()
    if row is None or row[0] is None:
        raise ReconstructionError(f"Cannot apply --window-hours: no rows for wallet {wallet} in {source.table}.")
    max_ts = int(row[0])
    return max_ts - (window_hours * 3600), max_ts


def where_wallet_time(source: SourceSpec, time_min: Optional[int], time_max: Optional[int]) -> Tuple[str, List[object]]:
    where = [f"{qident(source.wallet_col)} = ?"]
    params: List[object] = []
    if time_min is not None:
        where.append(f"{qident(source.ts_col)} >= ?")
        params.append(time_min)
    if time_max is not None:
        where.append(f"{qident(source.ts_col)} <= ?")
        params.append(time_max)
    if source.mode == "swaps":
        where.append(f"COALESCE({qident(source.has_sol_leg_col)}, 0) = 1")
    return " AND ".join(where), params


def fetch_rows(
    conn: sqlite3.Connection,
    source: SourceSpec,
    wallet: str,
    time_min: Optional[int],
    time_max: Optional[int],
) -> Iterable[sqlite3.Row]:
    where_sql, extra = where_wallet_time(source, time_min, time_max)
    params = [wallet, *extra]
    q = f"SELECT * FROM {qident(source.table)} WHERE {where_sql} ORDER BY {qident(source.ts_col)} ASC"
    for row in conn.execute(q, params):
        yield row


def fetch_direction_enums(
    conn: sqlite3.Connection,
    source: SourceSpec,
    wallet: str,
    time_min: Optional[int],
    time_max: Optional[int],
) -> Tuple[List[str], List[str]]:
    where_sql, extra = where_wallet_time(source, time_min, time_max)
    params = [wallet, *extra]

    flow_vals: List[str] = []
    if source.flow_direction_col is not None:
        qf = (
            f"SELECT DISTINCT lower(trim({qident(source.flow_direction_col)})) AS v "
            f"FROM {qident(source.table)} WHERE {where_sql} ORDER BY v"
        )
        flow_vals = [r[0] for r in conn.execute(qf, params).fetchall() if r[0] is not None]

    qs = (
        f"SELECT DISTINCT lower(trim({qident(source.sol_direction_col)})) AS v "
        f"FROM {qident(source.table)} WHERE {where_sql} ORDER BY v"
    )
    sol_vals = [r[0] for r in conn.execute(qs, params).fetchall() if r[0] is not None]
    return flow_vals, sol_vals


def sample_rows_for_error(
    conn: sqlite3.Connection,
    source: SourceSpec,
    wallet: str,
    time_min: Optional[int],
    time_max: Optional[int],
    bad_field: str,
) -> List[str]:
    where_sql, extra = where_wallet_time(source, time_min, time_max)
    params = [wallet, *extra]
    if bad_field == "flow_direction":
        bad_condition = (
            f"lower(trim({qident(source.flow_direction_col)})) NOT IN ('in','buy','b','out','sell','s') "
            f"OR {qident(source.flow_direction_col)} IS NULL"
        )
    else:
        bad_condition = (
            f"({qident(source.sol_direction_col)} IS NOT NULL AND "
            f"lower(trim({qident(source.sol_direction_col)})) NOT IN ('in','sell','s','out','buy','b'))"
        )

    fields = [
        "signature" if find_col(table_columns(conn, source.table), "signature") else None,
        source.ts_col,
        source.mint_col,
        source.token_amount_col,
        source.flow_direction_col if source.flow_direction_col is not None else None,
        source.sol_direction_col,
        source.sol_amount_col,
    ]
    fields = [f for f in fields if f is not None]
    select = ", ".join(f"{qident(f)}" for f in fields)
    q = (
        f"SELECT {select} FROM {qident(source.table)} WHERE {where_sql} AND ({bad_condition}) "
        f"ORDER BY {qident(source.ts_col)} ASC LIMIT 10"
    )
    out: List[str] = []
    for row in conn.execute(q, params):
        vals = []
        for f in fields:
            vals.append(f"{f}={row[f]}")
        out.append("; ".join(vals))
    return out


def derive_event_wallet_token_flow(
    row: sqlite3.Row,
    source: SourceSpec,
) -> Tuple[Event, bool]:
    row_id = str(row["signature"]) if "signature" in row.keys() and row["signature"] is not None else f"ts={row[source.ts_col]}"
    ts = int(row[source.ts_col])
    mint = str(row[source.mint_col])
    token_amt = int(row[source.token_amount_col])

    flow_dir = normalize_dir(row[source.flow_direction_col])
    if flow_dir in TOKEN_IN_DIRS:
        token_delta_raw = token_amt
    elif flow_dir in TOKEN_OUT_DIRS:
        token_delta_raw = -token_amt
    else:
        raise ReconstructionError(f"Unsupported flow_direction value encountered in row {row_id}: {row[source.flow_direction_col]}")

    sol_dir = normalize_dir(row[source.sol_direction_col])
    lamports_raw = row[source.sol_amount_col]

    if sol_dir in SOL_IN_DIRS:
        sol_delta_lamports = int(lamports_raw) if lamports_raw is not None else 0
        return Event(ts=ts, mint=mint, token_delta_raw=token_delta_raw, sol_delta_lamports=sol_delta_lamports, row_id=row_id), (
            lamports_raw is None
        )
    if sol_dir in SOL_OUT_DIRS:
        sol_delta_lamports = -(int(lamports_raw) if lamports_raw is not None else 0)
        return Event(ts=ts, mint=mint, token_delta_raw=token_delta_raw, sol_delta_lamports=sol_delta_lamports, row_id=row_id), (
            lamports_raw is None
        )

    if sol_dir is None and lamports_raw is not None:
        # Required anomaly treatment: keep event with 0 SOL delta.
        return Event(ts=ts, mint=mint, token_delta_raw=token_delta_raw, sol_delta_lamports=0, row_id=row_id), False

    raise ReconstructionError(
        f"Unsupported sol_direction value encountered in row {row_id}: sol_direction={row[source.sol_direction_col]} sol_amount_lamports={lamports_raw}"
    )


def derive_event_swaps(row: sqlite3.Row, source: SourceSpec) -> Tuple[Event, bool]:
    row_id = str(row["signature"]) if "signature" in row.keys() and row["signature"] is not None else f"ts={row[source.ts_col]}"
    ts = int(row[source.ts_col])
    mint = str(row[source.mint_col])
    token_amt = int(row[source.token_amount_col])

    sol_dir = normalize_dir(row[source.sol_direction_col])
    lamports_raw = row[source.sol_amount_col]

    if sol_dir in SOL_OUT_DIRS:
        token_delta_raw = token_amt
        sol_delta_lamports = -(int(lamports_raw) if lamports_raw is not None else 0)
        return Event(ts=ts, mint=mint, token_delta_raw=token_delta_raw, sol_delta_lamports=sol_delta_lamports, row_id=row_id), (
            lamports_raw is None
        )
    if sol_dir in SOL_IN_DIRS:
        token_delta_raw = -token_amt
        sol_delta_lamports = int(lamports_raw) if lamports_raw is not None else 0
        return Event(ts=ts, mint=mint, token_delta_raw=token_delta_raw, sol_delta_lamports=sol_delta_lamports, row_id=row_id), (
            lamports_raw is None
        )

    if sol_dir is None and lamports_raw is not None:
        # required anomaly treatment
        return Event(ts=ts, mint=mint, token_delta_raw=0, sol_delta_lamports=0, row_id=row_id), False

    raise ReconstructionError(
        f"Unsupported sol_direction value encountered in swaps row {row_id}: sol_direction={row[source.sol_direction_col]} sol_amount_lamports={lamports_raw}"
    )


def percentile(values: Sequence[float], p: float) -> Optional[float]:
    if not values:
        return None
    arr = sorted(float(v) for v in values)
    if len(arr) == 1:
        return arr[0]
    pos = (len(arr) - 1) * p
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return arr[lo]
    frac = pos - lo
    return arr[lo] * (1 - frac) + arr[hi] * frac


def fmt_num(v: Optional[float], ndigits: int = 9) -> str:
    if v is None:
        return ""
    return f"{v:.{ndigits}f}"


def write_tsv(path: Path, header: Sequence[str], rows: Iterable[Sequence[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(header)
        for r in rows:
            w.writerow(r)


def print_inspect(schema: Dict[str, List[str]]) -> None:
    print("Detected tables/columns:")
    for t in sorted(schema):
        print(f"- {t}: {', '.join(schema[t])}")
    print("\nSource detection:")
    print("- wallet_token_flow found" if "wallet_token_flow" in schema else "- wallet_token_flow not found")
    print("- swaps found" if "swaps" in schema else "- swaps not found")


def fail_with_schema_and_enums(schema: Dict[str, List[str]], flow_vals: List[str], sol_vals: List[str], msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    print(f"Direction enums: flow_direction={flow_vals} sol_direction={sol_vals}", file=sys.stderr)
    print("Schema summary:", file=sys.stderr)
    for t in sorted(schema):
        print(f"- {t}: {', '.join(schema[t])}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconstruct token trades for a wallet from SQLite swap cashflows")
    parser.add_argument("--db", required=True)
    parser.add_argument("--wallet", required=True)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--time-min", type=int)
    parser.add_argument("--time-max", type=int)
    parser.add_argument("--window-hours", type=int)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--inspect", action="store_true")
    parser.add_argument("--dump-enum", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"ERROR: DB path does not exist: {db_path}")

    conn = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    schema: Dict[str, List[str]] = {}
    flow_vals: List[str] = []
    sol_vals: List[str] = []
    try:
        schema = load_schema(conn)
        if args.inspect:
            print_inspect(schema)
            return

        source = detect_source(schema)
        time_min, time_max = build_time_bounds(conn, source, args.wallet, args.time_min, args.time_max, args.window_hours)

        flow_vals, sol_vals = fetch_direction_enums(conn, source, args.wallet, time_min, time_max)
        if args.verbose:
            print(f"Using source={source.table} mode={source.mode} time_min={time_min} time_max={time_max}")
            print(f"Enum flow_direction={flow_vals}")
            print(f"Enum sol_direction={sol_vals}")

        if args.dump_enum:
            print(f"flow_direction: {flow_vals}")
            print(f"sol_direction: {sol_vals}")
            return

        by_mint: Dict[str, TradeAgg] = {}
        sell_without_buy: set[str] = set()
        anomaly_by_mint: Dict[str, List[str]] = defaultdict(list)

        total_events = 0
        buy_events = 0
        sell_events = 0
        anomaly_events = 0
        null_sol_amount_with_direction = 0
        null_sol_direction_with_amount = 0

        for row in fetch_rows(conn, source, args.wallet, time_min, time_max):
            total_events += 1
            try:
                if source.mode == "wallet_token_flow":
                    ev, had_null_amount = derive_event_wallet_token_flow(row, source)
                else:
                    ev, had_null_amount = derive_event_swaps(row, source)
            except ReconstructionError as e:
                bad_field = "flow_direction" if "flow_direction" in str(e) else "sol_direction"
                examples = sample_rows_for_error(conn, source, args.wallet, time_min, time_max, bad_field)
                raise ReconstructionError(str(e) + f". Example rows: {examples}")

            if had_null_amount:
                null_sol_amount_with_direction += 1
            if row[source.sol_direction_col] is None and row[source.sol_amount_col] is not None:
                null_sol_direction_with_amount += 1

            agg = by_mint.get(ev.mint)
            if agg is None:
                agg = TradeAgg(mint=ev.mint, first_ts=ev.ts, last_ts=ev.ts)
                by_mint[ev.mint] = agg
            agg.first_ts = min(agg.first_ts, ev.ts)
            agg.last_ts = max(agg.last_ts, ev.ts)

            is_buy = ev.token_delta_raw > 0 and ev.sol_delta_lamports < 0
            is_sell = ev.token_delta_raw < 0 and ev.sol_delta_lamports > 0

            if is_buy:
                buy_events += 1
                agg.buys_count += 1
                if agg.entry_time is None:
                    agg.entry_time = ev.ts
                agg.entry_sol += (-ev.sol_delta_lamports) / LAMPORTS_PER_SOL
            elif is_sell:
                sell_events += 1
                agg.sells_count += 1
                agg.exit_time = ev.ts
                agg.exit_sol += ev.sol_delta_lamports / LAMPORTS_PER_SOL
                if agg.buys_count == 0:
                    sell_without_buy.add(ev.mint)
            else:
                anomaly_events += 1
                if len(anomaly_by_mint[ev.mint]) < 3:
                    anomaly_by_mint[ev.mint].append(
                        f"{ev.row_id} ts={ev.ts} token_delta_raw={ev.token_delta_raw} sol_delta_lamports={ev.sol_delta_lamports}"
                    )

        trades_rows: List[List[object]] = []
        closed_for_equity: List[Tuple[int, float]] = []
        net_closed: List[float] = []
        roi_closed: List[float] = []
        hold_closed: List[float] = []
        entry_closed: List[float] = []

        total_sol_spent = 0.0
        total_sol_received = 0.0

        for mint in sorted(by_mint, key=lambda m: (by_mint[m].entry_time if by_mint[m].entry_time is not None else 10**20, m)):
            t = by_mint[mint]
            total_sol_spent += t.entry_sol
            total_sol_received += t.exit_sol

            status = "CLOSED" if t.exit_time is not None else "OPEN"
            net_sol = t.exit_sol - t.entry_sol
            roi = (net_sol / t.entry_sol) if t.entry_sol > 0 else None
            hold = (t.exit_time - t.entry_time) if (t.entry_time is not None and t.exit_time is not None) else None

            if status == "CLOSED" and t.entry_time is not None and t.exit_time is not None:
                closed_for_equity.append((t.exit_time, net_sol))
                net_closed.append(net_sol)
                if roi is not None:
                    roi_closed.append(roi)
                hold_closed.append(float(hold))
                entry_closed.append(t.entry_sol)

            trades_rows.append(
                [
                    mint,
                    status,
                    t.entry_time if t.entry_time is not None else "",
                    t.exit_time if t.exit_time is not None else "",
                    hold if hold is not None else "",
                    fmt_num(t.entry_sol),
                    fmt_num(t.exit_sol),
                    fmt_num(net_sol),
                    fmt_num(roi),
                    t.buys_count,
                    t.sells_count,
                    t.buys_count,
                    t.sells_count,
                    t.first_ts,
                    t.last_ts,
                ]
            )

        outdir = Path(args.outdir)
        outdir.mkdir(parents=True, exist_ok=True)

        trades_path = outdir / "cented_trades.tsv"
        write_tsv(
            trades_path,
            [
                "mint",
                "status",
                "entry_time",
                "exit_time",
                "hold_seconds",
                "entry_sol",
                "exit_sol",
                "net_sol",
                "roi_on_cost",
                "buys_count",
                "sells_count",
                "buy_events",
                "sell_events",
                "first_ts",
                "last_ts",
            ],
            trades_rows,
        )

        closed_for_equity.sort(key=lambda x: x[0])
        cum = 0.0
        equity_rows: List[List[object]] = []
        for i, (ts, net) in enumerate(closed_for_equity, start=1):
            cum += net
            equity_rows.append([ts, fmt_num(cum), i])
        equity_path = outdir / "cented_equity_curve.tsv"
        write_tsv(equity_path, ["timestamp", "cumulative_net_sol", "trade_count"], equity_rows)

        wins = [x for x in net_closed if x > 0]
        losses = [x for x in net_closed if x < 0]

        win_rate = (len(wins) / len(net_closed)) if net_closed else None
        profit_factor = (sum(wins) / abs(sum(losses))) if losses else (math.inf if wins else None)
        expectancy = statistics.mean(net_closed) if net_closed else None

        roi_ps = {p: percentile(roi_closed, p) for p in [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]}
        net_ps = {p: percentile(net_closed, p) for p in [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]}
        hold_ps = {p: percentile(hold_closed, p) for p in [0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]}
        entry_ps = {p: percentile(entry_closed, p) for p in [0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]}

        peak = 0.0
        running = 0.0
        max_drawdown = 0.0
        for _, n in closed_for_equity:
            running += n
            peak = max(peak, running)
            max_drawdown = max(max_drawdown, peak - running)

        by_hour = Counter()
        for ts, _ in closed_for_equity:
            by_hour[(ts // 3600) % 24] += 1

        dist_path = outdir / "cented_distributions.txt"
        with dist_path.open("w", encoding="utf-8") as fh:
            fh.write(f"trade_count_closed\t{len(closed_for_equity)}\n")
            fh.write(f"trade_count_open\t{sum(1 for t in by_mint.values() if t.exit_time is None)}\n")
            fh.write(f"win_rate\t{fmt_num(win_rate, 6)}\n")
            fh.write(f"profit_factor\t{('inf' if profit_factor == math.inf else fmt_num(profit_factor, 6))}\n")
            fh.write(f"avg_win_sol\t{fmt_num(statistics.mean(wins) if wins else None)}\n")
            fh.write(f"avg_loss_sol\t{fmt_num(statistics.mean(losses) if losses else None)}\n")
            fh.write(f"median_win_sol\t{fmt_num(statistics.median(wins) if wins else None)}\n")
            fh.write(f"median_loss_sol\t{fmt_num(statistics.median(losses) if losses else None)}\n")
            fh.write(f"expectancy_per_trade\t{fmt_num(expectancy)}\n")

            fh.write("ROI_percentiles\n")
            for pp in [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]:
                fh.write(f"p{int(pp * 100)}\t{fmt_num(roi_ps[pp], 6)}\n")

            fh.write("NetSOL_percentiles\n")
            for pp in [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]:
                fh.write(f"p{int(pp * 100)}\t{fmt_num(net_ps[pp])}\n")

            fh.write("HoldSeconds_percentiles\n")
            for pp in [0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]:
                fh.write(f"p{int(pp * 100)}\t{fmt_num(hold_ps[pp], 3)}\n")

            fh.write("EntrySOL_percentiles\n")
            for pp in [0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]:
                fh.write(f"p{int(pp * 100)}\t{fmt_num(entry_ps[pp])}\n")

            fh.write(f"max_drawdown_sol\t{fmt_num(max_drawdown)}\n")
            fh.write("trades_per_hour\n")
            for h in range(24):
                fh.write(f"hour_{h:02d}\t{by_hour.get(h, 0)}\n")

            fh.write("loss_cap_signature\n")
            for pp in [0.01, 0.05, 0.10]:
                fh.write(f"roi_p{int(pp * 100)}\t{fmt_num(roi_ps[pp], 6)}\n")
            for pp in [0.01, 0.05, 0.10]:
                fh.write(f"net_sol_p{int(pp * 100)}\t{fmt_num(net_ps[pp])}\n")

        open_count = sum(1 for t in by_mint.values() if t.buys_count > 0 and t.sells_count == 0)

        print(f"Validation: source_table={source.table}")
        print(f"Validation: total_events_in_window={total_events}")
        print(f"Validation: buy_events={buy_events}")
        print(f"Validation: sell_events={sell_events}")
        print(f"Validation: anomaly_events={anomaly_events}")
        print(f"Validation: mints_seen={len(by_mint)}")
        print(f"Validation: open_trades_count={open_count}")
        print(f"Validation: sell_without_buy={','.join(sorted(sell_without_buy)) if sell_without_buy else '<none>'}")
        print(f"Validation: total_sol_spent={fmt_num(total_sol_spent)}")
        print(f"Validation: total_sol_received={fmt_num(total_sol_received)}")
        print(f"Validation: null_sol_amount_with_direction={null_sol_amount_with_direction}")
        print(f"Validation: null_sol_direction_with_amount={null_sol_direction_with_amount}")
        if anomaly_by_mint:
            print("Validation: anomaly_details_per_mint")
            for mint in sorted(anomaly_by_mint):
                print(f"  {mint}: {' | '.join(anomaly_by_mint[mint])}")

        print(f"OK: wrote {trades_path} {equity_path} {dist_path}")

    except ReconstructionError as exc:
        fail_with_schema_and_enums(schema, flow_vals, sol_vals, str(exc))
        raise SystemExit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
