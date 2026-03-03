#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import sqlite3
import statistics
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


KNOWN_WSOL_MINT = "So11111111111111111111111111111111111111112"

WALLET_COL_CANDIDATES = [
    "wallet",
    "scan_wallet",
    "wallet_address",
    "owner_wallet",
    "trader",
    "user",
    "user_wallet",
    "owner",
    "account",
]
MINT_COL_CANDIDATES = [
    "mint",
    "token_mint",
    "base_mint",
    "token_address",
    "asset_mint",
]
TS_COL_CANDIDATES = ["ts", "timestamp", "block_time", "time", "slot_time", "unix_time"]
TOKEN_DELTA_COL_CANDIDATES = [
    "token_delta",
    "wallet_delta_token",
    "delta_token",
    "amount_delta",
    "token_change",
]
SOL_DELTA_COL_CANDIDATES = [
    "sol_delta",
    "wallet_delta_sol",
    "delta_sol",
    "native_delta",
    "base_delta",
]

IN_MINT_COL_CANDIDATES = ["in_mint", "mint_in", "token_in_mint", "input_mint", "from_mint"]
OUT_MINT_COL_CANDIDATES = ["out_mint", "mint_out", "token_out_mint", "output_mint", "to_mint"]
AMOUNT_IN_COL_CANDIDATES = ["amount_in", "in_amount", "token_in_amount", "input_amount", "from_amount"]
AMOUNT_OUT_COL_CANDIDATES = ["amount_out", "out_amount", "token_out_amount", "output_amount", "to_amount"]


class ReconstructionError(RuntimeError):
    pass


@dataclass
class SourceMapping:
    table: str
    wallet_col: str
    ts_col: str
    mode: str  # direct | derived
    mint_col: Optional[str] = None
    token_delta_col: Optional[str] = None
    sol_delta_col: Optional[str] = None
    in_mint_col: Optional[str] = None
    out_mint_col: Optional[str] = None
    amount_in_col: Optional[str] = None
    amount_out_col: Optional[str] = None


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


def find_first(columns: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    by_lower = {c.lower(): c for c in columns}
    for c in candidates:
        if c.lower() in by_lower:
            return by_lower[c.lower()]
    return None


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({qident(table)})").fetchall()
    return [r[1] for r in rows]


def load_schema(conn: sqlite3.Connection) -> Dict[str, List[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    schema: Dict[str, List[str]] = {}
    for (table,) in rows:
        schema[table] = table_columns(conn, table)
    return schema


def detect_mappings(schema: Dict[str, List[str]]) -> Tuple[List[SourceMapping], List[SourceMapping]]:
    direct: List[SourceMapping] = []
    derived: List[SourceMapping] = []
    for table, cols in schema.items():
        wallet = find_first(cols, WALLET_COL_CANDIDATES)
        ts = find_first(cols, TS_COL_CANDIDATES)
        if not wallet or not ts:
            continue

        mint = find_first(cols, MINT_COL_CANDIDATES)
        token_delta = find_first(cols, TOKEN_DELTA_COL_CANDIDATES)
        sol_delta = find_first(cols, SOL_DELTA_COL_CANDIDATES)
        if mint and token_delta and sol_delta:
            direct.append(
                SourceMapping(
                    table=table,
                    wallet_col=wallet,
                    ts_col=ts,
                    mode="direct",
                    mint_col=mint,
                    token_delta_col=token_delta,
                    sol_delta_col=sol_delta,
                )
            )

        in_mint = find_first(cols, IN_MINT_COL_CANDIDATES)
        out_mint = find_first(cols, OUT_MINT_COL_CANDIDATES)
        amount_in = find_first(cols, AMOUNT_IN_COL_CANDIDATES)
        amount_out = find_first(cols, AMOUNT_OUT_COL_CANDIDATES)
        if in_mint and out_mint and amount_in and amount_out:
            derived.append(
                SourceMapping(
                    table=table,
                    wallet_col=wallet,
                    ts_col=ts,
                    mode="derived",
                    in_mint_col=in_mint,
                    out_mint_col=out_mint,
                    amount_in_col=amount_in,
                    amount_out_col=amount_out,
                )
            )
    return direct, derived


def pick_mapping(direct: List[SourceMapping], derived: List[SourceMapping]) -> SourceMapping:
    if len(direct) == 1:
        return direct[0]
    if len(direct) > 1:
        tables = ", ".join(f"{m.table}" for m in direct)
        raise ReconstructionError(
            "Ambiguous direct swap sources. Multiple tables match required direct fields: " + tables
        )
    if len(derived) == 1:
        return derived[0]
    if len(derived) > 1:
        tables = ", ".join(f"{m.table}" for m in derived)
        raise ReconstructionError(
            "No unambiguous direct source found; multiple derivable swap sources found: " + tables
        )
    raise ReconstructionError(
        "Unable to locate a table with required wallet/mint/timestamp/token_delta/sol_delta fields or unambiguous derivation fields."
    )


def infer_sol_mints(conn: sqlite3.Connection, mapping: SourceMapping, wallet: str) -> List[str]:
    candidates: Counter[str] = Counter()
    literals = {"sol", "wsol", "wrapped sol", "wrapped_sol", "native sol", KNOWN_WSOL_MINT.lower()}

    if mapping.mode == "direct":
        q = (
            f"SELECT {qident(mapping.mint_col)} AS mint, COUNT(*) AS c "
            f"FROM {qident(mapping.table)} WHERE {qident(mapping.wallet_col)} = ? "
            "GROUP BY mint ORDER BY c DESC LIMIT 200"
        )
        rows = conn.execute(q, (wallet,)).fetchall()
        for mint, count in rows:
            if mint is None:
                continue
            m = str(mint)
            ml = m.lower()
            if ml in literals or "sol" in ml:
                candidates[m] += int(count)
    else:
        q = (
            f"SELECT {qident(mapping.in_mint_col)} AS in_mint, {qident(mapping.out_mint_col)} AS out_mint "
            f"FROM {qident(mapping.table)} WHERE {qident(mapping.wallet_col)} = ? LIMIT 20000"
        )
        for in_mint, out_mint in conn.execute(q, (wallet,)):
            for mint in (in_mint, out_mint):
                if mint is None:
                    continue
                m = str(mint)
                ml = m.lower()
                if ml in literals or "sol" in ml:
                    candidates[m] += 1

    sure = []
    for mint in candidates:
        ml = mint.lower()
        if ml == KNOWN_WSOL_MINT.lower() or ml in {"sol", "wsol", "wrapped sol", "wrapped_sol", "native sol"}:
            sure.append(mint)

    if sure:
        return sorted(set(sure))
    return sorted(candidates.keys())


def to_float(value: object) -> float:
    if value is None:
        raise ValueError("NULL numeric value")
    return float(value)


def to_int(value: object) -> int:
    if value is None:
        raise ValueError("NULL timestamp")
    return int(value)


def percentile(values: Sequence[float], p: float) -> Optional[float]:
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    arr = sorted(float(x) for x in values)
    pos = (len(arr) - 1) * p
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return arr[lo]
    frac = pos - lo
    return arr[lo] * (1 - frac) + arr[hi] * frac


def fmt_num(x: Optional[float], ndigits: int = 9) -> str:
    if x is None:
        return ""
    return f"{x:.{ndigits}f}"


def build_time_bounds(
    conn: sqlite3.Connection,
    mapping: SourceMapping,
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
        f"SELECT MAX({qident(mapping.ts_col)}) FROM {qident(mapping.table)} "
        f"WHERE {qident(mapping.wallet_col)} = ?"
    )
    row = conn.execute(q, (wallet,)).fetchone()
    if row is None or row[0] is None:
        raise ReconstructionError("Cannot apply --window-hours: wallet has no timestamped events in selected source table.")
    max_ts = int(row[0])
    return max_ts - window_hours * 3600, max_ts


def query_events(
    conn: sqlite3.Connection,
    mapping: SourceMapping,
    wallet: str,
    time_min: Optional[int],
    time_max: Optional[int],
    sol_mints: Optional[Sequence[str]],
) -> Iterable[Tuple[int, str, float, float]]:
    where = [f"{qident(mapping.wallet_col)} = ?"]
    params: List[object] = [wallet]
    if time_min is not None:
        where.append(f"{qident(mapping.ts_col)} >= ?")
        params.append(time_min)
    if time_max is not None:
        where.append(f"{qident(mapping.ts_col)} <= ?")
        params.append(time_max)
    where_sql = " AND ".join(where)

    if mapping.mode == "direct":
        q = (
            f"SELECT {qident(mapping.ts_col)} AS ts, {qident(mapping.mint_col)} AS mint, "
            f"{qident(mapping.token_delta_col)} AS token_delta, {qident(mapping.sol_delta_col)} AS sol_delta "
            f"FROM {qident(mapping.table)} WHERE {where_sql} "
            "ORDER BY ts ASC"
        )
        for ts, mint, token_delta, sol_delta in conn.execute(q, params):
            if mint is None:
                continue
            yield to_int(ts), str(mint), to_float(token_delta), to_float(sol_delta)
        return

    if not sol_mints:
        raise ReconstructionError("Derived mode requires SOL mint detection, but no SOL mint candidates were available.")
    sol_set = {m.lower() for m in sol_mints}

    q = (
        f"SELECT {qident(mapping.ts_col)} AS ts, {qident(mapping.in_mint_col)} AS in_mint, "
        f"{qident(mapping.out_mint_col)} AS out_mint, {qident(mapping.amount_in_col)} AS amount_in, "
        f"{qident(mapping.amount_out_col)} AS amount_out "
        f"FROM {qident(mapping.table)} WHERE {where_sql} ORDER BY ts ASC"
    )

    for ts, in_mint, out_mint, amount_in, amount_out in conn.execute(q, params):
        if in_mint is None or out_mint is None:
            continue
        im = str(in_mint)
        om = str(out_mint)
        im_sol = im.lower() in sol_set
        om_sol = om.lower() in sol_set
        if im_sol == om_sol:
            continue

        ain = to_float(amount_in)
        aout = to_float(amount_out)
        t = to_int(ts)
        if im_sol:
            # SOL -> token: buy token
            token_mint = om
            token_delta = aout
            sol_delta = -ain
        else:
            # token -> SOL: sell token
            token_mint = im
            token_delta = -ain
            sol_delta = aout
        yield t, token_mint, token_delta, sol_delta


def write_tsv(path: Path, header: Sequence[str], rows: Iterable[Sequence[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconstruct per-mint trades from wallet swap cashflows.")
    parser.add_argument("--db", required=True, help="Path to SQLite DB file (masterwalletsdb.db)")
    parser.add_argument("--wallet", required=True, help="Wallet pubkey to reconstruct")
    parser.add_argument("--outdir", required=True, help="Output directory")
    parser.add_argument("--time-min", type=int)
    parser.add_argument("--time-max", type=int)
    parser.add_argument("--window-hours", type=int)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--inspect", action="store_true", help="Inspect candidate tables/columns and exit")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"ERROR: DB path does not exist: {db_path}")

    conn = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    try:
        schema = load_schema(conn)
        direct, derived = detect_mappings(schema)

        if args.inspect:
            print("Detected tables and columns:")
            for table in sorted(schema):
                print(f"- {table}: {', '.join(schema[table])}")
            print("\nDirect candidates:")
            if direct:
                for m in direct:
                    print(
                        f"- {m.table}: wallet={m.wallet_col}, ts={m.ts_col}, mint={m.mint_col}, "
                        f"token_delta={m.token_delta_col}, sol_delta={m.sol_delta_col}"
                    )
            else:
                print("- none")
            print("\nDerivable candidates:")
            if derived:
                for m in derived:
                    print(
                        f"- {m.table}: wallet={m.wallet_col}, ts={m.ts_col}, in_mint={m.in_mint_col}, "
                        f"out_mint={m.out_mint_col}, amount_in={m.amount_in_col}, amount_out={m.amount_out_col}"
                    )
            else:
                print("- none")
            return

        mapping = pick_mapping(direct, derived)

        sol_mints: Optional[List[str]] = None
        if mapping.mode == "derived":
            sol_mints = infer_sol_mints(conn, mapping, args.wallet)
            canonical = {s.lower() for s in sol_mints}
            if not sol_mints or KNOWN_WSOL_MINT.lower() not in canonical and "sol" not in canonical and "wsol" not in canonical:
                raise ReconstructionError(
                    "Could not confidently identify SOL/WSOL mint for derived deltas. "
                    f"Discovered candidates: {sol_mints}."
                )
            if args.verbose:
                print("Using SOL mint candidates:", ", ".join(sol_mints))

        time_min, time_max = build_time_bounds(
            conn, mapping, args.wallet, args.time_min, args.time_max, args.window_hours
        )

        by_mint: Dict[str, TradeAgg] = {}
        raw_events = 0
        total_sol_spent = 0.0
        total_sol_received = 0.0
        sell_without_buy: set[str] = set()

        for ts, mint, token_delta, sol_delta in query_events(
            conn, mapping, args.wallet, time_min, time_max, sol_mints
        ):
            raw_events += 1
            agg = by_mint.get(mint)
            if agg is None:
                agg = TradeAgg(mint=mint, first_ts=ts, last_ts=ts)
                by_mint[mint] = agg
            agg.first_ts = min(agg.first_ts, ts)
            agg.last_ts = max(agg.last_ts, ts)

            if token_delta > 0:
                agg.buys_count += 1
                if agg.entry_time is None:
                    agg.entry_time = ts
                spent = max(0.0, -sol_delta)
                agg.entry_sol += spent
                total_sol_spent += spent
            elif token_delta < 0:
                agg.sells_count += 1
                agg.exit_time = ts
                received = max(0.0, sol_delta)
                agg.exit_sol += received
                total_sol_received += received
                if agg.buys_count == 0:
                    sell_without_buy.add(mint)

        outdir = Path(args.outdir)
        outdir.mkdir(parents=True, exist_ok=True)

        trades_rows = []
        closed_rows: List[Tuple[int, float]] = []
        net_closed: List[float] = []
        roi_closed: List[float] = []
        hold_closed: List[float] = []
        entry_closed: List[float] = []

        for mint in sorted(by_mint, key=lambda m: (by_mint[m].entry_time if by_mint[m].entry_time is not None else 10**20, m)):
            t = by_mint[mint]
            status = "CLOSED" if t.exit_time is not None else "OPEN"
            net_sol = t.exit_sol - t.entry_sol
            roi = (net_sol / t.entry_sol) if t.entry_sol > 0 else None
            hold = (t.exit_time - t.entry_time) if (t.entry_time is not None and t.exit_time is not None) else None

            if status == "CLOSED" and t.entry_time is not None and t.exit_time is not None:
                closed_rows.append((t.exit_time, net_sol))
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

        closed_rows.sort(key=lambda x: x[0])
        equity_rows = []
        cum = 0.0
        for i, (ts, nsol) in enumerate(closed_rows, start=1):
            cum += nsol
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

        peak = -math.inf
        max_dd = 0.0
        running = 0.0
        for _, nsol in closed_rows:
            running += nsol
            peak = max(peak, running)
            max_dd = max(max_dd, peak - running)

        exit_hour_counter = Counter()
        for ts, _ in closed_rows:
            hour = (ts // 3600) % 24
            exit_hour_counter[hour] += 1

        dist_path = outdir / "cented_distributions.txt"
        with dist_path.open("w", encoding="utf-8") as fh:
            fh.write(f"trade_count_closed\t{len(closed_rows)}\n")
            fh.write(f"trade_count_open\t{sum(1 for t in by_mint.values() if t.exit_time is None)}\n")
            fh.write(f"win_rate\t{fmt_num(win_rate, 6)}\n")
            fh.write(f"profit_factor\t{('inf' if profit_factor == math.inf else fmt_num(profit_factor, 6))}\n")
            fh.write(f"avg_win_sol\t{fmt_num(statistics.mean(wins) if wins else None)}\n")
            fh.write(f"avg_loss_sol\t{fmt_num(statistics.mean(losses) if losses else None)}\n")
            fh.write(f"median_win_sol\t{fmt_num(statistics.median(wins) if wins else None)}\n")
            fh.write(f"median_loss_sol\t{fmt_num(statistics.median(losses) if losses else None)}\n")
            fh.write(f"expectancy_per_trade\t{fmt_num(expectancy)}\n")

            fh.write("ROI_percentiles\n")
            for p in [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]:
                fh.write(f"p{int(p*100)}\t{fmt_num(roi_ps[p], 6)}\n")

            fh.write("NetSOL_percentiles\n")
            for p in [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]:
                fh.write(f"p{int(p*100)}\t{fmt_num(net_ps[p])}\n")

            fh.write("HoldSeconds_percentiles\n")
            for p in [0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]:
                fh.write(f"p{int(p*100)}\t{fmt_num(hold_ps[p], 3)}\n")

            fh.write("EntrySOL_percentiles\n")
            for p in [0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]:
                fh.write(f"p{int(p*100)}\t{fmt_num(entry_ps[p])}\n")

            fh.write(f"max_drawdown_sol\t{fmt_num(max_dd)}\n")
            fh.write("trades_per_hour\n")
            for h in range(24):
                fh.write(f"hour_{h:02d}\t{exit_hour_counter.get(h, 0)}\n")

            fh.write("loss_cap_signature\n")
            for p in [0.01, 0.05, 0.10]:
                fh.write(f"roi_p{int(p*100)}\t{fmt_num(roi_ps[p], 6)}\n")
            for p in [0.01, 0.05, 0.10]:
                fh.write(f"net_sol_p{int(p*100)}\t{fmt_num(net_ps[p])}\n")

        # Validation prints
        print(f"Validation: total_sol_spent={fmt_num(total_sol_spent)}")
        print(f"Validation: total_sol_received={fmt_num(total_sol_received)}")
        print(f"Validation: raw_events_consumed={raw_events}")
        print(f"Validation: mints_seen={len(by_mint)}")
        open_count = sum(1 for t in by_mint.values() if t.buys_count > 0 and t.sells_count == 0)
        print(f"Validation: open_trades_buy_no_sell={open_count}")
        if sell_without_buy:
            print("Validation: anomaly_sell_without_buy=" + ",".join(sorted(sell_without_buy)))
        else:
            print("Validation: anomaly_sell_without_buy=<none>")

        print(f"OK: wrote {trades_path} {equity_path} {dist_path}")

    except ReconstructionError as exc:
        print("ERROR:", str(exc), file=sys.stderr)
        print("Schema summary:", file=sys.stderr)
        for table in sorted(schema):
            print(f"- {table}: {', '.join(schema[table])}", file=sys.stderr)
        raise SystemExit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
