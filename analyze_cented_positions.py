#!/usr/bin/env python3
"""Analyze live recorder JSONL into position-level trades."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MINT_KEYS = ("mint", "mint_address", "token_mint", "token", "tokenAddress", "address")
AMOUNT_KEYS = (
    "amount",
    "token_amount",
    "tokenAmount",
    "ui_amount",
    "uiAmount",
    "uiAmountString",
    "raw_amount",
    "rawAmount",
    "quantity",
)
TIME_KEYS = ("observed_utc", "observed_at", "timestamp", "time")


@dataclass
class PositionState:
    mint: str
    net_tokens: float = 0.0
    total_sol_deployed: float = 0.0
    total_sol_realized: float = 0.0
    start_time: str | None = None
    end_time: str | None = None
    adds: int = 0
    exits: int = 0
    max_position_size: float = 0.0
    status: str = "IDLE"


@dataclass
class TradeRecord:
    mint: str
    status: str
    start_time: str | None
    end_time: str | None
    duration_seconds: float | None
    total_sol_deployed: float
    total_sol_realized: float
    net_pnl: float
    number_of_adds: int
    number_of_exits: int
    max_position_size: float


def parse_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        if not value.strip():
            return default
        try:
            return float(value.replace(",", ""))
        except ValueError:
            return default
    return default


def normalize_time(record: dict[str, Any]) -> str | None:
    for key in TIME_KEYS:
        value = record.get(key)
        if value is None:
            continue
        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
            except (ValueError, OSError):
                continue
        if isinstance(value, str):
            return value
    return None


def parse_iso_or_epoch(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


def extract_mint(transfer: dict[str, Any]) -> str | None:
    for key in MINT_KEYS:
        v = transfer.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def extract_amount(transfer: dict[str, Any]) -> float:
    for key in AMOUNT_KEYS:
        if key in transfer:
            return abs(parse_float(transfer.get(key)))

    nested = transfer.get("token_amount")
    if isinstance(nested, dict):
        for key in ("ui_amount", "uiAmount", "amount", "uiAmountString"):
            if key in nested:
                return abs(parse_float(nested.get(key)))
    return 0.0


def compute_mint_deltas(record: dict[str, Any]) -> dict[str, float]:
    in_transfers = record.get("spl_in_transfers") or []
    out_transfers = record.get("spl_out_transfers") or []

    deltas: dict[str, float] = defaultdict(float)

    if isinstance(in_transfers, list):
        for t in in_transfers:
            if not isinstance(t, dict):
                continue
            mint = extract_mint(t)
            if not mint:
                continue
            deltas[mint] += extract_amount(t)

    if isinstance(out_transfers, list):
        for t in out_transfers:
            if not isinstance(t, dict):
                continue
            mint = extract_mint(t)
            if not mint:
                continue
            deltas[mint] -= extract_amount(t)

    return {mint: delta for mint, delta in deltas.items() if abs(delta) > 0}


def allocate_sol(sol_delta: float, mint_deltas: dict[str, float]) -> dict[str, float]:
    if not mint_deltas:
        return {}
    if len(mint_deltas) == 1:
        mint = next(iter(mint_deltas))
        return {mint: sol_delta}

    total_weight = sum(abs(v) for v in mint_deltas.values())
    if total_weight == 0:
        share = sol_delta / len(mint_deltas)
        return {mint: share for mint in sorted(mint_deltas)}
    return {mint: sol_delta * (abs(delta) / total_weight) for mint, delta in mint_deltas.items()}


def close_trade(state: PositionState) -> TradeRecord:
    start_ts = parse_iso_or_epoch(state.start_time)
    end_ts = parse_iso_or_epoch(state.end_time)
    duration = end_ts - start_ts if start_ts is not None and end_ts is not None else None
    return TradeRecord(
        mint=state.mint,
        status="CLOSED",
        start_time=state.start_time,
        end_time=state.end_time,
        duration_seconds=duration,
        total_sol_deployed=state.total_sol_deployed,
        total_sol_realized=state.total_sol_realized,
        net_pnl=state.total_sol_realized - state.total_sol_deployed,
        number_of_adds=state.adds,
        number_of_exits=state.exits,
        max_position_size=state.max_position_size,
    )


def open_trade_snapshot(state: PositionState) -> TradeRecord:
    return TradeRecord(
        mint=state.mint,
        status="OPEN",
        start_time=state.start_time,
        end_time=state.end_time,
        duration_seconds=None,
        total_sol_deployed=state.total_sol_deployed,
        total_sol_realized=state.total_sol_realized,
        net_pnl=state.total_sol_realized - state.total_sol_deployed,
        number_of_adds=state.adds,
        number_of_exits=state.exits,
        max_position_size=state.max_position_size,
    )


def to_dict(trade: TradeRecord) -> dict[str, Any]:
    return {
        "mint": trade.mint,
        "status": trade.status,
        "start_time": trade.start_time,
        "end_time": trade.end_time,
        "duration_seconds": trade.duration_seconds,
        "total_sol_deployed": trade.total_sol_deployed,
        "total_sol_realized": trade.total_sol_realized,
        "net_pnl": trade.net_pnl,
        "number_of_adds": trade.number_of_adds,
        "number_of_exits": trade.number_of_exits,
        "max_position_size": trade.max_position_size,
    }


def fmt(n: float | None) -> str:
    if n is None:
        return "-"
    return f"{n:.6f}"


def print_table(title: str, trades: list[TradeRecord]) -> None:
    print(f"\n=== {title} ===")
    print("Token | Deployed SOL | Realized SOL | Net PnL | Adds | Duration (sec)")
    for trade in trades:
        dur = "-" if trade.duration_seconds is None else f"{trade.duration_seconds:.2f}"
        print(
            f"{trade.mint} | {trade.total_sol_deployed:.6f} | {trade.total_sol_realized:.6f} | "
            f"{trade.net_pnl:.6f} | {trade.number_of_adds} | {dur}"
        )


def analyze(path: Path) -> tuple[list[TradeRecord], list[TradeRecord]]:
    states: dict[str, PositionState] = {}
    closed: list[TradeRecord] = []

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            if record.get("err"):
                continue

            mint_deltas = compute_mint_deltas(record)
            if not mint_deltas:
                continue

            sol_delta = parse_float(record.get("balance_delta_SOL"), default=None)
            if sol_delta is None:
                sol_delta = parse_float(record.get("sol_delta"), default=0.0)

            observed_time = normalize_time(record)
            mint_sol = allocate_sol(sol_delta, mint_deltas)

            for mint in sorted(mint_deltas):
                token_delta = mint_deltas[mint]
                if abs(token_delta) == 0:
                    continue

                state = states.setdefault(mint, PositionState(mint=mint))
                before = state.net_tokens
                after = before + token_delta

                if before <= 0 < after:
                    state.start_time = observed_time
                    state.end_time = None
                    state.total_sol_deployed = 0.0
                    state.total_sol_realized = 0.0
                    state.adds = 0
                    state.exits = 0
                    state.max_position_size = 0.0
                    state.status = "OPEN"

                if state.status == "OPEN":
                    if token_delta > 0:
                        state.adds += 1
                    elif token_delta < 0:
                        state.exits += 1

                    per_mint_sol_delta = mint_sol.get(mint, 0.0)
                    if per_mint_sol_delta < 0:
                        state.total_sol_deployed += abs(per_mint_sol_delta)
                    elif per_mint_sol_delta > 0:
                        state.total_sol_realized += per_mint_sol_delta

                    state.net_tokens = after
                    state.max_position_size = max(state.max_position_size, state.net_tokens)
                    state.end_time = observed_time

                    if before > 0 and after <= 0:
                        state.net_tokens = 0.0
                        closed.append(close_trade(state))
                        state.status = "IDLE"

    open_trades = [open_trade_snapshot(s) for s in states.values() if s.status == "OPEN" and s.net_tokens > 0]

    def sort_key(t: TradeRecord) -> tuple[float, str]:
        ts = parse_iso_or_epoch(t.start_time)
        return (float("inf") if ts is None else ts, t.mint)

    closed.sort(key=sort_key)
    open_trades.sort(key=sort_key)
    return closed, open_trades


def output_name(input_path: Path) -> Path:
    name = input_path.name
    if ".live" in name:
        prefix = name.split(".live", 1)[0]
    else:
        prefix = input_path.stem
    return input_path.with_name(f"{prefix}.position_summary.json")


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze position-level trades from live recorder JSONL")
    parser.add_argument("input_jsonl", type=Path, help="Path to live recorder JSONL file")
    args = parser.parse_args()

    closed, open_trades = analyze(args.input_jsonl)

    winners = [t for t in closed if t.net_pnl > 0]
    losers = [t for t in closed if t.net_pnl < 0]

    total_net = sum(t.net_pnl for t in closed)
    avg_win = (sum(t.net_pnl for t in winners) / len(winners)) if winners else 0.0
    avg_loss = (sum(t.net_pnl for t in losers) / len(losers)) if losers else 0.0
    largest_win = max((t.net_pnl for t in winners), default=0.0)
    largest_loss = min((t.net_pnl for t in losers), default=0.0)

    print("=== POSITION SUMMARY ===\n")
    print(f"Total Closed Trades: {len(closed)}")
    print(f"Winning Trades: {len(winners)}")
    print(f"Losing Trades: {len(losers)}")
    print(f"Open Positions: {len(open_trades)}")
    print()
    print(f"Total Net PnL: {fmt(total_net)}")
    print(f"Average Win: {fmt(avg_win)}")
    print(f"Average Loss: {fmt(avg_loss)}")
    print(f"Largest Win: {fmt(largest_win)}")
    print(f"Largest Loss: {fmt(largest_loss)}")

    print_table("WINNING TRADES", winners)
    print_table("LOSING TRADES", losers)

    summary = {
        "closed_trades": [to_dict(t) for t in closed],
        "open_trades": [to_dict(t) for t in open_trades],
        "metrics": {
            "total_closed_trades": len(closed),
            "winning_trades": len(winners),
            "losing_trades": len(losers),
            "open_positions": len(open_trades),
            "total_net_pnl": total_net,
            "average_win": avg_win,
            "average_loss": avg_loss,
            "largest_win": largest_win,
            "largest_loss": largest_loss,
        },
    }

    out_path = output_name(args.input_jsonl)
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    print(f"\nWrote summary JSON: {out_path}")


if __name__ == "__main__":
    main()
