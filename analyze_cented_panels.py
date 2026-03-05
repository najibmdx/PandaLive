#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from bisect import bisect_left
from collections import defaultdict
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any

getcontext().prec = 50

EPS = Decimal("0.0000000001")
MINT_KEYS = ("mint", "token_mint", "mint_address")
AMOUNT_KEYS = ("amount", "token_amount", "ui_amount", "uiAmount", "quantity")
PANELS = ("New Pairs", "Final Stretch", "Migrated", "Unknown")


def parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return Decimal(s)
        except InvalidOperation:
            return None
    return None


def parse_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return int(float(s))
        except ValueError:
            return None
    return None


def normalize_zero(x: Decimal) -> Decimal:
    return Decimal("0") if abs(x) <= EPS else x


def extract_mint(transfer: dict[str, Any]) -> str | None:
    for key in MINT_KEYS:
        mint = transfer.get(key)
        if isinstance(mint, str) and mint.strip():
            return mint.strip()
    return None


def extract_amount(transfer: dict[str, Any]) -> Decimal | None:
    for key in AMOUNT_KEYS:
        if key not in transfer:
            continue
        val = transfer.get(key)
        if isinstance(val, dict):
            for inner_key in AMOUNT_KEYS:
                inner_val = val.get(inner_key)
                dec = parse_decimal(inner_val)
                if dec is not None:
                    return dec
            continue
        dec = parse_decimal(val)
        if dec is not None:
            return dec
    return None


def classify_panel(mcap: Decimal | None) -> str:
    if mcap is None:
        return "Unknown"
    if mcap < Decimal("10000"):
        return "New Pairs"
    if mcap < Decimal("35000"):
        return "Final Stretch"
    return "Migrated"


def quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    vals = sorted(values)
    idx = (len(vals) - 1) * q
    low = int(idx)
    high = min(low + 1, len(vals) - 1)
    frac = idx - low
    return float(vals[low] + (vals[high] - vals[low]) * frac)


def quantile_block(values: list[float]) -> dict[str, float | None]:
    return {
        "p50": quantile(values, 0.50),
        "p75": quantile(values, 0.75),
        "p90": quantile(values, 0.90),
    }


def hold_block(values: list[float]) -> dict[str, float | None]:
    avg = sum(values) / len(values) if values else None
    return {
        "avg": avg,
        "p50": quantile(values, 0.50),
        "p90": quantile(values, 0.90),
    }


def format_decimal(d: Decimal | None) -> str:
    if d is None:
        return ""
    return format(d.normalize(), "f") if d != 0 else "0"


def parse_tx_events(jsonl_path: Path) -> tuple[list[dict[str, Any]], int, set[str]]:
    events: list[dict[str, Any]] = []
    line_count = 0
    mints_seen: set[str] = set()

    with jsonl_path.open("r", encoding="utf-8") as f:
        for raw in f:
            line_count += 1
            line = raw.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(rec, dict):
                continue
            tx_time = parse_int(rec.get("tx_time"))
            if tx_time is None:
                continue

            token_delta: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))

            in_transfers = rec.get("spl_in_transfers")
            if isinstance(in_transfers, list):
                for t in in_transfers:
                    if not isinstance(t, dict):
                        continue
                    mint = extract_mint(t)
                    amt = extract_amount(t)
                    if mint is None or amt is None:
                        continue
                    token_delta[mint] += amt

            out_transfers = rec.get("spl_out_transfers")
            if isinstance(out_transfers, list):
                for t in out_transfers:
                    if not isinstance(t, dict):
                        continue
                    mint = extract_mint(t)
                    amt = extract_amount(t)
                    if mint is None or amt is None:
                        continue
                    token_delta[mint] -= amt

            clean_delta = {}
            for mint, delta in token_delta.items():
                d = normalize_zero(delta)
                if d != 0:
                    clean_delta[mint] = d
                    mints_seen.add(mint)

            if not clean_delta:
                continue

            sol_delta = parse_decimal(rec.get("balance_delta_SOL"))
            if sol_delta is None:
                pre = parse_decimal(rec.get("pre_balance_SOL"))
                post = parse_decimal(rec.get("post_balance_SOL"))
                if pre is not None and post is not None:
                    sol_delta = post - pre

            events.append({"tx_time": tx_time, "token_delta": clean_delta, "sol_delta": sol_delta})

    events.sort(key=lambda x: x["tx_time"])
    return events, line_count, mints_seen


def build_mcap_index(csv_path: Path) -> dict[str, dict[str, Any]]:
    dedup: dict[tuple[str, int], Decimal | None] = {}

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not isinstance(row, dict):
                continue
            mint = (row.get("mint") or "").strip()
            trade_time = parse_int(row.get("trade_time"))
            if not mint or trade_time is None:
                continue
            mcap = parse_decimal(row.get("marketcap"))
            key = (mint, trade_time)
            if key not in dedup:
                dedup[key] = mcap
            else:
                prev = dedup[key]
                if prev is None and mcap is not None:
                    dedup[key] = mcap

    by_mint: dict[str, list[tuple[int, Decimal | None]]] = defaultdict(list)
    for (mint, ts), mcap in dedup.items():
        by_mint[mint].append((ts, mcap))

    index: dict[str, dict[str, Any]] = {}
    for mint, rows in by_mint.items():
        rows.sort(key=lambda x: x[0])
        ts_to_mcap = {ts: mcap for ts, mcap in rows}
        valid_rows = [(ts, mcap) for ts, mcap in rows if mcap is not None]
        valid_times = [ts for ts, _ in valid_rows]
        valid_mcaps = [mcap for _, mcap in valid_rows]
        index[mint] = {
            "exact": ts_to_mcap,
            "valid_times": valid_times,
            "valid_mcaps": valid_mcaps,
        }
    return index


def lookup_mcap(index: dict[str, dict[str, Any]], mint: str, target_time: int) -> Decimal | None:
    data = index.get(mint)
    if not data:
        return None
    exact = data["exact"]
    if target_time in exact and exact[target_time] is not None:
        return exact[target_time]

    valid_times = data["valid_times"]
    valid_mcaps = data["valid_mcaps"]
    if not valid_times:
        return None

    pos = bisect_left(valid_times, target_time)
    candidates: list[tuple[int, Decimal]] = []

    if pos < len(valid_times):
        dt = abs(valid_times[pos] - target_time)
        if dt <= 60:
            candidates.append((dt, valid_mcaps[pos]))
    if pos > 0:
        dt = abs(valid_times[pos - 1] - target_time)
        if dt <= 60:
            candidates.append((dt, valid_mcaps[pos - 1]))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def reconstruct_positions(events: list[dict[str, Any]], mcap_index: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    balances: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    active: dict[str, dict[str, Any]] = {}
    closed: list[dict[str, Any]] = []

    for event in events:
        tx_time = event["tx_time"]
        sol_delta: Decimal | None = event["sol_delta"]

        for mint, delta in sorted(event["token_delta"].items()):
            prev_bal = normalize_zero(balances[mint])

            if mint not in active and prev_bal > 0:
                active[mint] = {
                    "mint": mint,
                    "entry_time": tx_time,
                    "num_buys": 0,
                    "num_sells": 0,
                    "sol_out_gross": Decimal("0"),
                    "sol_in_gross": Decimal("0"),
                    "tx_count": 0,
                    "anomaly_negative_balance": False,
                }

            new_bal = prev_bal + delta
            new_bal = normalize_zero(new_bal)

            opened = prev_bal == 0 and new_bal > 0
            if opened:
                active[mint] = {
                    "mint": mint,
                    "entry_time": tx_time,
                    "num_buys": 0,
                    "num_sells": 0,
                    "sol_out_gross": Decimal("0"),
                    "sol_in_gross": Decimal("0"),
                    "tx_count": 0,
                    "anomaly_negative_balance": False,
                }

            position = active.get(mint)
            if position is not None:
                position["tx_count"] += 1
                if sol_delta is not None:
                    if delta > 0 and sol_delta < 0:
                        position["num_buys"] += 1
                        position["sol_out_gross"] += -sol_delta
                    elif delta < 0 and sol_delta > 0:
                        position["num_sells"] += 1
                        position["sol_in_gross"] += sol_delta

            if new_bal < 0:
                new_bal = Decimal("0")
                if position is not None:
                    position["anomaly_negative_balance"] = True

            balances[mint] = normalize_zero(new_bal)

            closed_now = prev_bal > 0 and balances[mint] == 0
            if closed_now and position is not None:
                entry_time = int(position["entry_time"])
                exit_time = int(tx_time)
                hold_sec = exit_time - entry_time
                entry_mcap = lookup_mcap(mcap_index, mint, entry_time)
                exit_mcap = lookup_mcap(mcap_index, mint, exit_time)
                rec = {
                    "mint": mint,
                    "entry_time": entry_time,
                    "exit_time": exit_time,
                    "hold_sec": hold_sec,
                    "entry_mcap": entry_mcap,
                    "exit_mcap": exit_mcap,
                    "entry_panel": classify_panel(entry_mcap),
                    "exit_panel": classify_panel(exit_mcap),
                    "num_buys": int(position["num_buys"]),
                    "num_sells": int(position["num_sells"]),
                    "sol_out_gross": position["sol_out_gross"],
                    "sol_in_gross": position["sol_in_gross"],
                    "net_sol": position["sol_in_gross"] - position["sol_out_gross"],
                    "tx_count": int(position["tx_count"]),
                    "anomaly_negative_balance": bool(position["anomaly_negative_balance"]),
                }
                assert rec["hold_sec"] >= 0
                assert rec["entry_time"] <= rec["exit_time"]
                closed.append(rec)
                del active[mint]

    open_positions_count = len(active)
    return closed, open_positions_count


def build_panel_stats(positions: list[dict[str, Any]], panel_key: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for panel in PANELS:
        subset = [p for p in positions if p.get(panel_key) == panel]
        hold_vals = [float(p["hold_sec"]) for p in subset]
        entry_vals = [float(p["entry_mcap"]) for p in subset if p.get("entry_mcap") is not None]
        exit_vals = [float(p["exit_mcap"]) for p in subset if p.get("exit_mcap") is not None]
        hb = hold_block(hold_vals)
        out[panel] = {
            "positions": len(subset),
            "avg_hold_sec": hb["avg"],
            "p50_hold_sec": hb["p50"],
            "p90_hold_sec": hb["p90"],
            "entry_mcap": quantile_block(entry_vals),
            "exit_mcap": quantile_block(exit_vals),
        }
    return out


def write_positions_csv(path: Path, positions: list[dict[str, Any]]) -> None:
    cols = [
        "mint",
        "entry_time",
        "exit_time",
        "hold_sec",
        "entry_mcap",
        "exit_mcap",
        "entry_panel",
        "exit_panel",
        "num_buys",
        "num_sells",
        "sol_out_gross",
        "sol_in_gross",
        "net_sol",
        "tx_count",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for p in positions:
            w.writerow(
                {
                    "mint": p["mint"],
                    "entry_time": p["entry_time"],
                    "exit_time": p["exit_time"],
                    "hold_sec": p["hold_sec"],
                    "entry_mcap": format_decimal(p.get("entry_mcap")),
                    "exit_mcap": format_decimal(p.get("exit_mcap")),
                    "entry_panel": p["entry_panel"],
                    "exit_panel": p["exit_panel"],
                    "num_buys": p["num_buys"],
                    "num_sells": p["num_sells"],
                    "sol_out_gross": format_decimal(p["sol_out_gross"]),
                    "sol_in_gross": format_decimal(p["sol_in_gross"]),
                    "net_sol": format_decimal(p["net_sol"]),
                    "tx_count": p["tx_count"],
                }
            )


def write_markdown(path: Path, breakdown: dict[str, Any], global_stats: dict[str, int]) -> None:
    lines: list[str] = []
    lines.append("# Cented Panel Breakdown")
    lines.append("")
    lines.append("## Global")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---:|")
    for k in (
        "total_closed_positions",
        "missing_entry_mcap_positions",
        "missing_exit_mcap_positions",
        "open_positions_count",
    ):
        lines.append(f"| {k} | {global_stats.get(k, 0)} |")

    for section_key, section_title in (("panel_by_entry", "By Entry Panel"), ("panel_by_exit", "By Exit Panel")):
        lines.append("")
        lines.append(f"## {section_title}")
        lines.append("")
        lines.append("| Panel | Positions | Avg Hold | P50 Hold | P90 Hold | Entry P50 | Entry P75 | Entry P90 | Exit P50 | Exit P75 | Exit P90 |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        section = breakdown.get(section_key, {})
        for panel in PANELS:
            v = section.get(panel, {})
            em = v.get("entry_mcap", {})
            xm = v.get("exit_mcap", {})
            lines.append(
                "| {panel} | {positions} | {avg_hold_sec} | {p50_hold_sec} | {p90_hold_sec} | {e50} | {e75} | {e90} | {x50} | {x75} | {x90} |".format(
                    panel=panel,
                    positions=v.get("positions", 0),
                    avg_hold_sec="" if v.get("avg_hold_sec") is None else f"{v.get('avg_hold_sec'):.2f}",
                    p50_hold_sec="" if v.get("p50_hold_sec") is None else f"{v.get('p50_hold_sec'):.2f}",
                    p90_hold_sec="" if v.get("p90_hold_sec") is None else f"{v.get('p90_hold_sec'):.2f}",
                    e50="" if em.get("p50") is None else f"{em.get('p50'):.2f}",
                    e75="" if em.get("p75") is None else f"{em.get('p75'):.2f}",
                    e90="" if em.get("p90") is None else f"{em.get('p90'):.2f}",
                    x50="" if xm.get("p50") is None else f"{xm.get('p50'):.2f}",
                    x75="" if xm.get("p75") is None else f"{xm.get('p75'):.2f}",
                    x90="" if xm.get("p90") is None else f"{xm.get('p90'):.2f}",
                )
            )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jsonl", required=True)
    parser.add_argument("--mcap-csv", required=True)
    parser.add_argument("--outdir", required=True)
    args = parser.parse_args()

    jsonl_path = Path(args.jsonl)
    mcap_csv = Path(args.mcap_csv)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    events, total_lines, mints_seen = parse_tx_events(jsonl_path)
    mcap_index = build_mcap_index(mcap_csv)
    positions, open_positions_count = reconstruct_positions(events, mcap_index)

    missing_entry_mcap = sum(1 for p in positions if p.get("entry_mcap") is None)
    missing_exit_mcap = sum(1 for p in positions if p.get("exit_mcap") is None)

    panel_by_entry = build_panel_stats(positions, "entry_panel")
    panel_by_exit = build_panel_stats(positions, "exit_panel")

    global_stats = {
        "total_closed_positions": len(positions),
        "missing_entry_mcap_positions": missing_entry_mcap,
        "missing_exit_mcap_positions": missing_exit_mcap,
        "open_positions_count": open_positions_count,
    }

    breakdown = {
        "panel_by_entry": panel_by_entry,
        "panel_by_exit": panel_by_exit,
        "notes": {
            "missing_mcap_positions": sum(1 for p in positions if p.get("entry_mcap") is None or p.get("exit_mcap") is None),
            "total_positions": len(positions),
            "total_closed_positions": len(positions),
            "missing_entry_mcap_positions": missing_entry_mcap,
            "missing_exit_mcap_positions": missing_exit_mcap,
            "open_positions_count": open_positions_count,
        },
    }

    write_positions_csv(outdir / "cented_positions_reconstructed.csv", positions)
    (outdir / "cented_panel_breakdown.json").write_text(json.dumps(breakdown, indent=2), encoding="utf-8")
    write_markdown(outdir / "cented_panel_breakdown.md", breakdown, global_stats)

    print(f"total tx lines read: {total_lines}")
    print(f"total mints seen: {len(mints_seen)}")
    print(f"closed positions: {len(positions)}")
    print(f"open positions: {open_positions_count}")
    print(f"missing entry mcap positions: {missing_entry_mcap}")
    print(f"missing exit mcap positions: {missing_exit_mcap}")
    print("panel counts by entry:")
    for panel in PANELS:
        print(f"  {panel}: {panel_by_entry[panel]['positions']}")


if __name__ == "__main__":
    main()
