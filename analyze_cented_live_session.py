#!/usr/bin/env python3
"""Analyze a Cented live-session JSONL transaction stream."""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any


SUMMARY_PATH = "Cented.live.session_summary.json"


@dataclass
class Tx:
    sig: str
    observed_utc: str | None
    ts: datetime | None
    slot: Any
    fee_lamports: int
    err: Any
    spl_in_count: int
    spl_out_count: int
    sol_delta: float
    compute_budget_micro_lamports: int | None
    raw: dict[str, Any]


@dataclass
class TradeEvent:
    tx: Tx
    cls: str  # BUY/SELL/OTHER


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze Cented live-session JSONL")
    parser.add_argument("input_jsonl", help="Path to Cented.live.jsonl")
    return parser.parse_args()


def parse_ts(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
    if isinstance(value, str):
        try:
            return int(float(value.strip()))
        except ValueError:
            return default
    return default


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return default
    return default


def extract_sig(tx: dict[str, Any]) -> str:
    sig0 = tx.get("sig0")
    if isinstance(sig0, str) and sig0.strip():
        return sig0.strip()
    signatures = tx.get("signatures")
    if isinstance(signatures, list) and signatures:
        first = signatures[0]
        if isinstance(first, str) and first.strip():
            return first.strip()
    signature = tx.get("signature")
    if isinstance(signature, str) and signature.strip():
        return signature.strip()
    return ""


def infer_spl_count(tx: dict[str, Any], direct_key: str, candidate_array_keys: list[str]) -> int:
    if direct_key in tx:
        return max(0, to_int(tx.get(direct_key), 0))
    for key in candidate_array_keys:
        arr = tx.get(key)
        if isinstance(arr, list):
            return len(arr)
    return 0


def extract_sol_delta(tx: dict[str, Any]) -> float:
    for key in ("balance_delta_SOL", "sol_delta", "balanceDeltaSOL", "solDelta"):
        if key in tx:
            return to_float(tx.get(key), 0.0)
    return 0.0


def extract_compute_budget(tx: dict[str, Any]) -> int | None:
    for key in (
        "computeBudget_microLamports",
        "compute_budget_micro_lamports",
        "computeBudgetMicroLamports",
    ):
        if key in tx and tx.get(key) is not None:
            return to_int(tx.get(key), 0)
    return None


def err_is_yes(err: Any) -> bool:
    if err is None:
        return False
    if err is False:
        return False
    if isinstance(err, str) and err.strip() == "":
        return False
    return True


def quantiles(values: list[float], probs: list[float]) -> dict[str, float | None]:
    out: dict[str, float | None] = {}
    if not values:
        for p in probs:
            out[f"p{int(p * 100)}"] = None
        return out
    v = sorted(values)
    n = len(v)
    for p in probs:
        if n == 1:
            q = v[0]
        else:
            idx = (n - 1) * p
            lo = int(math.floor(idx))
            hi = int(math.ceil(idx))
            if lo == hi:
                q = v[lo]
            else:
                frac = idx - lo
                q = v[lo] * (1 - frac) + v[hi] * frac
        out[f"p{int(p * 100)}"] = q
    return out


def distribution(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, "median": None, "p75": None, "p90": None, "p95": None, "max": None}
    q = quantiles(values, [0.75, 0.90, 0.95])
    return {
        "min": min(values),
        "median": median(values),
        "p75": q["p75"],
        "p90": q["p90"],
        "p95": q["p95"],
        "max": max(values),
    }


def round_3(v: float) -> float:
    return round(v + 1e-12, 3)


def top_buckets(values: list[float], top_n: int = 5) -> list[dict[str, Any]]:
    c = Counter(round_3(v) for v in values)
    ranked = sorted(c.items(), key=lambda kv: (-kv[1], kv[0]))
    return [{"bucket_sol": k, "count": v} for k, v in ranked[:top_n]]


def longest_run(classes: list[str], target: str) -> int:
    best = 0
    cur = 0
    for cls in classes:
        if cls == target:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def fmt_float(v: float | None, digits: int = 6) -> str:
    if v is None:
        return "n/a"
    return f"{v:.{digits}f}"


def fmt_dt(dt: datetime | None) -> str:
    if dt is None:
        return "n/a"
    return dt.isoformat().replace("+00:00", "Z")


def short_sig(sig: str, left: int = 6, right: int = 6) -> str:
    if len(sig) <= left + right + 3:
        return sig
    return f"{sig[:left]}...{sig[-right:]}"


def build_tx(raw: dict[str, Any]) -> Tx:
    sig = extract_sig(raw)
    observed_utc = raw.get("observed_utc")
    ts = parse_ts(observed_utc)
    fee_lamports = to_int(raw.get("fee_lamports"), 0)
    spl_in = infer_spl_count(raw, "spl_in_count", ["spl_in", "spl_in_transfers", "in_spl_transfers", "splIn"])
    spl_out = infer_spl_count(raw, "spl_out_count", ["spl_out", "spl_out_transfers", "out_spl_transfers", "splOut"])
    sol_delta = extract_sol_delta(raw)
    compute_budget = extract_compute_budget(raw)
    return Tx(
        sig=sig,
        observed_utc=observed_utc if isinstance(observed_utc, str) else None,
        ts=ts,
        slot=raw.get("slot"),
        fee_lamports=fee_lamports,
        err=raw.get("err"),
        spl_in_count=spl_in,
        spl_out_count=spl_out,
        sol_delta=sol_delta,
        compute_budget_micro_lamports=compute_budget,
        raw=raw,
    )


def main() -> None:
    args = parse_args()
    path = Path(args.input_jsonl)
    if not path.exists():
        raise SystemExit(f"Input file not found: {path}")

    total_lines = 0
    duplicates = 0
    by_sig: dict[str, Tx] = {}
    no_sig_counter = 0

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            total_lines += 1
            try:
                raw = json.loads(s)
            except json.JSONDecodeError:
                continue
            if not isinstance(raw, dict):
                continue
            tx = build_tx(raw)
            key = tx.sig
            if not key:
                key = f"__NO_SIG__{no_sig_counter:012d}"
                no_sig_counter += 1
            if key in by_sig:
                duplicates += 1
                continue
            by_sig[key] = tx

    unique_txs = list(by_sig.values())
    unique_sigs = len(unique_txs)

    ts_values = sorted([tx.ts for tx in unique_txs if tx.ts is not None])
    session_start = ts_values[0] if ts_values else None
    session_end = ts_values[-1] if ts_values else None
    duration_seconds = (session_end - session_start).total_seconds() if session_start and session_end else 0.0
    duration_minutes = duration_seconds / 60.0 if duration_seconds > 0 else 0.0

    err_yes = sum(1 for tx in unique_txs if err_is_yes(tx.err))
    err_no = unique_sigs - err_yes
    err_rate = (err_yes / unique_sigs) if unique_sigs else 0.0

    trade_events: list[TradeEvent] = []
    for tx in unique_txs:
        is_trade = (tx.spl_in_count > 0) or (tx.spl_out_count > 0) or (abs(tx.sol_delta) > 0)
        if not is_trade:
            continue
        if tx.spl_in_count > 0 and tx.sol_delta < 0:
            cls = "BUY"
        elif tx.spl_out_count > 0 and tx.sol_delta > 0:
            cls = "SELL"
        else:
            cls = "OTHER"
        trade_events.append(TradeEvent(tx=tx, cls=cls))

    trade_count = len(trade_events)
    trade_rate_per_min = (trade_count / duration_minutes) if duration_minutes > 0 else 0.0

    class_counts = Counter(te.cls for te in trade_events)
    class_seq = [te.cls for te in sorted(trade_events, key=lambda te: (te.tx.ts or datetime.min.replace(tzinfo=timezone.utc), te.tx.sig))]
    longest_buy_run = longest_run(class_seq, "BUY")
    longest_sell_run = longest_run(class_seq, "SELL")

    buy_sizes = [abs(te.tx.sol_delta) for te in trade_events if te.cls == "BUY"]
    sell_sizes = [te.tx.sol_delta for te in trade_events if te.cls == "SELL"]

    buy_dist = distribution(buy_sizes)
    sell_dist = distribution(sell_sizes)

    buy_buckets = top_buckets(buy_sizes, 5)
    sell_buckets = top_buckets(sell_sizes, 5)

    add_consistency_percent: float | None = None
    if buy_sizes and buy_buckets:
        center = buy_buckets[0]["bucket_sol"]
        if center == 0:
            add_consistency_percent = 100.0 if all(v == 0 for v in buy_sizes) else 0.0
        else:
            lo = center * 0.99
            hi = center * 1.01
            in_band = sum(1 for v in buy_sizes if lo <= v <= hi)
            add_consistency_percent = 100.0 * in_band / len(buy_sizes)

    sorted_trade_events = sorted(trade_events, key=lambda te: (te.tx.ts or datetime.min.replace(tzinfo=timezone.utc), te.tx.sig))
    gaps: list[float] = []
    prev_ts: datetime | None = None
    for te in sorted_trade_events:
        ts = te.tx.ts
        if ts is None:
            continue
        if prev_ts is not None:
            gap = (ts - prev_ts).total_seconds()
            gaps.append(max(gap, 0.0))
        prev_ts = ts

    gap_q = quantiles(gaps, [0.50, 0.75, 0.90, 0.95, 0.99])
    max_gap = max(gaps) if gaps else None

    total_sol_all = sum(tx.sol_delta for tx in unique_txs)
    total_sol_trade = sum(te.tx.sol_delta for te in trade_events)
    gross_sol_out = sum(abs(tx.sol_delta) for tx in unique_txs if tx.sol_delta < 0)
    gross_sol_in = sum(tx.sol_delta for tx in unique_txs if tx.sol_delta > 0)

    overall_fees = [tx.fee_lamports for tx in unique_txs]
    fees_by_class: dict[str, list[int]] = defaultdict(list)
    for te in trade_events:
        fees_by_class[te.cls].append(te.tx.fee_lamports)

    fee_top = sorted(
        trade_events,
        key=lambda te: (-te.tx.fee_lamports, te.tx.ts or datetime.min.replace(tzinfo=timezone.utc), te.tx.sig),
    )[:10]
    fee_top_rows = [
        {
            "observed_utc": fmt_dt(te.tx.ts),
            "sig": te.tx.sig,
            "short_sig": short_sig(te.tx.sig),
            "fee_lamports": te.tx.fee_lamports,
            "class": te.cls,
        }
        for te in fee_top
    ]

    compute_values = [tx.compute_budget_micro_lamports for tx in unique_txs if tx.compute_budget_micro_lamports is not None]
    compute_present_pct = (100.0 * len(compute_values) / unique_sigs) if unique_sigs else 0.0
    compute_dist = distribution([float(v) for v in compute_values])

    summary = {
        "input_file": str(path),
        "totals": {
            "total_lines": total_lines,
            "duplicates": duplicates,
            "unique_sigs": unique_sigs,
            "err_yes": err_yes,
            "err_no": err_no,
            "err_rate": err_rate,
        },
        "session": {
            "start_utc": fmt_dt(session_start),
            "end_utc": fmt_dt(session_end),
            "duration_seconds": duration_seconds,
            "duration_minutes": duration_minutes,
        },
        "trades": {
            "trade_events": trade_count,
            "trade_rate_per_minute": trade_rate_per_min,
            "class_counts": {
                "BUY": class_counts.get("BUY", 0),
                "SELL": class_counts.get("SELL", 0),
                "OTHER": class_counts.get("OTHER", 0),
            },
            "longest_run": {"BUY": longest_buy_run, "SELL": longest_sell_run},
            "buy": {
                "size_distribution_sol": buy_dist,
                "top_buckets_sol_rounded_0p001": buy_buckets,
                "add_consistency_percent_within_1pct_of_top_bucket": add_consistency_percent,
            },
            "sell": {
                "size_distribution_sol": sell_dist,
                "top_buckets_sol_rounded_0p001": sell_buckets,
            },
            "gap_seconds": {
                "quantiles": gap_q,
                "max": max_gap,
                "suggestion": "Choose burst gap threshold from quantiles above.",
            },
        },
        "capital": {
            "sum_sol_delta_all_unique_txs": total_sol_all,
            "sum_sol_delta_trade_events_only": total_sol_trade,
            "gross_sol_out": gross_sol_out,
            "gross_sol_in": gross_sol_in,
        },
        "fees": {
            "overall_fee_lamports_distribution": distribution([float(v) for v in overall_fees]),
            "by_class_fee_lamports_distribution": {
                k: distribution([float(v) for v in vals]) for k, vals in sorted(fees_by_class.items())
            },
            "top10_fee_txs": fee_top_rows,
        },
        "compute_budget_micro_lamports": {
            "present_count": len(compute_values),
            "present_percent": compute_present_pct,
            "distribution": compute_dist,
        },
    }

    report_lines = [
        "=== Cented Live Session Analysis ===",
        f"Input: {path}",
        "",
        "-- Totals --",
        f"Total lines: {total_lines}",
        f"Unique signatures: {unique_sigs}",
        f"Duplicates removed: {duplicates}",
        f"Errors: yes={err_yes}, no={err_no}, rate={err_rate:.4%}",
        "",
        "-- Session Window --",
        f"Start (UTC): {fmt_dt(session_start)}",
        f"End (UTC):   {fmt_dt(session_end)}",
        f"Duration:    {duration_seconds:.3f} sec ({duration_minutes:.3f} min)",
        "",
        "-- Trade Events --",
        f"Trade events: {trade_count}",
        f"Trade rate/min: {trade_rate_per_min:.6f}",
        f"Class counts: BUY={class_counts.get('BUY', 0)} SELL={class_counts.get('SELL', 0)} OTHER={class_counts.get('OTHER', 0)}",
        f"Longest BUY run: {longest_buy_run}",
        f"Longest SELL run: {longest_sell_run}",
        "",
        "-- BUY Sizing (abs(SOL delta)) --",
        (
            "Distribution min/median/p75/p90/p95/max: "
            f"{fmt_float(buy_dist['min'])} / {fmt_float(buy_dist['median'])} / {fmt_float(buy_dist['p75'])} / "
            f"{fmt_float(buy_dist['p90'])} / {fmt_float(buy_dist['p95'])} / {fmt_float(buy_dist['max'])}"
        ),
        "Top 5 buckets (0.001 SOL): " + ", ".join(
            f"{row['bucket_sol']:.3f}:{row['count']}" for row in buy_buckets
        ) if buy_buckets else "Top 5 buckets (0.001 SOL): n/a",
        "Add-consistency within ±1% of top bucket: "
        + (f"{add_consistency_percent:.2f}%" if add_consistency_percent is not None else "n/a"),
        "",
        "-- SELL Sizing (SOL delta) --",
        (
            "Distribution min/median/p75/p90/p95/max: "
            f"{fmt_float(sell_dist['min'])} / {fmt_float(sell_dist['median'])} / {fmt_float(sell_dist['p75'])} / "
            f"{fmt_float(sell_dist['p90'])} / {fmt_float(sell_dist['p95'])} / {fmt_float(sell_dist['max'])}"
        ),
        "Top 5 buckets (0.001 SOL): " + ", ".join(
            f"{row['bucket_sol']:.3f}:{row['count']}" for row in sell_buckets
        ) if sell_buckets else "Top 5 buckets (0.001 SOL): n/a",
        "",
        "-- Gap Distribution (sec) --",
        f"p50={fmt_float(gap_q['p50'])}, p75={fmt_float(gap_q['p75'])}, p90={fmt_float(gap_q['p90'])}, p95={fmt_float(gap_q['p95'])}, p99={fmt_float(gap_q['p99'])}, max={fmt_float(max_gap)}",
        "Choose burst gap threshold from quantiles above.",
        "",
        "-- Capital Accounting --",
        f"Sum SOL delta (all unique txs): {total_sol_all:.9f}",
        f"Sum SOL delta (trade events):  {total_sol_trade:.9f}",
        f"Gross SOL out (abs negatives): {gross_sol_out:.9f}",
        f"Gross SOL in (positives):      {gross_sol_in:.9f}",
        "",
        "-- Fees --",
        (
            "Overall fee distribution min/median/p75/p90/p95/max: "
            f"{fmt_float(summary['fees']['overall_fee_lamports_distribution']['min'], 3)} / "
            f"{fmt_float(summary['fees']['overall_fee_lamports_distribution']['median'], 3)} / "
            f"{fmt_float(summary['fees']['overall_fee_lamports_distribution']['p75'], 3)} / "
            f"{fmt_float(summary['fees']['overall_fee_lamports_distribution']['p90'], 3)} / "
            f"{fmt_float(summary['fees']['overall_fee_lamports_distribution']['p95'], 3)} / "
            f"{fmt_float(summary['fees']['overall_fee_lamports_distribution']['max'], 3)}"
        ),
        "Top 10 fee txs:",
    ]
    for row in fee_top_rows:
        report_lines.append(
            f"  {row['observed_utc']} | {row['short_sig']} | fee={row['fee_lamports']} lamports | class={row['class']}"
        )

    report_lines.extend(
        [
            "",
            "-- Compute Budget (microLamports) --",
            f"Present: {len(compute_values)}/{unique_sigs} ({compute_present_pct:.2f}%)",
            (
                "Distribution min/median/p75/p90/p95/max: "
                f"{fmt_float(compute_dist['min'], 3)} / {fmt_float(compute_dist['median'], 3)} / {fmt_float(compute_dist['p75'], 3)} / "
                f"{fmt_float(compute_dist['p90'], 3)} / {fmt_float(compute_dist['p95'], 3)} / {fmt_float(compute_dist['max'], 3)}"
            ),
            "",
            f"Summary JSON written to: {SUMMARY_PATH}",
        ]
    )

    print("\n".join(report_lines))

    out_path = Path(SUMMARY_PATH)
    out_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
