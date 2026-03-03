#!/usr/bin/env python3
import argparse
import csv
import math
import os
import sqlite3
import sys
from typing import List, Optional, Sequence, Tuple


REQUIRED_TRADES_COLUMNS = ("mint", "entry_time")
REQUIRED_SWAPS_COLUMNS = ("token_mint", "block_time")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute entry_time minus first swap time deltas per mint."
    )
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--trades-tsv", required=True, help="Path to trades TSV")
    parser.add_argument("--out", required=True, help="Output directory path")
    return parser.parse_args()


def fail(msg: str) -> None:
    raise RuntimeError(msg)


def ensure_db_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='swaps' LIMIT 1"
    )
    if cur.fetchone() is None:
        fail("Required table missing: swaps")

    cur.execute("PRAGMA table_info(swaps)")
    cols = {row[1] for row in cur.fetchall()}
    missing = [c for c in REQUIRED_SWAPS_COLUMNS if c not in cols]
    if missing:
        fail("Required column(s) missing in swaps: " + ", ".join(missing))


def read_trades(path: str) -> List[Tuple[str, int]]:
    rows: List[Tuple[str, int]] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        if reader.fieldnames is None:
            fail("trades-tsv is empty or missing header")

        missing = [c for c in REQUIRED_TRADES_COLUMNS if c not in reader.fieldnames]
        if missing:
            fail("Required column(s) missing in trades-tsv: " + ", ".join(missing))

        line_no = 1
        for row in reader:
            line_no += 1
            mint = (row.get("mint") or "").strip()
            entry_raw = (row.get("entry_time") or "").strip()
            if not mint:
                fail(f"Empty mint at trades-tsv line {line_no}")
            try:
                entry_time = int(entry_raw)
            except ValueError:
                fail(f"Invalid entry_time at trades-tsv line {line_no}: {entry_raw!r}")
            rows.append((mint, entry_time))

    return rows


def percentile(sorted_vals: Sequence[int], p: float) -> Optional[float]:
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])

    rank = (len(sorted_vals) - 1) * p
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return float(sorted_vals[lo])
    frac = rank - lo
    return float(sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac)


def format_num(value: Optional[float]) -> str:
    if value is None:
        return "NA"
    if abs(value - round(value)) < 1e-12:
        return str(int(round(value)))
    return f"{value:.6f}".rstrip("0").rstrip(".")


def format_pct(numer: int, denom: int) -> str:
    if denom == 0:
        return "NA"
    return f"{(100.0 * numer / denom):.6f}".rstrip("0").rstrip(".")


def main() -> int:
    args = parse_args()

    if not os.path.isfile(args.db):
        fail(f"Database file not found: {args.db}")
    if not os.path.isfile(args.trades_tsv):
        fail(f"Trades TSV file not found: {args.trades_tsv}")

    os.makedirs(args.out, exist_ok=True)
    delta_rows_path = os.path.join(args.out, "delta_rows.tsv")
    summary_path = os.path.join(args.out, "delta_summary.txt")

    trades = read_trades(args.trades_tsv)

    conn = sqlite3.connect(args.db)
    try:
        ensure_db_schema(conn)
        cur = conn.cursor()

        total_mints = len(trades)
        missing_first_swap = 0

        out_rows: List[Tuple[str, int, int, int]] = []
        deltas: List[int] = []
        negative_mints: List[str] = []

        for mint, entry_time in trades:
            cur.execute(
                "SELECT MIN(block_time) FROM swaps WHERE token_mint = ?",
                (mint,),
            )
            first_swap_time = cur.fetchone()[0]
            if first_swap_time is None:
                missing_first_swap += 1
                continue

            try:
                first_swap_int = int(first_swap_time)
            except (ValueError, TypeError):
                fail(
                    f"Non-integer first swap block_time for mint {mint!r}: {first_swap_time!r}"
                )

            delta = entry_time - first_swap_int
            out_rows.append((mint, first_swap_int, entry_time, delta))
            deltas.append(delta)
            if delta < 0 and len(negative_mints) < 10:
                negative_mints.append(mint)

        with open(delta_rows_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter="\t", lineterminator="\n")
            writer.writerow(["mint", "first_swap_time", "entry_time", "delta_sec"])
            writer.writerows(out_rows)

        sorted_deltas = sorted(deltas)
        p10 = percentile(sorted_deltas, 0.10)
        p25 = percentile(sorted_deltas, 0.25)
        p50 = percentile(sorted_deltas, 0.50)
        p75 = percentile(sorted_deltas, 0.75)
        p90 = percentile(sorted_deltas, 0.90)

        le10 = sum(1 for d in deltas if d <= 10)
        le30 = sum(1 for d in deltas if d <= 30)
        le60 = sum(1 for d in deltas if d <= 60)
        le120 = sum(1 for d in deltas if d <= 120)

        min_delta = min(sorted_deltas) if sorted_deltas else None
        max_delta = max(sorted_deltas) if sorted_deltas else None

        neg_count = sum(1 for d in deltas if d < 0)

        with open(summary_path, "w", encoding="utf-8", newline="") as f:
            f.write(f"total_mints\t{total_mints}\n")
            f.write(f"mints_missing_first_swap\t{missing_first_swap}\n")
            f.write(f"p10\t{format_num(p10)}\n")
            f.write(f"p25\t{format_num(p25)}\n")
            f.write(f"p50\t{format_num(p50)}\n")
            f.write(f"p75\t{format_num(p75)}\n")
            f.write(f"p90\t{format_num(p90)}\n")
            f.write(f"pct_le_10s\t{format_pct(le10, len(deltas))}\n")
            f.write(f"pct_le_30s\t{format_pct(le30, len(deltas))}\n")
            f.write(f"pct_le_60s\t{format_pct(le60, len(deltas))}\n")
            f.write(f"pct_le_120s\t{format_pct(le120, len(deltas))}\n")
            f.write(f"min_delta\t{format_num(float(min_delta) if min_delta is not None else None)}\n")
            f.write(f"max_delta\t{format_num(float(max_delta) if max_delta is not None else None)}\n")
            f.write(f"count_negative_deltas\t{neg_count}\n")
            if neg_count > 0:
                f.write("negative_delta_mints_first_10\t" + ",".join(negative_mints) + "\n")

    finally:
        conn.close()

    print(f"OK: wrote {delta_rows_path} and {summary_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
