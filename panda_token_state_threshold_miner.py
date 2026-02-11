#!/usr/bin/env python3
"""Deterministic threshold miner for Panda token states using local SQLite only.

State truth evaluation is performed on a regular per-mint time grid (default 60s bars).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
import sys
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence, Tuple


STATE_ORDER = [
    "TOKEN_DEATH",
    "TOKEN_DISTRIBUTION",
    "TOKEN_EXPANSION",
    "TOKEN_COORDINATION",
    "TOKEN_ACCELERATION",
    "TOKEN_EARLY_TREND",
    "TOKEN_IGNITION",
    "TOKEN_QUIET",
    "TOKEN_BASE_ACTIVITY",
]


KNOWN_BUY = "buy"
KNOWN_SELL = "sell"


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                return None
            return int(v)
        s = str(v).strip()
        if not s:
            return None
        if "." in s:
            return int(float(s))
        return int(s)
    except Exception:
        return None


def quantile(vals: Sequence[float], q: float, default: float = 0.0) -> float:
    if not vals:
        return default
    xs = sorted(float(v) for v in vals)
    if len(xs) == 1:
        return xs[0]
    q = max(0.0, min(1.0, q))
    pos = (len(xs) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return xs[lo]
    w = pos - lo
    return xs[lo] * (1.0 - w) + xs[hi] * w


def unique_sorted_ints(vals: Iterable[int]) -> List[int]:
    return sorted({int(v) for v in vals})


@dataclass
class Event:
    mint: str
    t: int
    wallet: str
    side: str
    sol_lamports: int
    event_type: str


@dataclass
class EventTape:
    events: List[Event]


@dataclass
class GridTape:
    mint: List[str]
    t: List[int]
    event_occurs: List[bool]
    event_count_bar: List[int]
    buy_count_bar: List[int]
    sell_count_bar: List[int]
    lamports_buy_bar: List[int]
    lamports_sell_bar: List[int]
    time_since_last_event: List[int]
    gap_to_prev_event_for_bar_event: List[int]
    # per-bar compact real observables
    bar_wallets_dedup: List[Tuple[str, ...]]
    bar_event_times: List[Tuple[int, ...]]
    bar_event_wallets: List[Tuple[str, ...]]
    bar_event_sides: List[Tuple[str, ...]]
    mint_ranges: Dict[str, Tuple[int, int]]

    @property
    def n(self) -> int:
        return len(self.t)


def discover_schema(conn: sqlite3.Connection) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for (table,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall():
        if table.startswith("sqlite_"):
            continue
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info('{table}')").fetchall()]
        out[table] = cols
    return out


def pick_col(cols: Sequence[str], names: Sequence[str]) -> Optional[str]:
    low = {c.lower(): c for c in cols}
    for n in names:
        if n in low:
            return low[n]
    for c in sorted(cols):
        lc = c.lower()
        for n in names:
            if n in lc:
                return c
    return None


def fetch_rows(conn: sqlite3.Connection, table: str, cols: Sequence[str]) -> Iterable[sqlite3.Row]:
    sel = ", ".join(['"{}"'.format(c) for c in cols])
    sql = f"SELECT {sel} FROM \"{table}\""
    cur = conn.execute(sql)
    while True:
        batch = cur.fetchmany(50000)
        if not batch:
            break
        for row in batch:
            yield row


def normalize_side(v: Any) -> str:
    if v is None:
        return "unknown"
    s = str(v).strip().lower()
    if s in {"buy", "b", "bid", "long", "in"}:
        return KNOWN_BUY
    if s in {"sell", "s", "ask", "short", "out"}:
        return KNOWN_SELL
    return "unknown"


def build_tape_from_whale_events(conn: sqlite3.Connection, cols: List[str]) -> Tuple[Optional[EventTape], Dict[str, Any]]:
    report: Dict[str, Any] = {"table": "whale_events", "usable": False, "reasons": [], "counts": {}}
    mint_c = pick_col(cols, ["mint", "token_mint", "token"])
    t_c = pick_col(cols, ["t", "time", "timestamp", "ts", "block_time"])
    wallet_c = pick_col(cols, ["wallet", "owner", "trader", "address"])
    side_c = pick_col(cols, ["side", "direction", "action"])
    lam_c = pick_col(cols, ["sol_lamports", "lamports", "size_lamports", "amount_lamports"])
    type_c = pick_col(cols, ["event_type", "type", "event"])

    if not mint_c:
        report["reasons"].append("missing mint column")
    if not t_c:
        report["reasons"].append("missing time/timestamp column")
    if not wallet_c:
        report["reasons"].append("missing wallet column")
    if report["reasons"]:
        return None, report

    use_cols = [mint_c, t_c, wallet_c]
    if side_c:
        use_cols.append(side_c)
    if lam_c:
        use_cols.append(lam_c)
    if type_c:
        use_cols.append(type_c)
    idx = {c: i for i, c in enumerate(use_cols)}

    events: List[Event] = []
    total = bad_mint = bad_time = bad_wallet = 0
    for r in fetch_rows(conn, "whale_events", use_cols):
        total += 1
        mint = "" if r[idx[mint_c]] is None else str(r[idx[mint_c]]).strip()
        t = safe_int(r[idx[t_c]])
        wallet = "" if r[idx[wallet_c]] is None else str(r[idx[wallet_c]]).strip()
        if not mint:
            bad_mint += 1
            continue
        if t is None:
            bad_time += 1
            continue
        if not wallet:
            bad_wallet += 1
            continue

        side = normalize_side(r[idx[side_c]]) if side_c else "unknown"
        lam = 0
        if lam_c:
            lv = safe_int(r[idx[lam_c]])
            lam = 0 if lv is None else abs(int(lv))
        et = "whale"
        if type_c and r[idx[type_c]] is not None:
            et = str(r[idx[type_c]])
        events.append(Event(mint=mint, t=int(t), wallet=wallet, side=side, sol_lamports=lam, event_type=et))

    report["counts"] = {
        "rows_total": total,
        "rows_valid": len(events),
        "rows_bad_mint": bad_mint,
        "rows_bad_time": bad_time,
        "rows_bad_wallet": bad_wallet,
    }
    if total == 0:
        report["reasons"].append("whale_events has zero rows")
        return None, report
    if not events:
        report["reasons"].append("all whale_events rows invalid after null/type filtering")
        return None, report

    events.sort(key=lambda e: (e.mint, e.t, e.wallet, e.event_type))
    report["usable"] = True
    return EventTape(events), report


def whale_events_has_token_mint(cols: List[str]) -> bool:
    return pick_col(cols, ["mint", "token_mint", "token", "in_mint", "out_mint"]) is not None


def _resolve_swap_mint(
    token_mint: Optional[Any],
    in_mint: Optional[Any],
    out_mint: Optional[Any],
) -> str:
    if token_mint is not None and str(token_mint).strip():
        return str(token_mint).strip()

    sol_like = {
        "sol",
        "wsol",
        "So11111111111111111111111111111111111111112",
    }
    in_s = "" if in_mint is None else str(in_mint).strip()
    out_s = "" if out_mint is None else str(out_mint).strip()

    in_is_sol = in_s in sol_like
    out_is_sol = out_s in sol_like

    if in_s and not in_is_sol and (out_is_sol or not out_s):
        return in_s
    if out_s and not out_is_sol and (in_is_sol or not in_s):
        return out_s
    if out_s:
        return out_s
    return in_s


def build_tape_from_swaps(conn: sqlite3.Connection, schema: Dict[str, List[str]]) -> Tuple[EventTape, Dict[str, Any]]:
    table = None
    for t in sorted(schema):
        if "swap" in t.lower():
            table = t
            break
    if not table:
        raise RuntimeError("No swaps-like table found for fallback.")

    cols = schema[table]
    mint_c = pick_col(cols, ["token_mint", "mint", "token"])
    in_mint_c = pick_col(cols, ["in_mint", "mint_in", "from_mint"])
    out_mint_c = pick_col(cols, ["out_mint", "mint_out", "to_mint"])
    t_c = pick_col(cols, ["t", "time", "timestamp", "ts", "block_time"])
    wallet_c = pick_col(cols, ["wallet", "owner", "trader", "address", "signer"])
    side_c = pick_col(cols, ["side", "direction", "action"])
    lam_c = pick_col(cols, ["sol_lamports", "lamports", "size_lamports", "amount_lamports", "value_lamports"])
    if not (t_c and wallet_c and lam_c):
        raise RuntimeError("Fallback swaps table missing required time/wallet/lamports columns.")
    if not (mint_c or (in_mint_c and out_mint_c)):
        raise RuntimeError("Fallback swaps table missing token_mint and no usable in_mint/out_mint pair.")

    use_cols = [t_c, wallet_c, lam_c]
    if mint_c:
        use_cols.append(mint_c)
    if in_mint_c:
        use_cols.append(in_mint_c)
    if out_mint_c:
        use_cols.append(out_mint_c)
    if side_c:
        use_cols.append(side_c)
    idx = {c: i for i, c in enumerate(use_cols)}
    raw: List[Tuple[str, int, str, str, int]] = []
    lamports: List[int] = []
    for r in fetch_rows(conn, table, use_cols):
        t = safe_int(r[idx[t_c]])
        wallet = "" if r[idx[wallet_c]] is None else str(r[idx[wallet_c]]).strip()
        lv = safe_int(r[idx[lam_c]])
        mint = _resolve_swap_mint(
            r[idx[mint_c]] if mint_c else None,
            r[idx[in_mint_c]] if in_mint_c else None,
            r[idx[out_mint_c]] if out_mint_c else None,
        )
        if not mint or t is None or not wallet or lv is None:
            continue
        lam = abs(int(lv))
        side = normalize_side(r[idx[side_c]]) if side_c else "unknown"
        raw.append((mint, int(t), wallet, side, lam))
        lamports.append(lam)
    if not raw:
        raise RuntimeError("Fallback swaps table has no usable rows.")

    whale_thr = max(1, int(quantile(lamports, 0.99, 1.0)))
    events = [Event(m, t, w, s, lam, "derived_whale_from_swaps") for (m, t, w, s, lam) in raw if lam >= whale_thr]
    if not events:
        raise RuntimeError("Fallback swaps-derived whale threshold produced zero events.")
    events.sort(key=lambda e: (e.mint, e.t, e.wallet, e.event_type))
    rep = {
        "source": f"{table} (fallback)",
        "fallback_reason": "whale_events truly missing; derived whale threshold from swaps lamports P99",
        "derived_whale_lamports_threshold": whale_thr,
    }
    return EventTape(events), rep


def build_event_tape(
    conn: sqlite3.Connection,
    schema: Dict[str, List[str]],
    outdir: Path,
    source_mode: str,
    strict: bool,
) -> Tuple[EventTape, Dict[str, Any], str]:
    has_whale = "whale_events" in schema
    whale_token_aware = has_whale and whale_events_has_token_mint(schema["whale_events"])

    if source_mode == "swaps":
        tape, rep = build_tape_from_swaps(conn, schema)
        return tape, {"source": rep["source"], "fallback_report": rep}, "swaps"

    if source_mode == "whale_events":
        if not has_whale:
            if strict:
                raise RuntimeError("Requested --source whale_events but table whale_events is missing.")
            eprint("[WARN] whale_events table missing; falling back to swaps")
            tape, rep = build_tape_from_swaps(conn, schema)
            return tape, {"source": rep["source"], "fallback_report": rep}, "swaps"
        if not whale_token_aware:
            msg = "Requested --source whale_events but whale_events has no mint linkage (no mint/token_mint/in_mint/out_mint)."
            if strict:
                raise RuntimeError(msg)
            eprint(f"[WARN] {msg} Falling back to swaps")
            tape, rep = build_tape_from_swaps(conn, schema)
            return tape, {"source": rep["source"], "fallback_report": rep}, "swaps"
        tape, rep = build_tape_from_whale_events(conn, schema["whale_events"])
        if tape is None:
            outdir.mkdir(parents=True, exist_ok=True)
            (outdir / "whale_events_unusable_report.json").write_text(json.dumps(rep, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            raise RuntimeError("whale_events table exists but is unusable; see whale_events_unusable_report.json")
        return tape, {"source": "whale_events", "whale_events_report": rep}, "whale_events"

    # auto mode
    if has_whale and whale_token_aware:
        tape, rep = build_tape_from_whale_events(conn, schema["whale_events"])
        if tape is not None:
            return tape, {"source": "whale_events", "whale_events_report": rep}, "whale_events"
        outdir.mkdir(parents=True, exist_ok=True)
        (outdir / "whale_events_unusable_report.json").write_text(json.dumps(rep, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        raise RuntimeError("whale_events table exists but is unusable; see whale_events_unusable_report.json")

    tape, rep = build_tape_from_swaps(conn, schema)
    return tape, {"source": rep["source"], "fallback_report": rep}, "swaps"


def split_by_mint(events: List[Event]) -> Dict[str, List[Event]]:
    out: Dict[str, List[Event]] = defaultdict(list)
    for e in events:
        out[e.mint].append(e)
    for m in out:
        out[m].sort(key=lambda x: (x.t, x.wallet, x.event_type))
    return dict(out)


def build_grid_tape(tape: EventTape, grid_seconds: int) -> GridTape:
    by_mint = split_by_mint(tape.events)

    gm: List[str] = []
    gt: List[int] = []
    event_occurs: List[bool] = []
    event_count_bar: List[int] = []
    buy_count_bar: List[int] = []
    sell_count_bar: List[int] = []
    lam_buy_bar: List[int] = []
    lam_sell_bar: List[int] = []
    time_since_last: List[int] = []
    gap_to_prev_for_event_bar: List[int] = []
    bar_wallets_dedup: List[Tuple[str, ...]] = []
    bar_event_times: List[Tuple[int, ...]] = []
    bar_event_wallets: List[Tuple[str, ...]] = []
    bar_event_sides: List[Tuple[str, ...]] = []
    mint_ranges: Dict[str, Tuple[int, int]] = {}

    for mint in sorted(by_mint):
        evs = by_mint[mint]
        tmin = evs[0].t
        tmax = evs[-1].t
        gstart = (tmin // grid_seconds) * grid_seconds
        gend = ((tmax + grid_seconds - 1) // grid_seconds) * grid_seconds
        times = list(range(gstart, gend + 1, grid_seconds))

        start_ix = len(gt)
        ei = 0
        last_event_t: Optional[int] = None
        prev_event_t: Optional[int] = None

        for T in times:
            bar_start = T - grid_seconds
            curr: List[Event] = []
            while ei < len(evs) and evs[ei].t <= T:
                if evs[ei].t > bar_start:
                    curr.append(evs[ei])
                ei += 1

            curr.sort(key=lambda e: (e.t, e.wallet, e.event_type))
            occurs = len(curr) > 0
            b = sum(1 for e in curr if e.side == KNOWN_BUY)
            s = sum(1 for e in curr if e.side == KNOWN_SELL)
            lb = sum(int(e.sol_lamports) for e in curr if e.side == KNOWN_BUY)
            ls = sum(int(e.sol_lamports) for e in curr if e.side == KNOWN_SELL)

            etimes = tuple(e.t for e in curr)
            ewallets = tuple(e.wallet for e in curr)
            esides = tuple(e.side for e in curr)
            wdedup = tuple(sorted({e.wallet for e in curr}))

            if occurs:
                first_ev_t = etimes[0]
                gap_ev = 10**9 if prev_event_t is None else max(0, first_ev_t - prev_event_t)
                prev_event_t = etimes[-1]
                last_event_t = prev_event_t
            else:
                gap_ev = 0

            tsle = 10**9 if last_event_t is None else max(0, T - last_event_t)

            gm.append(mint)
            gt.append(T)
            event_occurs.append(occurs)
            event_count_bar.append(len(curr))
            buy_count_bar.append(b)
            sell_count_bar.append(s)
            lam_buy_bar.append(lb)
            lam_sell_bar.append(ls)
            time_since_last.append(tsle)
            gap_to_prev_for_event_bar.append(gap_ev)
            bar_wallets_dedup.append(wdedup)
            bar_event_times.append(etimes)
            bar_event_wallets.append(ewallets)
            bar_event_sides.append(esides)

        mint_ranges[mint] = (start_ix, len(gt))

    return GridTape(
        mint=gm,
        t=gt,
        event_occurs=event_occurs,
        event_count_bar=event_count_bar,
        buy_count_bar=buy_count_bar,
        sell_count_bar=sell_count_bar,
        lamports_buy_bar=lam_buy_bar,
        lamports_sell_bar=lam_sell_bar,
        time_since_last_event=time_since_last,
        gap_to_prev_event_for_bar_event=gap_to_prev_for_event_bar,
        bar_wallets_dedup=bar_wallets_dedup,
        bar_event_times=bar_event_times,
        bar_event_wallets=bar_event_wallets,
        bar_event_sides=bar_event_sides,
        mint_ranges=mint_ranges,
    )


def collect_event_gaps(events: List[Event]) -> List[int]:
    by_mint = split_by_mint(events)
    gaps: List[int] = []
    for mint in sorted(by_mint):
        evs = by_mint[mint]
        for i in range(1, len(evs)):
            g = evs[i].t - evs[i - 1].t
            if g > 0:
                gaps.append(g)
    return gaps


def kmeans_1d_two(values: Sequence[float]) -> Tuple[float, float, List[int]]:
    c1 = quantile(values, 0.25)
    c2 = quantile(values, 0.75)
    if c1 == c2:
        c2 = c1 + 1e-6
    labels = [0] * len(values)
    for _ in range(50):
        changed = False
        for i, v in enumerate(values):
            l = 0 if abs(v - c1) <= abs(v - c2) else 1
            if labels[i] != l:
                labels[i] = l
                changed = True
        g1 = [v for v, l in zip(values, labels) if l == 0]
        g2 = [v for v, l in zip(values, labels) if l == 1]
        if not g1 or not g2:
            break
        n1 = sum(g1) / len(g1)
        n2 = sum(g2) / len(g2)
        if not changed and abs(n1 - c1) < 1e-10 and abs(n2 - c2) < 1e-10:
            break
        c1, c2 = n1, n2
    if c1 <= c2:
        return c1, c2, labels
    return c2, c1, [1 - l for l in labels]


def mode_hist(vals: Sequence[float], bins: int = 200) -> float:
    lo, hi = min(vals), max(vals)
    if hi <= lo + 1e-12:
        return lo
    step = (hi - lo) / bins
    hist = [0] * bins
    for v in vals:
        i = int((v - lo) / step)
        i = max(0, min(bins - 1, i))
        hist[i] += 1
    best = max(range(bins), key=lambda i: (hist[i], -i))
    return lo + (best + 0.5) * step


def valley_hist(vals: Sequence[float], left: float, right: float, bins: int = 400) -> float:
    lo, hi = min(vals), max(vals)
    if hi <= lo + 1e-12:
        return lo
    step = (hi - lo) / bins
    hist = [0] * bins
    for v in vals:
        i = int((v - lo) / step)
        i = max(0, min(bins - 1, i))
        hist[i] += 1
    li = max(0, min(bins - 1, int((left - lo) / step)))
    ri = max(0, min(bins - 1, int((right - lo) / step)))
    if li > ri:
        li, ri = ri, li
    best = min(range(li, ri + 1), key=lambda i: (hist[i], i))
    return lo + (best + 0.5) * step


def mine_silence_thresholds(gaps: List[int]) -> Tuple[int, int, Dict[str, Any]]:
    if not gaps:
        raise RuntimeError("No positive inter-event gaps available for silence mining")
    lg = [math.log(g) for g in gaps]
    m1, m2, labels = kmeans_1d_two(lg)
    spread = quantile(lg, 0.95) - quantile(lg, 0.05)
    sep = m2 - m1

    if sep > max(0.15, 0.15 * spread):
        valley = valley_hist(lg, m1, m2)
        ignition = max(1, int(round(math.exp(valley))))
        inactive = [math.exp(v) for v, l in zip(lg, labels) if l == 1]
        if inactive:
            death = int(round(quantile(inactive, 0.35)))
        else:
            death = int(round(math.exp((valley + m2) / 2.0)))
        method = "kmeans2_log_gap_valley_for_ignition_and_inactive_anchor_for_death"
    else:
        p90 = quantile(gaps, 0.9)
        active = [math.log(g) for g in gaps if g <= p90] or lg
        inactive = [math.log(g) for g in gaps if g > p90] or lg
        am = mode_hist(active)
        im = mode_hist(inactive)
        ignition = max(1, int(round(math.exp(am))))
        death = int(round(math.exp((am + im) / 2.0)))
        method = "hist_mode_active_inactive_fallback"

    death = max(2, death)
    if death <= ignition:
        death = ignition + max(1, ignition // 5)

    rep = {
        "method": method,
        "gap_count": len(gaps),
        "active_mean_log": m1,
        "inactive_mean_log": m2,
        "separation": sep,
        "IGNITION_SILENCE_THRESHOLD": ignition,
        "DEATH_SILENCE_THRESHOLD": death,
    }
    return ignition, death, rep


def rolling_sum(arr: List[int], times: List[int], W: int) -> List[int]:
    out = [0] * len(arr)
    left = 0
    s = 0
    for i, t in enumerate(times):
        while left <= i and times[left] <= t - W:
            s -= arr[left]
            left += 1
        s += arr[i]
        out[i] = s
    return out


def rolling_observables(grid: GridTape, W: int) -> Dict[str, List[float]]:
    """Exact rolling observables from real per-event timestamps and wallets.

    - unique_whales_W: exact distinct wallets in (T-W, T]
    - mean_interarrival_W: mean diffs of sorted true event timestamps in (T-W, T]
    """
    uniq = [0] * grid.n
    mean_inter = [float(W)] * grid.n
    rate = [0.0] * grid.n

    for mint in sorted(grid.mint_ranges):
        s, e = grid.mint_ranges[mint]
        # exact rolling distinct wallet counter
        ev_queue: Deque[Tuple[int, str]] = deque()
        wallet_counter: Dict[str, int] = defaultdict(int)

        # exact rolling time queue for interarrival and event rate
        t_queue: Deque[int] = deque()
        sum_diffs = 0.0  # sum of consecutive diffs inside t_queue

        for gi in range(s, e):
            T = grid.t[gi]

            # add current bar events (all true event timestamps and wallet labels)
            bt = grid.bar_event_times[gi]
            bw = grid.bar_event_wallets[gi]
            # these are already sorted by time at build stage
            for k in range(len(bt)):
                et = bt[k]
                ew = bw[k]
                ev_queue.append((et, ew))
                wallet_counter[ew] += 1

                if t_queue:
                    sum_diffs += float(et - t_queue[-1])
                t_queue.append(et)

            # pop old events <= T-W to keep open-left interval (T-W, T]
            cutoff = T - W
            while ev_queue and ev_queue[0][0] <= cutoff:
                old_t, old_w = ev_queue.popleft()
                wallet_counter[old_w] -= 1
                if wallet_counter[old_w] <= 0:
                    del wallet_counter[old_w]

            while t_queue and t_queue[0] <= cutoff:
                if len(t_queue) >= 2:
                    # remove edge between first and second
                    second = t_queue[1]
                    first = t_queue[0]
                    sum_diffs -= float(second - first)
                t_queue.popleft()

            uniq[gi] = len(wallet_counter)
            n_events = len(t_queue)
            rate[gi] = float(n_events) / float(max(1, W))
            if n_events >= 2:
                mean_inter[gi] = sum_diffs / float(n_events - 1)
            elif n_events == 1:
                mean_inter[gi] = float(W)
            else:
                mean_inter[gi] = float(W)

    return {"unique": uniq, "mean_inter": mean_inter, "rate": rate}


def transitions_per_hour(mask: List[bool], grid: GridTape) -> float:
    trans = 0
    hours = 0.0
    for mint in sorted(grid.mint_ranges):
        s, e = grid.mint_ranges[mint]
        m = mask[s:e]
        for i in range(1, len(m)):
            if m[i] != m[i - 1]:
                trans += 1
        if e - s >= 2:
            hours += max(1, grid.t[e - 1] - grid.t[s]) / 3600.0
    return trans / hours if hours > 0 else float("inf")


def mine_coordination(grid: GridTape, gaps: List[int], death_thr: int) -> Tuple[Dict[str, Any], Dict[str, List[Any]], Dict[str, Any]]:
    c_windows = unique_sorted_ints(max(10, min(600, int(quantile(gaps, q, 60.0)))) for q in (0.10, 0.25, 0.50, 0.75))
    if not c_windows:
        c_windows = [60]

    candidates = []
    act_rates = []
    cache: Dict[int, Dict[str, List[float]]] = {}
    for W in c_windows:
        obs = rolling_observables(grid, W)
        cache[W] = obs
        uniq = obs["unique"]
        mean_inter = obs["mean_inter"]
        uthr_c = unique_sorted_ints(max(1, int(quantile(uniq, q))) for q in (0.60, 0.70, 0.80, 0.90))
        ithr_c = unique_sorted_ints(max(1, int(quantile(mean_inter, q))) for q in (0.10, 0.25, 0.40, 0.50))
        for uthr in uthr_c:
            for ithr in ithr_c:
                mask = [uniq[i] >= uthr and mean_inter[i] <= ithr for i in range(grid.n)]
                act = sum(mask) / max(1, grid.n)
                act_rates.append(act)
                collisions = sum(1 for i in range(grid.n) if mask[i] and grid.time_since_last_event[i] >= death_thr) / max(1, grid.n)
                flick = transitions_per_hour(mask, grid)
                candidates.append({"W": W, "uthr": uthr, "ithr": ithr, "activation": act, "collisions": collisions, "flicker": flick})

    lo = quantile(act_rates, 0.05, 0.0)
    hi = quantile(act_rates, 0.20, 1.0)
    best = min(
        candidates,
        key=lambda c: (
            0 if lo <= c["activation"] <= hi else 1,
            c["collisions"],
            c["flicker"],
            abs(c["activation"] - ((lo + hi) / 2.0)),
            c["W"],
            c["uthr"],
            c["ithr"],
        ),
    )

    obs = cache[int(best["W"])]
    coord_mask = [obs["unique"][i] >= int(best["uthr"]) and obs["mean_inter"][i] <= int(best["ithr"]) for i in range(grid.n)]
    return (
        {
            "COORD_WINDOW_SECONDS": int(best["W"]),
            "COORD_UNIQUE_WHALES_THRESHOLD": int(best["uthr"]),
            "COORD_INTERARRIVAL_THRESHOLD": int(best["ithr"]),
        },
        {"coord_unique": [int(x) for x in obs["unique"]], "coord_inter": obs["mean_inter"], "coord_mask": coord_mask},
        {"candidate_windows": c_windows, "activation_band": [lo, hi], "selected": best, "num_candidates": len(candidates)},
    )


def mine_acceleration(grid: GridTape, coord_w: int, coord_mask: List[bool]) -> Tuple[Dict[str, Any], Dict[str, List[Any]], Dict[str, Any]]:
    short_c = unique_sorted_ints(max(5, min(coord_w, x)) for x in [coord_w // 4, coord_w // 3, coord_w // 2, int(math.sqrt(max(1, coord_w)) * 2)])
    long_c = unique_sorted_ints(max(coord_w + 1, x) for x in [coord_w, int(coord_w * 1.5), coord_w * 2, coord_w * 3])
    long_c = [x for x in long_c if x > min(short_c)] or [max(coord_w + 1, min(short_c) + 1)]

    best = None
    best_key = None
    best_feats = None
    cand_count = 0

    for ws in short_c:
        obs_s = rolling_observables(grid, ws)
        for wl in long_c:
            if wl <= ws:
                continue
            obs_l = rolling_observables(grid, wl)
            rs = obs_s["rate"]
            rl = obs_l["rate"]
            ratio = [rs[i] / max(1e-12, rl[i]) for i in range(grid.n)]
            thr_c = sorted({quantile(ratio, q, 1.2) for q in (0.70, 0.80, 0.90, 0.95) if quantile(ratio, q, 1.2) > 1.0}) or [1.2]
            early_proxy_thr = quantile(rl, 0.75, 0.0)
            for rthr in thr_c:
                cand_count += 1
                mask = [ratio[i] >= rthr for i in range(grid.n)]
                overlap_coord = sum(1 for i in range(grid.n) if mask[i] and coord_mask[i]) / max(1, grid.n)
                overlap_early = sum(1 for i in range(grid.n) if mask[i] and rl[i] >= early_proxy_thr) / max(1, grid.n)
                flick = transitions_per_hour(mask, grid)
                act = sum(mask) / max(1, grid.n)
                key = (overlap_coord + overlap_early, flick, abs(act - 0.10), ws, wl, rthr)
                if best_key is None or key < best_key:
                    best_key = key
                    best = {"ws": ws, "wl": wl, "rthr": float(rthr), "activation": act, "flicker": flick}
                    best_feats = (rs, rl, ratio, mask)

    assert best is not None and best_feats is not None
    rs, rl, ratio, mask = best_feats
    return (
        {"ACCEL_SHORT_WINDOW": int(best["ws"]), "ACCEL_LONG_WINDOW": int(best["wl"]), "ACCEL_RATIO_THRESHOLD": float(best["rthr"])},
        {"rate_short": rs, "rate_long": rl, "accel_ratio": ratio, "accel_mask": mask},
        {"short_candidates": short_c, "long_candidates": long_c, "selected": best, "num_candidates": cand_count},
    )


def mine_distribution(grid: GridTape, coord_w: int) -> Tuple[Dict[str, Any], Dict[str, List[Any]], Dict[str, Any]]:
    event_times = [grid.t[i] for i in range(grid.n) if grid.event_occurs[i]]
    lengths: List[int] = []
    if event_times:
        st = pv = event_times[0]
        for t in event_times[1:]:
            if t - pv > coord_w:
                lengths.append(pv - st)
                st = t
            pv = t
        lengths.append(pv - st)
    med_ep = max(60, int(quantile(lengths, 0.50, 60.0)))
    q75_ep = max(60, int(quantile(lengths, 0.75, float(med_ep))))
    cand_w = unique_sorted_ints(max(1, min(120, x // 60)) for x in [coord_w, med_ep, (coord_w + med_ep) // 2, q75_ep])

    # side-quality report from real side labels
    total_events = 0
    unknown_events = 0
    per_mint_unknown_ratio: List[float] = []
    for mint in sorted(grid.mint_ranges):
        s, e = grid.mint_ranges[mint]
        m_total = 0
        m_unknown = 0
        for i in range(s, e):
            sides = grid.bar_event_sides[i]
            m_total += len(sides)
            for sd in sides:
                if sd not in {KNOWN_BUY, KNOWN_SELL}:
                    m_unknown += 1
        if m_total > 0:
            per_mint_unknown_ratio.append(m_unknown / m_total)
        total_events += m_total
        unknown_events += m_unknown

    global_unknown_ratio = (unknown_events / total_events) if total_events > 0 else 1.0
    unknown_threshold = quantile(per_mint_unknown_ratio, 0.75, 1.0)
    side_unusable = total_events == 0 or global_unknown_ratio > unknown_threshold

    if side_unusable:
        # no guessing: distribution is disabled and reported
        false_mask = [False] * grid.n
        params = {
            "NET_FLOW_WINDOW": int(cand_w[0] if cand_w else max(1, coord_w // 60)),
            "SELL_DOM_THRESHOLD": 1.0,
            "MIN_ACTIVITY_FOR_DISTRIBUTION": 10**9,
        }
        feats = {
            "buy_count": [0] * grid.n,
            "sell_count": [0] * grid.n,
            "buy_lamports": [0] * grid.n,
            "sell_lamports": [0] * grid.n,
            "activity_count": [0] * grid.n,
            "sell_ratio": [0.0] * grid.n,
            "net_flow": [0] * grid.n,
            "distribution_mask": false_mask,
        }
        report = {
            "window_candidates_min": cand_w,
            "selected": "disabled_due_to_side_unknown_ratio",
            "num_candidates": 0,
            "side_quality": {
                "total_events": total_events,
                "unknown_events": unknown_events,
                "global_unknown_ratio": global_unknown_ratio,
                "unknown_ratio_threshold_X": unknown_threshold,
                "distribution_disabled": True,
            },
        }
        return params, feats, report

    best = None
    best_key = None
    best_feats = None
    num = 0

    for wmin in cand_w:
        W = wmin * 60
        buy_roll = rolling_sum(grid.buy_count_bar, grid.t, W)
        sell_roll = rolling_sum(grid.sell_count_bar, grid.t, W)
        buy_lam_roll = rolling_sum(grid.lamports_buy_bar, grid.t, W)
        sell_lam_roll = rolling_sum(grid.lamports_sell_bar, grid.t, W)
        total = [buy_roll[i] + sell_roll[i] for i in range(grid.n)]
        sell_ratio = [sell_roll[i] / max(1, total[i]) for i in range(grid.n)]
        net = [buy_roll[i] - sell_roll[i] for i in range(grid.n)]

        sthr_c = sorted({quantile(sell_ratio, q, 0.6) for q in (0.70, 0.80, 0.90, 0.95)}) or [0.6]
        athr_c = unique_sorted_ints(max(1, int(quantile(total, q, 1.0))) for q in (0.50, 0.60, 0.70, 0.80, 0.90))
        for sthr in sthr_c:
            for athr in athr_c:
                num += 1
                mask = [total[i] >= athr and sell_ratio[i] >= sthr and net[i] < 0 for i in range(grid.n)]
                meaningful = sum(1 for x in total if x >= athr) / max(1, grid.n)
                act = sum(mask) / max(1, grid.n)
                flick = transitions_per_hour(mask, grid)
                key = (abs(meaningful - 0.25), abs(act - 0.08), flick, wmin, sthr, athr)
                if best_key is None or key < best_key:
                    best_key = key
                    best = {"wmin": wmin, "sthr": float(sthr), "athr": int(athr), "activation": act, "flicker": flick}
                    best_feats = (buy_roll, sell_roll, buy_lam_roll, sell_lam_roll, total, sell_ratio, net, mask)

    assert best is not None and best_feats is not None
    buy_roll, sell_roll, buy_lam_roll, sell_lam_roll, total, sell_ratio, net, mask = best_feats
    return (
        {"NET_FLOW_WINDOW": int(best["wmin"]), "SELL_DOM_THRESHOLD": float(best["sthr"]), "MIN_ACTIVITY_FOR_DISTRIBUTION": int(best["athr"])},
        {
            "buy_count": buy_roll,
            "sell_count": sell_roll,
            "buy_lamports": buy_lam_roll,
            "sell_lamports": sell_lam_roll,
            "activity_count": total,
            "sell_ratio": sell_ratio,
            "net_flow": net,
            "distribution_mask": mask,
        },
        {
            "window_candidates_min": cand_w,
            "selected": best,
            "num_candidates": num,
            "side_quality": {
                "total_events": total_events,
                "unknown_events": unknown_events,
                "global_unknown_ratio": global_unknown_ratio,
                "unknown_ratio_threshold_X": unknown_threshold,
                "distribution_disabled": False,
            },
        },
    )


def mine_expansion(grid: GridTape, coord_w: int) -> Tuple[Dict[str, Any], Dict[str, List[Any]], Dict[str, Any]]:
    W = max(30, coord_w)
    # exact unique whales in W from real observables
    obs = rolling_observables(grid, W)
    growth = [float(x) for x in obs["unique"]]
    events_roll = rolling_sum(grid.event_count_bar, grid.t, W)
    diversity = [growth[i] / max(1.0, float(events_roll[i])) for i in range(grid.n)]

    gthr_c = sorted({quantile(growth, q, 2.0) for q in (0.60, 0.70, 0.80, 0.90) if quantile(growth, q, 2.0) >= 2.0}) or [2.0]
    dthr_c = sorted({quantile(diversity, q, 0.5) for q in (0.50, 0.60, 0.70, 0.80, 0.90) if quantile(diversity, q, 0.5) > 0}) or [0.5]

    best = None
    best_key = None
    best_mask = None
    num = 0
    for gt in gthr_c:
        for dt in dthr_c:
            num += 1
            mask = [growth[i] >= gt and diversity[i] >= dt for i in range(grid.n)]
            act = sum(mask) / max(1, grid.n)
            flick = transitions_per_hour(mask, grid)
            key = (abs(act - 0.10), flick, -gt, -dt)
            if best_key is None or key < best_key:
                best_key = key
                best = {"gt": float(gt), "dt": float(dt), "activation": act, "flicker": flick}
                best_mask = mask

    assert best is not None and best_mask is not None
    return (
        {"EXPANSION_GROWTH_THRESHOLD": float(best["gt"]), "DIVERSITY_THRESHOLD": float(best["dt"])},
        {"growth": growth, "diversity": diversity, "expansion_mask": best_mask},
        {"window_seconds": W, "selected": best, "num_candidates": num},
    )


def assign_states(grid: GridTape, thresholds: Dict[str, Any], feats: Dict[str, List[Any]]) -> Tuple[Dict[str, List[bool]], List[str]]:
    death_thr = int(thresholds["DEATH_SILENCE_THRESHOLD"])
    ign_thr = int(thresholds["IGNITION_SILENCE_THRESHOLD"])
    if death_thr <= ign_thr:
        raise RuntimeError("Invariant violated: DEATH_SILENCE_THRESHOLD must be > IGNITION_SILENCE_THRESHOLD")

    coord_w = int(thresholds["COORD_WINDOW_SECONDS"])
    events_in_death = rolling_sum(grid.event_count_bar, grid.t, death_thr)
    events_in_coord = rolling_sum(grid.event_count_bar, grid.t, coord_w)

    death = [
        (events_in_death[i] == 0) and (grid.time_since_last_event[i] >= death_thr) and (not grid.event_occurs[i])
        for i in range(grid.n)
    ]
    ignition = [
        grid.event_occurs[i] and (grid.gap_to_prev_event_for_bar_event[i] >= ign_thr) and (not death[i])
        for i in range(grid.n)
    ]

    coord = feats["coord_mask"]
    accel = feats["accel_mask"]
    distr = feats["distribution_mask"]
    expan = feats["expansion_mask"]

    rate_long = feats["rate_long"]
    early_thr = quantile(rate_long, 0.75, 0.0)
    early = [rate_long[i] >= early_thr and not coord[i] and not accel[i] and not distr[i] and not expan[i] for i in range(grid.n)]

    quiet = [
        (not death[i])
        and (not ignition[i])
        and grid.time_since_last_event[i] < ign_thr
        and (not coord[i])
        and (not accel[i])
        and (not distr[i])
        and (not expan[i])
        for i in range(grid.n)
    ]

    base = [events_in_coord[i] > 0 for i in range(grid.n)]

    states = {
        "TOKEN_DEATH": death,
        "TOKEN_DISTRIBUTION": distr,
        "TOKEN_EXPANSION": expan,
        "TOKEN_COORDINATION": coord,
        "TOKEN_ACCELERATION": accel,
        "TOKEN_EARLY_TREND": early,
        "TOKEN_IGNITION": ignition,
        "TOKEN_QUIET": quiet,
        "TOKEN_BASE_ACTIVITY": base,
    }

    final = [""] * grid.n
    for s in STATE_ORDER:
        for i in range(grid.n):
            if final[i] == "" and states[s][i]:
                final[i] = s
    for i in range(grid.n):
        if final[i] == "":
            final[i] = "TOKEN_QUIET"
    return states, final


def overlap_counts(states: Dict[str, List[bool]]) -> Dict[Tuple[str, str], Tuple[int, int]]:
    out: Dict[Tuple[str, str], Tuple[int, int]] = {}
    for a in STATE_ORDER:
        for b in STATE_ORDER:
            inc = sum(1 for i in range(len(states[a])) if states[a][i] and states[b][i])
            exc = 0 if (a == "TOKEN_BASE_ACTIVITY" or b == "TOKEN_BASE_ACTIVITY") else inc
            out[(a, b)] = (inc, exc)
    return out


def transition_rows(grid: GridTape, final: List[str]) -> List[Tuple[str, float, int, int]]:
    rows = []
    for mint in sorted(grid.mint_ranges):
        s, e = grid.mint_ranges[mint]
        seq = final[s:e]
        trans = sum(1 for i in range(1, len(seq)) if seq[i] != seq[i - 1])
        if e - s >= 2:
            dur = max(1, grid.t[e - 1] - grid.t[s])
            tph = trans / (dur / 3600.0)
        else:
            tph = 0.0
        rows.append((mint, tph, trans, e - s))
    rows.sort(key=lambda x: (-x[1], x[0]))
    return rows


def write_artifacts(outdir: Path, thresholds: Dict[str, Any], grid: GridTape, states: Dict[str, List[bool]], final: List[str]) -> Dict[str, str]:
    outdir.mkdir(parents=True, exist_ok=True)

    thresholds_json = json.dumps(thresholds, sort_keys=True, indent=2)
    (outdir / "thresholds.json").write_text(thresholds_json + "\n", encoding="utf-8")

    overlaps = overlap_counts(states)
    with (outdir / "state_overlap_matrix.tsv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["state_a", "state_b", "overlaps_including_base", "overlaps_excluding_base"])
        for a in STATE_ORDER:
            for b in STATE_ORDER:
                inc, exc = overlaps[(a, b)]
                w.writerow([a, b, inc, exc])

    counts = Counter(final)
    with (outdir / "post_arbitration_state_counts.tsv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["state", "count", "pct"])
        for s in STATE_ORDER:
            c = int(counts.get(s, 0))
            p = c / max(1, len(final))
            w.writerow([s, c, f"{p:.8f}"])

    tr = transition_rows(grid, final)
    with (outdir / "transition_rate_report.tsv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["mint", "transitions_per_hour", "transition_count", "grid_points"])
        for mint, tph, tc, n in tr:
            w.writerow([mint, f"{tph:.8f}", tc, n])

    with (outdir / "top_collision_examples.jsonl").open("w", encoding="utf-8") as f:
        num = 0
        for i in range(grid.n):
            trues_ex_base = [s for s in STATE_ORDER if s != "TOKEN_BASE_ACTIVITY" and states[s][i]]
            if len(trues_ex_base) > 1:
                rec = {"mint": grid.mint[i], "time": grid.t[i], "states_true": trues_ex_base}
                f.write(json.dumps(rec, sort_keys=True) + "\n")
                num += 1
                if num >= 500:
                    break

    with (outdir / "top_flicker_examples.jsonl").open("w", encoding="utf-8") as f:
        for mint, tph, tc, n in tr[:50]:
            f.write(json.dumps({"mint": mint, "transitions_per_hour": tph, "transition_count": tc, "grid_points": n}, sort_keys=True) + "\n")

    return {
        "thresholds_json": thresholds_json,
        "post_counts_tsv": (outdir / "post_arbitration_state_counts.tsv").read_text(encoding="utf-8"),
    }


def run_once(db: Path, outdir: Path, grid_seconds: int, source_mode: str, strict: bool) -> Dict[str, str]:
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    try:
        schema = discover_schema(conn)
        if not schema:
            raise RuntimeError("No tables found in SQLite DB.")

        event_tape, source_report, resolved_source = build_event_tape(conn, schema, outdir, source_mode, strict)
        print(f"[INFO] token_state_source={resolved_source}")
        gaps = collect_event_gaps(event_tape.events)
        if not gaps:
            raise RuntimeError("Cannot mine thresholds: no positive inter-event gaps.")

        ign_thr, death_thr, silence_report = mine_silence_thresholds(gaps)
        if death_thr <= ign_thr:
            death_thr = ign_thr + max(1, ign_thr // 5)

        grid = build_grid_tape(event_tape, grid_seconds)
        coord_params, coord_feats, coord_report = mine_coordination(grid, gaps, death_thr)
        accel_params, accel_feats, accel_report = mine_acceleration(grid, int(coord_params["COORD_WINDOW_SECONDS"]), coord_feats["coord_mask"])
        dist_params, dist_feats, dist_report = mine_distribution(grid, int(coord_params["COORD_WINDOW_SECONDS"]))
        exp_params, exp_feats, exp_report = mine_expansion(grid, int(coord_params["COORD_WINDOW_SECONDS"]))

        feats: Dict[str, List[Any]] = {}
        feats.update(coord_feats)
        feats.update(accel_feats)
        feats.update(dist_feats)
        feats.update(exp_feats)

        thresholds: Dict[str, Any] = {}
        thresholds.update(coord_params)
        thresholds.update(accel_params)
        thresholds.update(dist_params)
        thresholds.update(exp_params)
        thresholds["GRID_SECONDS"] = int(grid_seconds)
        thresholds["IGNITION_SILENCE_THRESHOLD"] = int(ign_thr)
        thresholds["DEATH_SILENCE_THRESHOLD"] = int(death_thr)

        states, final = assign_states(grid, thresholds, feats)

        ign_count = sum(1 for s in final if s == "TOKEN_IGNITION")
        if ign_count == 0:
            eligible = [g for g in gaps if g < thresholds["DEATH_SILENCE_THRESHOLD"]]
            if eligible:
                thresholds["IGNITION_SILENCE_THRESHOLD"] = max(1, int(quantile(eligible, 0.70, 1.0)))
                if thresholds["DEATH_SILENCE_THRESHOLD"] <= thresholds["IGNITION_SILENCE_THRESHOLD"]:
                    thresholds["DEATH_SILENCE_THRESHOLD"] = thresholds["IGNITION_SILENCE_THRESHOLD"] + 1
                states, final = assign_states(grid, thresholds, feats)

        thresholds["metadata"] = {
            "db_sha256": sha256_file(db),
            "script_sha256": sha256_file(Path(__file__).resolve()),
            "run_timestamp_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "deterministic_seed": 0,
            "event_source": source_report,
            "reports": {
                "silence": silence_report,
                "coordination": coord_report,
                "acceleration": accel_report,
                "distribution": dist_report,
                "expansion": exp_report,
            },
        }

        (outdir / "mining_report.json").write_text(
            json.dumps(
                {
                    "schema_tables": sorted(schema.keys()),
                    "source": source_report,
                    "silence": silence_report,
                    "coordination": coord_report,
                    "acceleration": accel_report,
                    "distribution": dist_report,
                    "expansion": exp_report,
                    "grid_seconds": grid_seconds,
                },
                sort_keys=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        return write_artifacts(outdir, thresholds, grid, states, final)
    finally:
        conn.close()


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Deterministic Panda token state threshold miner")
    p.add_argument("--db", required=True)
    p.add_argument("--outdir", required=True)
    p.add_argument("--strict", action="store_true")
    p.add_argument("--grid-seconds", type=int, default=60, help="Per-mint regular grid step in seconds (default: 60)")
    p.add_argument("--source", choices=["auto", "swaps", "whale_events"], default="auto", help="Token-state source selection (default: auto)")
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    db = Path(args.db).resolve()
    outdir = Path(args.outdir).resolve()
    if not db.exists():
        eprint(f"ERROR: DB not found: {db}")
        return 1
    if args.grid_seconds <= 0:
        eprint("ERROR: --grid-seconds must be positive")
        return 1

    try:
        first = run_once(db, outdir, args.grid_seconds, args.source, args.strict)
        second = run_once(db, outdir, args.grid_seconds, args.source, args.strict)

        j1 = json.loads(first["thresholds_json"])
        j2 = json.loads(second["thresholds_json"])
        for j in (j1, j2):
            if "metadata" in j and "run_timestamp_utc" in j["metadata"]:
                j["metadata"]["run_timestamp_utc"] = "<normalized>"
        if json.dumps(j1, sort_keys=True) != json.dumps(j2, sort_keys=True):
            raise RuntimeError("Determinism check failed: thresholds content mismatch")
        if first["post_counts_tsv"] != second["post_counts_tsv"]:
            raise RuntimeError("Determinism check failed: post_arbitration_state_counts.tsv mismatch")
        return 0
    except Exception as ex:
        eprint(f"ERROR: {ex}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
