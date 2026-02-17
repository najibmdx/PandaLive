#!/usr/bin/env python3
"""Deterministic threshold miner for Panda token states using local SQLite only."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


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
    if len(vals) == 1:
        return float(vals[0])
    xs = sorted(float(v) for v in vals)
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

    @property
    def n(self) -> int:
        return len(self.events)


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
    sel = ', '.join(['"{}"'.format(c) for c in cols])
    sql = f"SELECT {sel} FROM \"{table}\""
    cur = conn.execute(sql)
    while True:
        batch = cur.fetchmany(50000)
        if not batch:
            break
        for row in batch:
            yield row


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
    total = 0
    bad_mint = bad_time = bad_wallet = bad_type = 0
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
        if not isinstance(t, int):
            bad_type += 1
            continue
        side = "unknown"
        if side_c:
            side = "unknown" if r[idx[side_c]] is None else str(r[idx[side_c]]).lower().strip()
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
        "rows_bad_type": bad_type,
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


def build_tape_from_swaps(conn: sqlite3.Connection, schema: Dict[str, List[str]]) -> Tuple[EventTape, Dict[str, Any]]:
    table = None
    for t in sorted(schema):
        if "swap" in t.lower():
            table = t
            break
    if not table:
        raise RuntimeError("No swaps-like table found for fallback.")
    cols = schema[table]
    mint_c = pick_col(cols, ["mint", "token_mint", "token"])
    t_c = pick_col(cols, ["t", "time", "timestamp", "ts", "block_time"])
    wallet_c = pick_col(cols, ["wallet", "owner", "trader", "address", "signer"])
    side_c = pick_col(cols, ["side", "direction", "action"])
    lam_c = pick_col(cols, ["sol_lamports", "lamports", "size_lamports", "amount_lamports", "value_lamports"])
    if not (mint_c and t_c and wallet_c and lam_c):
        raise RuntimeError("Fallback swaps table missing required mint/time/wallet/lamports columns.")

    use_cols = [mint_c, t_c, wallet_c, lam_c] + ([side_c] if side_c else [])
    idx = {c: i for i, c in enumerate(use_cols)}
    raw: List[Tuple[str, int, str, str, int]] = []
    sizes: List[int] = []
    for r in fetch_rows(conn, table, use_cols):
        mint = "" if r[idx[mint_c]] is None else str(r[idx[mint_c]]).strip()
        t = safe_int(r[idx[t_c]])
        wallet = "" if r[idx[wallet_c]] is None else str(r[idx[wallet_c]]).strip()
        lam = safe_int(r[idx[lam_c]])
        if not mint or t is None or not wallet or lam is None:
            continue
        lam = abs(int(lam))
        side = "unknown" if not side_c or r[idx[side_c]] is None else str(r[idx[side_c]]).lower().strip()
        raw.append((mint, int(t), wallet, side, lam))
        sizes.append(lam)
    if not raw:
        raise RuntimeError("Fallback swaps table has no usable rows.")
    whale_thr = int(quantile(sizes, 0.99, default=1.0))
    whale_thr = max(1, whale_thr)
    events = [Event(m, t, w, s, lam, "derived_whale_from_swaps") for (m, t, w, s, lam) in raw if lam >= whale_thr]
    if not events:
        raise RuntimeError("Fallback swaps-derived whale threshold produced zero events.")
    events.sort(key=lambda e: (e.mint, e.t, e.wallet, e.event_type))
    report = {
        "source": f"{table} (fallback)",
        "fallback_reason": "whale_events truly missing; derived whale threshold from swaps lamports P99",
        "derived_whale_lamports_threshold": whale_thr,
    }
    return EventTape(events), report


def build_event_tape(conn: sqlite3.Connection, schema: Dict[str, List[str]], strict: bool, outdir: Path) -> Tuple[EventTape, Dict[str, Any]]:
    if "whale_events" in schema:
        tape, report = build_tape_from_whale_events(conn, schema["whale_events"])
        if tape is not None:
            return tape, {"source": "whale_events", "whale_events_report": report}
        # whale_events exists -> do NOT fallback unless table truly missing
        outdir.mkdir(parents=True, exist_ok=True)
        (outdir / "whale_events_unusable_report.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        raise RuntimeError("whale_events table exists but is unusable; see whale_events_unusable_report.json.")

    tape, r = build_tape_from_swaps(conn, schema)
    return tape, {"source": r["source"], "fallback_report": r}


def indices_by_mint(tape: EventTape) -> Dict[str, List[int]]:
    out: Dict[str, List[int]] = defaultdict(list)
    for i, e in enumerate(tape.events):
        out[e.mint].append(i)
    return out


def kmeans_1d_two_clusters(values: Sequence[float]) -> Tuple[float, float, List[int]]:
    c1 = quantile(values, 0.25)
    c2 = quantile(values, 0.75)
    if c1 == c2:
        c2 = c1 + 1e-6
    labels = [0] * len(values)
    for _ in range(50):
        changed = False
        for i, v in enumerate(values):
            d1 = abs(v - c1)
            d2 = abs(v - c2)
            lb = 0 if d1 <= d2 else 1
            if lb != labels[i]:
                labels[i] = lb
                changed = True
        g1 = [v for v, l in zip(values, labels) if l == 0]
        g2 = [v for v, l in zip(values, labels) if l == 1]
        if not g1 or not g2:
            break
        nc1 = sum(g1) / len(g1)
        nc2 = sum(g2) / len(g2)
        if abs(nc1 - c1) < 1e-10 and abs(nc2 - c2) < 1e-10 and not changed:
            break
        c1, c2 = nc1, nc2
    if c1 <= c2:
        return c1, c2, labels
    return c2, c1, [1 - l for l in labels]


def mode_log_hist(values: Sequence[float], bins: int = 200) -> float:
    lo, hi = min(values), max(values)
    if hi <= lo + 1e-12:
        return lo
    step = (hi - lo) / bins
    counts = [0] * bins
    for v in values:
        i = int((v - lo) / step)
        if i >= bins:
            i = bins - 1
        if i < 0:
            i = 0
        counts[i] += 1
    best_i = max(range(bins), key=lambda i: (counts[i], -i))
    return lo + (best_i + 0.5) * step


def valley_between_modes(values: Sequence[float], left: float, right: float, bins: int = 400) -> float:
    lo, hi = min(values), max(values)
    if hi <= lo + 1e-12:
        return lo
    step = (hi - lo) / bins
    counts = [0] * bins
    for v in values:
        i = int((v - lo) / step)
        if i >= bins:
            i = bins - 1
        if i < 0:
            i = 0
        counts[i] += 1
    li = max(0, min(bins - 1, int((left - lo) / step)))
    ri = max(0, min(bins - 1, int((right - lo) / step)))
    if li > ri:
        li, ri = ri, li
    if li == ri:
        return lo + (li + 0.5) * step
    best = li
    best_count = counts[li]
    for i in range(li, ri + 1):
        if counts[i] < best_count:
            best_count = counts[i]
            best = i
    return lo + (best + 0.5) * step


def mine_silence_thresholds(tape: EventTape, by_mint: Dict[str, List[int]]) -> Tuple[int, int, Dict[str, Any]]:
    gaps: List[int] = []
    for idx in by_mint.values():
        for j in range(1, len(idx)):
            g = tape.events[idx[j]].t - tape.events[idx[j - 1]].t
            if g > 0:
                gaps.append(g)
    if not gaps:
        raise RuntimeError("Insufficient consecutive whale events to mine silence thresholds.")

    lgaps = [math.log(g) for g in gaps]
    active_mean, inactive_mean, labels = kmeans_1d_two_clusters(lgaps)
    spread = (quantile(lgaps, 0.95) - quantile(lgaps, 0.05))
    separation = inactive_mean - active_mean

    if separation > max(0.15, 0.15 * spread):
        valley = valley_between_modes(lgaps, active_mean, inactive_mean)
        ignition = int(round(math.exp(valley)))
        inactive_cluster = [math.exp(v) for v, l in zip(lgaps, labels) if l == 1]
        if inactive_cluster:
            death = int(round(quantile(inactive_cluster, 0.35)))
        else:
            death = int(round(math.exp((valley + inactive_mean) / 2.0)))
        method = "kmeans2_log_gap_valley_plus_inactive_anchor"
    else:
        p90 = quantile(gaps, 0.90)
        active = [math.log(g) for g in gaps if g <= p90] or lgaps
        inactive = [math.log(g) for g in gaps if g > p90] or lgaps
        active_mode = mode_log_hist(active)
        inactive_mode = mode_log_hist(inactive)
        ignition = int(round(math.exp(active_mode)))
        death = int(round(math.exp((active_mode + inactive_mode) / 2.0)))
        method = "hist_mode_active_inactive_fallback"

    ignition = max(1, ignition)
    death = max(2, death)
    if death <= ignition:
        death = ignition + max(1, ignition // 5)

    return ignition, death, {
        "method": method,
        "gap_count": len(gaps),
        "active_mean_log": active_mean,
        "inactive_mean_log": inactive_mean,
        "separation": separation,
        "IGNITION_SILENCE_THRESHOLD": ignition,
        "DEATH_SILENCE_THRESHOLD": death,
    }


def rolling_window_counts(times: List[int], wallets: List[str], window_s: int) -> Tuple[List[int], List[float], List[float], List[int]]:
    n = len(times)
    uniq = [0] * n
    mean_inter = [float(window_s)] * n
    rate = [0.0] * n
    prev_count = [0] * n
    left = 0
    wallet_count: Dict[str, int] = defaultdict(int)
    for i in range(n):
        t = times[i]
        while left <= i and times[left] < t - window_s:
            w = wallets[left]
            wallet_count[w] -= 1
            if wallet_count[w] <= 0:
                del wallet_count[w]
            left += 1
        # count before adding current
        prev_count[i] = i - left
        wallet_count[wallets[i]] += 1
        size = i - left + 1
        uniq[i] = len(wallet_count)
        span = max(1, t - times[left])
        rate[i] = size / span
        if size >= 2:
            diffs = [times[k] - times[k - 1] for k in range(left + 1, i + 1)]
            mean_inter[i] = sum(diffs) / len(diffs)
    return uniq, mean_inter, rate, prev_count


def mine_coordination(tape: EventTape, by_mint: Dict[str, List[int]], gaps: List[int]) -> Tuple[Dict[str, Any], Dict[str, List[Any]], Dict[str, Any]]:
    base_windows = [int(quantile(gaps, q, default=60.0)) for q in (0.10, 0.25, 0.50, 0.75)]
    c_windows = unique_sorted_ints(max(10, min(600, w)) for w in base_windows if w > 0)
    if not c_windows:
        c_windows = [60]

    candidates = []
    all_activation = []
    cache: Dict[int, Tuple[List[int], List[float]]] = {}

    for w in c_windows:
        uniq_all = [0] * tape.n
        inter_all = [float(w)] * tape.n
        for idx in by_mint.values():
            tt = [tape.events[i].t for i in idx]
            ww = [tape.events[i].wallet for i in idx]
            u, mi, _, _ = rolling_window_counts(tt, ww, w)
            for local_i, global_i in enumerate(idx):
                uniq_all[global_i] = u[local_i]
                inter_all[global_i] = mi[local_i]
        cache[w] = (uniq_all, inter_all)
        uniq_thr = unique_sorted_ints(int(quantile(uniq_all, q)) for q in (0.60, 0.70, 0.80, 0.90) if quantile(uniq_all, q) >= 2)
        inter_thr = unique_sorted_ints(max(1, int(quantile(inter_all, q))) for q in (0.10, 0.25, 0.40, 0.50))
        if not uniq_thr:
            uniq_thr = [2]
        if not inter_thr:
            inter_thr = [max(1, w // 2)]
        for uthr in uniq_thr:
            for ithr in inter_thr:
                mask = [(u >= uthr and m <= ithr) for u, m in zip(uniq_all, inter_all)]
                act = sum(mask) / max(1, len(mask))
                all_activation.append(act)
                trans = 0
                hours = 0.0
                for idx in by_mint.values():
                    m = [mask[i] for i in idx]
                    trans += sum(1 for i in range(1, len(m)) if m[i] != m[i - 1])
                    if len(idx) >= 2:
                        hours += max(1, tape.events[idx[-1]].t - tape.events[idx[0]].t) / 3600.0
                flick = trans / hours if hours > 0 else float("inf")
                candidates.append({"W": w, "uthr": uthr, "ithr": ithr, "activation": act, "flicker": flick})

    lo = quantile(all_activation, 0.05, 0.0)
    hi = quantile(all_activation, 0.20, 1.0)

    best = None
    best_key = None
    for c in candidates:
        penalty = 0 if lo <= c["activation"] <= hi else 1
        key = (penalty, c["flicker"], abs(c["activation"] - ((lo + hi) / 2.0)), c["W"], c["uthr"], c["ithr"])
        if best_key is None or key < best_key:
            best_key = key
            best = c

    assert best is not None
    uniq_all, inter_all = cache[best["W"]]
    coord_mask = [(u >= best["uthr"] and m <= best["ithr"]) for u, m in zip(uniq_all, inter_all)]

    return (
        {
            "COORD_WINDOW_SECONDS": int(best["W"]),
            "COORD_UNIQUE_WHALES_THRESHOLD": int(best["uthr"]),
            "COORD_INTERARRIVAL_THRESHOLD": int(best["ithr"]),
        },
        {"coord_unique": uniq_all, "coord_inter": inter_all, "coord_mask": coord_mask},
        {"candidate_windows": c_windows, "activation_band": [lo, hi], "selected": best, "num_candidates": len(candidates)},
    )


def mine_acceleration(tape: EventTape, by_mint: Dict[str, List[int]], coord_w: int, coord_mask: List[bool]) -> Tuple[Dict[str, Any], Dict[str, List[Any]], Dict[str, Any]]:
    short_c = unique_sorted_ints(max(5, min(coord_w, x)) for x in [coord_w // 4, coord_w // 3, coord_w // 2, int(math.sqrt(max(1, coord_w)) * 2)])
    long_c = unique_sorted_ints(max(coord_w + 1, x) for x in [coord_w, int(coord_w * 1.5), coord_w * 2, coord_w * 3])
    long_c = [x for x in long_c if x > min(short_c)] or [max(coord_w + 1, min(short_c) + 1)]

    best = None
    best_key = None
    best_feats = None
    cand = 0

    for ws in short_c:
        for wl in long_c:
            if wl <= ws:
                continue
            rate_s = [0.0] * tape.n
            rate_l = [0.0] * tape.n
            for idx in by_mint.values():
                tt = [tape.events[i].t for i in idx]
                ww = [tape.events[i].wallet for i in idx]
                _, _, rs, _ = rolling_window_counts(tt, ww, ws)
                _, _, rl, _ = rolling_window_counts(tt, ww, wl)
                for k, gi in enumerate(idx):
                    rate_s[gi] = rs[k]
                    rate_l[gi] = rl[k]
            ratio = [rate_s[i] / max(1e-12, rate_l[i]) for i in range(tape.n)]
            thr_c = sorted({quantile(ratio, q) for q in (0.70, 0.80, 0.90, 0.95) if quantile(ratio, q) > 1.0}) or [1.2]
            early_proxy_thr = quantile(rate_l, 0.75)
            for rthr in thr_c:
                cand += 1
                mask = [r >= rthr for r in ratio]
                overlap_coord = sum(1 for i in range(tape.n) if mask[i] and coord_mask[i]) / max(1, tape.n)
                overlap_early = sum(1 for i in range(tape.n) if mask[i] and rate_l[i] >= early_proxy_thr) / max(1, tape.n)
                trans = 0
                hours = 0.0
                for idx in by_mint.values():
                    m = [mask[i] for i in idx]
                    trans += sum(1 for i in range(1, len(m)) if m[i] != m[i - 1])
                    if len(idx) >= 2:
                        hours += max(1, tape.events[idx[-1]].t - tape.events[idx[0]].t) / 3600.0
                flick = trans / hours if hours > 0 else float("inf")
                act = sum(mask) / max(1, tape.n)
                key = (overlap_coord + overlap_early, flick, abs(act - 0.10), ws, wl, rthr)
                if best_key is None or key < best_key:
                    best_key = key
                    best = {"ws": ws, "wl": wl, "rthr": float(rthr), "activation": act, "flicker": flick, "overlap_coord": overlap_coord, "overlap_early": overlap_early}
                    best_feats = (rate_s, rate_l, ratio, mask)

    assert best and best_feats
    rate_s, rate_l, ratio, mask = best_feats
    return (
        {"ACCEL_SHORT_WINDOW": int(best["ws"]), "ACCEL_LONG_WINDOW": int(best["wl"]), "ACCEL_RATIO_THRESHOLD": float(best["rthr"])},
        {"rate_short": rate_s, "rate_long": rate_l, "accel_ratio": ratio, "accel_mask": mask},
        {"short_candidates": short_c, "long_candidates": long_c, "selected": best, "num_candidates": cand},
    )


def mine_distribution(tape: EventTape, by_mint: Dict[str, List[int]], coord_w: int) -> Tuple[Dict[str, Any], Dict[str, List[Any]], Dict[str, Any]]:
    lengths: List[int] = []
    for idx in by_mint.values():
        if not idx:
            continue
        tt = [tape.events[i].t for i in idx]
        st = pv = tt[0]
        for t in tt[1:]:
            if t - pv > coord_w:
                lengths.append(pv - st)
                st = t
            pv = t
        lengths.append(pv - st)
    med_ep = max(60, int(quantile(lengths, 0.50, 60.0)))
    q75_ep = max(60, int(quantile(lengths, 0.75, float(med_ep))))
    cand_w = unique_sorted_ints(max(1, min(120, x // 60)) for x in [coord_w, med_ep, (coord_w + med_ep) // 2, q75_ep])

    sides = [e.side for e in tape.events]
    lamports = [e.sol_lamports for e in tape.events]

    best = None
    best_key = None
    best_feats = None
    num = 0

    for wmin in cand_w:
        W = wmin * 60
        buy = [0] * tape.n
        sell = [0] * tape.n
        for idx in by_mint.values():
            left = 0
            b = s = 0
            tt = [tape.events[i].t for i in idx]
            for j, gi in enumerate(idx):
                t = tt[j]
                while left <= j and tt[left] < t - W:
                    old = idx[left]
                    if sides[old] == "buy":
                        b -= 1
                    if sides[old] == "sell":
                        s -= 1
                    left += 1
                if sides[gi] == "buy":
                    b += 1
                if sides[gi] == "sell":
                    s += 1
                buy[gi] = b
                sell[gi] = s
        total = [buy[i] + sell[i] for i in range(tape.n)]
        sell_ratio = [sell[i] / max(1, total[i]) for i in range(tape.n)]
        net = [buy[i] - sell[i] for i in range(tape.n)]
        sthr_c = sorted({quantile(sell_ratio, q) for q in (0.70, 0.80, 0.90, 0.95)}) or [0.6]
        athr_c = unique_sorted_ints(max(1, int(quantile(total, q))) for q in (0.50, 0.60, 0.70, 0.80, 0.90)) or [1]
        for sthr in sthr_c:
            for athr in athr_c:
                num += 1
                mask = [(total[i] >= athr and sell_ratio[i] >= sthr and net[i] < 0) for i in range(tape.n)]
                meaningful = sum(1 for x in total if x >= athr) / max(1, tape.n)
                act = sum(mask) / max(1, tape.n)
                trans = 0
                hours = 0.0
                for idx in by_mint.values():
                    m = [mask[i] for i in idx]
                    trans += sum(1 for i in range(1, len(m)) if m[i] != m[i - 1])
                    if len(idx) >= 2:
                        hours += max(1, tape.events[idx[-1]].t - tape.events[idx[0]].t) / 3600.0
                flick = trans / hours if hours > 0 else float("inf")
                key = (abs(meaningful - 0.25), abs(act - 0.08), flick, wmin, sthr, athr)
                if best_key is None or key < best_key:
                    best_key = key
                    best = {"wmin": wmin, "sthr": float(sthr), "athr": int(athr), "activation": act, "meaningful": meaningful, "flicker": flick}
                    best_feats = (buy, sell, total, sell_ratio, net, mask)

    assert best and best_feats
    buy, sell, total, sell_ratio, net, mask = best_feats
    return (
        {"NET_FLOW_WINDOW": int(best["wmin"]), "SELL_DOM_THRESHOLD": float(best["sthr"]), "MIN_ACTIVITY_FOR_DISTRIBUTION": int(best["athr"])},
        {"buy_count": buy, "sell_count": sell, "activity_count": total, "sell_ratio": sell_ratio, "net_flow": net, "distribution_mask": mask},
        {"window_candidates_min": cand_w, "selected": best, "num_candidates": num},
    )


def mine_expansion(tape: EventTape, by_mint: Dict[str, List[int]], coord_w: int) -> Tuple[Dict[str, Any], Dict[str, List[Any]], Dict[str, Any]]:
    W = max(30, coord_w)
    growth = [0.0] * tape.n
    diversity = [0.0] * tape.n
    for idx in by_mint.values():
        tt = [tape.events[i].t for i in idx]
        ww = [tape.events[i].wallet for i in idx]
        left = 0
        counts: Dict[str, int] = defaultdict(int)
        for j, gi in enumerate(idx):
            t = tt[j]
            while left <= j and tt[left] < t - W:
                w = ww[left]
                counts[w] -= 1
                if counts[w] <= 0:
                    del counts[w]
                left += 1
            counts[ww[j]] += 1
            uniq = len(counts)
            ev = j - left + 1
            growth[gi] = float(uniq)
            diversity[gi] = float(uniq) / max(1, ev)

    gthr_c = sorted({quantile(growth, q) for q in (0.60, 0.70, 0.80, 0.90) if quantile(growth, q) >= 2.0}) or [2.0]
    dthr_c = sorted({quantile(diversity, q) for q in (0.50, 0.60, 0.70, 0.80, 0.90) if quantile(diversity, q) > 0.0}) or [0.5]

    best = None
    best_key = None
    best_mask = None
    num = 0
    for gt in gthr_c:
        for dt in dthr_c:
            num += 1
            mask = [(growth[i] >= gt and diversity[i] >= dt) for i in range(tape.n)]
            act = sum(mask) / max(1, tape.n)
            trans = 0
            hours = 0.0
            for idx in by_mint.values():
                m = [mask[i] for i in idx]
                trans += sum(1 for i in range(1, len(m)) if m[i] != m[i - 1])
                if len(idx) >= 2:
                    hours += max(1, tape.events[idx[-1]].t - tape.events[idx[0]].t) / 3600.0
            flick = trans / hours if hours > 0 else float("inf")
            key = (abs(act - 0.10), flick, -gt, -dt)
            if best_key is None or key < best_key:
                best_key = key
                best = {"gt": float(gt), "dt": float(dt), "activation": act, "flicker": flick}
                best_mask = mask

    assert best and best_mask is not None
    return (
        {"EXPANSION_GROWTH_THRESHOLD": float(best["gt"]), "DIVERSITY_THRESHOLD": float(best["dt"])},
        {"growth": growth, "diversity": diversity, "expansion_mask": best_mask},
        {"window_seconds": W, "selected": best, "num_candidates": num},
    )


def build_gap_prev_and_prevcount(tape: EventTape, by_mint: Dict[str, List[int]], death_window: int) -> Tuple[List[int], List[int]]:
    gap_prev = [10**9] * tape.n
    prev_count = [0] * tape.n
    for idx in by_mint.values():
        tt = [tape.events[i].t for i in idx]
        _, _, _, prev = rolling_window_counts(tt, [tape.events[i].wallet for i in idx], death_window)
        for j, gi in enumerate(idx):
            if j > 0:
                gap_prev[gi] = tt[j] - tt[j - 1]
            prev_count[gi] = prev[j]
    return gap_prev, prev_count


def assign_states(tape: EventTape, by_mint: Dict[str, List[int]], thr: Dict[str, Any], feats: Dict[str, List[Any]]) -> Tuple[Dict[str, List[bool]], List[str]]:
    death_w = int(thr["DEATH_SILENCE_THRESHOLD"])
    ignition_w = int(thr["IGNITION_SILENCE_THRESHOLD"])
    if death_w <= ignition_w:
        raise RuntimeError("Invariant violated: DEATH_SILENCE_THRESHOLD must be greater than IGNITION_SILENCE_THRESHOLD.")

    gap_prev, prev_count_death = build_gap_prev_and_prevcount(tape, by_mint, death_w)

    coord = feats["coord_mask"]
    accel = feats["accel_mask"]
    distr = feats["distribution_mask"]
    expan = feats["expansion_mask"]
    rate_long = feats["rate_long"]
    early_thr = quantile(rate_long, 0.75, 0.0)

    death = [(gap_prev[i] >= death_w and prev_count_death[i] == 0) for i in range(tape.n)]
    ignition = [(gap_prev[i] >= ignition_w and gap_prev[i] < death_w and prev_count_death[i] == 0) for i in range(tape.n)]
    early = [(rate_long[i] >= early_thr and not coord[i] and not accel[i] and not distr[i] and not expan[i]) for i in range(tape.n)]
    quiet = [
        (not death[i]) and (gap_prev[i] > 0) and (gap_prev[i] < ignition_w) and (not coord[i]) and (not accel[i]) and (not distr[i]) and (not expan[i])
        for i in range(tape.n)
    ]
    base = [True] * tape.n

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

    final = [""] * tape.n
    for s in STATE_ORDER:
        for i in range(tape.n):
            if final[i] == "" and states[s][i]:
                final[i] = s
    for i in range(tape.n):
        if not final[i]:
            final[i] = "TOKEN_BASE_ACTIVITY"
    return states, final


def overlap_counts(states: Dict[str, List[bool]]) -> Dict[Tuple[str, str], int]:
    out: Dict[Tuple[str, str], int] = {}
    for a in STATE_ORDER:
        for b in STATE_ORDER:
            out[(a, b)] = sum(1 for i in range(len(states[a])) if states[a][i] and states[b][i])
    return out


def transition_rows(tape: EventTape, by_mint: Dict[str, List[int]], final: List[str]) -> List[Tuple[str, float, int, int]]:
    rows = []
    for mint, idx in by_mint.items():
        if len(idx) <= 1:
            rows.append((mint, 0.0, 0, len(idx)))
            continue
        seq = [final[i] for i in idx]
        trans = sum(1 for i in range(1, len(seq)) if seq[i] != seq[i - 1])
        dur = max(1, tape.events[idx[-1]].t - tape.events[idx[0]].t)
        tph = trans / (dur / 3600.0)
        rows.append((mint, tph, trans, len(idx)))
    rows.sort(key=lambda r: (-r[1], r[0]))
    return rows


def write_artifacts(outdir: Path, thresholds: Dict[str, Any], tape: EventTape, by_mint: Dict[str, List[int]], states: Dict[str, List[bool]], final: List[str]) -> Dict[str, str]:
    outdir.mkdir(parents=True, exist_ok=True)
    thr_json = json.dumps(thresholds, sort_keys=True, indent=2)
    (outdir / "thresholds.json").write_text(thr_json + "\n", encoding="utf-8")

    ov = overlap_counts(states)
    with (outdir / "state_overlap_matrix.tsv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["state_a", "state_b", "count"])
        for a in STATE_ORDER:
            for b in STATE_ORDER:
                w.writerow([a, b, ov[(a, b)]])

    counts = Counter(final)
    with (outdir / "post_arbitration_state_counts.tsv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["state", "count", "pct"])
        for s in STATE_ORDER:
            c = int(counts.get(s, 0))
            p = c / max(1, len(final))
            w.writerow([s, c, f"{p:.8f}"])

    trans = transition_rows(tape, by_mint, final)
    with (outdir / "transition_rate_report.tsv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["mint", "transitions_per_hour", "transition_count", "event_count"])
        for r in trans:
            w.writerow([r[0], f"{r[1]:.8f}", r[2], r[3]])

    with (outdir / "top_collision_examples.jsonl").open("w", encoding="utf-8") as f:
        count = 0
        for i, e in enumerate(tape.events):
            trues = [s for s in STATE_ORDER if states[s][i]]
            if len(trues) > 1:
                if "TOKEN_DEATH" in trues:
                    activity_states = {"TOKEN_DISTRIBUTION", "TOKEN_EXPANSION", "TOKEN_COORDINATION", "TOKEN_ACCELERATION", "TOKEN_EARLY_TREND", "TOKEN_IGNITION", "TOKEN_BASE_ACTIVITY"}
                    trues = [s for s in trues if not (s == "TOKEN_DEATH" and any(x in activity_states for x in trues))]
                if len(trues) > 1:
                    f.write(json.dumps({"mint": e.mint, "time": e.t, "states_true": trues}, sort_keys=True) + "\n")
                    count += 1
                    if count >= 500:
                        break

    with (outdir / "top_flicker_examples.jsonl").open("w", encoding="utf-8") as f:
        for mint, tph, tc, ec in trans[:50]:
            f.write(json.dumps({"mint": mint, "transitions_per_hour": tph, "transition_count": tc, "event_count": ec}, sort_keys=True) + "\n")

    return {
        "thresholds_json": thr_json,
        "post_counts_tsv": (outdir / "post_arbitration_state_counts.tsv").read_text(encoding="utf-8"),
    }


def run_once(db: Path, outdir: Path, strict: bool) -> Dict[str, str]:
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    try:
        schema = discover_schema(conn)
        if not schema:
            raise RuntimeError("No tables found in SQLite DB.")
        tape, source = build_event_tape(conn, schema, strict, outdir)
        by_mint = indices_by_mint(tape)

        gaps = []
        for idx in by_mint.values():
            for j in range(1, len(idx)):
                g = tape.events[idx[j]].t - tape.events[idx[j - 1]].t
                if g > 0:
                    gaps.append(g)
        if not gaps:
            raise RuntimeError("Cannot mine thresholds: no positive inter-event gaps.")

        ignition_thr, death_thr, silence_report = mine_silence_thresholds(tape, by_mint)

        coord_params, coord_feats, coord_report = mine_coordination(tape, by_mint, gaps)
        accel_params, accel_feats, accel_report = mine_acceleration(tape, by_mint, coord_params["COORD_WINDOW_SECONDS"], coord_feats["coord_mask"])
        dist_params, dist_feats, dist_report = mine_distribution(tape, by_mint, coord_params["COORD_WINDOW_SECONDS"])
        exp_params, exp_feats, exp_report = mine_expansion(tape, by_mint, coord_params["COORD_WINDOW_SECONDS"])

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
        thresholds["IGNITION_SILENCE_THRESHOLD"] = int(ignition_thr)
        thresholds["DEATH_SILENCE_THRESHOLD"] = int(death_thr)

        # enforce invariant strictly
        if thresholds["DEATH_SILENCE_THRESHOLD"] <= thresholds["IGNITION_SILENCE_THRESHOLD"]:
            thresholds["DEATH_SILENCE_THRESHOLD"] = thresholds["IGNITION_SILENCE_THRESHOLD"] + max(1, thresholds["IGNITION_SILENCE_THRESHOLD"] // 5)

        states, final = assign_states(tape, by_mint, thresholds, feats)

        # ensure ignition is reachable where data allows
        ign_count = sum(1 for s in final if s == "TOKEN_IGNITION")
        if ign_count == 0:
            viable = [g for g in gaps if g < thresholds["DEATH_SILENCE_THRESHOLD"]]
            if viable:
                thresholds["IGNITION_SILENCE_THRESHOLD"] = max(1, int(quantile(viable, 0.70)))
                if thresholds["DEATH_SILENCE_THRESHOLD"] <= thresholds["IGNITION_SILENCE_THRESHOLD"]:
                    thresholds["DEATH_SILENCE_THRESHOLD"] = thresholds["IGNITION_SILENCE_THRESHOLD"] + 1
                states, final = assign_states(tape, by_mint, thresholds, feats)

        thresholds["metadata"] = {
            "db_sha256": sha256_file(db),
            "script_sha256": sha256_file(Path(__file__).resolve()),
            "run_timestamp_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "deterministic_seed": 0,
            "event_source": source,
            "reports": {
                "silence": silence_report,
                "coordination": coord_report,
                "acceleration": accel_report,
                "distribution": dist_report,
                "expansion": exp_report,
            },
        }

        (outdir / "mining_report.json").write_text(
            json.dumps({
                "schema_tables": sorted(schema.keys()),
                "source": source,
                "silence": silence_report,
                "coordination": coord_report,
                "acceleration": accel_report,
                "distribution": dist_report,
                "expansion": exp_report,
            }, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )

        return write_artifacts(outdir, thresholds, tape, by_mint, states, final)
    finally:
        conn.close()


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Deterministic Panda token state threshold miner")
    p.add_argument("--db", required=True)
    p.add_argument("--outdir", required=True)
    p.add_argument("--strict", action="store_true")
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    db = Path(args.db).resolve()
    outdir = Path(args.outdir).resolve()
    if not db.exists():
        eprint(f"ERROR: DB not found: {db}")
        return 1
    try:
        run1 = run_once(db, outdir, args.strict)
        run2 = run_once(db, outdir, args.strict)

        t1 = json.loads(run1["thresholds_json"])
        t2 = json.loads(run2["thresholds_json"])
        for t in (t1, t2):
            if "metadata" in t and "run_timestamp_utc" in t["metadata"]:
                t["metadata"]["run_timestamp_utc"] = "<normalized>"
        if json.dumps(t1, sort_keys=True) != json.dumps(t2, sort_keys=True):
            raise RuntimeError("Determinism check failed: thresholds.json content mismatch across two in-process runs")
        if run1["post_counts_tsv"] != run2["post_counts_tsv"]:
            raise RuntimeError("Determinism check failed: post_arbitration_state_counts.tsv mismatch across runs")
        return 0
    except Exception as ex:
        eprint(f"ERROR: {ex}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
