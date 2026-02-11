#!/usr/bin/env python3
"""
Deterministic threshold miner for Panda token state modeling from local SQLite only.

Usage:
  python panda_token_state_threshold_miner.py --db masterwalletsdb.db --outdir exports_thresholds --strict

Constraints:
- stdlib + sqlite3 + numpy (+ optional sklearn for GMM)
- deterministic output for same DB
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

try:
    from sklearn.mixture import GaussianMixture  # type: ignore
    HAVE_SKLEARN = True
except Exception:
    HAVE_SKLEARN = False


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


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        if isinstance(v, (int, np.integer)):
            return int(v)
        if isinstance(v, (float, np.floating)):
            if math.isnan(float(v)) or math.isinf(float(v)):
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


def quantile_int(arr: np.ndarray, q: float, default: int = 0) -> int:
    if arr.size == 0:
        return default
    return int(np.quantile(arr, q))


def median_int(arr: np.ndarray, default: int = 0) -> int:
    if arr.size == 0:
        return default
    return int(np.median(arr))


@dataclass
class EventTape:
    mint: np.ndarray
    t: np.ndarray
    wallet: np.ndarray
    side: np.ndarray
    sol_lamports: np.ndarray
    event_type: np.ndarray

    @property
    def n(self) -> int:
        return len(self.t)


def get_schema(conn: sqlite3.Connection) -> Dict[str, List[str]]:
    schema: Dict[str, List[str]] = {}
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    for (table,) in cur.fetchall():
        if table.startswith("sqlite_"):
            continue
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info('{table}')").fetchall()]
        schema[table] = cols
    return schema


def pick_col(cols: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    low = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand in low:
            return low[cand]
    # substring pass deterministic by sorted columns
    for c in sorted(cols):
        lc = c.lower()
        for cand in candidates:
            if cand in lc:
                return c
    return None


def fetch_rows(conn: sqlite3.Connection, table: str, columns: Sequence[str]) -> Iterable[sqlite3.Row]:
    col_sql = ", ".join([f'"{c}"' for c in columns])
    query = f"SELECT {col_sql} FROM \"{table}\""
    cur = conn.execute(query)
    while True:
        batch = cur.fetchmany(100000)
        if not batch:
            break
        for row in batch:
            yield row


def build_event_tape(conn: sqlite3.Connection, schema: Dict[str, List[str]], strict: bool) -> Tuple[EventTape, Dict[str, Any]]:
    report: Dict[str, Any] = {"source": None, "fallback_reason": None}

    def finalize(rows: List[Tuple[str, int, str, str, int, str]]) -> EventTape:
        if not rows:
            raise RuntimeError("No usable events found after parsing source tables.")
        rows.sort(key=lambda x: (x[0], x[1], x[2], x[5]))
        mints = np.array([r[0] for r in rows], dtype=object)
        ts = np.array([r[1] for r in rows], dtype=np.int64)
        wallets = np.array([r[2] for r in rows], dtype=object)
        sides = np.array([r[3] for r in rows], dtype=object)
        sols = np.array([r[4] for r in rows], dtype=np.int64)
        etypes = np.array([r[5] for r in rows], dtype=object)
        return EventTape(mints, ts, wallets, sides, sols, etypes)

    if "whale_events" in schema:
        cols = schema["whale_events"]
        mint_c = pick_col(cols, ["mint", "token_mint", "token"])
        t_c = pick_col(cols, ["t", "time", "timestamp", "ts", "block_time"])
        wallet_c = pick_col(cols, ["wallet", "owner", "trader", "address"])
        side_c = pick_col(cols, ["side", "direction", "action"])
        lamports_c = pick_col(cols, ["sol_lamports", "lamports", "size_lamports", "amount_lamports"])
        etype_c = pick_col(cols, ["event_type", "type", "event"])

        if mint_c and t_c and wallet_c:
            use_cols = [mint_c, t_c, wallet_c]
            for c in (side_c, lamports_c, etype_c):
                if c:
                    use_cols.append(c)
            rows = []
            idx = {c: i for i, c in enumerate(use_cols)}
            for r in fetch_rows(conn, "whale_events", use_cols):
                mint = str(r[idx[mint_c]]) if r[idx[mint_c]] is not None else ""
                t = safe_int(r[idx[t_c]])
                wallet = str(r[idx[wallet_c]]) if r[idx[wallet_c]] is not None else ""
                if not mint or t is None or not wallet:
                    continue
                side = "unknown"
                if side_c:
                    v = r[idx[side_c]]
                    side = str(v).lower() if v is not None else "unknown"
                lam = 0
                if lamports_c:
                    lv = safe_int(r[idx[lamports_c]])
                    lam = lv if lv is not None else 0
                et = "whale"
                if etype_c:
                    v = r[idx[etype_c]]
                    if v is not None:
                        et = str(v)
                rows.append((mint, int(t), wallet, side, int(lam), et))
            if rows:
                report["source"] = "whale_events"
                return finalize(rows), report
            report["fallback_reason"] = "whale_events present but no usable rows"
        else:
            report["fallback_reason"] = "whale_events present but required columns not found"

    swaps_table = None
    for t in sorted(schema.keys()):
        if "swap" in t.lower():
            swaps_table = t
            break
    if swaps_table is None:
        raise RuntimeError("Neither usable whale_events table nor swaps-like table found.")

    cols = schema[swaps_table]
    mint_c = pick_col(cols, ["mint", "token_mint", "token"])
    t_c = pick_col(cols, ["t", "time", "timestamp", "ts", "block_time"])
    wallet_c = pick_col(cols, ["wallet", "owner", "trader", "address", "signer"])
    side_c = pick_col(cols, ["side", "direction", "action"])
    lamports_c = pick_col(cols, ["sol_lamports", "lamports", "size_lamports", "amount_lamports", "in_lamports", "value_lamports"])
    if not (mint_c and t_c and wallet_c and lamports_c):
        raise RuntimeError(f"Fallback swaps table '{swaps_table}' missing required fields (mint/time/wallet/lamports).")

    use_cols = [mint_c, t_c, wallet_c, lamports_c] + ([side_c] if side_c else [])
    idx = {c: i for i, c in enumerate(use_cols)}

    lamports = []
    cache_rows = []
    for r in fetch_rows(conn, swaps_table, use_cols):
        mint = str(r[idx[mint_c]]) if r[idx[mint_c]] is not None else ""
        t = safe_int(r[idx[t_c]])
        wallet = str(r[idx[wallet_c]]) if r[idx[wallet_c]] is not None else ""
        lam = safe_int(r[idx[lamports_c]])
        if not mint or t is None or not wallet or lam is None:
            continue
        side = "unknown"
        if side_c:
            side = str(r[idx[side_c]]).lower() if r[idx[side_c]] is not None else "unknown"
        lam = abs(int(lam))
        lamports.append(lam)
        cache_rows.append((mint, int(t), wallet, side, lam))

    if not lamports:
        raise RuntimeError("No usable swap rows to derive whale events.")
    larr = np.array(lamports, dtype=np.int64)
    whale_thr = int(np.quantile(larr, 0.99))
    whale_thr = max(1, whale_thr)
    rows = []
    for mint, t, wallet, side, lam in cache_rows:
        if lam >= whale_thr:
            rows.append((mint, t, wallet, side, lam, "derived_whale_from_swaps"))

    if not rows:
        raise RuntimeError("Derived whale threshold produced zero whale events.")

    report["source"] = f"{swaps_table} (fallback)"
    report["fallback_reason"] = "whale_events absent/unusable; whale per-tx threshold mined from swaps lamports P99"
    report["derived_whale_lamports_threshold"] = whale_thr
    return finalize(rows), report


def split_by_mint(tape: EventTape) -> Dict[str, np.ndarray]:
    out: Dict[str, List[int]] = defaultdict(list)
    for i, m in enumerate(tape.mint.tolist()):
        out[m].append(i)
    return {m: np.array(idx, dtype=np.int64) for m, idx in out.items()}


def mine_silence_thresholds(tape: EventTape, mint_ix: Dict[str, np.ndarray]) -> Tuple[int, Dict[str, Any]]:
    gaps = []
    for _, idx in mint_ix.items():
        tt = tape.t[idx]
        if tt.size >= 2:
            d = np.diff(tt)
            d = d[d > 0]
            if d.size:
                gaps.append(d)
    if not gaps:
        raise RuntimeError("Insufficient consecutive events to mine silence thresholds.")
    gap = np.concatenate(gaps).astype(np.int64)
    lgap = np.log(gap.astype(np.float64))

    details: Dict[str, Any] = {"method": None, "fallback": False}
    threshold = None

    if HAVE_SKLEARN and lgap.size >= 20:
        try:
            x = lgap.reshape(-1, 1)
            gmm = GaussianMixture(n_components=2, covariance_type="full", random_state=0, init_params="kmeans", max_iter=300)
            gmm.fit(x)
            means = gmm.means_.flatten()
            order = np.argsort(means)
            m1, m2 = means[order[0]], means[order[1]]
            lo, hi = float(m1), float(m2)
            if hi - lo > 1e-9:
                grid = np.linspace(lo, hi, 2000)
                dens = np.exp(gmm.score_samples(grid.reshape(-1, 1)))
                xstar = float(grid[int(np.argmin(dens))])
                threshold = int(round(math.exp(xstar)))
                details["method"] = "gmm_density_valley"
                details["gmm_means_log"] = [float(m1), float(m2)]
        except Exception as ex:
            details["gmm_error"] = str(ex)

    if threshold is None:
        # deterministic histogram valley fallback
        p90 = np.quantile(gap, 0.90)
        active = gap[gap <= p90]
        if active.size == 0:
            active = gap
        threshold = int(np.quantile(active, 0.995))
        details["method"] = "active_regime_p99_5_of_gaps_le_p90"
        details["fallback"] = True

    threshold = max(1, int(threshold))
    return threshold, details


def rolling_features_for_window(tt: np.ndarray, wallets: np.ndarray, W: int) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    n = tt.size
    uniq = np.zeros(n, dtype=np.int64)
    mean_inter = np.zeros(n, dtype=np.float64)
    rate = np.zeros(n, dtype=np.float64)
    l = 0
    counts: Dict[str, int] = defaultdict(int)
    for i in range(n):
        t_now = int(tt[i])
        while l <= i and int(tt[l]) < t_now - W:
            w = wallets[l]
            counts[w] -= 1
            if counts[w] <= 0:
                del counts[w]
            l += 1
        # include current
        counts[wallets[i]] += 1
        size = i - l + 1
        uniq[i] = len(counts)
        span = max(1, int(tt[i]) - int(tt[l]))
        rate[i] = size / span
        if size >= 2:
            segment = tt[l : i + 1]
            dif = np.diff(segment)
            mean_inter[i] = float(np.mean(dif)) if dif.size else float(W)
        else:
            mean_inter[i] = float(W)
    return uniq, mean_inter, rate


def aggregate_candidate_metrics(cand: Dict[str, Any]) -> Dict[str, float]:
    act = cand["activation"]
    flick = cand["flicker"]
    coll = cand["collisions"]
    return {
        "activation": float(act),
        "flicker": float(flick),
        "collisions": float(coll),
        "score": float(coll + flick),
    }


def mine_coordination(tape: EventTape, mint_ix: Dict[str, np.ndarray], death_silence: int) -> Tuple[Dict[str, Any], Dict[str, np.ndarray], Dict[str, Any]]:
    # candidate windows from gap quantiles
    gaps = []
    for idx in mint_ix.values():
        tt = tape.t[idx]
        if tt.size >= 2:
            d = np.diff(tt)
            d = d[d > 0]
            if d.size:
                gaps.append(d)
    g = np.concatenate(gaps) if gaps else np.array([60], dtype=np.int64)
    raw_windows = [quantile_int(g, q, 60) for q in (0.10, 0.25, 0.50, 0.75)]
    c_windows = sorted({int(clamp(w, 10, 600)) for w in raw_windows if w > 0})
    if not c_windows:
        c_windows = [60]

    perW_features: Dict[int, Dict[str, np.ndarray]] = {}
    cand_summaries = []
    rates = []

    for W in c_windows:
        uniq_all = np.zeros(tape.n, dtype=np.int64)
        inter_all = np.zeros(tape.n, dtype=np.float64)
        for _, idx in mint_ix.items():
            u, mi, _ = rolling_features_for_window(tape.t[idx], tape.wallet[idx], W)
            uniq_all[idx] = u
            inter_all[idx] = mi

        uniq_thr_candidates = sorted(set(int(x) for x in np.quantile(uniq_all, [0.60, 0.70, 0.80, 0.90]) if x >= 2))
        if not uniq_thr_candidates:
            uniq_thr_candidates = [2]

        inter_thr_candidates = sorted(set(int(max(1, x)) for x in np.quantile(inter_all, [0.10, 0.25, 0.40, 0.50])))
        if not inter_thr_candidates:
            inter_thr_candidates = [max(1, W // 2)]

        for uthr in uniq_thr_candidates:
            for ithr in inter_thr_candidates:
                mask = (uniq_all >= uthr) & (inter_all <= ithr)
                act = float(np.mean(mask))
                rates.append(act)
                # flicker per hour aggregated
                transitions = 0
                hours = 0.0
                for idx in mint_ix.values():
                    m = mask[idx]
                    if m.size <= 1:
                        continue
                    transitions += int(np.sum(m[1:] != m[:-1]))
                    dur = max(1, int(tape.t[idx][-1] - tape.t[idx][0]))
                    hours += dur / 3600.0
                flick = (transitions / hours) if hours > 0 else float("inf")
                collisions = float(np.mean(mask & (np.diff(np.concatenate(([0], tape.t))) > death_silence)))
                cand_summaries.append({
                    "W": W,
                    "uthr": uthr,
                    "ithr": ithr,
                    "activation": act,
                    "flicker": flick,
                    "collisions": collisions,
                })

        perW_features[W] = {"unique": uniq_all, "mean_inter": inter_all}

    rate_arr = np.array(rates if rates else [0.0], dtype=np.float64)
    lo_band, hi_band = np.quantile(rate_arr, 0.05), np.quantile(rate_arr, 0.20)

    best = None
    best_key = None
    for c in cand_summaries:
        in_band = lo_band <= c["activation"] <= hi_band
        penalty = 0.0 if in_band else 1.0
        key = (
            penalty,
            c["collisions"],
            c["flicker"],
            -c["activation"],
            c["W"],
            c["uthr"],
            c["ithr"],
        )
        if best_key is None or key < best_key:
            best_key = key
            best = c

    assert best is not None
    W = int(best["W"])
    uthr = int(best["uthr"])
    ithr = int(best["ithr"])
    feats = perW_features[W]
    coord_mask = (feats["unique"] >= uthr) & (feats["mean_inter"] <= ithr)

    params = {
        "COORD_WINDOW_SECONDS": W,
        "COORD_UNIQUE_WHALES_THRESHOLD": uthr,
        "COORD_INTERARRIVAL_THRESHOLD": ithr,
    }
    report = {
        "candidate_windows": c_windows,
        "activation_band": [float(lo_band), float(hi_band)],
        "selected": best,
        "num_candidates": len(cand_summaries),
    }
    features = {
        "coord_unique": feats["unique"],
        "coord_inter": feats["mean_inter"],
        "coord_mask": coord_mask,
    }
    return params, features, report


def mine_acceleration(tape: EventTape, mint_ix: Dict[str, np.ndarray], coord_params: Dict[str, Any], coord_mask: np.ndarray) -> Tuple[Dict[str, Any], Dict[str, np.ndarray], Dict[str, Any]]:
    W = int(coord_params["COORD_WINDOW_SECONDS"])
    short_c = sorted({int(clamp(x, 5, max(10, W))) for x in [max(5, W // 4), max(5, W // 3), max(5, W // 2), int(np.sqrt(max(1, W)) * 2)]})
    long_c = sorted({int(clamp(x, W + 1, max(1200, W * 4))) for x in [W, int(W * 1.5), W * 2, W * 3] if int(x) > 0})
    long_c = [x for x in long_c if x > min(short_c)] or [max(W + 1, min(short_c) + 1)]

    best = None
    best_key = None
    best_feats = None
    cand_count = 0

    for ws in short_c:
        for wl in long_c:
            if wl <= ws:
                continue
            rate_s = np.zeros(tape.n, dtype=np.float64)
            rate_l = np.zeros(tape.n, dtype=np.float64)
            for idx in mint_ix.values():
                _, _, rs = rolling_features_for_window(tape.t[idx], tape.wallet[idx], ws)
                _, _, rl = rolling_features_for_window(tape.t[idx], tape.wallet[idx], wl)
                rate_s[idx] = rs
                rate_l[idx] = rl
            ratio = np.divide(rate_s, np.maximum(rate_l, 1e-12))
            thr_candidates = sorted(set(float(x) for x in np.quantile(ratio, [0.70, 0.80, 0.90, 0.95]) if np.isfinite(x) and x > 1.0))
            if not thr_candidates:
                thr_candidates = [1.2]
            for rthr in thr_candidates:
                cand_count += 1
                mask = ratio >= rthr
                overlap = float(np.mean(mask & coord_mask))
                # proxy early trend overlap: high long rate without accel
                early_proxy = rate_l >= np.quantile(rate_l, 0.75)
                overlap2 = float(np.mean(mask & early_proxy))
                transitions = 0
                hours = 0.0
                for idx in mint_ix.values():
                    m = mask[idx]
                    if m.size <= 1:
                        continue
                    transitions += int(np.sum(m[1:] != m[:-1]))
                    dur = max(1, int(tape.t[idx][-1] - tape.t[idx][0]))
                    hours += dur / 3600.0
                flick = transitions / hours if hours > 0 else float("inf")
                act = float(np.mean(mask))
                key = (overlap + overlap2, flick, abs(act - 0.10), ws, wl, rthr)
                if best_key is None or key < best_key:
                    best_key = key
                    best = {"ws": ws, "wl": wl, "rthr": float(rthr), "overlap": overlap, "overlap2": overlap2, "flicker": flick, "activation": act}
                    best_feats = (rate_s, rate_l, ratio, mask)

    assert best is not None and best_feats is not None
    rate_s, rate_l, ratio, mask = best_feats
    params = {
        "ACCEL_SHORT_WINDOW": int(best["ws"]),
        "ACCEL_LONG_WINDOW": int(best["wl"]),
        "ACCEL_RATIO_THRESHOLD": float(best["rthr"]),
    }
    features = {"rate_short": rate_s, "rate_long": rate_l, "accel_ratio": ratio, "accel_mask": mask}
    report = {"short_candidates": short_c, "long_candidates": long_c, "selected": best, "num_candidates": cand_count}
    return params, features, report


def mine_distribution(tape: EventTape, mint_ix: Dict[str, np.ndarray], coord_W: int) -> Tuple[Dict[str, Any], Dict[str, np.ndarray], Dict[str, Any]]:
    # median episode length by gaps <= coord_W
    lengths = []
    for idx in mint_ix.values():
        tt = tape.t[idx]
        if tt.size == 0:
            continue
        start = int(tt[0])
        prev = int(tt[0])
        for t in tt[1:]:
            ti = int(t)
            if ti - prev > coord_W:
                lengths.append(prev - start)
                start = ti
            prev = ti
        lengths.append(prev - start)
    med_ep = max(60, median_int(np.array(lengths, dtype=np.int64)) if lengths else 60)
    net_candidates = sorted({int(clamp(x // 60, 1, 120)) for x in [coord_W, med_ep, int((coord_W + med_ep) / 2), int(np.quantile(np.array(lengths if lengths else [med_ep]), 0.75))]})
    if not net_candidates:
        net_candidates = [max(1, coord_W // 60)]

    best = None
    best_key = None
    best_feats = None
    count = 0

    is_buy = np.array([1 if str(s).lower() == "buy" else 0 for s in tape.side], dtype=np.int64)
    is_sell = np.array([1 if str(s).lower() == "sell" else 0 for s in tape.side], dtype=np.int64)

    for wmin in net_candidates:
        W = wmin * 60
        buy_c = np.zeros(tape.n, dtype=np.int64)
        sell_c = np.zeros(tape.n, dtype=np.int64)
        buy_l = np.zeros(tape.n, dtype=np.int64)
        sell_l = np.zeros(tape.n, dtype=np.int64)
        for idx in mint_ix.values():
            tt = tape.t[idx]
            l = 0
            bcnt = 0
            scnt = 0
            bl = 0
            sl = 0
            for j, gi in enumerate(idx):
                tnow = int(tt[j])
                while l <= j and int(tt[l]) < tnow - W:
                    gj = idx[l]
                    if is_buy[gj]:
                        bcnt -= 1
                        bl -= int(tape.sol_lamports[gj])
                    if is_sell[gj]:
                        scnt -= 1
                        sl -= int(tape.sol_lamports[gj])
                    l += 1
                if is_buy[gi]:
                    bcnt += 1
                    bl += int(tape.sol_lamports[gi])
                if is_sell[gi]:
                    scnt += 1
                    sl += int(tape.sol_lamports[gi])
                buy_c[gi], sell_c[gi], buy_l[gi], sell_l[gi] = bcnt, scnt, bl, sl

        total_c = buy_c + sell_c
        sell_ratio = np.divide(sell_c, np.maximum(total_c, 1))
        net_flow = buy_c - sell_c
        sell_thr_c = sorted(set(float(x) for x in np.quantile(sell_ratio, [0.70, 0.80, 0.90, 0.95]) if np.isfinite(x)))
        min_act_c = sorted(set(int(x) for x in np.quantile(total_c, [0.50, 0.60, 0.70, 0.80, 0.90]) if x >= 1))
        if not sell_thr_c:
            sell_thr_c = [0.6]
        if not min_act_c:
            min_act_c = [1]
        for sthr in sell_thr_c:
            for mthr in min_act_c:
                count += 1
                mask = (total_c >= mthr) & (sell_ratio >= sthr) & (net_flow < 0)
                meaningful = float(np.mean(total_c >= mthr))
                act = float(np.mean(mask))
                transitions = 0
                hours = 0.0
                for idx in mint_ix.values():
                    m = mask[idx]
                    if m.size <= 1:
                        continue
                    transitions += int(np.sum(m[1:] != m[:-1]))
                    dur = max(1, int(tape.t[idx][-1] - tape.t[idx][0]))
                    hours += dur / 3600.0
                flick = transitions / hours if hours > 0 else float("inf")
                key = (abs(meaningful - 0.25), abs(act - 0.08), flick, wmin, sthr, mthr)
                if best_key is None or key < best_key:
                    best_key = key
                    best = {"wmin": wmin, "sthr": float(sthr), "mthr": int(mthr), "activation": act, "meaningful": meaningful, "flicker": flick}
                    best_feats = (buy_c, sell_c, net_flow, sell_ratio, total_c, mask)

    assert best is not None and best_feats is not None
    buy_c, sell_c, net_flow, sell_ratio, total_c, mask = best_feats
    params = {
        "NET_FLOW_WINDOW": int(best["wmin"]),
        "SELL_DOM_THRESHOLD": float(best["sthr"]),
        "MIN_ACTIVITY_FOR_DISTRIBUTION": int(best["mthr"]),
    }
    features = {
        "buy_count": buy_c,
        "sell_count": sell_c,
        "net_flow": net_flow,
        "sell_ratio": sell_ratio,
        "activity_count": total_c,
        "distribution_mask": mask,
    }
    report = {"window_candidates_min": net_candidates, "selected": best, "num_candidates": count}
    return params, features, report


def mine_expansion(tape: EventTape, mint_ix: Dict[str, np.ndarray], coord_W: int) -> Tuple[Dict[str, Any], Dict[str, np.ndarray], Dict[str, Any]]:
    W = max(30, coord_W)
    growth = np.zeros(tape.n, dtype=np.float64)
    diversity = np.zeros(tape.n, dtype=np.float64)
    for idx in mint_ix.values():
        tt = tape.t[idx]
        w = tape.wallet[idx]
        l = 0
        freq: Dict[str, int] = defaultdict(int)
        for j, gi in enumerate(idx):
            tnow = int(tt[j])
            while l <= j and int(tt[l]) < tnow - W:
                ww = w[l]
                freq[ww] -= 1
                if freq[ww] <= 0:
                    del freq[ww]
                l += 1
            freq[w[j]] += 1
            uniq = len(freq)
            events = j - l + 1
            growth[gi] = float(uniq)
            diversity[gi] = float(uniq) / float(max(1, events))

    gthr_c = sorted(set(float(x) for x in np.quantile(growth, [0.60, 0.70, 0.80, 0.90]) if x >= 2))
    dthr_c = sorted(set(float(x) for x in np.quantile(diversity, [0.50, 0.60, 0.70, 0.80, 0.90]) if x > 0))
    if not gthr_c:
        gthr_c = [2.0]
    if not dthr_c:
        dthr_c = [0.5]

    best = None
    best_key = None
    count = 0
    best_mask = None
    for gt in gthr_c:
        for dt in dthr_c:
            count += 1
            mask = (growth >= gt) & (diversity >= dt)
            act = float(np.mean(mask))
            transitions = 0
            hours = 0.0
            for idx in mint_ix.values():
                m = mask[idx]
                if m.size <= 1:
                    continue
                transitions += int(np.sum(m[1:] != m[:-1]))
                dur = max(1, int(tape.t[idx][-1] - tape.t[idx][0]))
                hours += dur / 3600.0
            flick = transitions / hours if hours > 0 else float("inf")
            key = (abs(act - 0.10), flick, -gt, -dt)
            if best_key is None or key < best_key:
                best_key = key
                best = {"gt": float(gt), "dt": float(dt), "activation": act, "flicker": flick}
                best_mask = mask

    assert best is not None and best_mask is not None
    params = {
        "EXPANSION_GROWTH_THRESHOLD": float(best["gt"]),
        "DIVERSITY_THRESHOLD": float(best["dt"]),
    }
    features = {"growth": growth, "diversity": diversity, "expansion_mask": best_mask}
    report = {"selected": best, "num_candidates": count, "window_seconds": W}
    return params, features, report


def assign_states(tape: EventTape, mint_ix: Dict[str, np.ndarray], thresholds: Dict[str, Any], feats: Dict[str, np.ndarray], death_silence: int, ignition_silence: int) -> Tuple[Dict[str, np.ndarray], np.ndarray, Dict[str, int]]:
    n = tape.n
    gap_prev = np.zeros(n, dtype=np.int64)
    for idx in mint_ix.values():
        if idx.size == 0:
            continue
        tt = tape.t[idx]
        g = np.diff(tt, prepend=tt[0])
        g[0] = 10 ** 9
        gap_prev[idx] = g

    coord = feats["coord_mask"]
    accel = feats["accel_mask"]
    distr = feats["distribution_mask"]
    expan = feats["expansion_mask"]

    death = gap_prev >= int(death_silence)
    ignition = (gap_prev >= int(ignition_silence)) & (~death)
    quiet = (~death) & (gap_prev > 0) & (gap_prev < int(ignition_silence)) & (~coord) & (~accel) & (~distr) & (~expan)

    rate_long = feats["rate_long"]
    early_thr = np.quantile(rate_long, 0.75) if rate_long.size else 0.0
    early = (rate_long >= early_thr) & (~coord) & (~accel) & (~distr) & (~expan)
    base = np.ones(n, dtype=bool)

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

    final = np.array(["" for _ in range(n)], dtype=object)
    for s in STATE_ORDER:
        mask = states[s] & (final == "")
        final[mask] = s
    final[final == ""] = "TOKEN_BASE_ACTIVITY"

    counts = {s: int(np.sum(final == s)) for s in STATE_ORDER}
    return states, final, counts


def overlap_matrix(states: Dict[str, np.ndarray]) -> Dict[Tuple[str, str], int]:
    out = {}
    for a in STATE_ORDER:
        for b in STATE_ORDER:
            out[(a, b)] = int(np.sum(states[a] & states[b]))
    return out


def transition_report(tape: EventTape, mint_ix: Dict[str, np.ndarray], final: np.ndarray) -> List[Tuple[str, float, int, int]]:
    rows = []
    for mint, idx in mint_ix.items():
        if idx.size <= 1:
            rows.append((mint, 0.0, 0, 1 if idx.size else 0))
            continue
        s = final[idx]
        trans = int(np.sum(s[1:] != s[:-1]))
        dur = max(1, int(tape.t[idx][-1] - tape.t[idx][0]))
        tph = trans / (dur / 3600.0)
        rows.append((mint, float(tph), trans, int(idx.size)))
    rows.sort(key=lambda x: (-x[1], x[0]))
    return rows


def write_outputs(outdir: Path, thresholds: Dict[str, Any], states: Dict[str, np.ndarray], final: np.ndarray, tape: EventTape, mint_ix: Dict[str, np.ndarray]) -> Dict[str, str]:
    outdir.mkdir(parents=True, exist_ok=True)

    thr_path = outdir / "thresholds.json"
    thr_json = json.dumps(thresholds, sort_keys=True, indent=2)
    thr_path.write_text(thr_json + "\n", encoding="utf-8")

    ov = overlap_matrix(states)
    ov_path = outdir / "state_overlap_matrix.tsv"
    with ov_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["state_a", "state_b", "count"])
        for a in STATE_ORDER:
            for b in STATE_ORDER:
                w.writerow([a, b, ov[(a, b)]])

    counts = Counter(final.tolist())
    ct_path = outdir / "post_arbitration_state_counts.tsv"
    with ct_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["state", "count", "pct"])
        n = len(final)
        for s in STATE_ORDER:
            c = int(counts.get(s, 0))
            p = (c / n) if n else 0.0
            w.writerow([s, c, f"{p:.8f}"])

    tr_rows = transition_report(tape, mint_ix, final)
    tr_path = outdir / "transition_rate_report.tsv"
    with tr_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["mint", "transitions_per_hour", "transition_count", "event_count"])
        for r in tr_rows:
            w.writerow([r[0], f"{r[1]:.8f}", r[2], r[3]])

    coll_path = outdir / "top_collision_examples.jsonl"
    with coll_path.open("w", encoding="utf-8") as f:
        multi = 0
        for i in range(tape.n):
            trues = [s for s in STATE_ORDER if states[s][i]]
            if len(trues) > 1:
                rec = {"mint": str(tape.mint[i]), "time": int(tape.t[i]), "states_true": trues}
                f.write(json.dumps(rec, sort_keys=True) + "\n")
                multi += 1
                if multi >= 500:
                    break

    flick_path = outdir / "top_flicker_examples.jsonl"
    with flick_path.open("w", encoding="utf-8") as f:
        for mint, tph, trans, ev in tr_rows[:50]:
            rec = {"mint": mint, "transitions_per_hour": tph, "transition_count": trans, "event_count": ev}
            f.write(json.dumps(rec, sort_keys=True) + "\n")

    return {
        "thresholds_json": thr_json,
        "post_counts_tsv": ct_path.read_text(encoding="utf-8"),
    }


def run_once(db_path: Path, outdir: Path, strict: bool) -> Dict[str, str]:
    if not db_path.exists():
        raise RuntimeError(f"DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        schema = get_schema(conn)
        if not schema:
            raise RuntimeError("No tables found in SQLite DB.")

        tape, source_report = build_event_tape(conn, schema, strict)
        mint_ix = split_by_mint(tape)

        death_thr, silence_report = mine_silence_thresholds(tape, mint_ix)
        ignition_thr = max(1, int(np.quantile(np.array([death_thr], dtype=np.int64), 0.5)))

        coord_params, coord_feats, coord_report = mine_coordination(tape, mint_ix, death_thr)
        accel_params, accel_feats, accel_report = mine_acceleration(tape, mint_ix, coord_params, coord_feats["coord_mask"])
        distr_params, distr_feats, distr_report = mine_distribution(tape, mint_ix, int(coord_params["COORD_WINDOW_SECONDS"]))
        exp_params, exp_feats, exp_report = mine_expansion(tape, mint_ix, int(coord_params["COORD_WINDOW_SECONDS"]))

        all_feats = {}
        all_feats.update(coord_feats)
        all_feats.update(accel_feats)
        all_feats.update(distr_feats)
        all_feats.update(exp_feats)

        thresholds: Dict[str, Any] = {}
        thresholds.update(coord_params)
        thresholds.update(accel_params)
        thresholds.update(distr_params)
        thresholds.update(exp_params)
        thresholds["DEATH_SILENCE_THRESHOLD"] = int(death_thr)
        thresholds["IGNITION_SILENCE_THRESHOLD"] = int(ignition_thr)

        thresholds["metadata"] = {
            "db_sha256": sha256_file(db_path),
            "script_sha256": sha256_file(Path(__file__).resolve()),
            "run_timestamp_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "deterministic_seed": 0,
            "event_source": source_report,
            "reports": {
                "silence": silence_report,
                "coordination": coord_report,
                "acceleration": accel_report,
                "distribution": distr_report,
                "expansion": exp_report,
            },
        }

        states, final, _ = assign_states(tape, mint_ix, thresholds, all_feats, death_thr, ignition_thr)

        # fallback/source report artifact
        rep_path = outdir / "mining_report.json"
        outdir.mkdir(parents=True, exist_ok=True)
        rep_path.write_text(json.dumps({
            "schema_tables": sorted(schema.keys()),
            "source_report": source_report,
            "silence_report": silence_report,
            "coordination_report": coord_report,
            "acceleration_report": accel_report,
            "distribution_report": distr_report,
            "expansion_report": exp_report,
        }, sort_keys=True, indent=2) + "\n", encoding="utf-8")

        return write_outputs(outdir, thresholds, states, final, tape, mint_ix)
    finally:
        conn.close()


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Deterministic Panda token state threshold miner")
    p.add_argument("--db", required=True, help="Path to SQLite DB (local only)")
    p.add_argument("--outdir", required=True, help="Output directory for artifacts")
    p.add_argument("--strict", action="store_true", help="Enable strict behavior and fail on uncertainty")
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    db_path = Path(args.db).resolve()
    outdir = Path(args.outdir).resolve()

    try:
        first = run_once(db_path, outdir, args.strict)
        second = run_once(db_path, outdir, args.strict)

        j1 = json.loads(first["thresholds_json"])
        j2 = json.loads(second["thresholds_json"])
        # Allow run timestamp to differ; compare everything else.
        for j in (j1, j2):
            if "metadata" in j and "run_timestamp_utc" in j["metadata"]:
                j["metadata"]["run_timestamp_utc"] = "<normalized>"
        if json.dumps(j1, sort_keys=True) != json.dumps(j2, sort_keys=True):
            raise RuntimeError("Determinism check failed: thresholds content mismatch between two in-process runs.")

        if first["post_counts_tsv"] != second["post_counts_tsv"]:
            raise RuntimeError("Determinism check failed: post_arbitration_state_counts.tsv mismatch between runs.")

        return 0
    except Exception as ex:
        eprint(f"ERROR: {ex}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
