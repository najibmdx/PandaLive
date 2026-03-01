#!/usr/bin/env python3
"""MODE B cross-wallet reverse engineering over a local SQLite database."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MODE B cross-wallet reverse engineering")
    parser.add_argument("--db", default="masterwalletsdb.db", help="Path to SQLite DB")
    parser.add_argument("--seeds", required=True, help="Path to seeds.txt (one wallet per line)")
    parser.add_argument("--outdir", default="out_network", help="Output directory")
    parser.add_argument("--topk", type=int, default=50, help="Top neighbors per seed")
    parser.add_argument("--episode-gap", type=int, default=90, help="Episode gap threshold in seconds")
    return parser.parse_args()


def load_seeds(path: Path) -> List[str]:
    seeds: List[str] = []
    seen = set()
    with path.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line not in seen:
                seen.add(line)
                seeds.append(line)
    return seeds


def get_table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]


def looks_like_endpoint(name: str) -> bool:
    n = name.lower()
    return (
        "wallet" in n
        or n.endswith("_wallet")
        or "src" in n
        or "dst" in n
        or "from" in n
        or "to" in n
        or n.endswith("_a")
        or n.endswith("_b")
        or n in {"a", "b"}
    )


def looks_like_weight(name: str) -> bool:
    n = name.lower()
    return any(k in n for k in ("weight", "count", "freq", "score")) or n in {"n"}


def choose_edge_columns(columns: Sequence[str]) -> Tuple[Optional[str], Optional[str], Optional[str], str]:
    lc_to_orig = {c.lower(): c for c in columns}
    preferred_pairs = [
        ("wallet_a", "wallet_b"),
        ("src_wallet", "dst_wallet"),
        ("from_wallet", "to_wallet"),
        ("source_wallet", "target_wallet"),
    ]
    for a, b in preferred_pairs:
        if a in lc_to_orig and b in lc_to_orig:
            wcol = choose_weight_column(columns)
            return lc_to_orig[a], lc_to_orig[b], wcol, "preferred_pair"

    candidates = [c for c in columns if looks_like_endpoint(c)]
    if len(candidates) < 2:
        if len(columns) >= 2:
            candidates = list(columns[:2])
        else:
            return None, None, None, "missing"

    wallets = sorted(candidates, key=lambda c: (0 if "wallet" in c.lower() else 1, c.lower()))[:2]
    wcol = choose_weight_column(columns)
    return wallets[0], wallets[1], wcol, "heuristic"


def choose_weight_column(columns: Sequence[str]) -> Optional[str]:
    lc_to_orig = {c.lower(): c for c in columns}
    for key in ["weight", "edge_weight", "count", "edge_count", "n", "freq", "score"]:
        if key in lc_to_orig:
            return lc_to_orig[key]
    cands = [c for c in columns if looks_like_weight(c)]
    if not cands:
        return None
    cands.sort(key=lambda c: (0 if "weight" in c.lower() else 1, c.lower()))
    return cands[0]


def placeholders(n: int) -> str:
    return ",".join("?" for _ in range(n))


def percentile_nearest_rank(values: Sequence[float], pct: float) -> float:
    if not values:
        return 0.0
    vals = sorted(values)
    rank = int(math.ceil(pct * len(vals)))
    idx = max(0, min(len(vals) - 1, rank - 1))
    return float(vals[idx])


def safe_median(values: Sequence[float]) -> float:
    return float(median(values)) if values else 0.0


def safe_ratio(a: float, b: float) -> float:
    return float(a / b) if b else 0.0


def escape_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def expand_neighbors(
    conn: sqlite3.Connection,
    seeds: Sequence[str],
    edges_table: str,
    col_a: str,
    col_b: str,
    weight_col: Optional[str],
    topk: int,
) -> Tuple[Dict[str, List[str]], int]:
    seed_neighbors: Dict[str, List[str]] = {}

    qa, qb = escape_ident(col_a), escape_ident(col_b)
    if weight_col:
        qw = escape_ident(weight_col)
        query = (
            f"SELECT {qa}, {qb}, COALESCE({qw}, 0) AS w FROM {escape_ident(edges_table)} "
            f"WHERE {qa}=? OR {qb}=?"
        )
    else:
        query = (
            f"SELECT {qa}, {qb}, 1 AS w FROM {escape_ident(edges_table)} "
            f"WHERE {qa}=? OR {qb}=?"
        )

    total_rows = 0
    for seed in sorted(seeds):
        neigh_weights = Counter()
        rows = conn.execute(query, (seed, seed)).fetchall()
        total_rows += len(rows)
        for a, b, w in rows:
            if not a or not b:
                continue
            a = str(a)
            b = str(b)
            weight = float(w if w is not None else 0)
            other = b if a == seed else a
            neigh_weights[other] += weight if weight_col else 1.0
        ranked = sorted(neigh_weights.items(), key=lambda x: (-x[1], x[0]))[: max(0, topk)]
        seed_neighbors[seed] = [w for w, _ in ranked]

    return seed_neighbors, total_rows


def compute_degree_map(
    conn: sqlite3.Connection,
    table: str,
    col_a: str,
    col_b: str,
    wallets: Sequence[str],
) -> Dict[str, int]:
    """Compute true graph degree from the entire wallet_edges table for selected wallets."""
    out: Dict[str, int] = {w: 0 for w in wallets}
    if not wallets:
        return out
    qa, qb = escape_ident(col_a), escape_ident(col_b)
    ph = placeholders(len(wallets))
    q = (
        f"SELECT {qa}, {qb} FROM {escape_ident(table)} "
        f"WHERE {qa} IN ({ph}) OR {qb} IN ({ph})"
    )
    params = tuple(wallets) + tuple(wallets)
    for a, b in conn.execute(q, params):
        if a is not None:
            sa = str(a)
            if sa in out:
                out[sa] += 1
        if b is not None:
            sb = str(b)
            if sb in out:
                out[sb] += 1
    return out


def fetch_wallet_token_data(conn: sqlite3.Connection, wallets: Sequence[str]) -> Tuple[Dict[str, set], Dict[Tuple[str, str], int], Dict[str, Dict[str, int]]]:
    wallet_tokens: Dict[str, set] = {w: set() for w in wallets}
    first_in: Dict[Tuple[str, str], int] = {}
    stats: Dict[str, Dict[str, int]] = {w: {"min_bt": 0, "max_bt": 0, "tokens_entered": 0} for w in wallets}

    if not wallets:
        return wallet_tokens, first_in, stats

    q = (
        "SELECT wtf.scan_wallet, wtf.token_mint, wtf.block_time, wtf.flow_direction "
        "FROM wallet_token_flow wtf "
        "LEFT JOIN tx ON tx.signature = wtf.signature AND tx.scan_wallet = wtf.scan_wallet "
        f"WHERE wtf.scan_wallet IN ({placeholders(len(wallets))}) "
        "AND (tx.err IS NULL OR tx.err = '') "
        "ORDER BY wtf.scan_wallet ASC, wtf.token_mint ASC, wtf.block_time ASC"
    )

    for wallet, token, block_time, flow_dir in conn.execute(q, tuple(wallets)):
        if wallet is None or token is None or block_time is None:
            continue
        wallet = str(wallet)
        token = str(token)
        bt = int(block_time)
        flow = str(flow_dir or "")
        wallet_tokens.setdefault(wallet, set()).add(token)

        if flow == "in":
            key = (wallet, token)
            if key not in first_in or bt < first_in[key]:
                first_in[key] = bt

        s = stats.setdefault(wallet, {"min_bt": 0, "max_bt": 0, "tokens_entered": 0})
        if s["min_bt"] == 0 or bt < s["min_bt"]:
            s["min_bt"] = bt
        if s["max_bt"] == 0 or bt > s["max_bt"]:
            s["max_bt"] = bt

    for wallet in wallets:
        tokens_entered = {token for (w, token) in first_in if w == wallet}
        stats.setdefault(wallet, {"min_bt": 0, "max_bt": 0, "tokens_entered": 0})
        stats[wallet]["tokens_entered"] = len(tokens_entered)

    return wallet_tokens, first_in, stats


def compute_overlap_rows(wallets: Sequence[str], wallet_tokens: Dict[str, set]) -> List[List[object]]:
    rows: List[List[object]] = []
    ordered = sorted(wallets)
    for i, a in enumerate(ordered):
        ta = wallet_tokens.get(a, set())
        for b in ordered[i:]:
            tb = wallet_tokens.get(b, set())
            overlap = len(ta.intersection(tb))
            ratio = safe_ratio(overlap, min(len(ta), len(tb)))
            rows.append([a, b, len(ta), len(tb), overlap, f"{ratio:.6f}"])
    return rows


def compute_lead_follow(
    first_in: Dict[Tuple[str, str], int],
) -> Tuple[List[List[object]], Dict[Tuple[str, str], Dict[str, float]], Dict[Tuple[str, str], int], int]:
    token_wallets: Dict[str, List[Tuple[str, int]]] = defaultdict(list)
    for (wallet, token), t in first_in.items():
        token_wallets[token].append((wallet, t))

    pair_deltas: Dict[Tuple[str, str], List[int]] = defaultdict(list)
    overlap_tokens_by_pair: Dict[Tuple[str, str], int] = Counter()

    multi_wallet_tokens = 0
    for token, events in token_wallets.items():
        if len(events) < 2:
            continue
        multi_wallet_tokens += 1
        events = sorted(events, key=lambda x: (x[0]))
        for i in range(len(events)):
            wa, ta = events[i]
            for j in range(i + 1, len(events)):
                wb, tb = events[j]
                if wa < wb:
                    a, b, da = wa, wb, tb - ta
                else:
                    a, b, da = wb, wa, ta - tb
                pair_deltas[(a, b)].append(int(da))
                overlap_tokens_by_pair[(a, b)] += 1

    rows: List[List[object]] = []
    pair_stats: Dict[Tuple[str, str], Dict[str, float]] = {}
    pair_shared_tokens: Dict[Tuple[str, str], int] = {}

    for pair in sorted(pair_deltas):
        deltas = pair_deltas[pair]
        if not deltas:
            continue
        lead_rate = sum(1 for d in deltas if d > 0) / len(deltas)
        med = safe_median(deltas)
        p90 = percentile_nearest_rank(deltas, 0.90)
        shared = overlap_tokens_by_pair[pair]
        a, b = pair
        rows.append([a, b, shared, f"{lead_rate:.6f}", f"{med:.3f}", f"{p90:.3f}"])
        pair_stats[pair] = {
            "shared_tokens": float(shared),
            "lead_rate": float(lead_rate),
            "median_delta": float(med),
            "p90_delta": float(p90),
        }
        pair_shared_tokens[pair] = int(shared)

    rows.sort(key=lambda r: (-int(r[2]), -float(r[3]), float(r[4]), r[0], r[1]))
    return rows, pair_stats, pair_shared_tokens, multi_wallet_tokens


def infer_membership_columns(columns: Sequence[str], kind: str) -> Tuple[Optional[str], Optional[str]]:
    lc_map = {c.lower(): c for c in columns}

    wallet_exact = ["scan_wallet", "wallet", "wallet_address", "member_wallet"]
    for w in wallet_exact:
        if w in lc_map:
            wallet_col = lc_map[w]
            break
    else:
        wallet_candidates = [c for c in columns if "wallet" in c.lower()]
        wallet_col = sorted(wallet_candidates, key=str.lower)[0] if wallet_candidates else None

    id_exact = [f"{kind}_id", kind, f"{kind}_key", "id"]
    id_col = None
    for c in id_exact:
        if c in lc_map:
            id_col = lc_map[c]
            break
    if id_col is None:
        id_candidates = [c for c in columns if kind in c.lower() or c.lower().endswith("_id")]
        id_col = sorted(id_candidates, key=str.lower)[0] if id_candidates else None

    return wallet_col, id_col


def fetch_group_map(
    conn: sqlite3.Connection,
    table: str,
    wallets: Sequence[str],
    wallet_col: Optional[str],
    id_col: Optional[str],
) -> Dict[str, set]:
    out: Dict[str, set] = {w: set() for w in wallets}
    if not wallets or not wallet_col or not id_col:
        return out
    q = (
        f"SELECT {escape_ident(wallet_col)}, {escape_ident(id_col)} FROM {escape_ident(table)} "
        f"WHERE {escape_ident(wallet_col)} IN ({placeholders(len(wallets))})"
    )
    for w, gid in conn.execute(q, tuple(wallets)):
        if w is None or gid is None:
            continue
        out.setdefault(str(w), set()).add(str(gid))
    return out


def build_seed_verdicts(
    conn: sqlite3.Connection,
    seeds: Sequence[str],
    expanded: Sequence[str],
    first_in: Dict[Tuple[str, str], int],
    pair_stats: Dict[Tuple[str, str], Dict[str, float]],
    overlap_lookup: Dict[Tuple[str, str], float],
    pair_shared_tokens: Dict[Tuple[str, str], int],
    cohort_map: Dict[str, set],
    cluster_map: Dict[str, set],
    wallet_stats: Dict[str, Dict[str, int]],
    episode_gap: int,
) -> List[dict]:
    verdicts: List[dict] = []

    for seed in sorted(seeds):
        q = (
            "SELECT wtf.token_mint, wtf.flow_direction, wtf.block_time "
            "FROM wallet_token_flow wtf "
            "LEFT JOIN tx ON tx.signature = wtf.signature AND tx.scan_wallet = wtf.scan_wallet "
            "WHERE wtf.scan_wallet=? AND (tx.err IS NULL OR tx.err='') "
            "ORDER BY wtf.block_time ASC, wtf.token_mint ASC"
        )
        rows = conn.execute(q, (seed,)).fetchall()

        episodes: List[int] = []
        last_by_key: Dict[Tuple[str, str], int] = {}
        episode_len: Dict[Tuple[str, str], int] = {}

        token_first_last: Dict[str, List[int]] = {}
        token_buys = Counter()
        token_sells = Counter()

        for token, flow, bt in rows:
            if token is None or bt is None:
                continue
            token = str(token)
            flow = str(flow or "")
            bt = int(bt)

            if token not in token_first_last:
                token_first_last[token] = [bt, bt]
            else:
                token_first_last[token][1] = bt

            if flow == "in":
                token_buys[token] += 1
            elif flow == "out":
                token_sells[token] += 1

            key = (token, flow)
            prev = last_by_key.get(key)
            if prev is None or (bt - prev) > episode_gap:
                if key in episode_len:
                    episodes.append(episode_len[key])
                episode_len[key] = 1
            else:
                episode_len[key] += 1
            last_by_key[key] = bt

        for key in sorted(episode_len):
            episodes.append(episode_len[key])

        hold_proxies = [v[1] - v[0] for v in token_first_last.values()]
        buys_per_token = list(token_buys.values())
        sell_tokens = [t for t, c in token_sells.items() if c > 0]
        partial_tokens = [t for t in sell_tokens if token_sells[t] >= 2]

        median_attempts = safe_median(episodes)
        p90_attempts = percentile_nearest_rank(episodes, 0.90)
        median_hold = safe_median(hold_proxies)
        median_buys = safe_median(buys_per_token)
        partial_exit_rate = safe_ratio(len(partial_tokens), len(sell_tokens))

        ws = wallet_stats.get(seed, {})
        active_window = max(0, int(ws.get("max_bt", 0)) - int(ws.get("min_bt", 0)))
        tokens_entered = int(ws.get("tokens_entered", 0))
        entered_per_hour = safe_ratio(tokens_entered, max(active_window, 1) / 3600.0)

        lead_vals = []
        lead_med_deltas = []
        overlap_vals = []
        for other in sorted(set(expanded) - {seed}):
            a, b = (seed, other) if seed < other else (other, seed)
            ps = pair_stats.get((a, b))
            if not ps:
                continue
            if ps["shared_tokens"] < 20:
                continue
            if seed == a:
                lead = ps["lead_rate"]
                md = ps["median_delta"]
            else:
                lead = 1.0 - ps["lead_rate"]
                md = -ps["median_delta"]
            lead_vals.append(lead)
            lead_med_deltas.append(md)
            overlap_vals.append(overlap_lookup.get((a, b), 0.0))

        lead_power = (sum(lead_vals) / len(lead_vals)) if lead_vals else 0.0
        lead_median_delta = safe_median(lead_med_deltas) if lead_med_deltas else 0.0
        overlap_power = (sum(overlap_vals) / len(overlap_vals)) if overlap_vals else 0.0

        sharedness = 0
        seed_groups = cohort_map.get(seed, set()).union(cluster_map.get(seed, set()))
        for other_seed in sorted(set(seeds) - {seed}):
            other_groups = cohort_map.get(other_seed, set()).union(cluster_map.get(other_seed, set()))
            if seed_groups.intersection(other_groups):
                sharedness += 1

        conds = [lead_power >= 0.7, lead_median_delta <= 10, overlap_power >= 0.4]
        met = sum(1 for c in conds if c)
        if met == 3:
            suspicion = "high"
        elif met >= 2:
            suspicion = "medium"
        else:
            suspicion = "low"

        if median_buys <= 1.5 and median_attempts <= 1.5:
            archetype = "sniper"
        elif median_buys >= 6 or p90_attempts >= 8:
            archetype = "ladder_accumulator"
        elif median_hold <= 30 and entered_per_hour >= 20:
            archetype = "recycler"
        else:
            archetype = "hybrid" if partial_exit_rate >= 0.5 else "controlled_scaler"

        if suspicion == "high" and lead_power >= 0.8 and abs(lead_median_delta) <= 5:
            replicable = "no" if sharedness >= 2 else "partial"
        elif suspicion == "high":
            replicable = "partial"
        else:
            replicable = "yes"

        metrics = {
            "median_attempts_per_episode": round(median_attempts, 6),
            "p90_attempts_per_episode": round(p90_attempts, 6),
            "median_hold_proxy_seconds": round(median_hold, 6),
            "median_buys_per_token": round(median_buys, 6),
            "partial_exit_rate": round(partial_exit_rate, 6),
            "tokens_entered_per_hour": round(entered_per_hour, 6),
            "lead_power": round(lead_power, 6),
            "lead_median_delta": round(lead_median_delta, 6),
            "overlap_power": round(overlap_power, 6),
            "cohort_sharedness": sharedness,

        }
        verdicts.append(
            {
                "wallet": seed,
                "metrics": metrics,
                "archetype": archetype,
                "replicable": replicable,
                "hidden_edge_suspicion": suspicion,
                "evidence": {
                    "episodes": {
                        "median_attempts_per_episode": metrics["median_attempts_per_episode"],
                        "p90_attempts_per_episode": metrics["p90_attempts_per_episode"],
                    },
                    "token_behavior": {
                        "median_buys_per_token": metrics["median_buys_per_token"],
                        "partial_exit_rate": metrics["partial_exit_rate"],
                        "median_hold_proxy_seconds": metrics["median_hold_proxy_seconds"],
                    },
                    "network": {
                        "lead_power": metrics["lead_power"],
                        "lead_median_delta": metrics["lead_median_delta"],
                        "overlap_power": metrics["overlap_power"],
                        "cohort_sharedness": sharedness,
                    },
                },
            }
        )

    verdicts.sort(key=lambda x: x["wallet"])
    return verdicts


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, header: Sequence[str], rows: Iterable[Sequence[object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in rows:
            w.writerow(row)


def main() -> None:
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    seeds = load_seeds(Path(args.seeds))
    seeds_set = set(seeds)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    log_lines: List[str] = []
    log_lines.append(f"db={args.db}")
    log_lines.append(f"seeds_file={args.seeds}")
    log_lines.append(f"topk={args.topk}")
    log_lines.append(f"episode_gap={args.episode_gap}")
    log_lines.append(f"seed_count={len(seeds)}")

    edge_columns = get_table_columns(conn, "wallet_edges")
    col_a, col_b, weight_col, edge_method = choose_edge_columns(edge_columns)
    log_lines.append(f"wallet_edges_columns={edge_columns}")
    log_lines.append(
        f"wallet_edges_selection=method:{edge_method}, endpoint_a:{col_a}, endpoint_b:{col_b}, weight:{weight_col or 'CONST_1'}"
    )

    if not col_a or not col_b:
        raise RuntimeError("Could not infer wallet_edges endpoint columns.")

    seed_neighbors, scanned_rows = expand_neighbors(
        conn,
        seeds,
        "wallet_edges",
        col_a,
        col_b,
        weight_col,
        args.topk,
    )

    neighbors = sorted({n for vals in seed_neighbors.values() for n in vals})
    expanded = sorted(seeds_set.union(neighbors))
    degree_map = compute_degree_map(conn, "wallet_edges", col_a, col_b, expanded)
    log_lines.append(f"wallet_edges_rows_scanned={scanned_rows}")
    log_lines.append(f"neighbor_count={len(neighbors)}")
    log_lines.append(f"expanded_count={len(expanded)}")

    wallet_tokens, first_in, wallet_stats = fetch_wallet_token_data(conn, expanded)

    overlap_rows = compute_overlap_rows(expanded, wallet_tokens)
    overlap_path = outdir / "overlap_matrix.csv"
    write_csv(
        overlap_path,
        ["wallet_a", "wallet_b", "tokens_a", "tokens_b", "overlap_count", "overlap_ratio"],
        overlap_rows,
    )

    overlap_lookup: Dict[Tuple[str, str], float] = {}
    for a, b, _, _, _, r in overlap_rows:
        overlap_lookup[(str(a), str(b))] = float(r)

    lf_rows, pair_stats, pair_shared_tokens, multi_wallet_tokens = compute_lead_follow(first_in)
    lead_path = outdir / "lead_follow.csv"
    write_csv(
        lead_path,
        ["wallet_a", "wallet_b", "shared_tokens", "lead_rate_a_over_b", "median_delta", "p90_delta"],
        lf_rows,
    )

    cohorts_cols = get_table_columns(conn, "cohorts")
    cm_cols = get_table_columns(conn, "cohort_members")
    log_lines.append(f"cohorts_columns={cohorts_cols}")
    wc_cols = get_table_columns(conn, "wallet_clusters")
    c_wallet_col, c_id_col = infer_membership_columns(cm_cols, "cohort")
    w_wallet_col, w_id_col = infer_membership_columns(wc_cols, "cluster")
    log_lines.append(f"cohort_members_columns={cm_cols}")
    log_lines.append(f"cohort_members_selection=wallet:{c_wallet_col}, cohort_id:{c_id_col}")
    log_lines.append(f"wallet_clusters_columns={wc_cols}")
    log_lines.append(f"wallet_clusters_selection=wallet:{w_wallet_col}, cluster_id:{w_id_col}")

    cohort_map = fetch_group_map(conn, "cohort_members", expanded, c_wallet_col, c_id_col)
    cluster_map = fetch_group_map(conn, "wallet_clusters", expanded, w_wallet_col, w_id_col)

    cohort_rows = []
    for w in sorted(expanded):
        cohorts = ",".join(sorted(cohort_map.get(w, set())))
        clusters = ",".join(sorted(cluster_map.get(w, set())))
        degree = degree_map.get(w, 0)
        cohort_rows.append([w, degree, cohorts, clusters])

    cohort_path = outdir / "cohorts_clusters.csv"
    write_csv(cohort_path, ["wallet", "degree", "cohorts", "clusters"], cohort_rows)

    verdicts = build_seed_verdicts(
        conn,
        seeds,
        expanded,
        first_in,
        pair_stats,
        overlap_lookup,
        pair_shared_tokens,
        cohort_map,
        cluster_map,
        wallet_stats,
        args.episode_gap,
    )

    verdict_path = outdir / "verdict_seeds.json"
    with verdict_path.open("w", encoding="utf-8") as f:
        json.dump(verdicts, f, indent=2, sort_keys=True)

    log_lines.append(f"first_in_pairs={len(first_in)}")
    log_lines.append(f"wallets_with_token_activity={sum(1 for w in expanded if wallet_tokens.get(w))}")
    log_lines.append(f"tokens_multi_wallet={multi_wallet_tokens}")
    log_lines.append(f"lead_follow_pairs={len(lf_rows)}")

    digests = {
        overlap_path.name: sha256_file(overlap_path),
        lead_path.name: sha256_file(lead_path),
        cohort_path.name: sha256_file(cohort_path),
        verdict_path.name: sha256_file(verdict_path),
    }

    for fname in sorted(digests):
        log_lines.append(f"sha256 {fname} {digests[fname]}")

    log_path = outdir / "run.log"
    with log_path.open("w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(log_lines) + "\n")

    conn.close()


if __name__ == "__main__":
    main()
