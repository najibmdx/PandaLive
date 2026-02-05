#!/usr/bin/env python3
"""
panda_phase4_4_discovery.py

Phase 4.4 discovery script with deterministic outputs.
"""

import argparse
import hashlib
import json
import math
import os
import sqlite3
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.mixture import GaussianMixture
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import RobustScaler


LENS_DEFS = {
    "A": ["N1_tx_rate", "N2_inflow_rate", "N3_outflow_rate", "N4_token_interaction_rate", "N5_burstiness_index"],
    "B": ["N6_counterparty_rate", "N7_counterparty_repetition_ratio", "N8_edge_density_norm"],
    "C": ["N9_cluster_membership_intensity"],
    "D": ["N11_whale_enter_recency_sec", "N12_whale_enter_magnitude_log", "N13_flow_delta_around_enter"],
    "E": ["N14_token_reentry_rate", "N15_capital_recycling_ratio"],
    "F": ["N16_wallet_age_log"],
}


class HardError(Exception):
    pass


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str) -> str:
    with open(path, "rb") as f:
        return sha256_bytes(f.read())


def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def column_names(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]


def require_tables(conn: sqlite3.Connection, tables: List[str]) -> None:
    missing = [t for t in tables if not table_exists(conn, t)]
    if missing:
        raise HardError(f"Missing required tables: {', '.join(missing)}")


def get_max_time(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT MAX(block_time) FROM swaps").fetchone()
    if row is None or row[0] is None:
        raise HardError("Could not compute max_time from swaps.block_time")
    return int(row[0])


def window_defs(max_time: int) -> Dict[str, Dict[str, int]]:
    return {
        "24h": {"start": max_time - 86400, "end": max_time, "duration": 86400},
        "7d": {"start": max_time - 604800, "end": max_time, "duration": 604800},
        "lifetime": {"start": 0, "end": max_time, "duration": max_time},
    }


def load_wallet_universe(conn: sqlite3.Connection) -> List[str]:
    query = """
        SELECT scan_wallet AS wallet FROM wallet_token_flow
        UNION
        SELECT scan_wallet AS wallet FROM swaps
        UNION
        SELECT wallet AS wallet FROM whale_transitions
    """
    rows = conn.execute(query).fetchall()
    wallets = sorted({r[0] for r in rows if r[0] is not None})
    return wallets


def load_wallet_token_flow(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT scan_wallet AS wallet,
               block_time AS time,
               token_mint AS mint,
               token_amount_raw AS amount_raw,
               flow_direction AS direction
        FROM wallet_token_flow
        """,
        conn,
    )


def load_swaps(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT scan_wallet AS wallet,
               signature AS signature,
               block_time AS time
        FROM swaps
        """,
        conn,
    )


def load_spl_transfers(conn: sqlite3.Connection) -> Optional[pd.DataFrame]:
    if not table_exists(conn, "spl_transfers_v2"):
        return None
    cols = column_names(conn, "spl_transfers_v2")
    required = {"scan_wallet", "signature"}
    if not required.issubset(cols):
        return None
    time_col = "block_time" if "block_time" in cols else "time" if "time" in cols else None
    if time_col is None:
        return None
    return pd.read_sql_query(
        f"""
        SELECT scan_wallet AS wallet,
               signature AS signature,
               {time_col} AS time
        FROM spl_transfers_v2
        """,
        conn,
    )


def load_wallet_edges(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT src_wallet AS src_wallet,
               dst_wallet AS dst_wallet,
               weight AS weight,
               window AS window,
               created_at_utc AS time,
               edge_type AS edge_type
        FROM wallet_edges
        """,
        conn,
    )


def load_whale_transitions(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT wallet,
               window,
               side,
               event_time,
               amount_lamports,
               supporting_flow_count,
               flow_ref
        FROM whale_transitions
        """,
        conn,
    )


def load_wallets_table(conn: sqlite3.Connection) -> Optional[pd.DataFrame]:
    if not table_exists(conn, "wallets"):
        return None
    cols = column_names(conn, "wallets")
    if not {"first_seen", "last_seen", "wallet"}.issubset(cols):
        return None
    return pd.read_sql_query(
        "SELECT wallet, first_seen, last_seen FROM wallets",
        conn,
    )


def load_wallet_clusters(conn: sqlite3.Connection) -> Optional[pd.DataFrame]:
    if not table_exists(conn, "wallet_clusters"):
        return None
    cols = column_names(conn, "wallet_clusters")
    required = {"scan_wallet", "window", "cluster_id"}
    if not required.issubset(cols):
        return None
    return pd.read_sql_query(
        """
        SELECT scan_wallet AS wallet,
               window,
               cluster_id
        FROM wallet_clusters
        """,
        conn,
    )


def drop_phase4_tables(conn: sqlite3.Connection) -> None:
    for table in [
        "phase4_features_norm",
        "phase4_runs",
        "phase4_patterns",
        "phase4_pattern_stats",
    ]:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()


def create_phase4_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS phase4_runs (
            run_id TEXT PRIMARY KEY,
            started_at INTEGER,
            code_sha256 TEXT,
            window_defs_json TEXT,
            feature_defs_json TEXT,
            max_time INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS phase4_features_norm (
            run_id TEXT,
            wallet TEXT,
            window TEXT,
            created_at INTEGER,
            N1_tx_rate REAL,
            N2_inflow_rate REAL,
            N3_outflow_rate REAL,
            N4_token_interaction_rate REAL,
            N5_burstiness_index REAL,
            N6_counterparty_rate REAL,
            N7_counterparty_repetition_ratio REAL,
            N8_edge_density_norm REAL,
            N9_cluster_membership_intensity REAL,
            N10_intra_cluster_flow_ratio REAL,
            N11_whale_enter_recency_sec REAL,
            N12_whale_enter_magnitude_log REAL,
            N13_flow_delta_around_enter REAL,
            N14_token_reentry_rate REAL,
            N15_capital_recycling_ratio REAL,
            N16_wallet_age_log REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS phase4_patterns (
            run_id TEXT,
            wallet TEXT,
            window TEXT,
            lens_id TEXT,
            pattern_id TEXT,
            is_noise INTEGER,
            created_at INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS phase4_pattern_stats (
            run_id TEXT,
            window TEXT,
            lens_id TEXT,
            pattern_id TEXT,
            member_count INTEGER
        )
        """
    )
    conn.commit()


def compute_burstiness(times: np.ndarray) -> Optional[float]:
    if len(times) < 3:
        return None
    gaps = np.diff(np.sort(times))
    if gaps.size == 0:
        return None
    mean_gap = np.mean(gaps)
    if mean_gap == 0:
        return None
    return float(np.std(gaps) / mean_gap)


def compute_token_reentry(flow_df: pd.DataFrame) -> Dict[str, float]:
    results = {}
    for wallet, wdf in flow_df.groupby("wallet"):
        reentry_count = 0
        total_tokens = wdf["mint"].nunique()
        if total_tokens == 0:
            results[wallet] = None
            continue
        for mint, mdf in wdf.groupby("mint"):
            outs = mdf[mdf["direction"] == "out"]["time"].values
            ins = mdf[mdf["direction"] == "in"]["time"].values
            if len(outs) == 0 or len(ins) == 0:
                continue
            if ins.max() > outs.min():
                reentry_count += 1
        results[wallet] = reentry_count / total_tokens if total_tokens > 0 else None
    return results


def compute_capital_recycling(flow_df: pd.DataFrame, total_outflow: Dict[str, float]) -> Dict[str, float]:
    results = {}
    for wallet, wdf in flow_df.groupby("wallet"):
        out_total = total_outflow.get(wallet)
        if out_total is None or out_total == 0:
            results[wallet] = None
            continue
        matched_sum = 0.0
        for mint, mdf in wdf.groupby("mint"):
            out_rows = mdf[mdf["direction"] == "out"].sort_values("time")
            in_rows = mdf[mdf["direction"] == "in"].sort_values("time")
            if out_rows.empty or in_rows.empty:
                continue
            in_times = in_rows["time"].values
            in_amts = in_rows["amount_abs"].values
            used = np.zeros(len(in_times), dtype=bool)
            for _, out_row in out_rows.iterrows():
                out_time = out_row["time"]
                out_amt = out_row["amount_abs"]
                idx = np.searchsorted(in_times, out_time, side="left")
                while idx < len(in_times):
                    if used[idx]:
                        idx += 1
                        continue
                    if in_times[idx] > out_time + 1800:
                        break
                    used[idx] = True
                    matched_sum += min(out_amt, in_amts[idx])
                    break
        results[wallet] = matched_sum / out_total if out_total > 0 else None
    return results


def build_phase4_features(
    wallets: List[str],
    flow_df: pd.DataFrame,
    swaps_df: pd.DataFrame,
    spl_df: Optional[pd.DataFrame],
    edges_df: pd.DataFrame,
    whale_df: pd.DataFrame,
    clusters_df: Optional[pd.DataFrame],
    wallets_df: Optional[pd.DataFrame],
    windows: Dict[str, Dict[str, int]],
    run_id: str,
    created_at: int,
) -> pd.DataFrame:
    tx_df = swaps_df.copy()
    if spl_df is not None:
        tx_df = pd.concat([tx_df, spl_df], ignore_index=True)
    tx_df = tx_df.dropna(subset=["wallet", "signature", "time"])

    flow_df = flow_df.dropna(subset=["wallet", "time", "mint", "direction"]).copy()
    flow_df["direction"] = flow_df["direction"].str.lower()
    flow_df["amount_abs"] = flow_df["amount_raw"].abs()

    edges_df = edges_df.dropna(subset=["src_wallet", "dst_wallet", "window"]).copy()

    whale_df = whale_df.dropna(subset=["wallet", "window", "event_time", "amount_lamports"]).copy()
    clusters_df = (
        clusters_df.dropna(subset=["wallet", "window", "cluster_id"]).copy()
        if clusters_df is not None
        else None
    )

    feature_rows = []

    for window_name, win in windows.items():
        start, end, duration = win["start"], win["end"], win["duration"]
        flow_win = flow_df[(flow_df["time"] >= start) & (flow_df["time"] <= end)]
        tx_win = tx_df[(tx_df["time"] >= start) & (tx_df["time"] <= end)]

        tx_counts = tx_win.groupby("wallet")["signature"].nunique().to_dict()

        inflow = flow_win[flow_win["direction"] == "in"].groupby("wallet")["amount_raw"].sum().to_dict()
        outflow = flow_win[flow_win["direction"] == "out"].groupby("wallet")["amount_raw"].sum().to_dict()

        unique_tokens = flow_win.groupby("wallet")["mint"].nunique().to_dict()

        burstiness = {}
        for wallet, wdf in flow_win.groupby("wallet"):
            burstiness[wallet] = compute_burstiness(wdf["time"].values)

        edges_win = edges_df[edges_df["window"] == window_name]
        if not edges_win.empty:
            norm_edges = pd.concat(
                [
                    edges_win[["src_wallet", "dst_wallet"]].rename(
                        columns={"src_wallet": "wallet", "dst_wallet": "counterparty"}
                    ),
                    edges_win[["dst_wallet", "src_wallet"]].rename(
                        columns={"dst_wallet": "wallet", "src_wallet": "counterparty"}
                    ),
                ],
                ignore_index=True,
            )
        else:
            norm_edges = pd.DataFrame(columns=["wallet", "counterparty"])

        counterparty_counts = norm_edges.groupby("wallet")["counterparty"].nunique().to_dict()
        interaction_counts = norm_edges.groupby(["wallet", "counterparty"]).size().reset_index(name="count")
        repeat_counts = (
            interaction_counts[interaction_counts["count"] >= 2]
            .groupby("wallet")
            .size()
            .to_dict()
        )
        actual_edges = norm_edges.groupby("wallet").size().to_dict()

        whale_win = whale_df[whale_df["window"] == window_name]
        whale_win = whale_win.sort_values(["wallet", "event_time", "flow_ref"])
        whale_first = whale_win.groupby("wallet").first().reset_index()
        whale_first = whale_first.set_index("wallet")

        cluster_counts = {}
        if clusters_df is not None:
            clusters_win = clusters_df[clusters_df["window"] == window_name]
            cluster_counts = (
                clusters_win.groupby("wallet")["cluster_id"].nunique().to_dict()
                if not clusters_win.empty
                else {}
            )

        flow_delta = {}
        total_flow_abs = flow_win.groupby("wallet")["amount_abs"].sum().to_dict()
        if not whale_first.empty:
            for wallet, row in whale_first.iterrows():
                enter_time = row["event_time"]
                wflow = flow_win[flow_win["wallet"] == wallet]
                pre = wflow[(wflow["time"] >= enter_time - 3600) & (wflow["time"] < enter_time)][
                    "amount_abs"
                ].sum()
                post = wflow[(wflow["time"] >= enter_time) & (wflow["time"] < enter_time + 3600)][
                    "amount_abs"
                ].sum()
                total = total_flow_abs.get(wallet)
                if total is None or total == 0:
                    flow_delta[wallet] = None
                else:
                    flow_delta[wallet] = float((post - pre) / total)

        token_reentry = compute_token_reentry(flow_win)
        capital_recycling = compute_capital_recycling(flow_win, outflow)

        for wallet in wallets:
            tx_count = tx_counts.get(wallet, 0)
            inflow_total = inflow.get(wallet)
            outflow_total = outflow.get(wallet)
            unique_token_count = unique_tokens.get(wallet)
            n1 = tx_count / duration if duration else None
            n2 = (inflow_total / duration) if (inflow_total is not None and duration) else None
            n3 = (outflow_total / duration) if (outflow_total is not None and duration) else None
            n4 = None
            if unique_token_count is not None and tx_count > 0:
                n4 = unique_token_count / tx_count

            n5 = burstiness.get(wallet)

            counterparty_count = counterparty_counts.get(wallet)
            n6 = None
            if counterparty_count is not None and tx_count > 0:
                n6 = counterparty_count / tx_count

            total_counterparties = counterparty_count
            repeat_edges = repeat_counts.get(wallet)
            n7 = None
            if total_counterparties is not None and total_counterparties > 0:
                n7 = (repeat_edges or 0) / total_counterparties

            n8 = None
            if total_counterparties is not None:
                possible_edges = total_counterparties * (total_counterparties - 1)
                if possible_edges > 0:
                    n8 = (actual_edges.get(wallet, 0) / possible_edges)

            n9 = None
            n10 = None
            if clusters_df is not None:
                cluster_count = cluster_counts.get(wallet, 0)
                n9 = cluster_count / duration if duration else None

            n11 = None
            n12 = None
            n13 = None
            if wallet in whale_first.index:
                enter_time = whale_first.loc[wallet, "event_time"]
                amount_lamports = whale_first.loc[wallet, "amount_lamports"]
                n11 = float(end - enter_time)
                n12 = float(math.log1p(amount_lamports)) if amount_lamports is not None else None
                n13 = flow_delta.get(wallet)

            n14 = token_reentry.get(wallet)
            n15 = capital_recycling.get(wallet)

            feature_rows.append(
                {
                    "run_id": run_id,
                    "wallet": wallet,
                    "window": window_name,
                    "created_at": created_at,
                    "N1_tx_rate": n1,
                    "N2_inflow_rate": n2,
                    "N3_outflow_rate": n3,
                    "N4_token_interaction_rate": n4,
                    "N5_burstiness_index": n5,
                    "N6_counterparty_rate": n6,
                    "N7_counterparty_repetition_ratio": n7,
                    "N8_edge_density_norm": n8,
                    "N9_cluster_membership_intensity": n9,
                    "N10_intra_cluster_flow_ratio": n10,
                    "N11_whale_enter_recency_sec": n11,
                    "N12_whale_enter_magnitude_log": n12,
                    "N13_flow_delta_around_enter": n13,
                    "N14_token_reentry_rate": n14,
                    "N15_capital_recycling_ratio": n15,
                    "N16_wallet_age_log": None,
                }
            )

    features_df = pd.DataFrame(feature_rows)

    age_map = {}
    if wallets_df is not None:
        for _, row in wallets_df.iterrows():
            if row["first_seen"] is None or row["last_seen"] is None:
                continue
            age = row["last_seen"] - row["first_seen"]
            age_map[row["wallet"]] = float(math.log1p(age))
    else:
        flow_stats = flow_df.groupby("wallet")["time"].agg(["min", "max"]).reset_index()
        for _, row in flow_stats.iterrows():
            age = row["max"] - row["min"]
            age_map[row["wallet"]] = float(math.log1p(age))

    features_df["N16_wallet_age_log"] = features_df["wallet"].map(age_map)

    return features_df


def choose_gmm_k(data: np.ndarray) -> Tuple[int, int, bool]:
    max_k = min(12, data.shape[0])
    if max_k <= 1:
        return 1, 0, False
    best_k = 1
    best_bic = None
    skipped = 0
    for k in range(1, max_k + 1):
        gmm = GaussianMixture(
            n_components=k,
            reg_covar=1e-6,
            init_params="kmeans",
            n_init=3,
            max_iter=500,
            random_state=0,
        )
        try:
            gmm.fit(data)
        except Exception:
            skipped += 1
            continue
        bic = gmm.bic(data)
        if best_bic is None or bic < best_bic:
            best_bic = bic
            best_k = k
    if best_bic is None:
        return 1, skipped, True
    return best_k, skipped, False


def run_dbscan(data: np.ndarray) -> np.ndarray:
    if data.shape[0] <= 5:
        return np.full(data.shape[0], -1)
    nbrs = NearestNeighbors(n_neighbors=5)
    nbrs.fit(data)
    distances, _ = nbrs.kneighbors(data)
    kth_distances = distances[:, 4]
    eps = np.percentile(kth_distances, 90)
    eps = float(eps)
    if eps <= 0.0:
        eps = 1e-12
        print(f"[INFO] DBSCAN eps clamped to {eps} (computed eps was <= 0)")
    db = DBSCAN(eps=eps, min_samples=5)
    return db.fit_predict(data)


def build_patterns(
    features_df: pd.DataFrame,
    wallets: List[str],
    windows: Dict[str, Dict[str, int]],
    run_id: str,
    created_at: int,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Dict[str, int]]]:
    pattern_rows = []
    stats_rows = []
    summary = {}

    for window_name in windows.keys():
        window_df = features_df[features_df["window"] == window_name]
        for lens_id, cols in LENS_DEFS.items():
            if lens_id == "F" and window_name != "lifetime":
                continue
            gmm_k = 0
            gmm_skipped = 0
            lens_df = window_df[["wallet"] + cols].copy()
            valid_mask = lens_df[cols].notna().all(axis=1)
            valid_df = lens_df[valid_mask]
            data = valid_df[cols].to_numpy()

            labels = None
            if data.shape[0] == 0:
                labels = np.array([])
            elif lens_id in {"A", "B", "E", "F"}:
                scaler = RobustScaler(quantile_range=(25, 75))
                data_scaled = scaler.fit_transform(data)
                k, skipped, use_fallback = choose_gmm_k(data_scaled)
                gmm_k = k
                gmm_skipped = skipped
                reg_covar = 1e-4 if use_fallback else 1e-6
                gmm = GaussianMixture(
                    n_components=k,
                    reg_covar=reg_covar,
                    init_params="kmeans",
                    n_init=3,
                    max_iter=500,
                    random_state=0,
                )
                gmm.fit(data_scaled)
                labels = gmm.predict(data_scaled)
            else:
                labels = run_dbscan(data)

            wallet_to_label = {}
            if data.shape[0] > 0:
                for wallet, label in zip(valid_df["wallet"].tolist(), labels.tolist()):
                    wallet_to_label[wallet] = label

            cluster_members = defaultdict(list)
            for wallet in wallets:
                if wallet not in wallet_to_label:
                    pattern_rows.append(
                        {
                            "run_id": run_id,
                            "wallet": wallet,
                            "window": window_name,
                            "lens_id": lens_id,
                            "pattern_id": None,
                            "is_noise": 1,
                            "created_at": created_at,
                        }
                    )
                    continue
                label = wallet_to_label[wallet]
                if label == -1:
                    pattern_rows.append(
                        {
                            "run_id": run_id,
                            "wallet": wallet,
                            "window": window_name,
                            "lens_id": lens_id,
                            "pattern_id": None,
                            "is_noise": 1,
                            "created_at": created_at,
                        }
                    )
                else:
                    cluster_members[label].append(wallet)

            for label, members in cluster_members.items():
                members_sorted = sorted(members)
                text = "\n".join(members_sorted)
                digest = sha1_text(text)[:12]
                pattern_id = f"{lens_id}:{window_name}:{digest}"
                for wallet in members_sorted:
                    pattern_rows.append(
                        {
                            "run_id": run_id,
                            "wallet": wallet,
                            "window": window_name,
                            "lens_id": lens_id,
                            "pattern_id": pattern_id,
                            "is_noise": 0,
                            "created_at": created_at,
                        }
                    )
                stats_rows.append(
                    {
                        "run_id": run_id,
                        "window": window_name,
                        "lens_id": lens_id,
                        "pattern_id": pattern_id,
                        "member_count": len(members_sorted),
                    }
                )

            wallets_total = len(wallets)
            noise_count = sum(
                1
                for w in wallets
                if (w not in wallet_to_label) or wallet_to_label.get(w, -1) == -1
            )
            clustered = wallets_total - noise_count
            top_sizes = sorted([len(v) for v in cluster_members.values()], reverse=True)[:5]
            summary_key = f"{window_name}:{lens_id}"
            summary_entry = {
                "wallets_total": wallets_total,
                "clustered": clustered,
                "noise": noise_count,
                "patterns_found": len(cluster_members),
                "top5_sizes": top_sizes,
            }
            if lens_id in {"A", "B", "E", "F"}:
                summary_entry["gmm_skipped_k"] = gmm_skipped if data.shape[0] > 0 else 0
                summary_entry["gmm_k"] = gmm_k if data.shape[0] > 0 else 0
            summary[summary_key] = summary_entry

    return pd.DataFrame(pattern_rows), pd.DataFrame(stats_rows), summary


def write_tsv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, sep="\t", index=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--fresh", action="store_true")
    args = parser.parse_args()

    db_path = args.db
    outdir = args.outdir
    this_file = os.path.abspath(__file__)
    code_sha = hashlib.sha256(open(this_file, "rb").read()).hexdigest()
    print(f"[CODE] file={this_file}")
    print(f"[CODE] sha256={code_sha}")
    os.makedirs(outdir, exist_ok=True)

    conn = sqlite3.connect(db_path)

    try:
        require_tables(conn, ["swaps", "wallet_token_flow", "wallet_edges", "whale_transitions"])
        max_time = get_max_time(conn)
        print(f"canonical max_time = {max_time}")

        if args.fresh:
            drop_phase4_tables(conn)
        create_phase4_tables(conn)

        win_defs = window_defs(max_time)
        wallets = load_wallet_universe(conn)
        if not wallets:
            raise HardError("Wallet universe is empty")

        flow_df = load_wallet_token_flow(conn)
        swaps_df = load_swaps(conn)
        spl_df = load_spl_transfers(conn)
        edges_df = load_wallet_edges(conn)
        whale_df = load_whale_transitions(conn)
        clusters_df = load_wallet_clusters(conn)
        wallets_df = load_wallets_table(conn)

        run_id = sha1_text(f"{code_sha}:{max_time}")
        created_at = max_time

        feature_defs = {
            "N8_actual_edges_rule": "COUNT(*) rows in wallet_edges involving wallet for window",
            "N9": "COUNT(DISTINCT cluster_id) from wallet_clusters per wallet/window",
            "N10": "NULL (no provable windowed membership table)",
            "lens_gmm_scaler": "RobustScaler(25,75)",
        }

        conn.execute(
            "INSERT OR REPLACE INTO phase4_runs (run_id, started_at, code_sha256, window_defs_json, feature_defs_json, max_time) VALUES (?, ?, ?, ?, ?, ?)",
            (
                run_id,
                created_at,
                code_sha,
                json.dumps(win_defs, sort_keys=True),
                json.dumps(feature_defs, sort_keys=True),
                max_time,
            ),
        )
        conn.commit()

        features_df = build_phase4_features(
            wallets,
            flow_df,
            swaps_df,
            spl_df,
            edges_df,
            whale_df,
            clusters_df,
            wallets_df,
            win_defs,
            run_id,
            created_at,
        )

        expected_rows = len(wallets) * len(win_defs)
        if len(features_df) != expected_rows:
            raise HardError(
                f"phase4_features_norm row count mismatch: expected {expected_rows}, got {len(features_df)}"
            )

        features_df = features_df.sort_values(["wallet", "window"]).reset_index(drop=True)

        features_df.to_sql("phase4_features_norm", conn, if_exists="append", index=False)

        rows = conn.execute(
            """
            SELECT window,
                   SUM(CASE WHEN N9_cluster_membership_intensity IS NULL THEN 1 ELSE 0 END) AS nulls,
                   COUNT(*) AS total
            FROM phase4_features_norm
            GROUP BY window
            """
        ).fetchall()
        print("[CHECK] N9 null coverage:", rows)
        for window, nulls, total in rows:
            if nulls == total:
                raise RuntimeError(
                    f"N9_cluster_membership_intensity ALL NULL for window={window}; check wallet_clusters join"
                )

        patterns_df, stats_df, summary = build_patterns(
            features_df,
            wallets,
            win_defs,
            run_id,
            created_at,
        )

        patterns_df = patterns_df.sort_values(["wallet", "window", "lens_id"]).reset_index(drop=True)
        stats_df = stats_df.sort_values(["window", "lens_id", "pattern_id"]).reset_index(drop=True)

        patterns_df.to_sql("phase4_patterns", conn, if_exists="append", index=False)
        stats_df.to_sql("phase4_pattern_stats", conn, if_exists="append", index=False)

        for window_name in win_defs.keys():
            lens_ids = [lid for lid in LENS_DEFS.keys() if not (lid == "F" and window_name != "lifetime")]
            for lens_id in lens_ids:
                subset = patterns_df[(patterns_df["window"] == window_name) & (patterns_df["lens_id"] == lens_id)]
                if len(subset) != len(wallets):
                    raise HardError(
                        f"phase4_patterns row count mismatch for {window_name}/{lens_id}: expected {len(wallets)}, got {len(subset)}"
                    )

        features_tsv = os.path.join(outdir, "phase4_features_norm.tsv")
        patterns_tsv = os.path.join(outdir, "phase4_patterns.tsv")
        stats_tsv = os.path.join(outdir, "phase4_pattern_stats.tsv")

        write_tsv(features_df, features_tsv)
        write_tsv(patterns_df, patterns_tsv)
        write_tsv(stats_df, stats_tsv)

        assignment_digest = sha256_bytes(
            "\n".join(
                (
                    patterns_df[["wallet", "window", "lens_id", "pattern_id", "is_noise"]]
                    .fillna("")
                    .astype(str)
                    .agg("\t".join, axis=1)
                )
            ).encode("utf-8")
        )

        manifest = {
            "phase4_features_norm.tsv": sha256_file(features_tsv),
            "phase4_patterns.tsv": sha256_file(patterns_tsv),
            "phase4_pattern_stats.tsv": sha256_file(stats_tsv),
            "pattern_assignment_digest": assignment_digest,
        }

        manifest_path = os.path.join(outdir, "phase4_4_run_manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, sort_keys=True)

        print("\nSUMMARY")
        print("[INFO] Lens C using N9 only; N10 remains NULL (not provable).")
        for key in sorted(summary.keys()):
            info = summary[key]
            top_sizes = ", ".join(str(x) for x in info["top5_sizes"]) if info["top5_sizes"] else ""
            gmm_info = ""
            if "gmm_k" in info:
                gmm_info = f" gmm_k={info['gmm_k']} gmm_skipped_k={info['gmm_skipped_k']}"
            print(
                f"{key} wallets_total={info['wallets_total']} clustered={info['clustered']} noise={info['noise']} patterns_found={info['patterns_found']} top5=[{top_sizes}]{gmm_info}"
            )

        print("\nDIGESTS")
        for k in sorted(manifest.keys()):
            print(f"{k}: {manifest[k]}")

    except HardError as exc:
        print(f"HARD ERROR: {exc}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
