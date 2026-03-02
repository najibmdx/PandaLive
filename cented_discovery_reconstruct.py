#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Dict, List, Optional, Sequence, Tuple


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Cented discovery-layer reconstruction from SQLite")
    p.add_argument("--db", default="masterwalletsdb.db", help="Path to SQLite DB")
    p.add_argument("--outdir", default="outdir", help="Output directory")
    p.add_argument("--cented-wallet", default="Cented", help="Cented wallet id/address/label to match")
    return p.parse_args()


def qident(x: str) -> str:
    return '"' + x.replace('"', '""') + '"'


def get_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name ASC").fetchall()
    return [r[0] for r in rows]


def get_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({qident(table)})").fetchall()
    return [r[1] for r in rows]


def find_first(columns: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    lc = {c.lower(): c for c in columns}
    for c in candidates:
        if c.lower() in lc:
            return lc[c.lower()]
    return None


def write_tsv(path: Path, header: List[str], rows: List[List[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(header)
        for row in rows:
            w.writerow(row)


def bucket_entry(delta: Optional[int]) -> Optional[str]:
    if delta is None:
        return None
    if delta <= 5:
        return "FAST"
    if delta <= 60:
        return "MID"
    return "SLOW"


def effect_size_numeric(wins: List[float], losses: List[float]) -> Optional[float]:
    if not wins or not losses:
        return None
    mw, ml = median(wins), median(losses)
    all_vals = wins + losses
    if len(all_vals) < 2:
        return None
    mu = sum(all_vals) / len(all_vals)
    var = sum((x - mu) ** 2 for x in all_vals) / max(1, (len(all_vals) - 1))
    sd = math.sqrt(var)
    if sd == 0:
        return None
    return (mw - ml) / sd


def odds_ratio_boolean(win_true: int, win_false: int, loss_true: int, loss_false: int) -> float:
    # Haldane-Anscombe correction for stability
    a, b, c, d = win_true + 0.5, win_false + 0.5, loss_true + 0.5, loss_false + 0.5
    return (a * d) / (b * c)


def main() -> None:
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    missing: List[str] = []

    conn = None
    tables = []
    db_path = Path(args.db)
    if db_path.exists():
        db_uri = f"file:{db_path.resolve()}?mode=ro"
        conn = sqlite3.connect(db_uri, uri=True)
        conn.row_factory = sqlite3.Row
        tables = get_tables(conn)
    else:
        missing.append(f"Database file not found: {args.db}. Needed: masterwalletsdb.db or explicit --db path.")

    flow_table = "wallet_token_flow" if "wallet_token_flow" in tables else None
    tx_table = "tx" if "tx" in tables else None

    master_rows: List[List[object]] = []
    trigger_rows: List[List[object]] = []
    deployer_rows: List[List[object]] = []
    routing_rows: List[List[object]] = []
    ranking_rows: List[List[object]] = []

    if conn is None:
        pass
    elif flow_table is None:
        missing.append("wallet_token_flow table missing; cannot derive Cented per-mint entry/exit metrics.")
    else:
        fcols = get_columns(conn, flow_table)
        wcol = find_first(fcols, ["scan_wallet", "wallet", "owner_wallet", "trader", "wallet_address"])
        mcol = find_first(fcols, ["token_mint", "mint", "token_address"])
        tcol = find_first(fcols, ["block_time", "ts", "timestamp", "time"])
        dcol = find_first(fcols, ["flow_direction", "direction", "side"])
        sigcol = find_first(fcols, ["signature", "tx_sig", "txid"])

        if not all([wcol, mcol, tcol, dcol]):
            missing.append("wallet_token_flow columns insufficient for first_buy/first_sell derivation.")
        else:
            q = (
                f"SELECT {qident(wcol)} AS wallet, {qident(mcol)} AS mint, {qident(tcol)} AS bt, "
                f"{qident(dcol)} AS dir, "
                + (f"{qident(sigcol)} AS sig " if sigcol else "NULL AS sig ")
                + f"FROM {qident(flow_table)} WHERE {qident(wcol)} = ? ORDER BY {qident(mcol)} ASC, {qident(tcol)} ASC, sig ASC"
            )
            rows = conn.execute(q, (args.cented_wallet,)).fetchall()

            by_mint: Dict[str, List[sqlite3.Row]] = defaultdict(list)
            for r in rows:
                by_mint[str(r["mint"])] .append(r)

            mint_features: Dict[str, Dict[str, object]] = {}
            first_buy_sigs: Dict[str, Optional[str]] = {}

            for mint in sorted(by_mint):
                events = by_mint[mint]
                in_times = [int(e["bt"]) for e in events if str(e["dir"]).lower() in {"in", "buy"} and e["bt"] is not None]
                out_times = [int(e["bt"]) for e in events if str(e["dir"]).lower() in {"out", "sell"} and e["bt"] is not None]
                first_buy_ts = min(in_times) if in_times else None
                first_sell_ts = min([t for t in out_times if first_buy_ts is None or t >= first_buy_ts], default=None)
                hold = (first_sell_ts - first_buy_ts) if (first_sell_ts is not None and first_buy_ts is not None) else None
                delta = hold

                clips = 0
                partial_exit_flag = 0
                if first_buy_ts is not None:
                    clips = sum(1 for e in events if str(e["dir"]).lower() in {"out", "sell"} and int(e["bt"]) >= first_buy_ts)
                    partial_exit_flag = 1 if clips > 1 else 0

                # net_sol from tx if possible
                net_sol = None
                if tx_table is not None and sigcol:
                    tx_cols = get_columns(conn, tx_table)
                    scan_wallet_col = find_first(tx_cols, ["scan_wallet", "wallet"])
                    tx_sig_col = find_first(tx_cols, ["signature", "tx_sig", "txid"])
                    sol_col = find_first(tx_cols, ["wallet_delta_sol", "delta_sol", "net_sol", "sol_delta"])
                    if scan_wallet_col and tx_sig_col and sol_col:
                        sigs = [str(e["sig"]) for e in events if e["sig"]]
                        if sigs:
                            ph = ",".join("?" for _ in sigs)
                            q_sol = (
                                f"SELECT SUM(COALESCE({qident(sol_col)},0)) AS s FROM {qident(tx_table)} "
                                f"WHERE {qident(scan_wallet_col)}=? AND {qident(tx_sig_col)} IN ({ph})"
                            )
                            rr = conn.execute(q_sol, (args.cented_wallet, *sigs)).fetchone()
                            net_sol = float(rr["s"]) if rr and rr["s"] is not None else None
                if net_sol is None:
                    missing.append("net_sol unavailable without tx SOL delta column for Cented wallet.")

                bucket = bucket_entry(delta)
                fast_label = None
                if bucket == "FAST" and net_sol is not None:
                    fast_label = "FAST_WIN" if net_sol > 0 else "FAST_LOSS"

                fb_sig = None
                if first_buy_ts is not None:
                    fb = [e for e in events if str(e["dir"]).lower() in {"in", "buy"} and int(e["bt"]) == first_buy_ts and e["sig"]]
                    if fb:
                        fb_sig = sorted(str(e["sig"]) for e in fb)[0]

                first_buy_sigs[mint] = fb_sig
                mint_features[mint] = {
                    "mint": mint,
                    "entry_delta_seconds": delta,
                    "first_buy_ts": first_buy_ts,
                    "first_sell_ts": first_sell_ts,
                    "hold_time_seconds": hold,
                    "net_sol": net_sol,
                    "clips_count": clips,
                    "partial_exit_flag": partial_exit_flag,
                    "entry_bucket": bucket,
                    "fast_outcome": fast_label,
                }

            # Birth timing markers (best-effort from swaps)
            swaps_table = "swaps" if "swaps" in tables else None
            s_mcol = s_tcol = s_prog = s_sig = None
            if swaps_table:
                scols = get_columns(conn, swaps_table)
                s_mcol = find_first(scols, ["token_mint", "mint", "base_mint", "token_address"])
                s_tcol = find_first(scols, ["block_time", "ts", "timestamp"])
                s_prog = find_first(scols, ["program_id", "program", "amm_program_id"])
                s_sig = find_first(scols, ["signature", "tx_sig", "txid"])

            deployer_programs: Dict[str, Counter] = defaultdict(Counter)
            deployer_wallet_by_mint: Dict[str, Optional[str]] = {}
            authority_by_mint: Dict[str, Tuple[Optional[str], Optional[str]]] = {}

            # optional token metadata table
            mint_meta_tbl = None
            for t in tables:
                if t.lower() in {"token_mint_intel", "mint_intel", "token_metadata", "mint_metadata"}:
                    mint_meta_tbl = t
                    break
            mint_meta = {}
            if mint_meta_tbl:
                mcols = get_columns(conn, mint_meta_tbl)
                mmint = find_first(mcols, ["token_mint", "mint"])
                mauth = find_first(mcols, ["mint_authority", "mint_authority_state"])
                fauth = find_first(mcols, ["freeze_authority", "freeze_authority_state"])
                if mmint:
                    q_meta = f"SELECT {qident(mmint)} AS mint, " + (f"{qident(mauth)} AS ma, " if mauth else "NULL AS ma, ") + (f"{qident(fauth)} AS fa " if fauth else "NULL AS fa ") + f"FROM {qident(mint_meta_tbl)}"
                    for r in conn.execute(q_meta):
                        mint_meta[str(r["mint"])] = (r["ma"], r["fa"])

            for mint in sorted(mint_features):
                f = mint_features[mint]
                first_buy_ts = f["first_buy_ts"]

                pool_init_ts = first_lp_add_ts = first_swap_ts = None
                deployer = None
                if swaps_table and s_mcol and s_tcol:
                    q_sw = f"SELECT * FROM {qident(swaps_table)} WHERE {qident(s_mcol)}=? ORDER BY {qident(s_tcol)} ASC"
                    sw = conn.execute(q_sw, (mint,)).fetchall()
                    if sw:
                        first_swap_ts = int(sw[0][s_tcol]) if sw[0][s_tcol] is not None else None
                        # Heuristic markers unavailable in generic swaps-only DB
                        if pool_init_ts is None:
                            pool_init_ts = None
                        if first_lp_add_ts is None:
                            first_lp_add_ts = None
                        # Deployer/program from earliest available row
                        for row in sw[:3]:
                            if s_prog and row[s_prog]:
                                deployer_programs[mint][str(row[s_prog])] += 1
                        if "owner" in row.keys() and sw[0]["owner"]:
                            deployer = str(sw[0]["owner"])

                if deployer is None:
                    deployer = None
                deployer_wallet_by_mint[mint] = deployer
                authority_by_mint[mint] = mint_meta.get(mint, (None, None))

                d_pool = (first_buy_ts - pool_init_ts) if (first_buy_ts is not None and pool_init_ts is not None) else None
                d_lp = (first_buy_ts - first_lp_add_ts) if (first_buy_ts is not None and first_lp_add_ts is not None) else None
                d_swap = (first_buy_ts - first_swap_ts) if (first_buy_ts is not None and first_swap_ts is not None) else None

                master_rows.append([
                    mint,
                    f["entry_delta_seconds"],
                    f["first_buy_ts"],
                    f["first_sell_ts"],
                    f["hold_time_seconds"],
                    f["net_sol"],
                    f["clips_count"],
                    f["partial_exit_flag"],
                    f["entry_bucket"],
                    f["fast_outcome"],
                ])

                trigger_rows.append([
                    mint,
                    pool_init_ts,
                    first_lp_add_ts,
                    first_swap_ts,
                    d_pool,
                    d_lp,
                    d_swap,
                ])

                # routing footprint from tx program ids if present
                route_programs: List[str] = []
                if tx_table and first_buy_sigs.get(mint):
                    tx_cols = get_columns(conn, tx_table)
                    sigc = find_first(tx_cols, ["signature", "tx_sig", "txid"])
                    swc = find_first(tx_cols, ["scan_wallet", "wallet"])
                    pcol = find_first(tx_cols, ["program_ids", "invoked_program_ids", "program_id_path"])
                    if sigc and swc and pcol:
                        rr = conn.execute(
                            f"SELECT {qident(pcol)} AS p FROM {qident(tx_table)} WHERE {qident(swc)}=? AND {qident(sigc)}=? LIMIT 1",
                            (args.cented_wallet, first_buy_sigs[mint]),
                        ).fetchone()
                        if rr and rr["p"]:
                            route_programs = [x.strip() for x in str(rr["p"]).split(",") if x.strip()]

                known = {
                    "jupiter": any("jup" in p.lower() for p in route_programs),
                    "raydium": any("ray" in p.lower() for p in route_programs),
                    "orca": any("orca" in p.lower() for p in route_programs),
                }
                routing_rows.append([
                    mint,
                    first_buy_sigs.get(mint),
                    "|".join(route_programs) if route_programs else None,
                    int(known["jupiter"]),
                    int(known["raydium"]),
                    int(known["orca"]),
                ])

            # deployer history
            mints_by_dep: Dict[str, List[str]] = defaultdict(list)
            for m, d in deployer_wallet_by_mint.items():
                if d:
                    mints_by_dep[d].append(m)

            fast_outcome = {r[0]: r[9] for r in master_rows}
            for mint in sorted(mint_features):
                dep = deployer_wallet_by_mint[mint]
                dep_mints = sorted(mints_by_dep.get(dep, [])) if dep else []
                dep_touched = len(dep_mints)
                dep_fast_total = sum(1 for mm in dep_mints if fast_outcome.get(mm) in {"FAST_WIN", "FAST_LOSS"})
                dep_fast_wins = sum(1 for mm in dep_mints if fast_outcome.get(mm) == "FAST_WIN")
                dep_fast_win_rate = (dep_fast_wins / dep_fast_total) if dep_fast_total else None
                progs = sorted(deployer_programs[mint].keys())
                ma, fa = authority_by_mint[mint]
                deployer_rows.append([
                    mint,
                    dep,
                    "|".join(progs) if progs else None,
                    ma,
                    fa,
                    dep_touched if dep else None,
                    dep_touched if dep else None,
                    dep_fast_win_rate,
                ])

            # ranking FAST_WIN vs FAST_LOSS
            master_map = {r[0]: r for r in master_rows}
            fwl = {m: master_map[m][9] for m in master_map}
            mints_fast = sorted([m for m, o in fwl.items() if o in {"FAST_WIN", "FAST_LOSS"}])

            boolean_features: Dict[str, Dict[str, int]] = {}
            numeric_data: Dict[str, Tuple[List[float], List[float]]] = {}

            def get_outcome(m: str) -> Optional[str]:
                return fwl.get(m)

            for m in mints_fast:
                outcome = get_outcome(m)
                if outcome is None:
                    continue
                row_d = next((r for r in deployer_rows if r[0] == m), None)
                row_t = next((r for r in trigger_rows if r[0] == m), None)
                row_r = next((r for r in routing_rows if r[0] == m), None)
                bool_vals = {
                    "partial_exit_flag": bool(master_map[m][7]),
                    "has_jupiter": bool(row_r[3]) if row_r else False,
                    "has_raydium": bool(row_r[4]) if row_r else False,
                    "has_orca": bool(row_r[5]) if row_r else False,
                    "mint_authority_present": (row_d[3] not in (None, "", "burned", "none")) if row_d else False,
                    "freeze_authority_present": (row_d[4] not in (None, "", "burned", "none")) if row_d else False,
                }
                for fname, val in bool_vals.items():
                    d = boolean_features.setdefault(fname, {"win_true": 0, "win_false": 0, "loss_true": 0, "loss_false": 0})
                    if outcome == "FAST_WIN":
                        d["win_true" if val else "win_false"] += 1
                    else:
                        d["loss_true" if val else "loss_false"] += 1

                numeric_vals = {
                    "hold_time_seconds": master_map[m][4],
                    "clips_count": master_map[m][6],
                    "d_first_swap": row_t[6] if row_t else None,
                }
                for fname, val in numeric_vals.items():
                    if val is None:
                        continue
                    wins, losses = numeric_data.setdefault(fname, ([], []))
                    if outcome == "FAST_WIN":
                        wins.append(float(val))
                    else:
                        losses.append(float(val))

            for fname, c in boolean_features.items():
                cov = (c["win_true"] + c["loss_true"] + c["win_false"] + c["loss_false"]) / max(1, len(mints_fast))
                orat = odds_ratio_boolean(c["win_true"], c["win_false"], c["loss_true"], c["loss_false"])
                ranking_rows.append([fname, "boolean", round(cov, 6), None, None, round(orat, 6), None])

            for fname, (wins, losses) in numeric_data.items():
                mw = median(wins) if wins else None
                ml = median(losses) if losses else None
                mdiff = (mw - ml) if (mw is not None and ml is not None) else None
                es = effect_size_numeric(wins, losses)
                cov = (len(wins) + len(losses)) / max(1, len(mints_fast))
                ranking_rows.append([fname, "numeric", round(cov, 6), mw, ml, mdiff, es])

            ranking_rows.sort(key=lambda r: ((abs(r[6]) if r[6] is not None else abs(r[5]) if r[5] is not None else 0), r[2], r[0]), reverse=True)
            ranking_rows = ranking_rows[:15]

    if conn is not None:
        conn.close()

    master_header = [
        "mint", "entry_delta_seconds", "first_buy_ts", "first_sell_ts", "hold_time_seconds", "net_sol", "clips_count", "partial_exit_flag", "entry_bucket", "fast_outcome"
    ]
    trigger_header = ["mint", "pool_init_ts", "first_lp_add_ts", "first_swap_ts", "d_pool_init", "d_lp_add", "d_first_swap"]
    deployer_header = ["mint", "deployer_wallet", "creation_program_ids", "mint_authority_state", "freeze_authority_state", "deployer_mints_launched_168h", "deployer_cented_touched_168h", "deployer_fast_win_rate_168h"]
    routing_header = ["mint", "first_buy_signature", "invoked_program_ids_ordered", "has_jupiter", "has_raydium", "has_orca"]
    ranking_header = ["feature", "feature_type", "coverage", "fast_win_median", "fast_loss_median", "median_diff_or_odds_ratio", "effect_size"]

    write_tsv(outdir / "cented_discovery_master.tsv", master_header, sorted(master_rows, key=lambda r: r[0]))
    write_tsv(outdir / "cented_trigger_deltas.tsv", trigger_header, sorted(trigger_rows, key=lambda r: r[0]))
    write_tsv(outdir / "cented_deployer_features.tsv", deployer_header, sorted(deployer_rows, key=lambda r: r[0]))
    write_tsv(outdir / "cented_routing_footprint.tsv", routing_header, sorted(routing_rows, key=lambda r: r[0]))
    write_tsv(outdir / "cented_fast_win_loss_feature_ranking.tsv", ranking_header, ranking_rows)

    readme = outdir / "README.txt"
    with readme.open("w", encoding="utf-8") as f:
        f.write("cented_discovery_master.tsv\tPer-mint Cented trade summary from wallet_token_flow rows for --cented-wallet.\n")
        f.write("cented_trigger_deltas.tsv\tPer-mint timing markers and deltas to first_buy_ts from earliest detectable events.\n")
        f.write("cented_deployer_features.tsv\tPer-mint deployer/program/authority/deployer-history features (best effort from available tables).\n")
        f.write("cented_routing_footprint.tsv\tProgram invocation footprint for Cented first-buy tx by mint.\n")
        f.write("cented_fast_win_loss_feature_ranking.tsv\tTop 15 features separating FAST_WIN vs FAST_LOSS with coverage.\n")
        f.write("\nMissing derivations / required tables:\n")
        if missing:
            for m in sorted(set(missing)):
                f.write(f"- {m}\n")
        else:
            f.write("- None\n")


if __name__ == "__main__":
    main()
