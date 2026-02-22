#!/usr/bin/env python3
"""
README / Usage
--------------
Deterministic Solana memecoin archetype evaluator (intelligence-only TSV outputs).

CLI:
    python panda_archetype_eval.py --db PATH_TO_DB --outdir OUTDIR [--mint MINT_ADDR]

Notes:
- Auto-discovers best-fit SQLite tables by column signatures (no hardcoded table names).
- Gracefully degrades if feature categories are missing; marks them unavailable in logic.
- Deterministic outputs for same DB: sorted query order, stable mint iteration, UTF-8 TSV with \n newlines,
  and SHA256 file digests in digests.txt.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import sqlite3
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

CONFIG: Dict[str, Any] = {
    "early_minutes": 10,
    "silent_gap_minutes": 10,
    "top_k_wallets": 10,
    "thin_liquidity_percentile": 20,
    "liquidity_vol_window": 12,
    "min_conf": 35,
    "max_conf": 95,
    "confidence_bucket": 5,
    "weights": {
        "P_PLUS": {
            "whale_inflow": 1.4,
            "early_entry": 1.1,
            "wallet_velocity": 1.0,
            "liquidity_stability": 0.8,
        },
        "P_MINUS": {
            "whale_outflow": 1.4,
            "early_exit": 1.1,
            "top_wallet_outflow": 1.0,
            "liquidity_deterioration": 0.9,
            "concentration_spike": 0.8,
        },
        "FRAGILITY": {
            "timing_cluster": 1.1,
            "repeated_size": 1.0,
            "clique_density": 1.1,
            "lp_remove": 0.9,
            "thin_liquidity": 1.2,
            "authority_flags": 1.0,
            "concentration_spike": 0.8,
        },
    },
    "tempo_persistence": {
        "low": {"warning_on": 3, "warning_off": 2, "shout_on": 6, "shout_off": 4},
        "mid": {"warning_on": 2, "warning_off": 2, "shout_on": 4, "shout_off": 3},
        "high": {"warning_on": 2, "warning_off": 1, "shout_on": 3, "shout_off": 2},
    },
    "direction_dom_margin": 0.12,
    "risk_warning": 0.55,
    "risk_shout": 0.75,
    "ghost_decay_steps": 5,
    "liq_floor_usd": 3000.0,
    "authority_risk_component": 1.0,
    "low_liquidity_risk_component": 1.0,
}


@dataclass(frozen=True)
class TableInfo:
    name: str
    columns: Tuple[str, ...]


@dataclass(frozen=True)
class CategoryTable:
    table: Optional[str]
    mapping: Dict[str, str]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Evaluate Solana memecoin archetypes from SQLite")
    p.add_argument("--db", required=True, help="Path to SQLite database")
    p.add_argument("--outdir", required=True, help="Output directory for TSV/CSV files")
    p.add_argument("--mint", help="Optional mint address to restrict processing")
    p.add_argument("--silent-gap-minutes", type=int, default=CONFIG["silent_gap_minutes"])
    p.add_argument("--early-minutes", type=int, default=CONFIG["early_minutes"])
    return p.parse_args()


def normalize_col(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def get_tables(conn: sqlite3.Connection) -> List[TableInfo]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    out: List[TableInfo] = []
    for (name,) in rows:
        cols = conn.execute(f"PRAGMA table_info('{name}')").fetchall()
        normalized = tuple(normalize_col(c[1]) for c in cols)
        out.append(TableInfo(name=name, columns=normalized))
    return out


def score_table(columns: Sequence[str], wanted: Dict[str, Sequence[str]]) -> Tuple[int, Dict[str, str]]:
    colset = set(columns)
    mapping: Dict[str, str] = {}
    score = 0
    for logical, aliases in wanted.items():
        for a in aliases:
            if a in colset:
                mapping[logical] = a
                score += 3
                break
        else:
            partial = [c for c in columns if any(k in c for k in aliases)]
            if partial:
                mapping[logical] = sorted(partial)[0]
                score += 1
    return score, mapping


def detect_categories(tables: Sequence[TableInfo]) -> Dict[str, CategoryTable]:
    signatures: Dict[str, Dict[str, Sequence[str]]] = {
        "events": {
            "ts": ("timestamp", "ts", "time", "block_time", "slot_time", "event_time"),
            "wallet": ("wallet", "scan_wallet", "owner", "address", "trader", "user", "signer"),
            "mint": ("mint", "token_mint", "token", "mint_address", "ca"),
            "amount": ("sol_amount_lamports", "token_amount_raw", "amount", "qty", "quantity", "token_amount", "size", "volume"),
            "side": ("sol_direction", "side", "direction", "action", "type"),
            "txid": ("txid", "signature", "tx_hash", "transaction", "txn"),
            "sol_direction": ("sol_direction",),
            "sol_amount": ("sol_amount_lamports",),
            "has_sol_leg": ("has_sol_leg",),
            "dex": ("dex",),
            "in_mint": ("in_mint",),
            "out_mint": ("out_mint",),
        },
        "clusters": {
            "wallet_a": ("wallet_a", "from_wallet", "src_wallet", "source_wallet", "node_a"),
            "wallet_b": ("wallet_b", "to_wallet", "dst_wallet", "target_wallet", "node_b"),
            "weight": ("weight", "score", "edge_weight", "strength"),
            "mint": ("mint", "token_mint", "token"),
        },
        "metadata": {
            "mint": ("mint", "token_mint", "mint_address", "token"),
            "mint_authority": ("mint_authority", "authority_mint", "has_mint_authority"),
            "freeze_authority": ("freeze_authority", "authority_freeze", "has_freeze_authority"),
            "mutable": ("is_mutable", "mutable", "metadata_mutable"),
        },
        "lp_events": {
            "ts": ("timestamp", "ts", "time", "block_time", "event_time"),
            "mint": ("mint", "token_mint", "token"),
            "action": ("action", "side", "type", "event"),
            "liquidity": ("liquidity", "pool_liquidity", "tvl", "lp_liquidity"),
            "amount": ("amount", "qty", "token_amount", "delta"),
        },
        "security": {
            "mint": ("mint", "token_mint", "mint_address", "token"),
            "mint_authority": ("mint_authority",),
            "freeze_authority": ("freeze_authority",),
            "token_program": ("token_program",),
            "decimals": ("decimals",),
            "supply_raw": ("supply_raw",),
            "last_updated": ("last_updated",),
        },
        "liquidity": {
            "mint": ("mint", "token_mint", "mint_address", "token"),
            "liquidity_usd": ("liquidity_usd",),
            "primary_pool": ("primary_pool",),
            "lp_locked_pct": ("lp_locked_pct",),
            "lp_lock_flag": ("lp_lock_flag",),
            "source": ("source",),
            "last_updated": ("last_updated",),
        },
        "holders": {
            "ts": ("timestamp", "ts", "time", "snapshot_time", "block_time"),
            "mint": ("mint", "token_mint", "token"),
            "wallet": ("wallet", "holder", "owner", "address"),
            "balance": ("balance", "amount", "token_balance", "holding"),
            "holder_count": ("holder_count", "holders", "num_holders"),
        },
    }
    result: Dict[str, CategoryTable] = {}
    for category, wanted in signatures.items():
        best_score = -1
        best_table: Optional[str] = None
        best_map: Dict[str, str] = {}
        for t in tables:
            score, mapping = score_table(t.columns, wanted)
            if category == "events":
                required = {"ts", "wallet", "mint"}
                if not required.issubset(set(mapping.keys())):
                    continue
                swap_discriminators = {"sol_direction", "sol_amount", "has_sol_leg", "dex", "in_mint", "out_mint"}
                if not swap_discriminators.intersection(set(mapping.keys())):
                    continue
                # hard preference for complete bindings over partial matches
                score += 100
            if score > best_score or (score == best_score and best_table and t.name < best_table):
                best_score, best_table, best_map = score, t.name, mapping
        min_needed = 6 if category == "events" else 4
        if best_score >= min_needed:
            result[category] = CategoryTable(best_table, best_map)
        else:
            result[category] = CategoryTable(None, {})
    return result


def to_epoch(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = int(value)
        if v > 10_000_000_000:
            return v // 1000
        return v
    s = str(value).strip()
    if not s:
        return None
    if s.isdigit():
        return to_epoch(int(s))
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError:
        return None


def ts_iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def build_events_query(table: CategoryTable, mint_filter: Optional[str]) -> Tuple[str, List[Any]]:
    m = table.mapping
    sel = [
        f"{m.get('ts', 'NULL')} AS ts",
        f"{m.get('wallet', 'NULL')} AS wallet",
        f"{m.get('mint', 'NULL')} AS mint",
        f"{m.get('amount', 'NULL')} AS amount",
        f"{m.get('side', 'NULL')} AS side",
        f"{m.get('txid', 'NULL')} AS txid",
    ]
    sql = f"SELECT {', '.join(sel)} FROM {table.table}"
    params: List[Any] = []
    if mint_filter and "mint" in m:
        sql += f" WHERE {m['mint']} = ?"
        params.append(mint_filter)
    order_parts = [m.get("mint"), m.get("ts"), m.get("wallet"), m.get("txid")]
    order = [x for x in order_parts if x]
    if order:
        sql += " ORDER BY " + ", ".join(order)
    return sql, params


def fetch_metadata(conn: sqlite3.Connection, table: CategoryTable) -> Dict[str, Dict[str, int]]:
    if not table.table:
        return {}
    m = table.mapping
    if "mint" not in m:
        return {}
    cols = [m["mint"]]
    for k in ("mint_authority", "freeze_authority", "mutable"):
        cols.append(m.get(k, "NULL"))
    sql = f"SELECT {', '.join(cols)} FROM {table.table} ORDER BY {m['mint']}"
    out: Dict[str, Dict[str, int]] = {}
    for row in conn.execute(sql):
        mint = str(row[0]) if row[0] is not None else ""
        if not mint:
            continue
        mint_auth = 1 if row[1] not in (None, "", 0, "0", False) else 0
        freeze_auth = 1 if row[2] not in (None, "", 0, "0", False) else 0
        mutable = 1 if row[3] not in (None, "", 0, "0", False) else 0
        out[mint] = {
            "mint_authority_active": mint_auth,
            "freeze_authority_active": freeze_auth,
            "mutable_metadata": mutable,
        }
    return out




def fetch_security_backfill(conn: sqlite3.Connection, table: CategoryTable) -> Dict[str, Dict[str, int]]:
    if not table.table:
        return {}
    m = table.mapping
    if "mint" not in m:
        return {}
    cols = [m["mint"], m.get("mint_authority", "NULL"), m.get("freeze_authority", "NULL")]
    sql = f"SELECT {', '.join(cols)} FROM {table.table} ORDER BY {m['mint']}"
    out: Dict[str, Dict[str, int]] = {}
    for row in conn.execute(sql):
        mint = str(row[0]) if row[0] is not None else ""
        if not mint:
            continue
        mint_auth = 1 if row[1] not in (None, "", 0, "0", False) else 0
        freeze_auth = 1 if row[2] not in (None, "", 0, "0", False) else 0
        out[mint] = {
            "mint_authority_present": mint_auth,
            "freeze_authority_present": freeze_auth,
        }
    return out


def fetch_liquidity_backfill(conn: sqlite3.Connection, table: CategoryTable) -> Dict[str, Dict[str, Optional[float]]]:
    if not table.table:
        return {}
    m = table.mapping
    if "mint" not in m:
        return {}
    cols = [m["mint"], m.get("liquidity_usd", "NULL")]
    sql = f"SELECT {', '.join(cols)} FROM {table.table} ORDER BY {m['mint']}"
    out: Dict[str, Dict[str, Optional[float]]] = {}
    for row in conn.execute(sql):
        mint = str(row[0]) if row[0] is not None else ""
        if not mint:
            continue
        liq_usd = float(row[1]) if row[1] not in (None, "") else None
        out[mint] = {"liquidity_usd": liq_usd}
    return out
def fetch_liquidity_events(conn: sqlite3.Connection, table: CategoryTable, mint_filter: Optional[str]) -> Dict[str, List[Tuple[int, str, float]]]:
    if not table.table:
        return {}
    m = table.mapping
    if "mint" not in m or "ts" not in m:
        return {}
    fields = [m["ts"], m["mint"], m.get("action", "NULL"), m.get("liquidity", "NULL"), m.get("amount", "NULL")]
    sql = f"SELECT {', '.join(fields)} FROM {table.table}"
    params: List[Any] = []
    if mint_filter:
        sql += f" WHERE {m['mint']} = ?"
        params = [mint_filter]
    sql += f" ORDER BY {m['ts']}, {m['mint']}"
    out: Dict[str, List[Tuple[int, str, float]]] = defaultdict(list)
    for r in conn.execute(sql, params):
        epoch = to_epoch(r[0])
        mint = str(r[1]) if r[1] is not None else ""
        if epoch is None or not mint:
            continue
        action = str(r[2]).lower() if r[2] is not None else ""
        liq = float(r[3]) if r[3] not in (None, "") else (float(r[4]) if r[4] not in (None, "") else 0.0)
        out[mint].append((epoch, action, liq))
    return out


def fetch_holder_snapshots(conn: sqlite3.Connection, table: CategoryTable, mint_filter: Optional[str]) -> Dict[str, Dict[int, List[float]]]:
    if not table.table:
        return {}
    m = table.mapping
    if not ({"mint", "ts"} <= set(m)):
        return {}
    fields = [m["ts"], m["mint"], m.get("wallet", "NULL"), m.get("balance", "NULL"), m.get("holder_count", "NULL")]
    sql = f"SELECT {', '.join(fields)} FROM {table.table}"
    params: List[Any] = []
    if mint_filter:
        sql += f" WHERE {m['mint']} = ?"
        params = [mint_filter]
    sql += f" ORDER BY {m['ts']}, {m['mint']}, {m.get('wallet', m['mint'])}"
    out: Dict[str, Dict[int, List[float]]] = defaultdict(lambda: defaultdict(list))
    for r in conn.execute(sql, params):
        epoch = to_epoch(r[0])
        mint = str(r[1]) if r[1] is not None else ""
        if epoch is None or not mint:
            continue
        balance = float(r[3]) if r[3] not in (None, "") else 0.0
        out[mint][epoch].append(balance)
    return out


def gini(values: Sequence[float]) -> float:
    vals = [v for v in values if v >= 0]
    if not vals:
        return 0.0
    vals.sort()
    n = len(vals)
    s = sum(vals)
    if s == 0:
        return 0.0
    cum = sum((i + 1) * v for i, v in enumerate(vals))
    return (2.0 * cum) / (n * s) - (n + 1) / n


def quantize_conf(v: float) -> int:
    v = max(CONFIG["min_conf"], min(CONFIG["max_conf"], v))
    b = CONFIG["confidence_bucket"]
    return int(round(v / b) * b)


def compute_conf(aligned: int, contrad: int, persistence: int) -> int:
    raw = 45 + aligned * 8 + min(20, persistence * 3) - contrad * 7
    return quantize_conf(float(raw))


def mean_abs_dev(vals: Sequence[float]) -> float:
    if not vals:
        return 0.0
    avg = sum(vals) / len(vals)
    return sum(abs(v - avg) for v in vals) / len(vals)


def choose_step_size(samples: Sequence[int]) -> int:
    if len(samples) < 3:
        return 60
    deltas = [b - a for a, b in zip(samples, samples[1:]) if b > a]
    if not deltas:
        return 60
    return 20 if min(deltas) <= 20 else 60


def classify_tempo(event_count: int) -> str:
    if event_count >= 24:
        return "high"
    if event_count >= 8:
        return "mid"
    return "low"


def set_level(state: str, pos_count: int, neg_count: int, tempo: str) -> str:
    cfg = CONFIG["tempo_persistence"][tempo]
    if state == "SHOUT":
        return "SHOUT" if pos_count >= cfg["shout_off"] else ("WARNING" if pos_count >= cfg["warning_on"] else "NONE")
    if state == "WARNING":
        if pos_count >= cfg["shout_on"]:
            return "SHOUT"
        return "WARNING" if pos_count >= cfg["warning_off"] else "NONE"
    return "SHOUT" if pos_count >= cfg["shout_on"] else ("WARNING" if pos_count >= cfg["warning_on"] else "NONE")


def dominant_dir(p_plus: float, p_minus: float) -> str:
    if p_plus <= 0 and p_minus <= 0:
        return "NEUTRAL"
    margin = CONFIG["direction_dom_margin"]
    if p_plus > p_minus * (1 + margin):
        return "PUMP"
    if p_minus > p_plus * (1 + margin):
        return "DUMP"
    return "NEUTRAL"


def resolve_archetype(signals: Dict[str, bool], current: str, can_flip: bool) -> str:
    priority = [
        ("The Vampire", "vampire"),
        ("The Time Bomb", "time_bomb"),
        ("The Bot Farm", "bot_farm"),
        ("The Phoenix", "phoenix"),
        ("The Ghost", "ghost"),
        ("The Accumulator", "accumulator"),
        ("The Distributor", "distributor"),
    ]
    chosen = current
    for label, key in priority:
        if signals.get(key, False):
            chosen = label
            break
    if not can_flip:
        return current
    return chosen


def short_reason(sig: Dict[str, bool]) -> str:
    parts = []
    if sig.get("vampire"):
        parts.append("G3 liquidity extraction + G1 outflow")
    if sig.get("time_bomb"):
        parts.append("G5 authority risk + G4 concentration")
    if sig.get("bot_farm"):
        parts.append("G2 coordination burst")
    if sig.get("phoenix"):
        parts.append("G6 resurrection after silence")
    if sig.get("ghost"):
        parts.append("G6 prolonged inactivity")
    if sig.get("accumulator"):
        parts.append("G1 inflow/entry persistence")
    if sig.get("distributor"):
        parts.append("G1 outflow/exit persistence")
    return "; ".join(parts[:2]) if parts else "mixed low-signal state"


def evaluate_mint(
    mint: str,
    rows: Sequence[Tuple[int, str, float, str, str]],
    metadata: Dict[str, Dict[str, int]],
    security_backfill: Dict[str, Dict[str, int]],
    liquidity_backfill: Dict[str, Dict[str, Optional[float]]],
    liq_events: Dict[str, List[Tuple[int, str, float]]],
    holder_snapshots: Dict[str, Dict[int, List[float]]],
    args: argparse.Namespace,
) -> Tuple[List[List[str]], List[str], Counter, int, List[List[str]]]:
    if not rows:
        return [], [], Counter(), 0, []
    times = [r[0] for r in rows]
    step = choose_step_size(times)
    first_ts = times[0]
    last_ts = times[-1]

    wallet_first_seen: Dict[str, int] = {}
    wallet_volume: Counter = Counter()
    for ts, wallet, amount, _side, _txid in rows:
        wallet_first_seen.setdefault(wallet, ts)
        wallet_volume[wallet] += abs(amount)
    early_cutoff = first_ts + args.early_minutes * 60
    early_wallets = {w for w, t in wallet_first_seen.items() if t <= early_cutoff}
    top_wallets = set(w for w, _ in wallet_volume.most_common(CONFIG["top_k_wallets"]))

    liq_by_step: Dict[int, List[Tuple[str, float]]] = defaultdict(list)
    all_liq_values: List[float] = []
    for ts, action, lv in liq_events.get(mint, []):
        step_ts = first_ts + ((ts - first_ts) // step) * step
        liq_by_step[step_ts].append((action, lv))
        all_liq_values.append(lv)
    thin_threshold = 0.0
    if all_liq_values:
        sv = sorted(all_liq_values)
        idx = int((CONFIG["thin_liquidity_percentile"] / 100.0) * (len(sv) - 1))
        thin_threshold = sv[idx]

    holder_by_step: Dict[int, float] = {}
    conc_spike_steps: set[int] = set()
    if mint in holder_snapshots:
        ordered = sorted(holder_snapshots[mint].items())
        last_g = 0.0
        for ts, balances in ordered:
            s_ts = first_ts + ((ts - first_ts) // step) * step
            g = gini(balances)
            holder_by_step[s_ts] = g
            if g - last_g > 0.08:
                conc_spike_steps.add(s_ts)
            last_g = g

    meta = metadata.get(mint, {"mint_authority_active": 0, "freeze_authority_active": 0, "mutable_metadata": 0})
    sec = security_backfill.get(mint, {"mint_authority_present": 0, "freeze_authority_present": 0})
    liq_backfill = liquidity_backfill.get(mint, {"liquidity_usd": None})
    authority_eligible = bool(sec.get("mint_authority_present") or sec.get("freeze_authority_present"))
    liq_usd = liq_backfill.get("liquidity_usd")

    step_rows: List[List[str]] = []
    exemplars: List[List[str]] = []
    archetype = "The Ghost"
    flip_count = 0
    arch_counts: Counter = Counter()
    last_dir_conf = None
    last_risk_conf = None

    dir_state = "NONE"
    risk_state = "NONE"
    dir_pos = 0
    dir_neg = 0
    risk_pos = 0
    risk_neg = 0

    direction_persist = 0
    risk_persist = 0
    silence_steps = 0
    resurrection_count = 0
    liq_window: Deque[float] = deque(maxlen=CONFIG["liquidity_vol_window"])

    idx = 0
    seen_wallets: set[str] = set()
    prev_step_had_events = False
    for step_ts in range(first_ts, last_ts + step, step):
        bucket: List[Tuple[int, str, float, str, str]] = []
        while idx < len(rows) and rows[idx][0] < step_ts + step:
            bucket.append(rows[idx])
            idx += 1
        event_count = len(bucket)
        tempo = classify_tempo(event_count)

        inflow = 0.0
        outflow = 0.0
        early_entry = 0
        early_exit = 0
        new_wallets = 0
        top_outflow = 0.0
        amounts: List[float] = []
        inter_arrivals: List[int] = []
        local_wallets = Counter()
        prev_ts = None

        for ts, wallet, amount, side, _txid in bucket:
            sval = (side or "").lower()
            is_out = sval in {"sell", "out", "remove", "withdraw"} or amount < 0
            if is_out:
                outflow += abs(amount)
            else:
                inflow += abs(amount)
            if wallet in early_wallets:
                if wallet not in seen_wallets:
                    early_entry += 1
                if is_out:
                    early_exit += 1
            if wallet not in seen_wallets:
                new_wallets += 1
            if wallet in top_wallets and is_out:
                top_outflow += abs(amount)
            seen_wallets.add(wallet)
            amounts.append(abs(amount))
            local_wallets[wallet] += 1
            if prev_ts is not None:
                inter_arrivals.append(max(0, ts - prev_ts))
            prev_ts = ts

        liq_remove_count = 0
        liq_vals = []
        for action, lv in liq_by_step.get(step_ts, []):
            liq_vals.append(lv)
            if "remove" in action or "withdraw" in action:
                liq_remove_count += 1
        for lv in liq_vals:
            liq_window.append(lv)

        liquidity_vol = mean_abs_dev(list(liq_window)) if len(liq_window) >= 2 else 0.0
        liq_stable = 1.0 / (1.0 + liquidity_vol)
        liq_deterioration = liquidity_vol
        thin_liq = 1 if liq_vals and min(liq_vals) <= thin_threshold else 0

        timing_cluster = 1.0 / (1.0 + (mean_abs_dev(inter_arrivals) if inter_arrivals else 0.0))
        repeated_size = 1.0 / (1.0 + (mean_abs_dev(amounts) if amounts else 0.0))
        clique_density = 0.0
        if event_count > 0:
            clique_density = sum(c for _, c in local_wallets.most_common(CONFIG["top_k_wallets"])) / event_count

        gini_now = holder_by_step.get(step_ts, 0.0)
        conc_spike = 1 if step_ts in conc_spike_steps else 0

        if event_count == 0:
            silence_steps += 1
        else:
            if silence_steps * step >= args.silent_gap_minutes * 60 and prev_step_had_events is False:
                resurrection_count += 1
            silence_steps = 0
        prev_step_had_events = event_count > 0
        silent_gap_flag = 1 if silence_steps * step >= args.silent_gap_minutes * 60 else 0

        w = CONFIG["weights"]
        p_plus = (
            w["P_PLUS"]["whale_inflow"] * inflow
            + w["P_PLUS"]["early_entry"] * early_entry
            + w["P_PLUS"]["wallet_velocity"] * new_wallets
            + w["P_PLUS"]["liquidity_stability"] * liq_stable
        )
        p_minus = (
            w["P_MINUS"]["whale_outflow"] * outflow
            + w["P_MINUS"]["early_exit"] * early_exit
            + w["P_MINUS"]["top_wallet_outflow"] * top_outflow
            + w["P_MINUS"]["liquidity_deterioration"] * liq_deterioration
            + w["P_MINUS"]["concentration_spike"] * conc_spike
        )
        authority_component = CONFIG["authority_risk_component"] if authority_eligible else 0.0
        low_liq_flag = 1 if (liq_usd is not None and liq_usd <= CONFIG["liq_floor_usd"]) else 0
        low_liq_component = CONFIG["low_liquidity_risk_component"] if low_liq_flag else 0.0
        fragility = (
            w["FRAGILITY"]["timing_cluster"] * timing_cluster
            + w["FRAGILITY"]["repeated_size"] * repeated_size
            + w["FRAGILITY"]["clique_density"] * clique_density
            + w["FRAGILITY"]["lp_remove"] * liq_remove_count
            + w["FRAGILITY"]["thin_liquidity"] * thin_liq
            + w["FRAGILITY"]["authority_flags"]
            * (meta["mint_authority_active"] + meta["freeze_authority_active"] + meta["mutable_metadata"])
            + w["FRAGILITY"]["concentration_spike"] * conc_spike
            + authority_component
            + low_liq_component
        )

        dom = dominant_dir(p_plus, p_minus)
        if dom in ("PUMP", "DUMP"):
            direction_persist += 1
        else:
            direction_persist = max(0, direction_persist - 1)

        risk_score = fragility / (1.0 + abs(p_plus - p_minus) / (1.0 + event_count))
        risk_on = risk_score >= CONFIG["risk_warning"]
        risk_high = risk_score >= CONFIG["risk_shout"]
        risk_persist = risk_persist + 1 if risk_on else max(0, risk_persist - 1)

        dir_pos = dir_pos + 1 if dom != "NEUTRAL" else max(0, dir_pos - 1)
        dir_state = set_level(dir_state, dir_pos, dir_neg, tempo)
        risk_pos = risk_pos + 1 if risk_on else max(0, risk_pos - 1)
        risk_state = set_level(risk_state, risk_pos, risk_neg, tempo)

        align = int(dom == "PUMP") + int(dom == "DUMP") + int(risk_on)
        contrad = int(dom == "NEUTRAL") + int(risk_high and dom == "PUMP" and p_minus > p_plus) + int(
            risk_high and dom == "DUMP" and p_plus > p_minus
        )
        dir_conf = compute_conf(align, contrad, direction_persist)
        risk_conf = compute_conf(int(risk_on) + int(risk_high), int(not risk_on), risk_persist)

        dir_alert = f"{dir_state}:{dom}" if dom != "NEUTRAL" else f"{dir_state}:NEUTRAL"
        risk_alert = f"{risk_state}:{'HIGH' if risk_high else ('ELEVATED' if risk_on else 'LOW')}"
        conflict = ""
        if dir_state == "SHOUT" and risk_high:
            conflict = (
                "SHOUT: CONFLICT — engineered pump confirmed"
                if dom == "PUMP"
                else "SHOUT: CONFLICT — engineered dump confirmed"
            )
        elif dir_state in {"WARNING", "SHOUT"} and risk_on:
            conflict = (
                "WARNING: CONFLICT — engineered pump conditions"
                if dom == "PUMP"
                else "WARNING: CONFLICT — engineered dump conditions"
            )

        distinct_wallets_in_window = len(local_wallets)
        top10_wallet_event_share = clique_density
        bot_farm_gate = distinct_wallets_in_window <= 25 and top10_wallet_event_share >= 0.60
        coordination_signature = timing_cluster > 0.7 and repeated_size > 0.7 and clique_density > 0.75

        signals = {
            "vampire": (liq_remove_count > 0 and outflow > inflow) or low_liq_flag == 1,
            "time_bomb": authority_eligible,
            "bot_farm": coordination_signature and bot_farm_gate,
            "phoenix": resurrection_count > 0 and inflow > outflow,
            "ghost": silent_gap_flag == 1,
            "accumulator": dom == "PUMP" and direction_persist >= 2,
            "distributor": dom == "DUMP" and direction_persist >= 2,
        }

        can_flip = dir_state == "SHOUT" or signals["ghost"]
        new_arch = resolve_archetype(signals, archetype, can_flip)
        if signals["ghost"] and silence_steps < CONFIG["ghost_decay_steps"]:
            new_arch = archetype
        if new_arch != archetype:
            flip_count += 1
            archetype = new_arch
            exemplars.append([archetype, mint, ts_iso(step_ts), short_reason(signals)])

        arch_counts[archetype] += 1

        if dir_conf != last_dir_conf or risk_conf != last_risk_conf:
            step_rows.append(
                [
                    ts_iso(step_ts),
                    mint,
                    archetype,
                    dir_alert,
                    str(dir_conf),
                    risk_alert,
                    str(risk_conf),
                    conflict,
                ]
            )
            last_dir_conf = dir_conf
            last_risk_conf = risk_conf

    summary = [
        mint,
        str(((last_ts - first_ts) // step) + 1),
        ";".join(f"{k}:{arch_counts[k]}" for k in sorted(arch_counts)),
        str(flip_count),
        ts_iso(first_ts),
        ts_iso(last_ts),
    ]
    return step_rows, summary, arch_counts, flip_count, exemplars


def list_mints(conn: sqlite3.Connection, events_table: CategoryTable, mint_filter: Optional[str]) -> List[str]:
    if not events_table.table or "mint" not in events_table.mapping:
        return []
    mint_col = events_table.mapping["mint"]
    sql = (
        f"SELECT DISTINCT {mint_col} AS mint FROM {events_table.table} "
        f"WHERE {mint_col} IS NOT NULL AND TRIM(CAST({mint_col} AS TEXT)) <> ''"
    )
    params: List[Any] = []
    if mint_filter:
        sql += f" AND {mint_col} = ?"
        params.append(mint_filter)
    sql += " ORDER BY mint"
    return [str(r[0]) for r in conn.execute(sql, params)]


def iter_mint_rows(conn: sqlite3.Connection, events_table: CategoryTable, mint_filter: Optional[str]) -> Iterator[Tuple[str, List[Tuple[int, str, float, str, str]]]]:
    sql, params = build_events_query(events_table, mint_filter)
    cur = conn.execute(sql, params)
    current_mint = None
    buf: List[Tuple[int, str, float, str, str]] = []
    for row in cur:
        epoch = to_epoch(row[0])
        wallet = str(row[1]) if row[1] is not None else ""
        mint = str(row[2]) if row[2] is not None else ""
        amount = float(row[3]) if row[3] not in (None, "") else 0.0
        side = str(row[4]) if row[4] is not None else ""
        txid = str(row[5]) if row[5] is not None else ""
        if epoch is None or not mint or not wallet:
            continue
        if current_mint is None:
            current_mint = mint
        if mint != current_mint:
            yield current_mint, buf
            buf = []
            current_mint = mint
        buf.append((epoch, wallet, amount, side, txid))
    if current_mint is not None and buf:
        yield current_mint, buf


def write_tsv(path: Path, header: Sequence[str], rows: Iterable[Sequence[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(header)
        for r in rows:
            w.writerow(list(r))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(f"DB path does not exist: {args.db}")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    tables = get_tables(conn)
    print(f"schema detected: {len(tables)} tables")
    categories = detect_categories(tables)
    missing = [k for k, v in categories.items() if not v.table]
    print("missing feature categories:", ", ".join(missing) if missing else "none")

    print(f"events.table: {categories['events'].table}")
    print(f"events.mapping: {categories['events'].mapping}")
    print(f"security.table: {categories['security'].table}")
    print(f"security.mapping: {categories['security'].mapping}")
    print(f"liquidity.table: {categories['liquidity'].table}")
    print(f"liquidity.mapping: {categories['liquidity'].mapping}")

    if not categories["events"].table:
        raise RuntimeError("Unable to identify events table by schema signature.")
    required_event_keys = {"ts", "wallet", "mint"}
    if not required_event_keys.issubset(set(categories["events"].mapping.keys())):
        raise RuntimeError(
            f"Events mapping missing required keys {sorted(required_event_keys)}: {categories['events'].mapping}"
        )

    metadata = fetch_metadata(conn, categories["metadata"])
    security_backfill = fetch_security_backfill(conn, categories["security"])
    liquidity_backfill = fetch_liquidity_backfill(conn, categories["liquidity"])
    liq = fetch_liquidity_events(conn, categories["lp_events"], args.mint)
    holders = fetch_holder_snapshots(conn, categories["holders"], args.mint)

    timeline_rows: List[List[str]] = []
    summary_rows: List[List[str]] = []
    exemplar_rows: List[List[str]] = []
    discovered_mints = list_mints(conn, categories["events"], args.mint)
    mint_count = len(discovered_mints)

    for mint, mint_rows in iter_mint_rows(conn, categories["events"], args.mint):
        step_rows, summary, _counts, _flips, ex = evaluate_mint(
            mint,
            mint_rows,
            metadata,
            security_backfill,
            liquidity_backfill,
            liq,
            holders,
            args,
        )
        timeline_rows.extend(step_rows)
        summary_rows.append(summary)
        exemplar_rows.extend(ex)

    timeline_rows.sort(key=lambda r: (r[1], r[0]))
    summary_rows.sort(key=lambda r: r[0])
    exemplar_rows.sort(key=lambda r: (r[0], r[1], r[2]))

    print(f"mint count: {mint_count}")

    t_path = outdir / "archetype_timeline.tsv"
    s_path = outdir / "archetype_summary.tsv"
    e_path = outdir / "exemplars.tsv"
    d_path = outdir / "digests.txt"

    write_tsv(
        t_path,
        [
            "ts_iso",
            "mint",
            "archetype",
            "direction_alert",
            "direction_conf_pct",
            "risk_alert",
            "risk_conf_pct",
            "conflict_alert",
        ],
        timeline_rows,
    )
    write_tsv(
        s_path,
        ["mint", "total_steps", "archetype_counts", "flip_count", "first_seen_ts", "last_seen_ts"],
        summary_rows,
    )
    write_tsv(e_path, ["archetype", "mint", "ts_iso", "short_reason"], exemplar_rows)

    with d_path.open("w", encoding="utf-8", newline="\n") as f:
        for p in (t_path, s_path, e_path):
            f.write(f"{p.name}\t{sha256_file(p)}\n")

    conn.close()


if __name__ == "__main__":
    main()
