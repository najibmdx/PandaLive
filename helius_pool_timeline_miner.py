#!/usr/bin/env python3
"""Pool-wide swap timeline miner around Cented probe trades."""

from __future__ import annotations

import argparse
import csv
import math
import os
import sqlite3
import sys
import time
from collections import Counter, deque
from dataclasses import dataclass
from typing import Any

import requests

BASE_URL = "https://api-mainnet.helius-rpc.com"
ADDRESS_PAGE_LIMIT = 100
ENHANCED_BATCH_SIZE = 100
MAX_PAGES = 30
MAX_RETRIES = 5
LAMPORTS_PER_SOL = 1_000_000_000

REQUIRED_COLUMNS = [
    "mint",
    "entry_time",
    "exit_time",
    "entry_sol",
    "net_sol",
    "roi_on_cost",
    "hold_seconds",
]

KNOWN_PROGRAM_IDS = {
    "11111111111111111111111111111111",  # SystemProgram
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # SPL Token
    "ATokenGPvR93M2qJ2k8L4fY2fR4fK7R8D8LQmNwGX1",  # Associated Token (legacy alias may vary)
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",  # Associated Token
    "ComputeBudget111111111111111111111111111111",  # ComputeBudget
    "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr",  # Memo
}

EXPLICIT_POOL_FIELDS = {
    "whirlpool",
    "ammid",
    "poolid",
    "pool",
    "lbpair",
    "pair",
}


class LoudError(Exception):
    """Raised for deterministic fail-loud behavior."""


@dataclass
class Probe:
    mint: str
    entry_time: int
    exit_time: int
    roi_on_cost: float
    hold_seconds: float
    row_num: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mine pool-wide swap timelines using Helius enhanced transactions")
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--cented-wallet", required=True)
    parser.add_argument("--trades-tsv", required=True)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--pad-seconds", type=int, default=120)
    parser.add_argument("--max-probes", type=int, default=3)
    parser.add_argument("--select-mode", choices=["stratified", "first"], default="stratified")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if args.pad_seconds < 0:
        raise LoudError("--pad-seconds must be >= 0")
    if args.max_probes <= 0:
        raise LoudError("--max-probes must be > 0")
    if not args.api_key.strip():
        raise LoudError("--api-key cannot be empty")
    if not args.cented_wallet.strip():
        raise LoudError("--cented-wallet cannot be empty")
    if not os.path.isfile(args.db):
        raise LoudError(f"SQLite db not found: {args.db}")
    if not os.path.isfile(args.trades_tsv):
        raise LoudError(f"Trades TSV not found: {args.trades_tsv}")
    return args


def info(verbose: bool, message: str) -> None:
    if verbose:
        print(f"INFO: {message}")


def parse_required_float(value: Any, field: str, row_num: int) -> float:
    if value is None or str(value).strip() == "":
        raise LoudError(f"Blank numeric value in '{field}' at TSV row {row_num}")
    try:
        num = float(str(value).strip())
    except ValueError as exc:
        raise LoudError(f"Invalid numeric value in '{field}' at TSV row {row_num}: {value!r}") from exc
    if not math.isfinite(num):
        raise LoudError(f"Non-finite numeric value in '{field}' at TSV row {row_num}: {value!r}")
    return num


def load_trades(path: str) -> list[Probe]:
    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        if not reader.fieldnames:
            raise LoudError("Trades TSV missing header")
        missing = [c for c in REQUIRED_COLUMNS if c not in reader.fieldnames]
        if missing:
            raise LoudError(f"Trades TSV missing required columns: {missing}")

        probes: list[Probe] = []
        for row_num, row in enumerate(reader, start=2):
            mint = (row.get("mint") or "").strip()
            if not mint:
                raise LoudError(f"Blank mint at TSV row {row_num}")
            entry_time = int(parse_required_float(row.get("entry_time"), "entry_time", row_num))
            exit_time = int(parse_required_float(row.get("exit_time"), "exit_time", row_num))
            _ = parse_required_float(row.get("entry_sol"), "entry_sol", row_num)
            _ = parse_required_float(row.get("net_sol"), "net_sol", row_num)
            roi = parse_required_float(row.get("roi_on_cost"), "roi_on_cost", row_num)
            hold = parse_required_float(row.get("hold_seconds"), "hold_seconds", row_num)
            if exit_time < entry_time:
                raise LoudError(f"exit_time < entry_time at TSV row {row_num}")
            probes.append(Probe(mint=mint, entry_time=entry_time, exit_time=exit_time, roi_on_cost=roi, hold_seconds=hold, row_num=row_num))

    if not probes:
        raise LoudError("Trades TSV contains no data rows")
    return probes


def roi_bucket(roi: float) -> str:
    if roi <= -0.284211:
        return "big_loss"
    if -0.284211 < roi <= -0.121632:
        return "mid_loss"
    if -0.121632 < roi <= 0.253085:
        return "mid"
    if 0.253085 < roi <= 0.688854:
        return "good"
    if 0.688854 < roi <= 1.118921:
        return "very_good"
    return "extreme"


def select_probes(probes: list[Probe], mode: str, max_probes: int) -> list[Probe]:
    if mode == "first":
        return probes[:max_probes]

    order = ["big_loss", "mid_loss", "mid", "good", "very_good", "extreme"]
    by_bucket: dict[str, list[Probe]] = {k: [] for k in order}
    for probe in probes:
        by_bucket[roi_bucket(probe.roi_on_cost)].append(probe)

    for key in by_bucket:
        by_bucket[key].sort(key=lambda p: (p.hold_seconds, p.entry_time, p.row_num))

    selected: list[Probe] = []
    picked_rows: set[int] = set()

    for key in order:
        if len(selected) >= max_probes:
            break
        if by_bucket[key]:
            chosen = by_bucket[key][0]
            selected.append(chosen)
            picked_rows.add(chosen.row_num)

    if len(selected) < max_probes:
        rest = [p for p in probes if p.row_num not in picked_rows]
        rest.sort(key=lambda p: (p.hold_seconds, p.entry_time, p.row_num))
        selected.extend(rest[: max_probes - len(selected)])

    return selected[:max_probes]


def request_with_retry(method: str, url: str, *, params: dict[str, Any] | None = None, json_payload: Any | None = None, timeout_s: int = 45) -> Any:
    delays = [1, 2, 4, 8, 16]
    for attempt in range(MAX_RETRIES):
        try:
            if method == "GET":
                resp = requests.get(url, params=params, timeout=timeout_s)
            elif method == "POST":
                resp = requests.post(url, params=params, json=json_payload, timeout=timeout_s)
            else:
                raise LoudError(f"Unsupported HTTP method: {method}")
        except (requests.Timeout, requests.ConnectionError):
            if attempt == MAX_RETRIES - 1:
                raise LoudError(f"HTTP {method} failed after retries: {url}")
            time.sleep(delays[attempt])
            continue

        status = resp.status_code
        if status == 401:
            raise LoudError("invalid api key")
        if status == 400:
            raise LoudError(f"HTTP 400 for {url}: {resp.text}")
        if status == 429 or 500 <= status <= 599:
            if attempt == MAX_RETRIES - 1:
                raise LoudError(f"HTTP {status} after retries for {url}: {resp.text}")
            time.sleep(delays[attempt])
            continue
        if status >= 400:
            raise LoudError(f"HTTP {status} for {url}: {resp.text}")

        try:
            return resp.json()
        except ValueError as exc:
            raise LoudError(f"Invalid JSON response from {url}") from exc

    raise LoudError(f"Unreachable retry state for {url}")


def fetch_enhanced_batch(api_key: str, signatures: list[str]) -> list[dict[str, Any]]:
    if not signatures:
        return []
    url = f"{BASE_URL}/v0/transactions/"
    params = {"api-key": api_key}
    out: list[dict[str, Any]] = []
    for i in range(0, len(signatures), ENHANCED_BATCH_SIZE):
        chunk = signatures[i : i + ENHANCED_BATCH_SIZE]
        data = request_with_retry("POST", url, params=params, json_payload={"transactions": chunk})
        if not isinstance(data, list):
            raise LoudError("Enhanced transactions endpoint did not return a list")
        out.extend(data)
    return out


def resolve_entry_signature(db_path: str, cented_wallet: str, mint: str) -> tuple[str, int]:
    sql = (
        "SELECT signature, block_time FROM swaps "
        "WHERE scan_wallet = ? AND token_mint = ? "
        "ORDER BY block_time ASC LIMIT 1"
    )
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(sql, (cented_wallet, mint))
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        raise LoudError(f"No entry signature found in swaps for wallet={cented_wallet} mint={mint}")
    signature, block_time = row
    if not signature or block_time is None:
        raise LoudError(f"Invalid swaps row for wallet={cented_wallet} mint={mint}: {row}")
    return str(signature), int(block_time)


def _extract_addresses(value: Any, out: set[str]) -> None:
    if isinstance(value, str):
        if 32 <= len(value) <= 48 and " " not in value:
            out.add(value)
        return
    if isinstance(value, dict):
        for v in value.values():
            _extract_addresses(v, out)
        return
    if isinstance(value, list):
        for item in value:
            _extract_addresses(item, out)


def _walk_instruction_accounts(container: Any, counter: Counter[str]) -> None:
    if isinstance(container, dict):
        accounts = container.get("accounts")
        if isinstance(accounts, list):
            for a in accounts:
                if isinstance(a, str):
                    counter[a] += 1
                elif isinstance(a, dict) and isinstance(a.get("pubkey"), str):
                    counter[a["pubkey"]] += 1
        if isinstance(container.get("programId"), str):
            counter[container["programId"]] += 1
        for v in container.values():
            _walk_instruction_accounts(v, counter)
    elif isinstance(container, list):
        for item in container:
            _walk_instruction_accounts(item, counter)


def resolve_pool_address(enh_tx: dict[str, Any], mint: str, cented_wallet: str, verbose: bool = False) -> tuple[str, str, str]:
    fee_payer = enh_tx.get("feePayer")
    source = str(enh_tx.get("source") or "")
    source_up = source.upper()

    addresses: set[str] = set()
    _extract_addresses(enh_tx, addresses)

    explicit_hits: list[tuple[str, str]] = []

    def scan_explicit(obj: Any) -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                kl = k.lower()
                if kl in EXPLICIT_POOL_FIELDS and isinstance(v, str) and v in addresses:
                    explicit_hits.append((kl, v))
                scan_explicit(v)
        elif isinstance(obj, list):
            for item in obj:
                scan_explicit(item)

    scan_explicit(enh_tx)

    if explicit_hits:
        explicit_hits.sort(key=lambda x: (x[0], x[1]))
        key, addr = explicit_hits[0]
        method = f"explicit_field:{key}"
        details = f"source={source_up or 'UNKNOWN'} explicit_hits={len(explicit_hits)}"
        return addr, method, details

    counter: Counter[str] = Counter()
    _walk_instruction_accounts(enh_tx.get("instructions", []), counter)
    _walk_instruction_accounts(enh_tx.get("innerInstructions", []), counter)
    _walk_instruction_accounts(enh_tx.get("transaction", {}), counter)

    if not counter:
        raise LoudError("Unable to derive pool candidates: no instruction accounts found")

    disallow = set(KNOWN_PROGRAM_IDS)
    disallow.add(mint)
    disallow.add(cented_wallet)
    if isinstance(fee_payer, str) and fee_payer:
        disallow.add(fee_payer)

    candidates = [(addr, count) for addr, count in counter.items() if addr not in disallow]
    candidates = [(a, c) for a, c in candidates if 32 <= len(a) <= 48]
    if not candidates:
        if verbose:
            top = sorted(counter.items(), key=lambda x: (-x[1], x[0]))[:10]
            print(f"INFO: resolve_pool_address debug top_candidates={top}")
        raise LoudError("No pool candidate remained after filtering")

    candidates.sort(key=lambda x: (-x[1], x[0]))
    pick_addr, pick_count = candidates[0]
    method = "frequency_heuristic"
    if "ORCA" in source_up:
        method = "orca_frequency_heuristic"
    elif "RAYDIUM" in source_up:
        method = "raydium_frequency_heuristic"
    elif "METEORA" in source_up:
        method = "meteora_frequency_heuristic"
    elif "JUPITER" in source_up:
        method = "jupiter_frequency_heuristic"

    details = f"source={source_up or 'UNKNOWN'} count={pick_count} candidates={len(candidates)}"
    return pick_addr, method, details


def extract_signature(tx: dict[str, Any]) -> str:
    sig = tx.get("signature")
    if isinstance(sig, str) and sig:
        return sig
    sigs = tx.get("signatures")
    if isinstance(sigs, list) and sigs and isinstance(sigs[0], str):
        return sigs[0]
    tr = tx.get("transaction")
    if isinstance(tr, dict):
        tr_sigs = tr.get("signatures")
        if isinstance(tr_sigs, list) and tr_sigs and isinstance(tr_sigs[0], str):
            return tr_sigs[0]
    raise LoudError(f"Transaction item missing signature: keys={sorted(tx.keys())}")


def get_block_time(tx: dict[str, Any]) -> int:
    bt = tx.get("blockTime")
    if bt is None:
        bt = tx.get("timestamp")
    if bt is None:
        raise LoudError(f"Transaction missing blockTime/timestamp for signature {extract_signature(tx)}")
    try:
        return int(bt)
    except (TypeError, ValueError) as exc:
        raise LoudError(f"Invalid blockTime/timestamp value {bt!r}") from exc


def is_swap_like(tx: dict[str, Any]) -> bool:
    if tx.get("type") == "SWAP":
        return True
    source = str(tx.get("source") or "").upper()
    return any(k in source for k in ("ORCA", "RAYDIUM", "JUPITER", "METEORA"))


def parse_token_amount(transfer: dict[str, Any]) -> tuple[float, bool]:
    amount = transfer.get("tokenAmount")
    if amount is None:
        amount = transfer.get("amount")
    if amount is None:
        raise LoudError("tokenTransfers item missing amount/tokenAmount")

    if isinstance(amount, (int, float, str)):
        try:
            return float(amount), False
        except ValueError as exc:
            raise LoudError(f"Invalid token amount: {amount!r}") from exc

    if isinstance(amount, dict):
        if amount.get("uiAmount") is not None:
            return float(amount["uiAmount"]), False
        if amount.get("uiAmountString") is not None:
            return float(amount["uiAmountString"]), False
        raw = amount.get("amount")
        decimals = amount.get("decimals")
        if raw is not None and decimals is not None:
            raw_f = float(raw)
            dec_i = int(decimals)
            return raw_f / (10 ** dec_i), False
        if raw is not None and decimals is None:
            return float(raw), True

    raise LoudError(f"Unsupported token amount format: {amount!r}")


def derive_row(tx: dict[str, Any], mint: str, cented_wallet: str) -> dict[str, Any] | None:
    if not is_swap_like(tx):
        return None

    token_transfers = tx.get("tokenTransfers")
    if not isinstance(token_transfers, list):
        return None
    probe_transfers = [t for t in token_transfers if (t.get("mint") or t.get("tokenMint")) == mint]
    if not probe_transfers:
        return None

    trader = tx.get("feePayer")
    if not trader:
        raise LoudError(f"Missing feePayer for signature {extract_signature(tx)}")

    token_in = 0.0
    token_out = 0.0
    token_unknown_units = False
    for t in probe_transfers:
        amt, unknown_units = parse_token_amount(t)
        token_unknown_units = token_unknown_units or unknown_units
        to_acc = t.get("toUserAccount")
        to_owner = t.get("toUserAccountOwner")
        from_acc = t.get("fromUserAccount")
        from_owner = t.get("fromUserAccountOwner")
        if to_acc == trader or to_owner == trader:
            token_in += amt
        if from_acc == trader or from_owner == trader:
            token_out += amt

    token_net = token_in - token_out

    native_transfers = tx.get("nativeTransfers")
    if not isinstance(native_transfers, list):
        native_transfers = []

    sol_in = 0
    sol_out = 0
    for nt in native_transfers:
        lamports = nt.get("amount")
        if lamports is None:
            lamports = nt.get("lamports")
        if lamports is None:
            raise LoudError(f"nativeTransfers item missing lamports/amount in {extract_signature(tx)}")
        lamports_i = int(lamports)
        if nt.get("toUserAccount") == trader:
            sol_in += lamports_i
        if nt.get("fromUserAccount") == trader:
            sol_out += lamports_i
    sol_net = sol_in - sol_out

    side = "ANOMALY"
    anomaly_reason = ""
    if token_net > 0 and sol_net < 0:
        side = "BUY"
    elif token_net < 0 and sol_net > 0:
        side = "SELL"
    else:
        anomalies = []
        if token_net == 0:
            anomalies.append("token_net_zero")
        if sol_net == 0:
            anomalies.append("sol_net_zero")
        if token_net > 0 and sol_net > 0:
            anomalies.append("both_positive")
        if token_net < 0 and sol_net < 0:
            anomalies.append("both_negative")
        if token_unknown_units:
            anomalies.append("token_units_raw_unknown")
        anomaly_reason = ",".join(anomalies) if anomalies else "side_rule_mismatch"

    if side != "ANOMALY" and token_unknown_units:
        anomaly_reason = "token_units_raw_unknown"

    return {
        "ts": get_block_time(tx),
        "signature": extract_signature(tx),
        "slot": tx.get("slot", ""),
        "trader": trader,
        "dex_source": tx.get("source", ""),
        "side": side,
        "sol_amount": abs(sol_net) / LAMPORTS_PER_SOL if sol_net != 0 else 0.0,
        "token_amount": abs(token_net),
        "is_cented": 1 if trader == cented_wallet else 0,
        "anomaly_reason": anomaly_reason,
    }


def peak_rate_10s(rows: list[dict[str, Any]], side: str) -> int:
    times = [int(r["ts"]) for r in rows if r["side"] == side]
    times.sort()
    dq: deque[int] = deque()
    peak = 0
    for t in times:
        dq.append(t)
        while dq and t - dq[0] > 10:
            dq.popleft()
        if len(dq) > peak:
            peak = len(dq)
    return peak


def write_outputs(base_dir: str, mint: str, rows: list[dict[str, Any]]) -> None:
    dossier_dir = os.path.join(base_dir, "dossiers", mint)
    os.makedirs(dossier_dir, exist_ok=True)

    timeline_path = os.path.join(dossier_dir, "pool_timeline.tsv")
    columns = [
        "ts",
        "signature",
        "slot",
        "trader",
        "dex_source",
        "side",
        "sol_amount",
        "token_amount",
        "is_cented",
        "anomaly_reason",
    ]
    with open(timeline_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, delimiter="\t", extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    total_rows = len(rows)
    buys = [r for r in rows if r["side"] == "BUY"]
    sells = [r for r in rows if r["side"] == "SELL"]
    anomalies = [r for r in rows if r["side"] == "ANOMALY"]
    min_ts = min((r["ts"] for r in rows), default="")
    max_ts = max((r["ts"] for r in rows), default="")

    summary = {
        "total_rows": total_rows,
        "total_swaps": len(buys) + len(sells),
        "anomalies_count": len(anomalies),
        "unique_traders": len({r["trader"] for r in rows}),
        "buys_count": len(buys),
        "sells_count": len(sells),
        "buy_sol_total": sum(float(r["sol_amount"]) for r in buys),
        "sell_sol_total": sum(float(r["sol_amount"]) for r in sells),
        "peak_buy_rate_10s": peak_rate_10s(rows, "BUY"),
        "peak_sell_rate_10s": peak_rate_10s(rows, "SELL"),
        "min_ts": min_ts,
        "max_ts": max_ts,
        "cented_rows_count": sum(1 for r in rows if r["is_cented"] == 1),
    }

    summary_path = os.path.join(dossier_dir, "pool_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as fh:
        for key in [
            "total_rows",
            "total_swaps",
            "anomalies_count",
            "unique_traders",
            "buys_count",
            "sells_count",
            "buy_sol_total",
            "sell_sol_total",
            "peak_buy_rate_10s",
            "peak_sell_rate_10s",
            "min_ts",
            "max_ts",
            "cented_rows_count",
        ]:
            fh.write(f"{key}\t{summary[key]}\n")


def mine_pool_transactions(api_key: str, pool_address: str, window_start: int, window_end: int, verbose: bool) -> list[dict[str, Any]]:
    url = f"{BASE_URL}/v0/addresses/{pool_address}/transactions/"
    params = {"api-key": api_key, "limit": ADDRESS_PAGE_LIMIT}
    before: str | None = None

    all_within_window: list[dict[str, Any]] = []

    for page in range(1, MAX_PAGES + 1):
        p = dict(params)
        if before:
            p["before"] = before
        page_data = request_with_retry("GET", url, params=p)
        if not isinstance(page_data, list):
            raise LoudError(f"Address tx endpoint returned non-list for pool={pool_address}")
        if not page_data:
            info(verbose, f"pool={pool_address} page={page} empty stop")
            break

        signatures: list[str] = []
        raw_with_bt: list[tuple[int, dict[str, Any]]] = []
        for item in page_data:
            if not isinstance(item, dict):
                raise LoudError("Address tx item is not an object")
            sig = extract_signature(item)
            bt = get_block_time(item)
            raw_with_bt.append((bt, item))
            signatures.append(sig)

        need_enhanced = any("tokenTransfers" not in item for _, item in raw_with_bt)
        if need_enhanced:
            enhanced = fetch_enhanced_batch(api_key, signatures)
            by_sig = {extract_signature(tx): tx for tx in enhanced}
            page_items = []
            for sig in signatures:
                tx = by_sig.get(sig)
                if tx is None:
                    raise LoudError(f"Missing enhanced tx for signature {sig}")
                page_items.append(tx)
        else:
            page_items = [item for _, item in raw_with_bt]

        page_min_bt = None
        in_window_count = 0
        for tx in page_items:
            bt = get_block_time(tx)
            if page_min_bt is None or bt < page_min_bt:
                page_min_bt = bt
            if window_start <= bt <= window_end:
                all_within_window.append(tx)
                in_window_count += 1

        info(verbose, f"pool={pool_address} page={page} fetched={len(page_items)} in_window={in_window_count}")

        last_sig = extract_signature(page_items[-1])
        before = last_sig

        if page_min_bt is not None and page_min_bt < window_start:
            break

        if page == MAX_PAGES:
            raise LoudError("Reached page cap (30). Increase cap to continue.")

    return all_within_window


def fetch_single_enhanced(api_key: str, signature: str) -> dict[str, Any]:
    txs = fetch_enhanced_batch(api_key, [signature])
    if len(txs) != 1:
        raise LoudError(f"Expected one enhanced tx for {signature}, got {len(txs)}")
    tx = txs[0]
    if not isinstance(tx, dict):
        raise LoudError(f"Enhanced tx for {signature} was not an object")
    return tx


def process_probe(args: argparse.Namespace, probe: Probe) -> None:
    window_start = probe.entry_time - args.pad_seconds
    window_end = probe.exit_time + args.pad_seconds
    info(args.verbose, f"mint={probe.mint} window_start={window_start} window_end={window_end}")

    entry_sig, _entry_bt = resolve_entry_signature(args.db, args.cented_wallet, probe.mint)
    info(args.verbose, f"mint={probe.mint} entry_sig={entry_sig}")

    entry_tx = fetch_single_enhanced(args.api_key, entry_sig)
    pool_addr, method, details = resolve_pool_address(entry_tx, probe.mint, args.cented_wallet, verbose=args.verbose)
    info(args.verbose, f"mint={probe.mint} entry_sig={entry_sig} pool={pool_addr} via {method} ({details})")

    txs = mine_pool_transactions(args.api_key, pool_addr, window_start, window_end, args.verbose)
    rows: list[dict[str, Any]] = []
    for tx in txs:
        row = derive_row(tx, probe.mint, args.cented_wallet)
        if row is not None:
            rows.append(row)

    rows.sort(key=lambda r: (int(r["ts"]), str(r["signature"])))
    write_outputs(args.outdir, probe.mint, rows)
    info(args.verbose, f"mint={probe.mint} rows_written={len(rows)}")


def main() -> int:
    try:
        args = parse_args()
        probes = load_trades(args.trades_tsv)
        selected = select_probes(probes, args.select_mode, args.max_probes)
        info(args.verbose, "selected probes=" + ", ".join(p.mint for p in selected))

        for probe in selected:
            process_probe(args, probe)

        print(f"OK: wrote dossiers for {len(selected)} mints to {args.outdir}")
        return 0
    except LoudError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
