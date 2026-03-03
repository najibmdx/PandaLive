#!/usr/bin/env python3
"""
helius_pool_timeline_miner.py

Build per-mint pool timeline dossiers from a trade reconstruction TSV using Helius APIs.
"""

import argparse
import csv
import math
import os
import sys
import time
from collections import deque

try:
    import requests
except ImportError as exc:
    raise SystemExit(
        "ERROR: requests is required but not installed. Install with: pip install requests"
    ) from exc

BASE_URL = "https://api-mainnet.helius-rpc.com"
LAMPORTS_PER_SOL = 1_000_000_000
ADDRESS_PAGE_LIMIT = 100
ENHANCED_BATCH_SIZE = 100
MAX_RETRIES = 5
MAX_PAGES = 20

REQUIRED_COLUMNS = [
    "mint",
    "entry_time",
    "exit_time",
    "entry_sol",
    "net_sol",
    "roi_on_cost",
    "hold_seconds",
]

ROI_BUCKETS = [
    ("big_loss", None, -0.284211, True, True),
    ("mid_loss", -0.284211, -0.121632, False, True),
    ("mid", -0.121632, 0.253085, False, True),
    ("good", 0.253085, 0.688854, False, True),
    ("very_good", 0.688854, 1.118921, False, True),
    ("extreme", 1.118921, None, False, False),
]


class LoudError(Exception):
    pass


def parse_args():
    parser = argparse.ArgumentParser(description="Mine Helius pool timelines around sampled trade windows")
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--cented-wallet", required=True)
    parser.add_argument("--trades-tsv", required=True)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--pad-seconds", type=int, default=120)
    parser.add_argument("--max-probes", type=int, default=3)
    parser.add_argument("--select-mode", default="stratified", choices=["stratified", "first"])
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
    return args


def parse_float(field_name, value, row_num):
    try:
        if value is None or str(value).strip() == "":
            raise ValueError("empty")
        return float(value)
    except Exception as exc:
        raise LoudError(
            "Invalid numeric value in column '%s' at TSV row %d: %r" % (field_name, row_num, value)
        ) from exc


def parse_epoch_seconds(field_name, value, row_num):
    n = parse_float(field_name, value, row_num)
    if not math.isfinite(n):
        raise LoudError(
            "Non-finite timestamp in column '%s' at TSV row %d: %r" % (field_name, row_num, value)
        )
    return int(n)


def load_trades(path):
    if not os.path.isfile(path):
        raise LoudError("Trades TSV not found: %s" % path)

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        if reader.fieldnames is None:
            raise LoudError("Trades TSV has no header row: %s" % path)
        missing = [c for c in REQUIRED_COLUMNS if c not in reader.fieldnames]
        if missing:
            raise LoudError(
                "Trades TSV missing required columns %s. Found columns: %s"
                % (missing, reader.fieldnames)
            )

        rows = []
        for row_num, row in enumerate(reader, start=2):
            mint = (row.get("mint") or "").strip()
            if not mint:
                raise LoudError("Empty mint value at TSV row %d" % row_num)
            entry_time = parse_epoch_seconds("entry_time", row.get("entry_time"), row_num)
            exit_time = parse_epoch_seconds("exit_time", row.get("exit_time"), row_num)
            if exit_time < entry_time:
                raise LoudError("exit_time < entry_time at TSV row %d" % row_num)
            roi = parse_float("roi_on_cost", row.get("roi_on_cost"), row_num)
            hold = parse_float("hold_seconds", row.get("hold_seconds"), row_num)
            rows.append(
                {
                    "_row_num": row_num,
                    "mint": mint,
                    "entry_time": entry_time,
                    "exit_time": exit_time,
                    "roi_on_cost": roi,
                    "hold_seconds": hold,
                }
            )
        if not rows:
            raise LoudError("Trades TSV has headers but no data rows: %s" % path)
        return rows


def bucket_for_roi(roi):
    for name, low, high, low_inclusive, high_inclusive in ROI_BUCKETS:
        low_ok = True if low is None else (roi >= low if low_inclusive else roi > low)
        high_ok = True if high is None else (roi <= high if high_inclusive else roi < high)
        if low_ok and high_ok:
            return name
    raise LoudError("Internal error: ROI did not match any bucket: %s" % roi)


def select_probes(rows, mode, max_probes):
    if mode == "first":
        return rows[:max_probes]

    grouped = {}
    for r in rows:
        b = bucket_for_roi(r["roi_on_cost"])
        grouped.setdefault(b, []).append(r)

    selected = []
    used_rows = set()

    for bucket_name, _low, _high, _li, _hi in ROI_BUCKETS:
        candidates = grouped.get(bucket_name, [])
        if not candidates:
            continue
        candidates = sorted(candidates, key=lambda x: (x["hold_seconds"], x["_row_num"]))
        pick = candidates[0]
        selected.append(pick)
        used_rows.add(pick["_row_num"])
        if len(selected) >= max_probes:
            return selected[:max_probes]

    if len(selected) < max_probes:
        rest = [r for r in rows if r["_row_num"] not in used_rows]
        rest.sort(key=lambda x: (x["hold_seconds"], x["_row_num"]))
        needed = max_probes - len(selected)
        selected.extend(rest[:needed])

    return selected[:max_probes]


def request_with_retry(method, url, params=None, json_payload=None, timeout=45, verbose=False):
    wait = 0.5
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if method == "GET":
                resp = requests.get(url, params=params, timeout=timeout)
            elif method == "POST":
                resp = requests.post(url, params=params, json=json_payload, timeout=timeout)
            else:
                raise LoudError("Unsupported HTTP method: %s" % method)
        except requests.RequestException as exc:
            if attempt >= MAX_RETRIES:
                raise LoudError("HTTP request failed after retries: %s (%s)" % (url, exc)) from exc
            if verbose:
                print("WARN: request exception attempt %d/%d: %s" % (attempt, MAX_RETRIES, exc), file=sys.stderr)
            time.sleep(wait)
            wait *= 2
            continue

        if resp.status_code == 429 or 500 <= resp.status_code <= 599:
            if attempt >= MAX_RETRIES:
                snippet = (resp.text or "")[:500].replace("\n", " ")
                raise LoudError(
                    "HTTP %d persisted after retries for %s. Response: %s" % (resp.status_code, url, snippet)
                )
            if verbose:
                print(
                    "WARN: HTTP %d attempt %d/%d for %s; backing off %.1fs"
                    % (resp.status_code, attempt, MAX_RETRIES, url, wait),
                    file=sys.stderr,
                )
            time.sleep(wait)
            wait *= 2
            continue

        if resp.status_code >= 400:
            snippet = (resp.text or "")[:800].replace("\n", " ")
            raise LoudError("HTTP %d for %s. Response: %s" % (resp.status_code, url, snippet))

        try:
            return resp.json()
        except ValueError as exc:
            raise LoudError("Invalid JSON response from %s: %s" % (url, exc)) from exc

    raise LoudError("Retry loop ended unexpectedly for %s" % url)


def get_address_transactions(api_key, address, before_signature=None, verbose=False):
    url = "%s/v0/addresses/%s/transactions/" % (BASE_URL, address)
    params = {"api-key": api_key, "limit": ADDRESS_PAGE_LIMIT}
    if before_signature:
        params["before"] = before_signature
    payload = request_with_retry("GET", url, params=params, verbose=verbose)
    if not isinstance(payload, list):
        raise LoudError("Expected list from address transactions endpoint for %s" % address)
    return payload


def get_enhanced_by_signatures(api_key, signatures, verbose=False):
    if not signatures:
        return []
    url = "%s/v0/transactions/" % BASE_URL
    params = {"api-key": api_key}
    payload = request_with_retry("POST", url, params=params, json_payload=signatures, verbose=verbose)
    if not isinstance(payload, list):
        raise LoudError("Expected list from enhanced transactions endpoint")
    return payload


def extract_block_time(tx):
    t = tx.get("timestamp")
    if t is None:
        t = tx.get("blockTime")
    if t is None:
        return None
    try:
        return int(t)
    except Exception:
        return None


def collect_signatures_in_window(api_key, mint, window_start, window_end, verbose=False):
    signatures = []
    before = None
    pages = 0

    while True:
        pages += 1
        page = get_address_transactions(api_key, mint, before_signature=before, verbose=verbose)
        if not page:
            break

        page_times = []
        for tx in page:
            sig = tx.get("signature")
            ts = extract_block_time(tx)
            if sig and ts is not None and window_start <= ts <= window_end:
                signatures.append(sig)
            if ts is not None:
                page_times.append(ts)

        before = page[-1].get("signature")
        if not before:
            raise LoudError("Address page missing signature for pagination on mint %s" % mint)

        if page_times:
            oldest = min(page_times)
            if oldest < window_start:
                break

        if pages >= MAX_PAGES:
            print(
                "WARN: reached paging cap (%d) for mint %s before confident full coverage"
                % (MAX_PAGES, mint),
                file=sys.stderr,
            )
            break

    # deterministic unique order (first occurrence order)
    seen = set()
    ordered = []
    for s in signatures:
        if s not in seen:
            seen.add(s)
            ordered.append(s)
    return ordered


def to_decimal_number(value):
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def infer_is_swap(tx):
    tx_type = str(tx.get("type") or "").upper()
    if tx_type == "SWAP":
        return True
    source = str(tx.get("description") or "").lower()
    if "swap" in source:
        return True
    return False


def first_account_fallback(tx):
    account_data = tx.get("accountData") or []
    if isinstance(account_data, list) and account_data:
        acct = account_data[0]
        if isinstance(acct, dict) and acct.get("account"):
            return acct.get("account")
    accounts = tx.get("accounts") or []
    if isinstance(accounts, list) and accounts:
        first = accounts[0]
        if isinstance(first, str) and first:
            return first
        if isinstance(first, dict):
            return first.get("account") or first.get("pubkey")
    return None


def net_token_for_trader(tx, trader, mint):
    recv = 0.0
    sent = 0.0
    involved = False
    for tr in tx.get("tokenTransfers") or []:
        if (tr.get("mint") or "") != mint:
            continue
        involved = True
        amount = tr.get("tokenAmount")
        if amount is None:
            amount = tr.get("amount")
        qty = to_decimal_number(amount)
        if (tr.get("toUserAccount") or "") == trader:
            recv += qty
        if (tr.get("fromUserAccount") or "") == trader:
            sent += qty
    return recv - sent, involved


def net_sol_lamports_for_trader(tx, trader):
    recv = 0
    sent = 0
    native = tx.get("nativeTransfers") or []
    if native:
        for nt in native:
            lamports = int(to_decimal_number(nt.get("amount")))
            if (nt.get("toUserAccount") or "") == trader:
                recv += lamports
            if (nt.get("fromUserAccount") or "") == trader:
                sent += lamports
        return recv - sent

    for acct in tx.get("accountData") or []:
        if (acct.get("account") or "") == trader:
            return int(to_decimal_number(acct.get("nativeBalanceChange")))
    return 0


def analyze_enhanced_tx(tx, mint, cented_wallet):
    if not infer_is_swap(tx):
        return None

    token_net, mint_involved = net_token_for_trader(tx, tx.get("feePayer") or "", mint)
    if not mint_involved:
        token_transfers = tx.get("tokenTransfers") or []
        for tr in token_transfers:
            if (tr.get("mint") or "") == mint:
                mint_involved = True
                break
    if not mint_involved:
        return None

    trader = (tx.get("feePayer") or "").strip()
    if not trader:
        trader = first_account_fallback(tx)
    if not trader:
        sig = tx.get("signature") or "<unknown-signature>"
        raise LoudError("Unable to determine trader (missing feePayer/first account) for tx %s" % sig)

    token_net, _ = net_token_for_trader(tx, trader, mint)
    sol_net_lamports = net_sol_lamports_for_trader(tx, trader)

    side = "ANOMALY"
    anomaly_reason = ""
    if token_net > 0 and sol_net_lamports < 0:
        side = "BUY"
    elif token_net < 0 and sol_net_lamports > 0:
        side = "SELL"
    else:
        if token_net == 0 and sol_net_lamports == 0:
            anomaly_reason = "zero_token_and_zero_sol_net"
        elif token_net == 0:
            anomaly_reason = "zero_token_net"
        elif sol_net_lamports == 0:
            anomaly_reason = "zero_sol_net"
        elif token_net > 0 and sol_net_lamports > 0:
            anomaly_reason = "token_and_sol_both_in"
        elif token_net < 0 and sol_net_lamports < 0:
            anomaly_reason = "token_and_sol_both_out"
        else:
            anomaly_reason = "unexpected_sign_combination"

    ts = extract_block_time(tx)
    if ts is None:
        raise LoudError("Transaction missing timestamp/blockTime for signature %s" % (tx.get("signature") or ""))

    return {
        "ts": ts,
        "signature": tx.get("signature") or "",
        "slot": tx.get("slot") or "",
        "trader": trader,
        "dex_source": tx.get("source") or "",
        "side": side,
        "sol_amount": abs(sol_net_lamports) / float(LAMPORTS_PER_SOL),
        "token_amount": abs(token_net),
        "is_cented": "1" if trader == cented_wallet else "0",
        "anomaly_reason": anomaly_reason,
    }


def rolling_peak(rows, side):
    ts_values = [r["ts"] for r in rows if r["side"] == side]
    ts_values.sort()
    q = deque()
    peak = 0
    for ts in ts_values:
        q.append(ts)
        while q and q[0] < ts - 9:
            q.popleft()
        if len(q) > peak:
            peak = len(q)
    return peak


def write_timeline(path, rows):
    headers = [
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
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
        w.writeheader()
        for r in rows:
            out = dict(r)
            out["sol_amount"] = "%.12f" % out["sol_amount"]
            out["token_amount"] = "%.12f" % out["token_amount"]
            w.writerow(out)


def write_summary(path, rows):
    total_rows = len(rows)
    total_swaps = sum(1 for r in rows if r["side"] in ("BUY", "SELL"))
    anomalies = sum(1 for r in rows if r["side"] == "ANOMALY")
    unique_traders = len({r["trader"] for r in rows})
    buys = [r for r in rows if r["side"] == "BUY"]
    sells = [r for r in rows if r["side"] == "SELL"]

    min_ts = min((r["ts"] for r in rows), default="")
    max_ts = max((r["ts"] for r in rows), default="")

    lines = [
        "total_rows=%d" % total_rows,
        "total_swaps=%d" % total_swaps,
        "anomalies_count=%d" % anomalies,
        "unique_traders=%d" % unique_traders,
        "buys_count=%d" % len(buys),
        "sells_count=%d" % len(sells),
        "buy_sol_total=%.12f" % sum(r["sol_amount"] for r in buys),
        "sell_sol_total=%.12f" % sum(r["sol_amount"] for r in sells),
        "peak_buy_rate_10s=%d" % rolling_peak(rows, "BUY"),
        "peak_sell_rate_10s=%d" % rolling_peak(rows, "SELL"),
        "min_ts=%s" % min_ts,
        "max_ts=%s" % max_ts,
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write("\n".join(lines) + "\n")


def process_probe(args, probe):
    mint = probe["mint"]
    window_start = probe["entry_time"] - args.pad_seconds
    window_end = probe["exit_time"] + args.pad_seconds

    dossier_dir = os.path.join(args.outdir, "dossiers", mint)
    os.makedirs(dossier_dir, exist_ok=True)

    if args.verbose:
        print(
            "INFO: mint=%s window=[%d,%d]" % (mint, window_start, window_end),
            file=sys.stderr,
        )

    signatures = collect_signatures_in_window(
        args.api_key, mint, window_start, window_end, verbose=args.verbose
    )

    rows = []
    for i in range(0, len(signatures), ENHANCED_BATCH_SIZE):
        batch = signatures[i : i + ENHANCED_BATCH_SIZE]
        enhanced = get_enhanced_by_signatures(args.api_key, batch, verbose=args.verbose)
        for tx in enhanced:
            ts = extract_block_time(tx)
            if ts is None or ts < window_start or ts > window_end:
                continue
            analyzed = analyze_enhanced_tx(tx, mint, args.cented_wallet)
            if analyzed is not None:
                rows.append(analyzed)

    rows.sort(key=lambda r: (r["ts"], r["signature"]))

    write_timeline(os.path.join(dossier_dir, "pool_timeline.tsv"), rows)
    write_summary(os.path.join(dossier_dir, "pool_summary.txt"), rows)


def main():
    args = parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    trades = load_trades(args.trades_tsv)
    probes = select_probes(trades, args.select_mode, args.max_probes)

    if args.verbose:
        print("INFO: selected %d probes using mode=%s" % (len(probes), args.select_mode), file=sys.stderr)

    for idx, probe in enumerate(probes, start=1):
        print("Processing mint %d/%d: %s" % (idx, len(probes), probe["mint"]))
        process_probe(args, probe)

    print("OK: wrote dossiers for %d mints to %s" % (len(probes), args.outdir))


if __name__ == "__main__":
    try:
        main()
    except LoudError as exc:
        raise SystemExit("ERROR: %s" % exc)
