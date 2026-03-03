#!/usr/bin/env python3
"""
cented_edge_audit.py

Audit sampled mints with Helius transaction data.
"""

import os
import sys
import csv
import json
import time
import sqlite3
import requests
from collections import defaultdict, Counter

BASE_URL = "https://api-mainnet.helius-rpc.com"
PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
RAYDIUM_PROGRAM_IDS = {
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUQYQ5eR2Q3z9wKQfK",
    "CPMMoo8L3F4NbTegBCKVNqj8fJ8oPt7Q2Nm2nY6KQfK",
    "routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS",
}


def ensure_outdir(path):
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


class ErrorLogger:
    def __init__(self, path):
        self.path = path

    def log(self, msg):
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        with open(self.path, "a", encoding="utf-8", newline="") as f:
            f.write("[%s] %s\n" % (ts, msg))


class CacheDB:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS sig_cache (signature TEXT PRIMARY KEY, response TEXT NOT NULL, saved_at INTEGER NOT NULL)"
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS addr_cache (address TEXT NOT NULL, before_sig TEXT NOT NULL, response TEXT NOT NULL, saved_at INTEGER NOT NULL, PRIMARY KEY(address, before_sig))"
        )
        self.conn.commit()

    def get_sig(self, signature):
        row = self.conn.execute("SELECT response FROM sig_cache WHERE signature=?", (signature,)).fetchone()
        if row:
            return json.loads(row[0])
        return None

    def put_sig(self, signature, payload):
        self.conn.execute(
            "INSERT OR REPLACE INTO sig_cache(signature,response,saved_at) VALUES (?,?,?)",
            (signature, json.dumps(payload, separators=(",", ":")), int(time.time())),
        )
        self.conn.commit()

    def get_addr(self, address, before_sig):
        key = before_sig if before_sig else ""
        row = self.conn.execute(
            "SELECT response FROM addr_cache WHERE address=? AND before_sig=?", (address, key)
        ).fetchone()
        if row:
            return json.loads(row[0])
        return None

    def put_addr(self, address, before_sig, payload):
        key = before_sig if before_sig else ""
        self.conn.execute(
            "INSERT OR REPLACE INTO addr_cache(address,before_sig,response,saved_at) VALUES (?,?,?,?)",
            (address, key, json.dumps(payload, separators=(",", ":")), int(time.time())),
        )
        self.conn.commit()


class HeliusClient:
    def __init__(self, api_key, rate_limit_rps, cache, err):
        self.api_key = api_key
        self.rate_limit_rps = float(rate_limit_rps)
        if self.rate_limit_rps <= 0:
            raise ValueError("rate_limit_rps must be > 0")
        self.min_interval = 1.0 / self.rate_limit_rps
        self.cache = cache
        self.err = err
        self.last_call_ts = 0.0

    def _rate_limit(self):
        now = time.time()
        delay = self.min_interval - (now - self.last_call_ts)
        if delay > 0:
            time.sleep(delay)
        self.last_call_ts = time.time()

    def _request(self, method, path, params=None, json_payload=None):
        url = BASE_URL + path
        if params is None:
            params = {}
        params["api-key"] = self.api_key

        backoff = 0.5
        attempts = 0
        while attempts < 7:
            attempts += 1
            self._rate_limit()
            try:
                if method == "GET":
                    resp = requests.get(url, params=params, timeout=30)
                else:
                    resp = requests.post(url, params=params, json=json_payload, timeout=30)
            except Exception as ex:
                self.err.log("HTTP exception %s %s: %s" % (method, url, str(ex)))
                if attempts >= 7:
                    raise
                time.sleep(backoff)
                backoff *= 2
                continue

            if resp.status_code == 429:
                self.err.log("429 for %s %s (attempt %d)" % (method, url, attempts))
                if attempts >= 7:
                    raise RuntimeError("429 persisted for %s" % url)
                time.sleep(backoff)
                backoff *= 2
                continue

            if resp.status_code >= 400:
                body = resp.text[:800].replace("\n", " ")
                self.err.log("HTTP %d for %s %s: %s" % (resp.status_code, method, url, body))
                raise RuntimeError("HTTP %d for %s" % (resp.status_code, url))

            try:
                return resp.json()
            except Exception as ex:
                self.err.log("JSON decode failed %s %s: %s" % (method, url, str(ex)))
                raise

        raise RuntimeError("Unexpected retry loop exit")

    def get_transactions_by_signatures(self, signatures):
        out = {}
        need = []
        for sig in signatures:
            cached = self.cache.get_sig(sig)
            if cached is None:
                need.append(sig)
            else:
                out[sig] = cached

        batch_size = 100
        i = 0
        while i < len(need):
            batch = need[i : i + batch_size]
            i += batch_size
            payload = self._request("POST", "/v0/transactions/", json_payload=batch)
            if not isinstance(payload, list):
                raise RuntimeError("Unexpected POST /v0/transactions response type")
            for item in payload:
                sig = item.get("signature")
                if sig:
                    self.cache.put_sig(sig, item)
                    out[sig] = item
            # explicit missing signatures should be logged
            for sig in batch:
                if sig not in out:
                    self.err.log("Signature not returned by Helius: %s" % sig)
        return out

    def get_address_transactions(self, address, before_sig=""):
        cached = self.cache.get_addr(address, before_sig)
        if cached is not None:
            return cached
        params = {"limit": 100}
        if before_sig:
            params["before"] = before_sig
        payload = self._request("GET", "/v0/addresses/%s/transactions/" % address, params=params)
        if not isinstance(payload, list):
            raise RuntimeError("Unexpected address transactions response type")
        self.cache.put_addr(address, before_sig, payload)
        return payload



def parse_cli(argv):
    defaults = {
        "--tx-map": "cented_tx_mint_map.tsv",
        "--mint-pnl": "cented_mint_pnl.tsv",
        "--programs": "cented_programs_by_tx.tsv",
        "--trigger-deltas": "cented_trigger_deltas.tsv",
        "--outdir": "out",
        "--sample": "20",
        "--window-cowallet": "3",
        "--window-pool": "20",
        "--rate-limit-rps": "5",
    }
    i = 1
    while i < len(argv):
        k = argv[i]
        if k not in defaults:
            raise ValueError("Unknown argument: %s" % k)
        if i + 1 >= len(argv):
            raise ValueError("Missing value for %s" % k)
        defaults[k] = argv[i + 1]
        i += 2

    return {
        "tx_map": defaults["--tx-map"],
        "mint_pnl": defaults["--mint-pnl"],
        "programs": defaults["--programs"],
        "trigger_deltas": defaults["--trigger-deltas"],
        "outdir": defaults["--outdir"],
        "sample": int(defaults["--sample"]),
        "window_cowallet": int(defaults["--window-cowallet"]),
        "window_pool": int(defaults["--window-pool"]),
        "rate_limit_rps": float(defaults["--rate-limit-rps"]),
    }


def read_tsv(path):
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def write_tsv(path, rows, headers):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def to_int(v, default=0):
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except Exception:
        return default


def to_float(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def detect_col(row, options):
    for c in options:
        if c in row:
            return c
    return None


def extract_program_ids(parsed_tx):
    out = []
    for ins in parsed_tx.get("instructions", []) or []:
        pid = ins.get("programId")
        if pid:
            out.append(pid)
    for ins in parsed_tx.get("innerInstructions", []) or []:
        for ix in ins.get("instructions", []) or []:
            pid = ix.get("programId")
            if pid:
                out.append(pid)
    return sorted(set(out))


def extract_compute_budget(parsed_tx):
    limit = ""
    price = ""
    for ins in parsed_tx.get("instructions", []) or []:
        if ins.get("programId") != "ComputeBudget111111111111111111111111111111":
            continue
        data = ins.get("data")
        if isinstance(data, dict):
            d_type = data.get("type", "")
            info = data.get("info", {}) or {}
            if d_type == "setComputeUnitLimit":
                limit = str(info.get("units", ""))
            elif d_type == "setComputeUnitPrice":
                price = str(info.get("microLamports", ""))
        elif isinstance(data, str):
            # no decode under stdlib-only constraints
            pass
    return limit, price


def select_sample_mints(mint_pnl_rows, sample_n):
    if not mint_pnl_rows:
        return []
    mint_col = detect_col(mint_pnl_rows[0], ["mint", "mint_address", "token_mint"])
    pnl_col = detect_col(mint_pnl_rows[0], ["net_sol", "net", "pnl_sol"])
    if not mint_col or not pnl_col:
        raise RuntimeError("mint_pnl TSV must contain mint and net_sol-like columns")

    sorted_rows = sorted(mint_pnl_rows, key=lambda r: to_float(r.get(pnl_col), 0.0), reverse=True)
    if sample_n <= 0:
        return []
    winner_n = sample_n // 2
    loser_n = sample_n // 2
    if sample_n % 2 == 1:
        winner_n += 1
    winners = sorted_rows[:winner_n]
    losers = sorted_rows[-loser_n:] if loser_n > 0 else []
    selected = winners + losers
    seen = set()
    out = []
    for r in selected:
        m = r.get(mint_col, "")
        if m and m not in seen:
            out.append(m)
            seen.add(m)
    return out


def median_int(values):
    if not values:
        return 0
    arr = sorted(values)
    n = len(arr)
    mid = n // 2
    if n % 2 == 1:
        return arr[mid]
    return int((arr[mid - 1] + arr[mid]) / 2)


def find_first_swap_time_from_trigger(trigger_rows, mint):
    for r in trigger_rows:
        m = r.get("mint") or r.get("mint_address") or r.get("token_mint")
        if m != mint:
            continue
        t = r.get("first_swap_block_time") or r.get("first_swap_time") or r.get("swap_block_time")
        s = r.get("first_swap_slot") or r.get("swap_slot")
        if t:
            return to_int(t, 0), to_int(s, 0)
    return 0, 0


def has_mint_token_transfer(parsed_tx, mint):
    for e in parsed_tx.get("tokenTransfers", []) or []:
        if e.get("mint") == mint and to_float(e.get("tokenAmount"), 0.0) > 0:
            return True
    return False


def extract_mint_buyers_sellers(parsed_tx, mint):
    buyers = set()
    sellers = set()
    buy_count = 0
    sell_count = 0
    for e in parsed_tx.get("tokenTransfers", []) or []:
        if e.get("mint") != mint:
            continue
        amount = to_float(e.get("tokenAmount"), 0.0)
        if amount <= 0:
            continue
        to_u = e.get("toUserAccount") or ""
        from_u = e.get("fromUserAccount") or ""
        if to_u:
            buyers.add(to_u)
            buy_count += 1
        if from_u:
            sellers.add(from_u)
            sell_count += 1
    return buyers, sellers, buy_count, sell_count


def address_history_until(client, address, min_block_time):
    all_rows = []
    before = ""
    for _ in range(20):
        batch = client.get_address_transactions(address, before)
        if not batch:
            break
        all_rows.extend(batch)
        oldest_sig = batch[-1].get("signature", "")
        oldest_bt = to_int(batch[-1].get("timestamp") or batch[-1].get("blockTime"), 0)
        if oldest_bt and oldest_bt < min_block_time:
            break
        if not oldest_sig:
            break
        before = oldest_sig
    return all_rows


def main():
    try:
        args = parse_cli(sys.argv)
    except Exception as ex:
        sys.stderr.write("Argument error: %s\n" % str(ex))
        sys.exit(2)

    if "HELIUS_API_KEY" not in os.environ or not os.environ["HELIUS_API_KEY"].strip():
        sys.stderr.write("Error: HELIUS_API_KEY is required in environment.\n")
        sys.exit(1)

    ensure_outdir(args["outdir"])
    err = ErrorLogger(os.path.join(args["outdir"], "errors.log"))
    cache = CacheDB(os.path.join(args["outdir"], "helius_cache.sqlite"))
    client = HeliusClient(os.environ["HELIUS_API_KEY"].strip(), args["rate_limit_rps"], cache, err)

    try:
        tx_map_rows = read_tsv(args["tx_map"])
        mint_pnl_rows = read_tsv(args["mint_pnl"])
        program_rows = read_tsv(args["programs"])
        trigger_rows = read_tsv(args["trigger_deltas"])
    except Exception as ex:
        err.log("Input read failure: %s" % str(ex))
        raise

    sample_mints = select_sample_mints(mint_pnl_rows, args["sample"])

    # Build tx->mint map and mint buy events
    mint_tx_rows = []
    if tx_map_rows:
        sig_col = detect_col(tx_map_rows[0], ["signature", "tx", "txid"])
        mint_col = detect_col(tx_map_rows[0], ["mint", "mint_address", "token_mint"])
        td_col = detect_col(tx_map_rows[0], ["token_delta", "delta_tokens", "mint_delta"])
        bt_col = detect_col(tx_map_rows[0], ["blockTime", "block_time", "timestamp"])
        wallet_col = detect_col(tx_map_rows[0], ["wallet", "owner", "trader"])
        if not sig_col or not mint_col:
            raise RuntimeError("tx map TSV missing signature/mint columns")

        for r in tx_map_rows:
            mint = r.get(mint_col, "")
            if mint in sample_mints:
                mint_tx_rows.append(
                    {
                        "mint": mint,
                        "signature": r.get(sig_col, ""),
                        "token_delta": to_float(r.get(td_col), 0.0) if td_col else 0.0,
                        "block_time": to_int(r.get(bt_col), 0) if bt_col else 0,
                        "wallet": r.get(wallet_col, "") if wallet_col else "",
                    }
                )

    # first cented buy per mint
    first_buy = {}
    for mint in sample_mints:
        buys = [r for r in mint_tx_rows if r["mint"] == mint and r["token_delta"] > 0]
        buys = sorted(buys, key=lambda x: (x["block_time"], x["signature"]))
        if buys:
            first_buy[mint] = dict(buys[0])
        else:
            err.log("No first buy found in tx map for mint %s" % mint)

    # Load parsed tx for all sample signatures
    all_sample_sigs = sorted(set([r["signature"] for r in mint_tx_rows if r["signature"]]))
    parsed_by_sig = {}
    if all_sample_sigs:
        parsed_by_sig = client.get_transactions_by_signatures(all_sample_sigs)

    # Refresh first-buy block_time with parsed transactions when available.
    for mint in list(first_buy.keys()):
        sig = first_buy[mint].get("signature", "")
        if not sig:
            continue
        p = parsed_by_sig.get(sig)
        if not p:
            err.log("Missing parsed tx for first-buy signature %s (mint %s)" % (sig, mint))
            continue
        parsed_bt = to_int(p.get("timestamp") or p.get("blockTime"), 0)
        if parsed_bt > 0:
            first_buy[mint]["block_time"] = parsed_bt

    # Extra map for declared program IDs by tx table
    programs_by_sig = defaultdict(set)
    if program_rows:
        psig_col = detect_col(program_rows[0], ["signature", "tx", "txid"])
        pprog_col = detect_col(program_rows[0], ["program_id", "program", "programId"])
        if psig_col and pprog_col:
            for r in program_rows:
                sig = r.get(psig_col, "")
                pid = r.get(pprog_col, "")
                if sig and pid:
                    programs_by_sig[sig].add(pid)

    # A) slot timing audit
    a_rows = []
    for mint in sample_mints:
        fb = first_buy.get(mint)
        if not fb:
            a_rows.append(
                {
                    "mint": mint,
                    "first_buy_signature": "",
                    "buy_block_time": "",
                    "buy_slot": "",
                    "first_swap_block_time": "",
                    "first_swap_slot": "",
                    "delta_seconds": "",
                    "delta_slots": "",
                    "source": "missing_first_buy",
                }
            )
            continue

        sig = fb["signature"]
        parsed = parsed_by_sig.get(sig, {})
        buy_slot = to_int(parsed.get("slot"), 0)
        buy_bt = to_int(parsed.get("timestamp") or parsed.get("blockTime"), fb["block_time"])

        first_swap_bt, first_swap_slot = find_first_swap_time_from_trigger(trigger_rows, mint)
        source = "trigger"
        if not first_swap_bt:
            source = "history"
            min_bt = buy_bt - 300
            rows = address_history_until(client, mint, min_bt)
            candidate = None
            for r in sorted(rows, key=lambda x: (to_int(x.get("timestamp") or x.get("blockTime"), 0), x.get("signature", ""))):
                bt = to_int(r.get("timestamp") or r.get("blockTime"), 0)
                if not bt or bt > buy_bt or bt < min_bt:
                    continue
                if has_mint_token_transfer(r, mint):
                    candidate = r
                    break
            if candidate:
                first_swap_bt = to_int(candidate.get("timestamp") or candidate.get("blockTime"), 0)
                first_swap_slot = to_int(candidate.get("slot"), 0)
            else:
                err.log("No fallback swap history found for mint %s" % mint)

        dsec = ""
        dslot = ""
        if first_swap_bt and buy_bt:
            dsec = str(buy_bt - first_swap_bt)
        if first_swap_slot and buy_slot:
            dslot = str(buy_slot - first_swap_slot)

        a_rows.append(
            {
                "mint": mint,
                "first_buy_signature": sig,
                "buy_block_time": buy_bt,
                "buy_slot": buy_slot,
                "first_swap_block_time": first_swap_bt,
                "first_swap_slot": first_swap_slot,
                "delta_seconds": dsec,
                "delta_slots": dslot,
                "source": source,
            }
        )

    write_tsv(
        os.path.join(args["outdir"], "cented_A_slot_timing.tsv"),
        a_rows,
        [
            "mint",
            "first_buy_signature",
            "buy_block_time",
            "buy_slot",
            "first_swap_block_time",
            "first_swap_slot",
            "delta_seconds",
            "delta_slots",
            "source",
        ],
    )

    # B) priority + compute audit
    b_rows = []
    for r in sorted(mint_tx_rows, key=lambda x: (x["mint"], x["block_time"], x["signature"])):
        sig = r["signature"]
        p = parsed_by_sig.get(sig, {})
        cu_limit, cu_price = extract_compute_budget(p)
        pids = set(extract_program_ids(p))
        pids.update(programs_by_sig.get(sig, set()))
        b_rows.append(
            {
                "mint": r["mint"],
                "signature": sig,
                "block_time": to_int(p.get("timestamp") or p.get("blockTime"), r["block_time"]),
                "slot": to_int(p.get("slot"), 0),
                "fee_lamports": to_int(p.get("fee"), 0),
                "compute_units_consumed": to_int(p.get("computeUnitsConsumed"), 0),
                "compute_unit_limit": cu_limit,
                "compute_unit_price": cu_price,
                "program_ids": ",".join(sorted(pids)),
            }
        )

    write_tsv(
        os.path.join(args["outdir"], "cented_B_priority_compute.tsv"),
        b_rows,
        [
            "mint",
            "signature",
            "block_time",
            "slot",
            "fee_lamports",
            "compute_units_consumed",
            "compute_unit_limit",
            "compute_unit_price",
            "program_ids",
        ],
    )

    # C) jito tip detection
    tip_file = "jito_tip_accounts.txt"
    tip_accounts = set()
    if os.path.isfile(tip_file):
        with open(tip_file, "r", encoding="utf-8") as f:
            for line in f:
                a = line.strip()
                if a:
                    tip_accounts.add(a)

    c_rows = []
    for r in b_rows:
        p = parsed_by_sig.get(r["signature"], {})
        tipped = 0
        tip_lamports = 0
        if tip_accounts:
            native = p.get("nativeTransfers", []) or []
            for n in native:
                to_acc = n.get("toUserAccount") or ""
                if to_acc in tip_accounts:
                    tipped = 1
                    tip_lamports += to_int(n.get("amount"), 0)
        c_rows.append(
            {
                "mint": r["mint"],
                "signature": r["signature"],
                "tipped": tipped,
                "tip_lamports": tip_lamports,
            }
        )

    write_tsv(
        os.path.join(args["outdir"], "cented_C_jito_tips.tsv"),
        c_rows,
        ["mint", "signature", "tipped", "tip_lamports"],
    )

    # D + E require mint histories
    mint_histories = {}
    for mint in sample_mints:
        fb = first_buy.get(mint)
        if not fb:
            mint_histories[mint] = []
            continue
        buy_bt = fb["block_time"]
        min_bt = buy_bt - max(300, args["window_pool"] + 10, args["window_cowallet"] + 10)
        rows = address_history_until(client, mint, min_bt)
        mint_histories[mint] = rows

    # D) co-wallet detection
    event_rows = []
    wallet_mints = defaultdict(set)
    for mint in sample_mints:
        fb = first_buy.get(mint)
        if not fb:
            continue
        center = fb["block_time"]
        for tx in mint_histories.get(mint, []):
            bt = to_int(tx.get("timestamp") or tx.get("blockTime"), 0)
            if bt == 0:
                continue
            if abs(bt - center) > args["window_cowallet"]:
                continue
            buyers, _sellers, buy_count, _sell_count = extract_mint_buyers_sellers(tx, mint)
            if buy_count <= 0:
                continue
            for w in sorted(buyers):
                if w == fb.get("wallet"):
                    continue
                event_rows.append(
                    {
                        "mint": mint,
                        "cented_buy_time": center,
                        "event_time": bt,
                        "signature": tx.get("signature", ""),
                        "wallet": w,
                    }
                )
                wallet_mints[w].add(mint)

    lb_rows = []
    for wallet, mint_set in sorted(wallet_mints.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        lb_rows.append({"wallet": wallet, "cooccurrences": len(mint_set)})

    write_tsv(
        os.path.join(args["outdir"], "cented_D_cowallet_events.tsv"),
        event_rows,
        ["mint", "cented_buy_time", "event_time", "signature", "wallet"],
    )
    write_tsv(
        os.path.join(args["outdir"], "cented_D_cowallet_leaderboard.tsv"),
        lb_rows,
        ["wallet", "cooccurrences"],
    )

    # E) pre-entry features
    e_rows = []
    for mint in sample_mints:
        fb = first_buy.get(mint)
        if not fb:
            continue
        buy_sig = fb["signature"]
        buy_time = fb["block_time"]

        pids = set()
        pids.update(programs_by_sig.get(buy_sig, set()))
        pids.update(extract_program_ids(parsed_by_sig.get(buy_sig, {})))

        route = "Other"
        if PUMP_FUN_PROGRAM_ID in pids:
            route = "Pump.fun"
        elif len(pids.intersection(RAYDIUM_PROGRAM_IDS)) > 0:
            route = "Raydium"

        unique_10s = set()
        buys_30 = 0
        sells_30 = 0
        for tx in mint_histories.get(mint, []):
            bt = to_int(tx.get("timestamp") or tx.get("blockTime"), 0)
            if not bt:
                continue
            buyers, _sellers, buys, sells = extract_mint_buyers_sellers(tx, mint)
            if buy_time - 10 <= bt < buy_time:
                for w in buyers:
                    unique_10s.add(w)
            if abs(bt - buy_time) <= 30:
                buys_30 += buys
                sells_30 += sells

        e_rows.append(
            {
                "mint": mint,
                "buy_signature": buy_sig,
                "buy_time": buy_time,
                "route_type": route,
                "unique_buyers_10s_before": len(unique_10s),
                "buys_pm30s": buys_30,
                "sells_pm30s": sells_30,
                "imbalance": buys_30 - sells_30,
            }
        )

    write_tsv(
        os.path.join(args["outdir"], "cented_E_preentry_features.tsv"),
        e_rows,
        [
            "mint",
            "buy_signature",
            "buy_time",
            "route_type",
            "unique_buyers_10s_before",
            "buys_pm30s",
            "sells_pm30s",
            "imbalance",
        ],
    )

    # Summary
    dslots = [to_int(r.get("delta_slots"), 0) for r in a_rows if str(r.get("delta_slots", "")).strip() != ""]
    pct_le2 = 0.0
    if dslots:
        le2 = len([x for x in dslots if x <= 2])
        pct_le2 = 100.0 * float(le2) / float(len(dslots))

    cu_prices = [to_int(r.get("compute_unit_price"), 0) for r in b_rows if str(r.get("compute_unit_price", "")).strip() != ""]
    cu_dist = Counter(cu_prices)

    tipped_count = len([r for r in c_rows if to_int(r.get("tipped"), 0) == 1])
    tipped_pct = (100.0 * tipped_count / len(c_rows)) if c_rows else 0.0

    summary_path = os.path.join(args["outdir"], "summary.txt")
    with open(summary_path, "w", encoding="utf-8", newline="") as f:
        f.write("Sampled mints: %d\n" % len(sample_mints))
        f.write("Entries with delta_slots <= 2: %.2f%%\n" % pct_le2)
        f.write("Median delta_slots: %d\n" % median_int(dslots))
        f.write("Compute unit price distribution:\n")
        for k in sorted(cu_dist.keys()):
            f.write("  %d\t%d\n" % (k, cu_dist[k]))
        f.write("Percent tipped: %.2f%%\n" % tipped_pct)
        f.write("Top 10 co-wallets:\n")
        for row in lb_rows[:10]:
            f.write("  %s\t%s\n" % (row["wallet"], row["cooccurrences"]))

    print("Done. Outputs in %s" % args["outdir"])


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        outdir = "out"
        # best effort error log without hiding failure
        try:
            ensure_outdir(outdir)
            ErrorLogger(os.path.join(outdir, "errors.log")).log("Fatal: %s" % str(ex))
        except Exception:
            pass
        sys.stderr.write("Fatal error: %s\n" % str(ex))
        sys.exit(1)

# README USAGE
# python cented_edge_audit.py ^
#   --tx-map cented_tx_mint_map.tsv ^
#   --mint-pnl cented_mint_pnl.tsv ^
#   --programs cented_programs_by_tx.tsv ^
#   --trigger-deltas cented_trigger_deltas.tsv ^
#   --outdir out ^
#   --sample 20 ^
#   --window-cowallet 3 ^
#   --window-pool 20 ^
#   --rate-limit-rps 5
