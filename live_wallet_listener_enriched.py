#!/usr/bin/env python3
"""
live_wallet_listener_enriched.py

Live single-wallet Solana listener with enrichment, dedupe, and audit logging.

Requirements:
  - requests
  - websocket-client (preferred for WS mode)
  - websockets (optional fallback WS client)

If WebSocket libraries are unavailable or WS repeatedly fails, script falls back to polling.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import queue
import random
import threading
import time
from collections import deque
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests

DEFAULT_WALLET = "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o"
DEFAULT_OUT = "Cented.live.jsonl"
DEFAULT_SEEN = "Cented.live.seen.txt"
DEFAULT_AUDIT = "Cented.live.audit.json"
MIN_REQUEST_GAP_SEC = 0.15
MAX_WS_RETRIES_BEFORE_POLL = 6


COMPUTE_BUDGET_PROGRAM = "ComputeBudget111111111111111111111111111111"


class ListenerState:
    def __init__(self, wallet: str, wallet_owner: str, out_path: Path, seen_path: Path, audit_path: Path) -> None:
        self.wallet = wallet
        self.wallet_owner = wallet_owner
        self.out_path = out_path
        self.seen_path = seen_path
        self.audit_path = audit_path

        self.sig_queue: queue.Queue[Tuple[str, str]] = queue.Queue()
        self.seen: Set[str] = set()
        self.seen_lock = threading.Lock()
        self.owner_cache: Dict[str, Optional[str]] = {}

        self.last_rpc_ts = 0.0
        self.last_owner_rpc_ts = 0.0

        self.audit: Dict[str, Any] = {
            "started_utc": utc_now_iso(),
            "last_seen_signature": None,
            "total_written": 0,
            "last_write_utc": None,
            "ws_reconnects": 0,
            "rpc_calls": 0,
            "owner_rpc_calls": 0,
            "owner_cache_hits": 0,
            "errors_count": 0,
            "priority_spikes": 0,
            "extreme_priority_spikes": 0,
            "total_buys": 0,
            "total_sells": 0,
            "burst_events": 0,
            "cluster_events": 0,
            "large_sol_moves": 0,
        }
        self.audit_lock = threading.Lock()

        self.last_60s_trades = deque()
        self.last_trade_timestamp: Optional[float] = None
        self.cluster_count = 0
        self.trade_lock = threading.Lock()


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def short_sig(sig: str) -> str:
    if len(sig) < 14:
        return sig
    return f"{sig[:7]}...{sig[-7:]}"


def atomic_write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)


def atomic_write_lines(path: Path, lines: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        for line in lines:
            f.write(line)
            f.write("\n")
    os.replace(tmp, path)


def load_seen(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    out: Set[str] = set()
    with path.open("r", encoding="utf-8") as f:
        for raw in f:
            s = raw.strip()
            if s:
                out.add(s)
    return out


def save_seen_atomic(state: ListenerState) -> None:
    with state.seen_lock:
        sigs = sorted(state.seen)
    atomic_write_lines(state.seen_path, sigs)


def update_audit(state: ListenerState, **kwargs: Any) -> None:
    with state.audit_lock:
        state.audit.update(kwargs)
        snapshot = dict(state.audit)
    atomic_write_json(state.audit_path, snapshot)


def backoff_sleep(attempt: int, base: float = 0.5, cap: float = 8.0) -> None:
    delay = min(cap, base * (2 ** max(0, attempt - 1)))
    delay = delay * (0.8 + random.random() * 0.4)
    time.sleep(delay)


class RpcClient:
    def __init__(self, state: ListenerState, rpc_url: str):
        self.state = state
        self.rpc_url = rpc_url
        self.session = requests.Session()
        self.req_id = 1

    def _rate_limit(self) -> None:
        now = time.time()
        gap = now - self.state.last_rpc_ts
        if gap < MIN_REQUEST_GAP_SEC:
            time.sleep(MIN_REQUEST_GAP_SEC - gap)
        self.state.last_rpc_ts = time.time()

    def call(self, method: str, params: List[Any], *, owner_call: bool = False) -> Any:
        payload = {
            "jsonrpc": "2.0",
            "id": self.req_id,
            "method": method,
            "params": params,
        }
        self.req_id += 1

        max_attempts = 7
        for attempt in range(1, max_attempts + 1):
            self._rate_limit()
            try:
                resp = self.session.post(self.rpc_url, json=payload, timeout=20)
                if resp.status_code == 429:
                    raise requests.HTTPError("429 Too Many Requests")
                resp.raise_for_status()
                data = resp.json()
                with self.state.audit_lock:
                    self.state.audit["rpc_calls"] += 1
                    if owner_call:
                        self.state.audit["owner_rpc_calls"] += 1
                if "error" in data and data["error"]:
                    err_msg = str(data["error"])
                    if "Too many requests" in err_msg or "429" in err_msg:
                        raise RuntimeError(err_msg)
                    return None
                return data.get("result")
            except Exception:
                if attempt == max_attempts:
                    with self.state.audit_lock:
                        self.state.audit["errors_count"] += 1
                    update_audit(self.state)
                    return None
                backoff_sleep(attempt)
        return None

    def get_transaction(self, signature: str) -> Optional[Dict[str, Any]]:
        # "jsonParsed" preserves token balances and parsed accounts cleanly.
        return self.call(
            "getTransaction",
            [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
        )

    def get_signatures_for_address(self, wallet: str, limit: int = 50) -> List[Dict[str, Any]]:
        result = self.call("getSignaturesForAddress", [wallet, {"limit": limit}])
        return result if isinstance(result, list) else []

    def get_account_owner(self, token_account: str) -> Optional[str]:
        if token_account in self.state.owner_cache:
            with self.state.audit_lock:
                self.state.audit["owner_cache_hits"] += 1
            return self.state.owner_cache[token_account]

        result = self.call(
            "getAccountInfo",
            [token_account, {"encoding": "jsonParsed", "commitment": "confirmed"}],
            owner_call=True,
        )
        owner = None
        try:
            owner = result["value"]["data"]["parsed"]["info"]["owner"]
        except Exception:
            owner = None
        self.state.owner_cache[token_account] = owner
        return owner


def _normalize_account_keys(tx: Dict[str, Any]) -> List[str]:
    msg = ((tx or {}).get("transaction") or {}).get("message") or {}
    out: List[str] = []
    for k in msg.get("accountKeys") or []:
        if isinstance(k, dict):
            pubkey = k.get("pubkey")
            if pubkey:
                out.append(pubkey)
        elif isinstance(k, str):
            out.append(k)
    return out


def _resolve_program_id(ix: Dict[str, Any], account_keys: List[str]) -> Optional[str]:
    if not isinstance(ix, dict):
        return None
    pid = ix.get("programId")
    if pid:
        return pid
    idx = ix.get("programIdIndex")
    if isinstance(idx, int) and 0 <= idx < len(account_keys):
        return account_keys[idx]
    return None


def _collect_program_ids(tx: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    account_keys = _normalize_account_keys(tx)
    top: List[str] = []
    inner: List[str] = []

    msg = ((tx or {}).get("transaction") or {}).get("message") or {}
    for ix in msg.get("instructions") or []:
        pid = _resolve_program_id(ix, account_keys)
        if pid:
            top.append(pid)

    meta = (tx or {}).get("meta") or {}
    for inner_set in meta.get("innerInstructions") or []:
        for ix in (inner_set or {}).get("instructions") or []:
            pid = _resolve_program_id(ix, account_keys)
            if pid:
                inner.append(pid)

    return sorted(set(top)), sorted(set(inner))


def _extract_compute_budget_params(tx: Dict[str, Any]) -> Tuple[bool, Optional[int], Optional[int]]:
    account_keys = _normalize_account_keys(tx)
    msg = ((tx or {}).get("transaction") or {}).get("message") or {}

    found = False
    units: Optional[int] = None
    micro_lamports: Optional[int] = None

    def parse_ix(ix: Dict[str, Any]) -> None:
        nonlocal found, units, micro_lamports
        pid = _resolve_program_id(ix, account_keys)
        if pid != COMPUTE_BUDGET_PROGRAM:
            return
        found = True
        parsed = ix.get("parsed") if isinstance(ix, dict) else None
        if isinstance(parsed, dict):
            typ = str(parsed.get("type") or "")
            info = parsed.get("info") or {}
            if typ.lower() in {"setcomputeunitlimit", "set_compute_unit_limit"}:
                v = info.get("units")
                if isinstance(v, int):
                    units = v
            if typ.lower() in {"setcomputeunitprice", "set_compute_unit_price"}:
                v = info.get("microLamports")
                if isinstance(v, int):
                    micro_lamports = v

    for ix in msg.get("instructions") or []:
        parse_ix(ix)

    meta = (tx or {}).get("meta") or {}
    for inner_set in meta.get("innerInstructions") or []:
        for ix in (inner_set or {}).get("instructions") or []:
            parse_ix(ix)

    return found, units, micro_lamports


def _best_tx_time(tx: Dict[str, Any]) -> Optional[int]:
    block_time = tx.get("blockTime")
    if isinstance(block_time, int):
        return block_time
    for key in ("timestamp", "time"):
        v = tx.get(key)
        if isinstance(v, int):
            return v
    return None


def _safe_int_str(val: Any) -> str:
    if val is None:
        return "0"
    if isinstance(val, str):
        return val
    if isinstance(val, int):
        return str(val)
    try:
        return str(int(val))
    except Exception:
        return "0"


def _to_decimal_sol(lamports: int) -> str:
    return str((Decimal(lamports) / Decimal(1_000_000_000)).normalize())


def extract_spl_transfers(
    tx: Dict[str, Any],
    wallet: str,
    signature: str,
    rpc: RpcClient,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    meta = (tx or {}).get("meta") or {}
    pre = meta.get("preTokenBalances") or []
    post = meta.get("postTokenBalances") or []

    def owner_for_tb(tb: Dict[str, Any]) -> Optional[str]:
        owner = tb.get("owner")
        if owner:
            return owner
        acct_idx = tb.get("accountIndex")
        if not isinstance(acct_idx, int):
            return None
        keys = _normalize_account_keys(tx)
        if not (0 <= acct_idx < len(keys)):
            return None
        token_account = keys[acct_idx]
        return rpc.get_account_owner(token_account)

    def key_of(tb: Dict[str, Any]) -> Tuple[Any, str, str]:
        return tb.get("accountIndex"), tb.get("mint") or "", owner_for_tb(tb) or ""

    pre_map: Dict[Tuple[Any, str, str], Dict[str, Any]] = {key_of(tb): tb for tb in pre if isinstance(tb, dict)}
    post_map: Dict[Tuple[Any, str, str], Dict[str, Any]] = {key_of(tb): tb for tb in post if isinstance(tb, dict)}

    all_keys = set(pre_map.keys()) | set(post_map.keys())
    incoming: List[Dict[str, Any]] = []
    outgoing: List[Dict[str, Any]] = []

    for k in all_keys:
        acct_idx, mint, owner = k
        if owner != wallet:
            continue
        pre_tb = pre_map.get(k, {})
        post_tb = post_map.get(k, {})
        pre_amt = int(_safe_int_str((((pre_tb.get("uiTokenAmount") or {})).get("amount"))))
        post_amt = int(_safe_int_str((((post_tb.get("uiTokenAmount") or {})).get("amount"))))
        delta = post_amt - pre_amt
        if delta == 0:
            continue

        transfer = {
            "mint": mint,
            "direction": "in" if delta > 0 else "out",
            "amount": str(abs(delta)),
            "from_addr": None,
            "to_addr": None,
            "signature": signature,
        }
        if delta > 0:
            transfer["to_addr"] = wallet
            incoming.append(transfer)
        else:
            transfer["from_addr"] = wallet
            outgoing.append(transfer)

    return incoming, outgoing


def enrich_transaction(
    tx: Dict[str, Any],
    wallet: str,
    wallet_owner: str,
    signature: str,
    observed_utc: str,
    rpc: RpcClient,
) -> Dict[str, Any]:
    meta = (tx or {}).get("meta") or {}
    msg = ((tx or {}).get("transaction") or {}).get("message") or {}

    top_ids, inner_ids = _collect_program_ids(tx)
    cb_present, cb_units, cb_micro = _extract_compute_budget_params(tx)
    spl_in, spl_out = extract_spl_transfers(tx, wallet, signature, rpc)

    slot = tx.get("slot")
    fee = meta.get("fee")
    err = meta.get("err")

    pre_sol = None
    post_sol = None
    delta_sol = None
    keys = _normalize_account_keys(tx)
    if wallet in keys:
        idx = keys.index(wallet)
        pre_balances = meta.get("preBalances") or []
        post_balances = meta.get("postBalances") or []
        if idx < len(pre_balances) and idx < len(post_balances):
            pre_lamports = int(pre_balances[idx])
            post_lamports = int(post_balances[idx])
            d_lamports = post_lamports - pre_lamports
            pre_sol = _to_decimal_sol(pre_lamports)
            post_sol = _to_decimal_sol(post_lamports)
            delta_sol = _to_decimal_sol(d_lamports)

    spl_in_count = len(spl_in)
    spl_out_count = len(spl_out)
    sol_delta_float = None
    try:
        sol_delta_float = float(delta_sol) if delta_sol is not None else None
    except Exception:
        sol_delta_float = None

    if spl_in_count > 0 and sol_delta_float is not None and sol_delta_float < 0:
        trade_type = "BUY"
    elif spl_out_count > 0 and sol_delta_float is not None and sol_delta_float > 0:
        trade_type = "SELL"
    else:
        trade_type = "OTHER"

    enriched = {
        "scan_wallet": wallet,
        "wallet_owner": wallet_owner,
        "mode": "live",
        "observed_utc": observed_utc,
        "sig0": signature,
        "tx_time": _best_tx_time(tx),
        "slot": slot,
        "fee_lamports": fee,
        "err": err,
        "computeBudget_present": cb_present,
        "computeBudget_units": cb_units,
        "computeBudget_microLamports": cb_micro,
        "program_ids_top": top_ids,
        "program_ids_inner": inner_ids,
        "spl_in_transfers": spl_in,
        "spl_out_transfers": spl_out,
        "pre_balance_SOL": pre_sol,
        "post_balance_SOL": post_sol,
        "balance_delta_SOL": delta_sol,
        "trade_type": trade_type,
        "trades_last_60s": 0,
        "cluster_count": 0,
        "tx": tx,
    }
    return enriched


def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))
        f.write("\n")
        f.flush()


def mark_seen(state: ListenerState, signature: str) -> bool:
    with state.seen_lock:
        if signature in state.seen:
            return False
        state.seen.add(signature)
    save_seen_atomic(state)
    return True


def process_signature(state: ListenerState, rpc: RpcClient, signature: str, observed_utc: str) -> None:
    if not mark_seen(state, signature):
        return

    tx = rpc.get_transaction(signature)
    if not tx:
        return

    enriched = enrich_transaction(tx, state.wallet, state.wallet_owner, signature, observed_utc, rpc)

    with state.audit_lock:
        state.audit["total_written"] += 1
        state.audit["last_seen_signature"] = signature
        state.audit["last_write_utc"] = utc_now_iso()
    
    cb_micro = enriched.get("computeBudget_microLamports")
    trade_type = enriched.get("trade_type") or "OTHER"
    sol_delta = enriched.get("balance_delta_SOL")
    spl_in_count = len(enriched.get("spl_in_transfers") or [])
    spl_out_count = len(enriched.get("spl_out_transfers") or [])

    now_ts = time.time()
    trades_last_60s = 0
    cluster_count = 0
    with state.trade_lock:
        if trade_type in {"BUY", "SELL"}:
            state.last_60s_trades.append(now_ts)
        while state.last_60s_trades and (now_ts - state.last_60s_trades[0]) > 60:
            state.last_60s_trades.popleft()
        trades_last_60s = len(state.last_60s_trades)

        if trade_type in {"BUY", "SELL"}:
            if state.last_trade_timestamp is not None and abs(now_ts - state.last_trade_timestamp) <= 2:
                state.cluster_count += 1
            else:
                state.cluster_count = 1
            state.last_trade_timestamp = now_ts
            cluster_count = state.cluster_count
        else:
            cluster_count = state.cluster_count

    enriched["trades_last_60s"] = trades_last_60s
    enriched["cluster_count"] = cluster_count

    append_jsonl(state.out_path, enriched)

    with state.audit_lock:
        if isinstance(cb_micro, int) and cb_micro > 500000:
            state.audit["priority_spikes"] += 1
        if isinstance(cb_micro, int) and cb_micro > 2000000:
            state.audit["extreme_priority_spikes"] += 1
        if trade_type == "BUY":
            state.audit["total_buys"] += 1
        elif trade_type == "SELL":
            state.audit["total_sells"] += 1
        if trades_last_60s >= 5:
            state.audit["burst_events"] += 1
        if cluster_count >= 3:
            state.audit["cluster_events"] += 1
        try:
            if sol_delta is not None and abs(float(sol_delta)) >= 5:
                state.audit["large_sol_moves"] += 1
        except Exception:
            pass
        audit_snapshot = dict(state.audit)
    atomic_write_json(state.audit_path, audit_snapshot)

    if isinstance(cb_micro, int) and cb_micro > 2000000:
        print(f"🚨 EXTREME PRIORITY {short_sig(signature)} uL={cb_micro}", flush=True)
    elif isinstance(cb_micro, int) and cb_micro > 500000:
        print(f"🔴 PRIORITY SPIKE {short_sig(signature)} uL={cb_micro}", flush=True)

    if trades_last_60s >= 5:
        print(f"⚡ BURST DETECTED: {trades_last_60s} trades in 60s", flush=True)

    if cluster_count >= 3:
        print(f"🐋 CLUSTER BURST x{cluster_count}", flush=True)

    try:
        if sol_delta is not None and abs(float(sol_delta)) >= 5:
            print(f"💰 LARGE SOL MOVE: {sol_delta}", flush=True)
    except Exception:
        pass

    print(
        f"{observed_utc} {trade_type} {short_sig(signature)} slot={enriched.get('slot')} "
        f"fee={enriched.get('fee_lamports')} uL={cb_micro} sol={sol_delta} "
        f"in={spl_in_count} out={spl_out_count} 60s={trades_last_60s} cluster={cluster_count}",
        flush=True,
    )


def worker_loop(state: ListenerState, rpc: RpcClient) -> None:
    while True:
        sig, observed_utc = state.sig_queue.get()
        try:
            process_signature(state, rpc, sig, observed_utc)
        except Exception as e:
            with state.audit_lock:
                state.audit["errors_count"] += 1
            update_audit(state)
            print(f"[warn] failed to process {short_sig(sig)}: {e}", flush=True)


def enqueue_signature(state: ListenerState, signature: Optional[str], observed_utc: Optional[str] = None) -> None:
    if not signature:
        return
    obs = observed_utc or utc_now_iso()
    with state.seen_lock:
        if signature in state.seen:
            return
    state.sig_queue.put((signature, obs))


def try_ws_listen_with_websocket_client(state: ListenerState, ws_url: str, wallet: str) -> bool:
    try:
        import websocket  # type: ignore
    except Exception:
        return False

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "logsSubscribe",
        "params": [{"mentions": [wallet]}, {"commitment": "confirmed"}],
    }

    for attempt in range(1, MAX_WS_RETRIES_BEFORE_POLL + 1):
        try:
            ws = websocket.create_connection(ws_url, timeout=30)
            ws.send(json.dumps(payload))
            _ = ws.recv()  # subscribe ack
            while True:
                raw = ws.recv()
                msg = json.loads(raw)
                params = msg.get("params") or {}
                result = params.get("result") or {}
                value = result.get("value") or {}
                sig = value.get("signature")
                if sig:
                    enqueue_signature(state, sig, utc_now_iso())
        except Exception as e:
            with state.audit_lock:
                state.audit["ws_reconnects"] += 1
                state.audit["errors_count"] += 1
            update_audit(state)
            print(f"[warn] websocket-client reconnect {attempt}: {e}", flush=True)
            backoff_sleep(attempt)
            continue
    return False


def try_ws_listen_with_websockets(state: ListenerState, ws_url: str, wallet: str) -> bool:
    try:
        import asyncio
        import websockets  # type: ignore
    except Exception:
        return False

    async def run_once() -> None:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "logsSubscribe",
            "params": [{"mentions": [wallet]}, {"commitment": "confirmed"}],
        }
        async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
            await ws.send(json.dumps(payload))
            await ws.recv()
            while True:
                raw = await ws.recv()
                msg = json.loads(raw)
                params = msg.get("params") or {}
                result = params.get("result") or {}
                value = result.get("value") or {}
                sig = value.get("signature")
                if sig:
                    enqueue_signature(state, sig, utc_now_iso())

    for attempt in range(1, MAX_WS_RETRIES_BEFORE_POLL + 1):
        try:
            asyncio.run(run_once())
        except Exception as e:
            with state.audit_lock:
                state.audit["ws_reconnects"] += 1
                state.audit["errors_count"] += 1
            update_audit(state)
            print(f"[warn] websockets reconnect {attempt}: {e}", flush=True)
            backoff_sleep(attempt)
            continue
    return False


def polling_loop(state: ListenerState, rpc: RpcClient) -> None:
    print("[info] entering polling mode", flush=True)
    while True:
        try:
            rows = rpc.get_signatures_for_address(state.wallet, limit=50)
            for row in reversed(rows):
                sig = row.get("signature") if isinstance(row, dict) else None
                enqueue_signature(state, sig, utc_now_iso())
            update_audit(state)
        except Exception as e:
            with state.audit_lock:
                state.audit["errors_count"] += 1
            update_audit(state)
            print(f"[warn] polling iteration failed: {e}", flush=True)
        time.sleep(1.0)


def pick_endpoints(api_key: str) -> Tuple[str, Optional[str]]:
    if api_key:
        rpc = f"https://mainnet.helius-rpc.com/?api-key={api_key}"
        ws = f"wss://mainnet.helius-rpc.com/?api-key={api_key}"
    else:
        rpc = "https://api.mainnet-beta.solana.com"
        ws = "wss://api.mainnet-beta.solana.com"
    return rpc, ws


def sanitize_owner_name(owner: str) -> str:
    owner = owner.replace(" ", "_")
    return "".join(ch for ch in owner if ch.isalnum() or ch in "_.-")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live enriched wallet listener for Solana")
    parser.add_argument("--wallet", default=None, help="wallet pubkey to track")
    parser.add_argument("--out", default=None, help="append-only JSONL output path")
    parser.add_argument("--seen", default=DEFAULT_SEEN, help="seen-signature file path")
    parser.add_argument("--audit", default=DEFAULT_AUDIT, help="audit sidecar JSON path")
    parser.add_argument("--ws", action="store_true", help="Enable websocket mode")
    parser.add_argument("--poll", action="store_true", help="Force polling mode")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api_key = os.getenv("HELIUS_API_KEY", "").strip()

    if args.wallet:
        wallet = args.wallet.strip()
    else:
        try:
            entered = input("Enter wallet address (Enter = default Cented): ").strip()
        except EOFError:
            entered = ""
        wallet = entered or DEFAULT_WALLET

    try:
        owner_entered = input("Enter wallet owner name (Enter = short wallet prefix): ").strip()
    except EOFError:
        owner_entered = ""
    wallet_owner = owner_entered or wallet[:6]
    wallet_owner_safe = sanitize_owner_name(wallet_owner) or wallet[:6]

    out_path = args.out if args.out else f"{wallet_owner_safe}.live.jsonl"

    rpc_url, ws_url = pick_endpoints(api_key)

    state = ListenerState(
        wallet=wallet,
        wallet_owner=wallet_owner,
        out_path=Path(out_path),
        seen_path=Path(args.seen),
        audit_path=Path(args.audit),
    )
    state.seen = load_seen(state.seen_path)
    update_audit(state)

    rpc = RpcClient(state, rpc_url)

    worker = threading.Thread(target=worker_loop, args=(state, rpc), daemon=True)
    worker.start()

    mode = "Polling" if args.poll else ("WebSocket" if args.ws or not args.poll else "Polling")
    print("--------------------------------------------------", flush=True)
    print("LIVE WALLET LISTENER STARTED", flush=True)
    print(f"Wallet: {wallet}", flush=True)
    print(f"Owner: {wallet_owner}", flush=True)
    print(f"Mode: {mode}", flush=True)
    print(f"Output: {state.out_path}", flush=True)
    print("--------------------------------------------------", flush=True)
    print(f"[info] rpc={rpc_url}", flush=True)
    print(f"[info] ws={ws_url}", flush=True)
    print(f"[info] loaded_seen={len(state.seen)}", flush=True)

    if args.poll:
        polling_loop(state, rpc)
        return

    ws_ok = False
    if ws_url:
        ws_ok = try_ws_listen_with_websocket_client(state, ws_url, wallet)
        if not ws_ok:
            ws_ok = try_ws_listen_with_websockets(state, ws_url, wallet)

    if not ws_ok:
        print("[warn] websocket unavailable after retries; switching to polling", flush=True)
        polling_loop(state, rpc)


if __name__ == "__main__":
    main()
