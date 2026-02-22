#!/usr/bin/env python3
"""
README
======
Deterministic SQLite backfill for Solana mint intelligence.

Usage:
  python backfill_mint_intel.py --db masterwalletsdb.db --helius-key HELIUS_API_KEY \
    --indexer dexscreener --indexer-key INDEXER_API_KEY --limit 0 --rate-rps 5 --resume 1 --workers 5

Notes:
- Processes mints in stable sorted order.
- Streams mint discovery from SQLite (no full in-memory load).
- Resumable via `backfill_runs` and per-run `last_mint` progress.
- Enriches:
  * `mint_security` from on-chain mint account data (Helius JSON-RPC).
  * `mint_liquidity` from indexer provider (`dexscreener`).
- Stores per-mint failures in `backfill_errors`.
- Concurrent network fetching with deterministic single-threaded DB commits.
"""

from __future__ import annotations

import argparse
import base64
import json
import sqlite3
import struct
import math
import sys
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Deque, Dict, Generator, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
B58_BASE = 58
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
TOKEN_2022_PROGRAM_ALT = "TokenzQdBNo95A8A7n8C9y7SWr2zJxdyx3Y3G5M8b6"
BATCH_SIZE = 200


@dataclass
class MintSecurity:
    mint: str
    token_program: Optional[str]
    mint_authority: Optional[str]
    freeze_authority: Optional[str]
    decimals: Optional[int]
    supply_raw: Optional[str]
    last_updated: int


@dataclass
class MintLiquidity:
    mint: str
    primary_pool: Optional[str]
    liquidity_usd: Optional[float]
    lp_locked_pct: Optional[float]
    lp_lock_flag: Optional[int]
    source: str
    last_updated: int


@dataclass
class MintResult:
    mint: str
    security: Optional[MintSecurity]
    liquidity: Optional[MintLiquidity]
    errors: List[Tuple[str, str, int]]


class GlobalRateLimiter:
    def __init__(self, rps: float):
        self.rps = float(rps) if rps > 0 else 0.0
        self.window_calls = max(1, int(math.ceil(self.rps))) if self.rps > 0 else 0
        self.window_seconds = (self.window_calls / self.rps) if self.rps > 0 else 0.0
        self.lock = threading.Lock()
        self.calls: Deque[float] = deque()

    def acquire(self) -> None:
        if self.rps <= 0:
            return
        while True:
            wait_for = 0.0
            now = time.monotonic()
            with self.lock:
                while self.calls and (now - self.calls[0]) >= self.window_seconds:
                    self.calls.popleft()
                if len(self.calls) < self.window_calls:
                    self.calls.append(now)
                    return
                wait_for = self.window_seconds - (now - self.calls[0])
            if wait_for > 0:
                time.sleep(wait_for)


class HttpClient:
    def __init__(self, rate_rps: float):
        self.rate = GlobalRateLimiter(rate_rps)
        self._local = threading.local()

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            retry = Retry(
                total=5,
                connect=5,
                read=5,
                backoff_factor=0.5,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset(["GET", "POST"]),
                raise_on_status=False,
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            self._local.session = session
        return session

    def post_json(self, url: str, payload: dict, timeout: int = 30) -> dict:
        self.rate.acquire()
        resp = self._session().post(url, json=payload, timeout=timeout)
        if resp.status_code >= 400:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"RPC error: {json.dumps(data['error'], sort_keys=True)}")
        return data

    def get_json(self, url: str, headers: Optional[dict] = None, timeout: int = 30) -> dict:
        self.rate.acquire()
        resp = self._session().get(url, headers=headers, timeout=timeout)
        if resp.status_code >= 400:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
        return resp.json()


def b58encode(raw: bytes) -> str:
    num = int.from_bytes(raw, byteorder="big", signed=False)
    encoded = ""
    while num > 0:
        num, rem = divmod(num, B58_BASE)
        encoded = B58_ALPHABET[rem] + encoded
    leading_zeros = 0
    for b in raw:
        if b == 0:
            leading_zeros += 1
        else:
            break
    return ("1" * leading_zeros) + (encoded or "")


def decode_spl_mint_account(data: bytes) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[str]]:
    if len(data) < 82:
        raise ValueError(f"Mint account data too short: {len(data)}")

    mint_auth_opt = struct.unpack_from("<I", data, 0)[0]
    mint_auth = b58encode(data[4:36]) if mint_auth_opt else None
    supply = struct.unpack_from("<Q", data, 36)[0]
    decimals = data[44]
    freeze_auth_opt = struct.unpack_from("<I", data, 46)[0]
    freeze_auth = b58encode(data[50:82]) if freeze_auth_opt else None
    return mint_auth, freeze_auth, int(decimals), str(supply)


class HeliusAuthorityProvider:
    def __init__(self, api_key: str, http_client: HttpClient):
        self.url = f"https://mainnet.helius-rpc.com/?api-key={api_key}"
        self.http = http_client

    def fetch(self, mint: str, ts: int) -> MintSecurity:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [mint, {"encoding": "base64", "commitment": "confirmed"}],
        }
        data = self.http.post_json(self.url, payload)
        value = (data.get("result") or {}).get("value")
        if value is None:
            raise RuntimeError("mint account not found")

        owner = value.get("owner")
        encoded = (value.get("data") or [None])[0]
        if not encoded:
            raise RuntimeError("missing account data")
        raw = base64.b64decode(encoded)
        mint_auth, freeze_auth, decimals, supply = decode_spl_mint_account(raw)

        token_program = owner
        if owner in (TOKEN_2022_PROGRAM, TOKEN_2022_PROGRAM_ALT):
            token_program = f"{owner}:token-2022"

        return MintSecurity(
            mint=mint,
            token_program=token_program,
            mint_authority=mint_auth,
            freeze_authority=freeze_auth,
            decimals=decimals,
            supply_raw=supply,
            last_updated=ts,
        )


class DexScreenerProvider:
    name = "dexscreener"

    def __init__(self, api_key: str, http_client: HttpClient):
        self.api_key = api_key
        self.http = http_client

    def fetch(self, mint: str, ts: int) -> MintLiquidity:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        headers = {"Accept": "application/json"}
        data = self.http.get_json(url, headers=headers)
        pairs = data.get("pairs") or []

        best_pool = None
        best_liq = None

        normalized = []
        for p in pairs:
            pair_addr = p.get("pairAddress")
            liq = (p.get("liquidity") or {}).get("usd")
            try:
                liq_val = float(liq) if liq is not None else None
            except (TypeError, ValueError):
                liq_val = None
            normalized.append((pair_addr or "", liq_val, p))

        normalized.sort(key=lambda x: ((x[1] is None), -(x[1] or 0.0), x[0]))
        if normalized:
            best_pool = normalized[0][2].get("pairAddress")
            best_liq = normalized[0][1]

        if best_liq is not None:
            best_liq = round(best_liq, 6)

        return MintLiquidity(
            mint=mint,
            primary_pool=best_pool,
            liquidity_usd=best_liq,
            lp_locked_pct=None,
            lp_lock_flag=None,
            source=self.name,
            last_updated=ts,
        )


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
    ).fetchone()
    return row is not None


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mint_security(
          mint TEXT PRIMARY KEY,
          token_program TEXT,
          mint_authority TEXT,
          freeze_authority TEXT,
          decimals INTEGER,
          supply_raw TEXT,
          last_updated INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mint_liquidity(
          mint TEXT PRIMARY KEY,
          primary_pool TEXT,
          liquidity_usd REAL,
          lp_locked_pct REAL,
          lp_lock_flag INTEGER,
          source TEXT,
          last_updated INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS backfill_runs(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          started_at INTEGER NOT NULL,
          finished_at INTEGER,
          mode TEXT NOT NULL,
          total_mints INTEGER,
          processed_mints INTEGER,
          last_mint TEXT,
          errors INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS backfill_errors(
          mint TEXT NOT NULL,
          category TEXT NOT NULL,
          error TEXT NOT NULL,
          ts INTEGER NOT NULL
        )
        """
    )
    conn.commit()


def resolve_source(conn: sqlite3.Connection) -> Tuple[str, str]:
    if table_exists(conn, "spl_transfers"):
        return "spl_transfers", "mint"
    if table_exists(conn, "swaps"):
        return "swaps", "token_mint"
    raise RuntimeError("Neither spl_transfers nor swaps table is available")


def count_mints(conn: sqlite3.Connection, table: str, col: str, last_mint: Optional[str], single_mint: Optional[str]) -> int:
    if single_mint:
        row = conn.execute(
            f"SELECT COUNT(1) FROM (SELECT DISTINCT {col} AS mint FROM {table} WHERE {col}=? LIMIT 1)",
            (single_mint,),
        ).fetchone()
        return int(row[0] if row else 0)

    where = [f"{col} IS NOT NULL", f"TRIM({col})<>''"]
    params = []
    if last_mint:
        where.append(f"{col} > ?")
        params.append(last_mint)
    sql = f"SELECT COUNT(1) FROM (SELECT DISTINCT {col} AS mint FROM {table} WHERE {' AND '.join(where)})"
    row = conn.execute(sql, params).fetchone()
    return int(row[0] if row else 0)


def iter_mints(
    conn: sqlite3.Connection,
    table: str,
    col: str,
    last_mint: Optional[str],
    single_mint: Optional[str],
    limit: int,
    chunk_size: int = 500,
) -> Generator[str, None, None]:
    if single_mint:
        sql = (
            f"SELECT DISTINCT {col} AS mint FROM {table} "
            f"WHERE {col}=? ORDER BY mint"
        )
        cur = conn.execute(sql, (single_mint,))
    else:
        where = [f"{col} IS NOT NULL", f"TRIM({col})<>''"]
        params = []
        if last_mint:
            where.append(f"{col} > ?")
            params.append(last_mint)
        sql = (
            f"SELECT DISTINCT {col} AS mint FROM {table} "
            f"WHERE {' AND '.join(where)} ORDER BY mint"
        )
        cur = conn.execute(sql, params)

    yielded = 0
    while True:
        rows = cur.fetchmany(chunk_size)
        if not rows:
            break
        for (mint,) in rows:
            if limit and yielded >= limit:
                return
            yielded += 1
            yield mint


def upsert_security(conn: sqlite3.Connection, s: MintSecurity) -> None:
    conn.execute(
        """
        INSERT INTO mint_security(mint, token_program, mint_authority, freeze_authority, decimals, supply_raw, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(mint) DO UPDATE SET
          token_program=excluded.token_program,
          mint_authority=excluded.mint_authority,
          freeze_authority=excluded.freeze_authority,
          decimals=excluded.decimals,
          supply_raw=excluded.supply_raw,
          last_updated=excluded.last_updated
        """,
        (
            s.mint,
            s.token_program,
            s.mint_authority,
            s.freeze_authority,
            s.decimals,
            s.supply_raw,
            s.last_updated,
        ),
    )


def upsert_liquidity(conn: sqlite3.Connection, l: MintLiquidity) -> None:
    conn.execute(
        """
        INSERT INTO mint_liquidity(mint, primary_pool, liquidity_usd, lp_locked_pct, lp_lock_flag, source, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(mint) DO UPDATE SET
          primary_pool=excluded.primary_pool,
          liquidity_usd=excluded.liquidity_usd,
          lp_locked_pct=excluded.lp_locked_pct,
          lp_lock_flag=excluded.lp_lock_flag,
          source=excluded.source,
          last_updated=excluded.last_updated
        """,
        (
            l.mint,
            l.primary_pool,
            l.liquidity_usd,
            l.lp_locked_pct,
            l.lp_lock_flag,
            l.source,
            l.last_updated,
        ),
    )


def log_error(conn: sqlite3.Connection, mint: str, category: str, error: str, ts: int) -> None:
    conn.execute(
        "INSERT INTO backfill_errors(mint, category, error, ts) VALUES (?, ?, ?, ?)",
        (mint, category, error[:1000], ts),
    )


def create_run(conn: sqlite3.Connection, mode: str, total: int) -> int:
    ts = int(time.time())
    cur = conn.execute(
        "INSERT INTO backfill_runs(started_at, mode, total_mints, processed_mints, errors) VALUES (?, ?, ?, 0, 0)",
        (ts, mode, total),
    )
    conn.commit()
    return int(cur.lastrowid)


def get_resume_run(conn: sqlite3.Connection, mode: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT id, started_at, finished_at, mode, total_mints, processed_mints, last_mint, errors
        FROM backfill_runs
        WHERE finished_at IS NULL AND mode = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (mode,),
    ).fetchone()


def update_run(conn: sqlite3.Connection, run_id: int, processed: int, last_mint: Optional[str], errors: int) -> None:
    conn.execute(
        "UPDATE backfill_runs SET processed_mints=?, last_mint=?, errors=? WHERE id=?",
        (processed, last_mint, errors, run_id),
    )


def finish_run(conn: sqlite3.Connection, run_id: int, processed: int, last_mint: Optional[str], errors: int) -> None:
    conn.execute(
        "UPDATE backfill_runs SET finished_at=?, processed_mints=?, last_mint=?, errors=? WHERE id=?",
        (int(time.time()), processed, last_mint, errors, run_id),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill mint authority and liquidity intelligence")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--helius-key", required=True, help="Helius API key")
    parser.add_argument("--indexer", required=True, choices=["dexscreener"], help="Indexer provider")
    parser.add_argument("--indexer-key", required=True, help="Indexer API key (reserved by provider)")
    parser.add_argument("--limit", type=int, default=0, help="Max mints to process (0 = all)")
    parser.add_argument("--rate-rps", type=float, default=5.0, help="Rate limit requests per second")
    parser.add_argument("--resume", type=int, default=1, help="Resume unfinished run if 1")
    parser.add_argument("--mint", default=None, help="Optional single mint override")
    parser.add_argument("--workers", type=int, default=5, help="Concurrent worker count")
    return parser.parse_args()


def process_mint(mint: str, authority: HeliusAuthorityProvider, indexer: DexScreenerProvider) -> MintResult:
    ts = int(time.time())
    security = None
    liquidity = None
    errors: List[Tuple[str, str, int]] = []

    try:
        security = authority.fetch(mint, ts)
    except Exception as exc:
        errors.append(("authority", str(exc), ts))

    try:
        liquidity = indexer.fetch(mint, ts)
    except Exception as exc:
        errors.append(("liquidity", str(exc), ts))

    return MintResult(mint=mint, security=security, liquidity=liquidity, errors=errors)


def flush_batch(
    conn: sqlite3.Connection,
    db_lock: threading.Lock,
    run_id: int,
    batch: List[MintResult],
    processed: int,
    errors: int,
    workers: int,
    total: int,
) -> Tuple[int, int, Optional[str]]:
    if not batch:
        return processed, errors, None

    ordered = sorted(batch, key=lambda r: r.mint)
    last_mint = ordered[-1].mint

    with db_lock:
        for result in ordered:
            if result.security is not None:
                upsert_security(conn, result.security)
            if result.liquidity is not None:
                upsert_liquidity(conn, result.liquidity)
            for category, err, ts in result.errors:
                errors += 1
                log_error(conn, result.mint, category, err, ts)

        processed += len(ordered)
        update_run(conn, run_id, processed, last_mint, errors)
        conn.commit()

    if len(ordered) >= BATCH_SIZE:
        print(f"processed={processed}/{total} last_mint={last_mint} errors={errors} workers={workers}")

    return processed, errors, last_mint


def main() -> int:
    args = parse_args()
    mode = f"indexer={args.indexer}|mint={args.mint or '*'}|limit={args.limit}"

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)

    table, col = resolve_source(conn)

    resume_row = get_resume_run(conn, mode) if args.resume == 1 else None
    resume_last_mint = resume_row["last_mint"] if resume_row else None
    run_id = int(resume_row["id"]) if resume_row else None
    processed = int(resume_row["processed_mints"] or 0) if resume_row else 0
    errors = int(resume_row["errors"] or 0) if resume_row else 0

    remaining = count_mints(conn, table, col, resume_last_mint, args.mint)
    if args.limit > 0:
        remaining = min(remaining, args.limit)
    total = processed + remaining

    if run_id is None:
        run_id = create_run(conn, mode, total)

    http_client = HttpClient(rate_rps=args.rate_rps)
    authority = HeliusAuthorityProvider(args.helius_key, http_client)
    indexer = DexScreenerProvider(args.indexer_key, http_client)

    db_lock = threading.Lock()

    mint_iter = iter_mints(conn, table, col, resume_last_mint, args.mint, args.limit)
    pending = {}
    completed_by_idx: Dict[int, MintResult] = {}
    ordered_buffer: List[MintResult] = []
    next_submit_idx = 0
    next_commit_idx = 0
    current_last_mint = resume_last_mint

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        for _ in range(max(1, args.workers)):
            try:
                mint = next(mint_iter)
            except StopIteration:
                break
            future = executor.submit(process_mint, mint, authority, indexer)
            pending[future] = (next_submit_idx, mint)
            next_submit_idx += 1

        while pending:
            done, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
            for fut in done:
                idx, mint = pending.pop(fut)
                try:
                    result = fut.result()
                except Exception as exc:
                    result = MintResult(mint=mint, security=None, liquidity=None, errors=[("worker", str(exc), int(time.time()))])
                completed_by_idx[idx] = result

                try:
                    mint = next(mint_iter)
                    nf = executor.submit(process_mint, mint, authority, indexer)
                    pending[nf] = (next_submit_idx, mint)
                    next_submit_idx += 1
                except StopIteration:
                    pass

            while next_commit_idx in completed_by_idx:
                ordered_buffer.append(completed_by_idx.pop(next_commit_idx))
                next_commit_idx += 1

                if len(ordered_buffer) >= BATCH_SIZE:
                    chunk = ordered_buffer[:BATCH_SIZE]
                    ordered_buffer = ordered_buffer[BATCH_SIZE:]
                    processed, errors, current_last_mint = flush_batch(
                        conn=conn,
                        db_lock=db_lock,
                        run_id=run_id,
                        batch=chunk,
                        processed=processed,
                        errors=errors,
                        workers=max(1, args.workers),
                        total=total,
                    )

    if ordered_buffer:
        processed, errors, current_last_mint = flush_batch(
            conn=conn,
            db_lock=db_lock,
            run_id=run_id,
            batch=ordered_buffer,
            processed=processed,
            errors=errors,
            workers=max(1, args.workers),
            total=total,
        )

    finish_run(conn, run_id, processed, current_last_mint, errors)
    conn.commit()
    print(f"done processed={processed}/{total} last_mint={current_last_mint} errors={errors} workers={max(1, args.workers)}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
