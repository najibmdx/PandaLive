#!/usr/bin/env python3
# CHANGELOG
# - Split creator-linked relationship evidence from creator-linked prefunding evidence.
# - Added pre-buy timing validation with a configurable prefunding window.
# - Tightened creator/platform evidence and created-token discovery requirements.
# - Made liquidity/trade attribution more conservative and NULL-friendly.
"""
Forensic Solana creator auditor.

Example Windows CMD usage:
    set HELIUS_API_KEY=your_helius_key_here
    python creator_audit.py --wallet CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o --seed-mint 3mecmcGqs4q9RMzFKZFZTBEcbtA7SPFKPKb9kDvxpump --outdir out_creator_audit
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import statistics
import sys
import time
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

import requests

getcontext().prec = 50

TOOL_VERSION = "1.1.0"
DEFAULT_RPC_URL = "https://api.mainnet-beta.solana.com"
LAMPORTS_PER_SOL = Decimal("1000000000")
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
METADATA_PROGRAM_ID = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"
ASSOCIATED_TOKEN_PROGRAM_ID = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"
NULL_VALUE = "NULL"
DEX_SOURCES = {"RAYDIUM", "ORCA", "PUMP_FUN", "JUPITER", "METEORA", "PHOENIX", "LIFINITY"}
SWAP_TYPES = {"SWAP", "BUY", "SELL"}
LIQUIDITY_TYPES = {"ADD_LIQUIDITY", "CREATE_POOL", "INITIALIZE_POOL", "ADD_POOL_LIQUIDITY"}
CREATE_METADATA_TYPES = {"createMetadataAccount", "createMetadataAccountV2", "createMetadataAccountV3"}
MINT_INIT_TYPES = {"initializeMint", "initializeMint2", "initializeMintCloseAuthority"}


@dataclass
class AuditConfig:
    wallet: str
    seed_mint: Optional[str]
    outdir: Path
    early_buyers_limit: int = 50
    funding_depth: int = 2
    launch_window_minutes: int = 60
    prefund_max_seconds: int = 3600
    rpc_url: str = DEFAULT_RPC_URL
    refresh: bool = False
    verbose: bool = False


@dataclass
class FailureCounter:
    helius_failures: int = 0
    rpc_failures: int = 0
    metadata_failures: int = 0
    partial_failures: int = 0


@dataclass
class CreatorEvidence:
    creator_wallet_determinable: str = ""
    creator_signed_creation_tx: str = "FALSE"
    creator_fee_payer_on_creation_tx: str = "FALSE"
    creator_is_update_authority: str = "FALSE"
    creator_is_mint_authority: str = "FALSE"
    creator_is_freeze_authority: str = "FALSE"
    creator_matches_platform_dev_evidence_if_any: str = "FALSE"
    creator_status: str = "INCONCLUSIVE"
    creator_reason: str = ""
    update_authority: str = ""
    mint_authority: str = ""
    freeze_authority: str = ""
    metadata_authority: str = ""
    platform_dev_wallet: str = ""
    evidence_signatures: str = ""
    evidence_class_count: str = "0"
    evidence_mint_init: str = "FALSE"
    evidence_metadata_create: str = "FALSE"
    evidence_platform_create: str = "FALSE"


@dataclass
class CreatedTokenRecord:
    mint: str
    creation_signature: str = ""
    creation_time_iso: str = ""
    creation_time_unix: str = ""
    slot: str = ""
    creator_wallet: str = ""
    symbol: str = ""
    name: str = ""
    platform: str = ""
    creator_status: str = "INCONCLUSIVE"
    creator_reason: str = ""
    creator_signed_creation_tx: str = "FALSE"
    creator_fee_payer_on_creation_tx: str = "FALSE"
    creator_is_update_authority: str = "FALSE"
    creator_is_mint_authority: str = "FALSE"
    creator_is_freeze_authority: str = "FALSE"
    creator_matches_platform_dev_evidence_if_any: str = "FALSE"
    update_authority: str = ""
    mint_authority: str = ""
    freeze_authority: str = ""
    metadata_authority: str = ""
    platform_dev_wallet: str = ""
    evidence_signature_list: str = ""
    evidence_class_count: str = "0"
    evidence_mint_init: str = "FALSE"
    evidence_metadata_create: str = "FALSE"
    evidence_platform_create: str = "FALSE"
    evidence_creator_signer: str = "FALSE"
    evidence_creator_fee_payer: str = "FALSE"


@dataclass
class FundingEdge:
    token_mint: str
    src_wallet: str
    dst_wallet: str
    depth_from_creator: int
    amount_lamports: str
    amount_sol: str
    signature: str
    slot: str
    block_time_iso: str
    block_time_unix: str
    block_time: str
    relation_type: str
    path_root_wallet: str
    launch_window_included: str


@dataclass
class CreatorFlowRow:
    event_time_iso: str
    event_time_unix: str
    slot: str
    signature: str
    flow_type: str
    token_mint: str
    counterparty: str
    raw_amount: str
    ui_amount: str
    notes: str
    evidence_class: str


@dataclass
class EarlyBuyerRow:
    wallet: str
    first_buy_sig: str
    first_buy_time_iso: str
    first_buy_time_unix: str
    first_buy_slot: str
    raw_amount: str
    ui_amount: str
    event_type: str
    funded_by_creator_directly: str = "FALSE"
    funded_by_creator_indirectly: str = "FALSE"
    indirect_path: str = ""
    received_token_from_creator: str = "FALSE"
    received_token_from_creator_linked_wallet: str = "FALSE"
    received_token_signature: str = ""
    funding_evidence_signatures: str = ""
    linked_from_creator: str = "FALSE"
    linked_depth: str = ""
    latest_creator_linked_funding_sig_before_buy: str = ""
    latest_creator_linked_funding_time_before_buy: str = ""
    funding_before_buy: str = "FALSE"
    funding_to_buy_delta_seconds: str = ""
    directly_prefunded: str = "FALSE"
    indirectly_prefunded: str = "FALSE"


@dataclass
class EarlyReceiptRow:
    wallet: str
    first_receive_sig: str
    first_receive_time_iso: str
    first_receive_time_unix: str
    first_receive_slot: str
    raw_amount: str
    ui_amount: str
    receipt_type: str
    source_wallet: str = ""


@dataclass
class FundingGraphResult:
    edges: List[FundingEdge]
    creator_linked_wallet_depth: Dict[str, int]
    path_by_wallet: Dict[str, List[str]]
    prefund_max_seconds: int


@dataclass
class TokenSummary:
    mint: str
    symbol: str
    name: str
    creator_wallet: str
    creator_confirmed: str
    creation_signature: str
    creation_time_iso: str
    creation_time_unix: str
    platform: str
    first_liquidity_signature: str = ""
    first_liquidity_time_iso: str = ""
    first_liquidity_time_unix: str = ""
    first_liquidity_evidence_class: str = ""
    creation_to_first_liquidity_seconds: str = ""
    first_meaningful_trade_signature: str = ""
    first_meaningful_trade_time_iso: str = ""
    first_meaningful_trade_time_unix: str = ""
    first_trade_evidence_class: str = ""
    creator_bought_own_token: str = "FALSE"
    creator_sold_own_token: str = "FALSE"
    creator_first_sell_time_iso: str = ""
    creator_first_sell_time_unix: str = ""
    creator_cumulative_sold_raw: str = ""
    creator_directly_funded_early_buyer: str = "FALSE"
    creator_indirectly_funded_early_buyer: str = "FALSE"
    creator_directly_linked_to_early_buyer: str = "FALSE"
    creator_indirectly_linked_to_early_buyer: str = "FALSE"
    creator_directly_prefunded_early_buyer: str = "FALSE"
    creator_indirectly_prefunded_early_buyer: str = "FALSE"
    early_buyer_received_tokens_from_creator: str = "FALSE"
    multiple_creator_linked_early_buyers: str = "FALSE"
    creator_exited_early: str = "FALSE"
    creator_current_visible_balance: str = ""
    creator_visible_holder_rank_if_known: str = ""
    creator_current_visible_balance_known: str = "FALSE"
    insufficient_data: str = "FALSE"
    creator_trade_evidence_signatures: str = ""
    funding_evidence_signatures: str = ""
    notes: str = ""


class ApiClient:
    def __init__(self, config: AuditConfig, failures: FailureCounter) -> None:
        self.config = config
        self.failures = failures
        self.helius_api_key = os.getenv("HELIUS_API_KEY", "").strip()
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def _request_with_retry(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        json_body: Optional[Mapping[str, Any]] = None,
        timeout: int = 30,
        request_kind: str,
    ) -> Any:
        delay = 1.0
        for attempt in range(5):
            try:
                response = self.session.request(method=method, url=url, params=params, json=json_body, timeout=timeout)
                if response.status_code == 429:
                    raise requests.HTTPError("rate limited", response=response)
                response.raise_for_status()
                return response.json() if response.text else None
            except (requests.RequestException, ValueError) as exc:
                if request_kind == "helius":
                    self.failures.helius_failures += 1
                elif request_kind == "rpc":
                    self.failures.rpc_failures += 1
                else:
                    self.failures.metadata_failures += 1
                logging.warning("Request failure (%s) attempt=%s url=%s error=%s", request_kind, attempt + 1, url, exc)
                if attempt == 4:
                    raise
                time.sleep(delay)
                delay *= 2
        raise RuntimeError("unreachable")

    def helius_get_transactions(self, address: str, before: Optional[str] = None, limit: int = 100) -> List[dict[str, Any]]:
        if not self.helius_api_key:
            return []
        url = f"https://api.helius.xyz/v0/addresses/{address}/transactions"
        params: Dict[str, Any] = {"api-key": self.helius_api_key, "limit": limit}
        if before:
            params["before"] = before
        try:
            payload = self._request_with_retry("GET", url, params=params, request_kind="helius")
        except Exception:
            return []
        return payload if isinstance(payload, list) else []

    def helius_get_all_transactions(self, address: str, max_pages: int = 25, page_limit: int = 100) -> List[dict[str, Any]]:
        collected: List[dict[str, Any]] = []
        before: Optional[str] = None
        for _ in range(max_pages):
            batch = self.helius_get_transactions(address, before=before, limit=page_limit)
            if not batch:
                break
            collected.extend(batch)
            before = batch[-1].get("signature")
            if len(batch) < page_limit:
                break
        deduped: Dict[str, dict[str, Any]] = {}
        for tx in collected:
            signature = tx.get("signature")
            if signature:
                deduped[signature] = tx
        return sorted(deduped.values(), key=lambda item: (safe_int(item.get("timestamp")), safe_int(item.get("slot"))))

    def helius_get_token_metadata(self, mints: Sequence[str]) -> Dict[str, dict[str, Any]]:
        if not self.helius_api_key or not mints:
            return {}
        url = f"https://api.helius.xyz/v0/token-metadata?api-key={self.helius_api_key}"
        results: Dict[str, dict[str, Any]] = {}
        for idx in range(0, len(mints), 100):
            chunk = list(mints[idx : idx + 100])
            try:
                payload = self._request_with_retry("POST", url, json_body={"mintAccounts": chunk}, request_kind="metadata")
            except Exception:
                self.failures.partial_failures += 1
                continue
            if isinstance(payload, list):
                for item in payload:
                    mint = item.get("mint") or item.get("account")
                    if mint:
                        results[mint] = item
        return results

    def rpc_call(self, method: str, params: Sequence[Any]) -> Any:
        body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": list(params)}
        try:
            payload = self._request_with_retry("POST", self.config.rpc_url, json_body=body, request_kind="rpc")
        except Exception:
            return None
        if isinstance(payload, dict) and payload.get("error"):
            self.failures.rpc_failures += 1
            logging.warning("RPC error method=%s error=%s", method, payload["error"])
            return None
        return payload.get("result") if isinstance(payload, dict) else None

    def get_signatures_for_address(self, address: str, before: Optional[str] = None, limit: int = 1000) -> List[dict[str, Any]]:
        options: Dict[str, Any] = {"limit": limit, "commitment": "confirmed"}
        if before:
            options["before"] = before
        result = self.rpc_call("getSignaturesForAddress", [address, options])
        return result if isinstance(result, list) else []

    def get_all_signatures_for_address(self, address: str, max_pages: int = 20, page_limit: int = 1000) -> List[dict[str, Any]]:
        rows: List[dict[str, Any]] = []
        before: Optional[str] = None
        for _ in range(max_pages):
            batch = self.get_signatures_for_address(address, before=before, limit=page_limit)
            if not batch:
                break
            rows.extend(batch)
            before = batch[-1].get("signature")
            if len(batch) < page_limit:
                break
        deduped = {row["signature"]: row for row in rows if row.get("signature")}
        return sorted(deduped.values(), key=lambda item: (safe_int(item.get("blockTime")), safe_int(item.get("slot"))))

    def get_transaction(self, signature: str) -> Optional[dict[str, Any]]:
        result = self.rpc_call(
            "getTransaction",
            [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}],
        )
        return result if isinstance(result, dict) else None

    def get_account_info(self, address: str, encoding: str = "jsonParsed") -> Optional[dict[str, Any]]:
        result = self.rpc_call("getAccountInfo", [address, {"encoding": encoding, "commitment": "confirmed"}])
        return result.get("value") if isinstance(result, dict) else None

    def get_multiple_accounts(self, addresses: Sequence[str], encoding: str = "jsonParsed") -> List[Optional[dict[str, Any]]]:
        if not addresses:
            return []
        result = self.rpc_call("getMultipleAccounts", [list(addresses), {"encoding": encoding, "commitment": "confirmed"}])
        if isinstance(result, dict) and isinstance(result.get("value"), list):
            return result["value"]
        return []

    def get_token_largest_accounts(self, mint: str) -> List[dict[str, Any]]:
        result = self.rpc_call("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
        return result.get("value", []) if isinstance(result, dict) else []


class ForensicAuditor:
    def __init__(self, config: AuditConfig) -> None:
        self.config = config
        self.failures = FailureCounter()
        self.api = ApiClient(config, self.failures)
        self.raw_evidence_dir = self.config.outdir / "raw_evidence"
        self.wallet_transactions: Optional[List[dict[str, Any]]] = None
        self.wallet_tx_map: Dict[str, dict[str, Any]] = {}
        self.metadata_cache: Dict[str, dict[str, Any]] = {}
        self.partial_tokens = 0
        self.token_counts: Dict[str, Dict[str, int]] = {}

    def run(self) -> None:
        self.prepare_dirs()
        created_tokens = self.discover_created_tokens(self.config.wallet)
        verification = self.verify_seed_mint(self.config.seed_mint, self.config.wallet, created_tokens) if self.config.seed_mint else None
        token_summaries: List[TokenSummary] = []
        aggregate_wallets: Counter[str] = Counter()
        aggregate_intermediaries: Counter[str] = Counter()
        generated_files: List[str] = []
        creator_linked_early_buyers = 0
        total_early_buyers = 0
        total_early_receipts = 0
        total_funding_edges = 0
        total_prefunded_early_buyers = 0
        total_direct_prefunded = 0
        total_indirect_prefunded = 0
        total_without_prior_funding = 0
        inconclusive_first_liquidity_tokens = 0
        inconclusive_first_trade_tokens = 0
        verification_evidence_counts = Counter[str]()

        if verification:
            path = self.config.outdir / "creator_verification.tsv"
            self.write_tsv(path, [verification])
            generated_files.append(str(path.relative_to(self.config.outdir)))
            for key in (
                "creator_signed_creation_tx",
                "creator_fee_payer_on_creation_tx",
                "creator_is_update_authority",
                "creator_is_mint_authority",
                "creator_is_freeze_authority",
                "creator_matches_platform_dev_evidence_if_any",
            ):
                if verification.get(key) == "TRUE":
                    verification_evidence_counts[key] += 1

        created_path = self.config.outdir / "created_tokens.tsv"
        self.write_tsv(created_path, [asdict(record) for record in created_tokens])
        generated_files.append(str(created_path.relative_to(self.config.outdir)))

        dossier_dir = self.config.outdir / "token_dossiers"
        dossier_dir.mkdir(exist_ok=True)

        for token in created_tokens:
            try:
                dossier = self.build_token_dossier(token)
            except Exception as exc:  # continue on partial failures
                self.partial_tokens += 1
                self.failures.partial_failures += 1
                logging.exception("Token dossier failed for mint=%s error=%s", token.mint, exc)
                continue
            token_summaries.append(dossier["summary"])
            total_early_buyers += len(dossier["early_buyers"])
            total_early_receipts += len(dossier["early_receipts"])
            total_funding_edges += len(dossier["funding_edges"])
            total_prefunded_early_buyers += sum(1 for row in dossier["early_buyers"] if row.directly_prefunded == "TRUE" or row.indirectly_prefunded == "TRUE")
            total_direct_prefunded += sum(1 for row in dossier["early_buyers"] if row.directly_prefunded == "TRUE")
            total_indirect_prefunded += sum(1 for row in dossier["early_buyers"] if row.indirectly_prefunded == "TRUE")
            total_without_prior_funding += sum(1 for row in dossier["early_buyers"] if row.linked_from_creator == "TRUE" and row.funding_before_buy != "TRUE")
            creator_linked_early_buyers += sum(
                1
                for row in dossier["early_buyers"]
                if row.linked_from_creator == "TRUE"
                or row.received_token_from_creator == "TRUE"
                or row.received_token_from_creator_linked_wallet == "TRUE"
            )
            if dossier["summary"].first_liquidity_signature == NULL_VALUE:
                inconclusive_first_liquidity_tokens += 1
            if dossier["summary"].first_meaningful_trade_signature == NULL_VALUE:
                inconclusive_first_trade_tokens += 1
            for row in dossier["early_buyers"]:
                aggregate_wallets[row.wallet] += 1
                for wallet in [part for part in row.indirect_path.split(">") if part and part not in {self.config.wallet, row.wallet}]:
                    aggregate_intermediaries[wallet] += 1
            files = self.write_dossier_files(dossier_dir, token.mint, dossier)
            generated_files.extend(files)

        aggregate_row = self.build_aggregate_summary(token_summaries, aggregate_wallets, aggregate_intermediaries)
        aggregate_path = self.config.outdir / "aggregate_summary.tsv"
        self.write_tsv(aggregate_path, [aggregate_row])
        generated_files.append(str(aggregate_path.relative_to(self.config.outdir)))

        summary_path = self.config.outdir / "summary.txt"
        summary_path.write_text(
            self.render_summary_txt(
                verification,
                created_tokens,
                token_summaries,
                aggregate_row,
                total_early_buyers,
                total_early_receipts,
                total_funding_edges,
            ),
            encoding="utf-8",
        )
        generated_files.append(str(summary_path.relative_to(self.config.outdir)))

        manifest = {
            "target_wallet": self.config.wallet,
            "seed_mint": self.config.seed_mint or "",
            "run_timestamp_utc": utc_iso(int(time.time())),
            "tool_version": TOOL_VERSION,
            "config": {
                "wallet": self.config.wallet,
                "seed_mint": self.config.seed_mint,
                "outdir": str(self.config.outdir),
                "early_buyers_limit": self.config.early_buyers_limit,
                "funding_depth": self.config.funding_depth,
                "launch_window_minutes": self.config.launch_window_minutes,
                "prefund_max_seconds": self.config.prefund_max_seconds,
                "rpc_url": self.config.rpc_url,
                "refresh": self.config.refresh,
                "verbose": self.config.verbose,
            },
            "files_generated": sorted(set(generated_files)),
            "counts": {
                "tokens_discovered": len(created_tokens),
                "tokens_processed": len(token_summaries),
                "verification_rows": 1 if verification else 0,
                "early_buyers_found": total_early_buyers,
                "early_receipts_found": total_early_receipts,
                "funding_edges_found": total_funding_edges,
                "creator_linked_early_buyers": creator_linked_early_buyers,
                "creator_prefunded_early_buyers": total_prefunded_early_buyers,
                "direct_prefunded_early_buyers": total_direct_prefunded,
                "indirect_prefunded_early_buyers": total_indirect_prefunded,
                "early_buyers_without_prior_creator_linked_funding": total_without_prior_funding,
                "inconclusive_first_liquidity_tokens": inconclusive_first_liquidity_tokens,
                "inconclusive_first_trade_tokens": inconclusive_first_trade_tokens,
            },
            "creator_verification_evidence_counts": dict(verification_evidence_counts),
            "api_failures": asdict(self.failures),
            "partial_tokens": self.partial_tokens,
        }
        manifest_path = self.config.outdir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    def prepare_dirs(self) -> None:
        self.config.outdir.mkdir(parents=True, exist_ok=True)
        self.raw_evidence_dir.mkdir(parents=True, exist_ok=True)

    def load_wallet_transactions(self) -> List[dict[str, Any]]:
        if self.wallet_transactions is None:
            self.wallet_transactions = self.api.helius_get_all_transactions(self.config.wallet, max_pages=50)
            self.wallet_tx_map = {tx.get("signature", ""): tx for tx in self.wallet_transactions if tx.get("signature")}
            (self.raw_evidence_dir / f"wallet_{self.config.wallet}_helius_transactions.json").write_text(
                json.dumps(self.wallet_transactions, indent=2),
                encoding="utf-8",
            )
        return self.wallet_transactions

    def discover_created_tokens(self, wallet: str) -> List[CreatedTokenRecord]:
        helius_txs = self.load_wallet_transactions()
        token_candidates: Dict[str, CreatedTokenRecord] = {}
        for tx in helius_txs:
            if not self.wallet_is_signer_or_fee_payer(tx, wallet):
                continue
            candidate_mints = self.extract_candidate_mints_from_explicit_instruction_accounts(tx)
            for mint in candidate_mints:
                if mint in token_candidates:
                    continue
                creation_record = self.confirm_token_creation(mint, wallet, tx)
                if creation_record:
                    token_candidates[mint] = creation_record
        metadata = self.api.helius_get_token_metadata(list(token_candidates))
        self.metadata_cache.update(metadata)
        for mint, record in token_candidates.items():
            self.apply_metadata(record, metadata.get(mint, {}))
        return sorted(token_candidates.values(), key=lambda item: (safe_int(item.creation_time_unix), item.mint))

    def verify_seed_mint(
        self,
        seed_mint: Optional[str],
        wallet: str,
        created_tokens: Sequence[CreatedTokenRecord],
    ) -> Optional[Dict[str, Any]]:
        if not seed_mint:
            return None
        matching = next((row for row in created_tokens if row.mint == seed_mint), None)
        if matching:
            return {
                "mint": matching.mint,
                "suspected_wallet": wallet,
                "status": matching.creator_status,
                "creator_wallet_determinable": matching.creator_wallet or NULL_VALUE,
                "creation_signature": matching.creation_signature or NULL_VALUE,
                "creation_time_iso": matching.creation_time_iso or NULL_VALUE,
                "creation_time_unix": matching.creation_time_unix or NULL_VALUE,
                "slot": matching.slot or NULL_VALUE,
                "platform": matching.platform or NULL_VALUE,
                "creator_signed_creation_tx": matching.creator_signed_creation_tx,
                "creator_fee_payer_on_creation_tx": matching.creator_fee_payer_on_creation_tx,
                "creator_is_update_authority": matching.creator_is_update_authority,
                "creator_is_mint_authority": matching.creator_is_mint_authority,
                "creator_is_freeze_authority": matching.creator_is_freeze_authority,
                "creator_matches_platform_dev_evidence_if_any": matching.creator_matches_platform_dev_evidence_if_any,
                "metadata_authority": matching.metadata_authority or NULL_VALUE,
                "update_authority": matching.update_authority or NULL_VALUE,
                "mint_authority": matching.mint_authority or NULL_VALUE,
                "freeze_authority": matching.freeze_authority or NULL_VALUE,
                "platform_dev_wallet": matching.platform_dev_wallet or NULL_VALUE,
                "reason": matching.creator_reason or NULL_VALUE,
                "evidence_signatures": matching.evidence_signature_list or NULL_VALUE,
                "evidence_class_count": matching.evidence_class_count,
            }
        account_info = self.api.get_account_info(seed_mint)
        parsed = (((account_info or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
        mint_authority = blank_if_none(parsed.get("mintAuthority"))
        freeze_authority = blank_if_none(parsed.get("freezeAuthority"))
        status = "INCONCLUSIVE"
        reason = "Seed mint was not confirmed in the wallet-linked creation set, and creator-class transaction evidence was not found."
        if mint_authority and mint_authority != wallet:
            status = "NOT_CONFIRMED"
            reason = f"Current mint authority is {mint_authority}, not the suspected wallet, and creation-tx evidence was not found."
        return {
            "mint": seed_mint,
            "suspected_wallet": wallet,
            "status": status,
            "creator_wallet_determinable": mint_authority or NULL_VALUE,
            "creation_signature": NULL_VALUE,
            "creation_time_iso": NULL_VALUE,
            "creation_time_unix": NULL_VALUE,
            "slot": NULL_VALUE,
            "platform": NULL_VALUE,
            "creator_signed_creation_tx": "FALSE",
            "creator_fee_payer_on_creation_tx": "FALSE",
            "creator_is_update_authority": "FALSE",
            "creator_is_mint_authority": truthy_flag(mint_authority == wallet),
            "creator_is_freeze_authority": truthy_flag(freeze_authority == wallet),
            "creator_matches_platform_dev_evidence_if_any": "FALSE",
            "metadata_authority": NULL_VALUE,
            "update_authority": NULL_VALUE,
            "mint_authority": mint_authority or NULL_VALUE,
            "freeze_authority": freeze_authority or NULL_VALUE,
            "platform_dev_wallet": NULL_VALUE,
            "reason": reason,
            "evidence_signatures": NULL_VALUE,
            "evidence_class_count": "1" if mint_authority == wallet or freeze_authority == wallet else "0",
        }

    def confirm_token_creation(self, mint: str, wallet: str, seed_tx: Optional[dict[str, Any]]) -> Optional[CreatedTokenRecord]:
        signatures = self.api.get_all_signatures_for_address(mint, max_pages=5)
        if not signatures:
            return None
        first_sig = signatures[0].get("signature", "")
        tx_detail = self.api.get_transaction(first_sig)
        if not tx_detail:
            return None
        evidence = self.extract_creation_evidence(mint, wallet, tx_detail, seed_tx)
        if evidence is None:
            return None
        first_slot = signatures[0].get("slot") or tx_detail.get("slot")
        first_block_time = signatures[0].get("blockTime") or tx_detail.get("blockTime")
        return CreatedTokenRecord(
            mint=mint,
            creation_signature=first_sig,
            creation_time_iso=utc_iso(first_block_time),
            creation_time_unix=str(first_block_time or ""),
            slot=str(first_slot or ""),
            creator_wallet=evidence.creator_wallet_determinable or wallet,
            platform=detect_platform(mint, seed_tx or {}, tx_detail),
            creator_status=evidence.creator_status,
            creator_reason=evidence.creator_reason,
            creator_signed_creation_tx=evidence.creator_signed_creation_tx,
            creator_fee_payer_on_creation_tx=evidence.creator_fee_payer_on_creation_tx,
            creator_is_update_authority=evidence.creator_is_update_authority,
            creator_is_mint_authority=evidence.creator_is_mint_authority,
            creator_is_freeze_authority=evidence.creator_is_freeze_authority,
            creator_matches_platform_dev_evidence_if_any=evidence.creator_matches_platform_dev_evidence_if_any,
            update_authority=evidence.update_authority,
            mint_authority=evidence.mint_authority,
            freeze_authority=evidence.freeze_authority,
            metadata_authority=evidence.metadata_authority,
            platform_dev_wallet=evidence.platform_dev_wallet,
            evidence_signature_list=evidence.evidence_signatures,
            evidence_class_count=evidence.evidence_class_count,
            evidence_mint_init=evidence.evidence_mint_init,
            evidence_metadata_create=evidence.evidence_metadata_create,
            evidence_platform_create=evidence.evidence_platform_create,
            evidence_creator_signer=evidence.creator_signed_creation_tx,
            evidence_creator_fee_payer=evidence.creator_fee_payer_on_creation_tx,
        )

    def extract_creation_evidence(
        self,
        mint: str,
        wallet: str,
        tx_detail: Mapping[str, Any],
        seed_tx: Optional[Mapping[str, Any]],
    ) -> Optional[CreatorEvidence]:
        transaction = tx_detail.get("transaction", {}) or {}
        message = transaction.get("message", {}) or {}
        account_keys = message.get("accountKeys", []) or []
        pubkeys = [entry.get("pubkey") if isinstance(entry, dict) else entry for entry in account_keys]
        signers = {entry.get("pubkey") for entry in account_keys if isinstance(entry, dict) and entry.get("signer")}
        fee_payer = pubkeys[0] if pubkeys else ""
        instructions = list(message.get("instructions", []) or []) + list((tx_detail.get("meta") or {}).get("innerInstructions") or [])
        parsed_info = (((self.api.get_account_info(mint) or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
        mint_authority = blank_if_none(parsed_info.get("mintAuthority"))
        freeze_authority = blank_if_none(parsed_info.get("freezeAuthority"))
        metadata_authority = ""
        update_authority = ""
        explicit_mint_init = False
        explicit_metadata_create = False
        platform_dev_wallet = ""
        for ix in flatten_instructions(instructions):
            program_id = instruction_program_id(ix)
            parsed = ix.get("parsed") if isinstance(ix, dict) else None
            ix_type = parsed.get("type") if isinstance(parsed, dict) else ""
            info = parsed.get("info") if isinstance(parsed, dict) else {}
            if program_id == TOKEN_PROGRAM_ID and ix_type in MINT_INIT_TYPES and info.get("mint") == mint:
                explicit_mint_init = True
            if program_id == METADATA_PROGRAM_ID and ix_type in CREATE_METADATA_TYPES and info.get("mint") == mint:
                explicit_metadata_create = True
                metadata_authority = blank_if_none(info.get("mintAuthority"))
                update_authority = blank_if_none(info.get("updateAuthority"))
            if is_platform_create_instruction(ix, mint):
                platform_dev_wallet = extract_platform_dev_wallet(ix)
        if not explicit_mint_init and not explicit_metadata_create and not platform_dev_wallet:
            return None

        evidence = CreatorEvidence(
            creator_wallet_determinable=wallet if wallet in signers or wallet == fee_payer else "",
            creator_signed_creation_tx=truthy_flag(wallet in signers and explicit_mint_init),
            creator_fee_payer_on_creation_tx=truthy_flag(wallet == fee_payer and (explicit_mint_init or explicit_metadata_create or bool(platform_dev_wallet))),
            creator_is_update_authority=truthy_flag(update_authority == wallet),
            creator_is_mint_authority=truthy_flag(mint_authority == wallet),
            creator_is_freeze_authority=truthy_flag(freeze_authority == wallet),
            creator_matches_platform_dev_evidence_if_any=truthy_flag(platform_dev_wallet == wallet if platform_dev_wallet else False),
            update_authority=update_authority,
            mint_authority=mint_authority,
            freeze_authority=freeze_authority,
            metadata_authority=metadata_authority,
            platform_dev_wallet=platform_dev_wallet,
            evidence_signatures="|".join(sig for sig in transaction.get("signatures", []) if sig),
            evidence_mint_init=truthy_flag(explicit_mint_init),
            evidence_metadata_create=truthy_flag(explicit_metadata_create),
            evidence_platform_create=truthy_flag(bool(platform_dev_wallet)),
        )
        class_count = sum(
            1
            for value in (
                evidence.creator_signed_creation_tx,
                evidence.creator_fee_payer_on_creation_tx,
                evidence.creator_is_update_authority,
                evidence.creator_is_mint_authority,
                evidence.creator_is_freeze_authority,
                evidence.creator_matches_platform_dev_evidence_if_any,
            )
            if value == "TRUE"
        )
        evidence.evidence_class_count = str(class_count)
        if evidence.creator_signed_creation_tx == "TRUE" and (
            evidence.creator_fee_payer_on_creation_tx == "TRUE"
            or evidence.creator_is_update_authority == "TRUE"
            or evidence.creator_matches_platform_dev_evidence_if_any == "TRUE"
        ):
            evidence.creator_status = "CONFIRMED_CREATOR"
            evidence.creator_reason = "Creation evidence includes signed mint initialization plus an additional creator-class linkage."
            evidence.creator_wallet_determinable = wallet
        elif class_count == 0:
            evidence.creator_status = "NOT_CONFIRMED"
            evidence.creator_reason = "Explicit mint creation instructions were located, but no creator-class evidence tied them to the suspected wallet."
            if mint_authority and mint_authority != wallet:
                evidence.creator_wallet_determinable = mint_authority
        else:
            evidence.creator_status = "INCONCLUSIVE"
            evidence.creator_reason = "Creation evidence exists but does not meet the confirmation threshold without ambiguity."
            evidence.creator_wallet_determinable = evidence.creator_wallet_determinable or mint_authority or update_authority or platform_dev_wallet
        if seed_tx and seed_tx.get("signature"):
            signatures = {sig for sig in evidence.evidence_signatures.split("|") if sig}
            signatures.add(seed_tx["signature"])
            evidence.evidence_signatures = "|".join(sorted(signatures))
        return evidence

    def apply_metadata(self, record: CreatedTokenRecord, metadata: Mapping[str, Any]) -> None:
        token_info = metadata.get("onChainMetadata") or {}
        metadata_data = token_info.get("metadata", {}) or {}
        record.name = blank_if_none(metadata_data.get("name") or metadata.get("name"))
        record.symbol = blank_if_none(metadata_data.get("symbol") or metadata.get("symbol"))
        record.update_authority = record.update_authority or blank_if_none(token_info.get("updateAuthority"))
        record.metadata_authority = record.metadata_authority or record.update_authority
        if record.update_authority == self.config.wallet:
            record.creator_is_update_authority = "TRUE"
        if not record.creator_wallet:
            record.creator_wallet = record.update_authority or record.mint_authority or self.config.wallet

    def build_token_dossier(self, token: CreatedTokenRecord) -> Dict[str, Any]:
        mint_txs = self.api.helius_get_all_transactions(token.mint, max_pages=25)
        (self.raw_evidence_dir / f"mint_{token.mint}_helius_transactions.json").write_text(json.dumps(mint_txs, indent=2), encoding="utf-8")
        creation_ts = safe_int(token.creation_time_unix)
        launch_window_end = creation_ts + (self.config.launch_window_minutes * 60) if creation_ts else 0
        creator_wallet = token.creator_wallet or self.config.wallet
        first_liquidity, first_liquidity_evidence = self.find_first_liquidity_event(mint_txs, token.mint)
        first_trade, first_trade_evidence = self.find_first_trade_event(mint_txs, token.mint)
        creator_flows = self.extract_creator_flows(token, mint_txs, creation_ts, launch_window_end)
        early_buyers, early_receipts = self.extract_early_participants(token, mint_txs, creation_ts, launch_window_end)
        funding_graph = self.build_creator_funding_graph(token.mint, creator_wallet, creation_ts, launch_window_end, early_buyers)
        transfer_links = self.detect_creator_token_transfers(token, mint_txs, early_buyers, funding_graph)
        apply_funding_to_early_buyers(early_buyers, funding_graph, transfer_links)

        creator_buy = any(row.flow_type == "creator_buy" for row in creator_flows)
        creator_sell_rows = [row for row in creator_flows if row.flow_type == "creator_sell"]
        creator_first_sell_ts = min((safe_int(row.event_time_unix) for row in creator_sell_rows if row.event_time_unix), default=0)
        creator_cumulative_sold_raw = sum_decimal_strings([row.raw_amount for row in creator_sell_rows])
        linked_buyers = [
            row
            for row in early_buyers
            if row.funded_by_creator_directly == "TRUE"
            or row.funded_by_creator_indirectly == "TRUE"
            or row.received_token_from_creator == "TRUE"
            or row.received_token_from_creator_linked_wallet == "TRUE"
        ]
        current_visible_balance, visible_rank = self.compute_creator_current_visible_balance(token)
        creator_exit_flag = "FALSE"
        if creator_first_sell_ts:
            public_momentum_ts = safe_int(first_trade.get("timestamp")) if first_trade else 0
            if launch_window_end and creator_first_sell_ts <= launch_window_end:
                creator_exit_flag = "TRUE"
            elif public_momentum_ts and creator_first_sell_ts <= public_momentum_ts:
                creator_exit_flag = "TRUE"
        summary = TokenSummary(
            mint=token.mint,
            symbol=token.symbol,
            name=token.name,
            creator_wallet=creator_wallet,
            creator_confirmed=truthy_flag(token.creator_status == "CONFIRMED_CREATOR"),
            creation_signature=token.creation_signature,
            creation_time_iso=token.creation_time_iso,
            creation_time_unix=token.creation_time_unix,
            platform=token.platform,
            first_liquidity_signature=or_null(first_liquidity.get("signature") if first_liquidity else ""),
            first_liquidity_time_iso=or_null(utc_iso(first_liquidity.get("timestamp")) if first_liquidity else ""),
            first_liquidity_time_unix=or_null(str(first_liquidity.get("timestamp", "")) if first_liquidity else ""),
            first_liquidity_evidence_class=or_null(first_liquidity_evidence),
            creation_to_first_liquidity_seconds=or_null(str((safe_int(first_liquidity.get("timestamp")) - creation_ts)) if first_liquidity and creation_ts else ""),
            first_meaningful_trade_signature=or_null(first_trade.get("signature") if first_trade else ""),
            first_meaningful_trade_time_iso=or_null(utc_iso(first_trade.get("timestamp")) if first_trade else ""),
            first_meaningful_trade_time_unix=or_null(str(first_trade.get("timestamp", "")) if first_trade else ""),
            first_trade_evidence_class=or_null(first_trade_evidence),
            creator_bought_own_token=truthy_flag(creator_buy),
            creator_sold_own_token=truthy_flag(bool(creator_sell_rows)),
            creator_first_sell_time_iso=or_null(utc_iso(creator_first_sell_ts)),
            creator_first_sell_time_unix=or_null(str(creator_first_sell_ts or "")),
            creator_cumulative_sold_raw=or_null(creator_cumulative_sold_raw),
            creator_directly_funded_early_buyer=truthy_flag(any(row.funded_by_creator_directly == "TRUE" for row in early_buyers)),
            creator_indirectly_funded_early_buyer=truthy_flag(any(row.funded_by_creator_indirectly == "TRUE" for row in early_buyers)),
            creator_directly_linked_to_early_buyer=truthy_flag(any(row.funded_by_creator_directly == "TRUE" for row in early_buyers)),
            creator_indirectly_linked_to_early_buyer=truthy_flag(any(row.funded_by_creator_indirectly == "TRUE" for row in early_buyers)),
            creator_directly_prefunded_early_buyer=truthy_flag(any(row.directly_prefunded == "TRUE" for row in early_buyers)),
            creator_indirectly_prefunded_early_buyer=truthy_flag(any(row.indirectly_prefunded == "TRUE" for row in early_buyers)),
            early_buyer_received_tokens_from_creator=truthy_flag(any(row.received_token_from_creator == "TRUE" or row.received_token_from_creator_linked_wallet == "TRUE" for row in early_buyers)),
            multiple_creator_linked_early_buyers=truthy_flag(len(linked_buyers) >= 2),
            creator_exited_early=creator_exit_flag,
            creator_current_visible_balance=or_null(current_visible_balance),
            creator_visible_holder_rank_if_known=or_null(visible_rank),
            creator_current_visible_balance_known=truthy_flag(bool(current_visible_balance)),
            insufficient_data=truthy_flag(not mint_txs),
            creator_trade_evidence_signatures="|".join(sorted({row.signature for row in creator_flows if row.signature})),
            funding_evidence_signatures="|".join(sorted({edge.signature for edge in funding_graph.edges if edge.signature})),
            notes=token.creator_reason,
        )
        self.token_counts[token.mint] = {
            "early_buyers": len(early_buyers),
            "early_receipts": len(early_receipts),
            "funding_edges": len(funding_graph.edges),
        }
        return {
            "summary": summary,
            "early_buyers": early_buyers,
            "early_receipts": early_receipts,
            "funding_edges": funding_graph.edges,
            "creator_flows": creator_flows,
        }

    def find_first_liquidity_event(self, txs: Sequence[dict[str, Any]], mint: str) -> Tuple[Optional[dict[str, Any]], str]:
        for tx in txs:
            tx_type = upper(tx.get("type"))
            source = upper(tx.get("source"))
            if tx_type in LIQUIDITY_TYPES and source in DEX_SOURCES and has_token_transfer_for_mint(tx, mint):
                return tx, "explicit_liquidity_tx_type"
        return None, ""

    def find_first_trade_event(self, txs: Sequence[dict[str, Any]], mint: str) -> Tuple[Optional[dict[str, Any]], str]:
        for tx in txs:
            tx_type = upper(tx.get("type"))
            source = upper(tx.get("source"))
            if tx_type in SWAP_TYPES and source in DEX_SOURCES and has_token_transfer_for_mint(tx, mint):
                return tx, "explicit_swap_tx_type"
        return None, ""

    def extract_creator_flows(
        self,
        token: CreatedTokenRecord,
        txs: Sequence[dict[str, Any]],
        creation_ts: int,
        launch_window_end: int,
    ) -> List[CreatorFlowRow]:
        creator_wallet = token.creator_wallet or self.config.wallet
        rows: List[CreatorFlowRow] = []
        for tx in txs:
            ts = safe_int(tx.get("timestamp"))
            if creation_ts and ts and ts < creation_ts - 300:
                continue
            if launch_window_end and ts and ts > launch_window_end + 86400:
                continue
            signature = tx.get("signature", "")
            slot = str(tx.get("slot", ""))
            tx_type = upper(tx.get("type"))
            source = upper(tx.get("source"))
            for native_transfer in tx.get("nativeTransfers", []) or []:
                src = native_transfer.get("fromUserAccount", "")
                dst = native_transfer.get("toUserAccount", "")
                amount = str(safe_int(native_transfer.get("amount")))
                if src == creator_wallet:
                    rows.append(CreatorFlowRow(utc_iso(ts), str(ts or ""), slot, signature, "creator_native_sol_out", "", dst, amount, lamports_to_sol_string(amount), "native transfer out from creator", "native_transfer"))
                if dst == creator_wallet:
                    rows.append(CreatorFlowRow(utc_iso(ts), str(ts or ""), slot, signature, "creator_native_sol_in", "", src, amount, lamports_to_sol_string(amount), "native transfer into creator", "native_transfer"))
            for transfer in tx.get("tokenTransfers", []) or []:
                if transfer.get("mint") != token.mint:
                    continue
                src = transfer.get("fromUserAccount", "")
                dst = transfer.get("toUserAccount", "")
                raw_amount = raw_amount_from_transfer(transfer)
                ui_amount = ui_amount_from_transfer(transfer)
                if dst == creator_wallet:
                    flow_type = "creator_buy" if tx_type in SWAP_TYPES and source in DEX_SOURCES else "creator_token_transfer_in"
                    evidence = "swap" if flow_type == "creator_buy" else "token_transfer"
                    rows.append(CreatorFlowRow(utc_iso(ts), str(ts or ""), slot, signature, flow_type, token.mint, src, raw_amount, ui_amount, "token arrived at creator wallet", evidence))
                elif src == creator_wallet:
                    flow_type = "creator_sell" if tx_type in SWAP_TYPES and source in DEX_SOURCES else "creator_token_transfer_out"
                    evidence = "swap" if flow_type == "creator_sell" else "token_transfer"
                    rows.append(CreatorFlowRow(utc_iso(ts), str(ts or ""), slot, signature, flow_type, token.mint, dst, raw_amount, ui_amount, "token left creator wallet", evidence))
        unique = {(row.signature, row.flow_type, row.counterparty, row.raw_amount): row for row in rows}
        return sorted(unique.values(), key=lambda row: (safe_int(row.event_time_unix), row.signature, row.flow_type))

    def extract_early_participants(
        self,
        token: CreatedTokenRecord,
        txs: Sequence[dict[str, Any]],
        creation_ts: int,
        launch_window_end: int,
    ) -> Tuple[List[EarlyBuyerRow], List[EarlyReceiptRow]]:
        buyers: Dict[str, EarlyBuyerRow] = {}
        receipts: Dict[str, EarlyReceiptRow] = {}
        creator_wallet = token.creator_wallet or self.config.wallet
        for tx in txs:
            ts = safe_int(tx.get("timestamp"))
            if creation_ts and ts and ts < creation_ts:
                continue
            if launch_window_end and ts and ts > launch_window_end:
                continue
            signature = tx.get("signature", "")
            slot = str(tx.get("slot", ""))
            tx_type = upper(tx.get("type"))
            source = upper(tx.get("source"))
            is_swap = tx_type in SWAP_TYPES and source in DEX_SOURCES
            for transfer in tx.get("tokenTransfers", []) or []:
                if transfer.get("mint") != token.mint:
                    continue
                src = transfer.get("fromUserAccount", "")
                dst = transfer.get("toUserAccount", "")
                raw_amount = raw_amount_from_transfer(transfer)
                ui_amount = ui_amount_from_transfer(transfer)
                if is_swap and dst and dst != creator_wallet:
                    if dst not in buyers:
                        buyers[dst] = EarlyBuyerRow(
                            wallet=dst,
                            first_buy_sig=signature,
                            first_buy_time_iso=utc_iso(ts),
                            first_buy_time_unix=str(ts or ""),
                            first_buy_slot=slot,
                            raw_amount=raw_amount,
                            ui_amount=ui_amount,
                            event_type="swap_buy",
                        )
                elif dst and dst != creator_wallet:
                    if dst not in receipts:
                        receipt_type = "transfer" if tx_type == "TRANSFER" else "unknown_token_receipt"
                        if not src:
                            receipt_type = "airdrop"
                        receipts[dst] = EarlyReceiptRow(
                            wallet=dst,
                            first_receive_sig=signature,
                            first_receive_time_iso=utc_iso(ts),
                            first_receive_time_unix=str(ts or ""),
                            first_receive_slot=slot,
                            raw_amount=raw_amount,
                            ui_amount=ui_amount,
                            receipt_type=receipt_type,
                            source_wallet=src,
                        )
            if len(buyers) >= self.config.early_buyers_limit:
                break
        ordered_buyers = list(sorted(buyers.values(), key=lambda row: (safe_int(row.first_buy_time_unix), row.first_buy_sig)))[: self.config.early_buyers_limit]
        ordered_receipts = list(sorted(receipts.values(), key=lambda row: (safe_int(row.first_receive_time_unix), row.first_receive_sig)))
        return ordered_buyers, ordered_receipts

    def build_creator_funding_graph(
        self,
        mint: str,
        creator_wallet: str,
        launch_window_start: int,
        launch_window_end: int,
        early_buyers: Sequence[EarlyBuyerRow],
    ) -> FundingGraphResult:
        buyer_wallets = {row.wallet for row in early_buyers}
        queue: Deque[Tuple[str, int]] = deque([(creator_wallet, 0)])
        visited_depth: Dict[str, int] = {creator_wallet: 0}
        path_by_wallet: Dict[str, List[str]] = {creator_wallet: [creator_wallet]}
        edges: Dict[Tuple[str, str, str, int], FundingEdge] = {}

        while queue:
            current_wallet, current_depth = queue.popleft()
            if current_depth >= self.config.funding_depth:
                continue
            txs = self.api.helius_get_all_transactions(current_wallet, max_pages=10)
            if current_wallet != creator_wallet:
                (self.raw_evidence_dir / f"wallet_{current_wallet}_helius_transactions.json").write_text(json.dumps(txs, indent=2), encoding="utf-8")
            for tx in txs:
                ts = safe_int(tx.get("timestamp"))
                if launch_window_start and ts and ts < launch_window_start:
                    continue
                if launch_window_end and ts and ts > launch_window_end:
                    continue
                signature = tx.get("signature", "")
                slot = str(tx.get("slot", ""))
                for native_transfer in tx.get("nativeTransfers", []) or []:
                    src = native_transfer.get("fromUserAccount", "")
                    dst = native_transfer.get("toUserAccount", "")
                    amount_lamports = str(safe_int(native_transfer.get("amount")))
                    if src != current_wallet or not dst or amount_lamports == "0":
                        continue
                    depth_from_creator = current_depth + 1
                    relation_type = "direct_native_transfer" if current_wallet == creator_wallet else "indirect_native_transfer"
                    edge = FundingEdge(
                        token_mint=mint,
                        src_wallet=src,
                        dst_wallet=dst,
                        depth_from_creator=depth_from_creator,
                        amount_lamports=amount_lamports,
                        amount_sol=lamports_to_sol_string(amount_lamports),
                        signature=signature,
                        slot=slot,
                        block_time_iso=utc_iso(ts),
                        block_time_unix=str(ts or ""),
                        block_time=str(ts or ""),
                        relation_type=relation_type,
                        path_root_wallet=creator_wallet,
                        launch_window_included="TRUE",
                    )
                    edges[(src, dst, signature, depth_from_creator)] = edge
                    next_path = path_by_wallet[current_wallet] + [dst]
                    if dst not in path_by_wallet or len(next_path) < len(path_by_wallet[dst]):
                        path_by_wallet[dst] = next_path
                    if visited_depth.get(dst, 99) > depth_from_creator:
                        visited_depth[dst] = depth_from_creator
                        queue.append((dst, depth_from_creator))
        if buyer_wallets:
            filtered_edges = {
                key: edge
                for key, edge in edges.items()
                if edge.dst_wallet in buyer_wallets or edge.src_wallet in path_nodes_for_targets(path_by_wallet, buyer_wallets)
            }
        else:
            filtered_edges = edges
        return FundingGraphResult(
            edges=sorted(filtered_edges.values(), key=lambda edge: (safe_int(edge.block_time_unix), edge.signature, edge.src_wallet, edge.dst_wallet)),
            creator_linked_wallet_depth=visited_depth,
            path_by_wallet={wallet: path for wallet, path in path_by_wallet.items() if wallet in buyer_wallets or wallet == creator_wallet},
            prefund_max_seconds=self.config.prefund_max_seconds,
        )

    def detect_creator_token_transfers(
        self,
        token: CreatedTokenRecord,
        txs: Sequence[dict[str, Any]],
        early_buyers: Sequence[EarlyBuyerRow],
        funding_graph: FundingGraphResult,
    ) -> Dict[str, Dict[str, str]]:
        creator = token.creator_wallet or self.config.wallet
        buyer_wallets = {row.wallet for row in early_buyers}
        linked_wallets = {wallet for wallet, depth in funding_graph.creator_linked_wallet_depth.items() if 0 < depth <= self.config.funding_depth}
        evidence: Dict[str, Dict[str, str]] = {}
        for tx in txs:
            signature = tx.get("signature", "")
            for transfer in tx.get("tokenTransfers", []) or []:
                if transfer.get("mint") != token.mint:
                    continue
                src = transfer.get("fromUserAccount", "")
                dst = transfer.get("toUserAccount", "")
                if dst not in buyer_wallets:
                    continue
                if src == creator:
                    evidence[dst] = {"signature": signature, "source": src, "kind": "creator_direct"}
                elif src in linked_wallets:
                    evidence[dst] = {"signature": signature, "source": src, "kind": "creator_linked"}
        return evidence

    def compute_creator_current_visible_balance(self, token: CreatedTokenRecord) -> Tuple[str, str]:
        largest = self.api.get_token_largest_accounts(token.mint)
        addresses = [row.get("address") for row in largest if row.get("address")]
        owners = self.api.get_multiple_accounts(addresses) if addresses else []
        creator_wallet = token.creator_wallet or self.config.wallet
        for rank, (largest_row, owner_account) in enumerate(zip(largest, owners), start=1):
            parsed = (((owner_account or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
            owner = parsed.get("owner", "")
            if owner == creator_wallet:
                visible_balance = blank_if_none(largest_row.get("amount") or largest_row.get("uiAmountString"))
                return visible_balance, str(rank)
        return "", ""

    def write_dossier_files(self, dossier_dir: Path, mint: str, dossier: Mapping[str, Any]) -> List[str]:
        generated: List[str] = []
        summary_path = dossier_dir / f"{mint}.summary.tsv"
        self.write_tsv(summary_path, [asdict(dossier["summary"])])
        generated.append(str(summary_path.relative_to(self.config.outdir)))
        buyers_path = dossier_dir / f"{mint}.early_buyers.tsv"
        self.write_tsv(buyers_path, [asdict(row) for row in dossier["early_buyers"]])
        generated.append(str(buyers_path.relative_to(self.config.outdir)))
        receipts_path = dossier_dir / f"{mint}.early_receipts.tsv"
        self.write_tsv(receipts_path, [asdict(row) for row in dossier["early_receipts"]])
        generated.append(str(receipts_path.relative_to(self.config.outdir)))
        edges_path = dossier_dir / f"{mint}.funding_edges.tsv"
        self.write_tsv(edges_path, [asdict(row) for row in dossier["funding_edges"]])
        generated.append(str(edges_path.relative_to(self.config.outdir)))
        flows_path = dossier_dir / f"{mint}.creator_flows.tsv"
        self.write_tsv(flows_path, [asdict(row) for row in dossier["creator_flows"]])
        generated.append(str(flows_path.relative_to(self.config.outdir)))
        return generated

    def build_aggregate_summary(self, summaries: Sequence[TokenSummary], wallets: Counter[str], intermediaries: Counter[str]) -> Dict[str, Any]:
        sell_deltas = [
            safe_int(item.creator_first_sell_time_unix) - safe_int(item.creation_time_unix)
            for item in summaries
            if item.creator_first_sell_time_unix not in {"", NULL_VALUE} and item.creation_time_unix not in {"", NULL_VALUE}
        ]
        liquidity_deltas = [
            safe_int(item.creation_to_first_liquidity_seconds)
            for item in summaries
            if item.creation_to_first_liquidity_seconds not in {"", NULL_VALUE}
        ]
        return {
            "target_wallet": self.config.wallet,
            "total_created_tokens_found": len(summaries),
            "tokens_where_creator_also_traded": sum(1 for item in summaries if item.creator_bought_own_token == "TRUE" or item.creator_sold_own_token == "TRUE"),
            "tokens_where_creator_sold": sum(1 for item in summaries if item.creator_sold_own_token == "TRUE"),
            "tokens_with_creator_directly_funded_early_buyers": sum(1 for item in summaries if item.creator_directly_funded_early_buyer == "TRUE"),
            "tokens_with_creator_indirectly_funded_early_buyers": sum(1 for item in summaries if item.creator_indirectly_funded_early_buyer == "TRUE"),
            "tokens_with_creator_directly_prefunded_early_buyers": sum(1 for item in summaries if item.creator_directly_prefunded_early_buyer == "TRUE"),
            "tokens_with_creator_indirectly_prefunded_early_buyers": sum(1 for item in summaries if item.creator_indirectly_prefunded_early_buyer == "TRUE"),
            "tokens_with_creator_linked_wallets_among_early_buyers": sum(1 for item in summaries if item.multiple_creator_linked_early_buyers == "TRUE" or item.early_buyer_received_tokens_from_creator == "TRUE"),
            "median_creation_to_creator_first_sell_seconds": str(int(statistics.median(sell_deltas))) if sell_deltas else NULL_VALUE,
            "median_creation_to_first_liquidity_seconds": str(int(statistics.median(liquidity_deltas))) if liquidity_deltas else NULL_VALUE,
            "top_repeated_linked_wallets": format_counter(wallets),
            "repeated_intermediaries": format_counter(intermediaries),
            "repeated_patterns": self.describe_repeated_patterns(summaries),
        }

    def describe_repeated_patterns(self, summaries: Sequence[TokenSummary]) -> str:
        total = len(summaries)
        if total == 0:
            return "No creator-linked token creations were confirmed from the available evidence."
        return "; ".join(
            [
                f"creator sold in {sum(1 for item in summaries if item.creator_sold_own_token == 'TRUE')}/{total} tokens",
                f"direct creator funding of early buyers observed in {sum(1 for item in summaries if item.creator_directly_funded_early_buyer == 'TRUE')}/{total} tokens",
                f"indirect creator funding of early buyers observed in {sum(1 for item in summaries if item.creator_indirectly_funded_early_buyer == 'TRUE')}/{total} tokens",
                f"creator-linked early buyer cluster observed in {sum(1 for item in summaries if item.multiple_creator_linked_early_buyers == 'TRUE')}/{total} tokens",
            ]
        )

    def render_summary_txt(
        self,
        verification: Optional[Mapping[str, Any]],
        created_tokens: Sequence[CreatedTokenRecord],
        summaries: Sequence[TokenSummary],
        aggregate: Mapping[str, Any],
        total_early_buyers: int,
        total_early_receipts: int,
        total_funding_edges: int,
    ) -> str:
        lines = [
            "Creator Audit Summary",
            f"Run timestamp UTC: {utc_iso(int(time.time()))}",
            f"Target wallet: {self.config.wallet}",
            f"Seed mint: {self.config.seed_mint or ''}",
            "",
            "What was confirmed:",
        ]
        if verification:
            lines.extend(
                [
                    f"- Seed mint creator status: {verification.get('status', '')}",
                    f"- Creator evidence classes: signed_creation_tx={verification.get('creator_signed_creation_tx', '')}, fee_payer={verification.get('creator_fee_payer_on_creation_tx', '')}, update_authority={verification.get('creator_is_update_authority', '')}, mint_authority={verification.get('creator_is_mint_authority', '')}, freeze_authority={verification.get('creator_is_freeze_authority', '')}, platform_dev_match={verification.get('creator_matches_platform_dev_evidence_if_any', '')}",
                    f"- Basis: {verification.get('reason', '')}",
                ]
            )
        else:
            lines.append("- No seed mint verification requested.")
        lines.extend(
            [
                "",
                "What was inconclusive:",
                "- Any field left blank or NULL was not reliably determinable from the available evidence.",
                "- Creator confirmation requires explicit creator-class evidence and is not inferred from early trading alone.",
                "",
                "Counts:",
                f"- Created tokens found: {len(created_tokens)}",
                f"- Tokens processed into dossiers: {len(summaries)}",
                f"- Early buyers found: {total_early_buyers}",
                f"- Early receipts found: {total_early_receipts}",
                f"- Funding edges found: {total_funding_edges}",
                f"- Tokens where creator sold: {aggregate.get('tokens_where_creator_sold', 0)}",
                f"- Tokens with direct creator funding of early buyers: {aggregate.get('tokens_with_creator_directly_funded_early_buyers', 0)}",
                f"- Tokens with indirect creator funding of early buyers: {aggregate.get('tokens_with_creator_indirectly_funded_early_buyers', 0)}",
                f"- Tokens with direct creator prefunding of early buyers: {aggregate.get('tokens_with_creator_directly_prefunded_early_buyers', 0)}",
                f"- Tokens with indirect creator prefunding of early buyers: {aggregate.get('tokens_with_creator_indirectly_prefunded_early_buyers', 0)}",
                "",
                "Creator-linked relationship evidence:",
                "- creator-centric native SOL path exists within the launch window",
                "- token transfer relationships from creator or creator-linked wallets are recorded separately",
                "",
                "Creator-linked prefunding evidence:",
                "- prefunding is only counted when the latest creator-linked incoming native transfer into a buyer is observed before the buyer's first buy and within the prefunding window",
                "",
                "Creator token trading evidence:",
                "- creator buy/sell evidence is limited to explicit swap transactions involving the mint",
                "",
                "Creator token transfer evidence:",
                "- creator token transfer in/out rows are recorded separately from swap buys and sells",
                "",
                "Evidence classes used:",
                "- creator-signed creation transaction",
                "- creator fee-payer role on creation transaction",
                "- mint/update/freeze authority matches",
                "- swap-based early buyer evidence",
                "- token transfer receipts recorded separately from buys",
                "",
                "Repeated patterns observed:",
                f"- {aggregate.get('repeated_patterns', '')}",
                f"- Top repeated linked wallets: {aggregate.get('top_repeated_linked_wallets', '') or 'None found'}",
                f"- Repeated intermediaries: {aggregate.get('repeated_intermediaries', '') or 'None found'}",
                "",
                "Limitations:",
                "- Results are limited to transactions available from Helius and the configured Solana RPC endpoint.",
                "- Creator_exited_early is TRUE only when an observed creator sell occurs within the launch window or before the first meaningful trade time if determinable.",
                "- Creator_current_visible_balance reflects current visible holder data only and is not a reconstructed launch-supply retention measure.",
                "- Relationship linkage does not by itself prove causal funding; prefunding requires observed creator-linked funding before the buy within the prefunding window.",
                "- First liquidity and first trade are left NULL when explicit evidence is not available.",
                f"- API failure counters: helius={self.failures.helius_failures}, rpc={self.failures.rpc_failures}, metadata={self.failures.metadata_failures}, partial={self.failures.partial_failures}.",
            ]
        )
        return "\n".join(lines) + "\n"

    def write_tsv(self, path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
        if not rows:
            path.write_text("\n", encoding="utf-8")
            return
        headers = list(rows[0].keys())
        lines = ["\t".join(headers)]
        for row in rows:
            values = [sanitize_tsv_value(row.get(header, "")) for header in headers]
            lines.append("\t".join(values))
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def wallet_is_signer_or_fee_payer(self, tx: Mapping[str, Any], wallet: str) -> bool:
        if tx.get("feePayer") == wallet:
            return True
        return wallet in (tx.get("signers") or [])

    def extract_candidate_mints_from_explicit_instruction_accounts(self, tx: Mapping[str, Any]) -> List[str]:
        candidates: Set[str] = set()
        for instruction in tx.get("instructions", []) or []:
            program_id = instruction_program_id(instruction)
            parsed = instruction.get("parsed") if isinstance(instruction, dict) else None
            ix_type = parsed.get("type") if isinstance(parsed, dict) else ""
            info = parsed.get("info") if isinstance(parsed, dict) else {}
            if program_id == TOKEN_PROGRAM_ID and ix_type in MINT_INIT_TYPES:
                mint = info.get("mint") if isinstance(info, dict) else None
                if is_probable_mint_address(mint):
                    candidates.add(mint)
            if program_id == METADATA_PROGRAM_ID and ix_type in CREATE_METADATA_TYPES:
                mint = info.get("mint") if isinstance(info, dict) else None
                if is_probable_mint_address(mint):
                    candidates.add(mint)
            if is_platform_create_instruction(instruction):
                for account in instruction_accounts(instruction):
                    if is_probable_mint_address(account):
                        candidates.add(account)
        return sorted(candidates)


def flatten_instructions(instructions: Sequence[Any]) -> Iterable[dict[str, Any]]:
    for item in instructions:
        if not isinstance(item, dict):
            continue
        if isinstance(item.get("instructions"), list):
            for child in item["instructions"]:
                if isinstance(child, dict):
                    yield child
        else:
            yield item


def apply_funding_to_early_buyers(
    early_buyers: Sequence[EarlyBuyerRow],
    funding_graph: FundingGraphResult,
    transfer_links: Mapping[str, Mapping[str, str]],
) -> None:
    edges_by_wallet: Dict[str, List[FundingEdge]] = defaultdict(list)
    for edge in funding_graph.edges:
        edges_by_wallet[edge.dst_wallet].append(edge)
    for row in early_buyers:
        depth = funding_graph.creator_linked_wallet_depth.get(row.wallet)
        if depth == 1:
            row.funded_by_creator_directly = "TRUE"
            row.linked_from_creator = "TRUE"
            row.linked_depth = "1"
        elif depth and depth > 1:
            row.funded_by_creator_indirectly = "TRUE"
            row.linked_from_creator = "TRUE"
            row.linked_depth = str(depth)
            row.indirect_path = ">".join(funding_graph.path_by_wallet.get(row.wallet, []))
        latest_prior_edge = None
        buy_ts = safe_int(row.first_buy_time_unix)
        for edge in sorted(edges_by_wallet.get(row.wallet, []), key=lambda item: safe_int(item.block_time_unix)):
            edge_ts = safe_int(edge.block_time_unix)
            if edge_ts and buy_ts and edge_ts < buy_ts:
                latest_prior_edge = edge
        row.funding_evidence_signatures = "|".join(sorted({edge.signature for edge in edges_by_wallet.get(row.wallet, []) if edge.signature}))
        if latest_prior_edge is not None:
            delta_seconds = buy_ts - safe_int(latest_prior_edge.block_time_unix)
            row.latest_creator_linked_funding_sig_before_buy = latest_prior_edge.signature
            row.latest_creator_linked_funding_time_before_buy = latest_prior_edge.block_time_iso
            row.funding_before_buy = "TRUE"
            row.funding_to_buy_delta_seconds = str(delta_seconds)
            if delta_seconds <= funding_graph.prefund_max_seconds:
                if latest_prior_edge.depth_from_creator == 1:
                    row.directly_prefunded = "TRUE"
                elif latest_prior_edge.depth_from_creator > 1:
                    row.indirectly_prefunded = "TRUE"
        link = transfer_links.get(row.wallet)
        if link:
            row.received_token_signature = link.get("signature", "")
            if link.get("kind") == "creator_direct":
                row.received_token_from_creator = "TRUE"
            elif link.get("kind") == "creator_linked":
                row.received_token_from_creator_linked_wallet = "TRUE"


def path_nodes_for_targets(path_by_wallet: Mapping[str, List[str]], targets: Set[str]) -> Set[str]:
    nodes: Set[str] = set()
    for target in targets:
        nodes.update(path_by_wallet.get(target, []))
    return nodes


def sanitize_tsv_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("\t", " ").replace("\n", " ").replace("\r", " ")


def safe_int(value: Any) -> int:
    try:
        if value in (None, "", NULL_VALUE):
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def utc_iso(ts: Optional[int]) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def parse_decimal(value: Any) -> Decimal:
    try:
        if value in (None, "", NULL_VALUE):
            return Decimal(0)
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(0)


def truthy_flag(value: bool) -> str:
    return "TRUE" if value else "FALSE"


def blank_if_none(value: Any) -> str:
    return "" if value in (None, "") else str(value)


def or_null(value: str) -> str:
    return value if value else NULL_VALUE


def upper(value: Any) -> str:
    return str(value or "").upper()


def lamports_to_sol_string(value: Any) -> str:
    lamports = parse_decimal(value)
    if lamports == 0:
        return "0"
    sol = lamports / LAMPORTS_PER_SOL
    return format_decimal(sol)


def format_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    rendered = format(normalized, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"


def sum_decimal_strings(values: Sequence[str]) -> str:
    total = Decimal(0)
    for value in values:
        total += parse_decimal(value)
    return format_decimal(total) if total else ""


def is_probable_solana_address(value: Any) -> bool:
    return isinstance(value, str) and 32 <= len(value) <= 44 and value not in {SYSTEM_PROGRAM_ID, TOKEN_PROGRAM_ID, ASSOCIATED_TOKEN_PROGRAM_ID, METADATA_PROGRAM_ID}


def is_probable_mint_address(value: Any) -> bool:
    return is_probable_solana_address(value)


def instruction_program_id(instruction: Mapping[str, Any]) -> str:
    return str(instruction.get("programId") or instruction.get("programIdIndex") or instruction.get("program") or "")


def instruction_accounts(instruction: Mapping[str, Any]) -> List[str]:
    accounts = instruction.get("accounts") or []
    return [account for account in accounts if isinstance(account, str)]


def is_platform_create_instruction(instruction: Mapping[str, Any], mint: Optional[str] = None) -> bool:
    parsed = instruction.get("parsed") if isinstance(instruction, dict) else None
    info = parsed.get("info") if isinstance(parsed, dict) else {}
    ix_type = parsed.get("type") if isinstance(parsed, dict) else ""
    if not isinstance(info, dict):
        return False
    mint_roles = [value for key, value in info.items() if "mint" in key.lower() and is_probable_mint_address(value)]
    if mint and mint not in mint_roles:
        return False
    explicit_creator_roles = any(is_probable_solana_address(info.get(key)) for key in ("creator", "user", "authority", "payer", "owner"))
    create_like_type = str(ix_type).lower() in {"create", "create_token", "createmint", "initialize", "initialize2"}
    return bool(mint_roles and explicit_creator_roles and create_like_type)


def extract_platform_dev_wallet(instruction: Mapping[str, Any]) -> str:
    parsed = instruction.get("parsed") if isinstance(instruction, dict) else None
    info = parsed.get("info") if isinstance(parsed, dict) else {}
    for key in ("user", "creator", "owner", "authority", "payer"):
        value = info.get(key) if isinstance(info, dict) else None
        if is_probable_solana_address(value):
            return str(value)
    for account in instruction_accounts(instruction):
        if is_probable_solana_address(account):
            return account
    return ""


def detect_platform(mint: str, helius_tx: Mapping[str, Any], rpc_tx: Mapping[str, Any]) -> str:
    source = blank_if_none(helius_tx.get("source"))
    if source:
        return source
    for instruction in flatten_instructions(((rpc_tx.get("transaction") or {}).get("message") or {}).get("instructions", []) or []):
        if is_platform_create_instruction(instruction, mint):
            return blank_if_none(instruction.get("program")) or blank_if_none(instruction_program_id(instruction))
    return ""


def raw_amount_from_transfer(transfer: Mapping[str, Any]) -> str:
    raw = transfer.get("rawTokenAmount")
    if isinstance(raw, dict):
        return blank_if_none(raw.get("tokenAmount") or raw.get("amount"))
    token_amount = transfer.get("tokenAmount")
    if token_amount is not None:
        return blank_if_none(token_amount)
    return ""


def ui_amount_from_transfer(transfer: Mapping[str, Any]) -> str:
    raw = transfer.get("rawTokenAmount")
    if isinstance(raw, dict):
        ui_amount = raw.get("tokenAmount") or raw.get("uiAmountString")
        if ui_amount not in (None, ""):
            return blank_if_none(ui_amount)
    token_amount = transfer.get("tokenAmount")
    return blank_if_none(token_amount)


def has_token_transfer_for_mint(tx: Mapping[str, Any], mint: str) -> bool:
    return any(transfer.get("mint") == mint for transfer in tx.get("tokenTransfers", []) or [])


def is_liquidity_shaped_tx(tx: Mapping[str, Any]) -> bool:
    transfers = tx.get("tokenTransfers", []) or []
    native_transfers = tx.get("nativeTransfers", []) or []
    return len(transfers) >= 2 or bool(native_transfers)


def format_counter(counter: Counter[str], limit: int = 10) -> str:
    if not counter:
        return NULL_VALUE
    return "|".join(f"{wallet}:{count}" for wallet, count in counter.most_common(limit))


def parse_args(argv: Optional[Sequence[str]] = None) -> AuditConfig:
    parser = argparse.ArgumentParser(description="Forensic Solana creator auditor.")
    parser.add_argument("--wallet", required=True, help="Suspected creator wallet address.")
    parser.add_argument("--seed-mint", dest="seed_mint", help="Seed mint to verify against the wallet.")
    parser.add_argument("--outdir", required=True, help="Output directory.")
    parser.add_argument("--early-buyers-limit", type=int, default=50, help="Number of early buyer wallets to capture per token.")
    parser.add_argument("--funding-depth", type=int, default=2, help="Funding graph depth from creator wallet.")
    parser.add_argument("--launch-window-minutes", type=int, default=60, help="Launch window size in minutes.")
    parser.add_argument("--prefund-max-seconds", type=int, default=3600, help="Maximum allowed seconds between creator-linked funding and first buy for prefunding classification.")
    parser.add_argument("--rpc-url", default=DEFAULT_RPC_URL, help="Optional Solana RPC URL.")
    parser.add_argument("--refresh", action="store_true", help="Accepted for CLI compatibility. Current implementation always fetches live data.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    args = parser.parse_args(argv)
    return AuditConfig(
        wallet=args.wallet,
        seed_mint=args.seed_mint,
        outdir=Path(args.outdir),
        early_buyers_limit=args.early_buyers_limit,
        funding_depth=args.funding_depth,
        launch_window_minutes=args.launch_window_minutes,
        prefund_max_seconds=args.prefund_max_seconds,
        rpc_url=args.rpc_url,
        refresh=args.refresh,
        verbose=args.verbose,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    config = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if config.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    auditor = ForensicAuditor(config)
    auditor.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
