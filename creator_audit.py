#!/usr/bin/env python3
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
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import requests

TOOL_VERSION = "1.0.0"
DEFAULT_RPC_URL = "https://api.mainnet-beta.solana.com"
LAMPORTS_PER_SOL = 1_000_000_000
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
METADATA_PROGRAM_ID = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"
ASSOCIATED_TOKEN_PROGRAM_ID = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"
NULL_VALUE = "NULL"


@dataclass
class AuditConfig:
    wallet: str
    seed_mint: Optional[str]
    outdir: Path
    early_buyers_limit: int = 50
    funding_depth: int = 2
    launch_window_minutes: int = 60
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
    update_authority: str = ""
    mint_authority: str = ""
    freeze_authority: str = ""
    metadata_authority: str = ""
    evidence_signature_list: str = ""


@dataclass
class FundingEdge:
    src_wallet: str
    dst_wallet: str
    amount_lamports: int
    amount_sol: str
    signature: str
    slot: str
    block_time_iso: str
    block_time_unix: str
    hop_depth: int
    path_root_creator: str


@dataclass
class CreatorFlowRow:
    event_type: str
    signature: str
    slot: str
    block_time_iso: str
    block_time_unix: str
    wallet: str
    token_amount: str
    token_mint: str
    sol_change_lamports: str
    sol_change: str
    counterparty: str
    evidence: str


@dataclass
class EarlyBuyerRow:
    wallet: str
    first_buy_time_iso: str
    first_buy_time_unix: str
    amount: str
    slot: str
    signature: str
    funded_by_creator_directly: str
    funded_by_creator_indirectly: str
    indirect_path: str
    received_token_from_creator_or_linked: str
    received_token_signature: str
    funding_evidence_signatures: str


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
    creation_to_first_liquidity_seconds: str = ""
    first_meaningful_trade_signature: str = ""
    first_meaningful_trade_time_iso: str = ""
    creator_bought_own_token: str = "FALSE"
    creator_sold_own_token: str = "FALSE"
    creator_first_sell_time_iso: str = ""
    creator_first_sell_time_unix: str = ""
    creator_cumulative_sold_amount: str = ""
    creator_directly_funded_early_buyer: str = "FALSE"
    creator_indirectly_funded_early_buyer: str = "FALSE"
    early_buyer_received_tokens_from_creator: str = "FALSE"
    multiple_creator_linked_early_buyers: str = "FALSE"
    creator_exited_early: str = "FALSE"
    creator_retained_supply: str = ""
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
                if not response.text:
                    return None
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                is_last = attempt == 4
                if request_kind == "helius":
                    self.failures.helius_failures += 1
                elif request_kind == "rpc":
                    self.failures.rpc_failures += 1
                else:
                    self.failures.metadata_failures += 1
                logging.warning("Request failure (%s) attempt=%s url=%s error=%s", request_kind, attempt + 1, url, exc)
                if is_last:
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
            sig = tx.get("signature")
            if sig:
                deduped[sig] = tx
        return sorted(deduped.values(), key=lambda item: (item.get("timestamp") or 0, item.get("slot") or 0))

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
        params: List[Any] = [address, {"limit": limit, "commitment": "confirmed"}]
        if before:
            params[1]["before"] = before
        result = self.rpc_call("getSignaturesForAddress", params)
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
        return sorted(deduped.values(), key=lambda item: (item.get("blockTime") or 0, item.get("slot") or 0))

    def get_transaction(self, signature: str) -> Optional[dict[str, Any]]:
        result = self.rpc_call(
            "getTransaction",
            [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}],
        )
        return result if isinstance(result, dict) else None

    def get_account_info(self, address: str, encoding: str = "jsonParsed") -> Optional[dict[str, Any]]:
        result = self.rpc_call("getAccountInfo", [address, {"encoding": encoding, "commitment": "confirmed"}])
        return result.get("value") if isinstance(result, dict) else None

    def get_token_largest_accounts(self, mint: str) -> List[dict[str, Any]]:
        result = self.rpc_call("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
        return result.get("value", []) if isinstance(result, dict) else []

    def get_multiple_accounts(self, addresses: Sequence[str], encoding: str = "jsonParsed") -> List[Optional[dict[str, Any]]]:
        if not addresses:
            return []
        result = self.rpc_call("getMultipleAccounts", [list(addresses), {"encoding": encoding, "commitment": "confirmed"}])
        if isinstance(result, dict) and isinstance(result.get("value"), list):
            return result["value"]
        return []


class ForensicAuditor:
    def __init__(self, config: AuditConfig) -> None:
        self.config = config
        self.failures = FailureCounter()
        self.api = ApiClient(config, self.failures)
        self.wallet_transactions: Optional[List[dict[str, Any]]] = None
        self.wallet_signature_index: Optional[List[dict[str, Any]]] = None
        self.wallet_tx_map: Dict[str, dict[str, Any]] = {}
        self.metadata_cache: Dict[str, dict[str, Any]] = {}
        self.raw_evidence_dir = self.config.outdir / "raw_evidence"

    def run(self) -> None:
        self.prepare_dirs()
        created_tokens = self.discover_created_tokens(self.config.wallet)
        verification = self.verify_seed_mint(self.config.seed_mint, self.config.wallet, created_tokens) if self.config.seed_mint else None
        token_summaries: List[TokenSummary] = []
        aggregate_wallets: Counter[str] = Counter()
        aggregate_intermediaries: Counter[str] = Counter()
        generated_files: List[str] = []

        if verification:
            path = self.config.outdir / "creator_verification.tsv"
            self.write_tsv(path, [verification])
            generated_files.append(str(path.relative_to(self.config.outdir)))

        created_path = self.config.outdir / "created_tokens.tsv"
        self.write_tsv(created_path, [asdict(record) for record in created_tokens])
        generated_files.append(str(created_path.relative_to(self.config.outdir)))

        dossier_dir = self.config.outdir / "token_dossiers"
        dossier_dir.mkdir(exist_ok=True)

        for token in created_tokens:
            dossier = self.build_token_dossier(token)
            token_summaries.append(dossier["summary"])
            for row in dossier["early_buyers"]:
                if row.wallet:
                    aggregate_wallets[row.wallet] += 1
                for wallet in [w for w in row.indirect_path.split(">") if w and w != self.config.wallet and w != row.wallet]:
                    aggregate_intermediaries[wallet] += 1

            files = self.write_dossier_files(dossier_dir, token.mint, dossier)
            generated_files.extend(files)

        aggregate_row = self.build_aggregate_summary(token_summaries, aggregate_wallets, aggregate_intermediaries)
        aggregate_path = self.config.outdir / "aggregate_summary.tsv"
        self.write_tsv(aggregate_path, [aggregate_row])
        generated_files.append(str(aggregate_path.relative_to(self.config.outdir)))

        summary_path = self.config.outdir / "summary.txt"
        summary_path.write_text(self.render_summary_txt(verification, created_tokens, token_summaries, aggregate_row), encoding="utf-8")
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
                "rpc_url": self.config.rpc_url,
                "refresh": self.config.refresh,
                "verbose": self.config.verbose,
            },
            "files_generated": sorted(set(generated_files)),
            "counts": {
                "tokens_discovered": len(created_tokens),
                "tokens_processed": len(token_summaries),
                "verification_rows": 1 if verification else 0,
            },
            "api_failures": asdict(self.failures),
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
            raw_path = self.raw_evidence_dir / f"wallet_{self.config.wallet}_helius_transactions.json"
            raw_path.write_text(json.dumps(self.wallet_transactions, indent=2), encoding="utf-8")
        return self.wallet_transactions

    def load_wallet_signature_index(self) -> List[dict[str, Any]]:
        if self.wallet_signature_index is None:
            self.wallet_signature_index = self.api.get_all_signatures_for_address(self.config.wallet, max_pages=50)
            raw_path = self.raw_evidence_dir / f"wallet_{self.config.wallet}_rpc_signatures.json"
            raw_path.write_text(json.dumps(self.wallet_signature_index, indent=2), encoding="utf-8")
        return self.wallet_signature_index

    def discover_created_tokens(self, wallet: str) -> List[CreatedTokenRecord]:
        helius_txs = self.load_wallet_transactions()
        token_candidates: Dict[str, CreatedTokenRecord] = {}
        for tx in helius_txs:
            if not self.wallet_is_signer_or_fee_payer(tx, wallet):
                continue
            mints_in_tx = set(self.extract_mints_from_helius_tx(tx))
            created_mints = set(self.extract_created_mints_from_helius_tx(tx))
            for mint in sorted(mints_in_tx | created_mints):
                if not mint or mint in token_candidates:
                    continue
                creation = self.confirm_token_creation(mint, wallet, tx)
                if creation:
                    token_candidates[mint] = creation
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
                "metadata_authority": matching.metadata_authority or NULL_VALUE,
                "update_authority": matching.update_authority or NULL_VALUE,
                "mint_authority": matching.mint_authority or NULL_VALUE,
                "freeze_authority": matching.freeze_authority or NULL_VALUE,
                "reason": matching.creator_reason or NULL_VALUE,
                "evidence_signatures": matching.evidence_signature_list or NULL_VALUE,
            }
        account_info = self.api.get_account_info(seed_mint)
        parsed = (((account_info or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
        mint_authority_raw = parsed.get("mintAuthority")
        freeze_authority_raw = parsed.get("freezeAuthority")
        mint_authority = coalesce(mint_authority_raw)
        freeze_authority = coalesce(freeze_authority_raw)
        status = "INCONCLUSIVE"
        reason = "Seed mint not found in wallet-linked creation candidates."
        if mint_authority_raw and mint_authority_raw != wallet:
            status = "NOT_CONFIRMED"
            reason = f"Mint authority on current account info is {mint_authority}, not suspected wallet."
        return {
            "mint": seed_mint,
            "suspected_wallet": wallet,
            "status": status,
            "creator_wallet_determinable": coalesce(mint_authority),
            "creation_signature": NULL_VALUE,
            "creation_time_iso": NULL_VALUE,
            "creation_time_unix": NULL_VALUE,
            "slot": NULL_VALUE,
            "platform": NULL_VALUE,
            "metadata_authority": NULL_VALUE,
            "update_authority": NULL_VALUE,
            "mint_authority": coalesce(mint_authority),
            "freeze_authority": coalesce(freeze_authority),
            "reason": reason,
            "evidence_signatures": NULL_VALUE,
        }

    def confirm_token_creation(self, mint: str, wallet: str, seed_tx: Optional[dict[str, Any]]) -> Optional[CreatedTokenRecord]:
        signatures = self.api.get_all_signatures_for_address(mint, max_pages=5)
        if not signatures:
            return None
        first_sig = signatures[0].get("signature", "")
        first_slot = signatures[0].get("slot")
        first_block_time = signatures[0].get("blockTime")
        tx_detail = self.api.get_transaction(first_sig)
        if not tx_detail:
            return None
        evidence = self.extract_creation_evidence(mint, wallet, tx_detail)
        if not evidence:
            return None
        source_tx = seed_tx or self.wallet_tx_map.get(first_sig) or {}
        platform = detect_platform(mint, source_tx, tx_detail)
        record = CreatedTokenRecord(
            mint=mint,
            creation_signature=first_sig,
            creation_time_iso=utc_iso(first_block_time),
            creation_time_unix=str(first_block_time or ""),
            slot=str(first_slot or tx_detail.get("slot") or ""),
            creator_wallet=evidence.get("creator_wallet", ""),
            platform=platform,
            creator_status=evidence.get("status", "INCONCLUSIVE"),
            creator_reason=evidence.get("reason", ""),
            update_authority=evidence.get("update_authority", ""),
            mint_authority=evidence.get("mint_authority", ""),
            freeze_authority=evidence.get("freeze_authority", ""),
            metadata_authority=evidence.get("metadata_authority", ""),
            evidence_signature_list="|".join(sorted(set(evidence.get("evidence_signatures", [first_sig])))),
        )
        return record

    def extract_creation_evidence(self, mint: str, wallet: str, tx_detail: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        transaction = tx_detail.get("transaction", {})
        message = transaction.get("message", {})
        account_keys = message.get("accountKeys", [])
        account_pubkeys = [acc.get("pubkey") if isinstance(acc, dict) else acc for acc in account_keys]
        signers = [acc.get("pubkey") for acc in account_keys if isinstance(acc, dict) and acc.get("signer")]
        fee_payer = account_pubkeys[0] if account_pubkeys else ""
        instructions = list(message.get("instructions", [])) + list((tx_detail.get("meta") or {}).get("innerInstructions") or [])
        parsed_info = (((self.api.get_account_info(mint) or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
        mint_authority = coalesce(parsed_info.get("mintAuthority"), blank="")
        freeze_authority = coalesce(parsed_info.get("freezeAuthority"), blank="")
        metadata_authority = ""
        update_authority = ""
        found_initialize_mint = False
        found_create_account = False
        for ix in flatten_instructions(instructions):
            program = ix.get("programId") or ix.get("program") or ""
            parsed = ix.get("parsed") or {}
            ix_type = parsed.get("type") if isinstance(parsed, dict) else ""
            info = parsed.get("info") if isinstance(parsed, dict) else {}
            if program == TOKEN_PROGRAM_ID and ix_type in {"initializeMint", "initializeMint2", "initializeMintCloseAuthority"}:
                if info.get("mint") == mint:
                    found_initialize_mint = True
            if program == SYSTEM_PROGRAM_ID and isinstance(info, dict):
                if info.get("newAccount") == mint:
                    found_create_account = True
            if program == METADATA_PROGRAM_ID and ix_type in {"createMetadataAccountV3", "createMetadataAccountV2", "createMetadataAccount"}:
                if info.get("mint") == mint:
                    metadata_authority = coalesce(info.get("mintAuthority"), blank="")
                    update_authority = coalesce(info.get("updateAuthority"), blank="")
        if mint not in account_pubkeys and not found_initialize_mint:
            return None
        creator_wallet = wallet if wallet in signers or wallet == fee_payer else ""
        status = "INCONCLUSIVE"
        reason = "Creation transaction found, but signer linkage to suspected wallet is incomplete."
        if found_initialize_mint and (wallet in signers or wallet == fee_payer):
            creator_wallet = wallet
            status = "CONFIRMED_CREATOR"
            reason = "Suspected wallet signed or paid for the earliest mint initialization transaction for this mint."
        elif mint_authority and mint_authority != wallet:
            creator_wallet = mint_authority
            status = "NOT_CONFIRMED"
            reason = f"Current mint authority is {mint_authority}, not suspected wallet, and earliest creation signer evidence does not tie to suspected wallet."
        elif not (found_initialize_mint or found_create_account):
            return None
        return {
            "creator_wallet": creator_wallet,
            "status": status,
            "reason": reason,
            "mint_authority": mint_authority,
            "freeze_authority": freeze_authority,
            "metadata_authority": metadata_authority,
            "update_authority": update_authority,
            "evidence_signatures": [tx_detail.get("transaction", {}).get("signatures", [""])[0]],
        }

    def apply_metadata(self, record: CreatedTokenRecord, metadata: Mapping[str, Any]) -> None:
        token_info = metadata.get("onChainMetadata") or {}
        metadata_data = token_info.get("metadata", {}) or {}
        record.name = metadata_data.get("name", "") or metadata.get("name", "") or ""
        record.symbol = metadata_data.get("symbol", "") or metadata.get("symbol", "") or ""
        record.update_authority = record.update_authority or token_info.get("updateAuthority", "") or ""
        record.metadata_authority = record.metadata_authority or token_info.get("updateAuthority", "") or ""

    def build_token_dossier(self, token: CreatedTokenRecord) -> Dict[str, Any]:
        mint_txs = self.api.helius_get_all_transactions(token.mint, max_pages=25)
        (self.raw_evidence_dir / f"mint_{token.mint}_helius_transactions.json").write_text(json.dumps(mint_txs, indent=2), encoding="utf-8")
        creation_ts = safe_int(token.creation_time_unix)
        launch_window_end = creation_ts + (self.config.launch_window_minutes * 60) if creation_ts else 0
        first_liquidity = self.find_first_liquidity_event(mint_txs)
        first_trade = self.find_first_trade_event(mint_txs)
        creator_flows = self.extract_creator_flows(token, mint_txs, creation_ts, launch_window_end)
        early_buyers = self.extract_early_buyers(token, mint_txs)
        funding_edges = self.build_funding_graph(token, early_buyers, creation_ts, launch_window_end)
        receive_map = self.detect_creator_token_transfers(token, mint_txs, {row.wallet for row in early_buyers})
        apply_funding_to_early_buyers(early_buyers, funding_edges, receive_map)

        creator_buy = any(row.event_type == "BUY" for row in creator_flows)
        creator_sell_rows = [row for row in creator_flows if row.event_type == "SELL"]
        creator_first_sell_ts = min((safe_int(row.block_time_unix) for row in creator_sell_rows if row.block_time_unix), default=0)
        creator_first_sell_iso = utc_iso(creator_first_sell_ts) if creator_first_sell_ts else ""
        creator_cumulative_sold = sum_decimal_strings([row.token_amount for row in creator_sell_rows])
        direct_flag = any(row.funded_by_creator_directly == "TRUE" for row in early_buyers)
        indirect_flag = any(row.funded_by_creator_indirectly == "TRUE" for row in early_buyers)
        receive_flag = any(row.received_token_from_creator_or_linked == "TRUE" for row in early_buyers)
        linked_count = sum(
            1
            for row in early_buyers
            if row.funded_by_creator_directly == "TRUE"
            or row.funded_by_creator_indirectly == "TRUE"
            or row.received_token_from_creator_or_linked == "TRUE"
        )
        creator_retained_supply = self.compute_creator_retained_supply(token)
        first_trade_ts = safe_int(first_trade.get("timestamp")) if first_trade else 0
        summary = TokenSummary(
            mint=token.mint,
            symbol=token.symbol,
            name=token.name,
            creator_wallet=token.creator_wallet or self.config.wallet,
            creator_confirmed="TRUE" if token.creator_status == "CONFIRMED_CREATOR" else "FALSE",
            creation_signature=token.creation_signature,
            creation_time_iso=token.creation_time_iso,
            creation_time_unix=token.creation_time_unix,
            platform=token.platform,
            first_liquidity_signature=first_liquidity.get("signature", "") if first_liquidity else "",
            first_liquidity_time_iso=utc_iso(first_liquidity.get("timestamp")) if first_liquidity else "",
            first_liquidity_time_unix=str(first_liquidity.get("timestamp", "")) if first_liquidity else "",
            creation_to_first_liquidity_seconds=str((safe_int(first_liquidity.get("timestamp")) - creation_ts) if first_liquidity and creation_ts else ""),
            first_meaningful_trade_signature=first_trade.get("signature", "") if first_trade else "",
            first_meaningful_trade_time_iso=utc_iso(first_trade_ts) if first_trade_ts else "",
            creator_bought_own_token="TRUE" if creator_buy else "FALSE",
            creator_sold_own_token="TRUE" if creator_sell_rows else "FALSE",
            creator_first_sell_time_iso=creator_first_sell_iso,
            creator_first_sell_time_unix=str(creator_first_sell_ts or ""),
            creator_cumulative_sold_amount=creator_cumulative_sold,
            creator_directly_funded_early_buyer="TRUE" if direct_flag else "FALSE",
            creator_indirectly_funded_early_buyer="TRUE" if indirect_flag else "FALSE",
            early_buyer_received_tokens_from_creator="TRUE" if receive_flag else "FALSE",
            multiple_creator_linked_early_buyers="TRUE" if linked_count >= 2 else "FALSE",
            creator_exited_early="TRUE" if creator_first_sell_ts and first_trade_ts and creator_first_sell_ts <= first_trade_ts + (self.config.launch_window_minutes * 60) else "FALSE",
            creator_retained_supply=creator_retained_supply,
            insufficient_data="TRUE" if not mint_txs else "FALSE",
            creator_trade_evidence_signatures="|".join(sorted({row.signature for row in creator_flows if row.signature})),
            funding_evidence_signatures="|".join(sorted({edge.signature for edge in funding_edges if edge.signature})),
            notes=token.creator_reason,
        )
        return {"summary": summary, "early_buyers": early_buyers, "funding_edges": funding_edges, "creator_flows": creator_flows}

    def find_first_liquidity_event(self, txs: Sequence[dict[str, Any]]) -> Optional[dict[str, Any]]:
        for tx in txs:
            source = str(tx.get("source", "")).upper()
            tx_type = str(tx.get("type", "")).upper()
            if "LIQUIDITY" in tx_type or tx_type in {"ADD_LIQUIDITY", "CREATE_POOL", "INITIALIZE_POOL"}:
                return tx
            if source in {"RAYDIUM", "PUMP_FUN", "ORCA"} and tx_type in {"SWAP", "UNKNOWN", "CREATE_POOL"}:
                return tx
        return None

    def find_first_trade_event(self, txs: Sequence[dict[str, Any]]) -> Optional[dict[str, Any]]:
        for tx in txs:
            if str(tx.get("type", "")).upper() == "SWAP":
                return tx
        return None

    def extract_creator_flows(
        self,
        token: CreatedTokenRecord,
        txs: Sequence[dict[str, Any]],
        creation_ts: int,
        launch_window_end: int,
    ) -> List[CreatorFlowRow]:
        rows: List[CreatorFlowRow] = []
        creator_wallet = token.creator_wallet or self.config.wallet
        for tx in txs:
            ts = safe_int(tx.get("timestamp"))
            if creation_ts and ts and ts < creation_ts - 300:
                continue
            if launch_window_end and ts and ts > launch_window_end + 86400:
                continue
            sig = tx.get("signature", "")
            slot = str(tx.get("slot", ""))
            native_change = extract_wallet_native_change(tx, creator_wallet)
            for transfer in tx.get("tokenTransfers", []) or []:
                from_user = transfer.get("fromUserAccount", "")
                to_user = transfer.get("toUserAccount", "")
                mint = transfer.get("mint", "")
                amount = stringify_amount(transfer)
                if mint != token.mint:
                    continue
                if to_user == creator_wallet:
                    rows.append(CreatorFlowRow("BUY", sig, slot, utc_iso(ts), str(ts or ""), creator_wallet, amount, mint, str(native_change), lamports_to_sol_string(native_change), from_user, "token transfer into creator wallet"))
                elif from_user == creator_wallet:
                    event_type = "SELL" if str(tx.get("type", "")).upper() == "SWAP" else "TRANSFER_OUT"
                    rows.append(CreatorFlowRow(event_type, sig, slot, utc_iso(ts), str(ts or ""), creator_wallet, amount, mint, str(native_change), lamports_to_sol_string(native_change), to_user, "token transfer out of creator wallet"))
        return sorted(rows, key=lambda row: (safe_int(row.block_time_unix), row.signature))

    def extract_early_buyers(self, token: CreatedTokenRecord, txs: Sequence[dict[str, Any]]) -> List[EarlyBuyerRow]:
        buyers: Dict[str, EarlyBuyerRow] = {}
        for tx in txs:
            tx_type = str(tx.get("type", "")).upper()
            if tx_type not in {"SWAP", "TRANSFER", "UNKNOWN"}:
                continue
            sig = tx.get("signature", "")
            ts = safe_int(tx.get("timestamp"))
            slot = str(tx.get("slot", ""))
            for transfer in tx.get("tokenTransfers", []) or []:
                if transfer.get("mint") != token.mint:
                    continue
                buyer = transfer.get("toUserAccount", "")
                if not buyer or buyer == (token.creator_wallet or self.config.wallet):
                    continue
                amount = stringify_amount(transfer)
                if buyer not in buyers:
                    buyers[buyer] = EarlyBuyerRow(
                        wallet=buyer,
                        first_buy_time_iso=utc_iso(ts),
                        first_buy_time_unix=str(ts or ""),
                        amount=amount,
                        slot=slot,
                        signature=sig,
                        funded_by_creator_directly="FALSE",
                        funded_by_creator_indirectly="FALSE",
                        indirect_path="",
                        received_token_from_creator_or_linked="FALSE",
                        received_token_signature="",
                        funding_evidence_signatures="",
                    )
                if len(buyers) >= self.config.early_buyers_limit:
                    break
            if len(buyers) >= self.config.early_buyers_limit:
                break
        return list(buyers.values())

    def build_funding_graph(
        self,
        token: CreatedTokenRecord,
        early_buyers: Sequence[EarlyBuyerRow],
        creation_ts: int,
        launch_window_end: int,
    ) -> List[FundingEdge]:
        relevant_wallets = {token.creator_wallet or self.config.wallet} | {row.wallet for row in early_buyers}
        adjacency: Dict[str, List[FundingEdge]] = defaultdict(list)
        wallets_to_scan = set(relevant_wallets)
        for row in early_buyers:
            funding_rows = self.api.helius_get_all_transactions(row.wallet, max_pages=10)
            (self.raw_evidence_dir / f"wallet_{row.wallet}_helius_transactions.json").write_text(json.dumps(funding_rows, indent=2), encoding="utf-8")
            for tx in funding_rows:
                ts = safe_int(tx.get("timestamp"))
                if creation_ts and ts and ts < creation_ts - 3600:
                    continue
                if launch_window_end and ts and ts > launch_window_end:
                    continue
                sig = tx.get("signature", "")
                slot = str(tx.get("slot", ""))
                for nt in tx.get("nativeTransfers", []) or []:
                    src = nt.get("fromUserAccount", "")
                    dst = nt.get("toUserAccount", "")
                    amount = safe_int(nt.get("amount"))
                    if not src or not dst or amount <= 0:
                        continue
                    if src in relevant_wallets or dst in relevant_wallets or dst == row.wallet or src == row.wallet:
                        edge = FundingEdge(src, dst, amount, lamports_to_sol_string(amount), sig, slot, utc_iso(ts), str(ts or ""), 1, token.creator_wallet or self.config.wallet)
                        adjacency[src].append(edge)
                        wallets_to_scan.add(src)
                        wallets_to_scan.add(dst)
        creator = token.creator_wallet or self.config.wallet
        target_wallets = {row.wallet for row in early_buyers}
        discovered: List[FundingEdge] = []
        queue: deque[Tuple[str, List[FundingEdge]]]=deque([(creator, [])])
        visited_depth: Dict[str, int] = {creator: 0}
        while queue:
            current, path = queue.popleft()
            current_depth = len(path)
            if current_depth >= self.config.funding_depth:
                continue
            for edge in adjacency.get(current, []):
                next_path = path + [edge]
                hop_depth = len(next_path)
                end_wallet = edge.dst_wallet
                if end_wallet in target_wallets:
                    for hop in next_path:
                        discovered.append(FundingEdge(hop.src_wallet, hop.dst_wallet, hop.amount_lamports, hop.amount_sol, hop.signature, hop.slot, hop.block_time_iso, hop.block_time_unix, hop_depth, creator))
                if visited_depth.get(end_wallet, 99) > hop_depth:
                    visited_depth[end_wallet] = hop_depth
                    queue.append((end_wallet, next_path))
        unique = {(e.src_wallet, e.dst_wallet, e.signature, e.amount_lamports, e.hop_depth): e for e in discovered}
        return sorted(unique.values(), key=lambda edge: (safe_int(edge.block_time_unix), edge.signature, edge.dst_wallet))

    def detect_creator_token_transfers(self, token: CreatedTokenRecord, txs: Sequence[dict[str, Any]], early_wallets: set[str]) -> Dict[str, Tuple[str, str]]:
        creator = token.creator_wallet or self.config.wallet
        linked_wallets = {edge.dst_wallet for edge in self.build_funding_graph(token, [], 0, 0)}
        receive_map: Dict[str, Tuple[str, str]] = {}
        for tx in txs:
            sig = tx.get("signature", "")
            for transfer in tx.get("tokenTransfers", []) or []:
                if transfer.get("mint") != token.mint:
                    continue
                src = transfer.get("fromUserAccount", "")
                dst = transfer.get("toUserAccount", "")
                if dst in early_wallets and (src == creator or src in linked_wallets):
                    receive_map[dst] = (sig, src)
        return receive_map

    def compute_creator_retained_supply(self, token: CreatedTokenRecord) -> str:
        largest = self.api.get_token_largest_accounts(token.mint)
        addresses = [row.get("address") for row in largest if row.get("address")]
        owners = self.api.get_multiple_accounts(addresses) if addresses else []
        creator = token.creator_wallet or self.config.wallet
        for largest_row, owner_account in zip(largest, owners):
            parsed = (((owner_account or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
            owner = parsed.get("owner", "")
            if owner == creator:
                ui_amount = ((largest_row.get("uiAmountString") or largest_row.get("amount") or ""))
                return str(ui_amount)
        return ""

    def write_dossier_files(self, dossier_dir: Path, mint: str, dossier: Mapping[str, Any]) -> List[str]:
        generated: List[str] = []
        summary_path = dossier_dir / f"{mint}.summary.tsv"
        self.write_tsv(summary_path, [asdict(dossier["summary"])])
        generated.append(str(summary_path.relative_to(self.config.outdir)))
        early_path = dossier_dir / f"{mint}.early_buyers.tsv"
        self.write_tsv(early_path, [asdict(row) for row in dossier["early_buyers"]])
        generated.append(str(early_path.relative_to(self.config.outdir)))
        edges_path = dossier_dir / f"{mint}.funding_edges.tsv"
        self.write_tsv(edges_path, [asdict(row) for row in dossier["funding_edges"]])
        generated.append(str(edges_path.relative_to(self.config.outdir)))
        flows_path = dossier_dir / f"{mint}.creator_flows.tsv"
        self.write_tsv(flows_path, [asdict(row) for row in dossier["creator_flows"]])
        generated.append(str(flows_path.relative_to(self.config.outdir)))
        return generated

    def build_aggregate_summary(self, summaries: Sequence[TokenSummary], wallets: Counter[str], intermediaries: Counter[str]) -> Dict[str, Any]:
        sell_deltas = [safe_int(item.creator_first_sell_time_unix) - safe_int(item.creation_time_unix) for item in summaries if item.creator_first_sell_time_unix and item.creation_time_unix]
        liquidity_deltas = [safe_int(item.creation_to_first_liquidity_seconds) for item in summaries if item.creation_to_first_liquidity_seconds]
        return {
            "target_wallet": self.config.wallet,
            "total_created_tokens_found": len(summaries),
            "tokens_where_creator_also_traded": sum(1 for item in summaries if item.creator_bought_own_token == "TRUE" or item.creator_sold_own_token == "TRUE"),
            "tokens_where_creator_sold": sum(1 for item in summaries if item.creator_sold_own_token == "TRUE"),
            "tokens_with_creator_directly_funded_early_buyers": sum(1 for item in summaries if item.creator_directly_funded_early_buyer == "TRUE"),
            "tokens_with_creator_indirectly_funded_early_buyers": sum(1 for item in summaries if item.creator_indirectly_funded_early_buyer == "TRUE"),
            "tokens_with_creator_linked_wallets_among_early_buyers": sum(1 for item in summaries if item.multiple_creator_linked_early_buyers == "TRUE" or item.early_buyer_received_tokens_from_creator == "TRUE"),
            "median_creation_to_creator_first_sell_seconds": str(int(statistics.median(sell_deltas))) if sell_deltas else "",
            "median_creation_to_first_liquidity_seconds": str(int(statistics.median(liquidity_deltas))) if liquidity_deltas else "",
            "top_repeated_linked_wallets": format_counter(wallets),
            "repeated_intermediaries": format_counter(intermediaries),
            "repeated_patterns": self.describe_repeated_patterns(summaries),
        }

    def describe_repeated_patterns(self, summaries: Sequence[TokenSummary]) -> str:
        total = len(summaries)
        if total == 0:
            return "No creator-linked token creations were confirmed from the available evidence."
        parts = [
            f"creator sold in {sum(1 for item in summaries if item.creator_sold_own_token == 'TRUE')}/{total} tokens",
            f"direct creator funding of early buyers observed in {sum(1 for item in summaries if item.creator_directly_funded_early_buyer == 'TRUE')}/{total} tokens",
            f"indirect creator funding of early buyers observed in {sum(1 for item in summaries if item.creator_indirectly_funded_early_buyer == 'TRUE')}/{total} tokens",
            f"creator-linked early buyer cluster observed in {sum(1 for item in summaries if item.multiple_creator_linked_early_buyers == 'TRUE')}/{total} tokens",
        ]
        return "; ".join(parts)

    def render_summary_txt(
        self,
        verification: Optional[Mapping[str, Any]],
        created_tokens: Sequence[CreatedTokenRecord],
        summaries: Sequence[TokenSummary],
        aggregate: Mapping[str, Any],
    ) -> str:
        lines = [
            "Creator Audit Summary",
            f"Run timestamp UTC: {utc_iso(int(time.time()))}",
            f"Target wallet: {self.config.wallet}",
            f"Seed mint: {self.config.seed_mint or ''}",
            "",
            "Confirmed / not confirmed:",
        ]
        if verification:
            lines.extend([
                f"- Seed mint status: {verification.get('status', '')}",
                f"- Basis: {verification.get('reason', '')}",
                f"- Creation signature: {verification.get('creation_signature', '')}",
                f"- Platform evidence: {verification.get('platform', '')}",
                f"- Authorities: mint={verification.get('mint_authority', '')}, freeze={verification.get('freeze_authority', '')}, update={verification.get('update_authority', '')}",
            ])
        else:
            lines.append("- No seed mint verification requested.")
        lines.extend([
            "",
            "Counts:",
            f"- Created tokens found: {len(created_tokens)}",
            f"- Tokens processed into dossiers: {len(summaries)}",
            f"- Tokens where creator sold: {aggregate.get('tokens_where_creator_sold', 0)}",
            f"- Tokens with direct creator funding of early buyers: {aggregate.get('tokens_with_creator_directly_funded_early_buyers', 0)}",
            f"- Tokens with indirect creator funding of early buyers: {aggregate.get('tokens_with_creator_indirectly_funded_early_buyers', 0)}",
            f"- Tokens with creator-linked early buyers: {aggregate.get('tokens_with_creator_linked_wallets_among_early_buyers', 0)}",
            "",
            "Repeated patterns observed:",
            f"- {aggregate.get('repeated_patterns', '')}",
            f"- Top repeated linked wallets: {aggregate.get('top_repeated_linked_wallets', '') or 'None found'}",
            f"- Repeated intermediaries: {aggregate.get('repeated_intermediaries', '') or 'None found'}",
            "",
            "Evidence limitations:",
            "- Results are limited to evidence returned by Helius and the configured RPC endpoint.",
            "- Creator attribution is only marked confirmed when the suspected wallet signs or pays for the earliest mint initialization transaction located for the mint.",
            "- Fields that could not be determined are left blank or NULL.",
            f"- API failure counters: helius={self.failures.helius_failures}, rpc={self.failures.rpc_failures}, metadata={self.failures.metadata_failures}, partial={self.failures.partial_failures}.",
        ])
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
        for signer in tx.get("signers", []) or []:
            if signer == wallet:
                return True
        return False

    def extract_mints_from_helius_tx(self, tx: Mapping[str, Any]) -> List[str]:
        mints = []
        for transfer in tx.get("tokenTransfers", []) or []:
            mint = transfer.get("mint")
            if is_probable_mint(mint):
                mints.append(mint)
        events = tx.get("events", {}) or {}
        for key in ("swap", "liquidity", "compressed"):
            value = events.get(key)
            if isinstance(value, dict):
                for inner_value in value.values():
                    if is_probable_mint(inner_value):
                        mints.append(inner_value)
        return list(dict.fromkeys(mints))

    def extract_created_mints_from_helius_tx(self, tx: Mapping[str, Any]) -> List[str]:
        candidates: List[str] = []
        for instruction in tx.get("instructions", []) or []:
            accounts = instruction.get("accounts") or []
            for account in accounts:
                if is_probable_mint(account):
                    candidates.append(account)
        description = str(tx.get("description", ""))
        for token in description.replace(",", " ").split():
            if is_probable_mint(token):
                candidates.append(token)
        return list(dict.fromkeys(candidates))


def flatten_instructions(instructions: Sequence[Any]) -> Iterable[dict[str, Any]]:
    for item in instructions:
        if not isinstance(item, dict):
            continue
        if "instructions" in item and isinstance(item["instructions"], list):
            for child in item["instructions"]:
                if isinstance(child, dict):
                    yield child
        else:
            yield item


def apply_funding_to_early_buyers(
    early_buyers: Sequence[EarlyBuyerRow],
    funding_edges: Sequence[FundingEdge],
    receive_map: Mapping[str, Tuple[str, str]],
) -> None:
    direct_by_wallet: Dict[str, List[FundingEdge]] = defaultdict(list)
    indirect_paths: Dict[str, List[FundingEdge]] = defaultdict(list)
    for edge in funding_edges:
        direct_by_wallet[edge.dst_wallet].append(edge)
        if edge.hop_depth > 1:
            indirect_paths[edge.dst_wallet].append(edge)
    for row in early_buyers:
        edges = direct_by_wallet.get(row.wallet, [])
        if any(edge.src_wallet == edge.path_root_creator and edge.dst_wallet == row.wallet for edge in edges):
            row.funded_by_creator_directly = "TRUE"
        if any(edge.hop_depth > 1 for edge in edges):
            row.funded_by_creator_indirectly = "TRUE"
            path_wallets = [edge.path_root_creator]
            path_wallets.extend(edge.dst_wallet for edge in sorted(edges, key=lambda item: item.hop_depth))
            row.indirect_path = ">".join(dict.fromkeys(path_wallets))
        row.funding_evidence_signatures = "|".join(sorted({edge.signature for edge in edges if edge.signature}))
        if row.wallet in receive_map:
            row.received_token_from_creator_or_linked = "TRUE"
            row.received_token_signature = receive_map[row.wallet][0]


def sanitize_tsv_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("\t", " ").replace("\n", " ").replace("\r", " ")


def utc_iso(ts: Optional[int]) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def safe_int(value: Any) -> int:
    try:
        if value in (None, "", NULL_VALUE):
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def coalesce(value: Any, blank: str = NULL_VALUE) -> str:
    return blank if value in (None, "") else str(value)


def lamports_to_sol_string(value: Any) -> str:
    try:
        return f"{int(value) / LAMPORTS_PER_SOL:.9f}"
    except (TypeError, ValueError):
        return ""


def stringify_amount(transfer: Mapping[str, Any]) -> str:
    token_amount = transfer.get("tokenAmount")
    if token_amount is not None:
        return str(token_amount)
    raw = transfer.get("rawTokenAmount") or {}
    if isinstance(raw, dict):
        return str(raw.get("tokenAmount") or raw.get("amount") or "")
    return ""


def sum_decimal_strings(values: Sequence[str]) -> str:
    total = 0.0
    for value in values:
        try:
            total += float(value)
        except (TypeError, ValueError):
            continue
    return f"{total:.9f}" if total else ""


def is_probable_mint(value: Any) -> bool:
    return isinstance(value, str) and 32 <= len(value) <= 44 and value not in {SYSTEM_PROGRAM_ID, TOKEN_PROGRAM_ID, ASSOCIATED_TOKEN_PROGRAM_ID, METADATA_PROGRAM_ID}


def detect_platform(mint: str, helius_tx: Mapping[str, Any], rpc_tx: Mapping[str, Any]) -> str:
    source = str(helius_tx.get("source", "") or "")
    if source:
        return source
    if mint.endswith("pump"):
        return "PUMP_FUN"
    message = ((rpc_tx.get("transaction") or {}).get("message") or {})
    for key in message.get("accountKeys", []):
        pubkey = key.get("pubkey") if isinstance(key, dict) else key
        if pubkey and "pump" in pubkey.lower():
            return "PUMP_FUN_RELATED_ACCOUNT"
    return ""


def extract_wallet_native_change(tx: Mapping[str, Any], wallet: str) -> int:
    for account_data in tx.get("accountData", []) or []:
        if account_data.get("account") == wallet:
            return safe_int(account_data.get("nativeBalanceChange"))
    return 0


def format_counter(counter: Counter[str], limit: int = 10) -> str:
    parts = [f"{wallet}:{count}" for wallet, count in counter.most_common(limit)]
    return "|".join(parts)


def parse_args(argv: Optional[Sequence[str]] = None) -> AuditConfig:
    parser = argparse.ArgumentParser(description="Forensic Solana creator auditor.")
    parser.add_argument("--wallet", required=True, help="Suspected creator wallet address.")
    parser.add_argument("--seed-mint", dest="seed_mint", help="Seed mint to verify against the wallet.")
    parser.add_argument("--outdir", required=True, help="Output directory.")
    parser.add_argument("--early-buyers-limit", type=int, default=50, help="Number of early buyer wallets to capture per token.")
    parser.add_argument("--funding-depth", type=int, default=2, help="Funding graph depth from creator wallet.")
    parser.add_argument("--launch-window-minutes", type=int, default=60, help="Launch window size in minutes.")
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
