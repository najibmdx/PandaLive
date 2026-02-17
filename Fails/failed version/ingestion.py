"""
M2: Live On-Chain Ingestion
M3: Canonical Event Normalizer

Subscribes to Solana chain data and normalizes to canonical events.
"""

import time
import requests
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from event_log import CanonicalEvent


class SolanaIngestion:
    """Live on-chain ingestion from Solana via Helius API."""
    
    def __init__(self, helius_api_key: str, mint: str):
        self.helius_api_key = helius_api_key
        self.mint = mint
        # Helius RPC endpoint - includes api-key in URL
        self.rpc_url = f"https://mainnet.helius-rpc.com/?api-key={helius_api_key}"
        self.headers = {"Content-Type": "application/json"}
    
    def fetch_signatures(
        self,
        address: str,
        before: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Fetch transaction signatures for an address.
        
        Args:
            address: Solana address (mint or wallet)
            before: Signature to fetch before (for pagination)
            limit: Max signatures to fetch
        
        Returns:
            List of signature info dicts
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [
                address,
                {
                    "limit": limit,
                    **({"before": before} if before else {})
                }
            ]
        }
        
        try:
            response = requests.post(
                self.rpc_url,
                json=payload,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            
            if "result" in result:
                return result["result"]
            else:
                return []
        
        except Exception as e:
            print(f"Error fetching signatures: {e}")
            return []
    
    def fetch_transaction(self, signature: str) -> Optional[Dict[str, Any]]:
        """
        Fetch transaction details.
        
        Args:
            signature: Transaction signature
        
        Returns:
            Transaction data or None
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0
                }
            ]
        }
        
        try:
            response = requests.post(
                self.rpc_url,
                json=payload,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            
            if "result" in result and result["result"]:
                return result["result"]
            else:
                return None
        
        except Exception as e:
            print(f"Error fetching transaction {signature[:8]}...: {e}")
            return None
    
    def poll_new_transactions(
        self,
        cursor_slot: int,
        cursor_signature: Optional[str]
    ) -> Tuple[List[Dict[str, Any]], int, Optional[str]]:
        """
        Poll for new transactions since cursor.
        
        Args:
            cursor_slot: Last processed slot
            cursor_signature: Last processed signature
        
        Returns:
            (transactions, new_slot, new_signature)
        """
        # Fetch recent signatures
        sigs = self.fetch_signatures(self.mint, before=None, limit=50)
        
        if not sigs:
            return [], cursor_slot, cursor_signature
        
        # Filter to only new signatures
        new_txs = []
        new_slot = cursor_slot
        new_sig = cursor_signature
        
        for sig_info in reversed(sigs):  # Process oldest first
            sig = sig_info["signature"]
            slot = sig_info["slot"]
            
            # Skip if we've already processed this
            if slot < cursor_slot:
                continue
            if slot == cursor_slot and cursor_signature and sig == cursor_signature:
                continue
            
            # Fetch full transaction
            tx = self.fetch_transaction(sig)
            if tx:
                new_txs.append(tx)
                new_slot = max(new_slot, slot)
                new_sig = sig
        
        return new_txs, new_slot, new_sig


class CanonicalEventNormalizer:
    """
    Normalizes raw Solana transactions to canonical events.
    
    Removes vendor/RPC shape differences and produces only what v4 logic needs.
    """
    
    def __init__(self, session_id: str, mint: str):
        self.session_id = session_id
        self.mint = mint
    
    def normalize_transaction(self, tx: Dict[str, Any]) -> List[CanonicalEvent]:
        """
        Convert raw Solana transaction to canonical events.
        
        Args:
            tx: Raw transaction from RPC
        
        Returns:
            List of canonical events (may be multiple per transaction)
        """
        events = []
        
        # Extract base fields
        signature = tx.get("transaction", {}).get("signatures", [""])[0]
        slot = tx.get("slot", 0)
        block_time = tx.get("blockTime", 0)
        
        # Extract instructions
        meta = tx.get("meta", {})
        message = tx.get("transaction", {}).get("message", {})
        instructions = message.get("instructions", [])
        
        # Extract account keys
        account_keys = []
        if isinstance(message.get("accountKeys"), list):
            for key in message.get("accountKeys", []):
                if isinstance(key, str):
                    account_keys.append(key)
                elif isinstance(key, dict):
                    account_keys.append(key.get("pubkey", ""))
        
        # Process each instruction
        for idx, instruction in enumerate(instructions):
            event = self._normalize_instruction(
                instruction,
                signature,
                slot,
                block_time,
                account_keys,
                meta,
                idx
            )
            
            if event:
                events.append(event)
        
        # If no events extracted, create a generic transaction event
        if not events and signature:
            events.append(
                CanonicalEvent(
                    session_id=self.session_id,
                    mint=self.mint,
                    slot=slot,
                    block_time=block_time,
                    signature=signature,
                    event_type="OTHER",
                    actors=account_keys[:5] if account_keys else [],
                    raw_ref=signature
                )
            )
        
        return events
    
    def _normalize_instruction(
        self,
        instruction: Dict[str, Any],
        signature: str,
        slot: int,
        block_time: int,
        account_keys: List[str],
        meta: Dict[str, Any],
        idx: int
    ) -> Optional[CanonicalEvent]:
        """Normalize a single instruction to canonical event."""
        
        # Extract program ID
        program_id = ""
        if "programId" in instruction:
            if isinstance(instruction["programId"], str):
                program_id = instruction["programId"]
            elif isinstance(instruction["programId"], dict):
                program_id = instruction["programId"].get("pubkey", "")
        
        # Extract parsed info if available
        parsed = instruction.get("parsed", {})
        
        # Determine event type and extract relevant data
        event_type = "OTHER"
        actors = []
        token_mint = ""
        amounts = {}
        dex = ""
        
        if isinstance(parsed, dict):
            info = parsed.get("info", {})
            instr_type = parsed.get("type", "")
            
            # Token transfer
            if instr_type in ["transfer", "transferChecked"]:
                event_type = "TOKEN_TRANSFER"
                actors = [
                    info.get("source", ""),
                    info.get("destination", ""),
                    info.get("authority", "")
                ]
                token_mint = info.get("mint", "")
                amounts = {"amount": str(info.get("amount", 0))}
            
            # Generic transfer
            elif instr_type == "transfer":
                event_type = "SOL_TRANSFER"
                actors = [
                    info.get("source", ""),
                    info.get("destination", "")
                ]
                amounts = {"lamports": str(info.get("lamports", 0))}
        
        # Detect swaps (heuristic: look for known DEX programs)
        known_dex_programs = {
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "raydium",
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "orca",
            "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP": "orca_v2",
        }
        
        if program_id in known_dex_programs:
            event_type = "SWAP"
            dex = known_dex_programs[program_id]
        
        # Filter actors
        actors = [a for a in actors if a]
        
        # Only create event if relevant to primitives
        if event_type != "OTHER" or program_id:
            return CanonicalEvent(
                session_id=self.session_id,
                mint=self.mint,
                slot=slot,
                block_time=block_time,
                signature=signature,
                event_type=event_type,
                actors=actors,
                program_id=program_id,
                dex=dex,
                token_mint=token_mint,
                amounts=amounts,
                raw_ref=f"{signature}:{idx}"
            )
        
        return None


def selftest_normalizer():
    """Self-test for normalizer."""
    
    # Mock transaction
    mock_tx = {
        "slot": 12345,
        "blockTime": 1640000000,
        "transaction": {
            "signatures": ["5j7s6NiJS3JAkvgkoc18WVAsiSaci2pxB2A6ueCJP4tprA2TFg9wSyTLeYouxPBJEMzJinENTkpA52YStRW5Dia7"],
            "message": {
                "accountKeys": ["wallet1", "wallet2"],
                "instructions": [
                    {
                        "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                        "parsed": {
                            "type": "transfer",
                            "info": {
                                "source": "wallet1",
                                "destination": "wallet2",
                                "authority": "wallet1",
                                "amount": 1000
                            }
                        }
                    }
                ]
            }
        },
        "meta": {}
    }
    
    normalizer = CanonicalEventNormalizer(
        session_id="test",
        mint="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr"
    )
    
    events = normalizer.normalize_transaction(mock_tx)
    
    assert len(events) >= 1
    assert events[0].slot == 12345
    assert events[0].signature.startswith("5j7s6NiJS3J")
    
    print("✓ CanonicalEventNormalizer selftest PASSED")


if __name__ == "__main__":
    selftest_normalizer()
