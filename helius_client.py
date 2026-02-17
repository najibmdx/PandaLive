"""Helius HTTP API client for PANDA LIVE.

Polls Helius transaction API for swap events and parses them into
FlowEvent objects with correct SOL extraction from nativeBalanceChange.

CRITICAL: SOL amounts are extracted from nativeBalanceChange (lamports).
- 1 SOL = 1,000,000,000 lamports
- Negative nativeBalanceChange = SOL out = BUY
- Positive nativeBalanceChange = SOL in = SELL
"""

import time
from typing import List, Optional

import requests

from ..models.events import FlowEvent

LAMPORTS_PER_SOL = 1_000_000_000
HELIUS_ENDPOINT = "https://api.helius.xyz/v0/addresses/{mint}/transactions"
DEFAULT_POLL_INTERVAL = 5.0
DEFAULT_TIMEOUT = 30


class HeliusClient:
    """Polls Helius HTTP API for swap transactions."""

    def __init__(
        self,
        api_key: str,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        if not api_key:
            raise ValueError(
                "HELIUS_API_KEY is required. "
                "Set it as an environment variable: export HELIUS_API_KEY='your-key'"
            )
        self.api_key = api_key
        self.poll_interval = poll_interval
        self.timeout = timeout
        self._last_poll_timestamp: Optional[int] = None
        self._estimated_liquidity_sol: Optional[float] = None

    def get_estimated_liquidity(self) -> float:
        """Get estimated token liquidity (computed from first batch of swaps)."""
        if self._estimated_liquidity_sol is None:
            return 50.0  # Default fallback
        return self._estimated_liquidity_sol

    def fetch_transactions(self, mint_address: str) -> List[dict]:
        """Fetch recent swap transactions from Helius.

        Args:
            mint_address: Token mint address (44-char Solana address).

        Returns:
            List of raw transaction dicts from Helius API.
            Returns empty list on error (does not crash).
        """
        url = HELIUS_ENDPOINT.format(mint=mint_address)
        params = {
            "api-key": self.api_key,
            "type": "SWAP",
            "limit": 100,  # Always get latest 100 for live monitoring
        }

        # Don't use pagination - always get latest transactions
        # Deduplication happens client-side via signatures in live_processor

        try:
            resp = requests.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            transactions = resp.json()

            return transactions if isinstance(transactions, list) else []

        except requests.exceptions.Timeout:
            return []
        except requests.exceptions.RequestException:
            return []
        except (ValueError, KeyError):
            return []

    def parse_transaction(self, tx: dict, mint_address: str) -> Optional[FlowEvent]:
        """Parse a single Helius transaction into a FlowEvent.

        CRITICAL: Extracts SOL from nativeBalanceChange, NOT token amounts.
        - nativeBalanceChange is in lamports (1 SOL = 1e9 lamports)
        - Negative = SOL spent = BUY
        - Positive = SOL received = SELL

        Args:
            tx: Raw transaction dict from Helius.
            mint_address: Token mint address for context.

        Returns:
            FlowEvent if parseable, None if transaction should be skipped.
        """
        try:
            signature = tx.get("signature", "")
            timestamp = tx.get("timestamp", 0)
            tx_type = tx.get("type", "")

            if tx_type != "SWAP":
                return None

            if not signature or not timestamp:
                return None

            # Find the fee payer / initiator account
            account_data = tx.get("accountData", [])
            if not account_data:
                # Try alternative field names
                account_data = tx.get("nativeTransfers", [])

            # Find the wallet that initiated the swap
            # The fee payer is typically the first account with nativeBalanceChange
            fee_payer = tx.get("feePayer", "")
            if not fee_payer or len(fee_payer) != 44:
                return None

            # Extract SOL change for the fee payer
            native_change_lamports = 0
            for acct in account_data:
                if acct.get("account") == fee_payer:
                    native_change_lamports = acct.get("nativeBalanceChange", 0)
                    break

            if native_change_lamports == 0:
                # Try feePayer's native balance from nativeTransfers
                native_transfers = tx.get("nativeTransfers", [])
                for nt in native_transfers:
                    if nt.get("fromUserAccount") == fee_payer:
                        native_change_lamports = -abs(nt.get("amount", 0))
                        break
                    elif nt.get("toUserAccount") == fee_payer:
                        native_change_lamports = abs(nt.get("amount", 0))
                        break

            if native_change_lamports == 0:
                return None

            # Convert lamports to SOL
            sol_amount = abs(native_change_lamports) / LAMPORTS_PER_SOL

            if sol_amount <= 0:
                return None

            # Determine direction from sign
            # Negative balance change = SOL went out = bought tokens = BUY
            # Positive balance change = SOL came in = sold tokens = SELL
            direction = "buy" if native_change_lamports < 0 else "sell"

            return FlowEvent(
                wallet=fee_payer,
                timestamp=timestamp,
                direction=direction,
                amount_sol=sol_amount,
                signature=signature,
                token_ca=mint_address,
            )

        except (KeyError, TypeError, ValueError):
            return None

    def poll_and_parse(self, mint_address: str) -> List[FlowEvent]:
        """Fetch and parse transactions in one call.

        Args:
            mint_address: Token mint address.

        Returns:
            List of parsed FlowEvent objects (may be empty).
        """
        transactions = self.fetch_transactions(mint_address)
        
        # ESTIMATE LIQUIDITY on first poll (cold start)
        if self._estimated_liquidity_sol is None and transactions:
            from ..config.dynamic_thresholds import estimate_liquidity_from_swaps
            self._estimated_liquidity_sol = estimate_liquidity_from_swaps(transactions)
            print(f"[PANDA] Estimated token liquidity: {self._estimated_liquidity_sol:.1f} SOL", flush=True)
        
        events: List[FlowEvent] = []

        for tx in transactions:
            flow = self.parse_transaction(tx, mint_address)
            if flow is not None:
                events.append(flow)

        return events

    def reset_pagination(self) -> None:
        """Reset pagination state (start from latest transactions)."""
        self._last_poll_timestamp = None
