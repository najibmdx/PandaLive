"""
PANDA LIVE Flow Ingestion

Normalizes and validates incoming flow events.
"""

from panda_live.models.events import FlowEvent
from typing import Optional


class FlowIngestion:
    """
    Handles normalization and validation of incoming flow events.
    """
    
    @staticmethod
    def normalize_flow(
        wallet: str,
        timestamp: int,
        direction: str,
        amount_sol: float,
        signature: str,
        token_ca: str
    ) -> Optional[FlowEvent]:
        """
        Normalize and validate a flow event.
        
        Args:
            wallet: Wallet address
            timestamp: Unix epoch seconds
            direction: "buy" or "sell" (case-insensitive)
            amount_sol: Flow amount in SOL (can be negative, will be abs'd)
            signature: Transaction signature
            token_ca: Token contract address (mint)
        
        Returns:
            FlowEvent if valid, None if invalid
        """
        # Validate required fields
        if not wallet or not signature or not token_ca:
            return None
        
        # Validate timestamp
        if timestamp <= 0:
            return None
        
        # Normalize direction
        direction = direction.lower().strip()
        if direction not in ["buy", "sell"]:
            return None
        
        # Normalize amount (always positive)
        amount_sol = abs(float(amount_sol))
        if amount_sol <= 0:
            return None
        
        try:
            return FlowEvent(
                wallet=wallet,
                timestamp=timestamp,
                direction=direction,
                amount_sol=amount_sol,
                signature=signature,
                token_ca=token_ca
            )
        except Exception as e:
            # Invalid data, return None
            return None
    
    @staticmethod
    def validate_flow(flow: FlowEvent) -> bool:
        """
        Additional validation for a flow event.
        
        Args:
            flow: FlowEvent to validate
        
        Returns:
            True if valid, False otherwise
        """
        # Check all required fields are present
        if not all([flow.wallet, flow.signature, flow.token_ca]):
            return False
        
        # Check timestamp is reasonable (after 2020, before 2100)
        if flow.timestamp < 1577836800 or flow.timestamp > 4102444800:
            return False
        
        # Check amount is positive
        if flow.amount_sol <= 0:
            return False
        
        # Check direction is valid
        if flow.direction not in ["buy", "sell"]:
            return False
        
        return True
