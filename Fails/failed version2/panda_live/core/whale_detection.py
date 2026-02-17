"""
PANDA LIVE Whale Detection

Detects whale threshold crossings with latched emission.
"""

from typing import Optional, List
from panda_live.models.token_state import TokenState
from panda_live.models.events import FlowEvent, WhaleEvent
from panda_live.config.thresholds import (
    WHALE_SINGLE_TX_SOL,
    WHALE_CUM_5MIN_SOL,
    WHALE_CUM_15MIN_SOL
)


class WhaleDetector:
    """
    Detects whale threshold crossings with latched emission.
    
    Latched emission = event emitted ONLY on FIRST threshold crossing.
    """
    
    def __init__(self, token_state: TokenState):
        self.token_state = token_state
    
    def detect_whales(self, flow: FlowEvent) -> List[WhaleEvent]:
        """
        Detect whale threshold crossings for a flow event.
        
        Args:
            flow: FlowEvent to check
        
        Returns:
            List of WhaleEvents (may be empty, or contain multiple events)
        """
        events = []
        
        # Get wallet state
        wallet = self.token_state.get_or_create_wallet(flow.wallet)
        
        # Check single TX threshold
        whale_tx = self._check_single_tx(flow, wallet)
        if whale_tx:
            events.append(whale_tx)
        
        # Check 5-minute cumulative threshold
        whale_5m = self._check_cumulative_5min(flow, wallet)
        if whale_5m:
            events.append(whale_5m)
        
        # Check 15-minute cumulative threshold
        whale_15m = self._check_cumulative_15min(flow, wallet)
        if whale_15m:
            events.append(whale_15m)
        
        return events
    
    def _check_single_tx(self, flow: FlowEvent, wallet) -> Optional[WhaleEvent]:
        """
        Check single transaction threshold.
        
        Latched: Only emit on FIRST crossing.
        """
        # Check if already triggered
        if wallet.whale_tx_triggered:
            return None
        
        # Check threshold
        if flow.amount_sol >= WHALE_SINGLE_TX_SOL:
            # Latch the trigger
            wallet.whale_tx_triggered = True
            
            # Create whale event
            return WhaleEvent(
                wallet=flow.wallet,
                timestamp=flow.timestamp,
                event_type="WHALE_TX",
                amount_sol=flow.amount_sol,
                threshold=WHALE_SINGLE_TX_SOL,
                signature=flow.signature,
                token_ca=flow.token_ca
            )
        
        return None
    
    def _check_cumulative_5min(self, flow: FlowEvent, wallet) -> Optional[WhaleEvent]:
        """
        Check 5-minute cumulative threshold.
        
        Latched: Only emit on FIRST crossing.
        """
        # Check if already triggered
        if wallet.whale_5m_triggered:
            return None
        
        # Check threshold
        if wallet.cumulative_5min >= WHALE_CUM_5MIN_SOL:
            # Latch the trigger
            wallet.whale_5m_triggered = True
            
            # Create whale event
            return WhaleEvent(
                wallet=flow.wallet,
                timestamp=flow.timestamp,
                event_type="WHALE_CUM_5M",
                amount_sol=wallet.cumulative_5min,
                threshold=WHALE_CUM_5MIN_SOL,
                signature=flow.signature,
                token_ca=flow.token_ca
            )
        
        return None
    
    def _check_cumulative_15min(self, flow: FlowEvent, wallet) -> Optional[WhaleEvent]:
        """
        Check 15-minute cumulative threshold.
        
        Latched: Only emit on FIRST crossing.
        """
        # Check if already triggered
        if wallet.whale_15m_triggered:
            return None
        
        # Check threshold
        if wallet.cumulative_15min >= WHALE_CUM_15MIN_SOL:
            # Latch the trigger
            wallet.whale_15m_triggered = True
            
            # Create whale event
            return WhaleEvent(
                wallet=flow.wallet,
                timestamp=flow.timestamp,
                event_type="WHALE_CUM_15M",
                amount_sol=wallet.cumulative_15min,
                threshold=WHALE_CUM_15MIN_SOL,
                signature=flow.signature,
                token_ca=flow.token_ca
            )
        
        return None
