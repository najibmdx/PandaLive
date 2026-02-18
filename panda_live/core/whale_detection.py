"""Whale threshold detection with latched emission for PANDA LIVE."""

from typing import List

from ..config.thresholds import WHALE_SINGLE_TX_SOL, WHALE_CUM_5MIN_SOL, WHALE_CUM_15MIN_SOL
from ..models.events import FlowEvent, WhaleEvent
from ..models.wallet_state import WalletState


class WhaleDetector:
    """Detects whale threshold crossings with latched emission.

    Each threshold fires exactly ONCE per wallet. Once a flag is set,
    subsequent crossings of the same threshold are suppressed.
    """
    
    def __init__(self, thresholds=None) -> None:
        """Initialize with dynamic thresholds.
        
        Args:
            thresholds: DynamicThresholds object. If None, uses default (50 SOL liquidity).
        """
        if thresholds is None:
            from ..config.dynamic_thresholds import calculate_thresholds, DEFAULT_LIQUIDITY_SOL
            thresholds = calculate_thresholds(DEFAULT_LIQUIDITY_SOL)
        
        # Use dynamic thresholds
        self.thresholds = thresholds
        self.whale_single_tx_sol = thresholds.whale_single_tx_sol
        self.whale_cum_5min_sol = thresholds.whale_cum_5min_sol
        self.whale_cum_15min_sol = thresholds.whale_cum_15min_sol

    def check_thresholds(
        self, wallet_state: WalletState, flow: FlowEvent
    ) -> List[WhaleEvent]:
        """Check all whale thresholds against current wallet state.

        Must be called AFTER TimeWindowManager.add_flow() so cumulative
        sums are up to date.

        Args:
            wallet_state: Wallet state with updated cumulative sums.
            flow: The flow event that triggered this check.

        Returns:
            List of WhaleEvent objects for newly crossed thresholds.
        """
        events: List[WhaleEvent] = []

        # Single TX threshold
        if not wallet_state.whale_tx_fired and flow.amount_sol >= self.whale_single_tx_sol:
            wallet_state.whale_tx_fired = True
            events.append(
                WhaleEvent(
                    wallet=flow.wallet,
                    timestamp=flow.timestamp,
                    event_type="WHALE_TX",
                    amount_sol=flow.amount_sol,
                    threshold=self.whale_single_tx_sol,
                    token_ca=flow.token_ca,
                    direction=flow.direction,
                )
            )

        # 5min cumulative threshold
        if (
            not wallet_state.whale_cum_5m_fired
            and wallet_state.cumulative_5min >= self.whale_cum_5min_sol
        ):
            wallet_state.whale_cum_5m_fired = True
            events.append(
                WhaleEvent(
                    wallet=flow.wallet,
                    timestamp=flow.timestamp,
                    event_type="WHALE_CUM_5M",
                    amount_sol=wallet_state.cumulative_5min,
                    threshold=self.whale_cum_5min_sol,
                    token_ca=flow.token_ca,
                    direction=flow.direction,
                )
            )

        # 15min cumulative threshold
        if (
            not wallet_state.whale_cum_15m_fired
            and wallet_state.cumulative_15min >= self.whale_cum_15min_sol
        ):
            wallet_state.whale_cum_15m_fired = True
            events.append(
                WhaleEvent(
                    wallet=flow.wallet,
                    timestamp=flow.timestamp,
                    event_type="WHALE_CUM_15M",
                    amount_sol=wallet_state.cumulative_15min,
                    threshold=self.whale_cum_15min_sol,
                    token_ca=flow.token_ca,
                    direction=flow.direction,
                )
            )

        return events