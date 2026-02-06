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
        if not wallet_state.whale_tx_fired and flow.amount_sol >= WHALE_SINGLE_TX_SOL:
            wallet_state.whale_tx_fired = True
            events.append(
                WhaleEvent(
                    wallet=flow.wallet,
                    timestamp=flow.timestamp,
                    event_type="WHALE_TX",
                    amount_sol=flow.amount_sol,
                    threshold=WHALE_SINGLE_TX_SOL,
                    token_ca=flow.token_ca,
                )
            )

        # 5min cumulative threshold
        if (
            not wallet_state.whale_cum_5m_fired
            and wallet_state.cumulative_5min >= WHALE_CUM_5MIN_SOL
        ):
            wallet_state.whale_cum_5m_fired = True
            events.append(
                WhaleEvent(
                    wallet=flow.wallet,
                    timestamp=flow.timestamp,
                    event_type="WHALE_CUM_5M",
                    amount_sol=wallet_state.cumulative_5min,
                    threshold=WHALE_CUM_5MIN_SOL,
                    token_ca=flow.token_ca,
                )
            )

        # 15min cumulative threshold
        if (
            not wallet_state.whale_cum_15m_fired
            and wallet_state.cumulative_15min >= WHALE_CUM_15MIN_SOL
        ):
            wallet_state.whale_cum_15m_fired = True
            events.append(
                WhaleEvent(
                    wallet=flow.wallet,
                    timestamp=flow.timestamp,
                    event_type="WHALE_CUM_15M",
                    amount_sol=wallet_state.cumulative_15min,
                    threshold=WHALE_CUM_15MIN_SOL,
                    token_ca=flow.token_ca,
                )
            )

        return events
