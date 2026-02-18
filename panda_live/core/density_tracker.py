"""Whale density tracking for PANDA LIVE.

Measures whale event density within a 2-minute sliding window
and tracks episode-maximum density for pressure peaking detection.
Direction-aware: tracks buy and sell density independently.
"""

from ..config.thresholds import PRESSURE_PEAKING_WINDOW
from ..models.token_state import TokenState


class DensityTracker:
    """Tracks whale density for pressure peaking detection."""

    def add_whale_event(
        self,
        token_state: TokenState,
        wallet: str,
        timestamp: int,
        direction: str = "buy",
    ) -> None:
        """Add a whale event to the 2-min tracking window.

        Also updates last_whale_timestamp and expires old entries.

        Args:
            token_state: Token state to update.
            wallet: Full 44-char wallet address.
            timestamp: Event timestamp.
            direction: "buy" or "sell".
        """
        token_state.whale_events_2min.append((timestamp, wallet, direction))
        token_state.prev_whale_timestamp = token_state.last_whale_timestamp
        token_state.last_whale_timestamp = timestamp

        # Remove events older than the 2-min window
        cutoff = timestamp - PRESSURE_PEAKING_WINDOW
        token_state.whale_events_2min = [
            entry for entry in token_state.whale_events_2min if entry[0] >= cutoff
        ]

    def get_current_density(self, token_state: TokenState) -> tuple:
        """Get current 2-min whale density (all directions).

        Returns:
            (unique_whale_count, density_per_second)
        """
        unique_wallets = {entry[1] for entry in token_state.whale_events_2min}
        whale_count = len(unique_wallets)
        density = whale_count / PRESSURE_PEAKING_WINDOW if PRESSURE_PEAKING_WINDOW > 0 else 0.0
        return whale_count, density

    def get_buy_density(self, token_state: TokenState) -> tuple:
        """Get current 2-min BUY whale density.

        Returns:
            (unique_buy_whale_count, buy_density_per_second)
        """
        buy_wallets = {entry[1] for entry in token_state.whale_events_2min if entry[2] == "buy"}
        count = len(buy_wallets)
        density = count / PRESSURE_PEAKING_WINDOW if PRESSURE_PEAKING_WINDOW > 0 else 0.0
        return count, density

    def get_sell_density(self, token_state: TokenState) -> tuple:
        """Get current 2-min SELL whale density.

        Returns:
            (unique_sell_whale_count, sell_density_per_second)
        """
        sell_wallets = {entry[1] for entry in token_state.whale_events_2min if entry[2] == "sell"}
        count = len(sell_wallets)
        density = count / PRESSURE_PEAKING_WINDOW if PRESSURE_PEAKING_WINDOW > 0 else 0.0
        return count, density

    def is_episode_max_density(
        self,
        token_state: TokenState,
        current_density: float,
    ) -> bool:
        """Check if current density is the episode maximum.

        Updates token_state.episode_max_density if a new max is reached.

        Returns:
            True if current > episode_max, False otherwise.
        """
        if current_density > token_state.episode_max_density:
            token_state.episode_max_density = current_density
            return True
        return False
