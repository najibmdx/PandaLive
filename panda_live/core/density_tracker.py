"""Whale density tracking for PANDA LIVE.

Measures whale event density within a 2-minute sliding window
and tracks episode-maximum density for pressure peaking detection.
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
    ) -> None:
        """Add a whale event to the 2-min tracking window.

        Also updates last_whale_timestamp and expires old entries.

        Args:
            token_state: Token state to update.
            wallet: Full 44-char wallet address.
            timestamp: Event timestamp.
        """
        token_state.whale_events_2min.append((timestamp, wallet))
        token_state.prev_whale_timestamp = token_state.last_whale_timestamp
        token_state.last_whale_timestamp = timestamp

        # Remove events older than the 2-min window
        cutoff = timestamp - PRESSURE_PEAKING_WINDOW
        token_state.whale_events_2min = [
            (ts, w) for ts, w in token_state.whale_events_2min if ts >= cutoff
        ]

    def get_current_density(self, token_state: TokenState) -> tuple:
        """Get current 2-min whale density.

        Returns:
            (unique_whale_count, density_per_second)
        """
        unique_wallets = {w for _, w in token_state.whale_events_2min}
        whale_count = len(unique_wallets)
        density = whale_count / PRESSURE_PEAKING_WINDOW if PRESSURE_PEAKING_WINDOW > 0 else 0.0
        return whale_count, density

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
