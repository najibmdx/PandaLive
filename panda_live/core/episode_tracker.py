"""Episode boundary management for PANDA LIVE.

Tracks episode lifecycle: start, re-ignition within same episode,
and episode end after 10 minutes of silence.
"""

from ..config.thresholds import EPISODE_END_SILENCE, EPISODE_REIGNITION_GAP
from ..models.token_state import TokenState


class EpisodeTracker:
    """Manages episode boundaries and re-ignition logic."""

    def check_episode_boundary(
        self,
        token_state: TokenState,
        current_time: int,
    ) -> tuple:
        """Check if episode should end due to silence.

        Args:
            token_state: Current token state.
            current_time: Current timestamp.

        Returns:
            (should_end_episode, is_reignition)
            should_end_episode: True if 10min silence reached.
            is_reignition: Always False from this method (checked separately).
        """
        # Use prev_whale_timestamp when available (the timestamp before
        # the current event updated last_whale_timestamp)
        ref_ts = token_state.prev_whale_timestamp or token_state.last_whale_timestamp
        if ref_ts is None:
            return False, False

        silence_duration = current_time - ref_ts

        if silence_duration >= EPISODE_END_SILENCE:
            return True, False

        return False, False

    def start_new_episode(self, token_state: TokenState, current_time: int) -> None:
        """Start a new episode, resetting episode-scoped state.

        Args:
            token_state: Token state to update.
            current_time: Timestamp for episode start.
        """
        token_state.episode_id += 1
        token_state.episode_start = current_time
        token_state.episode_max_density = 0.0
        token_state.whale_events_2min.clear()

        # Reset wave tracking for new episode
        token_state.current_wave = 1
        token_state.wave_start_time = current_time
        token_state.wave_early_wallets = set()
        token_state.wave_history = []
        token_state.last_exhaustion_signaled_pct = 0.0

    def check_reignition(
        self,
        token_state: TokenState,
        current_time: int,
    ) -> bool:
        """Check if new activity is re-ignition within the same episode.

        Uses prev_whale_timestamp (the timestamp before the current whale
        updated last_whale_timestamp) to measure the actual gap between
        the previous activity and the new whale event.

        Args:
            token_state: Current token state.
            current_time: Current timestamp.

        Returns:
            True if gap < EPISODE_REIGNITION_GAP (same episode),
            False if gap >= threshold (new episode needed).
        """
        ref_ts = token_state.prev_whale_timestamp
        if ref_ts is None:
            return False

        gap = current_time - ref_ts
        return gap < EPISODE_REIGNITION_GAP
