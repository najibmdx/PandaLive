"""Wallet behavioral signal detection for PANDA LIVE.

Detects four signal types from wallet activity patterns:
- TIMING: early vs late appearance relative to token birth
- COORDINATION: 3+ wallets with whale events within 60s
- PERSISTENCE: wallet re-appears across 2+ minute buckets
- EXHAUSTION: 60%+ early wallets silent with no replacement
"""

from typing import Dict, List, Tuple

from ..config.thresholds import (
    COORDINATION_MIN_WALLETS,
    COORDINATION_TIME_WINDOW,
    EARLY_WINDOW,
    EXHAUSTION_EARLY_WALLET_PERCENT,
    EXHAUSTION_SILENCE_THRESHOLD,
    PERSISTENCE_MAX_GAP,
    PERSISTENCE_MIN_APPEARANCES,
)
from ..models.events import WhaleEvent
from ..models.token_state import TokenState
from ..models.wallet_state import WalletState


class WalletSignalDetector:
    """Detects behavioral signals from wallet activity."""

    def __init__(self) -> None:
        self.recent_whale_events: List[WhaleEvent] = []

    def detect_timing(
        self,
        wallet_state: WalletState,
        token_state: TokenState,
    ) -> bool:
        """Detect if wallet appeared early (within EARLY_WINDOW of token birth).

        Sets wallet_state.is_early as a side effect.

        Returns:
            True if early, False otherwise.
        """
        if token_state.t0 is None:
            # Mid-flight start: first wallet is early by definition
            wallet_state.is_early = True
            return True

        delta = wallet_state.first_seen - token_state.t0
        is_early = delta <= EARLY_WINDOW
        wallet_state.is_early = is_early
        return is_early

    def detect_coordination(
        self,
        whale_event: WhaleEvent,
        current_time: int,
    ) -> Tuple[bool, List[str]]:
        """Detect if 3+ wallets had whale events within COORDINATION_TIME_WINDOW.

        Maintains a sliding window of recent whale events and checks for
        distinct wallet clustering.

        Returns:
            (is_coordinated, list_of_coordinated_wallet_addresses)
        """
        self.recent_whale_events.append(whale_event)

        # Expire events outside window
        cutoff = current_time - COORDINATION_TIME_WINDOW
        self.recent_whale_events = [
            e for e in self.recent_whale_events if e.timestamp >= cutoff
        ]

        unique_wallets = {e.wallet for e in self.recent_whale_events}

        if len(unique_wallets) >= COORDINATION_MIN_WALLETS:
            return True, sorted(unique_wallets)

        return False, []

    def detect_persistence(self, wallet_state: WalletState) -> bool:
        """Detect if wallet re-appeared across 2+ minute buckets within gap limit.

        Returns:
            True if persistent (2+ distinct 1-min buckets, gap <= PERSISTENCE_MAX_GAP).
        """
        buckets = sorted(wallet_state.minute_buckets)

        if len(buckets) < PERSISTENCE_MIN_APPEARANCES:
            return False

        gap_seconds = (buckets[-1] - buckets[0]) * 60
        return gap_seconds <= PERSISTENCE_MAX_GAP

    def detect_exhaustion(
        self,
        token_state: TokenState,
        current_time: int,
    ) -> Tuple[bool, Dict]:
        """Detect if 60%+ early wallets are silent AND no replacement whales.

        Returns:
            (is_exhausted, details_dict with breakdown context)
        """
        early_wallets = token_state.early_wallets

        if len(early_wallets) == 0:
            return False, {}

        # Count silent early wallets (no activity for EXHAUSTION_SILENCE_THRESHOLD)
        silent_early: List[str] = []
        for wallet_addr in early_wallets:
            wallet_state = token_state.active_wallets.get(wallet_addr)
            if wallet_state:
                silence_duration = current_time - wallet_state.last_seen
                if silence_duration >= EXHAUSTION_SILENCE_THRESHOLD:
                    silent_early.append(wallet_addr)

        disengagement_pct = len(silent_early) / len(early_wallets)

        if disengagement_pct < EXHAUSTION_EARLY_WALLET_PERCENT:
            return False, {}

        # Check for replacement whales (non-early wallets active in last 5min)
        lookback = 300
        replacement_count = 0
        for wallet_addr, wallet_state in token_state.active_wallets.items():
            if wallet_addr not in early_wallets:
                if (current_time - wallet_state.last_seen) < lookback:
                    replacement_count += 1

        if replacement_count == 0:
            return True, {
                "disengagement_pct": round(disengagement_pct, 2),
                "silent_early_count": len(silent_early),
                "total_early_count": len(early_wallets),
                "replacement_count": 0,
            }

        return False, {}
