"""Wallet behavioral signal detection for PANDA LIVE.

Detects four signal types from wallet activity patterns:
- TIMING: early vs late appearance relative to token birth
- COORDINATION: 3+ wallets with whale events within 60s
- PERSISTENCE: wallet re-appears across 2+ minute buckets
- EXHAUSTION: 60%+ early wallets silent (EVENT-DRIVEN)
"""

from typing import Dict, List, Tuple

from ..config.thresholds import (
    COORDINATION_MIN_WALLETS,
    COORDINATION_TIME_WINDOW,
    EARLY_WINDOW,
    EXHAUSTION_EARLY_WALLET_PERCENT,
    PERSISTENCE_MAX_GAP,
    PERSISTENCE_MIN_APPEARANCES,
)
from ..models.events import WhaleEvent
from ..models.token_state import TokenState
from ..models.wallet_state import WalletState


class WalletSignalDetector:
    """Behavioral signal detector for wallet patterns."""
    
    def __init__(self):
        """Initialize detector with event tracking."""
        self.recent_whale_events: List[WhaleEvent] = []

    def detect_timing(
        self,
        wallet_state: WalletState,
        token_state: TokenState,
    ) -> Tuple[bool, str]:
        """Detect TIMING signal: early vs late wallet entry.

        Early = first_seen within EARLY_WINDOW (300s) of token birth.
        Late = after EARLY_WINDOW.

        Returns:
            (is_new_signal, timing_type: "EARLY" | "LATE")
        """
        if wallet_state.timing_checked:
            return False, ""

        if token_state.t0 is None:
            return False, ""

        is_early = (wallet_state.first_seen - token_state.t0) <= EARLY_WINDOW

        wallet_state.timing_checked = True
        wallet_state.is_early = is_early

        if is_early:
            token_state.early_wallets.add(wallet_state.address)

        return True, "EARLY" if is_early else "LATE"

    def detect_coordination(
        self,
        whale_event: WhaleEvent,
        current_time: int,
    ) -> Tuple[bool, List[str]]:
        """Detect COORDINATION: 3+ whale events within 60s window.
        
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

    def detect_persistence(
        self,
        wallet_state: WalletState,
    ) -> Tuple[bool, int]:
        """Detect PERSISTENCE: wallet re-appears across 2+ distinct minutes.

        Returns:
            (is_persistent, num_distinct_minutes)
        """
        distinct_minutes = len(wallet_state.minute_buckets)

        if distinct_minutes < PERSISTENCE_MIN_APPEARANCES:
            return False, distinct_minutes

        buckets_sorted = sorted(wallet_state.minute_buckets)
        max_gap = 0
        for i in range(1, len(buckets_sorted)):
            gap = (buckets_sorted[i] - buckets_sorted[i - 1]) * 60
            if gap > max_gap:
                max_gap = gap

        is_persistent = max_gap <= PERSISTENCE_MAX_GAP
        return is_persistent, distinct_minutes

    def detect_exhaustion(
        self,
        token_state: TokenState,
        current_time: int,
    ) -> Tuple[bool, Dict]:
        """Detect if 60%+ early wallets are silent (EVENT-DRIVEN).
        
        Uses is_silent flags set by EventDrivenPatternDetector.
        No time-based checks - detection happened on events.
        REMOVED: Replacement whale check (design decision).

        Returns:
            (is_exhausted, details_dict with breakdown context)
        """
        early_wallets = token_state.early_wallets

        if len(early_wallets) == 0:
            return False, {}

        # Count silent early wallets (using event-driven is_silent flag)
        silent_early: List[str] = []
        for wallet_addr in early_wallets:
            wallet_state = token_state.active_wallets.get(wallet_addr)
            if wallet_state and wallet_state.is_silent:  # EVENT-DRIVEN!
                silent_early.append(wallet_addr)

        disengagement_pct = len(silent_early) / len(early_wallets)

        # EXHAUSTION = 60%+ early wallets silent (period)
        # No replacement check - late buyers are exit liquidity
        if disengagement_pct >= EXHAUSTION_EARLY_WALLET_PERCENT:
            return True, {
                "disengagement_pct": round(disengagement_pct, 2),
                "silent_early_count": len(silent_early),
                "total_early_count": len(early_wallets),
            }

        return False, {}
