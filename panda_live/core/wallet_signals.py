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
    EXHAUSTION_SIGNAL_STEP,
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

        ref_time = token_state.wave_start_time if token_state.wave_start_time else token_state.t0
        delta = wallet_state.first_seen - ref_time
        is_early = (0 <= delta <= EARLY_WINDOW)

        wallet_state.timing_checked = True
        wallet_state.is_early = is_early

        if is_early:
            token_state.early_wallets.add(wallet_state.address)
            token_state.wave_early_wallets.add(wallet_state.address)

        return True, "EARLY" if is_early else "LATE"

    def detect_coordination(
        self,
        whale_event: WhaleEvent,
        current_time: int,
    ) -> Tuple[bool, List[str], str]:
        """Detect COORDINATION: 3+ whale events within 60s window.

        Maintains a sliding window of recent whale events and checks for
        distinct wallet clustering. Returns direction of the dominant side.

        Returns:
            (is_coordinated, list_of_coordinated_wallet_addresses, coord_direction)
            coord_direction is "buy", "sell", or "mixed"
        """
        self.recent_whale_events.append(whale_event)

        # Expire events outside window
        cutoff = current_time - COORDINATION_TIME_WINDOW
        self.recent_whale_events = [
            e for e in self.recent_whale_events if e.timestamp >= cutoff
        ]

        unique_wallets = {e.wallet for e in self.recent_whale_events}

        if len(unique_wallets) >= COORDINATION_MIN_WALLETS:
            # Determine coordination direction
            buy_wallets = {e.wallet for e in self.recent_whale_events if e.direction == "buy"}
            sell_wallets = {e.wallet for e in self.recent_whale_events if e.direction == "sell"}

            if len(sell_wallets) == 0:
                coord_direction = "buy"
            elif len(buy_wallets) == 0:
                coord_direction = "sell"
            else:
                coord_direction = "mixed"

            return True, sorted(unique_wallets), coord_direction

        return False, [], ""

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

    def is_wave_exhausted(
        self,
        token_state: "TokenState",
        current_time: int,
    ) -> Tuple[bool, Dict]:
        """Raw exhaustion check for state machine — no dedup, no replacement gate.

        Pure question: Is 60%+ of current wave's early cohort silent?
        Uses event-driven is_silent flags (set by EventDrivenPatternDetector).

        Returns:
            (is_exhausted, details_dict)
        """
        wave_early = token_state.wave_early_wallets

        if len(wave_early) == 0:
            return False, {}

        silent_early = []
        for wallet_addr in wave_early:
            wallet_state = token_state.active_wallets.get(wallet_addr)
            if wallet_state and wallet_state.is_silent:
                silent_early.append(wallet_addr)

        disengagement_pct = len(silent_early) / len(wave_early)

        if disengagement_pct < EXHAUSTION_EARLY_WALLET_PERCENT:
            return False, {}

        return True, {
            "disengagement_pct": round(disengagement_pct, 2),
            "silent_early_count": len(silent_early),
            "total_early_count": len(wave_early),
        }

    def detect_exhaustion(
        self,
        token_state: TokenState,
        current_time: int,
    ) -> Tuple[bool, Dict]:
        """Detect if 60%+ wave early wallets are silent (EVENT-DRIVEN).

        Uses is_silent flags set by EventDrivenPatternDetector.
        Wave-scoped: checks wave_early_wallets, not episode-level early_wallets.
        Dedup: only signals at first crossing and each +10% step.
        REMOVED: Replacement whale check (design decision).

        Returns:
            (is_exhausted, details_dict with breakdown context)
        """
        if len(token_state.wave_early_wallets) == 0:
            return False, {}

        # Count silent wave-early wallets (using event-driven is_silent flag)
        silent_early: List[str] = []
        for wallet_addr in token_state.wave_early_wallets:
            wallet_state = token_state.active_wallets.get(wallet_addr)
            if wallet_state and wallet_state.is_silent:  # EVENT-DRIVEN!
                silent_early.append(wallet_addr)

        disengagement_pct = len(silent_early) / len(token_state.wave_early_wallets)

        # EXHAUSTION = 60%+ wave early wallets silent (period)
        # No replacement check - late buyers are exit liquidity
        if disengagement_pct >= EXHAUSTION_EARLY_WALLET_PERCENT:
            # Dedup: only signal at first crossing or each +10% step
            should_signal = False
            if token_state.last_exhaustion_signaled_pct == 0.0:
                should_signal = True  # First time crossing 60%
            elif disengagement_pct >= token_state.last_exhaustion_signaled_pct + EXHAUSTION_SIGNAL_STEP:
                should_signal = True  # Crossed next 10% step

            if should_signal:
                token_state.last_exhaustion_signaled_pct = disengagement_pct
                return True, {
                    "disengagement_pct": round(disengagement_pct, 2),
                    "silent_early_count": len(silent_early),
                    "total_early_count": len(token_state.wave_early_wallets),
                }

        return False, {}
