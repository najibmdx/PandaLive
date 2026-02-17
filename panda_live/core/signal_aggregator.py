"""Signal aggregator for PANDA LIVE.

Processes whale events through all signal detectors and emits
WalletSignalEvent objects with structured context.
"""

from typing import List, Optional, Tuple

from ..config.thresholds import COORDINATION_SAMPLE_WALLETS
from ..models.events import WhaleEvent, WalletSignalEvent
from ..models.token_state import TokenState
from ..models.wallet_state import WalletState
from .wallet_signals import WalletSignalDetector


class SignalAggregator:
    """Aggregates wallet signals and emits WalletSignalEvent objects."""

    def __init__(self) -> None:
        self.detector = WalletSignalDetector()

    def process_whale_event(
        self,
        whale_event: WhaleEvent,
        wallet_state: WalletState,
        token_state: TokenState,
        current_time: int,
    ) -> WalletSignalEvent:
        """Process a whale event through all signal detectors.

        Checks TIMING, COORDINATION, and PERSISTENCE signals for the wallet.
        EXHAUSTION is token-level and checked separately via check_exhaustion().

        Args:
            whale_event: The whale threshold crossing that triggered detection.
            wallet_state: Current state of the wallet.
            token_state: Current state of the token.
            current_time: Current timestamp.

        Returns:
            WalletSignalEvent with all detected signals and structured details.
        """
        signals: List[str] = []
        details: dict = {}

        # 1. TIMING (only check once per wallet — detect_timing handles latch)
        is_new_signal, timing_type = self.detector.detect_timing(wallet_state, token_state)
        if is_new_signal:
            ref_time = token_state.wave_start_time or token_state.t0 or 0
            signals.append("TIMING")
            details["timing"] = {
                "is_early": timing_type == "EARLY",
                "delta_seconds": wallet_state.first_seen - ref_time,
            }

        # 2. COORDINATION (direction-aware)
        is_coord, coordinated_wallets, coord_direction = self.detector.detect_coordination(
            whale_event, current_time
        )
        if is_coord:
            coord_signal = f"COORDINATION_{coord_direction.upper()}"
            signals.append(coord_signal)
            others = [w for w in coordinated_wallets if w != wallet_state.address]
            details["coordination"] = {
                "wallet_count": len(coordinated_wallets),
                "time_window_s": 60,
                "sample_wallets": others[:COORDINATION_SAMPLE_WALLETS],
                "direction": coord_direction,
            }

        # 3. PERSISTENCE
        is_persistent, distinct_minutes = self.detector.detect_persistence(wallet_state)
        if is_persistent:
            signals.append("PERSISTENCE")
            details["persistence"] = {
                "appearances": distinct_minutes,
                "buckets": sorted(wallet_state.minute_buckets),
            }

        return WalletSignalEvent(
            wallet=wallet_state.address,
            timestamp=current_time,
            token_ca=token_state.ca,
            signals=signals,
            details=details,
        )

    def check_wave_exhaustion(
        self,
        token_state: TokenState,
        current_time: int,
    ) -> Tuple[bool, dict]:
        """Check raw wave exhaustion for state machine — no dedup, no gates.

        Returns:
            (is_exhausted, details_dict)
        """
        return self.detector.is_wave_exhausted(token_state, current_time)

    def check_exhaustion(
        self,
        token_state: TokenState,
        current_time: int,
    ) -> Optional[WalletSignalEvent]:
        """Check token-level exhaustion signal.

        Returns:
            WalletSignalEvent with EXHAUSTION signal if detected, else None.
        """
        is_exhausted, exhaust_details = self.detector.detect_exhaustion(
            token_state, current_time
        )
        if is_exhausted:
            return WalletSignalEvent(
                wallet="",  # Token-level signal, no single wallet
                timestamp=current_time,
                token_ca=token_state.ca,
                signals=["EXHAUSTION"],
                details={"exhaustion": exhaust_details},
            )
        return None
