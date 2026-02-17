"""Signal aggregator for PANDA LIVE.

Processes whale events through all signal detectors and emits
WalletSignalEvent objects with structured context.
"""

from typing import List, Optional

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

        # 1. TIMING (only check once per wallet)
        if not wallet_state.timing_checked:
            is_new_signal, timing_type = self.detector.detect_timing(wallet_state, token_state)
            if is_new_signal:
                is_early = (timing_type == "EARLY")
                delta = (
                    wallet_state.first_seen - token_state.wave_start_time
                    if token_state.wave_start_time is not None
                    else 0
                )
                signals.append("TIMING")
                details["timing"] = {
                    "is_early": is_early,
                    "delta_seconds": delta,
                }
                if is_early:
                    token_state.wave_early_wallets.add(wallet_state.address)
                    token_state.early_wallets.add(wallet_state.address)
            wallet_state.timing_checked = True

        # 2. COORDINATION
        is_coord, coordinated_wallets = self.detector.detect_coordination(
            whale_event, current_time
        )
        if is_coord:
            signals.append("COORDINATION")
            others = [w for w in coordinated_wallets if w != wallet_state.address]
            details["coordination"] = {
                "wallet_count": len(coordinated_wallets),
                "time_window_s": 60,
                "sample_wallets": others[:COORDINATION_SAMPLE_WALLETS],
            }

        # 3. PERSISTENCE
        is_persistent = self.detector.detect_persistence(wallet_state)
        if is_persistent:
            signals.append("PERSISTENCE")
            details["persistence"] = {
                "appearances": len(wallet_state.minute_buckets),
                "buckets": sorted(wallet_state.minute_buckets),
            }

        return WalletSignalEvent(
            wallet=wallet_state.address,
            timestamp=current_time,
            token_ca=token_state.ca,
            signals=signals,
            details=details,
        )

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
