"""Per-token state tracking for PANDA LIVE."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from ..config.thresholds import REPLACEMENT_LOOKBACK_SECONDS, SILENT_G_MIN_SECONDS
from .wallet_state import WalletState


@dataclass
class TokenState:
    """Per-token state tracking.

    Manages all wallet states for a single token and tracks token-level metadata,
    current state machine position, and whale density within episodes.
    """

    ca: str  # Mint address
    t0: Optional[int] = None  # Birth timestamp (first observed swap)
    episode_id: int = 0
    episode_start: Optional[int] = None

    # State machine fields
    current_state: str = "TOKEN_QUIET"
    previous_state: Optional[str] = None
    state_changed_at: Optional[int] = None

    active_wallets: Dict[str, WalletState] = field(default_factory=dict)
    early_wallets: Set[str] = field(default_factory=set)

    last_whale_timestamp: Optional[int] = None
    prev_whale_timestamp: Optional[int] = None  # For reignition gap calculation

    # Density tracking: list of (timestamp, wallet_address) tuples
    whale_events_2min: List[Tuple[int, str]] = field(default_factory=list)
    episode_max_density: float = 0.0

    def compute_silent(self, current_time: int) -> Tuple[int, int, float]:
        """Compute silent X/Y/pct using SILENT_G_MIN_SECONDS and eligibility rules.

        Eligible wallets: activity_count >= 1 (had on-chain activity in episode).
        Silent: eligible wallet whose silence duration >= SILENT_G_MIN_SECONDS,
        where silence = current_time - max(episode_start, wallet.first_seen).
        At episode start or with no eligible wallets: returns (0, 0, 0.0).

        Returns:
            (silent_x, silent_y, silent_pct)
        """
        if self.episode_start is None:
            return 0, 0, 0.0

        eligible: List[WalletState] = [
            ws for ws in self.active_wallets.values()
            if ws.activity_count >= 1
        ]

        silent_y = len(eligible)
        if silent_y == 0:
            return 0, 0, 0.0

        silent_x = 0
        for ws in eligible:
            anchor = max(self.episode_start, ws.first_seen)
            silence_duration = current_time - ws.last_seen
            time_since_anchor = current_time - anchor
            if time_since_anchor >= SILENT_G_MIN_SECONDS and silence_duration >= SILENT_G_MIN_SECONDS:
                silent_x += 1

        silent_pct = round(silent_x / silent_y, 2) if silent_y > 0 else 0.0
        return silent_x, silent_y, silent_pct

    def compute_replacement(self, current_time: int) -> str:
        """Compute replacement state using existing non-early wallet logic.

        Uses REPLACEMENT_LOOKBACK_SECONDS (5 min) and non-early wallets.
        Returns "YES" if any non-early wallet active within lookback, else "NO".
        SLOWING is not authorized — not computed.

        Returns:
            "YES" or "NO"
        """
        for wallet_addr, ws in self.active_wallets.items():
            if wallet_addr not in self.early_wallets:
                if (current_time - ws.last_seen) < REPLACEMENT_LOOKBACK_SECONDS:
                    return "YES"
        return "NO"
