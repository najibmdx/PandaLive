"""Per-token state tracking for PANDA LIVE."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

# No longer importing SILENT_G_MIN_SECONDS or REPLACEMENT_LOOKBACK_SECONDS
# Silent detection now uses event-driven patterns (EventDrivenPatternDetector)
from .events import WaveRecord
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

    # Chain-aligned "now" (updated by LiveProcessor before render)
    chain_now: Optional[int] = None

    # WAVE TRACKING
    current_wave: int = 1
    wave_start_time: int = 0
    wave_early_wallets: Set[str] = field(default_factory=set)
    wave_history: List = field(default_factory=list)  # List[WaveRecord]

    # Exhaustion signal dedup
    last_exhaustion_signaled_pct: float = 0.0

    # Density tracking: list of (timestamp, wallet_address, direction) tuples
    whale_events_2min: List[Tuple[int, str, str]] = field(default_factory=list)
    episode_max_density: float = 0.0

    # Direction awareness (token-level aggregates)
    total_buy_volume_sol: float = 0.0   # Aggregate buy volume
    total_sell_volume_sol: float = 0.0  # Aggregate sell volume
    buy_tx_count: int = 0               # Total buy transactions
    sell_tx_count: int = 0              # Total sell transactions

    def compute_silent(self, current_time: int) -> Tuple[int, int, float]:
        """Compute silent X/Y/pct using EVENT-DRIVEN pattern detection.
        
        Uses pre-computed is_silent flags set by EventDrivenPatternDetector.
        Detection happens on EVENTS (wallet trades, state changes), not here.

        Returns:
            (silent_x, silent_y, silent_pct)
        """
        if self.episode_start is None:
            return 0, 0, 0.0

        # Count wallets with activity
        eligible = [
            ws for ws in self.active_wallets.values()
            if ws.activity_count >= 1
        ]

        silent_y = len(eligible)
        if silent_y == 0:
            return 0, 0, 0.0

        # Count silent wallets (using event-driven is_silent flag)
        silent_x = sum(1 for ws in eligible if ws.is_silent)

        silent_pct = round(silent_x / silent_y, 2) if silent_y > 0 else 0.0
        return silent_x, silent_y, silent_pct

    def compute_replacement(self, current_time: int) -> str:
        """Compute replacement state using existing non-early wallet logic.

        Uses 300 seconds (5 min) lookback for non-early wallets.
        Returns "YES" if any non-early wallet active within lookback, else "NO".
        SLOWING is not authorized — not computed.

        Returns:
            "YES" or "NO"
        """
        LOOKBACK = 300  # 5 minutes
        for wallet_addr, ws in self.active_wallets.items():
            if wallet_addr not in self.early_wallets:
                if (current_time - ws.last_seen) < LOOKBACK:
                    return "YES"
        return "NO"

    def compute_net_flow(self) -> float:
        """Compute net SOL flow (positive = inflow, negative = outflow)."""
        return self.total_buy_volume_sol - self.total_sell_volume_sol

    def compute_sell_ratio(self) -> float:
        """Compute sell transactions as fraction of total transactions."""
        total = self.buy_tx_count + self.sell_tx_count
        if total == 0:
            return 0.0
        return self.sell_tx_count / total
