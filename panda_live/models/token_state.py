"""Per-token state tracking for PANDA LIVE."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

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
