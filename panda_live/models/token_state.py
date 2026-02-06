"""Per-token state tracking for PANDA LIVE."""

from dataclasses import dataclass, field
from typing import Dict, Optional, Set

from .wallet_state import WalletState


@dataclass
class TokenState:
    """Per-token state tracking.

    Manages all wallet states for a single token and tracks token-level metadata.
    """

    ca: str  # Mint address
    t0: Optional[int] = None  # Birth timestamp (first observed swap)
    episode_id: int = 0
    episode_start: Optional[int] = None

    active_wallets: Dict[str, WalletState] = field(default_factory=dict)
    early_wallets: Set[str] = field(default_factory=set)

    last_whale_timestamp: Optional[int] = None
