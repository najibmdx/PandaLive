"""Event data models for PANDA LIVE."""

from dataclasses import dataclass, field
from typing import Dict, List, Literal


@dataclass
class FlowEvent:
    """Raw flow event from data source."""

    wallet: str  # Full 44-char Solana address
    timestamp: int  # Unix epoch seconds
    direction: Literal["buy", "sell"]
    amount_sol: float
    signature: str  # Transaction signature
    token_ca: str  # Token mint address


@dataclass
class WalletSignalEvent:
    """Wallet behavioral signal detection event.

    Signals are observations with structured context, not bare metrics.
    Each signal includes details providing breakdown of what was detected.
    """

    wallet: str  # Full 44-char Solana address
    timestamp: int
    token_ca: str
    signals: List[str]  # e.g., ["TIMING", "COORDINATION"]
    details: Dict[str, dict] = field(default_factory=dict)


@dataclass
class WhaleEvent:
    """Whale threshold crossing event (latched - fires once per threshold)."""

    wallet: str
    timestamp: int
    event_type: Literal["WHALE_TX", "WHALE_CUM_5M", "WHALE_CUM_15M"]
    amount_sol: float
    threshold: float
    token_ca: str


@dataclass
class WaveRecord:
    """Archived summary of a completed wave."""
    wave_id: int
    start_time: int
    end_time: int
    early_wallet_count: int
    peak_disengagement: float


@dataclass
class StateTransitionEvent:
    """Token state transition event.

    Represents an atomic state change in the token state machine,
    including episode context and the trigger that caused the transition.
    """

    token_ca: str
    timestamp: int
    episode_id: int
    from_state: str
    to_state: str
    trigger: str  # Human-readable trigger description
    details: dict = field(default_factory=dict)
