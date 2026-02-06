"""Event data models for PANDA LIVE."""

from dataclasses import dataclass
from typing import Literal


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
class WhaleEvent:
    """Whale threshold crossing event (latched - fires once per threshold)."""

    wallet: str
    timestamp: int
    event_type: Literal["WHALE_TX", "WHALE_CUM_5M", "WHALE_CUM_15M"]
    amount_sol: float
    threshold: float
    token_ca: str
