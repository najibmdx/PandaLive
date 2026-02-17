"""Per-wallet state tracking for PANDA LIVE."""

from dataclasses import dataclass, field
from collections import deque
from typing import Set, Deque, Tuple


@dataclass
class WalletState:
    """Per-wallet rolling state.

    Tracks flow history in rolling windows and latched whale detection flags.
    Each flow in the deques is stored as (timestamp, amount_sol).
    """

    address: str
    first_seen: int = 0
    last_seen: int = 0
    is_early: bool = False  # Within 300s of token birth

    # 1-min bucket timestamps (floor(timestamp / 60))
    minute_buckets: Set[int] = field(default_factory=set)

    # Rolling windows: deques of (timestamp, amount_sol)
    flows_5min: Deque[Tuple[int, float]] = field(default_factory=deque)
    flows_15min: Deque[Tuple[int, float]] = field(default_factory=deque)

    # Cumulative sums for current windows
    cumulative_5min: float = 0.0
    cumulative_15min: float = 0.0

    # Whale detection state (latched - once True, stays True)
    whale_tx_fired: bool = False
    whale_cum_5m_fired: bool = False
    whale_cum_15m_fired: bool = False

    # Episode activity tracking (for silent eligibility)
    activity_count: int = 0  # Incremented on each flow processed by pipeline

    # Signal detection state
    timing_checked: bool = False
