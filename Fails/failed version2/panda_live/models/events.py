"""
PANDA LIVE Event Models

All event types used throughout the system.
"""

from dataclasses import dataclass
from typing import Optional, List, Dict, Any


@dataclass
class FlowEvent:
    """Single wallet token flow event"""
    wallet: str
    timestamp: int  # Unix epoch seconds
    direction: str  # "buy" or "sell"
    amount_sol: float
    signature: str
    token_ca: str
    
    def __post_init__(self):
        """Normalize direction to lowercase"""
        self.direction = self.direction.lower()
        if self.direction not in ["buy", "sell"]:
            raise ValueError(f"Invalid direction: {self.direction}")


@dataclass
class WhaleEvent:
    """Whale threshold crossing event"""
    wallet: str
    timestamp: int
    event_type: str  # "WHALE_TX", "WHALE_CUM_5M", "WHALE_CUM_15M"
    amount_sol: float
    threshold: float
    signature: str
    token_ca: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "event_type": self.event_type,
            "timestamp": self.timestamp,
            "wallet": self.wallet,
            "amount_sol": self.amount_sol,
            "threshold": self.threshold,
            "signature": self.signature,
            "token_ca": self.token_ca
        }


@dataclass
class WalletSignalEvent:
    """Wallet signal detection event"""
    wallet: str
    timestamp: int
    signals: List[str]  # ["TIMING", "COORDINATION", "PERSISTENCE", "EXHAUSTION"]
    details: Dict[str, Any]
    token_ca: str
    episode_id: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "event_type": "WALLET_SIGNAL",
            "timestamp": self.timestamp,
            "token_ca": self.token_ca,
            "episode_id": self.episode_id,
            "wallet": self.wallet,
            "signals": self.signals,
            "details": self.details
        }


@dataclass
class StateTransitionEvent:
    """Token state transition event"""
    timestamp: int
    token_ca: str
    episode_id: int
    from_state: str
    to_state: str
    trigger: str
    trigger_details: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        result = {
            "event_type": "TOKEN_STATE_TRANSITION",
            "timestamp": self.timestamp,
            "token_ca": self.token_ca,
            "episode_id": self.episode_id,
            "from_state": self.from_state,
            "to_state": self.to_state,
            "trigger": self.trigger
        }
        if self.trigger_details:
            result["trigger_details"] = self.trigger_details
        return result


@dataclass
class SessionStartEvent:
    """Session initialization event"""
    timestamp: int
    token_ca: str
    config: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "event_type": "SESSION_START",
            "timestamp": self.timestamp,
            "token_ca": self.token_ca,
            "config": self.config
        }


@dataclass
class SessionEndEvent:
    """Session termination event"""
    timestamp: int
    token_ca: str
    reason: str
    final_state: str
    episode_id: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "event_type": "SESSION_END",
            "timestamp": self.timestamp,
            "token_ca": self.token_ca,
            "reason": self.reason,
            "final_state": self.final_state,
            "episode_id": self.episode_id
        }


@dataclass
class DensityMeasurement:
    """Whale density measurement for pressure peaking detection"""
    timestamp: int
    window_start: int
    window_end: int
    whale_count: int
    density: float  # whales per second
    
    def __lt__(self, other):
        """Enable sorting by whale_count"""
        return self.whale_count < other.whale_count
