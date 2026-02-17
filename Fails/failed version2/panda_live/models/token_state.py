"""
PANDA LIVE Token State Model

Tracks state for a single token (mint address).
"""

from collections import deque
from typing import Dict, Set, List, Optional
from panda_live.models.wallet_state import WalletState
from panda_live.models.events import DensityMeasurement


class TokenState:
    """
    Tracks complete state for a single token.
    
    All state is in-memory, no persistence.
    """
    
    def __init__(self, ca: str):
        self.ca = ca
        self.t0: int = 0  # Token birth timestamp (first observed swap)
        
        # Current state machine state
        self.current_state: str = "TOKEN_QUIET"
        self.previous_state: str = "TOKEN_QUIET"
        self.state_entry_time: int = 0
        
        # Episode tracking
        self.episode_id: int = 0
        self.episode_start: int = 0
        self.last_whale_timestamp: int = 0
        
        # Wallet tracking
        self.active_wallets: Dict[str, WalletState] = {}
        self.early_wallets: Set[str] = set()
        
        # Whale event history (for coordination detection)
        # Stores (timestamp, wallet, event_type) tuples
        self.whale_event_history: deque = deque(maxlen=100)
        
        # Density history (for pressure peaking detection)
        self.episode_density_history: List[DensityMeasurement] = []
        
        # State history (for analysis/debug)
        self.state_history: List[tuple] = []  # (timestamp, from_state, to_state, trigger)
    
    def set_t0(self, timestamp: int):
        """Set token birth timestamp (first observed swap)"""
        if self.t0 == 0:
            self.t0 = timestamp
    
    def get_or_create_wallet(self, address: str) -> WalletState:
        """Get existing wallet state or create new one"""
        if address not in self.active_wallets:
            self.active_wallets[address] = WalletState(address)
        return self.active_wallets[address]
    
    def add_whale_event(self, timestamp: int, wallet: str, event_type: str):
        """Add whale event to history for coordination detection"""
        self.whale_event_history.append((timestamp, wallet, event_type))
        self.last_whale_timestamp = timestamp
    
    def get_recent_whale_wallets(self, window_seconds: int, current_time: int) -> Set[str]:
        """
        Get unique wallets that had whale events in recent window.
        
        Args:
            window_seconds: Time window in seconds
            current_time: Current timestamp
        
        Returns:
            Set of wallet addresses
        """
        cutoff = current_time - window_seconds
        return {
            wallet for ts, wallet, _ in self.whale_event_history
            if ts >= cutoff
        }
    
    def count_whale_events_in_window(self, window_seconds: int, current_time: int) -> int:
        """Count whale events in recent window"""
        cutoff = current_time - window_seconds
        return sum(1 for ts, _, _ in self.whale_event_history if ts >= cutoff)
    
    def add_density_measurement(self, measurement: DensityMeasurement):
        """Add density measurement for pressure peaking detection"""
        self.episode_density_history.append(measurement)
    
    def get_episode_max_density(self) -> Optional[DensityMeasurement]:
        """Get maximum density measurement in current episode"""
        episode_measurements = [
            m for m in self.episode_density_history
            if m.timestamp >= self.episode_start
        ]
        if not episode_measurements:
            return None
        return max(episode_measurements, key=lambda m: m.whale_count)
    
    def transition_state(self, new_state: str, trigger: str, timestamp: int):
        """
        Transition to new state.
        
        Args:
            new_state: Target state
            trigger: Reason for transition
            timestamp: Transition timestamp
        """
        self.previous_state = self.current_state
        self.current_state = new_state
        self.state_entry_time = timestamp
        
        # Record in history
        self.state_history.append((timestamp, self.previous_state, new_state, trigger))
    
    def start_new_episode(self, timestamp: int):
        """
        Start a new episode.
        
        Args:
            timestamp: Episode start timestamp
        """
        self.episode_id += 1
        self.episode_start = timestamp
        self.episode_density_history = []
        
        # Reset whale triggers for all wallets
        for wallet in self.active_wallets.values():
            wallet.reset_whale_triggers()
    
    def get_active_wallet_count(self) -> int:
        """Get count of active wallets"""
        return len(self.active_wallets)
    
    def get_early_wallet_count(self) -> int:
        """Get count of early wallets"""
        return len(self.early_wallets)
    
    def get_persistent_wallet_count(self) -> int:
        """Get count of persistent wallets"""
        return sum(1 for w in self.active_wallets.values() if w.is_persistent)
    
    def get_disengaged_early_count(self, current_time: int, silence_threshold: int = 180) -> int:
        """Get count of disengaged early wallets"""
        return sum(
            1 for addr in self.early_wallets
            if addr in self.active_wallets and self.active_wallets[addr].is_silent(current_time, silence_threshold)
        )
    
    def get_disengagement_percentage(self, current_time: int, silence_threshold: int = 180) -> float:
        """
        Get percentage of early wallets that are disengaged.
        
        Returns:
            Percentage (0.0 to 1.0), or 0.0 if no early wallets
        """
        if not self.early_wallets:
            return 0.0
        
        disengaged = self.get_disengaged_early_count(current_time, silence_threshold)
        return disengaged / len(self.early_wallets)
    
    def cleanup_inactive_wallets(self, current_time: int, inactivity_threshold: int = 900):
        """
        Remove wallets that have been inactive for > threshold.
        
        Args:
            current_time: Current timestamp
            inactivity_threshold: Inactivity threshold in seconds (default 900 = 15 min)
        """
        inactive_wallets = [
            addr for addr, wallet in self.active_wallets.items()
            if (current_time - wallet.last_seen) > inactivity_threshold
        ]
        
        for addr in inactive_wallets:
            del self.active_wallets[addr]
            self.early_wallets.discard(addr)
    
    def __repr__(self):
        return (
            f"TokenState({self.ca[:8]}... "
            f"state={self.current_state} ep={self.episode_id} "
            f"wallets={len(self.active_wallets)} early={len(self.early_wallets)})"
        )
