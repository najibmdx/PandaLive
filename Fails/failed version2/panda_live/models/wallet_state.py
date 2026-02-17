"""
PANDA LIVE Wallet State Model

Tracks per-wallet state for signal detection.
"""

from collections import deque
from typing import Set, Optional
import time


class WalletState:
    """
    Tracks state for a single wallet within a token context.
    
    All state is in-memory, no persistence.
    """
    
    def __init__(self, address: str):
        self.address = address
        self.first_seen: int = 0
        self.last_seen: int = 0
        
        # Signal flags
        self.is_early: bool = False
        self.is_coordinated: bool = False
        self.is_persistent: bool = False
        self.is_disengaged: bool = False
        
        # Timing tracking
        self.minute_buckets: Set[int] = set()  # Set of floor(timestamp/60)
        
        # Rolling windows for cumulative detection
        # Each item: (timestamp, amount_sol)
        self.flows_5min: deque = deque()
        self.flows_15min: deque = deque()
        
        # Cumulative sums (cached for performance)
        self.cumulative_5min: float = 0.0
        self.cumulative_15min: float = 0.0
        
        # Whale event tracking (for latched emission)
        self.whale_tx_triggered: bool = False
        self.whale_5m_triggered: bool = False
        self.whale_15m_triggered: bool = False
    
    def add_flow(self, timestamp: int, amount_sol: float):
        """
        Add a flow to rolling windows and update cumulative sums.
        
        Args:
            timestamp: Unix epoch seconds
            amount_sol: Flow amount in SOL (positive value)
        """
        # Update first/last seen
        if self.first_seen == 0:
            self.first_seen = timestamp
        self.last_seen = timestamp
        
        # Add to minute bucket
        minute_bucket = timestamp // 60
        self.minute_buckets.add(minute_bucket)
        
        # Add to rolling windows
        flow = (timestamp, amount_sol)
        self.flows_5min.append(flow)
        self.flows_15min.append(flow)
        
        # Update cumulative sums
        self.cumulative_5min += amount_sol
        self.cumulative_15min += amount_sol
    
    def expire_windows(self, current_time: int):
        """
        Remove flows outside time windows and update cumulative sums.
        
        Args:
            current_time: Current Unix epoch seconds
        """
        # Expire 5-minute window
        cutoff_5min = current_time - 300  # 5 minutes ago
        while self.flows_5min and self.flows_5min[0][0] < cutoff_5min:
            _, amount = self.flows_5min.popleft()
            self.cumulative_5min -= amount
        
        # Expire 15-minute window
        cutoff_15min = current_time - 900  # 15 minutes ago
        while self.flows_15min and self.flows_15min[0][0] < cutoff_15min:
            _, amount = self.flows_15min.popleft()
            self.cumulative_15min -= amount
        
        # Clean up old minute buckets (keep only last 5 minutes for persistence check)
        cutoff_bucket = (current_time - 300) // 60
        self.minute_buckets = {b for b in self.minute_buckets if b >= cutoff_bucket}
    
    def check_persistence(self, current_time: int, max_gap: int = 300) -> bool:
        """
        Check if wallet shows persistence pattern.
        
        Persistence = appeared in >= 2 distinct 1-minute buckets within max_gap.
        
        Args:
            current_time: Current Unix epoch seconds
            max_gap: Maximum gap in seconds (default 300 = 5 minutes)
        
        Returns:
            True if wallet is persistent
        """
        if len(self.minute_buckets) < 2:
            return False
        
        sorted_buckets = sorted(self.minute_buckets)
        time_span = (sorted_buckets[-1] - sorted_buckets[0]) * 60  # Convert buckets to seconds
        
        return time_span <= max_gap
    
    def is_silent(self, current_time: int, threshold: int = 180) -> bool:
        """
        Check if wallet has been silent for threshold duration.
        
        Args:
            current_time: Current Unix epoch seconds
            threshold: Silence threshold in seconds (default 180 = 3 minutes)
        
        Returns:
            True if wallet has been silent for >= threshold
        """
        return (current_time - self.last_seen) >= threshold
    
    def reset_whale_triggers(self):
        """Reset whale event triggers (for new episode)"""
        self.whale_tx_triggered = False
        self.whale_5m_triggered = False
        self.whale_15m_triggered = False
    
    def __repr__(self):
        return (
            f"WalletState({self.address[:8]}... "
            f"early={self.is_early} coord={self.is_coordinated} "
            f"persist={self.is_persistent} cum_5m={self.cumulative_5min:.1f})"
        )
