"""
PANDA LIVE Density Tracker

Tracks whale density for pressure peaking detection.
"""

from typing import List
from panda_live.models.events import DensityMeasurement
from panda_live.models.token_state import TokenState
from panda_live.config.thresholds import PRESSURE_PEAKING_WINDOW, PRESSURE_PEAKING_MIN_WHALES


class DensityTracker:
    """
    Tracks whale density in 2-minute windows for pressure peaking detection.
    """
    
    def __init__(self, token_state: TokenState):
        self.token_state = token_state
    
    def compute_current_density(self, current_time: int) -> DensityMeasurement:
        """
        Compute whale density in last 2 minutes.
        
        Args:
            current_time: Current timestamp
        
        Returns:
            DensityMeasurement with whale count and density
        """
        window_start = current_time - PRESSURE_PEAKING_WINDOW
        
        # Get unique wallets with whale events in window
        whale_wallets = self.token_state.get_recent_whale_wallets(
            PRESSURE_PEAKING_WINDOW,
            current_time
        )
        
        whale_count = len(whale_wallets)
        density = whale_count / PRESSURE_PEAKING_WINDOW  # whales per second
        
        return DensityMeasurement(
            timestamp=current_time,
            window_start=window_start,
            window_end=current_time,
            whale_count=whale_count,
            density=density
        )
    
    def is_episode_max_density(self, current_density: DensityMeasurement) -> bool:
        """
        Check if current density is episode maximum so far.
        
        Args:
            current_density: Current density measurement
        
        Returns:
            True if current is max in episode, False otherwise
        """
        # Get episode max so far
        episode_max = self.token_state.get_episode_max_density()
        
        if episode_max is None:
            # First measurement in episode = max by default
            return True
        
        # Current must EXCEED previous max
        return current_density.whale_count > episode_max.whale_count
    
    def check_pressure_peaking(self, current_time: int) -> tuple:
        """
        Check if pressure peaking condition is met.
        
        Pressure peaking requires:
        1. >= 5 whales in 2-min window
        2. Episode maximum density so far
        
        Args:
            current_time: Current timestamp
        
        Returns:
            (is_peaking: bool, density: DensityMeasurement)
        """
        # Compute current density
        current_density = self.compute_current_density(current_time)
        
        # Store measurement
        self.token_state.add_density_measurement(current_density)
        
        # Check minimum whale count
        if current_density.whale_count < PRESSURE_PEAKING_MIN_WHALES:
            return False, current_density
        
        # Check if episode max
        is_max = self.is_episode_max_density(current_density)
        
        return is_max, current_density
