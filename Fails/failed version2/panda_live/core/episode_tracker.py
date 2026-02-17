"""
PANDA LIVE Episode Tracker

Manages episode boundaries and re-ignition logic.

Episode = continuous period of activity separated by 10+ min silence.
"""

from panda_live.config.thresholds import EPISODE_END_SILENCE, EPISODE_REIGNITION_GAP


class EpisodeTracker:
    """
    Tracks episode boundaries for a token.
    
    Episode rules:
    - Silence >= 10 min → Episode ends, state becomes QUIET
    - New whale after <10 min silence → Same episode, re-ignition
    - New whale after >=10 min silence → New episode
    """
    
    def __init__(self):
        self.current_episode_id = 0
    
    def should_end_episode(self, current_time: int, last_whale_time: int) -> bool:
        """
        Check if episode should end due to silence.
        
        Args:
            current_time: Current timestamp
            last_whale_time: Timestamp of last whale event
        
        Returns:
            True if silence >= EPISODE_END_SILENCE (10 min)
        """
        if last_whale_time == 0:
            # No whales yet, no episode to end
            return False
        
        silence = current_time - last_whale_time
        return silence >= EPISODE_END_SILENCE
    
    def should_start_new_episode(self, current_time: int, last_whale_time: int) -> bool:
        """
        Check if new whale should start a new episode vs continue current.
        
        Args:
            current_time: Timestamp of new whale event
            last_whale_time: Timestamp of last whale event (0 if none)
        
        Returns:
            True if gap >= EPISODE_REIGNITION_GAP (10 min) = new episode
            False if gap < 10 min = same episode, re-ignition
        """
        if last_whale_time == 0:
            # First whale ever = new episode
            return True
        
        gap = current_time - last_whale_time
        return gap >= EPISODE_REIGNITION_GAP
    
    def start_new_episode(self) -> int:
        """
        Start a new episode.
        
        Returns:
            New episode ID
        """
        self.current_episode_id += 1
        return self.current_episode_id
    
    def get_current_episode_id(self) -> int:
        """Get current episode ID"""
        return self.current_episode_id
