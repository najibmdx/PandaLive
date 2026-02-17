"""
PANDA LIVE Token State Machine

9-state machine with reversible transitions.

States:
1. TOKEN_QUIET
2. TOKEN_IGNITION
3. TOKEN_COORDINATION_SPIKE
4. TOKEN_EARLY_PHASE
5. TOKEN_PERSISTENCE_CONFIRMED
6. TOKEN_PARTICIPATION_EXPANSION
7. TOKEN_PRESSURE_PEAKING
8. TOKEN_EXHAUSTION_DETECTED
9. TOKEN_DISSIPATION
"""

from typing import Optional, Tuple
from panda_live.models.token_state import TokenState
from panda_live.models.events import StateTransitionEvent
from panda_live.core.episode_tracker import EpisodeTracker
from panda_live.core.density_tracker import DensityTracker
from panda_live.core.wallet_signals import WalletSignalsDetector


class TokenStateMachine:
    """
    Manages token state transitions based on wallet signals.
    
    All transitions are reversible (episodic).
    """
    
    def __init__(self, token_state: TokenState):
        self.token_state = token_state
        self.episode_tracker = EpisodeTracker()
        self.density_tracker = DensityTracker(token_state)
        self.signals_detector = WalletSignalsDetector(token_state)
    
    def evaluate_transition(self, current_time: int) -> Optional[StateTransitionEvent]:
        """
        Evaluate if state transition should occur.
        
        Args:
            current_time: Current timestamp
        
        Returns:
            StateTransitionEvent if transition occurs, None otherwise
        """
        current_state = self.token_state.current_state
        
        # PRIORITY 1: Episode boundary check (silence → QUIET)
        transition = self._check_episode_end(current_time)
        if transition:
            return transition
        
        # PRIORITY 2: State-specific forward transitions
        if current_state == "TOKEN_QUIET":
            return self._check_quiet_to_ignition(current_time)
        
        elif current_state == "TOKEN_IGNITION":
            return self._check_ignition_to_coordination_spike(current_time)
        
        elif current_state == "TOKEN_COORDINATION_SPIKE":
            return self._check_coordination_spike_to_early_phase(current_time)
        
        elif current_state == "TOKEN_EARLY_PHASE":
            return self._check_early_phase_to_persistence(current_time)
        
        elif current_state == "TOKEN_PERSISTENCE_CONFIRMED":
            return self._check_persistence_to_expansion(current_time)
        
        elif current_state == "TOKEN_PARTICIPATION_EXPANSION":
            return self._check_expansion_to_peaking(current_time)
        
        elif current_state == "TOKEN_PRESSURE_PEAKING":
            return self._check_peaking_to_exhaustion(current_time)
        
        elif current_state == "TOKEN_EXHAUSTION_DETECTED":
            return self._check_exhaustion_to_dissipation(current_time)
        
        elif current_state == "TOKEN_DISSIPATION":
            return self._check_dissipation_to_quiet(current_time)
        
        return None
    
    def handle_new_whale(self, current_time: int) -> Optional[StateTransitionEvent]:
        """
        Handle new whale event and check for re-ignition.
        
        Args:
            current_time: Timestamp of whale event
        
        Returns:
            StateTransitionEvent if re-ignition occurs, None otherwise
        """
        # Check if should start new episode or re-ignite
        if self.episode_tracker.should_start_new_episode(
            current_time,
            self.token_state.last_whale_timestamp
        ):
            # New episode
            new_ep_id = self.episode_tracker.start_new_episode()
            self.token_state.start_new_episode(current_time)
            self.token_state.episode_id = new_ep_id
            
            trigger = "new_episode"
        else:
            # Same episode, re-ignition
            trigger = "re_ignition_same_episode"
        
        # If currently QUIET or DISSIPATION, transition to IGNITION
        if self.token_state.current_state in ["TOKEN_QUIET", "TOKEN_DISSIPATION"]:
            return self._create_transition(
                "TOKEN_IGNITION",
                trigger,
                current_time
            )
        
        return None
    
    def _check_episode_end(self, current_time: int) -> Optional[StateTransitionEvent]:
        """Check if episode should end due to silence"""
        if self.episode_tracker.should_end_episode(
            current_time,
            self.token_state.last_whale_timestamp
        ):
            if self.token_state.current_state != "TOKEN_QUIET":
                return self._create_transition(
                    "TOKEN_QUIET",
                    "10_min_silence_episode_end",
                    current_time
                )
        return None
    
    def _check_quiet_to_ignition(self, current_time: int) -> Optional[StateTransitionEvent]:
        """QUIET → IGNITION: First whale detected"""
        # Handled by handle_new_whale()
        return None
    
    def _check_ignition_to_coordination_spike(self, current_time: int) -> Optional[StateTransitionEvent]:
        """IGNITION → COORDINATION_SPIKE: 3+ whales in 60s"""
        recent_whales = self.token_state.get_recent_whale_wallets(60, current_time)
        
        if len(recent_whales) >= 3:
            return self._create_transition(
                "TOKEN_COORDINATION_SPIKE",
                "3+_whales_in_60s",
                current_time,
                {"wallet_count": len(recent_whales)}
            )
        return None
    
    def _check_coordination_spike_to_early_phase(self, current_time: int) -> Optional[StateTransitionEvent]:
        """COORDINATION_SPIKE → EARLY_PHASE: Activity sustained 2+ min"""
        time_in_state = current_time - self.token_state.state_entry_time
        
        if time_in_state >= 120:  # 2 minutes
            return self._create_transition(
                "TOKEN_EARLY_PHASE",
                "sustained_beyond_burst",
                current_time,
                {"duration_seconds": time_in_state}
            )
        return None
    
    def _check_early_phase_to_persistence(self, current_time: int) -> Optional[StateTransitionEvent]:
        """EARLY_PHASE → PERSISTENCE_CONFIRMED: 2+ persistent wallets"""
        persistent_count = self.token_state.get_persistent_wallet_count()
        
        if persistent_count >= 2:
            return self._create_transition(
                "TOKEN_PERSISTENCE_CONFIRMED",
                "2+_persistent_wallets",
                current_time,
                {"persistent_count": persistent_count}
            )
        return None
    
    def _check_persistence_to_expansion(self, current_time: int) -> Optional[StateTransitionEvent]:
        """PERSISTENCE_CONFIRMED → PARTICIPATION_EXPANSION: New non-early whale"""
        recent_whales = self.token_state.get_recent_whale_wallets(300, current_time)
        new_whales = recent_whales - self.token_state.early_wallets
        
        if len(new_whales) > 0:
            return self._create_transition(
                "TOKEN_PARTICIPATION_EXPANSION",
                "new_non_early_whale",
                current_time,
                {"new_whale_count": len(new_whales)}
            )
        return None
    
    def _check_expansion_to_peaking(self, current_time: int) -> Optional[StateTransitionEvent]:
        """PARTICIPATION_EXPANSION → PRESSURE_PEAKING: 5+ whales in 2min, episode max"""
        is_peaking, density = self.density_tracker.check_pressure_peaking(current_time)
        
        if is_peaking:
            return self._create_transition(
                "TOKEN_PRESSURE_PEAKING",
                "5+_whales_in_2min_episode_max",
                current_time,
                {
                    "whale_count": density.whale_count,
                    "density": density.density
                }
            )
        return None
    
    def _check_peaking_to_exhaustion(self, current_time: int) -> Optional[StateTransitionEvent]:
        """PRESSURE_PEAKING → EXHAUSTION_DETECTED: 60% early silent + no replacement"""
        exhaustion = self.signals_detector.detect_exhaustion(current_time)
        
        if exhaustion:
            disengagement = self.token_state.get_disengagement_percentage(current_time, 180)
            recent_whales = self.token_state.get_recent_whale_wallets(300, current_time)
            replacement_count = len(recent_whales - self.token_state.early_wallets)
            
            return self._create_transition(
                "TOKEN_EXHAUSTION_DETECTED",
                "60%_early_disengaged_no_replacement",
                current_time,
                {
                    "disengagement_pct": disengagement,
                    "replacement_whales": replacement_count
                }
            )
        return None
    
    def _check_exhaustion_to_dissipation(self, current_time: int) -> Optional[StateTransitionEvent]:
        """EXHAUSTION_DETECTED → DISSIPATION: <1 whale per 5min"""
        whale_count = self.token_state.count_whale_events_in_window(300, current_time)
        
        if whale_count < 1:
            return self._create_transition(
                "TOKEN_DISSIPATION",
                "activity_collapsed",
                current_time,
                {"whale_events_5min": whale_count}
            )
        return None
    
    def _check_dissipation_to_quiet(self, current_time: int) -> Optional[StateTransitionEvent]:
        """DISSIPATION → QUIET: Handled by episode end check"""
        # Already handled by _check_episode_end
        return None
    
    def _create_transition(
        self,
        new_state: str,
        trigger: str,
        timestamp: int,
        trigger_details: Optional[dict] = None
    ) -> StateTransitionEvent:
        """
        Create state transition and update token state.
        
        Args:
            new_state: Target state
            trigger: Reason for transition
            timestamp: Transition timestamp
            trigger_details: Optional additional details
        
        Returns:
            StateTransitionEvent
        """
        from_state = self.token_state.current_state
        
        # Update token state
        self.token_state.transition_state(new_state, trigger, timestamp)
        
        return StateTransitionEvent(
            timestamp=timestamp,
            token_ca=self.token_state.ca,
            episode_id=self.token_state.episode_id,
            from_state=from_state,
            to_state=new_state,
            trigger=trigger,
            trigger_details=trigger_details
        )
