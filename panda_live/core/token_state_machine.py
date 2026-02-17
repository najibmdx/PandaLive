"""Token state machine for PANDA LIVE.

9-state machine that compresses wallet behavioral signals into a single
token state representing "what's happening with this token RIGHT NOW."

States:
    TOKEN_QUIET -> TOKEN_IGNITION -> TOKEN_COORDINATION_SPIKE ->
    TOKEN_EARLY_PHASE -> TOKEN_PERSISTENCE_CONFIRMED ->
    TOKEN_PARTICIPATION_EXPANSION -> TOKEN_PRESSURE_PEAKING ->
    TOKEN_EXHAUSTION_DETECTED -> TOKEN_DISSIPATION -> TOKEN_QUIET

Reverse transitions:
    TOKEN_EXHAUSTION_DETECTED -> TOKEN_PARTICIPATION_EXPANSION (new whale burst)
    TOKEN_DISSIPATION -> TOKEN_IGNITION (sudden reactivation)
"""

from typing import Optional

from ..config.thresholds import (
    DISSIPATION_WHALE_THRESHOLD,
    PRESSURE_PEAKING_MIN_WHALES,
)
from ..core.density_tracker import DensityTracker
from ..core.episode_tracker import EpisodeTracker
from ..core.severity_calculator import SeverityCalculator
from ..core.signal_aggregator import SignalAggregator
from ..models.events import StateTransitionEvent
from ..models.token_state import TokenState


class TokenStateMachine:
    """9-state machine that compresses wallet signals into token intelligence."""

    def __init__(self) -> None:
        self.episode_tracker = EpisodeTracker()
        self.density_tracker = DensityTracker()
        self.severity_calculator = SeverityCalculator()

    def evaluate_transition(
        self,
        token_state: TokenState,
        signal_aggregator: SignalAggregator,
        current_time: int,
    ) -> Optional[StateTransitionEvent]:
        """Evaluate whether a state transition should occur.

        Checks episode boundary first (highest priority), then evaluates
        forward and reverse transitions based on current state.

        Args:
            token_state: Current token state.
            signal_aggregator: For exhaustion checking.
            current_time: Current timestamp.

        Returns:
            StateTransitionEvent if transition occurs, None otherwise.
        """
        current = token_state.current_state

        # EPISODE BOUNDARY CHECK (highest priority)
        should_end, _ = self.episode_tracker.check_episode_boundary(
            token_state, current_time
        )
        if should_end and current != "TOKEN_QUIET":
            return self._transition(
                token_state,
                "TOKEN_QUIET",
                "10_min_silence_episode_end",
                {},
                current_time,
            )

        # --- FORWARD TRANSITIONS ---

        # QUIET -> IGNITION (first whale event)
        if current == "TOKEN_QUIET":
            if token_state.last_whale_timestamp is not None:
                # First-ever episode always starts fresh
                if token_state.episode_id == 0:
                    self.episode_tracker.start_new_episode(token_state, current_time)
                    trigger = "new_episode"
                else:
                    is_same_episode = self.episode_tracker.check_reignition(
                        token_state, current_time
                    )
                    if not is_same_episode:
                        self.episode_tracker.start_new_episode(token_state, current_time)
                        trigger = "new_episode"
                    else:
                        trigger = "re_ignition_same_episode"

                return self._transition(
                    token_state,
                    "TOKEN_IGNITION",
                    trigger,
                    {},
                    current_time,
                )

        # IGNITION -> COORDINATION_SPIKE (3+ wallets coordinated)
        if current == "TOKEN_IGNITION":
            coord_count = self._count_coordinated_wallets(token_state)
            if coord_count >= 3:
                return self._transition(
                    token_state,
                    "TOKEN_COORDINATION_SPIKE",
                    "3+_wallets_coordinated",
                    {"coordinated_count": coord_count},
                    current_time,
                )

        # COORDINATION_SPIKE -> EARLY_PHASE (sustained 2+ min)
        if current == "TOKEN_COORDINATION_SPIKE":
            if token_state.state_changed_at is not None:
                time_in_state = current_time - token_state.state_changed_at
                if time_in_state >= 120:
                    return self._transition(
                        token_state,
                        "TOKEN_EARLY_PHASE",
                        "sustained_beyond_burst",
                        {"duration_seconds": time_in_state},
                        current_time,
                    )

        # EARLY_PHASE -> PERSISTENCE_CONFIRMED (2+ persistent wallets)
        if current == "TOKEN_EARLY_PHASE":
            persistent_count = self._count_persistent_wallets(token_state)
            if persistent_count >= 2:
                return self._transition(
                    token_state,
                    "TOKEN_PERSISTENCE_CONFIRMED",
                    "2+_persistent_wallets",
                    {"persistent_count": persistent_count},
                    current_time,
                )

        # PERSISTENCE_CONFIRMED -> PARTICIPATION_EXPANSION (new non-early whale)
        if current == "TOKEN_PERSISTENCE_CONFIRMED":
            new_non_early = self._count_recent_non_early_wallets(
                token_state, current_time
            )
            if new_non_early > 0:
                return self._transition(
                    token_state,
                    "TOKEN_PARTICIPATION_EXPANSION",
                    "new_non_early_whales",
                    {"new_whale_count": new_non_early},
                    current_time,
                )

        # PARTICIPATION_EXPANSION -> PRESSURE_PEAKING (5+ whales in 2min, episode max)
        if current == "TOKEN_PARTICIPATION_EXPANSION":
            whale_count, density = self.density_tracker.get_current_density(token_state)
            if whale_count >= PRESSURE_PEAKING_MIN_WHALES:
                is_max = self.density_tracker.is_episode_max_density(
                    token_state, density
                )
                if is_max:
                    return self._transition(
                        token_state,
                        "TOKEN_PRESSURE_PEAKING",
                        "5+_whales_2min_episode_max",
                        {"whale_count": whale_count, "density": round(density, 4)},
                        current_time,
                    )

        # PRESSURE_PEAKING -> EXHAUSTION_DETECTED (60% early silent, no replacement)
        if current == "TOKEN_PRESSURE_PEAKING":
            exhaustion_event = signal_aggregator.check_exhaustion(
                token_state, current_time
            )
            if exhaustion_event is not None:
                return self._transition(
                    token_state,
                    "TOKEN_EXHAUSTION_DETECTED",
                    "60%_early_silent_no_replacement",
                    exhaustion_event.details.get("exhaustion", {}),
                    current_time,
                )

        # EXHAUSTION_DETECTED -> DISSIPATION (<1 whale per 5min)
        # Note: check dissipation BEFORE reverse transition so forward path has priority
        if current == "TOKEN_EXHAUSTION_DETECTED":
            recent_whale_count = self._count_recent_whales(token_state, current_time)
            if recent_whale_count < DISSIPATION_WHALE_THRESHOLD:
                return self._transition(
                    token_state,
                    "TOKEN_DISSIPATION",
                    "activity_collapsed",
                    {"recent_whale_count": recent_whale_count},
                    current_time,
                )

        # --- REVERSE TRANSITIONS ---

        # EXHAUSTION -> PARTICIPATION_EXPANSION (new whale burst)
        if current == "TOKEN_EXHAUSTION_DETECTED":
            recent_whales = self._count_recent_whales(
                token_state, current_time, lookback=60
            )
            if recent_whales >= 2:
                return self._transition(
                    token_state,
                    "TOKEN_PARTICIPATION_EXPANSION",
                    "new_whale_burst_reversal",
                    {"new_whale_count": recent_whales},
                    current_time,
                )

        # DISSIPATION -> IGNITION (sudden reactivation)
        if current == "TOKEN_DISSIPATION":
            recent_whales = self._count_recent_whales(
                token_state, current_time, lookback=60
            )
            if recent_whales >= 1:
                return self._transition(
                    token_state,
                    "TOKEN_IGNITION",
                    "sudden_reactivation",
                    {},
                    current_time,
                )

        return None

    def _transition(
        self,
        token_state: TokenState,
        new_state: str,
        trigger: str,
        details: dict,
        current_time: int,
    ) -> StateTransitionEvent:
        """Execute an atomic state transition.

        Updates token_state in-place, computes severity, and returns
        the transition event with severity attached to details.
        """
        from_state = token_state.current_state
        token_state.previous_state = from_state
        token_state.current_state = new_state
        token_state.state_changed_at = current_time

        event = StateTransitionEvent(
            token_ca=token_state.ca,
            timestamp=current_time,
            episode_id=token_state.episode_id,
            from_state=from_state,
            to_state=new_state,
            trigger=trigger,
            details=details,
        )

        # Compute and attach severity (non-invasive)
        severity = self.severity_calculator.compute_severity(event, token_state)
        if severity is not None:
            event.details["severity"] = severity

        return event

    def _count_coordinated_wallets(self, token_state: TokenState) -> int:
        """Count early wallets (proxy for coordination in current episode)."""
        return len(
            [w for w in token_state.active_wallets.values() if w.is_early]
        )

    def _count_persistent_wallets(self, token_state: TokenState) -> int:
        """Count wallets showing persistence (2+ distinct minute buckets)."""
        return sum(
            1
            for ws in token_state.active_wallets.values()
            if len(ws.minute_buckets) >= 2
        )

    def _count_recent_non_early_wallets(
        self, token_state: TokenState, current_time: int
    ) -> int:
        """Count non-early wallets active in last 5 minutes."""
        lookback = 300
        return sum(
            1
            for addr, ws in token_state.active_wallets.items()
            if addr not in token_state.early_wallets
            and (current_time - ws.last_seen) < lookback
        )

    def _count_recent_whales(
        self,
        token_state: TokenState,
        current_time: int,
        lookback: int = 300,
    ) -> int:
        """Count unique wallets with recent activity within lookback window."""
        return sum(
            1
            for ws in token_state.active_wallets.values()
            if (current_time - ws.last_seen) < lookback
        )
