"""Severity calculator for PANDA LIVE.

Computes S1-S5 ordinal severity annotations for state transitions.
Severity is presentation-layer intelligence that compresses internal
wallet signals into a single ordinal ranking per transition.

Severity is:
- Ordinal (S1-S5), not cardinal
- Transition-bound (only computed at state transitions)
- Episode-scoped (resets on new episode)
- Attached to StateTransitionEvent.details["severity"]
"""

from typing import Optional

from ..models.events import StateTransitionEvent
from ..models.token_state import TokenState

SEVERITY_WEAK = "S1"
SEVERITY_LIGHT = "S2"
SEVERITY_MODERATE = "S3"
SEVERITY_STRONG = "S4"
SEVERITY_EXTREME = "S5"


class SeverityCalculator:
    """Computes S1-S5 severity annotation for state transitions."""

    def __init__(self) -> None:
        self.last_severity: Optional[str] = None

    def compute_severity(
        self,
        transition: StateTransitionEvent,
        token_state: TokenState,
    ) -> Optional[str]:
        """Compute severity for a state transition.

        Args:
            transition: The state transition event.
            token_state: Current token state for context.

        Returns:
            Severity string (S1-S5), or None for TOKEN_QUIET.
        """
        to_state = transition.to_state
        details = transition.details

        # Reset on new episode
        if transition.trigger == "new_episode":
            self.last_severity = None

        severity: Optional[str]

        if to_state == "TOKEN_QUIET":
            severity = None

        elif to_state == "TOKEN_IGNITION":
            severity = self._severity_ignition(token_state)

        elif to_state == "TOKEN_COORDINATION_SPIKE":
            severity = self._severity_coordination_spike(details)

        elif to_state == "TOKEN_EARLY_PHASE":
            severity = self._severity_early_phase(details, token_state)

        elif to_state == "TOKEN_PERSISTENCE_CONFIRMED":
            severity = self._severity_persistence_confirmed(details)

        elif to_state == "TOKEN_PARTICIPATION_EXPANSION":
            severity = self._severity_participation_expansion(details, transition)

        elif to_state == "TOKEN_PRESSURE_PEAKING":
            severity = self._severity_pressure_peaking(details)

        elif to_state == "TOKEN_EXHAUSTION_DETECTED":
            severity = self._severity_exhaustion_detected(details)

        elif to_state == "TOKEN_DISSIPATION":
            severity = self._severity_dissipation(token_state)

        else:
            severity = SEVERITY_LIGHT

        if severity is not None:
            self.last_severity = severity

        return severity

    # --- Per-state severity methods ---

    def _severity_ignition(self, token_state: TokenState) -> str:
        """S1 if single whale, S2 if multiple early."""
        if len(token_state.early_wallets) >= 2:
            return SEVERITY_LIGHT
        return SEVERITY_WEAK

    def _severity_coordination_spike(self, details: dict) -> str:
        """S2 at 3 wallets, scaling to S5 at 6+."""
        coord_count = details.get("coordinated_count", 3)
        if coord_count >= 6:
            return SEVERITY_EXTREME
        if coord_count >= 5:
            return SEVERITY_STRONG
        if coord_count >= 4:
            return SEVERITY_MODERATE
        return SEVERITY_LIGHT

    def _severity_early_phase(self, details: dict, token_state: TokenState) -> str:
        """S2 baseline, S3 if sustained with 3+ early wallets."""
        duration = details.get("duration_seconds", 120)
        early_count = len(token_state.early_wallets)
        if duration >= 180 and early_count >= 3:
            return SEVERITY_MODERATE
        return SEVERITY_LIGHT

    def _severity_persistence_confirmed(self, details: dict) -> str:
        """Floor S3. S4 at 3 persistent, S5 at 4+."""
        persistent_count = details.get("persistent_count", 2)
        if persistent_count >= 4:
            return SEVERITY_EXTREME
        if persistent_count >= 3:
            return SEVERITY_STRONG
        return SEVERITY_MODERATE

    def _severity_participation_expansion(
        self, details: dict, transition: StateTransitionEvent
    ) -> str:
        """S2 baseline. Burst reversal or 3+ new whales escalates."""
        if transition.trigger == "new_whale_burst_reversal":
            return SEVERITY_STRONG
        new_whale_count = details.get("new_whale_count", 1)
        if new_whale_count >= 3:
            return SEVERITY_STRONG
        if new_whale_count >= 2:
            return SEVERITY_MODERATE
        return SEVERITY_LIGHT

    def _severity_pressure_peaking(self, details: dict) -> str:
        """Floor S3. S4 at 7+ whales, S5 at 10+."""
        whale_count = details.get("whale_count", 5)
        if whale_count >= 10:
            return SEVERITY_EXTREME
        if whale_count >= 7:
            return SEVERITY_STRONG
        return SEVERITY_MODERATE

    def _severity_exhaustion_detected(self, details: dict) -> str:
        """Floor S3. Scales with disengagement percentage."""
        pct = details.get("disengagement_pct", 0.6)
        if pct >= 0.80:
            return SEVERITY_EXTREME
        if pct >= 0.70:
            return SEVERITY_STRONG
        return SEVERITY_MODERATE

    def _severity_dissipation(self, token_state: TokenState) -> str:
        """S2 normal decay. S4 if following a strong/extreme state."""
        if self.last_severity in (SEVERITY_STRONG, SEVERITY_EXTREME):
            return SEVERITY_STRONG
        return SEVERITY_LIGHT
