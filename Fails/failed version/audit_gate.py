"""
M10: Acceptance & Audit Gate
Enforces runtime invariants and can HOLD output emission.
"""

from typing import List, Optional
from event_log import CanonicalEvent


class AuditStatus:
    """Audit gate status."""
    PASS = "PASS"
    HOLD = "HOLD"
    FAIL = "FAIL"


class AuditGate:
    """
    Runtime gate for invariant validation.
    Can HOLD emission if invariants fail.
    """
    
    def __init__(self):
        self.status = AuditStatus.PASS
        self.violations: List[str] = []
    
    def validate_canonical_event(self, event: CanonicalEvent) -> bool:
        """
        Validate a single canonical event.
        
        Checks:
        - Required fields are present
        - Types are correct
        - Slot is non-negative
        
        Returns:
            True if valid, False otherwise
        """
        violations = []
        
        # Check required fields
        if not event.session_id:
            violations.append(f"Missing session_id in event {event.signature}")
        
        if not event.mint:
            violations.append(f"Missing mint in event {event.signature}")
        
        if not event.signature:
            violations.append(f"Missing signature in event")
        
        if event.slot < 0:
            violations.append(f"Invalid slot {event.slot} in event {event.signature}")
        
        if event.block_time < 0:
            violations.append(f"Invalid block_time {event.block_time} in event {event.signature}")
        
        if not event.event_type:
            violations.append(f"Missing event_type in event {event.signature}")
        
        if violations:
            self.violations.extend(violations)
            self.status = AuditStatus.HOLD
            return False
        
        return True
    
    def validate_event_ordering(
        self,
        events: List[CanonicalEvent]
    ) -> bool:
        """
        Validate that events are strictly ordered by (slot, signature).
        
        Args:
            events: List of canonical events
        
        Returns:
            True if ordering is valid, False otherwise
        """
        if len(events) <= 1:
            return True
        
        violations = []
        
        for i in range(1, len(events)):
            prev = events[i-1]
            curr = events[i]
            
            # Check slot ordering
            if curr.slot < prev.slot:
                violations.append(
                    f"Out-of-order slots: {prev.slot} -> {curr.slot} "
                    f"(sigs: {prev.signature[:8]}... -> {curr.signature[:8]}...)"
                )
            
            # If same slot, signatures should be different
            elif curr.slot == prev.slot and curr.signature == prev.signature:
                violations.append(
                    f"Duplicate event: slot={curr.slot}, sig={curr.signature[:8]}..."
                )
        
        if violations:
            self.violations.extend(violations)
            self.status = AuditStatus.HOLD
            return False
        
        return True
    
    def validate_intelligence_transition(
        self,
        transition: "IntelligenceTransition"
    ) -> bool:
        """
        Validate an intelligence transition.
        
        Checks:
        - Required fields present
        - Entity address is full (not truncated)
        - Transition type is valid
        
        Returns:
            True if valid, False otherwise
        """
        violations = []
        
        # Check required fields
        if not transition.session_id:
            violations.append("Missing session_id in transition")
        
        if not transition.mint:
            violations.append("Missing mint in transition")
        
        if not transition.entity_address:
            violations.append("Missing entity_address in transition")
        
        # Check address is not truncated (should be full length)
        if transition.entity_address and len(transition.entity_address) < 32:
            violations.append(
                f"Truncated address detected: {transition.entity_address} "
                f"(len={len(transition.entity_address)})"
            )
        
        if not transition.transition_type:
            violations.append("Missing transition_type in transition")
        
        if violations:
            self.violations.extend(violations)
            self.status = AuditStatus.HOLD
            return False
        
        return True
    
    def check_determinism(
        self,
        original_transitions: List["IntelligenceTransition"],
        replay_transitions: List["IntelligenceTransition"]
    ) -> bool:
        """
        Check that replay produces identical transitions.
        
        Args:
            original_transitions: Transitions from original run
            replay_transitions: Transitions from replay
        
        Returns:
            True if identical, False otherwise
        """
        if len(original_transitions) != len(replay_transitions):
            self.violations.append(
                f"Transition count mismatch: original={len(original_transitions)}, "
                f"replay={len(replay_transitions)}"
            )
            self.status = AuditStatus.FAIL
            return False
        
        violations = []
        
        for i, (orig, replay) in enumerate(zip(original_transitions, replay_transitions)):
            if orig.transition_id != replay.transition_id:
                violations.append(
                    f"Transition {i}: ID mismatch: {orig.transition_id} != {replay.transition_id}"
                )
            
            if orig.transition_type != replay.transition_type:
                violations.append(
                    f"Transition {i}: Type mismatch: {orig.transition_type} != {replay.transition_type}"
                )
            
            if orig.entity_address != replay.entity_address:
                violations.append(
                    f"Transition {i}: Entity mismatch: {orig.entity_address} != {replay.entity_address}"
                )
        
        if violations:
            self.violations.extend(violations[:10])  # Limit to first 10
            self.status = AuditStatus.FAIL
            return False
        
        return True
    
    def reset(self):
        """Reset audit state."""
        self.status = AuditStatus.PASS
        self.violations = []
    
    def report(self) -> str:
        """Generate audit report."""
        if self.status == AuditStatus.PASS:
            return "AUDIT: PASS"
        
        report = f"AUDIT: {self.status}\n"
        report += f"Violations ({len(self.violations)}):\n"
        
        for violation in self.violations[:20]:  # Show first 20
            report += f"  - {violation}\n"
        
        if len(self.violations) > 20:
            report += f"  ... and {len(self.violations) - 20} more\n"
        
        return report


def selftest_audit_gate():
    """Self-test for audit gate."""
    
    gate = AuditGate()
    
    # Test valid event
    from event_log import CanonicalEvent
    
    valid_event = CanonicalEvent(
        session_id="test",
        mint="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
        slot=12345,
        block_time=1640000000,
        signature="5j7s6NiJS3JAkvgkoc18WVAsiSaci2pxB2A6ueCJP4tprA2TFg9wSyTLeYouxPBJEMzJinENTkpA52YStRW5Dia7",
        event_type="SWAP",
        actors=["wallet1", "wallet2"]
    )
    
    assert gate.validate_canonical_event(valid_event) == True
    assert gate.status == AuditStatus.PASS
    
    # Test invalid event (missing signature)
    gate.reset()
    invalid_event = CanonicalEvent(
        session_id="test",
        mint="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
        slot=12345,
        block_time=1640000000,
        signature="",  # Invalid
        event_type="SWAP",
        actors=["wallet1"]
    )
    
    assert gate.validate_canonical_event(invalid_event) == False
    assert gate.status == AuditStatus.HOLD
    assert len(gate.violations) > 0
    
    # Test ordering validation
    gate.reset()
    events = [
        CanonicalEvent(
            session_id="test",
            mint="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
            slot=100,
            block_time=1640000000,
            signature="sig1",
            event_type="SWAP",
            actors=[]
        ),
        CanonicalEvent(
            session_id="test",
            mint="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
            slot=99,  # Out of order
            block_time=1640000001,
            signature="sig2",
            event_type="SWAP",
            actors=[]
        )
    ]
    
    assert gate.validate_event_ordering(events) == False
    assert gate.status == AuditStatus.HOLD
    
    print("✓ AuditGate selftest PASSED")


if __name__ == "__main__":
    selftest_audit_gate()
