"""
M9: Deterministic Replay Runner
Replays canonical event log through the same pipeline.
Must produce byte-identical intelligence transitions.
"""

from pathlib import Path
from typing import List
from event_log import CanonicalEventLogReader
from intelligence_output import (
    IntelligenceTransition,
    IntelligenceOutputWriter,
    IntelligenceOutputReader
)
from intelligence_engine import (
    V4Primitives,
    IncrementalPrimitiveUpdater,
    WalletIntelligenceEngine,
    TokenIntelligenceCompressor
)
from audit_gate import AuditGate


class ReplayRunner:
    """
    Deterministic replay runner.
    
    Reads canonical events from log and processes through the same pipeline
    to verify deterministic output.
    """
    
    def __init__(
        self,
        events_log_path: Path,
        replay_alerts_path: Path,
        audit_gate: AuditGate
    ):
        self.events_log_path = Path(events_log_path)
        self.replay_alerts_path = Path(replay_alerts_path)
        self.audit_gate = audit_gate
    
    def run(self, token_name: str = "") -> List[IntelligenceTransition]:
        """
        Run replay.
        
        Args:
            token_name: Token name for display
        
        Returns:
            List of intelligence transitions produced
        """
        print(f"\nREPLAY: Reading events from {self.events_log_path.name}")
        
        # Read all canonical events
        reader = CanonicalEventLogReader(self.events_log_path)
        events = reader.read_all()
        
        print(f"REPLAY: Loaded {len(events)} canonical events")
        
        if not events:
            print("REPLAY: No events to replay")
            return []
        
        # Validate event ordering
        if not self.audit_gate.validate_event_ordering(events):
            print(f"REPLAY: Ordering validation FAILED")
            print(self.audit_gate.report())
            return []
        
        # Extract session info from first event
        first_event = events[0]
        session_id = first_event.session_id
        mint = first_event.mint
        
        print(f"REPLAY: Session {session_id}")
        print(f"REPLAY: Mint {mint}")
        
        # Initialize intelligence pipeline
        primitives = V4Primitives(mint=mint, session_id=session_id)
        updater = IncrementalPrimitiveUpdater(primitives)
        wallet_engine = WalletIntelligenceEngine(primitives)
        token_compressor = TokenIntelligenceCompressor(primitives)
        
        # Process events and collect transitions
        all_transitions: List[IntelligenceTransition] = []
        
        output_writer = IntelligenceOutputWriter(self.replay_alerts_path)
        
        with output_writer:
            for i, event in enumerate(events):
                # Validate event
                if not self.audit_gate.validate_canonical_event(event):
                    print(f"REPLAY: Event {i} validation FAILED")
                    continue
                
                # Update primitives
                updater.update(event)
                
                # Check wallet transitions
                wallet_transitions = wallet_engine.check_transitions(event, token_name)
                
                # Validate and emit wallet transitions
                for transition in wallet_transitions:
                    if self.audit_gate.validate_intelligence_transition(transition):
                        output_writer.emit(transition)
                        all_transitions.append(transition)
                
                # Check token state transition
                if wallet_transitions:
                    token_transition = token_compressor.compress(
                        wallet_transitions,
                        event.block_time,
                        token_name
                    )
                    
                    if token_transition:
                        if self.audit_gate.validate_intelligence_transition(token_transition):
                            output_writer.emit(token_transition)
                            all_transitions.append(token_transition)
        
        print(f"REPLAY: Produced {len(all_transitions)} intelligence transitions")
        print(f"REPLAY: Written to {self.replay_alerts_path.name}")
        
        return all_transitions
    
    def compare_with_original(
        self,
        original_alerts_path: Path
    ) -> bool:
        """
        Compare replay output with original to verify determinism.
        
        Args:
            original_alerts_path: Path to original alerts.tsv
        
        Returns:
            True if identical, False otherwise
        """
        print(f"\nDETERMINISM CHECK:")
        print(f"  Original: {original_alerts_path.name}")
        print(f"  Replay:   {self.replay_alerts_path.name}")
        
        # Read both
        original_reader = IntelligenceOutputReader(original_alerts_path)
        replay_reader = IntelligenceOutputReader(self.replay_alerts_path)
        
        original_transitions = original_reader.read_all()
        replay_transitions = replay_reader.read_all()
        
        print(f"  Original transitions: {len(original_transitions)}")
        print(f"  Replay transitions:   {len(replay_transitions)}")
        
        # Check determinism
        is_deterministic = self.audit_gate.check_determinism(
            original_transitions,
            replay_transitions
        )
        
        if is_deterministic:
            print("  ✓ DETERMINISM: PASS")
        else:
            print("  ✗ DETERMINISM: FAIL")
            print(self.audit_gate.report())
        
        return is_deterministic


def selftest_replay():
    """Self-test for replay runner."""
    import tempfile
    import shutil
    from event_log import CanonicalEvent, CanonicalEventLogWriter
    
    tmpdir = Path(tempfile.mkdtemp())
    try:
        # Create test event log
        events_path = tmpdir / "test.events.csv"
        alerts_path = tmpdir / "test.alerts.tsv"
        replay_alerts_path = tmpdir / "test_replay.alerts.tsv"
        
        # Write some test events
        writer = CanonicalEventLogWriter(events_path)
        
        events = [
            CanonicalEvent(
                session_id="test_session",
                mint="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
                slot=100,
                block_time=1640000000,
                signature="sig1",
                event_type="SWAP",
                actors=["wallet1", "wallet2"]
            ),
            CanonicalEvent(
                session_id="test_session",
                mint="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
                slot=101,
                block_time=1640000100,
                signature="sig2",
                event_type="SWAP",
                actors=["wallet3"]
            )
        ]
        
        with writer:
            for event in events:
                writer.append(event)
        
        # Run replay
        audit_gate = AuditGate()
        replay_runner = ReplayRunner(
            events_log_path=events_path,
            replay_alerts_path=replay_alerts_path,
            audit_gate=audit_gate
        )
        
        transitions = replay_runner.run(token_name="TestToken")
        
        # Should have produced transitions
        assert len(transitions) >= 1
        
        print("✓ ReplayRunner selftest PASSED")
        
    finally:
        shutil.rmtree(tmpdir)


if __name__ == "__main__":
    selftest_replay()
