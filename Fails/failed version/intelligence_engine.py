"""
M5: Incremental Primitive Updater (v4 logic scaffolding)
M6: Wallet Intelligence Engine (scaffolding)
M7: Token Intelligence Compressor (scaffolding)

These modules provide the structure for v4 logic integration.
Default behavior: safe, deterministic, TOKEN_QUIET only.
"""

from typing import List, Dict, Any, Set, Optional
from event_log import CanonicalEvent
from intelligence_output import IntelligenceTransition
from collections import defaultdict


class V4Primitives:
    """
    Container for v4 primitive state.
    
    This is the "proof layer" - incrementally updated from canonical events.
    In full implementation, this would contain the actual v4 tables/metrics.
    """
    
    def __init__(self, mint: str, session_id: str):
        self.mint = mint
        self.session_id = session_id
        
        # Primitive state (scaffolding - extend with actual v4 primitives)
        self.wallets_seen: Set[str] = set()
        self.wallet_tx_count: Dict[str, int] = defaultdict(int)
        self.wallet_first_seen: Dict[str, int] = {}
        self.wallet_last_seen: Dict[str, int] = {}
        
        self.swaps_count = 0
        self.transfers_count = 0
        
        # Time window tracking
        self.earliest_block_time: Optional[int] = None
        self.latest_block_time: Optional[int] = None
    
    def get_wallet_tx_count(self, wallet: str) -> int:
        """Get transaction count for a wallet."""
        return self.wallet_tx_count.get(wallet, 0)
    
    def get_wallet_first_seen(self, wallet: str) -> Optional[int]:
        """Get first seen time for a wallet."""
        return self.wallet_first_seen.get(wallet)
    
    def is_early_wallet(self, wallet: str, threshold_seconds: int = 300) -> bool:
        """
        Check if wallet is an early participant.
        
        Args:
            wallet: Wallet address
            threshold_seconds: Seconds from earliest to consider "early"
        
        Returns:
            True if wallet is early
        """
        if not self.earliest_block_time:
            return False
        
        first_seen = self.wallet_first_seen.get(wallet)
        if not first_seen:
            return False
        
        return (first_seen - self.earliest_block_time) <= threshold_seconds


class IncrementalPrimitiveUpdater:
    """
    Incrementally updates v4 primitives from canonical events.
    
    This is the streaming version of v4 batch processing.
    """
    
    def __init__(self, primitives: V4Primitives):
        self.primitives = primitives
    
    def update(self, event: CanonicalEvent):
        """
        Update primitives from a single canonical event.
        
        Args:
            event: Canonical event to process
        """
        # Update time window
        if self.primitives.earliest_block_time is None:
            self.primitives.earliest_block_time = event.block_time
        else:
            self.primitives.earliest_block_time = min(
                self.primitives.earliest_block_time,
                event.block_time
            )
        
        if self.primitives.latest_block_time is None:
            self.primitives.latest_block_time = event.block_time
        else:
            self.primitives.latest_block_time = max(
                self.primitives.latest_block_time,
                event.block_time
            )
        
        # Update wallet tracking
        for wallet in event.actors:
            if wallet:
                self.primitives.wallets_seen.add(wallet)
                self.primitives.wallet_tx_count[wallet] += 1
                
                if wallet not in self.primitives.wallet_first_seen:
                    self.primitives.wallet_first_seen[wallet] = event.block_time
                
                self.primitives.wallet_last_seen[wallet] = event.block_time
        
        # Update event type counters
        if event.event_type == "SWAP":
            self.primitives.swaps_count += 1
        elif event.event_type in ("TOKEN_TRANSFER", "SOL_TRANSFER"):
            self.primitives.transfers_count += 1
    
    def update_batch(self, events: List[CanonicalEvent]):
        """Update primitives from multiple events."""
        for event in events:
            self.update(event)


class WalletIntelligenceEngine:
    """
    Emits wallet intelligence transitions based on primitives.
    
    SCAFFOLDING: Default behavior is safe (no transitions).
    Extend this with actual v4 wallet detection logic.
    """
    
    # Wallet transition types (locked concepts)
    WALLET_DEVIATION_ENTER = "WALLET_DEVIATION_ENTER"
    WALLET_COORDINATION_ENTER = "WALLET_COORDINATION_ENTER"
    WALLET_PERSISTENCE_ENTER = "WALLET_PERSISTENCE_ENTER"
    WALLET_TIMING_EARLY_ENTER = "WALLET_TIMING_EARLY_ENTER"
    WALLET_EXHAUSTION_ENTER = "WALLET_EXHAUSTION_ENTER"
    
    def __init__(self, primitives: V4Primitives):
        self.primitives = primitives
        
        # Track which wallets have already emitted which transitions (latching)
        self.wallet_transitions_emitted: Dict[str, Set[str]] = defaultdict(set)
    
    def check_transitions(
        self,
        event: CanonicalEvent,
        token_name: str = ""
    ) -> List[IntelligenceTransition]:
        """
        Check for wallet intelligence transitions triggered by this event.
        
        Args:
            event: The canonical event that just occurred
            token_name: Token name for display
        
        Returns:
            List of new intelligence transitions (latched, transition-only)
        """
        transitions = []
        
        # SCAFFOLDING: This is where actual v4 wallet detection logic plugs in.
        # Default safe behavior: only detect very basic patterns.
        
        for wallet in event.actors:
            if not wallet:
                continue
            
            # Example: Detect early timing (first 5 minutes)
            if self._should_emit_early_timing(wallet):
                transition_id = f"{wallet}:early:{event.block_time}"
                
                transitions.append(
                    IntelligenceTransition(
                        session_id=self.primitives.session_id,
                        mint=self.primitives.mint,
                        token_name=token_name,
                        event_time=event.block_time,
                        entity_type=IntelligenceTransition.ENTITY_TYPE_WALLET,
                        entity_address=wallet,
                        entity_name="",
                        transition_type=self.WALLET_TIMING_EARLY_ENTER,
                        transition_id=transition_id,
                        supporting_refs=event.signature
                    )
                )
                
                # Mark as emitted (latch)
                self.wallet_transitions_emitted[wallet].add(self.WALLET_TIMING_EARLY_ENTER)
        
        return transitions
    
    def _should_emit_early_timing(self, wallet: str) -> bool:
        """Check if wallet should emit early timing transition."""
        # Don't re-emit
        if self.WALLET_TIMING_EARLY_ENTER in self.wallet_transitions_emitted[wallet]:
            return False
        
        # Check if wallet is early (within first 5 minutes)
        return self.primitives.is_early_wallet(wallet, threshold_seconds=300)
    
    # V4 INTEGRATION POINT: Add methods for other wallet intelligence types
    # - _should_emit_deviation
    # - _should_emit_coordination
    # - _should_emit_persistence
    # - _should_emit_exhaustion


class TokenIntelligenceCompressor:
    """
    Compresses wallet intelligence into exactly one token state.
    
    Token states (locked, exactly one active):
    1. TOKEN_QUIET
    2. TOKEN_IGNITION
    3. TOKEN_COORDINATION_SPIKE
    4. TOKEN_EARLY_PHASE
    5. TOKEN_PERSISTENCE_CONFIRMED
    6. TOKEN_PARTICIPATION_EXPANSION
    7. TOKEN_PRESSURE_PEAKING
    8. TOKEN_EXHAUSTION_DETECTED
    9. TOKEN_DISSIPATION
    
    SCAFFOLDING: Default is TOKEN_QUIET only.
    Extend with actual v4 compression mapping.
    """
    
    # Token states (locked)
    TOKEN_QUIET = "TOKEN_QUIET"
    TOKEN_IGNITION = "TOKEN_IGNITION"
    TOKEN_COORDINATION_SPIKE = "TOKEN_COORDINATION_SPIKE"
    TOKEN_EARLY_PHASE = "TOKEN_EARLY_PHASE"
    TOKEN_PERSISTENCE_CONFIRMED = "TOKEN_PERSISTENCE_CONFIRMED"
    TOKEN_PARTICIPATION_EXPANSION = "TOKEN_PARTICIPATION_EXPANSION"
    TOKEN_PRESSURE_PEAKING = "TOKEN_PRESSURE_PEAKING"
    TOKEN_EXHAUSTION_DETECTED = "TOKEN_EXHAUSTION_DETECTED"
    TOKEN_DISSIPATION = "TOKEN_DISSIPATION"
    
    def __init__(self, primitives: V4Primitives):
        self.primitives = primitives
        self.current_state = self.TOKEN_QUIET
        self.state_entered_at: Optional[int] = None
    
    def compress(
        self,
        wallet_transitions: List[IntelligenceTransition],
        event_time: int,
        token_name: str = ""
    ) -> Optional[IntelligenceTransition]:
        """
        Compress wallet transitions into token state transition.
        
        Args:
            wallet_transitions: New wallet transitions from this update
            event_time: Current event time
            token_name: Token name for display
        
        Returns:
            Token state transition if state changed, None otherwise
        """
        if not wallet_transitions:
            # No wallet intelligence → remain/stay in TOKEN_QUIET
            return None
        
        # SCAFFOLDING: This is where actual v4 compression mapping plugs in.
        # Default safe behavior: only transition to IGNITION if we see early wallets.
        
        new_state = self._compute_new_state(wallet_transitions)
        
        if new_state != self.current_state:
            # State transition occurred
            transition_type = f"{new_state}_ENTER"
            transition_id = f"{self.primitives.mint}:{new_state}:{event_time}"
            
            transition = IntelligenceTransition(
                session_id=self.primitives.session_id,
                mint=self.primitives.mint,
                token_name=token_name,
                event_time=event_time,
                entity_type=IntelligenceTransition.ENTITY_TYPE_TOKEN,
                entity_address=self.primitives.mint,
                entity_name=token_name,
                transition_type=transition_type,
                transition_id=transition_id,
                supporting_refs=f"{len(wallet_transitions)} wallet transitions"
            )
            
            self.current_state = new_state
            self.state_entered_at = event_time
            
            return transition
        
        return None
    
    def _compute_new_state(self, wallet_transitions: List[IntelligenceTransition]) -> str:
        """
        Compute new token state from wallet transitions.
        
        SCAFFOLDING: Minimal logic - extend with v4 mapping.
        """
        # If we have any early timing wallets, transition to IGNITION
        for transition in wallet_transitions:
            if transition.transition_type == WalletIntelligenceEngine.WALLET_TIMING_EARLY_ENTER:
                if self.current_state == self.TOKEN_QUIET:
                    return self.TOKEN_IGNITION
        
        # Otherwise, stay in current state
        return self.current_state
    
    # V4 INTEGRATION POINT: Add state transition logic
    # - Map wallet coordination → TOKEN_COORDINATION_SPIKE
    # - Map persistence → TOKEN_PERSISTENCE_CONFIRMED
    # - Map exhaustion → TOKEN_EXHAUSTION_DETECTED
    # etc.


def selftest_intelligence_engine():
    """Self-test for intelligence engine scaffolding."""
    
    # Create primitives
    primitives = V4Primitives(
        mint="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
        session_id="test_session"
    )
    
    # Create updater
    updater = IncrementalPrimitiveUpdater(primitives)
    
    # Create test event
    from event_log import CanonicalEvent
    
    event = CanonicalEvent(
        session_id="test_session",
        mint="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
        slot=12345,
        block_time=1640000000,
        signature="sig1",
        event_type="SWAP",
        actors=["wallet1", "wallet2"]
    )
    
    # Update primitives
    updater.update(event)
    
    assert "wallet1" in primitives.wallets_seen
    assert primitives.wallet_tx_count["wallet1"] == 1
    assert primitives.swaps_count == 1
    
    # Test wallet intelligence engine
    wallet_engine = WalletIntelligenceEngine(primitives)
    
    # Second event (early wallet)
    event2 = CanonicalEvent(
        session_id="test_session",
        mint="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
        slot=12346,
        block_time=1640000100,  # 100 seconds later
        signature="sig2",
        event_type="SWAP",
        actors=["wallet3"]
    )
    
    updater.update(event2)
    wallet_transitions = wallet_engine.check_transitions(event2, "TestToken")
    
    # Should emit early timing for wallet3
    assert len(wallet_transitions) == 1
    assert wallet_transitions[0].transition_type == WalletIntelligenceEngine.WALLET_TIMING_EARLY_ENTER
    
    # Test token compressor
    token_compressor = TokenIntelligenceCompressor(primitives)
    
    assert token_compressor.current_state == TokenIntelligenceCompressor.TOKEN_QUIET
    
    token_transition = token_compressor.compress(wallet_transitions, event2.block_time, "TestToken")
    
    # Should transition to IGNITION
    assert token_transition is not None
    assert token_transition.transition_type == "TOKEN_IGNITION_ENTER"
    assert token_compressor.current_state == TokenIntelligenceCompressor.TOKEN_IGNITION
    
    print("✓ IntelligenceEngine scaffolding selftest PASSED")


if __name__ == "__main__":
    selftest_intelligence_engine()
