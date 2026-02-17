"""
PANDA LIVE Phase 3 Test

Demonstrates token state machine with 9 states and episode management.
"""

import time
import sys
sys.path.insert(0, '/home/claude')

from panda_live.models import TokenState
from panda_live.core import (
    FlowIngestion,
    TimeWindowManager,
    WhaleDetector,
    WalletSignalsDetector,
    TokenStateMachine
)
from panda_live.logging import SessionLogger


def process_flow_with_state_machine(
    flow_data: tuple,
    base_time: int,
    token_state: TokenState,
    window_manager: TimeWindowManager,
    whale_detector: WhaleDetector,
    signals_detector: WalletSignalsDetector,
    state_machine: TokenStateMachine,
    logger: SessionLogger
):
    """Process a single flow and update state machine"""
    wallet_addr, timestamp, direction, amount, sig = flow_data
    
    print(f"[{timestamp - base_time}s] Processing {wallet_addr} {direction} {amount} SOL")
    
    # Normalize flow
    flow = FlowIngestion.normalize_flow(wallet_addr, timestamp, direction, amount, sig, token_state.ca)
    if not flow:
        return
    
    # Update windows
    window_manager.process_flow(flow)
    
    # Detect whales
    whales = whale_detector.detect_whales(flow)
    
    for whale in whales:
        print(f"  🐋 {whale.event_type} ({whale.amount_sol:.1f} SOL)")
        
        # Add to token history
        token_state.add_whale_event(whale.timestamp, whale.wallet, whale.event_type)
        
        # Check for re-ignition
        reignition = state_machine.handle_new_whale(whale.timestamp)
        if reignition:
            print(f"  ⚡ STATE: {reignition.from_state} → {reignition.to_state}")
            print(f"     Trigger: {reignition.trigger}")
            logger.log_state_transition(reignition)
        
        # Detect signals
        signal_event = signals_detector.detect_signals(whale, whale.timestamp)
        if signal_event:
            print(f"  📊 Signals: {signal_event.signals}")
            logger.log_wallet_signal(signal_event)
    
    # Evaluate state transitions
    transition = state_machine.evaluate_transition(timestamp)
    if transition:
        print(f"  ⚡ STATE: {transition.from_state} → {transition.to_state}")
        print(f"     Trigger: {transition.trigger}")
        if transition.trigger_details:
            print(f"     Details: {transition.trigger_details}")
        logger.log_state_transition(transition)
    
    print()


def test_phase3():
    """Test Phase 3 token state machine"""
    
    print("=" * 80)
    print("PANDA LIVE PHASE 3 TEST - TOKEN STATE MACHINE")
    print("=" * 80)
    print()
    
    # Initialize
    token_ca = "TestCA_Phase3"
    token_state = TokenState(token_ca)
    
    window_manager = TimeWindowManager(token_state)
    whale_detector = WhaleDetector(token_state)
    signals_detector = WalletSignalsDetector(token_state)
    state_machine = TokenStateMachine(token_state)
    
    logger = SessionLogger(
        token_ca=token_ca,
        log_level="INTELLIGENCE_ONLY",
        output_dir="logs"
    )
    
    logger.log_session_start({"phase": 3, "test": "state_machine"})
    
    base_time = int(time.time())
    
    print("EPISODE 1: QUIET → IGNITION → COORDINATION → EARLY_PHASE")
    print("=" * 80)
    
    # Scenario: Build up to coordination spike
    flows = [
        ("whale1", base_time, "buy", 12.0, "sig1"),  # First whale → IGNITION
        ("whale2", base_time + 20, "buy", 11.0, "sig2"),
        ("whale3", base_time + 40, "buy", 10.5, "sig3"),  # 3rd whale → COORDINATION_SPIKE
    ]
    
    for flow_data in flows:
        process_flow_with_state_machine(
            flow_data, base_time, token_state,
            window_manager, whale_detector, signals_detector,
            state_machine, logger
        )
    
    # Wait for sustained activity → EARLY_PHASE
    print(f"[Simulating 130s passage...]")
    sustained_time = base_time + 170
    transition = state_machine.evaluate_transition(sustained_time)
    if transition:
        print(f"  ⚡ STATE: {transition.from_state} → {transition.to_state}")
        print(f"     Trigger: {transition.trigger}")
        logger.log_state_transition(transition)
    print()
    
    print("TESTING: PERSISTENCE → EXPANSION → PRESSURE_PEAKING")
    print("=" * 80)
    
    # Add persistent wallets
    persist_flows = [
        ("whale1", base_time + 180, "buy", 8.0, "sig1b"),  # Re-appears (persistent)
        ("whale2", base_time + 200, "buy", 9.0, "sig2b"),  # Re-appears (persistent)
    ]
    
    for flow_data in persist_flows:
        process_flow_with_state_machine(
            flow_data, base_time, token_state,
            window_manager, whale_detector, signals_detector,
            state_machine, logger
        )
    
    # Transition to PERSISTENCE_CONFIRMED
    transition = state_machine.evaluate_transition(base_time + 200)
    if transition:
        print(f"  ⚡ STATE: {transition.from_state} → {transition.to_state}")
        print(f"     Trigger: {transition.trigger}")
        logger.log_state_transition(transition)
    print()
    
    # Add new non-early whales → EXPANSION
    expansion_flows = [
        ("whale_new_1", base_time + 400, "buy", 11.0, "sig_n1"),
        ("whale_new_2", base_time + 420, "buy", 12.0, "sig_n2"),
    ]
    
    for flow_data in expansion_flows:
        process_flow_with_state_machine(
            flow_data, base_time, token_state,
            window_manager, whale_detector, signals_detector,
            state_machine, logger
        )
    
    # Burst of whales → PRESSURE_PEAKING
    print("[Creating whale burst for pressure peaking...]")
    peaking_time = base_time + 500
    peaking_flows = [
        ("whale_peak_1", peaking_time, "buy", 10.0, "sig_p1"),
        ("whale_peak_2", peaking_time + 10, "buy", 10.5, "sig_p2"),
        ("whale_peak_3", peaking_time + 20, "buy", 11.0, "sig_p3"),
    ]
    
    for flow_data in peaking_flows:
        process_flow_with_state_machine(
            flow_data, base_time, token_state,
            window_manager, whale_detector, signals_detector,
            state_machine, logger
        )
    
    print("TESTING: EXHAUSTION → DISSIPATION → QUIET")
    print("=" * 80)
    
    # Simulate time passing for exhaustion
    print("[Simulating silence for exhaustion detection...]")
    exhaustion_time = base_time + 800
    
    # Check exhaustion
    transition = state_machine.evaluate_transition(exhaustion_time)
    if transition:
        print(f"  ⚡ STATE: {transition.from_state} → {transition.to_state}")
        print(f"     Trigger: {transition.trigger}")
        logger.log_state_transition(transition)
    else:
        print("  (Exhaustion conditions not met)")
    print()
    
    # Simulate more silence → DISSIPATION → QUIET
    print("[Simulating 10+ min silence for episode end...]")
    quiet_time = base_time + 1400  # 10+ min after last activity
    
    transition = state_machine.evaluate_transition(quiet_time)
    if transition:
        print(f"  ⚡ STATE: {transition.from_state} → {transition.to_state}")
        print(f"     Trigger: {transition.trigger}")
        logger.log_state_transition(transition)
    print()
    
    # Summary
    print("=" * 80)
    print("PHASE 3 SUMMARY")
    print("=" * 80)
    print(f"Token: {token_state.ca}")
    print(f"Final State: {token_state.current_state}")
    print(f"Episode ID: {token_state.episode_id}")
    print(f"Total State Transitions: {len(token_state.state_history)}")
    print()
    
    print("State Transition History:")
    for timestamp, from_state, to_state, trigger in token_state.state_history:
        rel_time = timestamp - base_time
        print(f"  [{rel_time}s] {from_state} → {to_state} ({trigger})")
    
    logger.log_session_end(
        reason="test_complete",
        final_state=token_state.current_state,
        episode_id=token_state.episode_id
    )
    logger.close()
    
    print()
    print(f"Session log: {logger.log_path}")
    print("=" * 80)


if __name__ == "__main__":
    test_phase3()
