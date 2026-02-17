"""
PANDA LIVE Phase 2 Test

Demonstrates wallet signals detection:
- TIMING (early detection)
- COORDINATION (3+ wallets in 60s)
- PERSISTENCE (2+ minute buckets)
- EXHAUSTION (60% early silent + no replacement)
"""

import time
import sys
sys.path.insert(0, '/home/claude')

from panda_live.models import TokenState, FlowEvent
from panda_live.core import (
    FlowIngestion,
    TimeWindowManager,
    WhaleDetector,
    WalletSignalsDetector
)
from panda_live.logging import SessionLogger


def test_phase2():
    """Test Phase 2 wallet signals"""
    
    print("=" * 80)
    print("PANDA LIVE PHASE 2 TEST - WALLET SIGNALS")
    print("=" * 80)
    print()
    
    # Initialize
    token_ca = "TestCA_Phase2"
    token_state = TokenState(token_ca)
    
    window_manager = TimeWindowManager(token_state)
    whale_detector = WhaleDetector(token_state)
    signals_detector = WalletSignalsDetector(token_state)
    
    logger = SessionLogger(
        token_ca=token_ca,
        log_level="INTELLIGENCE_ONLY",
        output_dir="logs"
    )
    
    config = {
        "phase": 2,
        "signals": ["TIMING", "COORDINATION", "PERSISTENCE", "EXHAUSTION"]
    }
    logger.log_session_start(config)
    
    base_time = int(time.time())
    
    print("TEST SCENARIOS:")
    print()
    
    # SCENARIO 1: Early timing detection
    print("=" * 80)
    print("SCENARIO 1: TIMING SIGNAL (Early Detection)")
    print("=" * 80)
    
    early_flows = [
        ("wallet_early_1", base_time, "buy", 12.0, "sig_e1"),
        ("wallet_early_2", base_time + 30, "buy", 11.0, "sig_e2"),
        ("wallet_early_3", base_time + 120, "buy", 10.5, "sig_e3"),  # Still early (120s < 300s)
        ("wallet_late_1", base_time + 400, "buy", 13.0, "sig_l1"),  # Not early (400s > 300s)
    ]
    
    for wallet_addr, timestamp, direction, amount, sig in early_flows:
        flow = FlowIngestion.normalize_flow(wallet_addr, timestamp, direction, amount, sig, token_ca)
        window_manager.process_flow(flow)
        
        whales = whale_detector.detect_whales(flow)
        for whale in whales:
            token_state.add_whale_event(whale.timestamp, whale.wallet, whale.event_type)
            
            # Detect signals
            signal_event = signals_detector.detect_signals(whale, timestamp)
            if signal_event:
                print(f"[{timestamp - base_time}s] {wallet_addr}:")
                print(f"  Signals: {signal_event.signals}")
                print(f"  Details: {signal_event.details}")
                logger.log_wallet_signal(signal_event)
    
    print(f"\nEarly wallets: {len(token_state.early_wallets)}")
    print(f"Early wallet addresses: {[w[:10] for w in token_state.early_wallets]}")
    print()
    
    # SCENARIO 2: Coordination detection
    print("=" * 80)
    print("SCENARIO 2: COORDINATION SIGNAL (3+ wallets in 60s)")
    print("=" * 80)
    
    coord_base = base_time + 500
    coord_flows = [
        ("wallet_coord_1", coord_base, "buy", 10.2, "sig_c1"),
        ("wallet_coord_2", coord_base + 20, "buy", 11.5, "sig_c2"),
        ("wallet_coord_3", coord_base + 45, "buy", 12.8, "sig_c3"),  # 3rd wallet within 60s
    ]
    
    for wallet_addr, timestamp, direction, amount, sig in coord_flows:
        flow = FlowIngestion.normalize_flow(wallet_addr, timestamp, direction, amount, sig, token_ca)
        window_manager.process_flow(flow)
        
        whales = whale_detector.detect_whales(flow)
        for whale in whales:
            token_state.add_whale_event(whale.timestamp, whale.wallet, whale.event_type)
            
            signal_event = signals_detector.detect_signals(whale, timestamp)
            if signal_event:
                print(f"[{timestamp - base_time}s] {wallet_addr}:")
                print(f"  Signals: {signal_event.signals}")
                if "COORDINATION" in signal_event.signals:
                    print(f"  Coordinated with: {signal_event.details.get('coordinated_with', [])}")
                logger.log_wallet_signal(signal_event)
    
    print()
    
    # SCENARIO 3: Persistence detection
    print("=" * 80)
    print("SCENARIO 3: PERSISTENCE SIGNAL (2+ appearances within 5min)")
    print("=" * 80)
    
    persist_base = base_time + 600
    persist_flows = [
        # wallet_persist appears in 3 different minute buckets
        ("wallet_persist", persist_base, "buy", 8.0, "sig_p1"),
        ("wallet_persist", persist_base + 70, "buy", 9.0, "sig_p2"),  # Different minute bucket
        ("wallet_persist", persist_base + 150, "buy", 10.0, "sig_p3"),  # Triggers cumulative + persistence
    ]
    
    for wallet_addr, timestamp, direction, amount, sig in persist_flows:
        flow = FlowIngestion.normalize_flow(wallet_addr, timestamp, direction, amount, sig, token_ca)
        window_manager.process_flow(flow)
        
        whales = whale_detector.detect_whales(flow)
        for whale in whales:
            token_state.add_whale_event(whale.timestamp, whale.wallet, whale.event_type)
            
            signal_event = signals_detector.detect_signals(whale, timestamp)
            if signal_event:
                print(f"[{timestamp - base_time}s] {wallet_addr}:")
                print(f"  Signals: {signal_event.signals}")
                if "PERSISTENCE" in signal_event.signals:
                    print(f"  Appearances: {signal_event.details.get('appearances')}")
                    print(f"  Time span: {signal_event.details.get('time_span_seconds')}s")
                logger.log_wallet_signal(signal_event)
    
    print()
    
    # SCENARIO 4: Exhaustion detection
    print("=" * 80)
    print("SCENARIO 4: EXHAUSTION SIGNAL (60% early silent, no replacement)")
    print("=" * 80)
    
    # Simulate time passing (early wallets go silent)
    exhaustion_check_time = base_time + 800
    
    print(f"Checking exhaustion at t={exhaustion_check_time - base_time}s...")
    print(f"Early wallets: {len(token_state.early_wallets)}")
    
    # Check each early wallet's silence
    for early_addr in token_state.early_wallets:
        wallet = token_state.active_wallets.get(early_addr)
        if wallet:
            silence = exhaustion_check_time - wallet.last_seen
            is_silent = silence >= 180
            print(f"  {early_addr[:10]}: last_seen={wallet.last_seen - base_time}s, silence={silence}s, silent={is_silent}")
    
    # Detect exhaustion
    exhaustion_detected = signals_detector.detect_exhaustion(exhaustion_check_time)
    print(f"\nExhaustion detected: {exhaustion_detected}")
    
    if exhaustion_detected:
        disengagement_pct = token_state.get_disengagement_percentage(exhaustion_check_time, 180)
        print(f"  Disengagement: {disengagement_pct:.1%}")
        
        recent_whales = token_state.get_recent_whale_wallets(300, exhaustion_check_time)
        replacement = recent_whales - token_state.early_wallets
        print(f"  Replacement whales: {len(replacement)}")
    
    print()
    
    # Summary
    print("=" * 80)
    print("PHASE 2 SUMMARY")
    print("=" * 80)
    print(f"Token CA: {token_state.ca}")
    print(f"Episode ID: {token_state.episode_id}")
    print(f"Active wallets: {token_state.get_active_wallet_count()}")
    print(f"Early wallets: {token_state.get_early_wallet_count()}")
    print(f"Persistent wallets: {token_state.get_persistent_wallet_count()}")
    print()
    
    print("Signal Breakdown:")
    timing_count = sum(1 for w in token_state.active_wallets.values() if w.is_early)
    coord_count = sum(1 for w in token_state.active_wallets.values() if w.is_coordinated)
    persist_count = sum(1 for w in token_state.active_wallets.values() if w.is_persistent)
    
    print(f"  TIMING: {timing_count} wallets")
    print(f"  COORDINATION: {coord_count} wallets")
    print(f"  PERSISTENCE: {persist_count} wallets")
    print(f"  EXHAUSTION: {'YES' if exhaustion_detected else 'NO'}")
    
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
    test_phase2()
