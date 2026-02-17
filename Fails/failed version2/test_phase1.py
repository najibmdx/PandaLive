"""
PANDA LIVE Phase 1 Test

Demonstrates core primitives:
- Flow ingestion
- Time window management
- Whale detection with latched emission
"""

import time
import sys
sys.path.insert(0, '/home/claude')

from panda_live.models import TokenState, FlowEvent
from panda_live.core import FlowIngestion, TimeWindowManager, WhaleDetector
from panda_live.logging import SessionLogger


def test_phase1():
    """Test Phase 1 core primitives"""
    
    print("=" * 80)
    print("PANDA LIVE PHASE 1 TEST")
    print("=" * 80)
    print()
    
    # Initialize token state
    token_ca = "BxK7test3mF9"
    token_state = TokenState(token_ca)
    
    # Initialize managers
    window_manager = TimeWindowManager(token_state)
    whale_detector = WhaleDetector(token_state)
    
    # Initialize logger
    logger = SessionLogger(
        token_ca=token_ca,
        log_level="FULL",
        output_dir="logs"
    )
    
    # Log session start
    config = {
        "whale_single_tx": 10.0,
        "whale_cum_5min": 25.0,
        "whale_cum_15min": 50.0
    }
    logger.log_session_start(config)
    
    print("Test Scenario: Simulated flow events with whale detection")
    print()
    
    # Base timestamp
    base_time = int(time.time())
    
    # Test flows
    test_flows = [
        # Wallet 1: Single large TX (should trigger WHALE_TX)
        ("wallet1", base_time, "buy", 12.5, "sig1"),
        
        # Wallet 2: Multiple small flows building to cumulative threshold
        ("wallet2", base_time + 10, "buy", 8.0, "sig2a"),
        ("wallet2", base_time + 60, "buy", 9.0, "sig2b"),
        ("wallet2", base_time + 120, "buy", 10.0, "sig2c"),  # Should trigger WHALE_CUM_5M (27 SOL total)
        
        # Wallet 3: Building to 15min threshold
        ("wallet3", base_time + 30, "buy", 15.0, "sig3a"),
        ("wallet3", base_time + 180, "buy", 18.0, "sig3b"),
        ("wallet3", base_time + 360, "buy", 20.0, "sig3c"),  # Should trigger WHALE_CUM_15M (53 SOL total)
        
        # Wallet 1: Another large TX (should NOT trigger - latched)
        ("wallet1", base_time + 200, "buy", 15.0, "sig1b"),
    ]
    
    # Process flows
    for wallet_addr, timestamp, direction, amount, sig in test_flows:
        print(f"[{timestamp - base_time}s] Processing: {wallet_addr} {direction} {amount} SOL")
        
        # Normalize flow
        flow = FlowIngestion.normalize_flow(
            wallet=wallet_addr,
            timestamp=timestamp,
            direction=direction,
            amount_sol=amount,
            signature=sig,
            token_ca=token_ca
        )
        
        if not flow:
            print("  ❌ Invalid flow")
            continue
        
        # Set t0 on first flow
        if token_state.t0 == 0:
            token_state.set_t0(timestamp)
            print(f"  ✅ Token birth set: t0={timestamp}")
        
        # Log flow
        logger.log_flow(flow)
        
        # Update time windows
        window_manager.process_flow(flow)
        
        # Get wallet state
        wallet = token_state.get_or_create_wallet(wallet_addr)
        print(f"     Cumulative 5min: {wallet.cumulative_5min:.1f} SOL")
        print(f"     Cumulative 15min: {wallet.cumulative_15min:.1f} SOL")
        
        # Detect whales
        whale_events = whale_detector.detect_whales(flow)
        
        for whale in whale_events:
            print(f"  🐋 WHALE DETECTED: {whale.event_type} ({whale.amount_sol:.1f} SOL >= {whale.threshold:.0f} SOL)")
            
            # Add to token history
            token_state.add_whale_event(whale.timestamp, whale.wallet, whale.event_type)
            
            # Log whale event
            logger.log_whale_detection(whale)
        
        print()
    
    # Summary
    print("=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"Token CA: {token_state.ca}")
    print(f"Token birth (t0): {token_state.t0}")
    print(f"Active wallets: {token_state.get_active_wallet_count()}")
    print(f"Total whale events: {len(token_state.whale_event_history)}")
    print()
    
    print("Whale Events:")
    for timestamp, wallet, event_type in token_state.whale_event_history:
        relative_time = timestamp - base_time
        print(f"  [{relative_time}s] {wallet} - {event_type}")
    
    print()
    print("Per-Wallet State:")
    for wallet_addr, wallet in token_state.active_wallets.items():
        print(f"  {wallet_addr}:")
        print(f"    First seen: {wallet.first_seen - base_time}s")
        print(f"    Last seen: {wallet.last_seen - base_time}s")
        print(f"    Cumulative 5min: {wallet.cumulative_5min:.1f} SOL")
        print(f"    Cumulative 15min: {wallet.cumulative_15min:.1f} SOL")
        print(f"    Whale TX triggered: {wallet.whale_tx_triggered}")
        print(f"    Whale 5M triggered: {wallet.whale_5m_triggered}")
        print(f"    Whale 15M triggered: {wallet.whale_15m_triggered}")
    
    # Log session end
    logger.log_session_end(
        reason="test_complete",
        final_state=token_state.current_state,
        episode_id=token_state.episode_id
    )
    logger.close()
    
    print()
    print(f"Session log saved: {logger.log_path}")
    print("=" * 80)


if __name__ == "__main__":
    test_phase1()
