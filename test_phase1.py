"""Phase 1 verification test for PANDA LIVE."""

import sys
import time
import tempfile
import json
from pathlib import Path

# Ensure imports work
from panda_live.models.events import FlowEvent, WhaleEvent
from panda_live.models.wallet_state import WalletState
from panda_live.models.token_state import TokenState
from panda_live.core.flow_ingestion import normalize_flow, FlowValidationError
from panda_live.core.time_windows import TimeWindowManager
from panda_live.core.whale_detection import WhaleDetector
from panda_live.logging.session_logger import SessionLogger
from panda_live.logging.log_replay import replay_session
from panda_live.config.wallet_names_loader import load_wallet_names
from panda_live.config.thresholds import (
    WHALE_SINGLE_TX_SOL, WHALE_CUM_5MIN_SOL, WHALE_CUM_15MIN_SOL,
)

WALLET = "A" * 44
TOKEN = "B" * 44
NOW = int(time.time())


def test_flow_ingestion():
    print("=== Flow Ingestion ===")
    flow = normalize_flow({
        "wallet": WALLET,
        "timestamp": NOW,
        "direction": "buy",
        "amount_sol": 12.0,
        "signature": "sig1",
        "token_ca": TOKEN,
    })
    assert flow.wallet == WALLET
    assert flow.amount_sol == 12.0
    print("  normalize_flow: OK")

    # Invalid wallet length
    try:
        normalize_flow({
            "wallet": "short",
            "timestamp": NOW,
            "direction": "buy",
            "amount_sol": 1.0,
            "signature": "sig",
            "token_ca": TOKEN,
        })
        assert False, "Should have raised"
    except FlowValidationError:
        print("  invalid wallet rejected: OK")

    # Invalid amount
    try:
        normalize_flow({
            "wallet": WALLET,
            "timestamp": NOW,
            "direction": "buy",
            "amount_sol": -1.0,
            "signature": "sig",
            "token_ca": TOKEN,
        })
        assert False, "Should have raised"
    except FlowValidationError:
        print("  negative amount rejected: OK")


def test_time_windows():
    print("=== Time Windows ===")
    mgr = TimeWindowManager()
    ws = WalletState(address=WALLET)

    f1 = FlowEvent(WALLET, NOW, "buy", 12.0, "sig1", TOKEN)
    f2 = FlowEvent(WALLET, NOW + 50, "buy", 8.0, "sig2", TOKEN)
    f3 = FlowEvent(WALLET, NOW + 100, "buy", 10.0, "sig3", TOKEN)

    mgr.add_flow(ws, f1)
    assert ws.cumulative_5min == 12.0
    assert ws.cumulative_15min == 12.0
    print(f"  After f1: cum5={ws.cumulative_5min}, cum15={ws.cumulative_15min}: OK")

    mgr.add_flow(ws, f2)
    assert ws.cumulative_5min == 20.0
    print(f"  After f2: cum5={ws.cumulative_5min}: OK")

    mgr.add_flow(ws, f3)
    assert ws.cumulative_5min == 30.0
    assert ws.cumulative_15min == 30.0
    print(f"  After f3: cum5={ws.cumulative_5min}, cum15={ws.cumulative_15min}: OK")

    # Test expiry: add flow 6 minutes later, f1 should drop from 5min window
    f4 = FlowEvent(WALLET, NOW + 361, "buy", 1.0, "sig4", TOKEN)
    mgr.add_flow(ws, f4)
    # At NOW+361, cutoff_5 = NOW+61, so f1(NOW) and f2(NOW+50) are expired from 5min
    # Only f3(NOW+100)=10 + f4=1 remain = 11.0
    assert ws.cumulative_5min == 11.0, f"Expected 11.0, got {ws.cumulative_5min}"
    assert ws.cumulative_15min == 31.0
    print(f"  After f4 (expiry): cum5={ws.cumulative_5min}, cum15={ws.cumulative_15min}: OK")


def test_whale_detection():
    print("=== Whale Detection ===")
    mgr = TimeWindowManager()
    detector = WhaleDetector()
    ws = WalletState(address=WALLET)

    # Flow 1: 12 SOL buy at t=100
    f1 = FlowEvent(WALLET, NOW, "buy", 12.0, "sig1", TOKEN)
    mgr.add_flow(ws, f1)
    events = detector.check_thresholds(ws, f1)
    assert len(events) == 1
    assert events[0].event_type == "WHALE_TX"
    assert events[0].amount_sol == 12.0
    print(f"  f1 -> WHALE_TX fired: OK")

    # Flow 2: 8 SOL buy at t=150
    f2 = FlowEvent(WALLET, NOW + 50, "buy", 8.0, "sig2", TOKEN)
    mgr.add_flow(ws, f2)
    events = detector.check_thresholds(ws, f2)
    assert len(events) == 0, f"Expected 0 events, got {[e.event_type for e in events]}"
    print(f"  f2 -> no events (cum5={ws.cumulative_5min} < 25): OK")

    # Flow 3: 10 SOL buy at t=200
    f3 = FlowEvent(WALLET, NOW + 100, "buy", 10.0, "sig3", TOKEN)
    mgr.add_flow(ws, f3)
    events = detector.check_thresholds(ws, f3)
    assert len(events) == 1
    assert events[0].event_type == "WHALE_CUM_5M"
    print(f"  f3 -> WHALE_CUM_5M fired (cum5={ws.cumulative_5min}): OK")

    # Verify latching: WHALE_TX should NOT fire again for 10 SOL tx
    assert ws.whale_tx_fired is True
    assert ws.whale_cum_5m_fired is True
    print(f"  Latched flags: whale_tx={ws.whale_tx_fired}, whale_cum_5m={ws.whale_cum_5m_fired}: OK")


def test_session_logging():
    print("=== Session Logging ===")
    with tempfile.TemporaryDirectory() as tmpdir:
        logger = SessionLogger(token_ca=TOKEN, log_level="FULL", output_dir=tmpdir)
        logger.log_session_start({"test": True})

        f1 = FlowEvent(WALLET, NOW, "buy", 12.0, "sig1", TOKEN)
        logger.log_flow(f1)

        we = WhaleEvent(WALLET, NOW, "WHALE_TX", 12.0, 10.0, TOKEN)
        logger.log_whale_event(we)

        logger.log_session_end("test_complete")

        # Replay and verify
        events = replay_session(str(logger.filepath))
        assert len(events) == 4
        assert events[0]["event_type"] == "SESSION_START"
        assert events[1]["event_type"] == "FLOW"
        assert events[2]["event_type"] == "WHALE_TX"
        assert events[3]["event_type"] == "SESSION_END"
        print(f"  FULL logging: {len(events)} events written/replayed: OK")

    # Test INTELLIGENCE_ONLY mode
    with tempfile.TemporaryDirectory() as tmpdir:
        logger = SessionLogger(token_ca=TOKEN, log_level="INTELLIGENCE_ONLY", output_dir=tmpdir)
        logger.log_session_start({"test": True})
        logger.log_flow(f1)  # Should be skipped
        logger.log_whale_event(we)  # Should be skipped
        logger.log_session_end("test_complete")

        events = replay_session(str(logger.filepath))
        assert len(events) == 2, f"Expected 2 events, got {len(events)}"
        assert events[0]["event_type"] == "SESSION_START"
        assert events[1]["event_type"] == "SESSION_END"
        print(f"  INTELLIGENCE_ONLY logging: {len(events)} events (flow/whale skipped): OK")


def test_wallet_names():
    print("=== Wallet Names ===")
    # Missing file returns empty dict
    result = load_wallet_names("nonexistent.json")
    assert result == {}
    print("  Missing file -> empty dict: OK")


def test_thresholds():
    print("=== Thresholds ===")
    assert WHALE_SINGLE_TX_SOL == 10
    assert WHALE_CUM_5MIN_SOL == 25
    assert WHALE_CUM_15MIN_SOL == 50
    print("  All thresholds correct: OK")


if __name__ == "__main__":
    test_thresholds()
    test_flow_ingestion()
    test_time_windows()
    test_whale_detection()
    test_session_logging()
    test_wallet_names()
    print("\n*** ALL PHASE 1 TESTS PASSED ***")
