"""Phase 2 verification test for PANDA LIVE - Wallet Signals."""

import tempfile

from panda_live.models.events import FlowEvent, WhaleEvent, WalletSignalEvent
from panda_live.models.wallet_state import WalletState
from panda_live.models.token_state import TokenState
from panda_live.core.time_windows import TimeWindowManager
from panda_live.core.whale_detection import WhaleDetector
from panda_live.core.wallet_signals import WalletSignalDetector
from panda_live.core.signal_aggregator import SignalAggregator
from panda_live.logging.session_logger import SessionLogger
from panda_live.logging.log_replay import replay_session

WALLET_A = "A" * 44
WALLET_B = "B" * 44
WALLET_C = "C" * 44
TOKEN = "T" * 44
T0 = 1000  # Token birth


def make_token_state() -> TokenState:
    ts = TokenState(ca=TOKEN, t0=T0)
    return ts


def make_wallet(addr: str, token_state: TokenState) -> WalletState:
    ws = WalletState(address=addr)
    token_state.active_wallets[addr] = ws
    return ws


def test_timing_signal():
    print("=== TIMING Signal ===")
    detector = WalletSignalDetector()
    ts = make_token_state()

    # Early wallet (100s after t0 = within 300s EARLY_WINDOW)
    ws_a = make_wallet(WALLET_A, ts)
    ws_a.first_seen = 1100
    assert detector.detect_timing(ws_a, ts) is True
    assert ws_a.is_early is True
    print(f"  Wallet A at t=1100 (delta=100s): early={ws_a.is_early}: OK")

    # Late wallet (600s after t0 = outside 300s EARLY_WINDOW)
    ws_late = make_wallet("L" * 44, ts)
    ws_late.first_seen = 1600
    assert detector.detect_timing(ws_late, ts) is False
    assert ws_late.is_early is False
    print(f"  Late wallet at t=1600 (delta=600s): early={ws_late.is_early}: OK")

    # Mid-flight (t0 is None)
    ts_mid = TokenState(ca=TOKEN, t0=None)
    ws_mid = WalletState(address=WALLET_A, first_seen=5000)
    assert detector.detect_timing(ws_mid, ts_mid) is True
    print(f"  Mid-flight wallet: early={ws_mid.is_early}: OK")


def test_coordination_signal():
    print("=== COORDINATION Signal ===")
    detector = WalletSignalDetector()

    # Wallet A whale at t=1100
    we_a = WhaleEvent(WALLET_A, 1100, "WHALE_TX", 12.0, 10.0, TOKEN)
    is_coord, wallets = detector.detect_coordination(we_a, 1100)
    assert is_coord is False
    print(f"  After wallet A: coordinated={is_coord}, wallets={len(wallets)}: OK")

    # Wallet B whale at t=1120
    we_b = WhaleEvent(WALLET_B, 1120, "WHALE_TX", 15.0, 10.0, TOKEN)
    is_coord, wallets = detector.detect_coordination(we_b, 1120)
    assert is_coord is False
    print(f"  After wallet B: coordinated={is_coord}, wallets={len(wallets)}: OK")

    # Wallet C whale at t=1140 (3 wallets within 40s < 60s window)
    we_c = WhaleEvent(WALLET_C, 1140, "WHALE_TX", 11.0, 10.0, TOKEN)
    is_coord, wallets = detector.detect_coordination(we_c, 1140)
    assert is_coord is True
    assert len(wallets) == 3
    assert WALLET_A in wallets and WALLET_B in wallets and WALLET_C in wallets
    print(f"  After wallet C: coordinated={is_coord}, wallets={wallets[:3]}...: OK")

    # Wallet D much later (outside window)
    we_d = WhaleEvent("D" * 44, 1300, "WHALE_TX", 10.0, 10.0, TOKEN)
    is_coord, wallets = detector.detect_coordination(we_d, 1300)
    # A, B, C should be expired (1300 - 60 = 1240, all < 1240)
    assert is_coord is False
    print(f"  Wallet D at t=1300 (outside window): coordinated={is_coord}: OK")


def test_persistence_signal():
    print("=== PERSISTENCE Signal ===")
    detector = WalletSignalDetector()

    # Wallet with only 1 bucket -> not persistent
    ws = WalletState(address=WALLET_A)
    ws.minute_buckets = {20}
    assert detector.detect_persistence(ws) is False
    print(f"  1 bucket: persistent={detector.detect_persistence(ws)}: OK")

    # Wallet with 2 buckets within gap -> persistent
    ws.minute_buckets = {20, 23}  # 3 min gap = 180s <= 300s
    assert detector.detect_persistence(ws) is True
    print(f"  2 buckets (gap=180s): persistent={detector.detect_persistence(ws)}: OK")

    # Wallet with buckets too far apart -> not persistent
    ws.minute_buckets = {20, 30}  # 10 min gap = 600s > 300s
    assert detector.detect_persistence(ws) is False
    print(f"  2 buckets (gap=600s): persistent={detector.detect_persistence(ws)}: OK")


def test_exhaustion_signal():
    print("=== EXHAUSTION Signal ===")
    detector = WalletSignalDetector()

    ts = make_token_state()
    ws_a = make_wallet(WALLET_A, ts)
    ws_b = make_wallet(WALLET_B, ts)
    ws_c = make_wallet(WALLET_C, ts)

    # Mark A, B, C as early
    ts.early_wallets = {WALLET_A, WALLET_B, WALLET_C}

    # A and B active recently, C active recently
    ws_a.last_seen = 1200
    ws_b.last_seen = 1200
    ws_c.last_seen = 1200

    # At t=1300 (100s since last seen < 180s threshold) -> no exhaustion
    is_exhausted, details = detector.detect_exhaustion(ts, 1300)
    assert is_exhausted is False
    print(f"  At t=1300 (all active): exhausted={is_exhausted}: OK")

    # At t=1700 (500s since last seen > 180s threshold for all 3)
    # But wallet C was active recently
    ws_c.last_seen = 1600
    is_exhausted, details = detector.detect_exhaustion(ts, 1700)
    # A silent (500s), B silent (500s), C active (100s)
    # 2/3 = 0.67 >= 0.60 threshold -> but need to check replacement
    # No non-early wallets active -> exhaustion!
    assert is_exhausted is True
    assert details["silent_early_count"] == 2
    assert details["total_early_count"] == 3
    assert details["replacement_count"] == 0
    assert details["disengagement_pct"] == 0.67
    print(f"  At t=1700 (A,B silent): exhausted={is_exhausted}, details={details}: OK")

    # Add a replacement whale -> no exhaustion
    ws_d = make_wallet("D" * 44, ts)
    ws_d.last_seen = 1650  # Active recently
    is_exhausted, details = detector.detect_exhaustion(ts, 1700)
    assert is_exhausted is False
    print(f"  With replacement wallet D: exhausted={is_exhausted}: OK")


def test_signal_aggregator():
    print("=== Signal Aggregator ===")
    aggregator = SignalAggregator()
    ts = make_token_state()

    # Setup: 3 wallets all appearing early with whale events
    mgr = TimeWindowManager()
    detector = WhaleDetector()

    ws_a = make_wallet(WALLET_A, ts)
    ws_b = make_wallet(WALLET_B, ts)
    ws_c = make_wallet(WALLET_C, ts)

    # Wallet A: whale at t=1100
    f_a = FlowEvent(WALLET_A, 1100, "buy", 12.0, "sig_a", TOKEN)
    mgr.add_flow(ws_a, f_a)
    whale_events_a = detector.check_thresholds(ws_a, f_a)
    assert len(whale_events_a) == 1

    sig_a = aggregator.process_whale_event(whale_events_a[0], ws_a, ts, 1100)
    assert "TIMING" in sig_a.signals
    assert WALLET_A in ts.early_wallets
    print(f"  Wallet A signals: {sig_a.signals}, details: {sig_a.details}: OK")

    # Wallet B: whale at t=1120
    f_b = FlowEvent(WALLET_B, 1120, "buy", 15.0, "sig_b", TOKEN)
    mgr.add_flow(ws_b, f_b)
    whale_events_b = detector.check_thresholds(ws_b, f_b)
    sig_b = aggregator.process_whale_event(whale_events_b[0], ws_b, ts, 1120)
    assert "TIMING" in sig_b.signals
    print(f"  Wallet B signals: {sig_b.signals}: OK")

    # Wallet C: whale at t=1140 -> should trigger COORDINATION
    f_c = FlowEvent(WALLET_C, 1140, "buy", 11.0, "sig_c", TOKEN)
    mgr.add_flow(ws_c, f_c)
    whale_events_c = detector.check_thresholds(ws_c, f_c)
    sig_c = aggregator.process_whale_event(whale_events_c[0], ws_c, ts, 1140)
    assert "TIMING" in sig_c.signals
    assert "COORDINATION" in sig_c.signals
    assert len(sig_c.details["coordination"]["sample_wallets"]) == 2  # A and B
    print(f"  Wallet C signals: {sig_c.signals}: OK")
    print(f"    coordination details: {sig_c.details['coordination']}")

    # Check exhaustion (token-level)
    exhaust = aggregator.check_exhaustion(ts, 1140)
    assert exhaust is None  # Too early, everyone active
    print(f"  Exhaustion at t=1140: None (all active): OK")


def test_signal_logging():
    print("=== Signal Logging ===")
    with tempfile.TemporaryDirectory() as tmpdir:
        logger = SessionLogger(token_ca=TOKEN, log_level="INTELLIGENCE_ONLY", output_dir=tmpdir)
        logger.log_session_start({"phase": 2})

        # Signals should be logged even in INTELLIGENCE_ONLY mode
        sig = WalletSignalEvent(
            wallet=WALLET_A,
            timestamp=1100,
            token_ca=TOKEN,
            signals=["TIMING", "COORDINATION"],
            details={
                "timing": {"is_early": True, "delta_seconds": 100},
                "coordination": {"wallet_count": 3, "time_window_s": 60, "sample_wallets": [WALLET_B, WALLET_C]},
            },
        )
        logger.log_wallet_signal(sig)
        logger.log_session_end("test_complete")

        events = replay_session(str(logger.filepath))
        assert len(events) == 3  # START + WALLET_SIGNAL + END
        assert events[1]["event_type"] == "WALLET_SIGNAL"
        assert events[1]["signals"] == ["TIMING", "COORDINATION"]
        assert events[1]["details"]["coordination"]["sample_wallets"][0] == WALLET_B
        print(f"  INTELLIGENCE_ONLY logs signals: {len(events)} events: OK")
        print(f"    Signal event: signals={events[1]['signals']}")


if __name__ == "__main__":
    test_timing_signal()
    test_coordination_signal()
    test_persistence_signal()
    test_exhaustion_signal()
    test_signal_aggregator()
    test_signal_logging()
    print("\n*** ALL PHASE 2 TESTS PASSED ***")
