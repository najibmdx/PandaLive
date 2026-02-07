"""Scalability hardening tests for PANDA LIVE.

Verifies all 5 O(1)-bounded emission fixes:
1. Coordination payload never exceeds COORDINATION_SAMPLE_WALLETS
2. Wallet panel never renders > MAX_WALLET_LINES wallets + summary
3. Event buffer stays within both count and byte caps under stress
4. Logger truncates long details
5. Active wallet cap with LRU eviction
"""

import json
import tempfile

from panda_live.cli.panels import EventPanel, WalletPanel
from panda_live.config.thresholds import (
    COORDINATION_SAMPLE_WALLETS,
    MAX_ACTIVE_WALLETS,
    MAX_DETAIL_CHARS,
    MAX_EVENT_BUFFER_BYTES,
    MAX_WALLET_LINES,
)
from panda_live.core.signal_aggregator import SignalAggregator
from panda_live.core.time_windows import TimeWindowManager
from panda_live.core.whale_detection import WhaleDetector
from panda_live.logging.session_logger import SessionLogger
from panda_live.models.events import FlowEvent, WhaleEvent, WalletSignalEvent
from panda_live.models.token_state import TokenState
from panda_live.models.wallet_state import WalletState
from panda_live.orchestration.live_processor import LiveProcessor
from panda_live.cli.renderer import CLIRenderer

TOKEN = "T" * 44


def addr(letter: str, idx: int = 0) -> str:
    """Generate a unique 44-char address."""
    base = f"{letter}{idx:03d}"
    return base.ljust(44, "0")


def test_coordination_payload_cap():
    """Coordination sample_wallets never exceeds COORDINATION_SAMPLE_WALLETS."""
    print("=== Coordination Payload Cap ===")
    aggregator = SignalAggregator()
    ts = TokenState(ca=TOKEN, t0=1000)
    mgr = TimeWindowManager()
    detector = WhaleDetector()

    # Create 10 wallets all whaling within 60s coordination window
    wallets = []
    for i in range(10):
        a = addr("W", i)
        ws = WalletState(address=a)
        ts.active_wallets[a] = ws
        wallets.append((a, ws))

    # Feed all 10 as whale events within coordination window
    for i, (a, ws) in enumerate(wallets):
        flow = FlowEvent(a, 1100 + i * 5, "buy", 12.0, f"sig_{i}", TOKEN)
        mgr.add_flow(ws, flow)
        whale_events = detector.check_thresholds(ws, flow)
        for we in whale_events:
            sig = aggregator.process_whale_event(we, ws, ts, 1100 + i * 5)

            if "COORDINATION" in sig.signals:
                sample = sig.details["coordination"]["sample_wallets"]
                total = sig.details["coordination"]["wallet_count"]
                assert len(sample) <= COORDINATION_SAMPLE_WALLETS, (
                    f"sample_wallets has {len(sample)} entries, max is {COORDINATION_SAMPLE_WALLETS}"
                )
                # wallet_count should reflect actual count, not capped
                assert total >= len(sample)
                print(f"  Wallet {i}: wallet_count={total}, sample={len(sample)}: OK")

    print(f"  COORDINATION_SAMPLE_WALLETS={COORDINATION_SAMPLE_WALLETS}: enforced")


def test_wallet_panel_cap():
    """WalletPanel never renders more than MAX_WALLET_LINES wallets."""
    print("=== Wallet Panel Cap ===")
    panel = WalletPanel()
    ts = TokenState(ca=TOKEN, t0=1000)

    # Create 30 wallets with signals
    signals = {}
    for i in range(30):
        a = addr("P", i)
        ws = WalletState(address=a, first_seen=1000 + i, last_seen=1500)
        ws.minute_buckets = {16, 18}
        ts.active_wallets[a] = ws
        if i < 25:
            ts.early_wallets.add(a)
        signals[a] = ["TIMING", "PERSISTENCE"] if i < 25 else ["PERSISTENCE"]

    lines = panel.render(ts, signals, max_lines=50)

    # Count wallet address lines (44-char addresses)
    wallet_lines = [l for l in lines if any(addr("P", i) in l for i in range(30))]
    assert len(wallet_lines) <= MAX_WALLET_LINES, (
        f"Rendered {len(wallet_lines)} wallet lines, max is {MAX_WALLET_LINES}"
    )

    # Should have summary for remaining
    assert any("more wallets not shown" in l for l in lines), "Missing summary line"
    print(f"  30 wallets -> {len(wallet_lines)} rendered (max {MAX_WALLET_LINES}): OK")
    print(f"  Summary line present: OK")


def test_event_buffer_stress():
    """Event buffer stays within both count and byte caps under 1000-event stress."""
    print("=== Event Buffer Stress Test ===")
    panel = EventPanel(buffer_size=100)

    # Pump 1000 events
    for i in range(1000):
        sig = WalletSignalEvent(
            wallet=addr("S", i % 50),
            timestamp=1000 + i,
            token_ca=TOKEN,
            signals=["TIMING", "COORDINATION", "PERSISTENCE"],
            details={"test": f"event_{i}" * 20},
        )
        panel.add_wallet_signal(sig)

    # Check count cap
    assert len(panel._events) <= panel._buffer_size, (
        f"Buffer has {len(panel._events)} events, max is {panel._buffer_size}"
    )

    # Check byte cap
    assert panel._buffer_bytes <= MAX_EVENT_BUFFER_BYTES, (
        f"Buffer is {panel._buffer_bytes} bytes, max is {MAX_EVENT_BUFFER_BYTES}"
    )

    # Verify byte tracking is accurate
    actual_bytes = sum(len(e.encode("utf-8", errors="replace")) for e in panel._events)
    assert panel._buffer_bytes == actual_bytes, (
        f"Tracked bytes {panel._buffer_bytes} != actual {actual_bytes}"
    )

    print(f"  1000 events -> {len(panel._events)} retained, {panel._buffer_bytes} bytes: OK")
    print(f"  Count cap ({panel._buffer_size}): enforced")
    print(f"  Byte cap ({MAX_EVENT_BUFFER_BYTES}): enforced")
    print(f"  Byte tracking accurate: OK")


def test_logger_detail_truncation():
    """SessionLogger truncates long details to MAX_DETAIL_CHARS."""
    print("=== Logger Detail Truncation ===")

    # Test string truncation
    long_string = "x" * 1000
    capped = SessionLogger._cap_details(long_string)
    assert len(capped) <= MAX_DETAIL_CHARS + 3  # +3 for "..."
    assert capped.endswith("...")
    print(f"  Long string ({len(long_string)} chars) -> {len(capped)} chars: OK")

    # Test list truncation
    long_list = list(range(100))
    capped_list = SessionLogger._cap_details(long_list)
    assert len(capped_list) <= COORDINATION_SAMPLE_WALLETS
    print(f"  Long list ({len(long_list)} items) -> {len(capped_list)} items: OK")

    # Test nested dict with long values
    nested = {
        "a": "y" * 500,
        "b": list(range(50)),
        "c": {"inner": "z" * 800},
    }
    capped_nested = SessionLogger._cap_details(nested)
    assert len(capped_nested["a"]) <= MAX_DETAIL_CHARS + 3
    assert len(capped_nested["b"]) <= COORDINATION_SAMPLE_WALLETS
    assert len(capped_nested["c"]["inner"]) <= MAX_DETAIL_CHARS + 3
    print(f"  Nested dict truncation: OK")

    # Test actual JSONL write with large details
    with tempfile.TemporaryDirectory() as tmpdir:
        logger = SessionLogger(token_ca=TOKEN, log_level="FULL", output_dir=tmpdir)
        logger.log_session_start({"mode": "test"})

        sig = WalletSignalEvent(
            wallet="A" * 44,
            timestamp=1000,
            token_ca=TOKEN,
            signals=["COORDINATION"],
            details={"coordination": {"sample_wallets": ["W" * 44] * 50, "big_field": "Q" * 1000}},
        )
        logger.log_wallet_signal(sig)
        logger.close()

        # Read and verify bounded
        with open(str(logger.filepath), "r") as f:
            for line in f:
                data = json.loads(line)
                if data.get("event_type") == "WALLET_SIGNAL":
                    coord = data["details"]["coordination"]
                    assert len(coord["sample_wallets"]) <= COORDINATION_SAMPLE_WALLETS
                    assert len(coord["big_field"]) <= MAX_DETAIL_CHARS + 3
                    print(f"  JSONL sample_wallets capped to {len(coord['sample_wallets'])}: OK")
                    print(f"  JSONL big_field capped to {len(coord['big_field'])} chars: OK")


def test_active_wallet_cap_lru():
    """Active wallet cap enforces MAX_ACTIVE_WALLETS with LRU eviction."""
    print("=== Active Wallet Cap (LRU) ===")
    with tempfile.TemporaryDirectory() as tmpdir:
        logger = SessionLogger(token_ca=TOKEN, log_level="FULL", output_dir=tmpdir)
        renderer = CLIRenderer()
        processor = LiveProcessor(
            token_ca=TOKEN,
            helius_client=None,
            session_logger=logger,
            cli_renderer=renderer,
        )

        # Feed MAX_ACTIVE_WALLETS + 10 unique wallets
        num_wallets = MAX_ACTIVE_WALLETS + 10
        logger.log_session_start({"mode": "test"})

        for i in range(num_wallets):
            a = addr("L", i)
            flow = FlowEvent(a, 1000 + i, "buy", 1.0, f"sig_lru_{i}", TOKEN)
            processor.process_flow(flow)

        active_count = len(processor.token_state.active_wallets)
        assert active_count <= MAX_ACTIVE_WALLETS, (
            f"Active wallets {active_count} exceeds cap {MAX_ACTIVE_WALLETS}"
        )

        # The most recent wallets should be retained (LRU evicts oldest)
        latest_addr = addr("L", num_wallets - 1)
        assert latest_addr in processor.token_state.active_wallets, (
            "Most recent wallet was evicted (should be retained)"
        )

        # The earliest wallet should have been evicted
        earliest_addr = addr("L", 0)
        assert earliest_addr not in processor.token_state.active_wallets, (
            "Earliest wallet was retained (should be evicted)"
        )

        logger.close()
        print(f"  {num_wallets} wallets -> {active_count} active (cap {MAX_ACTIVE_WALLETS}): OK")
        print(f"  Most recent wallet retained: OK")
        print(f"  Oldest wallet evicted: OK")


if __name__ == "__main__":
    test_coordination_payload_cap()
    test_wallet_panel_cap()
    test_event_buffer_stress()
    test_logger_detail_truncation()
    test_active_wallet_cap_lru()
    print("\n*** ALL SCALABILITY TESTS PASSED ***")
