"""Phase 4 verification test for PANDA LIVE - CLI, Helius, Orchestration."""

import tempfile
import time

from panda_live.cli.layout import calculate_layout
from panda_live.cli.panels import EventPanel, TokenPanel, WalletPanel
from panda_live.cli.renderer import CLIRenderer
from panda_live.config.thresholds import LOG_DIR
from panda_live.integrations.helius_client import HeliusClient, LAMPORTS_PER_SOL
from panda_live.logging.session_logger import SessionLogger
from panda_live.logging.log_replay import replay_session
from panda_live.models.events import FlowEvent, StateTransitionEvent, WalletSignalEvent
from panda_live.models.token_state import TokenState
from panda_live.models.wallet_state import WalletState
from panda_live.orchestration.live_processor import LiveProcessor

TOKEN = "T" * 44


def addr(letter: str) -> str:
    return letter * 44


def test_layout_calculation():
    print("=== Layout Calculation ===")
    # Large terminal
    layout = calculate_layout(120, 50)
    assert layout["header"] == 4
    assert layout["token_panel"] == 15
    assert layout["wallet_panel"] == 20
    assert layout["event_stream"] >= 5
    print(f"  120x50: token={layout['token_panel']}, wallet={layout['wallet_panel']}, event={layout['event_stream']}: OK")

    # Medium terminal
    layout = calculate_layout(100, 40)
    assert layout["token_panel"] == 12
    assert layout["wallet_panel"] == 15
    print(f"  100x40: token={layout['token_panel']}, wallet={layout['wallet_panel']}: OK")

    # Small terminal (panels may shrink to fit)
    layout = calculate_layout(80, 30)
    assert layout["token_panel"] >= 6
    assert layout["wallet_panel"] >= 6
    assert layout["event_stream"] >= 5
    print(f"  80x30: token={layout['token_panel']}, wallet={layout['wallet_panel']}: OK")

    # Minimal terminal
    layout = calculate_layout(80, 24)
    assert layout["token_panel"] >= 6
    assert layout["wallet_panel"] >= 6
    assert layout["event_stream"] >= 5
    print(f"  80x24: token={layout['token_panel']}, wallet={layout['wallet_panel']}, event={layout['event_stream']}: OK")


def test_token_panel():
    print("=== Token Panel ===")
    panel = TokenPanel()
    ts = TokenState(ca=TOKEN, t0=1000, episode_id=1, episode_start=1000,
                    current_state="TOKEN_PRESSURE_PEAKING", state_changed_at=1400)

    transitions = [
        StateTransitionEvent(TOKEN, 1400, 1, "TOKEN_PARTICIPATION_EXPANSION",
                              "TOKEN_PRESSURE_PEAKING", "5+_whales_2min_episode_max",
                              {"whale_count": 7, "severity": "S4"}),
        StateTransitionEvent(TOKEN, 1300, 1, "TOKEN_PERSISTENCE_CONFIRMED",
                              "TOKEN_PARTICIPATION_EXPANSION", "new_non_early_whales",
                              {"new_whale_count": 2, "severity": "S2"}),
    ]

    lines = panel.render(ts, transitions, 1500, max_lines=12)

    assert any("PRESSURE_PEAKING" in l for l in lines)
    assert any("[S4]" in l for l in lines)
    assert any("EP: 1" in l for l in lines)
    assert any("Phase: Peaking" in l for l in lines)
    assert any("Silent:" in l for l in lines)
    assert any("Repl:" in l for l in lines)
    assert any("Recent Transitions" in l for l in lines)
    print(f"  Rendered {len(lines)} lines: OK")
    for line in lines[:8]:
        if line.strip():
            print(f"    {line}")


def test_wallet_panel():
    print("=== Wallet Panel ===")
    names = {addr("A"): "Alpha", addr("B"): "Whale2"}
    panel = WalletPanel(wallet_names=names)

    ts = TokenState(ca=TOKEN, t0=1000)
    ws_a = WalletState(address=addr("A"), first_seen=1000, last_seen=1300, is_early=True)
    ws_a.minute_buckets = {16, 18}
    ws_b = WalletState(address=addr("B"), first_seen=1020, last_seen=1200, is_early=True)
    ws_c = WalletState(address=addr("C"), first_seen=1400, last_seen=1500)
    ts.active_wallets = {addr("A"): ws_a, addr("B"): ws_b, addr("C"): ws_c}
    ts.early_wallets = {addr("A"), addr("B")}

    signals = {
        addr("A"): ["TIMING", "COORDINATION", "PERSISTENCE"],
        addr("B"): ["TIMING", "PERSISTENCE"],
        addr("C"): [],
    }

    lines = panel.render(ts, signals, max_lines=15)

    assert any("Active: 3" in l for l in lines)
    assert any("Early: 2" in l for l in lines)
    assert any("Persist: 1" in l for l in lines)
    # Short wallet addresses displayed
    assert any("AAAA...AAAA" in l for l in lines)
    # With filler, all 3 wallets rendered (cap=4), so C appears as filler
    assert any("CCCC...CCCC" in l for l in lines), "Filler wallet C should be rendered"
    print(f"  Rendered {len(lines)} lines: OK")
    for line in lines[:10]:
        if line.strip():
            print(f"    {line}")


def test_event_panel():
    print("=== Event Panel ===")
    panel = EventPanel(buffer_size=50)

    # Add state transition
    t = StateTransitionEvent(TOKEN, int(time.time()), 1, "TOKEN_QUIET", "TOKEN_IGNITION",
                              "new_episode", {"severity": "S1"})
    panel.add_state_transition(t)

    # Add wallet signal
    sig = WalletSignalEvent(addr("A"), int(time.time()), TOKEN, ["TIMING", "COORDINATION"])
    panel.add_wallet_signal(sig)

    # Add info
    panel.add_info("Session started")

    lines = panel.render(max_lines=8)
    assert any("STATE:" in l for l in lines)
    assert any("SIGNAL:" in l for l in lines)
    assert any("Session started" in l for l in lines)
    print(f"  Rendered {len(lines)} lines: OK")
    for line in lines:
        if line.strip():
            print(f"    {line}")


def test_renderer_frame():
    print("=== CLI Renderer Frame ===")
    names = {addr("A"): "Alpha"}
    renderer = CLIRenderer(wallet_names=names)

    ts = TokenState(ca=TOKEN, t0=1000, episode_id=1, episode_start=1000,
                    current_state="TOKEN_IGNITION", state_changed_at=1000)

    transition = StateTransitionEvent(TOKEN, 1000, 1, "TOKEN_QUIET", "TOKEN_IGNITION",
                                       "new_episode", {"severity": "S1"})
    renderer.add_transition(transition)

    sig = WalletSignalEvent(addr("A"), 1000, TOKEN, ["TIMING"])
    renderer.add_wallet_signal(sig)

    frame = renderer.render_frame(ts, 1100)
    assert "PANDA LIVE" in frame
    assert "IGNITION" in frame
    assert "TOKEN:" in frame
    assert "Active:" in frame
    assert "EVENT STREAM" in frame
    print(f"  Frame rendered ({len(frame)} chars): OK")
    # Print first few lines
    for line in frame.split("\n")[:8]:
        print(f"    {line[:80]}")


def test_helius_parse_transaction():
    print("=== Helius Transaction Parsing ===")
    client = HeliusClient(api_key="test-key")

    # Mock Helius SWAP transaction with correct structure
    mock_tx = {
        "signature": "5hG9kL2pAbCdEfGhIjKlMnOpQrStUvWxYz1234567890abcdef",
        "timestamp": int(time.time()),
        "type": "SWAP",
        "feePayer": addr("A"),
        "accountData": [
            {
                "account": addr("A"),
                "nativeBalanceChange": -5_000_000_000,  # -5 SOL = BUY
            }
        ],
    }

    flow = client.parse_transaction(mock_tx, TOKEN)
    assert flow is not None
    assert flow.wallet == addr("A")
    assert flow.amount_sol == 5.0
    assert flow.direction == "buy"
    print(f"  BUY parse: wallet={flow.wallet[:8]}..., sol={flow.amount_sol}, dir={flow.direction}: OK")

    # SELL transaction
    mock_sell = {
        "signature": "sig_sell_123",
        "timestamp": int(time.time()),
        "type": "SWAP",
        "feePayer": addr("B"),
        "accountData": [
            {
                "account": addr("B"),
                "nativeBalanceChange": 3_500_000_000,  # +3.5 SOL = SELL
            }
        ],
    }

    flow_sell = client.parse_transaction(mock_sell, TOKEN)
    assert flow_sell is not None
    assert flow_sell.amount_sol == 3.5
    assert flow_sell.direction == "sell"
    print(f"  SELL parse: sol={flow_sell.amount_sol}, dir={flow_sell.direction}: OK")

    # Non-SWAP (should be skipped)
    mock_other = {"signature": "sig", "timestamp": 1000, "type": "TRANSFER", "feePayer": addr("A")}
    assert client.parse_transaction(mock_other, TOKEN) is None
    print("  Non-SWAP skipped: OK")

    # Zero balance change (should be skipped)
    mock_zero = {
        "signature": "sig2", "timestamp": 1000, "type": "SWAP",
        "feePayer": addr("A"),
        "accountData": [{"account": addr("A"), "nativeBalanceChange": 0}],
    }
    assert client.parse_transaction(mock_zero, TOKEN) is None
    print("  Zero balance skipped: OK")

    # Lamports conversion check
    assert 10_000_000_000 / LAMPORTS_PER_SOL == 10.0
    assert 500_000_000 / LAMPORTS_PER_SOL == 0.5
    print(f"  Lamports conversion: 10B={10_000_000_000 / LAMPORTS_PER_SOL} SOL, 500M={500_000_000 / LAMPORTS_PER_SOL} SOL: OK")


def test_live_processor_integration():
    print("=== Live Processor Integration ===")
    with tempfile.TemporaryDirectory() as tmpdir:
        logger = SessionLogger(token_ca=TOKEN, log_level="FULL", output_dir=tmpdir)
        renderer = CLIRenderer()
        processor = LiveProcessor(
            token_ca=TOKEN,
            helius_client=None,
            session_logger=logger,
            cli_renderer=renderer,
        )

        t0 = 1000

        # Start session (normally done by run()/run_demo())
        logger.log_session_start({"mode": "test"})

        # Feed flows through processor
        flows = [
            FlowEvent(addr("A"), t0, "buy", 12.0, "sig_a1", TOKEN),
            FlowEvent(addr("B"), t0 + 20, "buy", 15.0, "sig_b1", TOKEN),
            FlowEvent(addr("C"), t0 + 40, "buy", 11.0, "sig_c1", TOKEN),
        ]

        for f in flows:
            processor.process_flow(f)

        # Verify token state
        ts = processor.token_state
        assert ts.t0 == t0
        assert len(ts.active_wallets) == 3
        assert ts.current_state != "TOKEN_QUIET"  # Should have progressed
        print(f"  Token state: {ts.current_state}, wallets: {len(ts.active_wallets)}: OK")

        # Verify we can render a frame
        frame = renderer.render_frame(ts, t0 + 100)
        assert "PANDA LIVE" in frame
        assert "TOKEN:" in frame
        print(f"  Frame renders after processing: OK")

        # Check session log has events
        logger.log_session_end("test_complete")
        events = replay_session(str(logger.filepath))
        event_types = [e["event_type"] for e in events]
        assert "SESSION_START" in event_types
        assert "SESSION_END" in event_types
        # Should have flows, whale events, signals, and/or transitions
        assert len(events) > 2
        print(f"  Session log: {len(events)} events, types: {set(event_types)}: OK")


def test_demo_flows():
    print("=== Demo Flow Generation ===")
    from panda_live_main import build_demo_flows

    flows = build_demo_flows(TOKEN)
    assert len(flows) > 10
    assert all(isinstance(f, FlowEvent) for f in flows)
    assert all(f.token_ca == TOKEN for f in flows)

    # Verify temporal ordering
    for i in range(1, len(flows)):
        assert flows[i].timestamp >= flows[i - 1].timestamp

    # Verify mix of buys and sells
    directions = {f.direction for f in flows}
    assert "buy" in directions
    assert "sell" in directions

    print(f"  Generated {len(flows)} demo flows with buys+sells: OK")


def test_cli_display_rules():
    """Verify CLI output follows Goldilocks principle."""
    print("=== CLI Display Rules ===")
    names = {addr("A"): "Alpha"}
    renderer = CLIRenderer(wallet_names=names)

    ts = TokenState(ca=TOKEN, t0=1000, episode_id=1, episode_start=1000,
                    current_state="TOKEN_PRESSURE_PEAKING", state_changed_at=1400)
    ws_a = WalletState(address=addr("A"), first_seen=1000, last_seen=1400, is_early=True)
    ws_a.minute_buckets = {16, 18, 20}
    ts.active_wallets = {addr("A"): ws_a}
    ts.early_wallets = {addr("A")}

    t = StateTransitionEvent(TOKEN, 1400, 1, "TOKEN_PARTICIPATION_EXPANSION",
                              "TOKEN_PRESSURE_PEAKING", "5+_whales_2min_episode_max",
                              {"whale_count": 7, "severity": "S4"})
    renderer.add_transition(t)
    sig = WalletSignalEvent(addr("A"), 1400, TOKEN, ["TIMING", "PERSISTENCE"])
    renderer.add_wallet_signal(sig)

    frame = renderer.render_frame(ts, 1500)

    # REQUIRED: Compressed summaries with context
    assert "Active:" in frame
    assert "Early:" in frame
    # REQUIRED: State + severity together
    assert "PRESSURE_PEAKING" in frame
    assert "[S4]" in frame
    # REQUIRED: Short wallet addresses in wallet pane
    assert "AAAA...AAAA" in frame
    # REQUIRED: Silent and Replacement metrics
    assert "Silent:" in frame
    assert "Repl:" in frame

    # FORBIDDEN: Internal trigger names should NOT appear
    assert "5+_whales_2min_episode_max" not in frame
    assert "5_whales_2min" not in frame

    print("  Compressed summaries with context: OK")
    print("  State + severity together: OK")
    print("  Short wallet addresses shown: OK")
    print("  Silent + Replacement metrics shown: OK")
    print("  Internal triggers hidden: OK")


if __name__ == "__main__":
    test_layout_calculation()
    test_token_panel()
    test_wallet_panel()
    test_event_panel()
    test_renderer_frame()
    test_helius_parse_transaction()
    test_live_processor_integration()
    test_demo_flows()
    test_cli_display_rules()
    print("\n*** ALL PHASE 4 TESTS PASSED ***")
