"""Phase 3 verification test for PANDA LIVE - Token State Machine."""

import tempfile

from panda_live.models.events import FlowEvent, WhaleEvent, StateTransitionEvent
from panda_live.models.wallet_state import WalletState
from panda_live.models.token_state import TokenState
from panda_live.core.time_windows import TimeWindowManager
from panda_live.core.whale_detection import WhaleDetector
from panda_live.core.signal_aggregator import SignalAggregator
from panda_live.core.episode_tracker import EpisodeTracker
from panda_live.core.density_tracker import DensityTracker
from panda_live.core.token_state_machine import TokenStateMachine
from panda_live.logging.session_logger import SessionLogger
from panda_live.logging.log_replay import replay_session

TOKEN = "T" * 44
T0 = 1000


def addr(letter: str) -> str:
    """Generate a 44-char wallet address from a single character."""
    return letter * 44


def make_wallet(token_state: TokenState, wallet: str, first_seen: int, is_early: bool = False) -> WalletState:
    ws = WalletState(address=wallet, first_seen=first_seen, last_seen=first_seen, is_early=is_early)
    token_state.active_wallets[wallet] = ws
    if is_early:
        token_state.early_wallets.add(wallet)
    return ws


def test_episode_tracker():
    print("=== Episode Tracker ===")
    tracker = EpisodeTracker()
    ts = TokenState(ca=TOKEN, t0=T0)

    # No whale timestamp -> no boundary
    should_end, _ = tracker.check_episode_boundary(ts, 2000)
    assert should_end is False
    print("  No whale timestamp: no boundary: OK")

    # Set last whale, check within 10min
    ts.last_whale_timestamp = 1500
    should_end, _ = tracker.check_episode_boundary(ts, 2000)
    assert should_end is False
    print("  500s silence: no boundary: OK")

    # 10min silence -> episode end
    should_end, _ = tracker.check_episode_boundary(ts, 2100)
    assert should_end is True
    print("  600s silence: episode end: OK")

    # Re-ignition check (uses prev_whale_timestamp for gap calculation)
    ts.prev_whale_timestamp = 1500
    assert tracker.check_reignition(ts, 1800) is True   # 300s < 600s
    assert tracker.check_reignition(ts, 2200) is False   # 700s >= 600s
    print("  Re-ignition: 300s=same episode, 700s=new episode: OK")

    # Start new episode
    tracker.start_new_episode(ts, 3000)
    assert ts.episode_id == 1
    assert ts.episode_start == 3000
    assert ts.episode_max_density == 0.0
    print(f"  New episode: id={ts.episode_id}, start={ts.episode_start}: OK")


def test_density_tracker():
    print("=== Density Tracker ===")
    tracker = DensityTracker()
    ts = TokenState(ca=TOKEN, t0=T0)

    # Add 5 whale events from different wallets within 2min
    for i, letter in enumerate("ABCDE"):
        tracker.add_whale_event(ts, addr(letter), 1100 + i * 10)

    whale_count, density = tracker.get_current_density(ts)
    assert whale_count == 5
    print(f"  5 whales in 2min window: count={whale_count}, density={density:.4f}: OK")

    # Check episode max
    assert tracker.is_episode_max_density(ts, density) is True
    assert ts.episode_max_density == density
    print(f"  Episode max updated: {ts.episode_max_density:.4f}: OK")

    # Same density is not a new max
    assert tracker.is_episode_max_density(ts, density) is False
    print("  Same density: not new max: OK")

    # Expire old events (add event 3min later)
    tracker.add_whale_event(ts, addr("F"), 1400)
    whale_count_2, _ = tracker.get_current_density(ts)
    assert whale_count_2 == 1  # Only F remains
    print(f"  After 3min: count={whale_count_2} (old expired): OK")


def test_state_machine_full_lifecycle():
    print("=== State Machine Full Lifecycle ===")
    sm = TokenStateMachine()
    agg = SignalAggregator()
    ts = TokenState(ca=TOKEN, t0=T0)
    transitions = []

    def assert_transition(t, expected_to, label):
        assert t is not None, f"Expected transition to {expected_to} at {label}"
        assert t.to_state == expected_to, f"Expected {expected_to}, got {t.to_state} at {label}"
        transitions.append(t)
        print(f"  {label}: {t.from_state} -> {t.to_state} (trigger={t.trigger}): OK")

    # --- Episode 1 ---

    # t=1000: First whale -> QUIET -> IGNITION
    ws_a = make_wallet(ts, addr("A"), 1000, is_early=True)
    sm.density_tracker.add_whale_event(ts, addr("A"), 1000)
    t = sm.evaluate_transition(ts, agg, 1000)
    assert_transition(t, "TOKEN_IGNITION", "t=1000 first whale")
    assert ts.episode_id == 1

    # t=1020: 2nd wallet
    ws_b = make_wallet(ts, addr("B"), 1020, is_early=True)
    sm.density_tracker.add_whale_event(ts, addr("B"), 1020)
    t = sm.evaluate_transition(ts, agg, 1020)
    # Only 2 coordinated -> stays IGNITION
    assert t is None
    print("  t=1020: 2 wallets, stays IGNITION: OK")

    # t=1040: 3rd wallet -> IGNITION -> COORDINATION_SPIKE
    ws_c = make_wallet(ts, addr("C"), 1040, is_early=True)
    sm.density_tracker.add_whale_event(ts, addr("C"), 1040)
    t = sm.evaluate_transition(ts, agg, 1040)
    assert_transition(t, "TOKEN_COORDINATION_SPIKE", "t=1040 3rd wallet")

    # t=1100: Not yet 2 min in COORDINATION_SPIKE (only 60s)
    t = sm.evaluate_transition(ts, agg, 1100)
    assert t is None
    print("  t=1100: 60s in COORDINATION_SPIKE, stays: OK")

    # t=1160: 2min sustained -> COORDINATION_SPIKE -> EARLY_PHASE
    t = sm.evaluate_transition(ts, agg, 1160)
    assert_transition(t, "TOKEN_EARLY_PHASE", "t=1160 sustained 2min")

    # Add minute buckets to A and B for persistence
    ws_a.minute_buckets = {16, 18}  # 2 distinct buckets
    ws_b.minute_buckets = {16, 19}

    # t=1300: 2+ persistent wallets -> EARLY_PHASE -> PERSISTENCE_CONFIRMED
    t = sm.evaluate_transition(ts, agg, 1300)
    assert_transition(t, "TOKEN_PERSISTENCE_CONFIRMED", "t=1300 persistence")

    # t=1400: Add non-early whale -> PERSISTENCE_CONFIRMED -> PARTICIPATION_EXPANSION
    ws_d = make_wallet(ts, addr("D"), 1400)  # Not early
    ws_d.last_seen = 1400
    t = sm.evaluate_transition(ts, agg, 1400)
    assert_transition(t, "TOKEN_PARTICIPATION_EXPANSION", "t=1400 non-early whale")

    # t=1500: 5+ whales in 2min -> PARTICIPATION_EXPANSION -> PRESSURE_PEAKING
    ws_e = make_wallet(ts, addr("E"), 1480)
    sm.density_tracker.add_whale_event(ts, addr("D"), 1400)
    sm.density_tracker.add_whale_event(ts, addr("E"), 1480)
    # Now density window has: A(1000-expired), B(1020-expired), C(1040-expired), D(1400), E(1480)
    # Actually need 5 in 2min window. Let me add more recent ones.
    ws_f = make_wallet(ts, addr("F"), 1490)
    ws_g = make_wallet(ts, addr("G"), 1495)
    sm.density_tracker.add_whale_event(ts, addr("F"), 1490)
    sm.density_tracker.add_whale_event(ts, addr("G"), 1495)
    # Also re-add A with recent time to get 5 unique in window
    sm.density_tracker.add_whale_event(ts, addr("A"), 1498)
    ws_a.last_seen = 1498
    t = sm.evaluate_transition(ts, agg, 1500)
    assert_transition(t, "TOKEN_PRESSURE_PEAKING", "t=1500 pressure peak")

    # t=1700: Make 60% early wallets silent for exhaustion
    # Early wallets: A, B, C. Make B and C silent (>180s)
    ws_b.last_seen = 1400  # 300s silence
    ws_c.last_seen = 1400  # 300s silence
    ws_a.last_seen = 1650  # Still active
    # Non-early wallets D, E, F, G all need to be silent too (no replacement)
    ws_d.last_seen = 1200
    ws_e.last_seen = 1200
    ws_f.last_seen = 1200
    ws_g.last_seen = 1200
    t = sm.evaluate_transition(ts, agg, 1700)
    assert_transition(t, "TOKEN_EXHAUSTION_DETECTED", "t=1700 exhaustion")

    # t=1900: All wallets silent -> EXHAUSTION -> DISSIPATION
    for ws in ts.active_wallets.values():
        ws.last_seen = 1500  # All >400s ago
    t = sm.evaluate_transition(ts, agg, 1900)
    assert_transition(t, "TOKEN_DISSIPATION", "t=1900 dissipation")

    # t=2500: 10min silence -> DISSIPATION -> QUIET
    t = sm.evaluate_transition(ts, agg, 2500)
    assert_transition(t, "TOKEN_QUIET", "t=2500 episode end")
    assert ts.episode_id == 1  # Still episode 1 until new one starts
    print(f"  Episode 1 complete: {len(transitions)} transitions")

    # --- Episode 2 ---

    # t=3200: New whale (>10min gap) -> QUIET -> IGNITION (episode 2)
    ws_h = make_wallet(ts, addr("H"), 3200)
    sm.density_tracker.add_whale_event(ts, addr("H"), 3200)
    t = sm.evaluate_transition(ts, agg, 3200)
    assert_transition(t, "TOKEN_IGNITION", "t=3200 new episode")
    assert ts.episode_id == 2
    assert ts.episode_max_density == 0.0  # Reset for new episode
    print(f"  Episode 2 started: id={ts.episode_id}")


def test_reverse_transitions():
    print("=== Reverse Transitions ===")
    sm = TokenStateMachine()
    agg = SignalAggregator()

    # Test DISSIPATION -> IGNITION (sudden reactivation)
    ts = TokenState(ca=TOKEN, t0=T0, current_state="TOKEN_DISSIPATION",
                    state_changed_at=1800, episode_id=1, last_whale_timestamp=1800)
    ws_a = make_wallet(ts, addr("A"), 1850)
    ws_a.last_seen = 1850
    t = sm.evaluate_transition(ts, agg, 1860)
    assert t is not None
    assert t.to_state == "TOKEN_IGNITION"
    assert t.trigger == "sudden_reactivation"
    print(f"  DISSIPATION -> IGNITION (reactivation): OK")

    # Test EXHAUSTION -> PARTICIPATION_EXPANSION (new whale burst)
    ts2 = TokenState(ca=TOKEN, t0=T0, current_state="TOKEN_EXHAUSTION_DETECTED",
                     state_changed_at=1700, episode_id=1, last_whale_timestamp=1700)
    # Need 2+ recent whales in 60s AND at least 1 whale in 300s for non-dissipation
    ws_x = make_wallet(ts2, addr("X"), 1750)
    ws_x.last_seen = 1750
    ws_y = make_wallet(ts2, addr("Y"), 1755)
    ws_y.last_seen = 1755
    t = sm.evaluate_transition(ts2, agg, 1760)
    assert t is not None
    assert t.to_state == "TOKEN_PARTICIPATION_EXPANSION"
    assert t.trigger == "new_whale_burst_reversal"
    print(f"  EXHAUSTION -> PARTICIPATION_EXPANSION (burst reversal): OK")


def test_state_transition_logging():
    print("=== State Transition Logging ===")
    with tempfile.TemporaryDirectory() as tmpdir:
        logger = SessionLogger(token_ca=TOKEN, log_level="INTELLIGENCE_ONLY", output_dir=tmpdir)
        logger.log_session_start({"phase": 3})

        transition = StateTransitionEvent(
            token_ca=TOKEN,
            timestamp=1000,
            episode_id=1,
            from_state="TOKEN_QUIET",
            to_state="TOKEN_IGNITION",
            trigger="new_episode",
            details={},
        )
        logger.log_state_transition(transition)
        logger.log_session_end("test_complete")

        events = replay_session(str(logger.filepath))
        assert len(events) == 3  # START + TRANSITION + END
        assert events[1]["event_type"] == "STATE_TRANSITION"
        assert events[1]["from_state"] == "TOKEN_QUIET"
        assert events[1]["to_state"] == "TOKEN_IGNITION"
        assert events[1]["episode_id"] == 1
        print(f"  INTELLIGENCE_ONLY logs transitions: {len(events)} events: OK")
        print(f"    Transition: {events[1]['from_state']} -> {events[1]['to_state']}")


def test_episode_id_increments():
    print("=== Episode ID Increments ===")
    sm = TokenStateMachine()
    agg = SignalAggregator()
    ts = TokenState(ca=TOKEN, t0=T0)

    # Episode 1
    make_wallet(ts, addr("A"), 1000)
    sm.density_tracker.add_whale_event(ts, addr("A"), 1000)
    t1 = sm.evaluate_transition(ts, agg, 1000)
    assert t1.episode_id == 1
    print(f"  First episode: id={t1.episode_id}: OK")

    # Force to QUIET via episode end
    ts.current_state = "TOKEN_DISSIPATION"
    ts.state_changed_at = 1000
    t_quiet = sm.evaluate_transition(ts, agg, 1700)
    assert t_quiet.to_state == "TOKEN_QUIET"

    # Episode 2 (>10min gap)
    make_wallet(ts, addr("B"), 2700)
    sm.density_tracker.add_whale_event(ts, addr("B"), 2700)
    t2 = sm.evaluate_transition(ts, agg, 2700)
    assert t2.episode_id == 2
    assert ts.episode_max_density == 0.0
    print(f"  Second episode: id={t2.episode_id}, density reset: OK")


if __name__ == "__main__":
    test_episode_tracker()
    test_density_tracker()
    test_state_machine_full_lifecycle()
    test_reverse_transitions()
    test_state_transition_logging()
    test_episode_id_increments()
    print("\n*** ALL PHASE 3 TESTS PASSED ***")
