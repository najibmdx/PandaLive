"""Phase 3.5 verification test for PANDA LIVE - Severity System."""

import tempfile

from panda_live.models.events import FlowEvent, StateTransitionEvent
from panda_live.models.wallet_state import WalletState
from panda_live.models.token_state import TokenState
from panda_live.core.time_windows import TimeWindowManager
from panda_live.core.whale_detection import WhaleDetector
from panda_live.core.signal_aggregator import SignalAggregator
from panda_live.core.token_state_machine import TokenStateMachine
from panda_live.core.severity_calculator import (
    SeverityCalculator,
    SEVERITY_WEAK,
    SEVERITY_LIGHT,
    SEVERITY_MODERATE,
    SEVERITY_STRONG,
    SEVERITY_EXTREME,
)
from panda_live.logging.session_logger import SessionLogger
from panda_live.logging.log_replay import replay_session

TOKEN = "T" * 44
T0 = 1000


def addr(letter: str) -> str:
    return letter * 44


def make_wallet(ts: TokenState, wallet: str, first_seen: int, is_early: bool = False) -> WalletState:
    ws = WalletState(address=wallet, first_seen=first_seen, last_seen=first_seen, is_early=is_early)
    ts.active_wallets[wallet] = ws
    if is_early:
        ts.early_wallets.add(wallet)
    return ws


def test_severity_calculator_standalone():
    """Test SeverityCalculator in isolation with constructed transitions."""
    print("=== Severity Calculator (Standalone) ===")
    calc = SeverityCalculator()
    ts = TokenState(ca=TOKEN, t0=T0, episode_id=1)

    # IGNITION with 1 early wallet -> S1
    make_wallet(ts, addr("A"), 1000, is_early=True)
    t = StateTransitionEvent(TOKEN, 1000, 1, "TOKEN_QUIET", "TOKEN_IGNITION", "new_episode")
    assert calc.compute_severity(t, ts) == "S1"
    print("  IGNITION (1 early) -> S1: OK")

    # IGNITION with 2+ early -> S2
    make_wallet(ts, addr("B"), 1020, is_early=True)
    t2 = StateTransitionEvent(TOKEN, 1020, 1, "TOKEN_QUIET", "TOKEN_IGNITION", "re_ignition_same_episode")
    assert calc.compute_severity(t2, ts) == "S2"
    print("  IGNITION (2 early) -> S2: OK")

    # COORDINATION_SPIKE with 3 wallets -> S2
    t3 = StateTransitionEvent(TOKEN, 1040, 1, "TOKEN_IGNITION", "TOKEN_COORDINATION_SPIKE",
                               "3+_wallets_coordinated", {"coordinated_count": 3})
    assert calc.compute_severity(t3, ts) == "S2"
    print("  COORDINATION_SPIKE (3 wallets) -> S2: OK")

    # COORDINATION_SPIKE with 4 wallets -> S3
    t4 = StateTransitionEvent(TOKEN, 1040, 1, "TOKEN_IGNITION", "TOKEN_COORDINATION_SPIKE",
                               "3+_wallets_coordinated", {"coordinated_count": 4})
    assert calc.compute_severity(t4, ts) == "S3"
    print("  COORDINATION_SPIKE (4 wallets) -> S3: OK")

    # COORDINATION_SPIKE with 5 wallets -> S4
    t5 = StateTransitionEvent(TOKEN, 1040, 1, "TOKEN_IGNITION", "TOKEN_COORDINATION_SPIKE",
                               "3+_wallets_coordinated", {"coordinated_count": 5})
    assert calc.compute_severity(t5, ts) == "S4"
    print("  COORDINATION_SPIKE (5 wallets) -> S4: OK")

    # COORDINATION_SPIKE with 6 wallets -> S5
    t6 = StateTransitionEvent(TOKEN, 1040, 1, "TOKEN_IGNITION", "TOKEN_COORDINATION_SPIKE",
                               "3+_wallets_coordinated", {"coordinated_count": 6})
    assert calc.compute_severity(t6, ts) == "S5"
    print("  COORDINATION_SPIKE (6 wallets) -> S5: OK")

    # EARLY_PHASE short duration -> S2
    t7 = StateTransitionEvent(TOKEN, 1160, 1, "TOKEN_COORDINATION_SPIKE", "TOKEN_EARLY_PHASE",
                               "sustained_beyond_burst", {"duration_seconds": 120})
    assert calc.compute_severity(t7, ts) == "S2"
    print("  EARLY_PHASE (120s, 2 early) -> S2: OK")

    # EARLY_PHASE sustained with 3+ early -> S3
    make_wallet(ts, addr("C"), 1050, is_early=True)
    t8 = StateTransitionEvent(TOKEN, 1200, 1, "TOKEN_COORDINATION_SPIKE", "TOKEN_EARLY_PHASE",
                               "sustained_beyond_burst", {"duration_seconds": 200})
    assert calc.compute_severity(t8, ts) == "S3"
    print("  EARLY_PHASE (200s, 3 early) -> S3: OK")

    # PERSISTENCE_CONFIRMED with 2 -> S3
    t9 = StateTransitionEvent(TOKEN, 1300, 1, "TOKEN_EARLY_PHASE", "TOKEN_PERSISTENCE_CONFIRMED",
                               "2+_persistent_wallets", {"persistent_count": 2})
    assert calc.compute_severity(t9, ts) == "S3"
    print("  PERSISTENCE_CONFIRMED (2) -> S3: OK")

    # PERSISTENCE_CONFIRMED with 3 -> S4
    t10 = StateTransitionEvent(TOKEN, 1300, 1, "TOKEN_EARLY_PHASE", "TOKEN_PERSISTENCE_CONFIRMED",
                                "2+_persistent_wallets", {"persistent_count": 3})
    assert calc.compute_severity(t10, ts) == "S4"
    print("  PERSISTENCE_CONFIRMED (3) -> S4: OK")

    # PERSISTENCE_CONFIRMED with 4 -> S5
    t11 = StateTransitionEvent(TOKEN, 1300, 1, "TOKEN_EARLY_PHASE", "TOKEN_PERSISTENCE_CONFIRMED",
                                "2+_persistent_wallets", {"persistent_count": 4})
    assert calc.compute_severity(t11, ts) == "S5"
    print("  PERSISTENCE_CONFIRMED (4) -> S5: OK")

    # PARTICIPATION_EXPANSION with 1 new -> S2
    t12 = StateTransitionEvent(TOKEN, 1400, 1, "TOKEN_PERSISTENCE_CONFIRMED",
                                "TOKEN_PARTICIPATION_EXPANSION", "new_non_early_whales",
                                {"new_whale_count": 1})
    assert calc.compute_severity(t12, ts) == "S2"
    print("  PARTICIPATION_EXPANSION (1 new) -> S2: OK")

    # PARTICIPATION_EXPANSION burst reversal -> S4
    t13 = StateTransitionEvent(TOKEN, 1400, 1, "TOKEN_EXHAUSTION_DETECTED",
                                "TOKEN_PARTICIPATION_EXPANSION", "new_whale_burst_reversal",
                                {"new_whale_count": 2})
    assert calc.compute_severity(t13, ts) == "S4"
    print("  PARTICIPATION_EXPANSION (burst reversal) -> S4: OK")

    # PRESSURE_PEAKING with 5 whales -> S3
    t14 = StateTransitionEvent(TOKEN, 1500, 1, "TOKEN_PARTICIPATION_EXPANSION",
                                "TOKEN_PRESSURE_PEAKING", "5+_whales_2min_episode_max",
                                {"whale_count": 5, "density": 0.0417})
    assert calc.compute_severity(t14, ts) == "S3"
    print("  PRESSURE_PEAKING (5 whales) -> S3: OK")

    # PRESSURE_PEAKING with 7 whales -> S4
    t15 = StateTransitionEvent(TOKEN, 1500, 1, "TOKEN_PARTICIPATION_EXPANSION",
                                "TOKEN_PRESSURE_PEAKING", "5+_whales_2min_episode_max",
                                {"whale_count": 7, "density": 0.0583})
    assert calc.compute_severity(t15, ts) == "S4"
    print("  PRESSURE_PEAKING (7 whales) -> S4: OK")

    # EXHAUSTION_DETECTED 60% -> S3
    t16 = StateTransitionEvent(TOKEN, 1700, 1, "TOKEN_PRESSURE_PEAKING",
                                "TOKEN_EXHAUSTION_DETECTED", "60%_early_silent_no_replacement",
                                {"disengagement_pct": 0.60})
    assert calc.compute_severity(t16, ts) == "S3"
    print("  EXHAUSTION_DETECTED (60%) -> S3: OK")

    # EXHAUSTION_DETECTED 70% -> S4
    t17 = StateTransitionEvent(TOKEN, 1700, 1, "TOKEN_PRESSURE_PEAKING",
                                "TOKEN_EXHAUSTION_DETECTED", "60%_early_silent_no_replacement",
                                {"disengagement_pct": 0.70})
    assert calc.compute_severity(t17, ts) == "S4"
    print("  EXHAUSTION_DETECTED (70%) -> S4: OK")

    # EXHAUSTION_DETECTED 80% -> S5
    t18 = StateTransitionEvent(TOKEN, 1700, 1, "TOKEN_PRESSURE_PEAKING",
                                "TOKEN_EXHAUSTION_DETECTED", "60%_early_silent_no_replacement",
                                {"disengagement_pct": 0.80})
    assert calc.compute_severity(t18, ts) == "S5"
    print("  EXHAUSTION_DETECTED (80%) -> S5: OK")

    # DISSIPATION after S4 last_severity -> S4 (post-extreme)
    calc.last_severity = "S4"
    t19 = StateTransitionEvent(TOKEN, 1900, 1, "TOKEN_EXHAUSTION_DETECTED",
                                "TOKEN_DISSIPATION", "activity_collapsed",
                                {"recent_whale_count": 0})
    assert calc.compute_severity(t19, ts) == "S4"
    print("  DISSIPATION (post-S4) -> S4: OK")

    # DISSIPATION after S2 -> S2 (normal decay)
    calc.last_severity = "S2"
    t20 = StateTransitionEvent(TOKEN, 1900, 1, "TOKEN_EXHAUSTION_DETECTED",
                                "TOKEN_DISSIPATION", "activity_collapsed",
                                {"recent_whale_count": 0})
    assert calc.compute_severity(t20, ts) == "S2"
    print("  DISSIPATION (post-S2) -> S2: OK")

    # TOKEN_QUIET -> None
    t21 = StateTransitionEvent(TOKEN, 2500, 1, "TOKEN_DISSIPATION", "TOKEN_QUIET",
                                "10_min_silence_episode_end")
    assert calc.compute_severity(t21, ts) is None
    print("  TOKEN_QUIET -> None: OK")


def test_severity_in_state_machine():
    """Test severity is properly attached to transition details by state machine."""
    print("=== Severity in State Machine ===")
    sm = TokenStateMachine()
    agg = SignalAggregator()
    ts = TokenState(ca=TOKEN, t0=T0)

    # t=1000: QUIET -> IGNITION (1 early) -> S1
    ws_a = make_wallet(ts, addr("A"), 1000, is_early=True)
    sm.density_tracker.add_whale_event(ts, addr("A"), 1000)
    t = sm.evaluate_transition(ts, agg, 1000)
    assert t is not None
    assert t.details.get("severity") == "S1"
    print(f"  IGNITION: severity={t.details['severity']}: OK")

    # Add 2 more early wallets for coordination
    ws_b = make_wallet(ts, addr("B"), 1020, is_early=True)
    sm.density_tracker.add_whale_event(ts, addr("B"), 1020)
    sm.evaluate_transition(ts, agg, 1020)  # No transition (only 2)

    ws_c = make_wallet(ts, addr("C"), 1040, is_early=True)
    sm.density_tracker.add_whale_event(ts, addr("C"), 1040)

    # t=1040: IGNITION -> COORDINATION_SPIKE (3 wallets) -> S2
    # Note: _count_coordinated_wallets counts is_early wallets = 3
    t = sm.evaluate_transition(ts, agg, 1040)
    assert t is not None
    assert t.to_state == "TOKEN_COORDINATION_SPIKE"
    assert t.details.get("severity") == "S2"
    print(f"  COORDINATION_SPIKE (3): severity={t.details['severity']}: OK")

    # t=1160: COORDINATION_SPIKE -> EARLY_PHASE -> S2
    t = sm.evaluate_transition(ts, agg, 1160)
    assert t is not None
    assert t.to_state == "TOKEN_EARLY_PHASE"
    assert t.details.get("severity") == "S2"  # 120s < 180s threshold, so S2
    print(f"  EARLY_PHASE: severity={t.details['severity']}: OK")

    # Add persistence
    ws_a.minute_buckets = {16, 18}
    ws_b.minute_buckets = {16, 19}

    # t=1300: EARLY_PHASE -> PERSISTENCE_CONFIRMED (2) -> S3
    t = sm.evaluate_transition(ts, agg, 1300)
    assert t is not None
    assert t.to_state == "TOKEN_PERSISTENCE_CONFIRMED"
    assert t.details.get("severity") == "S3"
    print(f"  PERSISTENCE_CONFIRMED (2): severity={t.details['severity']}: OK")

    # t=1400: Add non-early whale -> PARTICIPATION_EXPANSION -> S2
    ws_d = make_wallet(ts, addr("D"), 1400)
    ws_d.last_seen = 1400
    t = sm.evaluate_transition(ts, agg, 1400)
    assert t is not None
    assert t.to_state == "TOKEN_PARTICIPATION_EXPANSION"
    assert t.details.get("severity") == "S2"
    print(f"  PARTICIPATION_EXPANSION (1 new): severity={t.details['severity']}: OK")

    # t=1500: 7 whales in 2min -> PRESSURE_PEAKING -> S4
    for i, letter in enumerate("EFGHIJ"):
        w = make_wallet(ts, addr(letter), 1480 + i)
        w.last_seen = 1480 + i
        sm.density_tracker.add_whale_event(ts, addr(letter), 1480 + i)
    sm.density_tracker.add_whale_event(ts, addr("A"), 1490)
    ws_a.last_seen = 1490
    t = sm.evaluate_transition(ts, agg, 1500)
    assert t is not None
    assert t.to_state == "TOKEN_PRESSURE_PEAKING"
    assert t.details.get("severity") == "S4"
    print(f"  PRESSURE_PEAKING ({t.details.get('whale_count')} whales): severity={t.details['severity']}: OK")

    # t=1700: Exhaustion (70% disengaged) -> S4
    # Make 3 of 3 early wallets silent (B, C silent > 180s, A active)
    ws_b.last_seen = 1400
    ws_c.last_seen = 1400
    ws_a.last_seen = 1650
    # Non-early wallets also silent
    for letter in "DEFGHIJ":
        ts.active_wallets[addr(letter)].last_seen = 1200
    t = sm.evaluate_transition(ts, agg, 1700)
    assert t is not None
    assert t.to_state == "TOKEN_EXHAUSTION_DETECTED"
    assert t.details.get("severity") in ("S3", "S4")
    print(f"  EXHAUSTION_DETECTED: severity={t.details['severity']}: OK")

    # t=1900: Dissipation -> S2 (post-S3 exhaustion = normal decay)
    for ws in ts.active_wallets.values():
        ws.last_seen = 1500
    t = sm.evaluate_transition(ts, agg, 1900)
    assert t is not None
    assert t.to_state == "TOKEN_DISSIPATION"
    assert t.details.get("severity") == "S2"
    print(f"  DISSIPATION (post-S3): severity={t.details['severity']}: OK")

    # t=2500: Episode end -> QUIET -> no severity
    t = sm.evaluate_transition(ts, agg, 2500)
    assert t is not None
    assert t.to_state == "TOKEN_QUIET"
    assert "severity" not in t.details
    print(f"  QUIET: no severity in details: OK")


def test_severity_episode_reset():
    """Test that severity resets on new episode."""
    print("=== Severity Episode Reset ===")
    calc = SeverityCalculator()
    ts = TokenState(ca=TOKEN, t0=T0, episode_id=1)

    # Set high severity
    calc.last_severity = "S5"

    # New episode trigger resets
    t = StateTransitionEvent(TOKEN, 3200, 2, "TOKEN_QUIET", "TOKEN_IGNITION", "new_episode")
    make_wallet(ts, addr("X"), 3200, is_early=True)
    sev = calc.compute_severity(t, ts)
    assert sev == "S1"
    # last_severity should now be S1, not S5
    assert calc.last_severity == "S1"
    print(f"  New episode resets severity: last={calc.last_severity}: OK")


def test_severity_in_jsonl():
    """Test severity appears in JSONL log output."""
    print("=== Severity in JSONL ===")
    with tempfile.TemporaryDirectory() as tmpdir:
        logger = SessionLogger(token_ca=TOKEN, log_level="INTELLIGENCE_ONLY", output_dir=tmpdir)
        logger.log_session_start({"phase": "3.5"})

        transition = StateTransitionEvent(
            token_ca=TOKEN,
            timestamp=1040,
            episode_id=1,
            from_state="TOKEN_IGNITION",
            to_state="TOKEN_COORDINATION_SPIKE",
            trigger="3+_wallets_coordinated",
            details={"coordinated_count": 4, "severity": "S3"},
        )
        logger.log_state_transition(transition)
        logger.log_session_end("test_complete")

        events = replay_session(str(logger.filepath))
        assert len(events) == 3
        st = events[1]
        assert st["event_type"] == "STATE_TRANSITION"
        assert st["details"]["severity"] == "S3"
        assert st["details"]["coordinated_count"] == 4
        print(f"  JSONL contains severity={st['details']['severity']} alongside details: OK")


def test_severity_minimum_floors():
    """Test that certain states have minimum severity floors."""
    print("=== Severity Minimum Floors ===")
    calc = SeverityCalculator()
    ts = TokenState(ca=TOKEN, t0=T0, episode_id=1)

    # PERSISTENCE can never be S1 or S2
    t = StateTransitionEvent(TOKEN, 1300, 1, "TOKEN_EARLY_PHASE", "TOKEN_PERSISTENCE_CONFIRMED",
                              "2+_persistent_wallets", {"persistent_count": 2})
    assert calc.compute_severity(t, ts) == "S3"
    print("  PERSISTENCE floor: S3 (minimum): OK")

    # PRESSURE can never be S1 or S2
    t = StateTransitionEvent(TOKEN, 1500, 1, "TOKEN_PARTICIPATION_EXPANSION",
                              "TOKEN_PRESSURE_PEAKING", "5+_whales_2min_episode_max",
                              {"whale_count": 5})
    assert calc.compute_severity(t, ts) == "S3"
    print("  PRESSURE floor: S3 (minimum): OK")

    # EXHAUSTION can never be S1 or S2
    t = StateTransitionEvent(TOKEN, 1700, 1, "TOKEN_PRESSURE_PEAKING",
                              "TOKEN_EXHAUSTION_DETECTED", "60%_early_silent_no_replacement",
                              {"disengagement_pct": 0.60})
    assert calc.compute_severity(t, ts) == "S3"
    print("  EXHAUSTION floor: S3 (minimum): OK")


if __name__ == "__main__":
    test_severity_calculator_standalone()
    test_severity_in_state_machine()
    test_severity_episode_reset()
    test_severity_in_jsonl()
    test_severity_minimum_floors()
    print("\n*** ALL PHASE 3.5 TESTS PASSED ***")
