"""Patch B2 verification tests — ChainTimeClock, timebase plumbing, no negative deltas."""

from unittest.mock import patch
from panda_live.core.chain_time_clock import ChainTimeClock
from panda_live.models.token_state import TokenState
from panda_live.models.wallet_state import WalletState

TOKEN = "T" * 44


def addr(letter: str, idx: int = 0) -> str:
    base = f"{letter}{idx:03d}"
    return base.ljust(44, "0")


def test_clock_live_mode_no_events():
    """Before any events, clock.now() returns wallclock."""
    print("=== ChainTimeClock live mode, no events ===")
    with patch("panda_live.core.chain_time_clock.time") as mock_time:
        mock_time.time.return_value = 1700000100.0
        clock = ChainTimeClock(replay_mode=False)
        now = clock.now()
        assert now == 1700000100, f"Expected wallclock fallback, got {now}"
        assert clock.last_chain_ts is None
        assert clock.offset is None
        print(f"  now={now}, last_chain_ts=None, offset=None: OK")


def test_clock_live_mode_offset_mapping():
    """Live mode: wallclock maps to chain domain via offset."""
    print("=== ChainTimeClock live mode, offset mapping ===")
    with patch("panda_live.core.chain_time_clock.time") as mock_time:
        # Wallclock is 300s ahead of chain time (session started later)
        chain_event_ts = 1700000000
        wallclock_at_observe = 1700000300.0
        mock_time.time.return_value = wallclock_at_observe

        clock = ChainTimeClock(replay_mode=False)
        clock.observe(chain_event_ts)

        # offset = wallclock - chain = 300
        assert clock.offset == 300, f"Expected offset=300, got {clock.offset}"
        assert clock.last_chain_ts == chain_event_ts

        # Now advance wallclock by 60s
        mock_time.time.return_value = 1700000360.0
        now = clock.now()
        # chain_now = 1700000360 - 300 = 1700000060
        assert now == 1700000060, f"Expected 1700000060, got {now}"
        print(f"  offset={clock.offset}, chain_now={now}: OK")


def test_clock_live_mode_never_below_last_chain_ts():
    """chain_now never goes below the latest observed chain timestamp."""
    print("=== ChainTimeClock live mode, clamped to last_chain_ts ===")
    with patch("panda_live.core.chain_time_clock.time") as mock_time:
        chain_ts = 1700000500
        # Wallclock only slightly ahead — offset is small
        mock_time.time.return_value = 1700000501.0
        clock = ChainTimeClock(replay_mode=False)
        clock.observe(chain_ts)  # offset = 1

        # Observe a later chain event
        clock.observe(1700000600)  # last_chain_ts = 600

        # Wallclock hasn't advanced enough: 1700000501 - 1 = 1700000500 < 1700000600
        now = clock.now()
        assert now == 1700000600, f"Expected clamped to 1700000600, got {now}"
        print(f"  chain_now={now} (clamped to last_chain_ts): OK")


def test_clock_replay_mode():
    """Replay mode: now() returns last observed chain timestamp only."""
    print("=== ChainTimeClock replay mode ===")
    clock = ChainTimeClock(replay_mode=True)

    clock.observe(1700000100)
    assert clock.now() == 1700000100

    clock.observe(1700000200)
    assert clock.now() == 1700000200

    # Earlier event doesn't move the clock backwards
    clock.observe(1700000150)
    assert clock.now() == 1700000200, "Replay clock should not go backwards"
    print(f"  replay now={clock.now()}: OK")


def test_clock_offset_locked_on_first_observe():
    """Offset is set on first observe and never changes."""
    print("=== ChainTimeClock offset locked on first observe ===")
    with patch("panda_live.core.chain_time_clock.time") as mock_time:
        mock_time.time.return_value = 1700000300.0
        clock = ChainTimeClock(replay_mode=False)
        clock.observe(1700000000)  # offset = 300

        # Second observe at different wallclock — offset stays the same
        mock_time.time.return_value = 1700000400.0
        clock.observe(1700000200)
        assert clock.offset == 300, f"Offset should be locked at 300, got {clock.offset}"
        print(f"  offset stays {clock.offset} after second observe: OK")


def test_no_negative_ages_with_chain_now():
    """Simulates session_start > first_event by several minutes.

    Verifies no negative ages when using chain_now for wallet age computation.
    """
    print("=== No negative ages with chain_now ===")
    with patch("panda_live.core.chain_time_clock.time") as mock_time:
        # Session starts at wallclock 1700000600 (10 min after first chain event)
        session_wallclock = 1700000600.0
        first_event_chain_ts = 1700000000  # 10 min earlier in chain time

        mock_time.time.return_value = session_wallclock
        clock = ChainTimeClock(replay_mode=False)
        clock.observe(first_event_chain_ts)
        # offset = 600

        # Set up token state like LiveProcessor would
        ts = TokenState(ca=TOKEN)
        ts.episode_start = first_event_chain_ts
        ts.chain_now = clock.now()  # 1700000600 - 600 = 1700000000

        # Add a wallet that was first seen at the chain event time
        w = addr("A", 1)
        ws = WalletState(address=w)
        ws.first_seen = first_event_chain_ts
        ws.last_seen = first_event_chain_ts
        ws.activity_count = 1
        ts.active_wallets[w] = ws

        # Wallet age should never be negative
        age = ts.chain_now - ws.first_seen
        assert age >= 0, f"Negative age: {age}"
        print(f"  chain_now={ts.chain_now}, first_seen={ws.first_seen}, age={age}: OK")

        # Advance wallclock by 120s
        mock_time.time.return_value = session_wallclock + 120.0
        ts.chain_now = clock.now()  # 1700000720 - 600 = 1700000120
        age = ts.chain_now - ws.first_seen
        assert age == 120, f"Expected age=120, got {age}"
        print(f"  After 120s: chain_now={ts.chain_now}, age={age}: OK")


def test_episode_start_equals_earliest_event():
    """Verify episode_start is anchored to chain time, not wallclock."""
    print("=== episode_start equals earliest event timestamp ===")
    from panda_live.core.episode_tracker import EpisodeTracker

    ts = TokenState(ca=TOKEN)
    tracker = EpisodeTracker()

    chain_event_ts = 1700000000
    tracker.start_new_episode(ts, chain_event_ts)

    assert ts.episode_start == chain_event_ts, (
        f"episode_start should be {chain_event_ts}, got {ts.episode_start}"
    )
    assert ts.episode_id == 1
    print(f"  episode_start={ts.episode_start}, episode_id={ts.episode_id}: OK")


def test_wallet_panel_uses_chain_now():
    """WalletPanel uses token_state.chain_now for wallet age, not wallclock."""
    print("=== WalletPanel uses chain_now for wallet age ===")
    from panda_live.cli.panels import WalletPanel

    ts = TokenState(ca=TOKEN)
    ts.chain_now = 1700000120  # Chain-aligned "now"

    w = addr("W", 1)
    ws = WalletState(address=w)
    ws.first_seen = 1700000000
    ws.last_seen = 1700000100
    ws.activity_count = 1
    ts.active_wallets[w] = ws

    signals = {w: ["early_entry"]}
    panel = WalletPanel()
    lines = panel.render(ts, signals, max_lines=20)

    # Find the age line — should show 120s (chain_now - first_seen)
    age_found = False
    for line in lines:
        if "120s" in line:
            age_found = True
            break
    assert age_found, f"Expected '120s' in wallet age. Lines: {lines}"
    print(f"  Wallet age uses chain_now (120s found): OK")


def test_chain_now_field_on_token_state():
    """TokenState has chain_now field, defaults to None."""
    print("=== chain_now field on TokenState ===")
    ts = TokenState(ca=TOKEN)
    assert ts.chain_now is None, f"Default should be None, got {ts.chain_now}"
    ts.chain_now = 1700000050
    assert ts.chain_now == 1700000050
    print(f"  chain_now defaults None, settable to {ts.chain_now}: OK")


if __name__ == "__main__":
    tests = [
        test_clock_live_mode_no_events,
        test_clock_live_mode_offset_mapping,
        test_clock_live_mode_never_below_last_chain_ts,
        test_clock_replay_mode,
        test_clock_offset_locked_on_first_observe,
        test_no_negative_ages_with_chain_now,
        test_episode_start_equals_earliest_event,
        test_wallet_panel_uses_chain_now,
        test_chain_now_field_on_token_state,
    ]
    passed = 0
    failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            print(f"  FAILED: {t.__name__}: {e}")
            failed += 1
    print(f"\n{'='*50}")
    print(f"Patch B2 tests: {passed} passed, {failed} failed")
    if failed:
        raise SystemExit(1)
