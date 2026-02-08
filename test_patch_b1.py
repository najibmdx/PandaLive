"""Patch B1 verification tests — silent eligibility, replacement, invariants."""

from panda_live.config.thresholds import SILENT_G_MIN_SECONDS, REPLACEMENT_LOOKBACK_SECONDS
from panda_live.models.token_state import TokenState
from panda_live.models.wallet_state import WalletState
from panda_live.core.time_windows import TimeWindowManager
from panda_live.models.events import FlowEvent

TOKEN = "T" * 44


def addr(letter: str, idx: int = 0) -> str:
    base = f"{letter}{idx:03d}"
    return base.ljust(44, "0")


def test_silent_g_min_constant():
    """SILENT_G_MIN_SECONDS is exactly 540 (9 minutes)."""
    print("=== SILENT_G_MIN_SECONDS constant ===")
    assert SILENT_G_MIN_SECONDS == 540
    print(f"  SILENT_G_MIN_SECONDS={SILENT_G_MIN_SECONDS}: OK")


def test_replacement_lookback_constant():
    """REPLACEMENT_LOOKBACK_SECONDS is exactly 300 (5 minutes)."""
    print("=== REPLACEMENT_LOOKBACK_SECONDS constant ===")
    assert REPLACEMENT_LOOKBACK_SECONDS == 300
    print(f"  REPLACEMENT_LOOKBACK_SECONDS={REPLACEMENT_LOOKBACK_SECONDS}: OK")


def test_activity_count_increments():
    """activity_count increments on each add_flow call."""
    print("=== activity_count increments ===")
    mgr = TimeWindowManager()
    ws = WalletState(address=addr("A"))
    assert ws.activity_count == 0

    for i in range(5):
        flow = FlowEvent(ws.address, 1000 + i * 10, "buy", 1.0, f"sig_{i}", TOKEN)
        mgr.add_flow(ws, flow)

    assert ws.activity_count == 5
    print(f"  5 flows -> activity_count={ws.activity_count}: OK")


def test_silent_episode_start_invariant():
    """At episode start, silent must be 0/0 with pct 0.0."""
    print("=== Silent episode-start invariant ===")
    ts = TokenState(ca=TOKEN)
    ts.episode_id = 1
    ts.episode_start = 1000

    # No wallets at all -> 0/0
    sx, sy, pct = ts.compute_silent(1000)
    assert sx == 0 and sy == 0 and pct == 0.0
    print(f"  No wallets at episode start: {sx}/{sy} pct={pct}: OK")

    # Add wallet with activity_count=0 -> still 0/0 (not eligible)
    ws = WalletState(address=addr("A"), first_seen=1000, last_seen=1000)
    ws.activity_count = 0
    ts.active_wallets[ws.address] = ws
    sx, sy, pct = ts.compute_silent(1000)
    assert sx == 0 and sy == 0 and pct == 0.0
    print(f"  Wallet with activity_count=0: {sx}/{sy} pct={pct}: OK")

    # Set activity_count=1 -> eligible, but time=0 (< 540s) -> not silent
    ws.activity_count = 1
    sx, sy, pct = ts.compute_silent(1000)
    assert sx == 0 and sy == 1 and pct == 0.0
    print(f"  Wallet with activity, t=episode_start: {sx}/{sy} pct={pct}: OK")


def test_silent_no_episode():
    """No episode started -> 0/0."""
    print("=== Silent no episode ===")
    ts = TokenState(ca=TOKEN)
    sx, sy, pct = ts.compute_silent(5000)
    assert sx == 0 and sy == 0 and pct == 0.0
    print(f"  No episode: {sx}/{sy} pct={pct}: OK")


def test_silent_eligibility_excludes_zero_activity():
    """Wallets with activity_count=0 excluded from both numerator and denominator."""
    print("=== Silent eligibility exclusion ===")
    ts = TokenState(ca=TOKEN)
    ts.episode_id = 1
    ts.episode_start = 1000

    # Wallet A: activity_count=3, last_seen=1100, went silent
    ws_a = WalletState(address=addr("A"), first_seen=1000, last_seen=1100)
    ws_a.activity_count = 3
    ts.active_wallets[ws_a.address] = ws_a

    # Wallet B: activity_count=0, never actually transacted in episode
    ws_b = WalletState(address=addr("B"), first_seen=1000, last_seen=1000)
    ws_b.activity_count = 0
    ts.active_wallets[ws_b.address] = ws_b

    # At t=2000 (900s since episode start, 900s since A last seen)
    # A: anchor = max(1000, 1000) = 1000, time_since_anchor=1000 >= 540, silence=900 >= 540 -> silent
    # B: excluded (activity_count=0)
    sx, sy, pct = ts.compute_silent(2000)
    assert sy == 1, f"Expected eligible=1, got {sy}"
    assert sx == 1, f"Expected silent=1, got {sx}"
    assert pct == 1.0
    print(f"  A(active) silent, B(zero activity) excluded: {sx}/{sy} pct={pct}: OK")


def test_silent_anchor_uses_max_of_episode_and_first_seen():
    """Silence anchor = max(episode_start, first_seen)."""
    print("=== Silent anchor rule ===")
    ts = TokenState(ca=TOKEN)
    ts.episode_id = 1
    ts.episode_start = 1000

    # Wallet joins late at t=1200, last active at t=1200
    ws = WalletState(address=addr("A"), first_seen=1200, last_seen=1200)
    ws.activity_count = 1
    ts.active_wallets[ws.address] = ws

    # At t=1600: anchor=max(1000, 1200)=1200, time_since_anchor=400 < 540 -> not silent yet
    sx, sy, pct = ts.compute_silent(1600)
    assert sx == 0 and sy == 1
    print(f"  t=1600 (400s since anchor): {sx}/{sy}: not silent yet: OK")

    # At t=1800: anchor=1200, time_since_anchor=600 >= 540, silence=600 >= 540 -> silent
    sx, sy, pct = ts.compute_silent(1800)
    assert sx == 1 and sy == 1
    print(f"  t=1800 (600s since anchor): {sx}/{sy}: silent: OK")


def test_silent_wallet_still_active_not_silent():
    """Wallet with recent activity is not silent even if time_since_anchor >= 540."""
    print("=== Silent wallet still active ===")
    ts = TokenState(ca=TOKEN)
    ts.episode_id = 1
    ts.episode_start = 1000

    ws = WalletState(address=addr("A"), first_seen=1000, last_seen=1800)
    ws.activity_count = 5
    ts.active_wallets[ws.address] = ws

    # At t=1900: anchor=1000, time_since_anchor=900 >= 540
    # But silence_duration = 1900 - 1800 = 100 < 540 -> NOT silent
    sx, sy, pct = ts.compute_silent(1900)
    assert sx == 0 and sy == 1
    print(f"  Active wallet (silence=100s): {sx}/{sy}: not silent: OK")


def test_silent_multiple_wallets():
    """Mixed eligible wallets: some silent, some active, some ineligible."""
    print("=== Silent multiple wallets ===")
    ts = TokenState(ca=TOKEN)
    ts.episode_id = 1
    ts.episode_start = 1000

    # W0: eligible, silent (last_seen=1000, 1000s ago at t=2000)
    ws0 = WalletState(address=addr("W", 0), first_seen=1000, last_seen=1000)
    ws0.activity_count = 2
    ts.active_wallets[ws0.address] = ws0

    # W1: eligible, still active (last_seen=1950, 50s ago)
    ws1 = WalletState(address=addr("W", 1), first_seen=1000, last_seen=1950)
    ws1.activity_count = 3
    ts.active_wallets[ws1.address] = ws1

    # W2: NOT eligible (activity_count=0)
    ws2 = WalletState(address=addr("W", 2), first_seen=1000, last_seen=1000)
    ws2.activity_count = 0
    ts.active_wallets[ws2.address] = ws2

    # W3: eligible, silent (last_seen=1100, 900s ago)
    ws3 = WalletState(address=addr("W", 3), first_seen=1000, last_seen=1100)
    ws3.activity_count = 1
    ts.active_wallets[ws3.address] = ws3

    sx, sy, pct = ts.compute_silent(2000)
    assert sy == 3, f"Expected 3 eligible, got {sy}"  # W0, W1, W3
    assert sx == 2, f"Expected 2 silent, got {sx}"  # W0, W3
    assert pct == 0.67
    print(f"  4 wallets (1 ineligible): {sx}/{sy} pct={pct}: OK")


def test_replacement_yes():
    """Non-early wallet active within 5min -> YES."""
    print("=== Replacement YES ===")
    ts = TokenState(ca=TOKEN, t0=1000)
    ts.early_wallets = {addr("E", 0)}

    # Early wallet
    ws_e = WalletState(address=addr("E", 0), last_seen=1500)
    ts.active_wallets[ws_e.address] = ws_e

    # Non-early wallet, active recently
    ws_r = WalletState(address=addr("R", 0), last_seen=1900)
    ts.active_wallets[ws_r.address] = ws_r

    result = ts.compute_replacement(2000)
    assert result == "YES"
    print(f"  Non-early wallet active 100s ago: {result}: OK")


def test_replacement_no():
    """No non-early wallet active within 5min -> NO."""
    print("=== Replacement NO ===")
    ts = TokenState(ca=TOKEN, t0=1000)
    ts.early_wallets = {addr("E", 0)}

    # Only early wallet
    ws_e = WalletState(address=addr("E", 0), last_seen=1500)
    ts.active_wallets[ws_e.address] = ws_e

    result = ts.compute_replacement(2000)
    assert result == "NO"
    print(f"  Only early wallets: {result}: OK")


def test_replacement_no_stale_non_early():
    """Non-early wallet outside 5min lookback -> NO."""
    print("=== Replacement NO (stale) ===")
    ts = TokenState(ca=TOKEN, t0=1000)
    ts.early_wallets = {addr("E", 0)}

    ws_e = WalletState(address=addr("E", 0), last_seen=1500)
    ts.active_wallets[ws_e.address] = ws_e

    # Non-early wallet, but stale (last_seen 400s ago, >= 300s lookback)
    ws_r = WalletState(address=addr("R", 0), last_seen=1600)
    ts.active_wallets[ws_r.address] = ws_r

    result = ts.compute_replacement(2000)
    assert result == "NO"
    print(f"  Non-early wallet stale (400s ago): {result}: OK")


def test_replacement_empty():
    """No wallets at all -> NO."""
    print("=== Replacement empty ===")
    ts = TokenState(ca=TOKEN)
    result = ts.compute_replacement(2000)
    assert result == "NO"
    print(f"  No wallets: {result}: OK")


def test_pipeline_integration():
    """Full pipeline: flows through TimeWindowManager increment activity_count."""
    print("=== Pipeline integration ===")
    mgr = TimeWindowManager()
    ts = TokenState(ca=TOKEN, t0=1000)
    ts.episode_id = 1
    ts.episode_start = 1000

    # Simulate 3 wallets with varying activity
    addrs = [addr("P", i) for i in range(3)]
    for a in addrs:
        ts.active_wallets[a] = WalletState(address=a)

    # W0: 3 flows
    for i in range(3):
        f = FlowEvent(addrs[0], 1000 + i * 10, "buy", 1.0, f"s0_{i}", TOKEN)
        mgr.add_flow(ts.active_wallets[addrs[0]], f)

    # W1: 1 flow
    f1 = FlowEvent(addrs[1], 1050, "buy", 2.0, "s1_0", TOKEN)
    mgr.add_flow(ts.active_wallets[addrs[1]], f1)

    # W2: 0 flows (never transacted)

    assert ts.active_wallets[addrs[0]].activity_count == 3
    assert ts.active_wallets[addrs[1]].activity_count == 1
    assert ts.active_wallets[addrs[2]].activity_count == 0

    # At episode start -> silent 0/0
    sx, sy, pct = ts.compute_silent(1050)
    assert sx == 0  # nobody has been quiet for 540s yet
    assert sy == 2  # W0 and W1 eligible, W2 excluded
    print(f"  At t=1050: {sx}/{sy} pct={pct}: OK")

    # At t=1600 (550s after W1, 570-590s after W0 flows)
    # W0: anchor=max(1000,1000)=1000, time_since_anchor=600>=540, silence=1600-1020=580>=540 -> silent
    # W1: anchor=max(1000,1050)=1050, time_since_anchor=550>=540, silence=1600-1050=550>=540 -> silent
    sx, sy, pct = ts.compute_silent(1600)
    assert sx == 2 and sy == 2
    print(f"  At t=1600: {sx}/{sy} pct={pct}: OK")

    print("  Pipeline activity_count tracking: OK")


if __name__ == "__main__":
    test_silent_g_min_constant()
    test_replacement_lookback_constant()
    test_activity_count_increments()
    test_silent_episode_start_invariant()
    test_silent_no_episode()
    test_silent_eligibility_excludes_zero_activity()
    test_silent_anchor_uses_max_of_episode_and_first_seen()
    test_silent_wallet_still_active_not_silent()
    test_silent_multiple_wallets()
    test_replacement_yes()
    test_replacement_no()
    test_replacement_no_stale_non_early()
    test_replacement_empty()
    test_pipeline_integration()
    print("\n*** ALL PATCH B1 TESTS PASSED ***")
