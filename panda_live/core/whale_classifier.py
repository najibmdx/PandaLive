"""Whale behaviour classification for PANDA LIVE.

Event-driven classification of individual whale wallets into one of six
verdict labels based on existing WalletState fields. Called by LiveProcessor
on each flow event, stored on WalletState.whale_verdict.

Verdicts:
    FLIPPING     - Was net buyer, now eroding position with sell momentum confirmed
    DISTRIBUTING - Net seller overall (sell volume > buy volume)
    ACCUMULATING - Net buyer, positive delta_2m — conviction intact
    BUILDING     - Net buyer, delta_2m near zero — slowing but not reversing
    INACTIVE     - No activity in last 120s, not yet silent
    GONE         - is_silent flag set — left the token
"""

from ..models.token_state import TokenState
from ..models.wallet_state import WalletState


# Verdict priority for ranking (1 = most urgent, rendered first)
VERDICT_PRIORITY = {
    "FLIPPING": 1,
    "DISTRIBUTING": 2,
    "ACCUMULATING": 3,
    "BUILDING": 4,
    "INACTIVE": 5,
    "GONE": 6,
}


def classify_whale(ws: WalletState, current_time: int, token_state: TokenState) -> str:
    """Classify a whale's current behaviour from existing WalletState fields.

    Returns one of: FLIPPING | DISTRIBUTING | ACCUMULATING | BUILDING | INACTIVE | GONE
    """
    buy_vol = ws.total_buy_sol
    sell_vol = ws.total_sell_sol

    # GONE: wallet hasn't appeared in this wave and inactive 120s+
    # (wave-aware — prevents flicker on wave reset of is_silent)
    if ws.last_seen < token_state.wave_start_time:
        time_since_last = current_time - ws.last_seen
        if time_since_last > 120:
            return "GONE"

    # GONE: is_silent flag set by EventDrivenPatternDetector
    if ws.is_silent:
        return "GONE"

    # INACTIVE: no recent activity but not yet silent
    time_since_last = current_time - ws.last_seen
    if time_since_last > 120:
        return "INACTIVE"

    # FLIPPING: was a net buyer, now actively selling
    # Requires BOTH: erosion (sell >= buy * 0.30) AND momentum confirmation
    if buy_vol > 0 and sell_vol >= buy_vol * 0.30:
        if _momentum_confirms_sell(ws, current_time):
            return "FLIPPING"

    # DISTRIBUTING: net seller overall
    if sell_vol > buy_vol:
        return "DISTRIBUTING"

    # ACCUMULATING: net buyer, still actively buying (delta_2m positive)
    net = buy_vol - sell_vol
    delta_2m = _compute_delta_2m(ws, current_time)
    if net > 0 and delta_2m > 0:
        return "ACCUMULATING"

    # BUILDING: net buyer, pace slowing
    if net > 0:
        return "BUILDING"

    # Fallback
    return "INACTIVE"


def _momentum_confirms_sell(ws: WalletState, current_time: int) -> bool:
    """Momentum confirmation for FLIPPING detection.

    Weighted majority of last 3 transactions are sells
    AND short-window net delta is negative.
    """
    recent = list(ws.direction_history)[-3:]
    if len(recent) < 2:
        return False

    buy_weight = sum(amt for _, direction, amt in recent if direction == "buy")
    sell_weight = sum(amt for _, direction, amt in recent if direction == "sell")

    weighted_majority_sell = sell_weight > buy_weight
    delta_2m = _compute_delta_2m(ws, current_time)
    short_window_negative = delta_2m < 0

    return weighted_majority_sell and short_window_negative


def _compute_delta_2m(ws: WalletState, current_time: int) -> float:
    """Net SOL change in last 120 seconds."""
    cutoff = current_time - 120
    recent = [
        (amt if direction == "buy" else -amt)
        for ts, direction, amt in ws.direction_history
        if ts >= cutoff
    ]
    return sum(recent)


def get_whale_arrow(verdict: str) -> str:
    """Get directional arrow for whale verdict display."""
    return {
        "FLIPPING": "\u2193",      # ↓
        "DISTRIBUTING": "\u2193",  # ↓
        "ACCUMULATING": "\u2191",  # ↑
        "BUILDING": "\u2192",      # →
        "INACTIVE": "\u2014",      # —
        "GONE": "\u2715",          # ✕
    }.get(verdict, "\u2014")


def get_whale_tier(ws: WalletState) -> int:
    """Determine highest threshold tier crossed this episode."""
    total = ws.total_buy_sol + ws.total_sell_sol
    if ws.whale_cum_15m_fired or total >= 50:
        return 3
    if ws.whale_cum_5m_fired or total >= 25:
        return 2
    if ws.whale_tx_fired or total >= 10:
        return 1
    return 0
