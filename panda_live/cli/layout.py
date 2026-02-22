"""Adaptive layout calculation for PANDA LIVE CLI — Upgrade 3.

Calculates panel heights based on terminal dimensions with
breakpoints for large/medium/small/minimal modes.

Adjusted panel heights to accommodate redesigned left pane (18+ lines)
and split right pane (Whale Watch + Emergence Layer).
"""

from typing import Dict

# Fixed allocations
HEADER_ROWS = 4
MIN_EVENT_ROWS = 5
BORDER_OVERHEAD = 6  # Top/bottom borders + separators


def calculate_layout(cols: int, rows: int) -> Dict[str, int]:
    """Calculate panel heights based on terminal dimensions.

    Args:
        cols: Terminal width in columns.
        rows: Terminal height in rows.

    Returns:
        Dict with keys: header, token_panel, wallet_panel, event_stream, cols.
    """
    available = rows - HEADER_ROWS - BORDER_OVERHEAD

    if rows >= 50:
        token_h = 18   # left pane (regime layer — full content)
        wallet_h = 28  # right pane (6 whales x 3 lines + emergence layer)
        event_h = max(10, available - 46)
    elif rows >= 40:
        token_h = 15
        wallet_h = 24
        event_h = max(8, available - 39)
    elif rows >= 30:
        token_h = 12
        wallet_h = 18
        event_h = max(6, available - 30)
    else:
        token_h = 10
        wallet_h = 14
        event_h = max(5, available - 24)

    # Ensure event stream gets at least MIN_EVENT_ROWS
    used = token_h + wallet_h
    if event_h < MIN_EVENT_ROWS:
        event_h = MIN_EVENT_ROWS

    # If we overshot, shrink panels proportionally
    if used + event_h > available + MIN_EVENT_ROWS:
        excess = used + MIN_EVENT_ROWS - available
        token_h = max(8, token_h - excess // 2)
        wallet_h = max(10, wallet_h - (excess - excess // 2))
        event_h = max(MIN_EVENT_ROWS, available - token_h - wallet_h)

    return {
        "header": HEADER_ROWS,
        "token_panel": token_h,
        "wallet_panel": wallet_h,
        "event_stream": event_h,
        "cols": cols,
    }
