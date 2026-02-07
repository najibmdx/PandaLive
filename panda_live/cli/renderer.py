"""Terminal rendering engine for PANDA LIVE CLI.

Renders adaptive split-screen display with token intelligence (left),
wallet signals (right), and scrolling event stream (bottom).
"""

import os
import sys
import time
from typing import Dict, List, Optional

from ..models.events import StateTransitionEvent, WalletSignalEvent
from ..models.token_state import TokenState
from .layout import calculate_layout
from .panels import EventPanel, TokenPanel, WalletPanel


def _get_terminal_size() -> tuple:
    """Get terminal dimensions, with fallback."""
    try:
        size = os.get_terminal_size()
        return size.columns, size.lines
    except (OSError, ValueError):
        return 120, 40


class CLIRenderer:
    """Renders adaptive split-screen terminal display."""

    def __init__(self, wallet_names: Optional[Dict[str, str]] = None) -> None:
        self.wallet_names = wallet_names or {}
        self.token_panel = TokenPanel()
        self.wallet_panel = WalletPanel(wallet_names=self.wallet_names)
        self.event_panel = EventPanel()
        self._recent_transitions: List[StateTransitionEvent] = []
        self._wallet_signals: Dict[str, List[str]] = {}

    def add_transition(self, transition: StateTransitionEvent) -> None:
        """Record a state transition and add to event stream."""
        self._recent_transitions.insert(0, transition)
        self._recent_transitions = self._recent_transitions[:20]
        self.event_panel.add_state_transition(transition)

    def add_wallet_signal(self, signal: WalletSignalEvent) -> None:
        """Record wallet signals and add to event stream."""
        if signal.signals:
            self._wallet_signals[signal.wallet] = signal.signals
            self.event_panel.add_wallet_signal(signal)

    def add_info(self, message: str) -> None:
        """Add informational message to event stream."""
        self.event_panel.add_info(message)

    def render_frame(
        self,
        token_state: TokenState,
        current_time: int,
    ) -> str:
        """Render a complete display frame.

        Args:
            token_state: Current token state.
            current_time: Current timestamp.

        Returns:
            Complete frame as a single string ready for terminal output.
        """
        cols, rows = _get_terminal_size()
        layout = calculate_layout(cols, rows)

        output_lines: List[str] = []

        # Header
        output_lines.extend(self._render_header(token_state, current_time, cols))

        # Side-by-side panels: token (left) | wallet (right)
        panel_height = max(layout["token_panel"], layout["wallet_panel"])
        left_width = cols // 2 - 1
        right_width = cols - left_width - 3  # 3 for border + separator

        token_lines = self.token_panel.render(
            token_state, self._recent_transitions, current_time, panel_height
        )
        wallet_lines = self.wallet_panel.render(
            token_state, self._wallet_signals, panel_height
        )

        # Top border
        output_lines.append(
            "+" + "-" * left_width + "+" + "-" * right_width + "+"
        )

        # Combine side-by-side
        for i in range(panel_height):
            left = token_lines[i] if i < len(token_lines) else ""
            right = wallet_lines[i] if i < len(wallet_lines) else ""
            left = left[:left_width].ljust(left_width)
            right = right[:right_width].ljust(right_width)
            output_lines.append(f"|{left}|{right}|")

        # Middle border
        output_lines.append(
            "+" + "-" * left_width + "+" + "-" * right_width + "+"
        )

        # Event stream (full width)
        event_lines = self.event_panel.render(layout["event_stream"])
        event_width = cols - 2
        output_lines.append("+" + "-" * event_width + "+")
        for line in event_lines:
            output_lines.append("|" + line[:event_width].ljust(event_width) + "|")
        output_lines.append("+" + "-" * event_width + "+")

        return "\n".join(output_lines)

    def _render_header(
        self, token_state: TokenState, current_time: int, cols: int
    ) -> List[str]:
        """Render the header bar."""
        mint_display = token_state.ca
        if len(mint_display) > 20:
            mint_display = f"{mint_display[:4]}...{mint_display[-4:]}"

        ep_str = f"Episode: {token_state.episode_id}"

        duration = ""
        if token_state.episode_start:
            dur_sec = current_time - token_state.episode_start
            minutes = dur_sec // 60
            secs = dur_sec % 60
            duration = f" | Duration: {minutes}m {secs:02d}s"

        header_text = f" PANDA LIVE | Token: {mint_display} | {ep_str}{duration} "
        border = "=" * (cols - 2)

        return [
            "+" + border + "+",
            "|" + header_text.ljust(cols - 2) + "|",
            "+" + border + "+",
            "",
        ]

    def clear_screen(self) -> None:
        """Clear the terminal screen."""
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()

    def display(self, frame: str) -> None:
        """Write frame to terminal (move cursor to top, overwrite)."""
        sys.stdout.write("\033[H")
        sys.stdout.write(frame)
        sys.stdout.flush()
