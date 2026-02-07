"""Panel formatters for PANDA LIVE CLI.

Formats token intelligence, wallet signals, and event stream
into lists of display-ready strings following the Goldilocks principle:
compressed intelligence WITH structural breakdowns.
"""

import time
from typing import Dict, List, Optional, Tuple

from ..models.events import StateTransitionEvent, WalletSignalEvent
from ..models.token_state import TokenState
from ..models.wallet_state import WalletState


def _format_duration(seconds: int) -> str:
    """Format seconds into human-readable duration."""
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    secs = seconds % 60
    if minutes < 60:
        return f"{minutes}m {secs:02d}s"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}h {mins:02d}m"


def _format_time(ts: int) -> str:
    """Format unix timestamp as HH:MM:SS."""
    return time.strftime("%H:%M:%S", time.localtime(ts))


def _short_addr(addr: str) -> str:
    """Shorten a 44-char address for event stream display (first4...last4)."""
    if len(addr) >= 44:
        return f"{addr[:4]}...{addr[-4:]}"
    return addr


class TokenPanel:
    """Formats token intelligence panel content."""

    def render(
        self,
        token_state: TokenState,
        recent_transitions: List[StateTransitionEvent],
        current_time: int,
        max_lines: int = 15,
    ) -> List[str]:
        """Render token intelligence panel.

        Args:
            token_state: Current token state.
            recent_transitions: Most recent transitions (newest first).
            current_time: Current timestamp.
            max_lines: Maximum lines to render.

        Returns:
            List of formatted display strings.
        """
        lines: List[str] = []
        lines.append(" TOKEN INTELLIGENCE")
        lines.append("")

        # State + severity
        state_name = token_state.current_state.replace("TOKEN_", "")
        severity = ""
        if recent_transitions:
            sev = recent_transitions[0].details.get("severity")
            if sev:
                severity = f" [{sev}]"
        lines.append(f" State: {state_name}{severity}")

        # Episode
        lines.append(f" Episode: {token_state.episode_id}")

        # Time in current state
        if token_state.state_changed_at and current_time > token_state.state_changed_at:
            dur = _format_duration(current_time - token_state.state_changed_at)
            lines.append(f" Time in State: {dur}")

        # Episode duration
        if token_state.episode_start:
            ep_dur = _format_duration(current_time - token_state.episode_start)
            lines.append(f" Episode Duration: {ep_dur}")

        lines.append("")

        # Recent transitions
        if recent_transitions:
            lines.append(" Recent Transitions:")
            for t in recent_transitions[: max_lines - len(lines) - 1]:
                ts_str = _format_time(t.timestamp)
                to_short = t.to_state.replace("TOKEN_", "")
                sev = t.details.get("severity", "")
                sev_str = f" [{sev}]" if sev else ""
                lines.append(f" {ts_str} -> {to_short}{sev_str}")

        # Pad to max_lines
        while len(lines) < max_lines:
            lines.append("")

        return lines[:max_lines]


class WalletPanel:
    """Formats wallet signals panel content."""

    def __init__(self, wallet_names: Optional[Dict[str, str]] = None) -> None:
        self.wallet_names = wallet_names or {}

    def render(
        self,
        token_state: TokenState,
        wallet_signals: Dict[str, List[str]],
        max_lines: int = 20,
    ) -> List[str]:
        """Render wallet signals panel.

        Args:
            token_state: Current token state.
            wallet_signals: Mapping wallet_address -> list of signal names.
            max_lines: Maximum lines to render.

        Returns:
            List of formatted display strings.
        """
        lines: List[str] = []
        lines.append(" WALLET SIGNALS")
        lines.append("")

        # Summary line with context
        active = len(token_state.active_wallets)
        early = len(token_state.early_wallets)
        persistent = sum(
            1 for ws in token_state.active_wallets.values()
            if len(ws.minute_buckets) >= 2
        )

        early_pct = f"({early * 100 // active}%)" if active > 0 else ""
        lines.append(f" Active:{active} | Early:{early}{early_pct} | Persist:{persistent}")
        lines.append("")

        # Wallet details with signals
        for addr, signals in sorted(wallet_signals.items(), key=lambda x: len(x[1]), reverse=True):
            if len(lines) >= max_lines - 1:
                break

            name = self.wallet_names.get(addr, "")
            name_str = f" ({name})" if name else ""
            lines.append(f" {addr}{name_str}:")

            if len(lines) >= max_lines:
                break

            # Format signal flags as compact tags
            flags = "".join(f"[{s[:5].upper()}]" for s in signals)
            lines.append(f"   {flags}")

        # Pad to max_lines
        while len(lines) < max_lines:
            lines.append("")

        return lines[:max_lines]


class EventPanel:
    """Formats scrolling event stream."""

    def __init__(self, buffer_size: int = 100) -> None:
        self._events: List[str] = []
        self._buffer_size = buffer_size

    def add_state_transition(self, transition: StateTransitionEvent) -> None:
        """Add a state transition event to the stream."""
        ts = _format_time(transition.timestamp)
        from_short = transition.from_state.replace("TOKEN_", "")
        to_short = transition.to_state.replace("TOKEN_", "")
        sev = transition.details.get("severity", "")
        sev_str = f" [{sev}]" if sev else ""
        self._append(f"[{ts}] STATE: {from_short} -> {to_short}{sev_str}")

    def add_wallet_signal(self, signal: WalletSignalEvent) -> None:
        """Add a wallet signal event to the stream."""
        ts = _format_time(signal.timestamp)
        addr = _short_addr(signal.wallet) if signal.wallet else "TOKEN"
        sigs = ", ".join(signal.signals)
        self._append(f"[{ts}] SIGNAL: {addr} -> {sigs}")

    def add_info(self, message: str) -> None:
        """Add an informational message to the stream."""
        ts = _format_time(int(time.time()))
        self._append(f"[{ts}] {message}")

    def render(self, max_lines: int = 10) -> List[str]:
        """Render most recent events.

        Returns:
            List of formatted event strings (newest at bottom).
        """
        lines = [" EVENT STREAM", ""]
        recent = self._events[-(max_lines - 2):]
        for ev in recent:
            lines.append(f" {ev}")
        while len(lines) < max_lines:
            lines.append("")
        return lines[:max_lines]

    def _append(self, event_str: str) -> None:
        """Append event and enforce buffer size."""
        self._events.append(event_str)
        if len(self._events) > self._buffer_size:
            self._events = self._events[-self._buffer_size:]
