"""Panel formatters for PANDA LIVE CLI.

Formats token intelligence, wallet signals, and event stream
into lists of display-ready strings following the Goldilocks principle:
compressed intelligence WITH structural breakdowns.
"""

import time
from typing import Dict, List, Optional, Tuple

from ..config.thresholds import MAX_EVENT_BUFFER_BYTES, MAX_WALLET_LINES
from ..models.events import StateTransitionEvent, WalletSignalEvent
from ..models.token_state import TokenState
from ..models.wallet_state import WalletState

# State -> Phase mapping (deterministic, no logic invented)
_STATE_TO_PHASE = {
    "TOKEN_QUIET": "",
    "TOKEN_IGNITION": "Early",
    "TOKEN_COORDINATION_SPIKE": "Early",
    "TOKEN_EARLY_PHASE": "Early",
    "TOKEN_PERSISTENCE_CONFIRMED": "Building",
    "TOKEN_PARTICIPATION_EXPANSION": "Expanding",
    "TOKEN_PRESSURE_PEAKING": "Peaking",
    "TOKEN_EXHAUSTION_DETECTED": "Dying",
    "TOKEN_DISSIPATION": "Dying",
}

# Max wallets rendered in right pane
_WALLET_DISPLAY_CAP = 4


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

        # Line 1: TOKEN: <mint_short> | EP: <id> | STATE: <STATE> [Sx]
        mint_short = _short_addr(token_state.ca)
        state_name = token_state.current_state.replace("TOKEN_", "")
        severity = ""
        if recent_transitions:
            sev = recent_transitions[0].details.get("severity")
            if sev:
                severity = f" [{sev}]"
        lines.append(f" TOKEN: {mint_short} | EP: {token_state.episode_id} W:{token_state.current_wave} | STATE: {state_name}{severity}")

        # Line 2: Phase
        phase = _STATE_TO_PHASE.get(token_state.current_state, "")
        lines.append(f" Phase: {phase}" if phase else " Phase:")

        # Line 3: blank
        lines.append("")

        # Line 4: Capital: PRESENT | Active: X | Early: X (pct%) | Persist: X
        active = len(token_state.active_wallets)
        active_addrs = set(token_state.active_wallets.keys())
        early_active = len(active_addrs.intersection(token_state.early_wallets))
        persistent = sum(
            1 for ws in token_state.active_wallets.values()
            if len(ws.minute_buckets) >= 2
        )
        early_pct = f"({early_active * 100 // active}%)" if active > 0 else "(0%)"
        lines.append(f" Capital: PRESENT | Active: {active} | Early: {early_active} {early_pct} | Persist: {persistent}")

        # Line 5: Pressure: → | Silent: X/Y | Repl: YES|NO
        silent_x, silent_y, _ = token_state.compute_silent(current_time)
        replacement = token_state.compute_replacement(current_time)
        lines.append(f" Pressure: \u2192 | Silent: {silent_x}/{silent_y} | Repl: {replacement}")

        # Line 6: HB | Whale: Ns | Tx: Ns
        whale_ago = ""
        if token_state.last_whale_timestamp is not None:
            whale_ago = f"{current_time - token_state.last_whale_timestamp}s"
        else:
            whale_ago = "-"
        last_tx_ts = max(
            (ws.last_seen for ws in token_state.active_wallets.values()),
            default=0,
        )
        tx_ago = f"{current_time - last_tx_ts}s" if last_tx_ts > 0 else "-"
        lines.append(f" HB | Whale: {whale_ago} | Tx: {tx_ago}")

        # Line 7: blank
        lines.append("")

        # Line 8+: Recent Transitions
        lines.append(" Recent Transitions:")
        if recent_transitions:
            for t in recent_transitions[: max_lines - len(lines)]:
                ts_str = _format_time(t.timestamp)
                to_short = t.to_state.replace("TOKEN_", "")
                sev = t.details.get("severity", "")
                sev_str = f" [{sev}]" if sev else ""
                lines.append(f" {ts_str} \u2192 {to_short}{sev_str}")

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
        now = token_state.chain_now if token_state.chain_now is not None else int(time.time())

        # Line 1: Active: X | Early: X (pct%) | Persist: X
        active = len(token_state.active_wallets)
        active_addrs = set(token_state.active_wallets.keys())
        early_active = len(active_addrs.intersection(token_state.early_wallets))
        persistent = sum(
            1 for ws in token_state.active_wallets.values()
            if len(ws.minute_buckets) >= 2
        )
        early_pct = f"({early_active * 100 // active}%)" if active > 0 else "(0%)"
        lines.append(f" Active: {active} | Early: {early_active} {early_pct} | Persist: {persistent}")

        # Line 2: blank
        lines.append("")

        # Wallet details with signals (capped to _WALLET_DISPLAY_CAP)
        sorted_wallets = sorted(
            wallet_signals.items(), key=lambda x: len(x[1]), reverse=True
        )
        rendered_count = 0

        for wallet_addr, signals in sorted_wallets:
            if rendered_count >= _WALLET_DISPLAY_CAP:
                break
            if len(lines) >= max_lines - 2:
                break
            if not signals:
                continue

            w_short = _short_addr(wallet_addr)
            lines.append(f" {w_short}:")

            if len(lines) >= max_lines - 1:
                break

            flags = "".join(f"[{s[:5].upper()}]" for s in signals)
            ws = token_state.active_wallets.get(wallet_addr)
            age_str = ""
            if ws and ws.first_seen > 0:
                age = now - ws.first_seen
                age_str = f"   {age}s"
            lines.append(f"   {flags}{age_str}")
            rendered_count += 1

        # Final line: remaining active wallets
        remaining = active - rendered_count
        if remaining > 0:
            lines.append(f" (+{remaining} more active wallets)")

        # Pad to max_lines
        while len(lines) < max_lines:
            lines.append("")

        return lines[:max_lines]


class EventPanel:
    """Formats scrolling event stream."""

    def __init__(self, buffer_size: int = 100) -> None:
        self._events: List[str] = []
        self._buffer_size = buffer_size
        self._buffer_bytes: int = 0
        self._max_buffer_bytes: int = MAX_EVENT_BUFFER_BYTES

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
        """Append event and enforce both count and byte caps."""
        event_bytes = len(event_str.encode("utf-8", errors="replace"))
        self._events.append(event_str)
        self._buffer_bytes += event_bytes

        # Evict oldest until both caps satisfied
        while self._events and (
            len(self._events) > self._buffer_size
            or self._buffer_bytes > self._max_buffer_bytes
        ):
            removed = self._events.pop(0)
            self._buffer_bytes -= len(removed.encode("utf-8", errors="replace"))
