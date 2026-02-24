"""Panel formatters for PANDA LIVE CLI — Upgrade 3.

Three-layer intelligence display:
  Left pane  — Token Intelligence (Regime Layer)
  Right pane — Whale Watch (Capital Layer) + Wallet Signals (Emergence Layer)
  Bottom     — Event Stream (intelligence only)
"""

import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from ..config.thresholds import MAX_EVENT_BUFFER_BYTES
from ..core.pattern_analysis import PatternVerdict
from ..core.whale_classifier import (
    VERDICT_PRIORITY,
    get_whale_arrow,
    get_whale_tier,
    _compute_delta_2m,
)
from ..models.events import StateTransitionEvent
from ..models.token_state import TokenState
from ..models.wallet_state import WalletState

# State -> Phase mapping (expanded for Upgrade 3)
_STATE_TO_PHASE = {
    "TOKEN_QUIET": "Dead",
    "TOKEN_IGNITION": "Igniting",
    "TOKEN_COORDINATION_SPIKE": "Coordinating",
    "TOKEN_EARLY_PHASE": "Early Entry",
    "TOKEN_PERSISTENCE_CONFIRMED": "Building",
    "TOKEN_PARTICIPATION_EXPANSION": "Expanding",
    "TOKEN_PRESSURE_PEAKING": "Peak Pressure",
    "TOKEN_EXHAUSTION_DETECTED": "Exhausting",
    "TOKEN_DISSIPATION": "Dying",
}

# Circled number indices for whale ranking
_CIRCLED_NUMBERS = "\u2460\u2461\u2462\u2463\u2464\u2465\u2466\u2467"


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


def _repl_display(token_state: TokenState, verdict: Optional[PatternVerdict],
                  current_time: int) -> str:
    """Compute replacement display with EXIT guard."""
    repl = token_state.compute_replacement(current_time)
    if repl == "YES":
        if verdict and verdict.wave_trend in ("FADING", "COLLAPSING"):
            return "\u26a0 EXIT?"
    return repl


@dataclass
class EmergenceState:
    """Tracks current state of each emergence signal type."""
    new_entry_summary: str = "\u2014"
    new_entry_last: Optional[int] = None
    cluster_summary: str = "\u2014"
    cluster_last: Optional[int] = None
    flip_summary: str = "\u2014"
    flip_last: Optional[int] = None
    inactivity_summary: str = "\u2014"
    inactivity_last: Optional[int] = None
    distribution_summary: str = "\u2014"
    distribution_last: Optional[int] = None


class TokenPanel:
    """Formats token intelligence panel content (Regime Layer)."""

    def render(
        self,
        token_state: TokenState,
        recent_transitions: List[StateTransitionEvent],
        current_time: int,
        max_lines: int = 18,
        verdict: Optional[PatternVerdict] = None,
    ) -> List[str]:
        """Render token intelligence panel — Regime Layer.

        Layout (up to 24 lines):
          Status bar | Divider | blank
          CONVICTION block (2 lines)
          CAPITAL block (3 lines)
          COHORT block (2 lines)
          PEAK WAVE section (3 lines)
          PULSE section (3 lines)
          RECENT TRANSITIONS (up to 4 entries)
        """
        lines: List[str] = []

        # --- Status bar ---
        phase_name = _STATE_TO_PHASE.get(
            token_state.current_state, token_state.current_state.replace("TOKEN_", "")
        )
        wave = token_state.current_wave
        ep = token_state.episode_id
        duration = ""
        if token_state.episode_start:
            dur_sec = current_time - token_state.episode_start
            duration = _format_duration(dur_sec)
        lines.append(f" W:{wave}  |  {phase_name}  |  EP:{ep}  |  {duration}")

        # --- Divider ---
        lines.append(" " + "\u2500" * 50)

        # --- Blank ---
        lines.append("")

        # --- CONVICTION block ---
        if verdict:
            if verdict.cliff_detected:
                # Cliff overrides normal wave_trend display (Change 2)
                lines.append(
                    f" \U0001f534 STRUCTURE BROKEN \u2014 EXIT"
                    f" | {verdict.cliff_from}\u2192{verdict.cliff_to}"
                    f" (-{verdict.cliff_drop_pct}%)"
                )
                lines.append(f"   {verdict.wave_trend_detail}")
            else:
                lines.append(f" \u25c6 CONVICTION:   {verdict.wave_trend}")
                lines.append(f"   {verdict.wave_trend_detail}")
        else:
            lines.append(" \u25c6 CONVICTION:   ANALYZING")
            lines.append("   Waiting for wave data...")

        # --- Entry signal (Change 1) ---
        if verdict and verdict.entry_signal:
            lines.append(
                f" \u26a1 ENTRY WINDOW"
                f" | Buyers: {verdict.entry_buyers}"
                f" | Pressure: {verdict.entry_buy_density:.2f}/s"
            )

        # --- Blank ---
        lines.append("")

        # --- CAPITAL block ---
        buy_vol = token_state.total_buy_volume_sol
        sell_vol = token_state.total_sell_volume_sol
        net_flow = buy_vol - sell_vol
        total_vol = buy_vol + sell_vol
        if verdict:
            lines.append(f" \u25c6 CAPITAL:      {verdict.capital_verdict}")
        else:
            lines.append(" \u25c6 CAPITAL:      NEUTRAL")
        lines.append(
            f"   {net_flow:+.1f} SOL net  |  Buy: {buy_vol:,.0f} SOL  Sell: {sell_vol:,.0f} SOL"
        )
        if total_vol > 0:
            buy_pct = buy_vol / total_vol * 100
            sell_pct = sell_vol / total_vol * 100
            lines.append(f"   Episode pressure:  {buy_pct:.1f}% buy  /  {sell_pct:.1f}% sell")
        else:
            lines.append("   Episode pressure:  —")

        # --- Blank ---
        lines.append("")

        # --- COHORT block ---
        # Rule: "Wave N forming" ONLY when wave_early_wallets is empty.
        # Once cohort has members, always show exhaustion label + silent count.
        total_early = len(token_state.wave_early_wallets)
        if total_early == 0:
            lines.append(f" \u25c6 COHORT:       Wave {wave} forming")
            lines.append(f"   Wave {wave} cohort forming")
        elif verdict and verdict.exhaustion_label:
            lines.append(f" \u25c6 COHORT:       {verdict.exhaustion_label}")
            # Count silent ONLY among wave early wallets (not all active wallets)
            silent_count = sum(
                1 for addr in token_state.wave_early_wallets
                if addr in token_state.active_wallets
                and token_state.active_wallets[addr].is_silent
            )
            pct = int(silent_count / total_early * 100)
            lines.append(f"   {silent_count}/{total_early} ({pct}%) early wallets silent")
        else:
            lines.append(f" \u25c6 COHORT:       NONE")
            lines.append(f"   0/{total_early} (0%) early wallets silent")

        # --- Blank ---
        lines.append("")

        # --- PEAK WAVE section ---
        lines.append(" \u2500\u2500 PEAK WAVE " + "\u2500" * 39)
        if token_state.wave_history:
            peak_record = max(token_state.wave_history,
                              key=lambda r: r.early_wallet_count)
            duration_s = peak_record.end_time - peak_record.start_time
            lines.append(
                f"   W{peak_record.wave_id}: density {peak_record.peak_density:.4f}"
                f"  |  {peak_record.peak_buy_whale_count} buyers  |  {duration_s}s"
            )
            # Current density from live window
            buy_wallets = {e[1] for e in token_state.whale_events_2min if e[2] == "buy"}
            current_density = len(buy_wallets) / 120.0 if buy_wallets else 0.0
            delta_pct = ""
            if peak_record.peak_density > 0:
                delta = int((1 - current_density / peak_record.peak_density) * 100)
                if delta > 0:
                    delta_pct = f"  (-{delta}%)"
                else:
                    delta_pct = f"  (+{abs(delta)}%)"
            lines.append(
                f"   Current vs peak:  W{wave} density {current_density:.4f}{delta_pct}"
            )
        else:
            lines.append("   Peak wave data available after first wave completes.")
            lines.append("")

        # --- Blank ---
        lines.append("")

        # --- PULSE section ---
        lines.append(" \u2500\u2500 PULSE " + "\u2500" * 43)
        whale_ago = "-"
        if token_state.last_whale_timestamp is not None:
            whale_ago = f"{current_time - token_state.last_whale_timestamp}s"
        last_tx_ts = max(
            (ws.last_seen for ws in token_state.active_wallets.values()),
            default=0,
        )
        tx_ago = f"{current_time - last_tx_ts}s" if last_tx_ts > 0 else "-"
        active = len(token_state.active_wallets)
        lines.append(f"   Active: {whale_ago}   Tx: {tx_ago}   Wallets: {active}")

        early_count = len(token_state.early_wallets)
        early_pct = f"({early_count * 100 // active}%)" if active > 0 else "(0%)"
        persistent = sum(
            1 for ws in token_state.active_wallets.values()
            if len(ws.minute_buckets) >= 2
        )
        repl = _repl_display(token_state, verdict, current_time)
        lines.append(
            f"   Early: {early_count} {early_pct}   Persist: {persistent}   Repl: {repl}"
        )

        # --- Blank ---
        lines.append("")

        # --- RECENT TRANSITIONS section ---
        lines.append(" \u2500\u2500 RECENT TRANSITIONS " + "\u2500" * 31)
        if recent_transitions:
            for t in recent_transitions[:4]:
                ts_str = _format_time(t.timestamp)
                from_phase = _STATE_TO_PHASE.get(
                    t.from_state, t.from_state.replace("TOKEN_", "")
                )
                to_phase = _STATE_TO_PHASE.get(
                    t.to_state, t.to_state.replace("TOKEN_", "")
                )
                sev = t.details.get("severity", "")
                sev_str = f"  [{sev}]" if sev else ""
                lines.append(f"   {ts_str}  {from_phase} \u2192 {to_phase}{sev_str}")
        else:
            lines.append("   No transitions yet")

        # Pad to max_lines
        while len(lines) < max_lines:
            lines.append("")

        return lines[:max_lines]


class WalletPanel:
    """Formats Whale Watch (Capital Layer) + Wallet Signals (Emergence Layer)."""

    def __init__(self, wallet_names: Optional[Dict[str, str]] = None) -> None:
        self.wallet_names = wallet_names or {}
        # Session-scoped cluster naming
        self._cluster_id_counter: int = 0
        self._coordination_clusters: Dict[str, str] = {}  # frozenset_key -> CLU:X

    def render(
        self,
        token_state: TokenState,
        wallet_signals: Dict[str, List[str]],
        max_lines: int = 28,
        emergence: Optional[EmergenceState] = None,
        current_time: Optional[int] = None,
    ) -> List[str]:
        """Render Whale Watch + Emergence Layer."""
        lines: List[str] = []
        now = current_time or (
            token_state.chain_now if token_state.chain_now is not None
            else int(time.time())
        )

        # ═══ CAPITAL WATCH (Capital Layer) ═══
        lines.append(" CAPITAL WATCH \u2500 Capital Layer")
        lines.append("")

        # Collect whales with verdicts
        whale_entries = self._rank_whales(token_state, now)

        # Allocate lines: 6 whales x 4 lines each (3 content + 1 blank) = 24 max
        # Reserve 8 lines for emergence layer
        whale_max = max_lines - 9
        whale_rendered = 0

        for idx, (addr, ws, verdict_label) in enumerate(whale_entries[:6]):
            if len(lines) + 4 > whale_max:
                break

            circled = _CIRCLED_NUMBERS[idx] if idx < len(_CIRCLED_NUMBERS) else f"({idx + 1})"
            lines.append(f" {circled} {verdict_label}")
            lines.append(f"   {addr}")

            net = ws.total_buy_sol - ws.total_sell_sol
            delta_2m = _compute_delta_2m(ws, now)
            tier = get_whale_tier(ws)
            age = now - ws.first_seen if ws.first_seen > 0 else 0
            arrow = get_whale_arrow(verdict_label)

            # Cluster tag
            cluster_tag = self._get_cluster_tag(addr, wallet_signals)

            lines.append(
                f"   Net: {net:+.1f} SOL  \u0394\u0032m: {delta_2m:+.1f}"
                f"  B:{ws.total_buy_sol:.1f} S:{ws.total_sell_sol:.1f}"
                f"  T{tier}  entry:{age}s  {cluster_tag}  {arrow}"
            )
            lines.append("")
            whale_rendered += 1

        if whale_rendered == 0:
            lines.append("   No active wallets detected yet")
            lines.append("")

        # Separator
        lines.append(" " + "\u2500" * 50)

        # ═══ WALLET SIGNALS (Emergence Layer) ═══
        lines.append(" WALLET SIGNALS \u2500 Emergence Layer")

        if emergence is None:
            emergence = EmergenceState()

        # NEW ENTRY
        last_str = _format_time(emergence.new_entry_last) if emergence.new_entry_last else "\u2014"
        lines.append(f"   NEW ENTRY      {emergence.new_entry_summary:<36} last: {last_str}")

        # CLUSTER
        last_str = _format_time(emergence.cluster_last) if emergence.cluster_last else "\u2014"
        lines.append(f"   CLUSTER        {emergence.cluster_summary:<36} last: {last_str}")

        # FLIP
        last_str = _format_time(emergence.flip_last) if emergence.flip_last else "\u2014"
        lines.append(f"   FLIP           {emergence.flip_summary:<36} last: {last_str}")

        # INACTIVITY
        last_str = _format_time(emergence.inactivity_last) if emergence.inactivity_last else "\u2014"
        lines.append(f"   INACTIVITY     {emergence.inactivity_summary:<36} last: {last_str}")

        # DISTRIBUTION
        last_str = _format_time(emergence.distribution_last) if emergence.distribution_last else "\u2014"
        lines.append(f"   DISTRIBUTION   {emergence.distribution_summary:<36} last: {last_str}")

        # Pad to max_lines
        while len(lines) < max_lines:
            lines.append("")

        return lines[:max_lines]

    def _rank_whales(
        self, token_state: TokenState, current_time: int
    ) -> List[Tuple[str, WalletState, str]]:
        """Rank whales by verdict priority, net SOL, tier weight."""
        entries = []
        for addr, ws in token_state.active_wallets.items():
            # Only include wallets that have whale events
            if not (ws.whale_tx_fired or ws.whale_cum_5m_fired or ws.whale_cum_15m_fired):
                continue
            verdict_label = ws.whale_verdict
            priority = VERDICT_PRIORITY.get(verdict_label, 5)
            net = abs(ws.total_buy_sol - ws.total_sell_sol)
            tier = get_whale_tier(ws)
            entries.append((priority, -net, -tier, addr, ws, verdict_label))

        entries.sort(key=lambda e: (e[0], e[1], e[2]))
        return [(addr, ws, vl) for _, _, _, addr, ws, vl in entries]

    def _get_cluster_tag(self, addr: str, wallet_signals: Dict[str, List[str]]) -> str:
        """Get cluster tag for a wallet based on coordination signals."""
        signals = wallet_signals.get(addr, [])
        for sig in signals:
            if sig.startswith("COORDINATION"):
                # Temporary: InsightX will provide real cluster IDs in Upgrade 4
                if addr not in self._coordination_clusters:
                    cluster_letter = chr(ord("A") + self._cluster_id_counter % 26)
                    self._cluster_id_counter += 1
                    self._coordination_clusters[addr] = f"CLU:{cluster_letter}"
                return f"[{self._coordination_clusters[addr]}]"
        return "[CLU:-]"


class EventPanel:
    """Formats scrolling event stream — intelligence only."""

    def __init__(self, buffer_size: int = 100) -> None:
        self._events: List[str] = []
        self._buffer_size = buffer_size
        self._buffer_bytes: int = 0
        self._max_buffer_bytes: int = MAX_EVENT_BUFFER_BYTES
        self._last_event_timestamp: Optional[int] = None

    def add_state_transition(self, transition: StateTransitionEvent) -> None:
        """Add a state transition event to the stream (human-readable phases)."""
        ts = _format_time(transition.timestamp)
        from_phase = _STATE_TO_PHASE.get(
            transition.from_state,
            transition.from_state.replace("TOKEN_", "")
        )
        to_phase = _STATE_TO_PHASE.get(
            transition.to_state,
            transition.to_state.replace("TOKEN_", "")
        )
        sev = transition.details.get("severity", "")
        sev_str = f"  [{sev}]" if sev else ""
        # Upgrade 4 — replace "whale"/"whales" in trigger display (Change 4)
        trigger_display = transition.trigger.replace("whales", "buyers").replace("whale", "buyer")
        self._maybe_add_quiet_marker(transition.timestamp)
        self._append(f"[{ts}] \u25c6 {from_phase} \u2192 {to_phase}{sev_str}")

    def add_exhaustion(self, timestamp: int, pct: int, wave_num: int) -> None:
        """Add exhaustion intelligence event to the stream."""
        ts = _format_time(timestamp)
        self._maybe_add_quiet_marker(timestamp)
        self._append(f"[{ts}]   \u26a1 EXHAUSTION FIRED  {pct}% cohort silent  (wave {wave_num})")

    def add_info(self, message: str) -> None:
        """Add an informational message to the stream."""
        ts = _format_time(int(time.time()))
        self._append(f"[{ts}] {message}")

    def render(self, max_lines: int = 10) -> List[str]:
        """Render most recent events (intelligence only).

        Returns:
            List of formatted event strings (newest at bottom).
        """
        lines = [" EVENT STREAM \u2500 intelligence only", ""]
        recent = self._events[-(max_lines - 2):]
        for ev in recent:
            lines.append(f" {ev}")
        while len(lines) < max_lines:
            lines.append("")
        return lines[:max_lines]

    def _maybe_add_quiet_marker(self, timestamp: int) -> None:
        """Insert quiet gap marker when >60s between intelligence events."""
        if self._last_event_timestamp is not None:
            gap = timestamp - self._last_event_timestamp
            if gap > 60:
                mins = gap // 60
                self._append(f"   \u2500\u2500 {mins} min quiet \u2500\u2500")
        self._last_event_timestamp = timestamp

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
