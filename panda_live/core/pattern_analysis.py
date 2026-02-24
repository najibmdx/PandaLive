"""Pattern analysis layer for PANDA LIVE.

Synthesizes existing engine intelligence into compressed trader verdicts.
Reads ONLY from TokenState — no new detection, no new heuristics.

Verdicts produced:
  - wave_trend:    BUILDING | STABLE | FADING | COLLAPSING
  - capital:       INFLOW_STRONG | INFLOW_WEAK | NEUTRAL | OUTFLOW_WEAK | OUTFLOW_STRONG
  - exhaustion:    label + percentage + wave context
  - profile:       ANALYZING (placeholder until Upgrade 2.5)
"""

from dataclasses import dataclass
from typing import List, Optional

from ..models.token_state import TokenState


@dataclass
class PatternVerdict:
    """Compressed trader verdict synthesized from engine state.

    All fields have safe defaults — if analysis cannot run (insufficient
    data), the verdict degrades gracefully to neutral/unknown labels
    rather than crashing or producing misleading output.
    """

    # Wave trend
    wave_trend: str = "INSUFFICIENT_DATA"       # BUILDING|STABLE|FADING|COLLAPSING
    wave_trend_detail: str = ""                  # e.g. "174->67->18->14->9->8 (7 waves)"

    # Capital
    capital_verdict: str = "NEUTRAL"             # INFLOW_STRONG|INFLOW_WEAK|NEUTRAL|OUTFLOW_WEAK|OUTFLOW_STRONG
    capital_detail: str = ""                     # e.g. "-197.5 SOL (65% sell pressure)"

    # Exhaustion
    exhaustion_label: str = ""                   # NONE|EARLY|SIGNIFICANT|SEVERE|CRITICAL
    exhaustion_pct: float = 0.0                  # Current wave silent %
    exhaustion_detail: str = ""                  # e.g. "96% of wave 7 early cohort silent"

    # Upgrade 4 — Entry signal (Change 1)
    entry_signal: bool = False                   # True when entry window is live
    entry_buyers: int = 0                        # buyer count at PRESSURE_PEAKING
    entry_buy_density: float = 0.0               # density at PRESSURE_PEAKING

    # Upgrade 4 — Cliff exit signal (Change 2)
    cliff_detected: bool = False                 # True when current wave drop >= 60%
    cliff_from: int = 0                          # Previous wave cohort
    cliff_to: int = 0                            # Current wave cohort
    cliff_drop_pct: int = 0                      # Integer percentage drop

    # Profile (placeholder — Upgrade 2.5)
    profile: str = "ANALYZING"                   # Always ANALYZING until 2.5


class PatternAnalyzer:
    """Synthesizes TokenState into compressed PatternVerdict.

    Called by LiveProcessor on state transitions and at display refresh.
    Reads only from TokenState — zero side effects, zero state mutation.
    """

    def analyze(self, token_state: TokenState, current_time: int) -> PatternVerdict:
        """Produce a PatternVerdict from current engine state.

        Args:
            token_state: Current token state (read-only).
            current_time: Current chain timestamp.

        Returns:
            PatternVerdict with all fields populated.
        """
        verdict = PatternVerdict()

        self._compute_wave_trend(verdict, token_state)
        self._compute_cliff(verdict, token_state)
        self._compute_ghost_gate(verdict, token_state)
        self._compute_capital(verdict, token_state)
        self._compute_exhaustion(verdict, token_state, current_time)
        self._compute_entry_signal(verdict, token_state)
        # profile stays "ANALYZING" — Upgrade 2.5

        return verdict

    def _compute_wave_trend(
        self, verdict: PatternVerdict, token_state: TokenState
    ) -> None:
        """Derive wave trend from wave_history + current wave cohort size.

        Logic (no invented thresholds — pure sequence reading):

        If only 1 wave (no history yet):
            - BUILDING if current wave has 3+ early wallets
            - INSUFFICIENT_DATA otherwise

        If 2+ waves in history:
            - Build cohort sequence: [wave1_count, wave2_count, ..., current_count]
            - Compare last 2 entries (most recent trend matters most)
            - BUILDING:    last > second_last (growing)
            - STABLE:      last == second_last (flat)
            - FADING:      last < second_last AND last > 0 (declining but alive)
            - COLLAPSING:  last == 0 OR last < (second_last * 0.25) (near-zero)

        Detail string always shows the full sequence so trader can see the arc,
        not just the label.
        """
        history = token_state.wave_history  # List[WaveRecord]
        current_cohort = len(token_state.wave_early_wallets)
        current_wave = token_state.current_wave

        # Build full sequence including current wave
        cohort_sequence: List[int] = [
            wr.early_wallet_count for wr in history
        ]
        cohort_sequence.append(current_cohort)

        # Sequence display string (e.g. "79→89→40→57→38→1")
        if len(cohort_sequence) > 1:
            seq_str = "\u2192".join(str(c) for c in cohort_sequence)
            # Peak anchor: identify peak wave and compute decay %
            peak_val = max(cohort_sequence)
            peak_wave = cohort_sequence.index(peak_val) + 1
            current = cohort_sequence[-1]
            if peak_val > 0 and current != peak_val:
                decay_pct = int((1 - current / peak_val) * 100)
                anchor = f"  |  peak W{peak_wave}:{peak_val}  (-{decay_pct}%)"
            else:
                anchor = ""
            verdict.wave_trend_detail = f"{seq_str}{anchor}"
        else:
            verdict.wave_trend_detail = f"{current_cohort} wallets (wave {current_wave})"

        # Insufficient data: only 1 wave with no history
        if len(cohort_sequence) < 2:
            if current_cohort >= 3:
                verdict.wave_trend = "BUILDING"
            else:
                verdict.wave_trend = "INSUFFICIENT_DATA"
            return

        last = cohort_sequence[-1]
        second_last = cohort_sequence[-2]

        if last == 0 or (second_last > 0 and last < second_last * 0.25):
            verdict.wave_trend = "COLLAPSING"
        elif last < second_last:
            verdict.wave_trend = "FADING"
        elif last == second_last:
            verdict.wave_trend = "STABLE"
        else:
            verdict.wave_trend = "BUILDING"

    def _compute_entry_signal(
        self, verdict: PatternVerdict, token_state: TokenState
    ) -> None:
        """Surface entry signal on first PRESSURE_PEAKING in an episode.

        Fires ONCE per episode — after the first PP transition, sets
        entry_signal_fired on token_state so subsequent PP cycles are silent.
        """
        if token_state.current_state != "TOKEN_PRESSURE_PEAKING":
            return
        if token_state.entry_signal_fired:
            return

        verdict.entry_signal = True
        verdict.entry_buyers = token_state.last_pp_buy_count
        verdict.entry_buy_density = token_state.last_pp_buy_density
        token_state.entry_signal_fired = True

    def _compute_cliff(
        self, verdict: PatternVerdict, token_state: TokenState
    ) -> None:
        """Detect structural cliff when wave cohort drops >= 60%.

        Compares the last two entries in wave_history (completed waves).
        Also tracks session peak cohort for ghost wave gating.
        """
        history = token_state.wave_history
        if len(history) < 2:
            # Track session peak even with single wave
            for wr in history:
                if wr.early_wallet_count > token_state.session_peak_cohort:
                    token_state.session_peak_cohort = wr.early_wallet_count
            return

        prev_cohort = history[-2].early_wallet_count
        curr_cohort = history[-1].early_wallet_count

        if prev_cohort > 0:
            drop_pct = (prev_cohort - curr_cohort) / prev_cohort
            if drop_pct >= 0.60:
                verdict.cliff_detected = True
                verdict.cliff_from = prev_cohort
                verdict.cliff_to = curr_cohort
                verdict.cliff_drop_pct = int(drop_pct * 100)
                verdict.wave_trend = "COLLAPSING"
                token_state.session_cliff_fired = True

        # Track session peak across all waves
        for wr in history:
            if wr.early_wallet_count > token_state.session_peak_cohort:
                token_state.session_peak_cohort = wr.early_wallet_count

    def _compute_ghost_gate(
        self, verdict: PatternVerdict, token_state: TokenState
    ) -> None:
        """Gate false BUILDING signals after a cliff has fired.

        After the first cliff in a session, any wave where the cohort is
        below 30% of session peak should display GHOST instead of BUILDING.
        """
        if not token_state.session_cliff_fired:
            return

        peak = token_state.session_peak_cohort
        if peak == 0:
            return

        # Get current cohort from the same sequence used by _compute_wave_trend
        history = token_state.wave_history
        current_cohort = len(token_state.wave_early_wallets)
        cohort_sequence = [wr.early_wallet_count for wr in history]
        cohort_sequence.append(current_cohort)

        current = cohort_sequence[-1] if cohort_sequence else 0
        ghost_threshold = peak * 0.30

        if current < ghost_threshold and verdict.wave_trend == "BUILDING":
            verdict.wave_trend = "GHOST"
            verdict.wave_trend_detail += f"  [below 30% of peak={peak}]"

    def _compute_capital(
        self, verdict: PatternVerdict, token_state: TokenState
    ) -> None:
        """Derive capital verdict from net SOL flow.

        NET FLOW IS THE PRIMARY SIGNAL — ALWAYS.
        Sign of net_flow determines INFLOW vs OUTFLOW. No exceptions.
        Transaction count ratio is displayed as context only.

        STRONG vs WEAK: net flow > 15% of total volume = material (STRONG).
        This ratio threshold works across all token sizes.

        Why net flow, not tx count:
        Memecoins routinely show majority buy *count* with net SOL *outflow*
        because whales exit in large amounts while retail buys in small amounts.
        Count ratio would label this INFLOW_STRONG — a directional lie.
        Net flow cannot be fooled by transaction size asymmetry.
        """
        total_tx = token_state.buy_tx_count + token_state.sell_tx_count
        if total_tx == 0:
            verdict.capital_verdict = "NEUTRAL"
            verdict.capital_detail = "no transactions yet"
            return

        net_flow = token_state.compute_net_flow()
        total_volume = token_state.total_buy_volume_sol + token_state.total_sell_volume_sol

        # STRONG vs WEAK: is net flow material relative to total volume?
        if total_volume > 0:
            flow_ratio = abs(net_flow) / total_volume
            is_strong = flow_ratio >= 0.15
        else:
            is_strong = False

        # PRIMARY: sign of net_flow determines direction
        if net_flow > 0:
            verdict.capital_verdict = "INFLOW_STRONG" if is_strong else "INFLOW_WEAK"
        elif net_flow < 0:
            verdict.capital_verdict = "OUTFLOW_STRONG" if is_strong else "OUTFLOW_WEAK"
        else:
            verdict.capital_verdict = "NEUTRAL"

        # Detail string: exact numbers + tx count context
        net_sign = "+" if net_flow >= 0 else ""
        sell_ratio = token_state.compute_sell_ratio()
        sell_pct = int(sell_ratio * 100)
        buy_pct = 100 - sell_pct
        verdict.capital_detail = (
            f"{net_sign}{net_flow:.1f} SOL | "
            f"{token_state.buy_tx_count}B / {token_state.sell_tx_count}S "
            f"({buy_pct}% buy / {sell_pct}% sell)"
        )

    def _compute_exhaustion(
        self, verdict: PatternVerdict, token_state: TokenState, current_time: int
    ) -> None:
        """Derive exhaustion label from current wave silent percentage.

        Reads: token_state.wave_early_wallets, wallet_state.is_silent flags.
        These are pre-computed by EventDrivenPatternDetector — no detection here.

        Empty wave_early_wallets has two meanings:
        - Wave 1, no whales yet          -> "Wave 1 cohort forming"
        - Wave N>1, post-reset forming   -> "Wave N cohort forming"
        Both are handled by checking current_wave.

        Exhaustion labels graduated from the existing 60% threshold:
            < 30%  = NONE
            30-50% = EARLY
            50-70% = SIGNIFICANT   (60% trigger point falls here)
            70-85% = SEVERE
            > 85%  = CRITICAL
        """
        wave_early = token_state.wave_early_wallets
        wave_num = token_state.current_wave

        # Empty cohort — always means "forming", never "none yet"
        if not wave_early:
            verdict.exhaustion_label = "NONE"
            verdict.exhaustion_pct = 0.0
            verdict.exhaustion_detail = f"Wave {wave_num} cohort forming"
            return

        silent_count = sum(
            1 for addr in wave_early
            if addr in token_state.active_wallets
            and token_state.active_wallets[addr].is_silent
        )
        total = len(wave_early)
        pct = silent_count / total if total > 0 else 0.0

        verdict.exhaustion_pct = round(pct, 2)

        if pct >= 0.85:
            verdict.exhaustion_label = "CRITICAL"
        elif pct >= 0.70:
            verdict.exhaustion_label = "SEVERE"
        elif pct >= 0.50:
            verdict.exhaustion_label = "SIGNIFICANT"
        elif pct >= 0.30:
            verdict.exhaustion_label = "EARLY"
        else:
            verdict.exhaustion_label = "NONE"

        verdict.exhaustion_detail = (
            f"{silent_count}/{total} wave {wave_num} "
            f"early wallets silent ({int(pct * 100)}%)"
        )
