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
        self._compute_capital(verdict, token_state)
        self._compute_exhaustion(verdict, token_state, current_time)
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

        # Sequence display string (e.g. "174->67->18->14")
        if len(cohort_sequence) > 1:
            seq_str = "\u2192".join(str(c) for c in cohort_sequence)
            verdict.wave_trend_detail = f"{seq_str} ({current_wave} wave{'s' if current_wave > 1 else ''})"
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

    def _compute_capital(
        self, verdict: PatternVerdict, token_state: TokenState
    ) -> None:
        """Derive capital verdict from net flow and sell pressure ratio.

        Reads: token_state.compute_net_flow(), compute_sell_ratio()

        Both already computed in TokenState — zero new logic.

        Thresholds are ratio-based (not SOL amounts) so they work across
        all token sizes:

        Sell ratio:
            > 0.65  = strong sell pressure
            > 0.55  = weak sell pressure
            0.45-0.55 = neutral
            < 0.45  = buy pressure
            < 0.35  = strong buy pressure

        Combined with net_flow sign for final label.

        Detail string shows exact numbers so trader can verify.
        """
        total_tx = token_state.buy_tx_count + token_state.sell_tx_count
        if total_tx == 0:
            verdict.capital_verdict = "NEUTRAL"
            verdict.capital_detail = "no transactions yet"
            return

        net_flow = token_state.compute_net_flow()
        sell_ratio = token_state.compute_sell_ratio()
        buy_ratio = 1.0 - sell_ratio

        # Label
        if sell_ratio > 0.65:
            verdict.capital_verdict = "OUTFLOW_STRONG"
        elif sell_ratio > 0.55:
            verdict.capital_verdict = "OUTFLOW_WEAK"
        elif sell_ratio < 0.35:
            verdict.capital_verdict = "INFLOW_STRONG"
        elif sell_ratio < 0.45:
            verdict.capital_verdict = "INFLOW_WEAK"
        else:
            verdict.capital_verdict = "NEUTRAL"

        # Detail string
        net_sign = "+" if net_flow >= 0 else ""
        sell_pct = int(sell_ratio * 100)
        buy_pct = int(buy_ratio * 100)
        verdict.capital_detail = (
            f"{net_sign}{net_flow:.1f} SOL | "
            f"{token_state.buy_tx_count}B / {token_state.sell_tx_count}S "
            f"({buy_pct}% buy / {sell_pct}% sell)"
        )

    def _compute_exhaustion(
        self, verdict: PatternVerdict, token_state: TokenState, current_time: int
    ) -> None:
        """Derive exhaustion label from current wave silent percentage.

        Reads: token_state.wave_early_wallets, wallet_state.is_silent flags
        (same data exhaustion detection reads — no duplication of detection logic,
        just reading the already-computed output)

        Thresholds derived from the same 60% exhaustion threshold already in use,
        expressed as graduated labels:

            < 30%  = NONE
            30-50% = EARLY
            50-70% = SIGNIFICANT  (crosses the 60% trigger point here)
            70-85% = SEVERE
            > 85%  = CRITICAL

        Detail string shows count + wave context.
        """
        wave_early = token_state.wave_early_wallets
        if not wave_early:
            verdict.exhaustion_label = "NONE"
            verdict.exhaustion_pct = 0.0
            verdict.exhaustion_detail = "no early cohort yet"
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
            f"{silent_count}/{total} wave {token_state.current_wave} "
            f"early wallets silent ({int(pct * 100)}%)"
        )
