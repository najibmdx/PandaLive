"""Event-driven pattern detector for PANDA LIVE.

Data-driven behavioral pattern detection using thresholds mined from 7GB
historical database (masterwalletsdb.db).

Patterns trigger on EVENTS (wallet trades, state changes) not TIMERS.
"""

from typing import Dict, Tuple
from collections import deque

# DATA-DRIVEN THRESHOLDS (mined from 7GB database)
# Source: patterns_report.json from masterwalletsdb.db analysis

# Cohort comparison window: P75 gap = 120s (2 minutes)
# 75% of trading gaps are shorter than this
COHORT_WINDOW_SECONDS = 120  # 2 minutes

# Activity drop threshold: P75 drop = 88.2% (use 85% conservative)
# 75% of wallets drop 88%+ activity when going silent
ACTIVITY_DROP_THRESHOLD = 0.85  # 85% drop

# Trade history window for rate calculation (5 minutes)
TRADE_HISTORY_WINDOW = 300  # 5 minutes

# Minimum trades needed to calculate meaningful rates
MIN_TRADES_FOR_RATE = 5


class EventDrivenPatternDetector:
    """
    Detect behavioral patterns using event-driven architecture.
    
    Triggered by EVENTS (wallet trades, state changes), not timers.
    Uses data-driven thresholds from historical analysis.
    """
    
    def __init__(self):
        """Initialize pattern detector."""
        pass
    
    def on_wallet_trade(
        self,
        wallet_state,
        current_time: int,
        token_state,
        direction: str = "buy"
    ) -> None:
        """
        EVENT TRIGGER: Wallet just traded.

        Update wallet's activity metrics for future pattern detection.
        CRITICAL: A sell does NOT mean the wallet is "actively participating."
        A sell means the wallet is EXITING. Only buys reset silent status.

        Args:
            wallet_state: WalletState that just traded
            current_time: Current timestamp
            token_state: Global token state
            direction: "buy" or "sell"
        """
        # Update trade history
        wallet_state.trade_history.append(current_time)
        wallet_state.lifetime_trade_count += 1

        # Clean old history (keep last 5 minutes)
        cutoff = current_time - TRADE_HISTORY_WINDOW
        while wallet_state.trade_history and wallet_state.trade_history[0] < cutoff:
            wallet_state.trade_history.popleft()

        # DIRECTION-AWARE SILENT LOGIC
        # Buy = active participation — mark as not silent
        # Sell = exiting — do NOT reset silent status
        if direction == "buy":
            wallet_state.is_silent = False
            wallet_state.silent_pattern = ""
    
    def on_token_activity(
        self,
        token_state,
        current_time: int
    ) -> Dict[str, Tuple[bool, str]]:
        """
        EVENT TRIGGER: Token has activity (any wallet just traded).
        
        Check ALL wallets using COHORT COMPARISON pattern:
        - Token is active (someone just traded)
        - Which wallets haven't participated recently?
        - Mark non-participants as silent
        
        This is the CORE event-driven pattern - triggered by token activity,
        uses cohort comparison (not time triggers).
        
        Args:
            token_state: Global token state with all wallets
            current_time: Current timestamp
            
        Returns:
            Dict mapping wallet_address -> (is_silent, pattern)
        """
        results = {}
        
        # Cohort comparison window (2 minutes from data)
        recent_cutoff = current_time - COHORT_WINDOW_SECONDS
        
        # Check each wallet
        for wallet_addr, wallet_state in token_state.active_wallets.items():
            # Skip wallets with no activity
            if wallet_state.activity_count == 0:
                continue
            
            # PATTERN: COHORT COMPARISON
            # Has wallet participated in recent token activity?
            if wallet_state.last_seen < recent_cutoff:
                # Token is active (we're in this function because activity happened)
                # But this wallet hasn't traded in 2+ minutes
                # BEHAVIORAL: Wallet stopped while others continue

                if not wallet_state.is_silent:  # New detection
                    wallet_state.is_silent = True
                    wallet_state.silent_pattern = "COHORT_COMPARISON"
                    wallet_state.silent_since = current_time

                results[wallet_addr] = (True, "COHORT_COMPARISON")
            else:
                # Wallet has recent activity — but what kind?
                # DIRECTION-AWARE: only BUY activity counts as "participation"
                # A wallet that is only selling is EXITING, not re-engaging.
                # If it was already silent and its last action was a sell,
                # keep it silent — selling doesn't prove active participation.
                if wallet_state.is_silent and wallet_state.last_direction == "sell":
                    results[wallet_addr] = (True, wallet_state.silent_pattern)
                else:
                    if wallet_state.is_silent:  # Was silent, now actively buying
                        wallet_state.is_silent = False
                        wallet_state.silent_pattern = ""
                    results[wallet_addr] = (False, "ACTIVE")
        
        return results
    
    def on_state_transition(
        self,
        token_state,
        new_state: str,
        current_time: int
    ) -> Dict[str, Tuple[bool, str]]:
        """
        EVENT TRIGGER: Token state changed.
        
        Check wallets using LIFECYCLE POSITION pattern:
        - When reaching PRESSURE_PEAKING, check which wallets stopped before peak
        - Instant detection (no waiting)
        
        Args:
            token_state: Global token state
            new_state: New state token transitioned to
            current_time: Current timestamp
            
        Returns:
            Dict mapping wallet_address -> (is_silent, pattern)
        """
        results = {}
        
        # PATTERN: STOPPED BEFORE PEAK
        # When token reaches pressure peak, mark wallets that stopped earlier
        if new_state == "TOKEN_PRESSURE_PEAKING":
            peak_time = current_time
            
            for wallet_addr, wallet_state in token_state.active_wallets.items():
                if wallet_state.activity_count == 0:
                    continue
                
                # Did wallet stop trading BEFORE peak?
                if wallet_state.last_seen < peak_time:
                    # BEHAVIORAL: Wallet exited before pressure built
                    
                    if not wallet_state.is_silent:  # New detection
                        wallet_state.is_silent = True
                        wallet_state.silent_pattern = "STOPPED_BEFORE_PEAK"
                        wallet_state.silent_since = current_time
                    
                    results[wallet_addr] = (True, "STOPPED_BEFORE_PEAK")
                else:
                    # Wallet still active at peak
                    results[wallet_addr] = (False, "ACTIVE_AT_PEAK")
        
        return results
    
    def check_activity_drop(
        self,
        wallet_state,
        current_time: int
    ) -> Tuple[bool, str, float]:
        """
        OPTIONAL PATTERN: Activity Drop
        
        Check if wallet's trading rate dropped significantly.
        This can be called on wallet trade events for additional detection.
        
        Args:
            wallet_state: Wallet to check
            current_time: Current timestamp
            
        Returns:
            (is_silent, pattern, drop_percentage)
        """
        # Need enough history
        if wallet_state.lifetime_trade_count < MIN_TRADES_FOR_RATE:
            return False, "INSUFFICIENT_HISTORY", 0.0
        
        # Calculate historical rate (lifetime)
        if wallet_state.first_seen == 0:
            return False, "NO_HISTORY", 0.0
        
        lifetime_duration = (current_time - wallet_state.first_seen) / 60  # minutes
        if lifetime_duration < 1:
            return False, "TOO_RECENT", 0.0
        
        historical_rate = wallet_state.lifetime_trade_count / lifetime_duration
        
        # Calculate recent rate (last 3 minutes)
        recent_cutoff = current_time - 180  # 3 minutes
        recent_trades = [t for t in wallet_state.trade_history if t >= recent_cutoff]
        
        if len(recent_trades) < 2:
            recent_rate = 0.0
        else:
            recent_duration = (recent_trades[-1] - recent_trades[0]) / 60
            if recent_duration < 0.5:
                return False, "RECENT_TOO_SHORT", 0.0
            recent_rate = len(recent_trades) / recent_duration
        
        # Check activity drop
        if historical_rate < 0.5:  # Need meaningful baseline
            return False, "LOW_BASELINE", 0.0
        
        drop_pct = (historical_rate - recent_rate) / historical_rate
        
        # DATA-DRIVEN THRESHOLD: 85% drop
        if drop_pct >= ACTIVITY_DROP_THRESHOLD:
            return True, "ACTIVITY_DROP", drop_pct
        
        return False, "ACTIVE", drop_pct
    
    def compute_silent_metrics(
        self,
        token_state,
        current_time: int
    ) -> Tuple[int, int, float]:
        """
        Compute silent wallet counts for display.
        
        Uses pre-computed is_silent flags (set by event-driven detection).
        This is called periodically for UI updates, but detection happens
        on events (not here).
        
        Args:
            token_state: Global token state
            current_time: Current timestamp
            
        Returns:
            (silent_count, total_count, silent_percentage)
        """
        # Count wallets with activity
        eligible = [
            ws for ws in token_state.active_wallets.values()
            if ws.activity_count >= 1
        ]
        
        if not eligible:
            return 0, 0, 0.0
        
        # Count silent wallets (using pre-computed flag)
        silent_count = sum(1 for ws in eligible if ws.is_silent)
        total_count = len(eligible)
        
        silent_pct = (silent_count / total_count) if total_count > 0 else 0.0
        
        return silent_count, total_count, silent_pct
