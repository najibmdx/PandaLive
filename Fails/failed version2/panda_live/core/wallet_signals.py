"""
PANDA LIVE Wallet Signals Detection

Detects 4 wallet signal types:
1. TIMING - Early vs late appearance
2. COORDINATION - Acting with 2+ other wallets within 60s
3. PERSISTENCE - Re-appearing across 2+ minute buckets within 5min
4. EXHAUSTION - Early wallet goes silent while others remain active
"""

from typing import List, Set, Dict, Optional
from panda_live.models.token_state import TokenState
from panda_live.models.events import WhaleEvent, WalletSignalEvent
from panda_live.config.thresholds import (
    EARLY_WINDOW,
    COORDINATION_MIN_WALLETS,
    COORDINATION_TIME_WINDOW,
    PERSISTENCE_MIN_APPEARANCES,
    PERSISTENCE_MAX_GAP,
    EXHAUSTION_SILENCE_THRESHOLD,
    EXHAUSTION_EARLY_WALLET_PERCENT
)


class WalletSignalsDetector:
    """
    Detects wallet signals based on whale events and wallet behavior.
    """
    
    def __init__(self, token_state: TokenState):
        self.token_state = token_state
    
    def detect_signals(self, whale_event: WhaleEvent, current_time: int) -> Optional[WalletSignalEvent]:
        """
        Detect wallet signals for a whale event.
        
        Args:
            whale_event: WhaleEvent that triggered signal detection
            current_time: Current timestamp
        
        Returns:
            WalletSignalEvent if any signals detected, None otherwise
        """
        wallet_addr = whale_event.wallet
        wallet = self.token_state.get_or_create_wallet(wallet_addr)
        
        signals = []
        details = {}
        
        # 1. TIMING SIGNAL
        timing_signal = self._detect_timing(wallet, whale_event.timestamp)
        if timing_signal:
            signals.append("TIMING")
            details.update(timing_signal)
        
        # 2. COORDINATION SIGNAL
        coord_signal = self._detect_coordination(wallet_addr, whale_event.timestamp)
        if coord_signal:
            signals.append("COORDINATION")
            details.update(coord_signal)
        
        # 3. PERSISTENCE SIGNAL
        persist_signal = self._detect_persistence(wallet, current_time)
        if persist_signal:
            signals.append("PERSISTENCE")
            details.update(persist_signal)
        
        # No signals detected
        if not signals:
            return None
        
        return WalletSignalEvent(
            wallet=wallet_addr,
            timestamp=whale_event.timestamp,
            signals=signals,
            details=details,
            token_ca=self.token_state.ca,
            episode_id=self.token_state.episode_id
        )
    
    def detect_exhaustion(self, current_time: int) -> bool:
        """
        Detect exhaustion signal (token-level, not per-wallet).
        
        Exhaustion = 60%+ early wallets silent AND no replacement whales.
        
        Args:
            current_time: Current timestamp
        
        Returns:
            True if exhaustion detected
        """
        # Must have early wallets
        if not self.token_state.early_wallets:
            return False
        
        # Count disengaged early wallets
        disengagement_pct = self.token_state.get_disengagement_percentage(
            current_time,
            EXHAUSTION_SILENCE_THRESHOLD
        )
        
        # Check if >= 60% disengaged
        if disengagement_pct < EXHAUSTION_EARLY_WALLET_PERCENT:
            return False
        
        # Check for replacement whales (non-early whales in last 5min)
        recent_whales = self.token_state.get_recent_whale_wallets(300, current_time)
        replacement_whales = recent_whales - self.token_state.early_wallets
        
        # Exhaustion = high disengagement AND no replacement
        return len(replacement_whales) == 0
    
    def _detect_timing(self, wallet, timestamp: int) -> Optional[Dict]:
        """
        Detect TIMING signal (early appearance).
        
        Early = wallet appeared within EARLY_WINDOW (300s) of token birth (t0).
        For mid-flight start: relative early (within 300s of observation start).
        """
        # Check if already marked early
        if wallet.is_early:
            return {
                "is_early": True,
                "early_type": "confirmed"
            }
        
        # Check if token has t0 set
        if self.token_state.t0 == 0:
            # No t0 yet, this IS t0
            self.token_state.set_t0(timestamp)
            wallet.is_early = True
            self.token_state.early_wallets.add(wallet.address)
            return {
                "is_early": True,
                "early_type": "token_birth"
            }
        
        # Check if within early window
        time_since_t0 = timestamp - self.token_state.t0
        
        if time_since_t0 <= EARLY_WINDOW:
            wallet.is_early = True
            self.token_state.early_wallets.add(wallet.address)
            return {
                "is_early": True,
                "early_type": "relative",
                "seconds_after_t0": time_since_t0
            }
        
        return None
    
    def _detect_coordination(self, wallet_addr: str, timestamp: int) -> Optional[Dict]:
        """
        Detect COORDINATION signal.
        
        Coordination = 3+ wallets (including this one) had whale events within 60s.
        """
        # Get wallets with whale events in coordination window
        coordinated_wallets = self.token_state.get_recent_whale_wallets(
            COORDINATION_TIME_WINDOW,
            timestamp
        )
        
        # Must have at least 3 wallets (including current)
        if len(coordinated_wallets) >= COORDINATION_MIN_WALLETS:
            # Mark wallet as coordinated
            wallet = self.token_state.active_wallets.get(wallet_addr)
            if wallet:
                wallet.is_coordinated = True
            
            # Get other wallets (exclude current)
            other_wallets = [w for w in coordinated_wallets if w != wallet_addr]
            
            return {
                "is_coordinated": True,
                "coordinated_with": other_wallets,
                "coordination_window": f"{COORDINATION_TIME_WINDOW}s",
                "total_coordinated_wallets": len(coordinated_wallets)
            }
        
        return None
    
    def _detect_persistence(self, wallet, current_time: int) -> Optional[Dict]:
        """
        Detect PERSISTENCE signal.
        
        Persistence = wallet appeared in >= 2 distinct 1-minute buckets within 5min gap.
        """
        # Check if already marked persistent
        if wallet.is_persistent:
            return {
                "is_persistent": True,
                "appearances": len(wallet.minute_buckets)
            }
        
        # Check persistence condition
        is_persistent = wallet.check_persistence(current_time, PERSISTENCE_MAX_GAP)
        
        if is_persistent:
            wallet.is_persistent = True
            
            sorted_buckets = sorted(wallet.minute_buckets)
            time_span = (sorted_buckets[-1] - sorted_buckets[0]) * 60
            
            return {
                "is_persistent": True,
                "appearances": len(wallet.minute_buckets),
                "time_span_seconds": time_span
            }
        
        return None
