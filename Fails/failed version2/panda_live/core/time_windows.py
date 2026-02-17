"""
PANDA LIVE Time Windows

Manages rolling time windows for cumulative detection.
"""

from panda_live.models.token_state import TokenState
from panda_live.models.events import FlowEvent
from panda_live.config.thresholds import WINDOW_5MIN, WINDOW_15MIN


class TimeWindowManager:
    """
    Manages rolling time windows for all wallets within a token.
    """
    
    def __init__(self, token_state: TokenState):
        self.token_state = token_state
    
    def process_flow(self, flow: FlowEvent):
        """
        Process a flow event and update time windows.
        
        Args:
            flow: FlowEvent to process
        """
        # Get or create wallet state
        wallet = self.token_state.get_or_create_wallet(flow.wallet)
        
        # Add flow to rolling windows
        wallet.add_flow(flow.timestamp, flow.amount_sol)
        
        # Expire old flows from windows
        wallet.expire_windows(flow.timestamp)
    
    def expire_all_windows(self, current_time: int):
        """
        Expire old flows from all wallet windows.
        
        Args:
            current_time: Current timestamp
        """
        for wallet in self.token_state.active_wallets.values():
            wallet.expire_windows(current_time)
    
    def get_cumulative_5min(self, wallet_address: str) -> float:
        """
        Get current 5-minute cumulative sum for a wallet.
        
        Args:
            wallet_address: Wallet address
        
        Returns:
            Cumulative SOL in last 5 minutes
        """
        wallet = self.token_state.active_wallets.get(wallet_address)
        if not wallet:
            return 0.0
        return wallet.cumulative_5min
    
    def get_cumulative_15min(self, wallet_address: str) -> float:
        """
        Get current 15-minute cumulative sum for a wallet.
        
        Args:
            wallet_address: Wallet address
        
        Returns:
            Cumulative SOL in last 15 minutes
        """
        wallet = self.token_state.active_wallets.get(wallet_address)
        if not wallet:
            return 0.0
        return wallet.cumulative_15min
