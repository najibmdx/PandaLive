"""Dynamic whale threshold calculator for PANDA LIVE.

Calculates whale thresholds based on token liquidity, not fixed SOL amounts.
This allows PANDA to work correctly for ALL memecoin sizes from $10K to $100M.

Design Philosophy:
- Trader should NOT configure thresholds manually
- System adapts automatically like a trading platform (GMGN/Axiom)
- Whale = % of liquidity pool, not absolute SOL amount

Threshold Logic:
- Whale single TX = 0.5% of pool (someone moving meaningful % of liquidity)
- Whale 5min cumulative = 1% of pool
- Whale 15min cumulative = 2% of pool

Safety Bounds:
- Min thresholds prevent noise on tiny tokens (< $10K)
- Max thresholds catch whales even on huge tokens (> $50M)
"""

from typing import Tuple
from dataclasses import dataclass


# Whale = % of liquidity pool
WHALE_SINGLE_TX_PERCENT = 0.005      # 0.5% of pool
WHALE_CUM_5MIN_PERCENT = 0.01        # 1% in 5min
WHALE_CUM_15MIN_PERCENT = 0.02       # 2% in 15min

# Safety bounds (SOL)
MIN_WHALE_SINGLE = 0.1               # Ignore dust on tiny tokens
MAX_WHALE_SINGLE = 100.0             # Catch whales on huge tokens

MIN_WHALE_5MIN = 0.5
MAX_WHALE_5MIN = 250.0

MIN_WHALE_15MIN = 1.0
MAX_WHALE_15MIN = 500.0

# Fallback if liquidity unknown
DEFAULT_LIQUIDITY_SOL = 50.0


@dataclass
class DynamicThresholds:
    """Whale thresholds calculated from token liquidity."""
    
    whale_single_tx_sol: float
    whale_cum_5min_sol: float
    whale_cum_15min_sol: float
    token_liquidity_sol: float
    
    def __str__(self) -> str:
        return (
            f"DynamicThresholds("
            f"liquidity={self.token_liquidity_sol:.1f} SOL, "
            f"whale_tx={self.whale_single_tx_sol:.2f}, "
            f"whale_5m={self.whale_cum_5min_sol:.2f}, "
            f"whale_15m={self.whale_cum_15min_sol:.2f})"
        )


def calculate_thresholds(token_liquidity_sol: float) -> DynamicThresholds:
    """Calculate whale thresholds from token liquidity.
    
    Args:
        token_liquidity_sol: SOL liquidity in the token's pool.
                            If unknown, use DEFAULT_LIQUIDITY_SOL.
    
    Returns:
        DynamicThresholds with calculated values.
        
    Example:
        >>> # Small token with 30 SOL liquidity
        >>> thresholds = calculate_thresholds(30.0)
        >>> thresholds.whale_single_tx_sol
        0.15  # 30 * 0.005 = 0.15 SOL
        
        >>> # Large token with 5000 SOL liquidity
        >>> thresholds = calculate_thresholds(5000.0)
        >>> thresholds.whale_single_tx_sol
        25.0  # 5000 * 0.005 = 25 SOL
    """
    if token_liquidity_sol <= 0:
        token_liquidity_sol = DEFAULT_LIQUIDITY_SOL
    
    # Calculate from % of pool
    whale_single = token_liquidity_sol * WHALE_SINGLE_TX_PERCENT
    whale_5min = token_liquidity_sol * WHALE_CUM_5MIN_PERCENT
    whale_15min = token_liquidity_sol * WHALE_CUM_15MIN_PERCENT
    
    # Apply bounds
    whale_single = max(MIN_WHALE_SINGLE, min(whale_single, MAX_WHALE_SINGLE))
    whale_5min = max(MIN_WHALE_5MIN, min(whale_5min, MAX_WHALE_5MIN))
    whale_15min = max(MIN_WHALE_15MIN, min(whale_15min, MAX_WHALE_15MIN))
    
    return DynamicThresholds(
        whale_single_tx_sol=whale_single,
        whale_cum_5min_sol=whale_5min,
        whale_cum_15min_sol=whale_15min,
        token_liquidity_sol=token_liquidity_sol,
    )


def estimate_liquidity_from_swaps(swap_transactions: list) -> float:
    """Estimate token liquidity from swap transaction data.
    
    Helius Enhanced API provides pool reserves in swap transactions.
    We can extract this to estimate liquidity without extra API calls.
    
    Args:
        swap_transactions: List of Helius swap transaction dicts.
        
    Returns:
        Estimated SOL liquidity. Returns DEFAULT_LIQUIDITY_SOL if unable to estimate.
        
    Note:
        This is a heuristic. For Pump.fun tokens, the bonding curve
        mechanics mean we can infer liquidity from price impact.
    """
    if not swap_transactions:
        return DEFAULT_LIQUIDITY_SOL
    
    # Try to extract from Helius enhanced data
    # Look for reserve amounts in tokenTransfers or accountData
    
    # Heuristic: Use average swap size as proxy
    # Large average swaps = deeper liquidity
    # This is rough but works for quick estimation
    
    try:
        sol_amounts = []
        for tx in swap_transactions[:20]:  # Sample first 20
            # Try to find native (SOL) transfers
            native_transfers = tx.get("nativeTransfers", [])
            for nt in native_transfers:
                amount = abs(nt.get("amount", 0)) / 1e9  # Lamports to SOL
                if 0.01 < amount < 1000:  # Reasonable swap range
                    sol_amounts.append(amount)
        
        if sol_amounts:
            avg_swap = sum(sol_amounts) / len(sol_amounts)
            # Heuristic: avg swap is ~0.5-2% of liquidity
            # So liquidity ≈ avg_swap / 0.01
            estimated = avg_swap / 0.01
            
            # Sanity bounds: 5-10000 SOL
            estimated = max(5.0, min(estimated, 10000.0))
            return estimated
            
    except (KeyError, TypeError, ValueError):
        pass
    
    return DEFAULT_LIQUIDITY_SOL


# Example usage:
if __name__ == "__main__":
    print("Dynamic Threshold Calculator - Examples\n")
    
    examples = [
        ("Tiny token", 5.0),
        ("Small token (like dogshit)", 30.0),
        ("Medium token", 500.0),
        ("Large token", 5000.0),
        ("Huge token", 50000.0),
    ]
    
    for name, liquidity in examples:
        thresholds = calculate_thresholds(liquidity)
        print(f"{name} ({liquidity} SOL liquidity):")
        print(f"  Whale single TX: {thresholds.whale_single_tx_sol:.2f} SOL")
        print(f"  Whale 5min cum:  {thresholds.whale_cum_5min_sol:.2f} SOL")
        print(f"  Whale 15min cum: {thresholds.whale_cum_15min_sol:.2f} SOL")
        print()
