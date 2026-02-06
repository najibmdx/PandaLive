"""Locked parameters for PANDA LIVE. DO NOT CHANGE."""

# Whale Thresholds (SOL)
WHALE_SINGLE_TX_SOL: float = 10
WHALE_CUM_5MIN_SOL: float = 25
WHALE_CUM_15MIN_SOL: float = 50

# Time Windows (seconds)
WINDOW_1MIN: int = 60
WINDOW_5MIN: int = 300
WINDOW_15MIN: int = 900
EARLY_WINDOW: int = 300  # First 5 minutes after token birth

# Coordination
COORDINATION_MIN_WALLETS: int = 3
COORDINATION_TIME_WINDOW: int = 60  # seconds

# Persistence
PERSISTENCE_MIN_APPEARANCES: int = 2  # distinct 1-min buckets
PERSISTENCE_MAX_GAP: int = 300  # 5 minutes

# Exhaustion
EXHAUSTION_SILENCE_THRESHOLD: int = 180  # 3 minutes
EXHAUSTION_EARLY_WALLET_PERCENT: float = 0.60  # 60%

# Session Logging
LOG_LEVEL_DEFAULT: str = "INTELLIGENCE_ONLY"
LOG_FORMAT: str = "JSONL"
LOG_DIR: str = "logs/"
