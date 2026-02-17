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
# REMOVED: EXHAUSTION_SILENCE_THRESHOLD (replaced by event-driven detection)
# Silent detection now uses EventDrivenPatternDetector with data-driven thresholds:
#   - COHORT_WINDOW_SECONDS = 120s (2min, P75 from 7GB database)
#   - Triggered by EVENTS (wallet trades, state changes), not timers
EXHAUSTION_EARLY_WALLET_PERCENT: float = 0.60  # 60%

# Episode tracking
EPISODE_END_SILENCE: int = 600  # 10 minutes
EPISODE_REIGNITION_GAP: int = 600  # <10min = same episode, >=10min = new

# Pressure peaking
PRESSURE_PEAKING_MIN_WHALES: int = 5
PRESSURE_PEAKING_WINDOW: int = 120  # 2 minutes

# Dissipation
DISSIPATION_WHALE_THRESHOLD: int = 1  # <1 whale per lookback = dissipation
DISSIPATION_LOOKBACK: int = 300  # 5 minutes

# Scalability Caps
MAX_ACTIVE_WALLETS: int = 200
MAX_WALLET_LINES: int = 10
MAX_EVENT_BUFFER_BYTES: int = 256_000  # 256 KB
MAX_DETAIL_CHARS: int = 320
COORDINATION_SAMPLE_WALLETS: int = 5

# Session Logging
LOG_LEVEL_DEFAULT: str = "INTELLIGENCE_ONLY"
LOG_FORMAT: str = "JSONL"
LOG_DIR: str = "logs/"
