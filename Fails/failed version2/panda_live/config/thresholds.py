"""
PANDA LIVE Configuration - All Locked Parameters

These values are FROZEN based on approved specification.
Do not modify without explicit approval.
"""

# ==============================================================================
# WHALE THRESHOLDS (Micro-time adjusted for memecoin speed)
# ==============================================================================

WHALE_SINGLE_TX_SOL = 10.0
"""Single transaction threshold: 10 SOL"""

WHALE_CUM_5MIN_SOL = 25.0
"""5-minute cumulative threshold: 25 SOL (not 20-30 range)"""

WHALE_CUM_15MIN_SOL = 50.0
"""15-minute cumulative threshold: 50 SOL (not 40-60 range)"""


# ==============================================================================
# TIME WINDOWS (Seconds)
# ==============================================================================

WINDOW_1MIN = 60
"""1-minute window for bucket tracking"""

WINDOW_5MIN = 300
"""5-minute window for cumulative detection"""

WINDOW_15MIN = 900
"""15-minute window for cumulative detection"""

EARLY_WINDOW = 300
"""First 5 minutes after t0 = "early" window"""


# ==============================================================================
# COORDINATION DETECTION
# ==============================================================================

COORDINATION_MIN_WALLETS = 3
"""Minimum wallets required for coordination signal (not 2)"""

COORDINATION_TIME_WINDOW = 60
"""Time window for coordination detection: 60 seconds"""


# ==============================================================================
# PERSISTENCE DETECTION
# ==============================================================================

PERSISTENCE_MIN_APPEARANCES = 2
"""Minimum distinct 1-minute buckets for persistence"""

PERSISTENCE_MAX_GAP = 300
"""Maximum gap between appearances: 5 minutes"""


# ==============================================================================
# EXHAUSTION DETECTION
# ==============================================================================

EXHAUSTION_SILENCE_THRESHOLD = 180
"""Minimum silence duration for disengagement: 3 minutes"""

EXHAUSTION_EARLY_WALLET_PERCENT = 0.60
"""Percentage of early wallets that must be disengaged: 60%"""


# ==============================================================================
# PRESSURE PEAKING
# ==============================================================================

PRESSURE_PEAKING_MIN_WHALES = 5
"""Minimum whales in window for pressure peaking"""

PRESSURE_PEAKING_WINDOW = 120
"""Time window for pressure peaking: 2 minutes"""


# ==============================================================================
# EPISODE BOUNDARIES
# ==============================================================================

EPISODE_END_SILENCE = 600
"""Silence duration that ends episode and triggers QUIET: 10 minutes"""

EPISODE_REIGNITION_GAP = 600
"""Gap < 10 min = same episode, >= 10 min = new episode"""


# ==============================================================================
# SESSION LOGGING
# ==============================================================================

LOG_LEVEL_DEFAULT = "INTELLIGENCE_ONLY"
"""Default log level: INTELLIGENCE_ONLY (signals + states only)"""

LOG_LEVEL_OPTIONS = ["FULL", "INTELLIGENCE_ONLY", "MINIMAL"]
"""Available log levels"""

EVENT_BUFFER_SIZE = 100
"""Event stream buffer size for CLI display"""


# ==============================================================================
# CLI RENDERING
# ==============================================================================

PANEL_REFRESH_RATE = 5.0
"""Panel refresh rate in seconds"""

TERMINAL_MINIMUM = (80, 24)
"""Minimum supported terminal size (cols, rows)"""

HEARTBEAT_OPTIONAL = True
"""Heartbeat emission is opt-in via --heartbeat flag"""
