# PANDA LIVE - MASTER HANDOVER DOCUMENT
## Session Date: February 7, 2026
## Status: Phase 1-4 Complete, Critical Bug Discovered

---

# TABLE OF CONTENTS

1. [Executive Summary](#executive-summary)
2. [System Context & Purpose](#system-context--purpose)
3. [Sacred Principles](#sacred-principles)
4. [Complete System Architecture](#complete-system-architecture)
5. [Locked Parameters](#locked-parameters)
6. [Phase-by-Phase Implementation](#phase-by-phase-implementation)
7. [Severity System (Phase 3.5)](#severity-system-phase-35)
8. [CLI Design](#cli-design)
9. [Current Status](#current-status)
10. [Critical Bug Discovered](#critical-bug-discovered)
11. [Repository Information](#repository-information)
12. [Next Steps](#next-steps)

---

# EXECUTIVE SUMMARY

**PANDA LIVE is a real-time memecoin situational awareness system for Solana.**

**NOT:** Prediction engine, signal generator, telemetry dashboard
**IS:** Compressed intelligence showing "what's happening with this token RIGHT NOW"

**Implementation Status:**
- ✅ Phase 1: Core primitives (flow, windows, whale detection)
- ✅ Phase 2: Wallet signals (TIMING, COORDINATION, PERSISTENCE, EXHAUSTION)
- ✅ Phase 3: Token state machine (9 states, episodes, density)
- ✅ Phase 3.5: Severity system (S1-S5 ordinal annotation)
- ✅ Phase 4: CLI + Helius integration
- ❌ **CRITICAL BUG:** Coordination signal bloat causes hangs on active tokens

**Repository:** `github.com/najibmdx/PandaLive`
**Branch:** `main` (all phases merged)

---

# SYSTEM CONTEXT & PURPOSE

## What PANDA LIVE Is

**Real-time situational awareness tool** that provides:
- ✅ Faster awareness than manually watching Solscan
- ✅ Faster than GMGN/Axiom dashboards  
- ✅ Compressed view vs inferring from price action alone

**The edge is SPEED + SYNTHESIS, not prediction.**

## What PANDA LIVE Is NOT

- ❌ NOT a prediction system (does NOT predict price pumps)
- ❌ NOT a signal generator (no buy/sell signals)
- ❌ NOT a telemetry dashboard (no raw data dumps)
- ❌ NOT a scoring/ranking system across tokens

## The Fundamental Learning

**From v4 failure:**
User tested v4 patterns extensively and found:
1. Patterns appeared before pumps, during pumps, AND before failures
2. No systematic profitability from acting on signals
3. No informational asymmetry (all derivable from public on-chain data)

**Conclusion:** v4's patterns were telemetry, not intelligence.

**v4 proved these were telemetry dressed up as intelligence.**

---

# SACRED PRINCIPLES

## Principle 1: Intelligence vs Telemetry (THE GOLDEN RULE)

**The Test:**
> "Does this help make a decision, or is it just a number?"
> If number → delete it.

### ❌ TELEMETRY (Raw Data Diarrhea):
```
[14:38:47] WHALE: 6vN5...mL1w 17.6 SOL
[14:38:41] WHALE: 1sJ9...tC4d 26.8 SOL
[14:38:35] WHALE: 8rK2...hB7n 22.3 SOL
[14:38:22] WHALE: 3fD6...wP9m 19.1 SOL
[14:38:15] WHALE: 5tL8...jN3q 13.9 SOL
```
→ 8 separate events, you must interpret each one

### ✅ INTELLIGENCE (Compressed):
```
Active:8 | Early:3(37%) | Persist:2 | Coord:5 | Diseng:1(33%)
```
→ ONE LINE. Situation compressed. Actionable context.

### The Goldilocks Principle

**Too much detail = Telemetry diarrhea**
- Can't see the forest for the trees
- Overwhelmed

**Too compressed = Loses context**
- Just "8 wallets"
- What does it mean?

**Right level = Intelligence**
- Compressed enough to read at a glance
- Structured enough to show breakdown
- Detailed enough to be actionable

**Intelligence = Right level of compression + Right structure**

## Principle 2: Wallet Signals vs Token Intelligence

**Wallet Signals (Phase 2):**
- Observations about individual wallet behavior
- NO asymmetry or hidden knowledge
- Examples: TIMING, COORDINATION, PERSISTENCE, EXHAUSTION

**Token Intelligence (Phase 3):**
- Compressed structural patterns from wallet signals
- State machine that answers "what's happening RIGHT NOW"
- Examples: TOKEN_COORDINATION_SPIKE, TOKEN_PRESSURE_PEAKING

**Only token-level compression qualifies as intelligence.**

## Principle 3: Episode Awareness

**Episodes are critical for memecoin tempo:**
- Memecoins re-ignite (not linear decay)
- Episode boundary = 10 min silence
- <10min gap = same episode (re-ignition)
- ≥10min gap = new episode
- All density/severity measurements are episode-scoped

## Principle 4: Micro-Time Windows

**Memecoin tempo is SECONDS TO MINUTES, not days:**
- 50 SOL in 5 minutes is MASSIVE (not 24h)
- v4's 24h/7d windows miss memecoin speed entirely
- Most memecoins are dead within days
- PANDA uses: 1min/5min/15min windows

## Principle 5: Non-Predictive

**PANDA outputs:**
- ✅ Current state (what IS happening)
- ✅ Recent transitions (what WAS happening)
- ❌ Future predictions (what WILL happen)

**No "will pump" language. Ever.**

---

# COMPLETE SYSTEM ARCHITECTURE

## Data Flow (All 5 Phases)

```
Helius API (5s poll)
    ↓
Phase 1: Flow Ingestion → Time Windows → Whale Detection (latched)
    ↓ (WhaleEvent)
Phase 2: Signal Detection → TIMING/COORDINATION/PERSISTENCE/EXHAUSTION
    ↓ (WalletSignalEvent)
Phase 3: State Machine → 9 states (forward + reverse transitions)
    ↓ (StateTransitionEvent)
Phase 3.5: Severity Calculator → S1-S5 annotation (in details dict)
    ↓ (StateTransitionEvent with severity)
Phase 4: CLI Renderer → Adaptive split-screen display
    ↓
User sees: Token state + Wallet signals + Event stream
    +
Session Logger → JSONL file (INTELLIGENCE_ONLY default)
```

## Intelligence Layers

| Layer | Input | Output | Purpose |
|-------|-------|--------|---------|
| **Phase 1** | Helius transactions | WhaleEvent | Threshold detection |
| **Phase 2** | WhaleEvent | WalletSignalEvent | Behavioral patterns |
| **Phase 3** | WalletSignalEvent | StateTransitionEvent | Token state compression |
| **Phase 3.5** | StateTransitionEvent | Severity (S1-S5) | Ordinal strength annotation |
| **Phase 4** | All events | Terminal display | User interface |

---

# LOCKED PARAMETERS

## DO NOT CHANGE THESE VALUES

```python
# Whale Thresholds (SOL) - EXACT VALUES
WHALE_SINGLE_TX_SOL = 10
WHALE_CUM_5MIN_SOL = 25
WHALE_CUM_15MIN_SOL = 50

# Time Windows (seconds)
WINDOW_1MIN = 60
WINDOW_5MIN = 300
WINDOW_15MIN = 900
EARLY_WINDOW = 300  # First 5 minutes after token birth

# Coordination
COORDINATION_MIN_WALLETS = 3
COORDINATION_TIME_WINDOW = 60  # seconds

# Persistence
PERSISTENCE_MIN_APPEARANCES = 2  # distinct 1-min buckets
PERSISTENCE_MAX_GAP = 300  # 5 minutes

# Exhaustion
EXHAUSTION_SILENCE_THRESHOLD = 180  # 3 minutes
EXHAUSTION_EARLY_WALLET_PERCENT = 0.60  # 60%

# Episode Tracking
EPISODE_END_SILENCE = 600  # 10 minutes
EPISODE_REIGNITION_GAP = 600  # <10min = same episode, >=10min = new

# Pressure Peaking
PRESSURE_PEAKING_MIN_WHALES = 5
PRESSURE_PEAKING_WINDOW = 120  # 2 minutes

# Dissipation
DISSIPATION_WHALE_THRESHOLD = 1  # <1 whale per 5min
DISSIPATION_LOOKBACK = 300  # 5 minutes

# Logging
LOG_LEVEL_DEFAULT = "INTELLIGENCE_ONLY"
LOG_FORMAT = "JSONL"
LOG_DIR = "logs/"

# CLI
CLI_REFRESH_RATE = 5  # seconds
CLI_EVENT_BUFFER = 100  # events
CLI_MIN_TERMINAL_SIZE = (80, 24)  # cols x rows
```

## Rationale for Micro-Time Thresholds

**Why 10/25/50 SOL (not higher)?**
- Memecoin liquidity is shallow
- 50 SOL in 5 minutes moves the market significantly
- These are meaningful amounts for micro-cap tokens

**Why 5/15 min windows (not 24h/7d)?**
- Memecoin tempo is minutes, not days
- Most memecoins pump and die within hours
- 24h windows miss the action entirely

**Internal Consistency Check:**
- 25 SOL / 5 min = 300 SOL/hour
- 50 SOL / 15 min = 200 SOL/hour
- Thresholds scale correctly ✓

---

# PHASE-BY-PHASE IMPLEMENTATION

## Phase 1: Core Primitives

**Files Created:**
```
panda_live/
├── models/
│   ├── events.py (FlowEvent, WhaleEvent)
│   ├── wallet_state.py (WalletState with rolling windows)
│   └── token_state.py (TokenState)
├── core/
│   ├── flow_ingestion.py (normalize_flow validation)
│   ├── time_windows.py (TimeWindowManager - 1/5/15min)
│   └── whale_detection.py (WhaleDetector with latched emission)
├── config/
│   ├── thresholds.py (all locked parameters)
│   └── wallet_names_loader.py (JSON name mapping)
└── logging/
    ├── session_logger.py (JSONL logger)
    └── log_replay.py (session replay)
```

**Key Features:**
- ✅ Latched emission (each threshold fires ONCE only per wallet)
- ✅ Rolling time windows with expiry
- ✅ Minute bucket tracking for persistence
- ✅ JSONL session logging

**Test Scenario Passed:**
```python
# Flow 1: 12 SOL → WHALE_TX fires
# Flow 2: 8 SOL → No WHALE_TX (already fired, latched)
# Flow 3: 10 SOL → WHALE_CUM_5M fires (12+8+10=30 >= 25)
```

## Phase 2: Wallet Signals

**Files Created:**
```
panda_live/core/
├── wallet_signals.py (WalletSignalDetector)
└── signal_aggregator.py (SignalAggregator)
```

**Modified:**
```
models/events.py (added WalletSignalEvent)
models/wallet_state.py (added timing_checked field)
logging/session_logger.py (added log_wallet_signal)
```

**The 4 Wallet Signals:**

### 1. TIMING
- **What:** Early appearance (within 300s of token birth)
- **Detection:** `first_seen - t0 <= 300`
- **Mid-flight handling:** First wallet seen = "early" (relative)
- **Sets:** `wallet_state.is_early` flag
- **Adds to:** `token_state.early_wallets` set

### 2. COORDINATION
- **What:** 3+ wallets acting together within 60s
- **Detection:** Temporal clustering (sliding 60s window)
- **NOT:** Graph-based (too slow for real-time)
- **Returns:** List of coordinated wallet addresses

### 3. PERSISTENCE
- **What:** Re-appearing across 2+ minute buckets
- **Detection:** `len(minute_buckets) >= 2` AND `max_gap <= 300s`
- **Signals:** Sustained intent, not one-shot behavior

### 4. EXHAUSTION
- **What:** 60%+ early wallets silent AND no replacement
- **Detection:** Token-level signal
- **Silence:** 180s+ with no whale activity
- **Replacement:** New non-early whales in last 5min
- **Critical:** Silence alone ≠ exhaustion

**Signal Event Format (Goldilocks Principle):**
```json
{
  "event_type": "WALLET_SIGNAL",
  "wallet": "7hG9kL2p...",
  "signals": ["TIMING", "COORDINATION"],
  "details": {
    "timing": {
      "is_early": true,
      "delta_seconds": 127
    },
    "coordination": {
      "coordinated_with": ["9pM4...", "2nQ7...", "5tL8..."],
      "time_window": 60
    }
  }
}
```

**Note:** Details include lists WITH context, counts WITH breakdowns.

## Phase 3: Token State Machine

**Files Created:**
```
panda_live/core/
├── episode_tracker.py (EpisodeTracker)
├── density_tracker.py (DensityTracker)
└── token_state_machine.py (TokenStateMachine)
```

**Modified:**
```
models/events.py (added StateTransitionEvent)
models/token_state.py (added current_state, episode fields, density tracking)
```

**The 9 Token States:**

```python
TOKEN_QUIET = "TOKEN_QUIET"
TOKEN_IGNITION = "TOKEN_IGNITION"
TOKEN_COORDINATION_SPIKE = "TOKEN_COORDINATION_SPIKE"
TOKEN_EARLY_PHASE = "TOKEN_EARLY_PHASE"
TOKEN_PERSISTENCE_CONFIRMED = "TOKEN_PERSISTENCE_CONFIRMED"
TOKEN_PARTICIPATION_EXPANSION = "TOKEN_PARTICIPATION_EXPANSION"
TOKEN_PRESSURE_PEAKING = "TOKEN_PRESSURE_PEAKING"
TOKEN_EXHAUSTION_DETECTED = "TOKEN_EXHAUSTION_DETECTED"
TOKEN_DISSIPATION = "TOKEN_DISSIPATION"
```

**State Transitions (Forward Path):**

```
QUIET → IGNITION
  Trigger: First whale detected
  
IGNITION → COORDINATION_SPIKE
  Trigger: 3+ wallets coordinated
  
COORDINATION_SPIKE → EARLY_PHASE
  Trigger: Sustained 2+ minutes
  
EARLY_PHASE → PERSISTENCE_CONFIRMED
  Trigger: 2+ persistent wallets
  
PERSISTENCE_CONFIRMED → PARTICIPATION_EXPANSION
  Trigger: New non-early whale
  
PARTICIPATION_EXPANSION → PRESSURE_PEAKING
  Trigger: 5+ whales in 2min AND episode max density
  
PRESSURE_PEAKING → EXHAUSTION_DETECTED
  Trigger: 60% early silent, no replacement
  
EXHAUSTION_DETECTED → DISSIPATION
  Trigger: <1 whale per 5min
  
DISSIPATION → QUIET
  Trigger: 10min silence (episode end)
```

**Reverse Transitions (Re-ignition):**

```
EXHAUSTION → PARTICIPATION_EXPANSION
  Trigger: New whale burst (2+ whales in 60s)
  
DISSIPATION → IGNITION
  Trigger: Sudden reactivation
```

**Episode Management:**

- Episode boundary IS the QUIET transition (atomic)
- Episode ID increments only on new episodes
- Episode start timestamp tracked
- All density measurements scoped to episode
- <10min gap = same episode (re-ignition)
- ≥10min gap = new episode

**Density Tracking (Critical for Pressure Peaking):**

- 2-minute rolling window of (timestamp, wallet) tuples
- Count unique wallets in window
- Track episode max density
- Pressure peaking requires BOTH:
  1. ≥5 whales in current 2-min window
  2. Current density > all previous densities in episode

**Key Design Decision:**
`prev_whale_timestamp` stores old `last_whale_timestamp` before density tracker updates it, ensuring correct gap measurement for re-ignition logic.

## Phase 3.5: Severity System

**File Created:**
```
panda_live/core/severity_calculator.py (SeverityCalculator)
```

**Integration:**
- Non-invasive wrapper (doesn't modify state machine)
- Severity computed at transition time
- Stored in `StateTransitionEvent.details["severity"]`
- NOT a new field on the dataclass

**The 5 Severity Levels:**

```python
S1 = "WEAK"
S2 = "LIGHT"
S3 = "MODERATE"
S4 = "STRONG"
S5 = "EXTREME"
```

**Locked Severity Mapping:**

| State | Severity Range | Key Thresholds |
|-------|---------------|----------------|
| **IGNITION** | S1-S2 | S1: single whale, S2: multiple early |
| **COORDINATION_SPIKE** | S2-S5 | 3→S2, 4→S3, 5→S4, 6+→S5 |
| **EARLY_PHASE** | S2-S3 | S3: 180s+ sustained with 3+ early |
| **PERSISTENCE_CONFIRMED** | S3-S5 | Floor S3, 2→S3, 3→S4, 4+→S5 |
| **PARTICIPATION_EXPANSION** | S2-S4 | S4 on burst reversal or 3+ new |
| **PRESSURE_PEAKING** | S3-S5 | Floor S3, 7+→S4, 10+→S5 |
| **EXHAUSTION_DETECTED** | S3-S5 | Floor S3, 70%→S4, 80%→S5 |
| **DISSIPATION** | S2-S5 | S4 if following S4/S5 state |
| **QUIET** | None | No severity (no transition context) |

**Critical Rules:**

- ✅ Severity is ordinal (ranked), not cardinal
- ✅ Episode-scoped (resets on new episode)
- ✅ Transition-bound (only emitted at transitions)
- ✅ Latched per state (doesn't update mid-state)
- ❌ No decimals, percentages, numeric scores
- ❌ No severity without transition
- ❌ No cross-token comparison

**Output Format:**

```json
{
  "event_type": "STATE_TRANSITION",
  "from_state": "TOKEN_IGNITION",
  "to_state": "TOKEN_COORDINATION_SPIKE",
  "details": {
    "coordinated_count": 4,
    "severity": "S3"
  }
}
```

## Phase 4: CLI + Helius Integration

**Files Created:**
```
panda_live/
├── cli/
│   ├── layout.py (responsive panel sizing)
│   ├── panels.py (Token/Wallet/Event panels)
│   └── renderer.py (CLIRenderer)
├── integrations/
│   └── helius_client.py (HTTP polling, SOL extraction)
├── orchestration/
│   └── live_processor.py (real-time event loop)
└── panda_live_main.py (entry point)
```

**CLI Layout (Adaptive Split-Screen):**

```
╔═══════════════════════════════════════════════════════════════════╗
║ PANDA LIVE | Token: BxK7...3mF9 | Episode: 1 | Duration: 6m 32s  ║
╚═══════════════════════════════════════════════════════════════════╝

┌─────────────────────────────────────┬─────────────────────────────┐
│ TOKEN INTELLIGENCE                  │ WALLET SIGNALS              │
├─────────────────────────────────────┼─────────────────────────────┤
│ State: PRESSURE_PEAKING [S3]        │ Active:8 | Early:3(37%)     │
│ Episode: 1                          │                             │
│ Time in State: 2m 35s               │ 7hG9...kL2p (WhaleMaster)   │
│                                     │   [TIMIN][COORD][PERSI]     │
│ Recent Transitions:                 │   Last: 3m 12s ago          │
│   14:38:47 → PRESSURE_PEAKING [S3]  │                             │
│   14:37:04 → PARTICIPATION_EXP [S2] │ 9pM4...dR8w (EarlyBird)     │
└─────────────────────────────────────┴─────────────────────────────┘

┌─ EVENT STREAM ────────────────────────────────────────────────────┐
│ [14:38:47] STATE: PARTICIPATION_EXPANSION → PRESSURE_PEAKING      │
│ [14:38:47] SIGNAL: 5tL8...jN3q → COORDINATION                     │
└───────────────────────────────────────────────────────────────────┘
```

**Rendering System:**
- 5-second panel refresh (not 1s - prevents flicker)
- Instant event append (responsive without flickering)
- Adaptive layout (4 breakpoints: 24/30/40/50+ rows)
- Cursor-based updates (no full screen clear)

**Helius Integration:**

- HTTP polling (NOT websockets) - 5-second interval
- Correct SOL extraction: `nativeBalanceChange / 1e9`
- Negative balance change = BUY
- Positive balance change = SELL
- 30-second timeout for cold start
- Pagination via `last_signature`
- Graceful error recovery

**User Workflow (Default):**

```bash
# 1. Set API key
export HELIUS_API_KEY='your-key'

# 2. Run PANDA LIVE
python panda_live_main.py

# 3. Enter token mint when prompted
Enter token mint address: [paste Solana token CA]

# 4. System auto-creates logs/ and starts monitoring
```

**Command Options:**

| Flag | Purpose |
|------|---------|
| `--demo` | Simulated data (no API key needed) |
| `--token-ca ADDRESS` | Skip prompt, monitor specific token |
| `--log-level FULL` | Log all events (flows + whales + signals + states) |
| `--log-level MINIMAL` | State transitions only |
| `--refresh-rate N` | Change panel refresh speed (default 5s) |
| `--wallet-names PATH` | Load custom wallet name mapping |

---

# SEVERITY SYSTEM (PHASE 3.5)

## Full Specification

**Canonical Definition:**

> Severity is an ordinal compression of internal wallet intelligence, emitted only at token state transitions.

**What Severity IS:**
- ✅ Ordinal (ranked S1-S5)
- ✅ Episode-scoped (resets on new episode)
- ✅ Latched (constant within a state)
- ✅ Wallet-derived (based on signal patterns)
- ✅ Non-predictive (describes current strength, not future outcome)

**What Severity is NOT:**
- ❌ Telemetry (not raw counts)
- ❌ Probability (not prediction)
- ❌ Confidence score
- ❌ Expected return
- ❌ Cross-token ranking

**When Severity is Computed:**

- ✅ ONLY at state transition time
- ❌ NEVER on heartbeat/periodic checks
- ❌ NEVER mid-state
- ❌ NEVER without a transition

**Storage:**

- Severity lives in `StateTransitionEvent.details["severity"]`
- NOT a new field on the dataclass
- This preserves Phase 3 state machine purity

**Reverse Transitions:**

When a reverse transition occurs (e.g., EXHAUSTION → PARTICIPATION_EXPANSION):
- Severity is recomputed
- Reflects current conditions
- May increase or decrease
- Still episode-bound

**Output Examples:**

**LIVE (default):**
```
TOKEN_STATE: COORDINATION_SPIKE [S4]
EPISODE: 2
```

**JSONL log:**
```json
{
  "event_type": "STATE_TRANSITION",
  "to_state": "TOKEN_COORDINATION_SPIKE",
  "details": {
    "coordinated_count": 5,
    "severity": "S4"
  }
}
```

**EXPLAIN (optional verbose mode):**
```
Severity: STRONG
Drivers:
- Tight wallet clustering
- Multiple early actors
```

---

# CLI DESIGN

## Layout Philosophy

**Split-Screen Vertical (Option B - APPROVED):**

- Left half: Token Intelligence (state machine output)
- Right half: Wallet Signals (behavioral observations)
- Bottom: Event Stream (scrolling chronological log)

**Why vertical not horizontal?**
- Full width for each section (more readable)
- Token intel on top (most important)
- Works on any terminal width (no horizontal scroll)

## Adaptive Breakpoints

| Terminal Rows | Mode | Features |
|--------------|------|----------|
| **24-29** | Collapsed | Single-line summaries, flags [E][P][C], top 5 wallets only |
| **30-39** | Compact | Multi-line but compressed, top 8 wallets |
| **40-49** | Standard | Full panel detail, all active wallets |
| **50+** | Expanded | Maximum detail, full event history |

## Panel Content (Goldilocks Applied)

### Token Intelligence Panel (Left)

**Shows:**
- ✅ Current state name + severity
- ✅ Episode ID + duration
- ✅ Time in current state
- ✅ Recent state transitions (last 5-6)
- ✅ Episode max density indicator (YES/NO)

**Does NOT show:**
- ❌ Raw whale counts
- ❌ Density calculations (0.042 whales/sec)
- ❌ Internal trigger logic
- ❌ SOL amounts

### Wallet Signals Panel (Right)

**Shows:**
- ✅ Compressed summary: `Active:8 | Early:3(37%) | Persist:2`
- ✅ Full 44-character wallet addresses
- ✅ Wallet names (if loaded from JSON)
- ✅ Signal flags: `[TIMIN][COORD][PERSI]`
- ✅ Last seen time

**Does NOT show:**
- ❌ Individual whale transaction amounts
- ❌ Cumulative SOL per wallet
- ❌ Raw coordination window timings

### Event Stream Panel (Bottom)

**Shows:**
- ✅ State transitions with severity
- ✅ Wallet signals detected
- ✅ Timestamps
- ✅ Last 100 events (buffer)

**Does NOT show:**
- ❌ Every individual whale event
- ❌ Flow-level details
- ❌ Raw Helius transaction data

## Wallet Name Display

**Format:**
```
7hG9kL2pAbCdEfGhIjKlMnOpQrStUvWxYz (WhaleMaster)
  [TIMING][COORDINATION][PERSISTENCE]
  Last Seen: 3m 12s ago
```

**JSON mapping file:**
```json
{
  "FULL_WALLET_ADDRESS": "WalletName",
  "FULL_MINT_ADDRESS": "TokenName"
}
```

---

# CURRENT STATUS

## What's Working ✅

**Phase 1: Core Primitives**
- ✅ Flow ingestion and validation
- ✅ Time window management (1/5/15 min)
- ✅ Whale detection with latched emission
- ✅ Session logging (JSONL)
- ✅ Wallet name loading

**Phase 2: Wallet Signals**
- ✅ TIMING detection (early/late, relative for mid-flight)
- ✅ COORDINATION detection (3+ wallets in 60s)
- ✅ PERSISTENCE detection (2+ minute buckets)
- ✅ EXHAUSTION detection (60% early silent + no replacement)
- ✅ Signal event logging

**Phase 3: State Machine**
- ✅ Episode tracking (10min boundary, re-ignition)
- ✅ Density tracking (2-min window, episode max)
- ✅ All 9 states implemented
- ✅ Forward transitions working
- ✅ Reverse transitions working
- ✅ State transition logging

**Phase 3.5: Severity**
- ✅ S1-S5 computation
- ✅ All state mappings correct
- ✅ Severity in details dict (non-invasive)
- ✅ Episode-scoped reset

**Phase 4: CLI + Integration**
- ✅ Adaptive split-screen layout
- ✅ Helius HTTP integration
- ✅ Correct SOL extraction (Bug #2 from v4 FIXED)
- ✅ Live event processing loop
- ✅ Demo mode for testing
- ✅ Main entry point with argparse

## What's Deployed 🚀

**Repository:** `github.com/najibmdx/PandaLive`
**Branch:** `main`
**Status:** All 5 phases merged and committed

**File Count:**
- Phase 1: 17 files, 708 lines
- Phase 2: +2 files (wallet_signals.py, signal_aggregator.py)
- Phase 3: +3 files (episode_tracker, density_tracker, state_machine)
- Phase 3.5: +1 file (severity_calculator.py)
- Phase 4: +10 files, 1372 lines (cli/, integrations/, orchestration/)

**Total: ~33 files, ~2000+ lines of production code**

## What's Tested ✅

**All phases pass their test scenarios:**

- ✅ Phase 1: Latched emission works correctly
- ✅ Phase 2: Signal detection accurate
- ✅ Phase 3: State transitions fire correctly
- ✅ Phase 3.5: Severity computed per mapping
- ✅ Phase 4: Demo mode displays correctly

**Real-world testing:**
- ✅ Demo mode works perfectly
- ⚠️ Live mode works but has critical bug (see below)

---

# CRITICAL BUG DISCOVERED

## The Problem

**Coordination signal bloat causes hangs on active tokens.**

## What Happened

**Test case:**
- Token: `FfoMHGyQnvgFu3sh2dDt2CNYDTPxvwdPU35aeNoGpump`
- User described as "medium active" token
- PANDA ran for a while, then hung

**Log analysis revealed:**

Session log had wallet signal events with **44+ coordinated wallet addresses** in a single signal:

```json
{
  "event_type": "WALLET_SIGNAL",
  "wallet": "HyYNVYm...",
  "signals": ["COORDINATION"],
  "details": {
    "coordination": {
      "coordinated_with": [44 wallet addresses listed...]
    }
  }
}
```

**Result:**
- Massive JSON objects (10KB+ per event)
- Slow I/O for logging
- Display renderer trying to process 44-address lists
- System becomes unresponsive
- Eventually hangs

## The Scale Problem

**If "medium active" token causes 44-wallet coordination:**

| Token Type | Expected Coordination | Current Status |
|------------|----------------------|----------------|
| Low activity | 3-10 wallets | ✅ Works |
| Medium | 20-50 wallets | ⚠️ Hangs |
| Active | 100-500 wallets | ❌ Will crash |
| Hyper-active | 500-1000 wallets | ❌ Won't start |
| Moonshot | 2000+ wallets | ❌ Instant death |

**PANDA must handle all memecoin types, including moonshots.**

## Root Cause

**Current design stores ALL coordinated wallet addresses:**

```python
# In signal_aggregator.py
coordination_detail = {
    "coordinated_with": [list of ALL wallet addresses...],
    "time_window": 60
}
```

**This doesn't scale when:**
- Active tokens have 100+ wallets whaling within 60s
- Every wallet gets a signal event
- Every event contains the full list
- List grows exponentially with activity

## Why This Violates the Goldilocks Principle

**Current coordination signal is TOO DETAILED:**

```json
{
  "coordinated_with": [
    "wallet1", "wallet2", "wallet3", ... [41 more wallets]
  ]
}
```

This is **telemetry** (raw list of all addresses), not **intelligence** (compressed pattern).

**What it SHOULD be:**

```json
{
  "wallet_count": 44,
  "time_window": 60,
  "sample_wallets": ["wallet1", "wallet2", "wallet3"]
}
```

This is **intelligence** (count + context + reference sample).

## Impact Assessment

**What uses coordination data?**

Need to verify before changing:

1. **State machine transitions?**
   - Does it need the full list or just the count?
   
2. **Severity calculation?**
   - Currently uses count from list length
   - Could use direct count instead
   
3. **Display rendering?**
   - Shows compressed summary `Coord:44`
   - Doesn't display full list anyway
   
4. **Session logs?**
   - Used for replay/analysis
   - Do we need all addresses or just pattern?

**Unknown:** Full impact requires code analysis.

## User's Critical Warning

> "this token is considered rather medium active -- then one that got no signals that is one ACTIVE -- and then there are hyper active tokens -- those will just go ballistic -- and we haven't even talked about moonshot tokens -- those will just go ballistic!!! -- Panda must be able to handle all them -- memecoins of all kinds !!!"

**Translation:**
- Current bug appears on "medium" tokens
- Active/hyper-active/moonshot tokens will be MUCH worse
- PANDA must scale to handle thousands of coordinated wallets
- This is a fundamental architecture issue, not edge case

## Potential Solutions (NOT YET IMPLEMENTED)

**User rejected rushing to fix without understanding impact.**

**Key insight from user:**
> "you must consider what these changes will impact -- and needs evaluation and deeper consideration"

**Before changing anything, must determine:**

1. **Data flow:** Where is `coordinated_with` list used?
2. **Actual bottleneck:** Logging? Display? Memory? Core logic?
3. **Design intent:** Is individual wallet tracking essential?
4. **Scale requirements:** Max expected whales/min to support?

**Proposed investigation steps (NOT DONE YET):**

1. Trace coordination data usage across codebase
2. Add performance instrumentation to identify bottleneck
3. Determine if wallet list is used for logic or just logging
4. Decide on compression strategy based on findings

## What NOT to Do

❌ Do NOT change coordination signal format without understanding downstream impact
❌ Do NOT add arbitrary caps without knowing what breaks
❌ Do NOT assume the bottleneck without measurement
❌ Do NOT "fix" anything until the user approves the approach

## Current State of This Issue

**Status:** Identified but NOT fixed
**Blocker:** Needs deeper analysis before implementing changes
**Owner:** Next session must investigate before modifying

---

# REPOSITORY INFORMATION

## GitHub Details

**URL:** `https://github.com/najibmdx/PandaLive`
**Owner:** `najibmdx`
**Branch:** `main`
**Status:** All 5 phases merged

## File Structure

```
PandaLive/
├── panda_live/
│   ├── __init__.py
│   ├── models/
│   │   ├── __init__.py
│   │   ├── events.py
│   │   ├── wallet_state.py
│   │   └── token_state.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── flow_ingestion.py
│   │   ├── time_windows.py
│   │   ├── whale_detection.py
│   │   ├── wallet_signals.py
│   │   ├── signal_aggregator.py
│   │   ├── episode_tracker.py
│   │   ├── density_tracker.py
│   │   ├── token_state_machine.py
│   │   └── severity_calculator.py
│   ├── config/
│   │   ├── __init__.py
│   │   ├── thresholds.py
│   │   └── wallet_names_loader.py
│   ├── logging/
│   │   ├── __init__.py
│   │   ├── session_logger.py
│   │   └── log_replay.py
│   ├── cli/
│   │   ├── __init__.py
│   │   ├── layout.py
│   │   ├── panels.py
│   │   └── renderer.py
│   ├── integrations/
│   │   ├── __init__.py
│   │   └── helius_client.py
│   └── orchestration/
│       ├── __init__.py
│       └── live_processor.py
├── panda_live_main.py
├── logs/ (auto-created)
└── [various archive files from previous sessions]
```

## How to Clone and Run

```bash
# Clone the repository
git clone https://github.com/najibmdx/PandaLive.git
cd PandaLive

# Set Helius API key
export HELIUS_API_KEY='your-key-here'

# Run demo mode (no API key needed)
python panda_live_main.py --demo

# Run live mode
python panda_live_main.py
```

---

# NEXT STEPS

## Immediate Priorities (Session 2)

### 1. Investigate Coordination Bloat Bug

**Must answer:**
- Where is `coordinated_with` list actually used?
- Is it needed for state machine logic?
- Is it needed for severity calculation?
- Or is it only for logging/display?

**How to investigate:**
```bash
# Ask Claude Code to search codebase
grep -r "coordinated_with" panda_live/
```

**Then:** Trace every usage and determine if list can be replaced with count.

### 2. Identify Performance Bottleneck

**Add instrumentation:**
- Time each processing step
- Measure memory usage
- Profile I/O operations
- Identify where hang occurs

**Tools:**
- Python `time` module
- Memory profiler
- Logging at each stage

### 3. Determine Scale Requirements

**User must answer:**
- What's the max whales/min PANDA should handle?
- 50? 100? 500? 1000+?
- Is real-time monitoring during moonshot required?
- Or is post-session analysis acceptable for extreme cases?

### 4. Design Compression Strategy

**Based on investigation findings, choose:**

**Option A: Count-only (if list not used):**
```python
{
  "wallet_count": 44,
  "time_window": 60
}
```

**Option B: Count + Sample (if reference needed):**
```python
{
  "wallet_count": 44,
  "time_window": 60,
  "sample_wallets": [first 3 wallets...]
}
```

**Option C: Hierarchical (if relationships matter):**
```python
{
  "wallet_count": 44,
  "clusters": 3,
  "largest_cluster": 28
}
```

**Option D: External storage (if full list essential):**
```python
{
  "wallet_count": 44,
  "wallet_list_id": "coord_1234"
}
# Store full list separately, reference by ID
```

## Secondary Priorities

### 5. Memory Management

**Consider adding:**
- Max active wallet cap (e.g., 100 most recent)
- Wallet eviction policy (LRU)
- Event buffer size limit (not just count)

### 6. High-Volume Token Mode

**Potentially add:**
- `--minimal` flag for moonshot tokens
- Reduces logging to state transitions only
- Caps active wallet tracking
- Sacrifices detail for speed

### 7. Performance Optimization

**Potential improvements:**
- Async I/O for logging
- Batch event processing
- Display render throttling
- Memory pooling

### 8. Testing at Scale

**Create stress tests:**
- Simulate 1000 whales/min
- Verify no memory leaks
- Ensure display remains responsive
- Validate log file sizes

## Long-Term Enhancements (Future Sessions)

- Session replay tool (full implementation)
- Multi-token monitoring
- Alert system on state transitions
- Export formats (CSV, JSON)
- API for external integration
- Historical data analysis mode

---

# CRITICAL REMINDERS FOR NEXT SESSION

## Rules That Must Never Change

**1. The Goldilocks Principle:**
- Too much detail = telemetry
- Too compressed = meaningless
- Right level = intelligence with structure

**2. Intelligence vs Telemetry Test:**
- "Does this help make a decision, or is it just a number?"
- If number → delete it

**3. No Prediction:**
- PANDA shows what IS, not what WILL BE
- No "will pump" language ever

**4. Episode Awareness:**
- All measurements are episode-scoped
- 10min silence = episode end
- Re-ignition logic is critical

**5. Micro-Time Windows:**
- Memecoin tempo is minutes, not days
- 50 SOL in 5 min is massive
- Don't use 24h/7d windows

## What Can Be Changed

**✅ Allowed:**
- Coordination signal format (if proven safe)
- Event buffer size/limits
- Memory management strategy
- Display rendering optimizations
- Logging compression
- Performance improvements

**❌ Forbidden:**
- Locked threshold values (10/25/50 SOL, etc.)
- State machine logic (9 states, transitions)
- Episode boundary definition (10 min)
- Severity scale (S1-S5)
- Goldilocks principle
- Intelligence vs telemetry distinction

## Key Context for Continuation

**User is:**
- Experienced with memecoins (not a beginner)
- Needs PANDA to work at scale (moonshots included)
- Values proper analysis over quick fixes
- Will reject changes that violate core principles
- Expects Claude Code to do implementation
- Expects this assistant to design/specify only

**Assistant must:**
- Read skill documentation before every task
- Show prompts for approval before sending to Claude Code
- Not code directly (Claude Code does implementation)
- Consider impact before proposing changes
- Ask user for decisions when tradeoffs exist
- Maintain sacred principles at all costs

## Working Model

**This Assistant:**
- Designs and specifies
- Prepares prompts for Claude Code
- Reviews implementations
- Debugs issues
- Updates specifications

**Claude Code:**
- Implements code based on prompts
- Creates files and modules
- Runs tests
- Commits to repository

**User:**
- Makes strategic decisions
- Tests live deployments
- Reports bugs and issues
- Approves changes

---

# SESSION LOG SUMMARY

## What Was Accomplished

**Phase 1-4 Implementation:**
- All phases designed, specified, and implemented
- Complete system working end-to-end
- Repository updated with all code
- Demo mode validated

**Critical Bug Discovery:**
- Coordination bloat identified on real token
- Scale problem understood
- User prevented premature fix
- Investigation plan established

**Specification Refinement:**
- Goldilocks principle clarified through examples
- Intelligence vs telemetry distinction hardened
- Severity system fully documented
- All locked parameters confirmed

## What Was NOT Accomplished

**Coordination Bug Fix:**
- Identified but not fixed
- Investigation not completed
- Impact analysis not done
- Solution not implemented

**Scale Testing:**
- Not tested on hyper-active tokens
- Not tested on moonshots
- Performance profiling not done
- Bottleneck not measured

**Production Hardening:**
- Memory management not optimized
- Error handling not fully tested
- Edge cases not all covered
- High-volume mode not added

## Key Learnings

**1. User rejected quick fixes:**
- Demanded proper impact analysis
- Insisted on understanding before changing
- Prevented potentially breaking changes

**2. Scale is critical:**
- PANDA must handle all memecoin types
- Medium token already causes issues
- Moonshots will be orders of magnitude worse
- Architecture must scale, not just handle edge cases

**3. Goldilocks is fundamental:**
- Not just "don't show raw data"
- It's "compress WITH structure and context"
- Intelligence requires right level of detail
- Too much OR too little both fail

**4. Episode awareness is essential:**
- Memecoin dynamics are episodic
- Re-ignition is real and common
- All measurements must be episode-scoped
- This differentiates PANDA from v4

## Session End State

**User requested:**
> "log everything that occurred in this chat and produce a lossless master handover prompt to continue in another chat -- include all the rules and context hardlined here in this chat"

**This document is that handover.**

---

# APPENDIX: QUICK REFERENCE

## Run Commands

```bash
# Demo mode (no API key)
python panda_live_main.py --demo

# Live mode (requires HELIUS_API_KEY)
export HELIUS_API_KEY='your-key'
python panda_live_main.py

# Live mode with options
python panda_live_main.py \
  --token-ca ADDRESS \
  --log-level FULL \
  --refresh-rate 3 \
  --wallet-names names.json
```

## File Locations

```
Working code: /PandaLive/panda_live/
Entry point: /PandaLive/panda_live_main.py
Logs: /PandaLive/logs/
Config: /PandaLive/panda_live/config/
```

## Key Metrics

```
Whale thresholds: 10/25/50 SOL
Time windows: 1/5/15 minutes
Episode boundary: 10 minutes
Coordination window: 60 seconds
Early window: 300 seconds (5 minutes)
Exhaustion threshold: 60% early wallets
Refresh rate: 5 seconds
```

## State Sequence

```
QUIET → IGNITION → COORDINATION_SPIKE → EARLY_PHASE → 
PERSISTENCE_CONFIRMED → PARTICIPATION_EXPANSION → 
PRESSURE_PEAKING → EXHAUSTION_DETECTED → DISSIPATION → QUIET
```

## Severity Range

```
S1 (WEAK) → S2 (LIGHT) → S3 (MODERATE) → S4 (STRONG) → S5 (EXTREME)
```

---

# END OF HANDOVER

**This document contains everything needed to continue PANDA LIVE development in a new session.**

**Next session should begin by:**
1. Reading this entire document
2. Investigating coordination bloat bug
3. Implementing approved fixes
4. Testing at scale

**Repository:** `github.com/najibmdx/PandaLive`
**Status:** Phase 1-4 complete, coordination bug blocking scale
**Owner:** najibmdx
**Date:** February 7, 2026
