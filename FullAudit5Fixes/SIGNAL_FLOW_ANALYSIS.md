# PANDA LIVE - COMPLETE SIGNAL FLOW ANALYSIS

## DATA FLOW: From Helius to Display

```
┌─────────────────────────────────────────────────────────────────────┐
│ HELIUS API                                                          │
│ Returns: Raw transaction JSON                                       │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│ helius_client.py::poll_and_parse()                                  │
│ Parses transaction → FlowEvent                                      │
│ FlowEvent(wallet, timestamp, direction, amount_sol, signature)      │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│ live_processor.py::process_flow(flow)                               │
│ - Updates wallet state                                              │
│ - Calls time_window_mgr.add_flow()                                  │
│ - Calls whale_detector.check_thresholds()                           │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│ whale_detection.py::check_thresholds(ws, flow)                      │
│ Returns: List[WhaleEvent] (0-3 events)                              │
│                                                                      │
│ Checks 3 thresholds (LATCHED):                                      │
│ 1. WHALE_TX (single transaction ≥ threshold)                        │
│ 2. WHALE_CUM_5M (5min cumulative ≥ threshold)                       │
│ 3. WHALE_CUM_15M (15min cumulative ≥ threshold)                     │
│                                                                      │
│ Example return:                                                     │
│ [                                                                    │
│   WhaleEvent(type="WHALE_TX", amount=15 SOL),                       │
│   WhaleEvent(type="WHALE_CUM_5M", amount=30 SOL),                   │
│   WhaleEvent(type="WHALE_CUM_15M", amount=60 SOL)                   │
│ ]                                                                    │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│ ⚠️  CRITICAL BUG POINT - live_processor.py Line 188-189             │
│                                                                      │
│ for whale_event in whale_events:  # ← LOOPS 3 TIMES!               │
│     _process_whale_event(whale_event, ws, current_time)             │
│                                                                      │
│ CONSEQUENCE: If wallet crosses all 3 thresholds in one flow,        │
│ the signal detection runs 3 SEPARATE TIMES!                         │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ├──────────────────┬──────────────────┐
                     ▼                  ▼                  ▼
              [Iteration 1]      [Iteration 2]      [Iteration 3]
              WHALE_TX           WHALE_CUM_5M       WHALE_CUM_15M
                     │                  │                  │
                     ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ live_processor.py::_process_whale_event(whale_event, ws, time)      │
│                                                                      │
│ 1. Logs whale event                                                 │
│ 2. Updates density tracker (adds whale event to 2min window)        │
│ 3. Calls signal_aggregator.process_whale_event()                    │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│ signal_aggregator.py::process_whale_event(...)                      │
│                                                                      │
│ Checks 3 signals:                                                   │
│ 1. TIMING (once per wallet - has latch)                             │
│ 2. COORDINATION (EVERY TIME - NO LATCH!) ← BUG!                     │
│ 3. PERSISTENCE (EVERY TIME - NO LATCH!)                             │
│                                                                      │
│ Returns: WalletSignalEvent                                          │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│ ⚠️  SIGNAL DUPLICATION OCCURS                                       │
│                                                                      │
│ ITERATION 1 (WHALE_TX):                                             │
│   → detect_timing() → TIMING signal (first time only)               │
│   → detect_coordination() → COORDINATION (44 wallets)               │
│   → detect_persistence() → PERSISTENCE (if 2+ buckets)              │
│   → Logs: WalletSignalEvent([TIMING, COORDINATION, PERSISTENCE])    │
│                                                                      │
│ ITERATION 2 (WHALE_CUM_5M):                                         │
│   → detect_timing() → skipped (already checked)                     │
│   → detect_coordination() → COORDINATION (SAME 44 wallets!) ← DUP!  │
│   → detect_persistence() → PERSISTENCE (SAME buckets!) ← DUP!       │
│   → Logs: WalletSignalEvent([COORDINATION, PERSISTENCE])            │
│                                                                      │
│ ITERATION 3 (WHALE_CUM_15M):                                        │
│   → detect_timing() → skipped (already checked)                     │
│   → detect_coordination() → COORDINATION (SAME 44 wallets!) ← DUP!  │
│   → detect_persistence() → PERSISTENCE (SAME buckets!) ← DUP!       │
│   → Logs: WalletSignalEvent([COORDINATION, PERSISTENCE])            │
│                                                                      │
│ RESULT: 3 signal events logged for SAME wallet at SAME timestamp!   │
└─────────────────────────────────────────────────────────────────────┘
```

## ROOT CAUSE ANALYSIS

### Issue #1: Coordination Signal Spam

**Why it happens:**

1. **Design Intent:** Whale detection returns multiple events to track which thresholds were crossed
   - This is CORRECT for density tracking (need all 3 events)
   - This is CORRECT for logging whale events (want granularity)

2. **Implementation Bug:** Signal detection runs ONCE PER WHALE EVENT
   - Coordination detection has NO LATCH
   - Coordination detection checks "3+ wallets in last 60s"
   - ALL 3 whale events happen at SAME TIMESTAMP
   - Same 3+ wallets are in window for ALL 3 checks
   - Result: COORDINATION fires 3 times

3. **Why TIMING doesn't spam:** Has a latch (`timing_checked = True`)

4. **Why COORDINATION spams:** NO latch mechanism

**From your log data:**
- Total signals: 5699
- Coordination signals: 5697 (99.96%)
- Unique (wallet, timestamp) pairs: 3451
- Duplicate signals: 5699 - 3451 = 2248 (39.5% are duplicates)

**Math check:**
- If every wallet fires 3 whale events on first appearance
- And 2 whale events on subsequent appearances
- Average: ~1.6 signal events per wallet per timestamp
- 3451 unique pairs × 1.65 = ~5694 signals ✓ (matches observed 5699)

### Issue #2: Instant State Cascade

**Why it might happen:**

1. **State machine evaluates after EVERY signal** (line 199-205)
2. If 200 wallets fire coordination signals in 1 second
3. State machine runs 200 times in 1 second
4. Each evaluation can trigger transition
5. Result: Multiple transitions in same second

**From your log:**
```
timestamp 1770637072 (19:37:52):
- EARLY_PHASE → PERSISTENCE_CONFIRMED
- PERSISTENCE_CONFIRMED → PARTICIPATION_EXPANSION
- PARTICIPATION_EXPANSION → PRESSURE_PEAKING
```

**Three transitions in ONE second!**

**Hypothesis:** NOT caused by spam signals
- More likely: state machine transition logic allows cascading
- OR: multiple wallets triggering different transition conditions simultaneously

**Need to verify:** Does fixing Issue #1 prevent this?

### Issue #3: Event Stream Filter

**Current observation:** Event stream shows 99.96% COORDINATION

**Hypothesis A:** Filter bug
- Renderer only displays coordination signals
- Other signals filtered out

**Hypothesis B:** Data bug  
- 99.96% of signals ARE coordination (due to Issue #1 spam)
- Renderer correctly displays what it receives
- Fixing Issue #1 would reveal variety

**Need to check:** Does renderer filter signals, or does it display all signals it receives?

### Issue #4: Early Wallet Detection

**Observation:**
- CLI shows: "Early: 0 (0%)"
- But log has: 421 TIMING signals
- TIMING signal indicates early wallet detection

**Hypothesis A:** TIMING signals created but not counted
- `wallet_state.is_early` flag set correctly
- But `token_state.early_wallets` set not populated
- Or: Early % calculation uses wrong data source

**Hypothesis B:** TIMING signals duplicated too
- 421 TIMING signals are actually ~140 unique wallets
- Due to Issue #1 duplication
- But still should be counted!

**Need to check:**
1. Where is early_wallets set populated?
2. How is Early % calculated in display?
3. Is there duplication in TIMING signals?

### Issue #5: State Staleness

**Observation:**
- Token dumped (110K → 17K mcap per Axiom)
- PANDA shows: PRESSURE_PEAKING [S5]
- No EXHAUSTION or DISSIPATION transition detected

**Hypothesis A:** Expected behavior
- Token went QUIET (no activity)
- Not EXHAUSTED (which requires 60% early wallets silent + no replacement)
- State machine correctly stays in last known state

**Hypothesis B:** Detection bug
- EXHAUSTION detection broken
- Should have triggered but didn't
- Need to check exhaustion detection logic

**Need to check:**
1. What was the actual wallet activity after peak?
2. Did early wallets go silent?
3. Was there replacement activity?
4. Should EXHAUSTION have fired?

## CRITICAL DEPENDENCIES

```
Issue #1 (Coordination Spam)
    ↓
    ├──→ Might fix Issue #2 (less signals = less state evaluations)
    ├──→ Might fix Issue #3 (reveal signal variety after deduplication)
    ├──→ Might fix Issue #4 (correct counts after deduplication)
    └──→ MUST fix before Option C (pattern analysis needs clean data)

Issue #2 (State Cascade)
    ↓
    └──→ Probably independent of Issue #1 (need to verify)

Issue #3 (Event Stream)
    ↓
    └──→ Depends on Issue #1 (might be data bug, not filter bug)

Issue #4 (Early Detection)
    ↓
    └──→ Depends on Issue #1 (might be count bug from duplication)

Issue #5 (State Staleness)
    ↓
    └──→ Independent (separate detection logic)

Option C (Pattern Analysis)
    ↓
    └──→ BLOCKED by Issue #1 (needs clean deduplicated data)

CLI Design
    ↓
    └──→ BLOCKED by Option C (needs pattern analysis to exist)
```

## NEXT STEPS

1. **Fix Issue #1 FIRST** (highest priority, blocks everything)
2. **Verify if Issues #2, #3, #4 auto-fix**
3. **Investigate remaining issues**
4. **Then build Option C** (after data is clean)
5. **Then design CLI** (after features exist)

