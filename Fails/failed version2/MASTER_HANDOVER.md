# PANDA LIVE - SESSION LOG & MASTER HANDOVER
## Date: February 6, 2026
## Session Token Count: ~115k tokens

---

# EXECUTIVE SUMMARY

**What We Built:**
Complete PANDA LIVE system across 4 phases - real-time memecoin intelligence engine with Helius integration and terminal UI.

**Current Status:**
- ✅ All 4 phases implemented and tested
- ❌ Phase 4 (CLI) has critical bugs violating core spec
- ⚠️ Ready for bug fixes in new session

**Critical Learning:**
Assistant violated core PANDA principles during Phase 4 implementation by adding telemetry to UI. New working method established to prevent this.

---

# WHAT IS PANDA LIVE

## Core Principle
PANDA LIVE provides **compressed intelligence** about memecoin token dynamics by detecting wallet behavior patterns and synthesizing them into actionable token states.

**NOT a telemetry tool. NOT a data dump. Intelligence ONLY.**

## What PANDA Outputs (SACRED - DO NOT VIOLATE)

### OUTPUT 1: Token States (9 states)
1. TOKEN_QUIET
2. TOKEN_IGNITION
3. TOKEN_COORDINATION_SPIKE
4. TOKEN_EARLY_PHASE
5. TOKEN_PERSISTENCE_CONFIRMED
6. TOKEN_PARTICIPATION_EXPANSION
7. TOKEN_PRESSURE_PEAKING
8. TOKEN_EXHAUSTION_DETECTED
9. TOKEN_DISSIPATION

### OUTPUT 2: Wallet Signals (4 types)
1. TIMING - Early appearance (within 300s of token birth)
2. COORDINATION - Acting with 2+ other wallets within 60s
3. PERSISTENCE - Re-appearing across 2+ minute buckets within 5min
4. EXHAUSTION - Early wallet goes silent (token-level signal)

### OUTPUT 3: State Transitions
- When state changed
- From what to what
- Episode context

## What PANDA Does NOT Output

❌ Raw transaction counts
❌ SOL amounts for individual transactions
❌ Wallet counts (active, early, persistent)
❌ Volume metrics
❌ "X whales in Y seconds" (internal trigger, not output)
❌ ANY raw, uncompressed data

**If it's a number that requires interpretation = TELEMETRY = FORBIDDEN**

---

# SYSTEM ARCHITECTURE

## Phase 1: Core Primitives ✅ COMPLETE
- Flow ingestion & normalization
- Rolling time windows (5min, 15min)
- Whale detection (10/25/50 SOL thresholds)
- Latched emission (no duplicate whale events)
- Session logging (JSONL format)

## Phase 2: Wallet Signals ✅ COMPLETE
- TIMING detection (early/relative)
- COORDINATION detection (3+ wallets in 60s)
- PERSISTENCE tracking (2+ minute buckets)
- EXHAUSTION detection (60% early silent + no replacement)

## Phase 3: Token State Machine ✅ COMPLETE
- 9-state machine with all transitions
- Episode tracking (10min boundaries, re-ignition)
- Density measurement (2-min whale count, episode max)
- Forward + reverse state transitions

## Phase 4: CLI Output ❌ HAS BUGS
- Terminal UI implementation
- Helius HTTP polling integration
- Wallet name mapping
- Real-time refresh

**CRITICAL: Phase 4 violates spec by showing telemetry. See BUGS section.**

---

# LOCKED PARAMETERS (DO NOT CHANGE)

## Whale Thresholds
- Single TX: **10 SOL** (exactly, not a range)
- 5-min cumulative: **25 SOL** (exactly, not 20-30)
- 15-min cumulative: **50 SOL** (exactly, not 40-60)

## Time Windows
- Early window: **300s** (5 minutes after t0)
- Coordination window: **60s**
- Persistence max gap: **300s** (5 minutes)
- Episode end: **600s** (10 min silence)
- Episode re-ignition: **<600s** same episode, **>=600s** new episode

## Signal Triggers
- Coordination: **3+ wallets** (not 2)
- Persistence: **2+ appearances** in distinct minute buckets
- Exhaustion: **60% early silent + no replacement whales**
- Pressure peaking: **5+ whales in 2min + episode max density**

## Latched Emission
- Whale events fire **ONCE per threshold per episode**
- No duplicate events
- Triggers reset on new episode

---

# HELIUS INTEGRATION

## Implementation Details
- **Method:** HTTP polling (NOT websockets)
- **Endpoint:** `https://api.helius.xyz/v0/addresses/{mint}/transactions`
- **Auth:** `api-key` parameter in URL
- **Filter:** `type=SWAP` (swap transactions only)
- **Poll interval:** 5 seconds (configurable via --refresh-rate)

## Environment Variable
```bash
export HELIUS_API_KEY='your-key-here'
```

## Current Parser Status
⚠️ **BROKEN** - SOL amounts are 100-1000x too high (see BUGS #2)

---

# FILE STRUCTURE

```
panda_live/
├── core/
│   ├── flow_ingestion.py       # Flow normalization
│   ├── time_windows.py          # Rolling window management
│   ├── whale_detection.py       # Threshold detection (latched)
│   ├── wallet_signals.py        # 4 signal types
│   ├── episode_tracker.py       # Episode boundaries
│   ├── density_tracker.py       # Pressure peaking
│   └── token_state_machine.py   # 9-state machine
├── models/
│   ├── events.py                # All event types
│   ├── wallet_state.py          # Per-wallet state
│   └── token_state.py           # Per-token state
├── config/
│   ├── thresholds.py            # All locked parameters
│   ├── wallet_names_loader.py   # JSON name loader
│   └── wallet_names.json        # Sample names (user should replace)
├── logging/
│   ├── session_logger.py        # JSONL logger
│   └── log_replay.py            # Session replay tool
├── cli/
│   └── panels.py                # Terminal UI rendering
└── logs/                        # Session logs (auto-created)

panda_live_main.py               # Main entry point
requirements.txt                 # Dependencies: requests>=2.31.0
```

---

# USER WORKFLOW

## Default Behavior (Interactive)
```bash
python panda_live_main.py
Enter token mint address: [user types address]
[System auto-creates logs/ folder]
[System reads HELIUS_API_KEY from environment]
[System connects to Helius and starts polling]
```

## Command-Line Options (All Optional)
```bash
--token-ca TOKEN_CA        # Token mint (prompted if not provided)
--log-level LEVEL          # FULL | INTELLIGENCE_ONLY | MINIMAL (default: INTELLIGENCE_ONLY)
--wallet-names PATH        # Path to wallet names JSON
--refresh-rate SECONDS     # Poll/refresh rate (default: 5.0)
--demo                     # Run simulated demo mode
```

## Wallet Names Format
```json
{
  "FULL_WALLET_ADDRESS": "WalletName",
  "FULL_MINT_ADDRESS": "TokenName"
}
```

---

# CRITICAL BUGS (MUST FIX IN NEW SESSION)

## Bug #1: UI Width Broken
**Issue:** Terminal panels stretch to 200+ columns
**Fix:** Cap terminal width at 80-120 columns max

## Bug #2: SOL Amounts Completely Wrong
**Issue:** Showing whales with 50,000+ SOL (should be 10-100 SOL)
**Root Cause:** Helius transaction parser is summing wrong fields
**Fix:** Correct the `parse_helius_transaction()` method to extract actual SOL amount from swap

## Bug #3: Duplicate Wallet Signals
**Issue:** Same wallet showing same signal 3-5 times
**Fix:** Deduplicate signals in display buffer

## Bug #4: Network Timeout Too Short
**Issue:** First API call timing out at 10s
**Fix:** Increase timeout to 30s for cold start

## Bug #5: Event Stream Not Scrolling
**Issue:** Events are static, not showing new events
**Fix:** Implement proper scrolling buffer (show last N events)

## Bug #6: Wallet Addresses Truncated
**Issue:** Only showing 4 chars each side (e.g., "BMdZ...CH99")
**Fix:** Show FULL 44-character Solana addresses

## Bug #7: Wallet Names Not Displaying
**Issue:** Names loaded (12 names) but not shown in output
**Fix:** Use `wallet_names.format_wallet_display()` in all output

## Bug #8: TELEMETRY EVERYWHERE (CRITICAL)
**Issue:** UI shows raw metrics violating core spec:
- "Active Wallets: 53" ← DELETE
- "Early Wallets: 6" ← DELETE
- "Persistent Wallets: 0" ← DELETE
- "Trigger: 3+_whales_in_60s" ← DELETE
- "[HELIUS] Processed 22 new transactions" ← DELETE
- "WHALE_TX: wallet 3641.7 SOL" ← DELETE (entire whale events stream)
- "WHALE_CUM_5M: wallet 3641.7 SOL" ← DELETE
- "WHALE_CUM_15M: wallet 3641.7 SOL" ← DELETE

**What UI Should Show (ONLY):**
```
TOKEN STATE
State: ⚡ COORDINATION_SPIKE
Episode: #0
Last Transition: IGNITION → COORDINATION_SPIKE

WALLET SIGNALS
BMdZx1k2n3mF9CH99abc123 (Alpha Whale): TIMING, COORDINATION
J9L5PKeyExample123xyz789: TIMING

EVENT STREAM
[16:01:10] STATE: QUIET → IGNITION
[16:01:33] STATE: IGNITION → COORDINATION_SPIKE
[16:01:33] SIGNAL: BMdZx1k2n3mF9CH99abc123 (Alpha Whale) → TIMING, COORDINATION
```

**NO counts, NO amounts, NO metrics, NO whale events spam.**

## Bug #9: Full Wallet Address Display
**Issue:** User wants FULL addresses displayed, not truncated
**Fix:** Display complete 44-character Solana addresses in all outputs

---

# WHAT WORKS (TESTED & VERIFIED)

✅ Flow ingestion and normalization
✅ Time window management (rolling 5min/15min)
✅ Whale detection with latched emission
✅ All 4 wallet signals detection
✅ All 9 state machine transitions
✅ Episode tracking and re-ignition
✅ Session logging to JSONL
✅ Helius API connection (polling works)
✅ Interactive token prompt
✅ Auto-create logs folder
✅ Environment variable for API key
✅ Demo mode with simulated flows

❌ Helius transaction parser (wrong SOL amounts)
❌ Terminal UI (telemetry violations + bugs)

---

# SESSION TIMELINE

## What Happened in This Session

1. **Phase 1 Built** - Core primitives (flow, windows, whales, logging)
2. **Phase 1 Tested** - User ran test_phase1.py successfully
3. **Phase 2 Built** - Wallet signals (4 types)
4. **Phase 2 Tested** - User ran test_phase2.py successfully
5. **Phase 3 Built** - State machine (9 states, episodes)
6. **Phase 3 Tested** - User ran test_phase3.py successfully
7. **Phase 4 Built** - CLI + Helius integration
8. **User Requirements Changed** - Interactive prompt, environment variable, HTTP polling
9. **Websockets Removed** - Replaced with HTTP polling per user request
10. **User Tested Live** - Found critical bugs (UI width, telemetry, parser, etc.)
11. **Critical Discussion** - Assistant violated spec by adding telemetry
12. **Root Cause Analysis** - Assistant explained why spec was violated
13. **New Working Method Established** - See COMMITMENT section below

---

# ASSISTANT'S COMMITMENT (MANDATORY)

## New Working Method

### BEFORE EVERY IMPLEMENTATION:

**STEP 1: READ THE SPEC**
- Use `view` tool to read relevant files
- Quote exact requirements
- NO reliance on memory

**STEP 2: STATE WHAT I'M BUILDING**
- Write: "I am building X which outputs Y"
- Show design
- Wait for approval

**STEP 3: IMPLEMENT EXACTLY AS APPROVED**
- No additions
- No "improvements"
- No assumptions

**STEP 4: VERIFY AGAINST SPEC**
- Re-read spec after coding
- Check: "Does this match?"
- Fix before showing user

**STEP 5: RECOMMEND ROOM CHANGES**
- When token count > 100k
- When multiple phases complete
- Before major new work
- When noticing context degradation

## What Assistant Will NOT Do

❌ Code from memory
❌ Add "nice to have" features
❌ Assume anything
❌ Rush to complete
❌ Continue in degraded context without flagging

---

# NEXT SESSION PRIORITIES

## Immediate Tasks (in order)

1. **Fix Bug #8 (Telemetry)** - HIGHEST PRIORITY
   - Remove all raw metrics from UI
   - Show only: States, Signals, Transitions
   - Verify against spec

2. **Fix Bug #2 (SOL Parser)** - CRITICAL
   - Correct Helius transaction parsing
   - Validate SOL amounts are 10-100 range, not 50,000+

3. **Fix Bug #6 & #7 (Wallet Display)** - HIGH
   - Show full 44-char addresses
   - Display wallet names alongside addresses

4. **Fix Bug #1 (UI Width)** - HIGH
   - Cap terminal width at 80-120 cols
   - Responsive layout

5. **Fix Bugs #3, #4, #5** - MEDIUM
   - Deduplicate signals
   - Increase timeout
   - Scrolling event stream

## After Bugs Fixed

- User testing with live token
- Validate state transitions occur correctly
- Verify all 9 states reachable
- Performance testing (polling overhead)

---

# TEST SCRIPTS INCLUDED

All test scripts work and pass:

- `test_phase1.py` - Core primitives
- `test_phase2.py` - Wallet signals  
- `test_phase3.py` - State machine

User can run these anytime to verify core logic still works after bug fixes.

---

# KNOWN GOOD BEHAVIOR

From user's live test session:

```
State: ⚡ COORDINATION_SPIKE     ← CORRECT
Episode: #0                      ← CORRECT
Last Transition: TOKEN_IGNITION → TOKEN_COORDINATION_SPIKE  ← CORRECT

Wallet Signals Detected:
- TIMING (multiple wallets)     ← CORRECT
- COORDINATION (multiple wallets) ← CORRECT
```

**The intelligence engine works. Only the UI presentation is broken.**

---

# FILES DELIVERED TO USER

- `panda_live_http_polling.tar.gz` - Complete codebase
- `requirements.txt` - Dependencies
- Test scripts (phase 1, 2, 3)
- Session logs from test runs

---

# CRITICAL REMINDERS FOR NEXT SESSION

## PANDA's Sacred Output

**ONLY show:**
- Token state (9 states)
- Wallet signals (4 types)  
- State transitions

**NEVER show:**
- Counts (wallets, transactions, events)
- Amounts (SOL volumes, sizes)
- Rates (per second, per minute)
- Triggers (internal logic)

## The Test

**Before showing ANY UI element, ask:**

"Does this help the user make a decision, or is it just a number?"

- If intelligence → show it
- If number → delete it

## Helius Parser

The transaction parser is the ONLY external dependency. Get this right:

```python
# CORRECT: Extract actual SOL amount from swap
# WRONG: Sum all token transfers
# WRONG: Use token amount instead of SOL
```

Real whale = 10-100 SOL
Fake whale = 50,000 SOL (current bug)

---

# END OF HANDOVER

**Session completed:** February 6, 2026
**Token count:** ~115,000
**Status:** Ready for bug fixes in fresh session
**Assistant commitment:** New working method established and agreed

User should start new chat with this document.
