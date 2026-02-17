# PANDA LIVE - Real-Time Memecoin Situational Awareness

**Complete implementation with Helius integration ready.**

---

## **WHAT IS PANDA LIVE?**

PANDA LIVE provides **compressed intelligence** about memecoin token dynamics in real-time by detecting wallet behavior patterns and synthesizing them into actionable token states.

**Core Principle:** Instead of overwhelming you with raw transaction data (telemetry), PANDA LIVE compresses wallet behaviors into **4 wallet signals** and **9 token states** that give you situational awareness at a glance.

---

## **QUICK START**

### **1. Install Dependencies**
```bash
pip install -r requirements.txt
```

### **2. Set Helius API Key (for production)**
```bash
export HELIUS_API_KEY='your-helius-api-key-here'
```

### **3. Run PANDA LIVE**

**Demo Mode (no API key needed):**
```bash
python panda_live_main.py
```
You'll be prompted for a token address, then run in demo mode.

**Production Mode (live Helius data):**
```bash
export HELIUS_API_KEY='your-key'
python panda_live_main.py
```
Enter your token mint address when prompted.

---

## **NEW DEFAULT BEHAVIOR**

**Simply run:**
```bash
python panda_live_main.py
```

**System will:**
1. ✅ Prompt you for token mint address interactively
2. ✅ Auto-create `logs/` output folder
3. ✅ Read `HELIUS_API_KEY` from environment
4. ✅ Connect to Helius websocket (or offer demo mode if no key)

**No command-line arguments required!**

---

## **COMMAND-LINE OPTIONS (all optional)**

```bash
python panda_live_main.py [OPTIONS]

--token-ca TOKEN_CA        Token mint address (prompted if not provided)
--log-level LEVEL          FULL | INTELLIGENCE_ONLY | MINIMAL (default: INTELLIGENCE_ONLY)
--wallet-names PATH        Path to wallet names JSON file
--refresh-rate SECONDS     Panel refresh rate (default: 5.0)
--demo                     Force demo mode (simulated flows)
```

---

## **HELIUS INTEGRATION**

PANDA LIVE connects to Helius RPC websocket to receive live Solana transactions.

### **Setup:**

1. Get API key from [helius.dev](https://helius.dev)
2. Set environment variable:
   ```bash
   export HELIUS_API_KEY='your-api-key-here'
   ```
3. Run PANDA LIVE (it will auto-connect)

### **What Gets Monitored:**

- All transactions mentioning your token mint
- Parses swap events (buy/sell)
- Extracts wallet, direction, amount, timestamp
- Feeds into PANDA LIVE intelligence engine

### **Fallback:**

If `HELIUS_API_KEY` not set, PANDA LIVE offers to run in demo mode instead.

---

### **Phase 1: Core Primitives**
- Flow ingestion & normalization
- Rolling time windows (5min, 15min)
- Whale detection (10/25/50 SOL thresholds)
- Latched emission (no duplicate whale events)
- Session logging (JSONL format)

### **Phase 2: Wallet Signals**
4 wallet-level signals detected:
1. **TIMING** - Early appearance (within 300s of token birth)
2. **COORDINATION** - Acting with 2+ other wallets within 60s
3. **PERSISTENCE** - Re-appearing across 2+ minute buckets within 5min
4. **EXHAUSTION** - Early wallet goes silent (token-level)

### **Phase 3: Token State Machine**
9 token states with reversible transitions:
1. **TOKEN_QUIET** - No activity
2. **TOKEN_IGNITION** - First whale detected
3. **TOKEN_COORDINATION_SPIKE** - 3+ whales in 60s
4. **TOKEN_EARLY_PHASE** - Sustained beyond burst (2+ min)
5. **TOKEN_PERSISTENCE_CONFIRMED** - 2+ persistent wallets
6. **TOKEN_PARTICIPATION_EXPANSION** - New non-early whales
7. **TOKEN_PRESSURE_PEAKING** - 5+ whales in 2min, episode max density
8. **TOKEN_EXHAUSTION_DETECTED** - 60% early silent, no replacement
9. **TOKEN_DISSIPATION** - Activity collapsed

**Episode Management:**
- Episode = continuous attention span on token
- 10 min silence → Episode ends, state becomes QUIET
- New whale after <10 min → Re-ignition (same episode)
- New whale after >=10 min → New episode

### **Phase 4: CLI Output**
- Adaptive terminal layout (80x24 minimum, responsive)
- 3-panel display (Token State / Wallet Signals / Event Stream)
- Wallet name mapping
- Real-time refresh (configurable rate)

---

## **INSTALLATION**

Extract the archive:
```bash
tar -xzf panda_live_complete.tar.gz
cd panda_live_complete
```

No dependencies required beyond Python 3.8+.

---

## **USAGE**

### **Run Demo Mode:**
```bash
python panda_live_main.py --token-ca "DemoToken" --demo
```

This simulates a full episode with whale events showing all state transitions.

### **Production Mode (requires live flow data):**
```bash
python panda_live_main.py --token-ca "YourTokenAddress" --wallet-names path/to/wallet_names.json
```

### **Command-Line Options:**
```
--token-ca TOKEN_CA        Token contract address (required)
--log-level LEVEL          FULL | INTELLIGENCE_ONLY | MINIMAL (default: INTELLIGENCE_ONLY)
--wallet-names PATH        Path to wallet names JSON file
--refresh-rate SECONDS     Panel refresh rate (default: 5.0)
--demo                     Run demo mode with simulated flows
```

---

## **WALLET NAMES JSON FORMAT**

```json
{
  "FULL_WALLET_ADDRESS": "WalletName",
  "FULL_MINT_ADDRESS": "TokenName"
}
```

Example:
```json
{
  "7hG9pKxV3...abc123": "Alpha Whale",
  "9pM4dR2x8...xyz789": "Beta Whale"
}
```

---

## **SESSION LOGS**

All sessions are logged to `logs/panda_live_session_{token}_{timestamp}.jsonl`

**Log Levels:**
- **FULL** - All events (flows, whales, signals, states)
- **INTELLIGENCE_ONLY** (default) - Only signals and states
- **MINIMAL** - Only state transitions

### **Replay Session:**
```bash
python -m panda_live.logging.log_replay logs/panda_live_session_*.jsonl
```

This shows session summary with all state transitions.

---

## **TESTING**

Three test scripts are included:

### **Phase 1 Test: Core Primitives**
```bash
python test_phase1.py
```
Tests: Flow ingestion, time windows, whale detection, latched emission

### **Phase 2 Test: Wallet Signals**
```bash
python test_phase2.py
```
Tests: TIMING, COORDINATION, PERSISTENCE, EXHAUSTION signals

### **Phase 3 Test: State Machine**
```bash
python test_phase3.py
```
Tests: All 9 states, episode management, state transitions

---

## **LOCKED PARAMETERS**

All thresholds are frozen in `panda_live/config/thresholds.py`:

**Whale Thresholds:**
- Single TX: 10 SOL
- 5-min cumulative: 25 SOL
- 15-min cumulative: 50 SOL

**Time Windows:**
- Early window: 300s (5 min)
- Coordination window: 60s
- Persistence gap: 300s max
- Episode end: 600s (10 min)

**Signal Triggers:**
- Coordination: 3+ wallets
- Persistence: 2+ appearances
- Exhaustion: 60% early silent + no replacement
- Pressure peaking: 5+ whales in 2min + episode max

---

## **FILE STRUCTURE**

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
│   └── wallet_names.json        # Sample names
├── logging/
│   ├── session_logger.py        # JSONL logger
│   └── log_replay.py            # Session replay tool
├── cli/
│   └── panels.py                # Terminal UI rendering
└── logs/                        # Session logs (gitignored)

panda_live_main.py               # Main entry point
test_phase1.py                   # Phase 1 test
test_phase2.py                   # Phase 2 test
test_phase3.py                   # Phase 3 test
```

---

## **DESIGN PRINCIPLES**

1. **Intelligence, Not Telemetry**
   - No raw transaction dumps
   - Compressed signals and states only
   - Actionable situational awareness

2. **Episode-Based Reasoning**
   - Token states are episodic (reversible)
   - Episodes separated by 10min silence
   - Re-ignition logic for same episode

3. **Latched Emission**
   - Whale events fire ONCE per threshold per episode
   - No duplicate/redundant events
   - Clean signal stream

4. **Relative Early Timing**
   - Supports mid-flight monitoring (LIVE mode)
   - Early = within 300s of observation start
   - No dependency on chain creation time

5. **Non-Telemetry Logging**
   - Default: INTELLIGENCE_ONLY (signals + states)
   - FULL mode is opt-in
   - User controls data granularity

---

## **DIFFERENCES FROM V4**

PANDA LIVE is **NOT** just "v4 real-time":

**64% NEW logic:**
- Episode tracking (boundaries, re-ignition)
- Exhaustion detection (60% + no replacement)
- State machine (9 states, reversible)
- CLI rendering
- Density measurement (micro-time)

**20% ADAPTED from v4:**
- Time windows (24h→5min, 7d→15min)
- Coordination (graph→temporal)
- Persistence (trade count→minute buckets)

**27% DIRECT REUSE:**
- Flow ingestion
- Whale thresholds (10 SOL)
- Basic timing detection

---

## **NEXT STEPS FOR PRODUCTION**

To connect to live Solana data:

1. **Add RPC Websocket Listener**
   - Subscribe to token program logs
   - Parse swap transactions
   - Feed FlowEvents to `panda.process_flow()`

2. **Add Token Discovery**
   - Monitor new token launches
   - Auto-start PANDA LIVE on detected tokens
   - Multi-token monitoring

3. **Add Alert System**
   - State transition alerts
   - Webhook integration
   - Discord/Telegram notifications

4. **Add Historical Replay**
   - Load historical transactions
   - Replay through state machine
   - Validate intelligence accuracy

---

## **LICENSE**

Proprietary - All Rights Reserved

---

## **CONTACT**

For questions or issues, refer to project documentation.

---

**PANDA LIVE - Situational Awareness for Memecoin Markets**
