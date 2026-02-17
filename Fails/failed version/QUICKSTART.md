# PANDA LIVE 5.0 - Quick Start Guide

## Installation

```bash
# 1. Navigate to the project directory
cd panda_live_5.0

# 2. Set up your Helius API key as environment variable
export HELIUS_API_KEY=your_helius_api_key_here

# 3. Install dependencies (only requests needed for live mode)
pip install requests --break-system-packages

# 4. Verify installation with selftest
python panda_live.py --selftest
```

## Quick Examples

### Example 1: Interactive Mode (Simplest)

```bash
# Set API key once
export HELIUS_API_KEY=your_helius_api_key_here

# Launch interactive mode
python panda_live.py

# You'll be prompted:
# Enter token mint address: [paste your mint address here]
```

**What happens:**
- System prompts you for the token mint address
- Automatically creates output directory `./panda_live_data/`
- Uses Helius API key from environment
- Starts monitoring immediately

**Example session:**
```
================================================================================
PANDA LIVE 5.0 - Interactive Mode
================================================================================

Enter token mint address: 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr
Using default output directory: ./panda_live_data
Using Helius API key from environment variable

Session ID: 7GCihgDB_20240115_142000_abc12345
Mint: 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr
Output Dir: ./panda_live_data
================================================================================

Monitoring started... (Ctrl+C to stop)
```

### Example 2: Command Line Mode (Full Control)

```bash
# Specify everything explicitly
python panda_live.py \
  --mint 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr \
  --outdir ./custom_data \
  --helius-api-key YOUR_HELIUS_API_KEY
```

**What happens:**
- Creates session and starts monitoring
- Polls Solana for new transactions every 5 seconds
- Displays intelligence transitions in real-time
- Writes canonical events to `<MINT>.events.csv`
- Writes transitions to `<MINT>.alerts.tsv`
- Press Ctrl+C to stop

**Output you'll see:**
```
================================================================================
PANDA LIVE 5.0 - Intelligence Console
================================================================================
Token CA: 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr
================================================================================

Monitoring started... (Ctrl+C to stop)

[2024-01-15 14:23:45] [PASS]
  WALLET: DYw8jCTfwHNRJhhmFcbXvVDTqWMEVFBX6ZKUmG5CNSKK
  -> WALLET_TIMING_EARLY_ENTER
  Refs: 5j7s6NiJS3JAkvgkoc18WVAsiSaci2pxB2A6ueCJP4tpr...

[2024-01-15 14:24:12] [PASS]
  TOKEN: 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr
  -> TOKEN_IGNITION_ENTER
  Refs: 1 wallet transitions
```

### Example 3: Resume Existing Session

```bash
# Just run again - will resume from last cursor
python panda_live.py
# Enter same mint address when prompted
```

### Example 4: Fresh Session

```bash
# Create a new session (new session_id, reset cursor)
python panda_live.py --fresh
# Enter mint address when prompted
```

### Example 5: Replay & Verify Determinism

```bash
# 1. Run live for a while, then stop (Ctrl+C)

# 2. Replay the canonical event log
python panda_live.py \
  --mint 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr \
  --replay ./panda_live_data/7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr.events.csv
```

**What happens:**
- Reads all events from the canonical log
- Processes through the same pipeline
- Writes to `<MINT>_replay.alerts.tsv`
- Compares with original `<MINT>.alerts.tsv`
- Reports PASS or FAIL for determinism

**Expected output:**
```
================================================================================
PANDA LIVE 5.0 - Replay Mode
================================================================================
Events log: ./live_data/7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr.events.csv
Replay output: ./live_data/7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr_replay.alerts.tsv
================================================================================

REPLAY: Reading events from 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr.events.csv
REPLAY: Loaded 150 canonical events
REPLAY: Session 7GCihgDB_20240115_142000_abc12345
REPLAY: Mint 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr
REPLAY: Produced 5 intelligence transitions
REPLAY: Written to 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr_replay.alerts.tsv

Replay complete: 5 transitions

DETERMINISM CHECK:
  Original: 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr.alerts.tsv
  Replay:   7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr_replay.alerts.tsv
  Original transitions: 5
  Replay transitions:   5
  ✓ DETERMINISM: PASS

✓ DETERMINISM VERIFIED
```

## File Organization

After running, your output directory will contain:

```
panda_live_data/
├── 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr.events.csv      # Canonical events
├── 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr.alerts.tsv      # Intelligence transitions
├── 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr.session.json    # Session state
└── 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr_replay.alerts.tsv  # Replay output
```

## Inspecting Output Files

### View canonical events:
```bash
# See all on-chain events captured
head -20 ./panda_live_data/7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr.events.csv
```

### View intelligence transitions:
```bash
# See all intelligence state transitions
cat ./panda_live_data/7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr.alerts.tsv
```

### Compare original vs replay:
```bash
# Should be identical
diff ./panda_live_data/7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr.alerts.tsv \
     ./panda_live_data/7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr_replay.alerts.tsv
```

## Common Use Cases

### Use Case 1: Quick Monitor (Interactive)
```bash
# Set API key once in your shell profile (~/.bashrc or ~/.zshrc)
export HELIUS_API_KEY=your_key_here

# Then just run
python panda_live.py
# Paste mint address when prompted
```

### Use Case 2: Monitor Multiple Tokens
```bash
# Terminal 1
python panda_live.py  # Enter TOKEN_1 mint

# Terminal 2
python panda_live.py  # Enter TOKEN_2 mint

# Terminal 3
python panda_live.py  # Enter TOKEN_3 mint
```

### Use Case 3: Audit Historical Session
```bash
# Replay an old session's event log
python panda_live.py \
  --mint 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr \
  --replay ./old_session/7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr.events.csv
```

## What Intelligence is Currently Detected?

**Default Implementation (Scaffolding):**
- ✓ Early timing detection (wallets appearing in first 5 minutes)
- ✓ Token ignition (when early wallets detected)

**Not Yet Implemented (Needs v4 Integration):**
- ⚠ Deviation detection
- ⚠ Coordination detection  
- ⚠ Persistence detection
- ⚠ Exhaustion detection
- ⚠ Full token state transitions (2-9)

To extend with full v4 logic, edit `intelligence_engine.py` (see README.md).

## Troubleshooting

### No transitions appearing?
- **Expected** with default scaffolding (only early timing implemented)
- Token needs activity in first 5 minutes to trigger transitions
- Extend wallet intelligence engine with full v4 logic for more detections

### "AUDIT: HOLD" messages?
- Check violations in output
- Usually: out-of-order events or validation failures
- System will continue ingesting but not emit transitions

### Replay determinism fails?
- Check for timestamp-based logic that should be excluded
- Verify JSON encoding is stable (sorted keys)
- Review wallet/token intelligence for non-deterministic behavior

## Next Steps

1. **Run selftest** to verify installation
2. **Monitor a token live** to collect events
3. **Run replay** to verify determinism
4. **Extend with v4 logic** for full intelligence detection

## Getting Help

- Check the main README.md for detailed architecture
- Review module source code for implementation details
- Contact development team for v4 integration support
