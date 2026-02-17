# PANDA LIVE 5.0

End-to-end, audit-ready, deterministic, real-time intelligence console for Solana tokens.

## Overview

PANDA LIVE 5.0 is a live monitoring system that:
- Ingests on-chain Solana events in real-time
- Applies proven v4 intelligence logic incrementally
- Outputs **intelligence state transitions only** (latched)
- Maintains deterministic, append-only audit logs
- **NEVER outputs**: price, TA, PnL, scores, rankings, or telemetry metrics

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    PANDA LIVE 5.0                            │
├─────────────────────────────────────────────────────────────┤
│  Ingestion → Normalizer → Event Log → Primitive Updater    │
│      ↓            ↓           ↓              ↓              │
│  Wallet Intel ← Token Compressor ← Audit Gate              │
│      ↓                    ↓                                  │
│  CLI Display + alerts.tsv (transitions only)                │
└─────────────────────────────────────────────────────────────┘
```

## File Structure

```
panda_live_5.0/
├── panda_live.py              # Main CLI orchestrator
├── session_manager.py          # M1: Session lifecycle
├── event_log.py               # M4: Canonical event log (append-only)
├── ingestion.py               # M2+M3: Live ingestion + normalizer
├── audit_gate.py              # M10: Runtime invariant validation
├── intelligence_output.py      # M8: Transition output writer
├── intelligence_engine.py      # M5+M6+M7: Primitives + Intelligence
├── replay.py                  # M9: Deterministic replay
└── README.md                  # This file
```

## Installation

```bash
# No external dependencies required for core functionality
# (uses only Python standard library)

# For live ingestion, you need:
pip install requests --break-system-packages
```

## Usage

### 1. Live Monitoring (Interactive Mode - Recommended)

The simplest way to start:

```bash
# Set your Helius API key once
export HELIUS_API_KEY=your_helius_api_key_here

# Run in interactive mode
python panda_live.py

# You'll be prompted for the mint address
# Output directory ./panda_live_data/ is created automatically
```

### 2. Live Monitoring (Command Line Mode)

Specify all parameters explicitly:

```bash
python panda_live.py \
  --mint 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr \
  --outdir ./data \
  --helius-api-key YOUR_HELIUS_API_KEY
```

**Fresh session** (create new session, don't resume):

```bash
python panda_live.py --fresh
# Or with explicit args:
python panda_live.py \
  --mint 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr \
  --outdir ./data \
  --helius-api-key YOUR_HELIUS_API_KEY \
  --fresh
```

### 3. Deterministic Replay

Replay from canonical event log to verify determinism:

```bash
python panda_live.py \
  --mint 7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr \
  --outdir ./data \
  --replay ./data/7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr.events.csv
```

### 4. Self-Test

Run all module self-tests (no network required):

```bash
python panda_live.py --selftest
```

## Output Files

Per mint, PANDA LIVE creates these files in `--outdir`:

### `<MINT>.events.csv`
Canonical event log (append-only, source of truth):
- All on-chain events normalized to canonical schema
- Strictly ordered by (slot, signature)
- Used for deterministic replay

### `<MINT>.alerts.tsv`
Intelligence transition log (append-only, latched transitions only):
- Wallet intelligence transitions (deviation, coordination, persistence, timing, exhaustion)
- Token state transitions (exactly one active state)
- **FULL addresses** + names where available
- NO telemetry, counts, scores, or rankings

### `<MINT>.session.json`
Session state (for resume):
- Session ID, cursor position, status

## Intelligence Outputs

### Wallet Transitions (5 types)
1. `WALLET_DEVIATION_ENTER` - Wallet deviates from baseline
2. `WALLET_COORDINATION_ENTER` - Wallets acting together
3. `WALLET_PERSISTENCE_ENTER` - Not one-shot behavior
4. `WALLET_TIMING_EARLY_ENTER` - Early participant detected
5. `WALLET_EXHAUSTION_ENTER` - Initiator stops

### Token States (9 states, exactly one active)
1. `TOKEN_QUIET` - No intelligence signals
2. `TOKEN_IGNITION` - Initial activation
3. `TOKEN_COORDINATION_SPIKE` - Coordinated activity spike
4. `TOKEN_EARLY_PHASE` - Early phase activity
5. `TOKEN_PERSISTENCE_CONFIRMED` - Persistent behavior confirmed
6. `TOKEN_PARTICIPATION_EXPANSION` - Expanding participation
7. `TOKEN_PRESSURE_PEAKING` - Activity pressure peaking
8. `TOKEN_EXHAUSTION_DETECTED` - Exhaustion signals
9. `TOKEN_DISSIPATION` - Activity dissipating

## Display Rules (HARD-LOCKED)

- **ALWAYS show FULL addresses** (no truncation)
- **ALWAYS show token/wallet names** where available
- **NEVER show**: price, TA, PnL, scores, rankings, "top wallets", counts as product
- **State transitions only** (latched, no repeated emissions)

## Acceptance Checklist

### ✓ Module Integration
- [ ] M1: Session manager creates/resumes sessions correctly
- [ ] M2: Ingestion polls Solana and fetches transactions
- [ ] M3: Normalizer produces canonical events
- [ ] M4: Event log writer appends to CSV
- [ ] M5: Primitive updater processes events incrementally
- [ ] M6: Wallet intelligence engine emits transitions (latched)
- [ ] M7: Token compressor maintains exactly one active state
- [ ] M8: Output emitter writes to CLI + alerts.tsv
- [ ] M9: Replay runner reads events.csv and produces alerts
- [ ] M10: Audit gate validates invariants

### ✓ Determinism
- [ ] Replay produces identical alerts.tsv (byte-identical transition rows)
- [ ] Canonical events strictly ordered by (slot, signature)
- [ ] No duplicate transitions emitted
- [ ] Stable JSON encoding (sorted keys)
- [ ] Stable TSV formatting

### ✓ Audit & Safety
- [ ] Event validation catches missing fields
- [ ] Ordering validation catches out-of-order events
- [ ] Transition validation catches truncated addresses
- [ ] Audit gate can HOLD emission on failure
- [ ] Selftest runs without network

### ✓ Output Contract
- [ ] alerts.tsv contains only transitions (no telemetry)
- [ ] Full addresses displayed everywhere
- [ ] Token names displayed where available
- [ ] No price/TA/PnL/scores/rankings
- [ ] Latched transitions only (no repeats)

### ✓ File Conventions
- [ ] Filenames: `<MINT>.events.csv`, `<MINT>.alerts.tsv`
- [ ] All logs append-only
- [ ] Files created in user-specified --outdir
- [ ] No run IDs in filenames

## Determinism Verification

To verify deterministic behavior:

```bash
# 1. Run live monitoring for a while, then stop (Ctrl+C)
python panda_live.py --mint <MINT> --outdir ./data --helius-api-key <KEY>

# 2. Run replay on the captured events
python panda_live.py --mint <MINT> --outdir ./data \
  --replay ./data/<MINT>.events.csv

# 3. Check output - should show "DETERMINISM: PASS"
```

## Extending with v4 Logic

The system provides scaffolding for full v4 integration:

### Primitive Updater (M5)
Edit `intelligence_engine.py` → `V4Primitives` class:
- Add actual v4 primitive tables/metrics
- Extend `IncrementalPrimitiveUpdater.update()` to populate them

### Wallet Intelligence (M6)
Edit `intelligence_engine.py` → `WalletIntelligenceEngine` class:
- Implement `_should_emit_deviation()`
- Implement `_should_emit_coordination()`
- Implement `_should_emit_persistence()`
- Implement `_should_emit_exhaustion()`

### Token Compressor (M7)
Edit `intelligence_engine.py` → `TokenIntelligenceCompressor` class:
- Implement `_compute_new_state()` with full v4 mapping:
  - Map wallet coordination → TOKEN_COORDINATION_SPIKE
  - Map persistence → TOKEN_PERSISTENCE_CONFIRMED
  - Map exhaustion → TOKEN_EXHAUSTION_DETECTED
  - etc.

**Critical**: All extensions must maintain determinism and latched transitions.

## Troubleshooting

### "AUDIT: HOLD" displayed
- Check audit gate violations in output
- Common causes: out-of-order events, missing fields, truncated addresses

### Replay doesn't match original
- Check for non-deterministic logic in intelligence engine
- Verify stable JSON encoding (sorted keys)
- Check for timestamp-based logic that should be excluded

### No transitions detected
- This is expected if using default scaffolding (only early timing implemented)
- Extend wallet intelligence engine with full v4 logic

### Ingestion errors
- Verify Helius API key is valid
- Check network connectivity
- Verify mint address is correct

## License

Internal use only. Not for distribution.

## Support

For issues or questions, contact the development team.
