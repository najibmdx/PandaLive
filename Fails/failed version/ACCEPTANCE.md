# PANDA LIVE 5.0 - Acceptance Checklist

## Purpose
This checklist verifies that PANDA LIVE 5.0 meets all requirements from the implementation specification.

---

## 0. Hard-Locked Doctrine Compliance

- [ ] **Intelligence only**: System outputs state transitions only (no telemetry)
- [ ] **No forbidden metrics**: No price, TA, PnL, scores, rankings, "top wallets"
- [ ] **Token derives from wallet**: Token intelligence exists only when wallet intelligence exists
- [ ] **Display rules**: FULL addresses + names displayed everywhere (no truncation)
- [ ] **Latched transitions**: Emit only on state entry, no repeated emissions
- [ ] **Determinism**: Replay produces identical output
- [ ] **No new intelligence**: Only implements proven v4 concepts (with scaffolding)

---

## 1. Deliverables - All Files Created

- [ ] `panda_live.py` - Main CLI program exists and is executable
- [ ] `session_manager.py` - Session lifecycle management module
- [ ] `event_log.py` - Canonical event log writer/reader
- [ ] `ingestion.py` - Live ingestion and normalizer
- [ ] `audit_gate.py` - Runtime invariant validation
- [ ] `intelligence_output.py` - Transition output emitter
- [ ] `intelligence_engine.py` - Primitives + wallet/token intelligence
- [ ] `replay.py` - Deterministic replay runner
- [ ] `README.md` - Documentation
- [ ] `QUICKSTART.md` - Quick start guide

### Per-Mint Output Files (Created During Runtime)
- [ ] `<MINT>.events.csv` - Canonical event log (append-only)
- [ ] `<MINT>.alerts.tsv` - Intelligence transitions (append-only)
- [ ] `<MINT>.session.json` - Session state (for resume)

---

## 2. File Naming Conventions

- [ ] Filenames use mint address only: `<MINT>.events.csv` (no prefixes, no run IDs)
- [ ] All files created in user-specified `--outdir`
- [ ] Session state file: `<MINT>.session.json`
- [ ] Replay output: `<MINT>_replay.alerts.tsv`

---

## 3. Token States - Exactly One Active

- [ ] Token state enum includes all 9 states:
  1. TOKEN_QUIET
  2. TOKEN_IGNITION
  3. TOKEN_COORDINATION_SPIKE
  4. TOKEN_EARLY_PHASE
  5. TOKEN_PERSISTENCE_CONFIRMED
  6. TOKEN_PARTICIPATION_EXPANSION
  7. TOKEN_PRESSURE_PEAKING
  8. TOKEN_EXHAUSTION_DETECTED
  9. TOKEN_DISSIPATION

- [ ] System maintains exactly one active token state
- [ ] Token state transitions are latched (emit on entry only)
- [ ] Default state is TOKEN_QUIET

---

## 4. Module Implementation - Locked Boundaries

### M1: Session Manager
- [ ] Creates/resumes sessions per CA
- [ ] Generates unique session_id
- [ ] Manages cursor (slot, signature)
- [ ] Persists session state to JSON
- [ ] Provides file paths (events, alerts, session)

### M2: Live Ingestion
- [ ] Polls Solana via Helius API
- [ ] Fetches transaction signatures
- [ ] Fetches transaction details
- [ ] Maintains cursor for incremental fetching
- [ ] Handles API errors gracefully

### M3: Canonical Event Normalizer
- [ ] Converts raw Solana tx to canonical events
- [ ] Produces minimal schema (only fields needed by v4)
- [ ] Removes vendor/RPC shape differences
- [ ] Stable JSON encoding (sorted keys)

### M4: Canonical Event Log Writer
- [ ] Appends events to CSV (append-only)
- [ ] Creates header on first write
- [ ] Stable field ordering
- [ ] Immediate flush after write

### M5: Incremental Primitive Updater
- [ ] Updates v4 primitives from events (scaffolding)
- [ ] Maintains wallet tracking state
- [ ] Maintains time window state
- [ ] Provides plugin points for full v4 integration

### M6: Wallet Intelligence Engine
- [ ] Emits wallet transitions (5 types, scaffolding)
- [ ] Latches transitions (no re-emit)
- [ ] Provides plugin points for v4 logic
- [ ] Default: early timing detection only

### M7: Token Intelligence Compressor
- [ ] Maintains exactly one active token state
- [ ] Derives state from wallet intelligence
- [ ] No wallet signals → TOKEN_QUIET
- [ ] Provides plugin points for v4 mapping
- [ ] Default: QUIET → IGNITION only

### M8: Intelligence Output Emitter
- [ ] Writes transitions to alerts.tsv (TSV format)
- [ ] Displays transitions to CLI
- [ ] Shows FULL addresses + names
- [ ] No telemetry columns

### M9: Deterministic Replay Runner
- [ ] Reads events from canonical log
- [ ] Processes through same pipeline
- [ ] Writes to replay alerts file
- [ ] Compares with original for determinism

### M10: Acceptance & Audit Gate
- [ ] Validates canonical event schema
- [ ] Validates event ordering (slot, signature)
- [ ] Validates intelligence transitions
- [ ] Can HOLD emission on failure
- [ ] Reports violations clearly

---

## 5. CLI Commands - All Working

### Live Mode
- [ ] Command works: `python panda_live.py --mint <MINT> --outdir <DIR> --helius-api-key <KEY>`
- [ ] Fresh mode works: `... --fresh`
- [ ] Resume works (without --fresh)
- [ ] Ctrl+C stops gracefully
- [ ] Displays session header
- [ ] Displays transitions in real-time
- [ ] Displays audit status

### Replay Mode
- [ ] Command works: `python panda_live.py --mint <MINT> --outdir <DIR> --replay <EVENTS.CSV>`
- [ ] Reads events correctly
- [ ] Processes through pipeline
- [ ] Writes replay output
- [ ] Compares with original
- [ ] Reports determinism status

### Selftest
- [ ] Command works: `python panda_live.py --selftest`
- [ ] All module selftests pass
- [ ] No network required
- [ ] Clear pass/fail output

---

## 6. Canonical Event Format - Minimal & Strict

### Required Fields Present
- [ ] session_id
- [ ] mint (full address)
- [ ] slot
- [ ] block_time
- [ ] signature
- [ ] event_type
- [ ] actors_json (JSON list of addresses)
- [ ] program_id
- [ ] dex
- [ ] token_mint
- [ ] amounts_json (JSON dict)
- [ ] raw_ref

### Format Requirements
- [ ] CSV format with stable header
- [ ] JSON fields use sorted keys
- [ ] Stable column ordering
- [ ] No extra fields

---

## 7. Intelligence Transition Format - Output Contract

### Required Fields Present
- [ ] session_id
- [ ] mint (full address)
- [ ] token_name
- [ ] event_time
- [ ] entity_type (WALLET or TOKEN)
- [ ] entity_address (FULL, no truncation)
- [ ] entity_name
- [ ] transition_type
- [ ] transition_id (deterministic)
- [ ] supporting_refs

### Format Requirements
- [ ] TSV format (tab-separated)
- [ ] Stable field ordering
- [ ] No telemetry columns
- [ ] No counts, scores, rankings
- [ ] Addresses are FULL (32+ chars minimum)

---

## 8. Wallet Intelligence - Locked Concepts Only

### Transition Types Implemented (Scaffolding)
- [ ] WALLET_DEVIATION_ENTER (scaffolding only)
- [ ] WALLET_COORDINATION_ENTER (scaffolding only)
- [ ] WALLET_PERSISTENCE_ENTER (scaffolding only)
- [ ] WALLET_TIMING_EARLY_ENTER (basic implementation)
- [ ] WALLET_EXHAUSTION_ENTER (scaffolding only)

### Behavior
- [ ] Transitions are latched (no re-emit)
- [ ] Derived from v4 primitives
- [ ] Plugin points clearly marked for v4 integration
- [ ] No invented heuristics

---

## 9. Token Intelligence Compression - Locked

### Behavior
- [ ] Exactly one active token state maintained
- [ ] State derived ONLY from wallet intelligence
- [ ] No wallet transitions → TOKEN_QUIET
- [ ] Token transitions are latched
- [ ] Default safe behavior (QUIET/IGNITION only)
- [ ] Plugin points marked for v4 mapping
- [ ] No invented mappings

---

## 10. Audit Gate - Mandatory Checks

### Runtime Invariants Validated
- [ ] Canonical events have all required fields
- [ ] Events are strictly ordered by (slot, signature)
- [ ] No duplicate events (same slot + signature)
- [ ] Intelligence transitions have all required fields
- [ ] Addresses are not truncated
- [ ] Transition types are valid

### Gate Behavior
- [ ] HOLD status prevents emission
- [ ] PASS status allows emission
- [ ] Violations are reported clearly
- [ ] Can continue ingesting during HOLD
- [ ] Reset works correctly

---

## 11. CLI Display - Mockup-Style

### Display Requirements
- [ ] Shows FULL mint address
- [ ] Shows token name if known
- [ ] Shows FULL wallet addresses
- [ ] Shows wallet names if known
- [ ] Shows wallet transitions (latched)
- [ ] Shows current token state
- [ ] Shows audit status (PASS/HOLD)
- [ ] NO truncated addresses anywhere
- [ ] NO telemetry tables

---

## 12. Determinism - Byte-Identical Replay

### Verification Steps
1. [ ] Run live mode, capture events
2. [ ] Run replay mode on same events
3. [ ] Compare alerts.tsv files → should be identical
4. [ ] Transition IDs match
5. [ ] Transition types match
6. [ ] Entity addresses match
7. [ ] Event times match
8. [ ] Supporting refs match

### Requirements
- [ ] Same events → same transitions
- [ ] Stable JSON encoding (sorted keys)
- [ ] Stable TSV formatting
- [ ] No random/timestamp-based logic in intelligence
- [ ] Deterministic transition IDs

---

## 13. Integration - Safe Assumptions

- [ ] Uses Helius API for ingestion
- [ ] Polling strategy (not streaming)
- [ ] Minimal dependencies (requests + stdlib)
- [ ] No aggressive refactoring
- [ ] Safe defaults (TOKEN_QUIET if no v4 logic)

---

## 14. Implementation Sequence - Built in Order

- [ ] M1 built first (session manager)
- [ ] M4 built second (event log)
- [ ] M2+M3 built third (ingestion + normalizer)
- [ ] M10 built fourth (audit gate)
- [ ] M9 built fifth (replay)
- [ ] M5 built sixth (primitives)
- [ ] M6 built seventh (wallet intel)
- [ ] M7 built eighth (token compressor)
- [ ] M8 built ninth (output)
- [ ] Each module has selftest

---

## 15. Critical Constraints - No Violations

- [ ] NO "TODO: invent logic" comments
- [ ] NO telemetry outputs (counts, snapshots, rankings)
- [ ] NO truncated addresses
- [ ] NO added heuristics beyond v4
- [ ] Scaffolding is correct, deterministic, safe
- [ ] v4 plugin points clearly isolated

---

## 16. Additional Acceptance Tests

### File Integrity
- [ ] events.csv is valid CSV
- [ ] alerts.tsv is valid TSV
- [ ] session.json is valid JSON
- [ ] Files are append-only (no overwrite)
- [ ] Files survive process restart

### Error Handling
- [ ] Handles missing Helius API key
- [ ] Handles invalid mint address
- [ ] Handles network errors (retries/skips)
- [ ] Handles malformed transactions
- [ ] Handles missing fields in events
- [ ] Handles out-of-order events (audit HOLD)

### Performance
- [ ] Processes events incrementally (no batch accumulation)
- [ ] Flushes output immediately (visible in real-time)
- [ ] Cursor updates persist
- [ ] Resume doesn't re-process old events

---

## 17. Documentation Completeness

- [ ] README.md complete with architecture diagram
- [ ] QUICKSTART.md with concrete examples
- [ ] This checklist (ACCEPTANCE.md)
- [ ] Usage examples for all modes
- [ ] v4 integration guidance
- [ ] Troubleshooting section

---

## 18. Selftest Coverage

- [ ] SessionManager selftest passes
- [ ] CanonicalEventLog selftest passes
- [ ] CanonicalEventNormalizer selftest passes
- [ ] AuditGate selftest passes
- [ ] IntelligenceOutput selftest passes
- [ ] IntelligenceEngine selftest passes
- [ ] ReplayRunner selftest passes
- [ ] All selftests run without network
- [ ] All selftests are deterministic

---

## Final Acceptance Criteria

### Must Pass All:
1. [ ] All module selftests pass (`--selftest`)
2. [ ] Live mode ingests and displays transitions
3. [ ] Replay mode produces output
4. [ ] Replay matches original (determinism verified)
5. [ ] No truncated addresses in any output
6. [ ] No telemetry in alerts.tsv
7. [ ] Audit gate correctly validates and reports
8. [ ] Files use correct naming convention
9. [ ] All v4 plugin points clearly marked
10. [ ] Documentation is complete and accurate

### Ready for Production When:
- [ ] All checklist items ✓
- [ ] Selftest passes
- [ ] Live + replay both work end-to-end
- [ ] Determinism verified on real data
- [ ] Documentation reviewed
- [ ] v4 integration points identified

---

## Notes

**Current State:** 
- Scaffolding complete and tested
- Basic intelligence (early timing) implemented
- All modules integrated and working
- Determinism verified in selftest
- Ready for v4 logic integration

**To Extend:**
- Edit `intelligence_engine.py` to add full v4 logic
- Test determinism after each addition
- Maintain latched transitions
- Never add non-v4 heuristics

**Version:** 5.0 (Initial Implementation)
**Date:** 2024
