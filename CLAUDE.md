# CLAUDE.md - PandaLive

## Project Overview

PandaLive is a **Python-based data pipeline for Solana blockchain wallet analysis**. It processes raw transaction data through multiple phases to detect whale wallets, compute behavioral features, discover patterns, and identify coordinated activity (cohorts, capital recycling).

**Not a web application** — this is a CLI-driven batch processing system backed by SQLite.

## Tech Stack

- **Language:** Python 3.11+
- **Database:** SQLite3 (via stdlib `sqlite3`)
- **Key libraries:** `pandas`, `numpy`, `scikit-learn` (DBSCAN, GaussianMixture)
- **No web framework, no ORM, no Docker, no CI/CD**
- **No `requirements.txt`** — install deps manually: `pip install pandas numpy scikit-learn`

## Project Structure

All scripts live at the repository root. No nested package structure in the working tree.

```
PandaLive/
├── schema.sql                      # Full SQLite DDL (26 tables)
├── tables.txt                      # Table name listing
│
├── Phase 2 – Feature Computation
│   ├── panda_phase2_create.py      # Schema creation for phase 2 tables
│   ├── panda_phase2_1_wtokenflow.py  # Wallet token flow aggregation
│   ├── panda_phase2_2_wEdges.py    # Inter-wallet edge graph
│   ├── panda_phase2_3_cohorts.py   # Cohort detection
│   ├── panda_phase2_4_Cmetrics.py  # Cohort metrics
│   ├── panda_phase2_4_ownerlink.py # Owner-link analysis
│   ├── panda_phase2_4_recheck.py   # Re-check validation
│   ├── panda_phase2_5_cohort.py    # Cohort refinement
│   ├── panda_phase2_7_acceptv2.py  # Acceptance testing
│   ├── panda_phase2_7_db_inspect.py # DB inspection
│   ├── panda_phase2_7_tokenflow.py # Token flow audit
│   ├── panda_phase2_reaudit.py     # Re-audit v1
│   └── panda_phase2_reauditV2.py   # Re-audit v2
│
├── Phase 3 – Whale Detection & Forensics
│   ├── panda_phase3_1_Wfeatures.py    # Wallet features
│   ├── panda_phase3_1_Accept.py       # Phase 3.1 acceptance
│   ├── panda_phase3_2_build_whale.py  # Whale builder
│   ├── panda_phase3_2_persist.py      # Whale state persistence
│   ├── panda_phase3_2_recompute.py    # Recompute whale states
│   ├── panda_phase3_2_flowref.py      # Flow reference resolution
│   ├── panda_phase3_2_semantics.py    # Semantic analysis (largest script)
│   ├── panda_phase3_2_forensics.py    # Forensics v1
│   ├── panda_phase3_2_forensicsV2.py  # Forensics v2
│   ├── panda_phase3_2_forensicsv3.py  # Forensics v3
│   ├── panda_phase3_2_forensicsv4.py  # Forensics v4
│   ├── panda_phase3_2_accept_whale.py # Whale acceptance v1
│   ├── panda_phase3_2_accept_whale_v2.py # Whale acceptance v2
│   ├── panda_phase3_3_transitions.py  # State transitions
│   ├── panda_phase3_3_acceptance.py   # Phase 3.3 acceptance
│   ├── panda_phase3_dbinspect.py      # DB inspector
│   └── panda_phase3_nuketables.py     # Table cleanup
│
├── Phase 4 – Pattern Discovery & Normalization
│   ├── panda_phase4_Wedges.py         # Edge extraction
│   ├── panda_phase4_Wclusters.py      # Wallet clustering
│   ├── panda_phase4_4_discovery.py    # Pattern discovery (DBSCAN)
│   ├── panda_phase4_5_validate.py     # Validation
│   ├── panda_phase4_6_feature.py      # Feature extraction
│   ├── panda_phase4_7_null.py         # Null handling
│   └── panda_phase4_db_inspect.py     # Phase 4 DB inspection
│
├── SPL Transfer Processing
│   ├── panda_spl_v2_create.py         # SPL v2 schema
│   ├── panda_spl_v2_extract.py        # SPL v2 extraction
│   ├── panda_spl_v2_inspect.py        # SPL v2 inspection
│   └── panda_spl_v2_qa.py             # SPL v2 QA
│
├── Utilities & Auditing
│   ├── panda_db_sufficiency_audit.py  # Data quality audit (largest file)
│   ├── panda_backfill_spl_transfers_from_rawjson.py  # SPL backfill
│   ├── extract_profit_situations.py   # Profit extraction
│   ├── step2_validate_archetypes.py   # Archetype validation
│   ├── inspect_db.py                  # Generic DB inspector
│   ├── count_edge_tokens.py           # Edge token counter
│   └── make_absurd_rows.py            # Test data generator
│
├── V4 Scripts
│   ├── panda_v4_phase2_6_inspection.py
│   └── panda_v4_phase2_7_tokenflow.py
│
├── Tests
│   ├── test_mapping.py                # Column mapping tests
│   ├── test_phase1.py                 # (in archive)
│   ├── test_phase2.py                 # (in archive)
│   └── test_phase3.py                 # (in archive)
│
├── Data Files
│   ├── KOLScanWallets.txt             # KOL wallet list
│   ├── powerwallets_wallet_name_only.txt  # Power wallet list
│   ├── whale_events.sample.tsv        # Sample data
│   ├── whale_states.sample.tsv
│   └── whale_transitions.sample.tsv
│
└── Archives
    ├── panda_live_final_v2.tar.gz     # Packaged application
    └── ...other archives
```

## Data Pipeline Phases

```
Raw Transaction Data (tx, spl_transfers, swaps)
  → Phase 2: Token flow aggregation, edge graph, cohort detection
  → Phase 3: Whale detection, forensic analysis, state transitions
  → Phase 4: Feature normalization (N1-N16), DBSCAN clustering, pattern discovery
```

## Running Scripts

Every script is a standalone CLI tool invoked with `--db`:

```bash
python3 panda_phase2_1_wtokenflow.py --db /path/to/db.sqlite3
python3 panda_phase2_2_wEdges.py --db /path/to/db.sqlite3 --now-ts 1700000000
python3 panda_phase4_4_discovery.py --db /path/to/db.sqlite3
```

Common flags:
- `--db` (required) — path to SQLite database
- `--now-ts` — override current timestamp (unix epoch seconds)
- `--topn` — top-N count for ranked results
- `--recreate-empty` — drop and recreate tables
- `--skip-checks` — skip validation steps

## Database

**26 SQLite tables.** Schema in `schema.sql`. Key groups:

| Group | Tables |
|-------|--------|
| Raw data | `tx`, `spl_transfers`, `spl_transfers_v2`, `swaps`, `files`, `wallets` |
| Phase 2 | `wallet_token_flow`, `wallet_edges`, `wallet_features`, `cohorts`, `cohort_members`, `recycling_flags`, `phase2_runs` |
| Phase 3 | `whale_events`, `whale_states`, `whale_transitions` |
| Phase 4 | `phase4_runs`, `phase4_features_norm`, `phase4_patterns`, `phase4_pattern_stats`, `phase4_cluster_runs`, `phase4_edge_runs`, `wallet_clusters`, `cluster_runs` |

**Window kinds** are constrained to: `lifetime`, `24h`, `7d`

**16 normalized features** (N1–N16) in `phase4_features_norm`:
- N1-N5: Activity (tx rate, inflow/outflow rate, token interaction, burstiness)
- N6-N8: Network (counterparty rate, repetition ratio, edge density)
- N9-N10: Clustering (membership intensity, intra-cluster flow)
- N11-N13: Whale (enter recency, magnitude, flow delta)
- N14-N16: Lifecycle (token reentry, capital recycling, wallet age)

## Key Constants

```python
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
WSOL_MINT = "So11111111111111111111111111111111111111112"
WINDOWS = [("lifetime", 0, None), ("24h", ...), ("7d", ...)]
```

## Data Quality Doctrine

Defined in `panda_db_sufficiency_audit.py`:
- **Zero-guesswork doctrine** — no assumptions about missing data
- Raw JSON preservation ≥ 95%
- Time anchor coverage ≥ 99% of blocks
- Maximum time gap: 6 hours
- Zero duplicate tolerance
- Invariant: `net_amount_raw = in_amount - out_amount`

## Code Conventions

- **Shebang:** `#!/usr/bin/env python3` on every script
- **Type hints** used throughout (from `typing` module)
- **argparse** for all CLI interfaces
- **Direct SQLite3** — no ORM; raw SQL with parameterized queries
- **Naming:** `panda_phase{N}_{step}_{description}.py`
- **Versioned iterations:** forensics has v1-v4, acceptance has v1-v2
- **Acceptance scripts** produce JSON summary files (e.g., `phase3_1_accept_wallet_features.summary.json`)
- **Column mapping** uses candidate lists for flexible schema matching (see `SPL_TRANSFER_FIELD_CANDIDATES` pattern)

## Testing

Tests are lightweight validation scripts, not pytest/unittest:

```bash
python3 test_mapping.py /path/to/db.sqlite3
```

Tests verify:
- Column existence and schema conformance
- Data invariants (QA checks)
- Acceptance criteria via JSON output

## Important Notes for AI Assistants

1. **No build system** — scripts run directly with `python3`
2. **All state lives in SQLite** — the `.db` file is the single source of truth
3. **Scripts are idempotent** — designed for re-runs with `--recreate-empty`
4. **Window constraint** is enforced at the DB level (`CHECK` clauses) — only `lifetime`, `24h`, `7d`
5. **Amounts are in raw units** (lamports for SOL, raw token amounts) — not human-readable decimals
6. **Versioned scripts** (v2, v3, v4 suffixes) represent iterative improvements; the highest version is typically the current one
7. **No environment variables or `.env` files** — all config via CLI args and hardcoded constants
8. **Archives** (`*.tar.gz`, `*.zip`) contain packaged versions; the working scripts are at repo root
