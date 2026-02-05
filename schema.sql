CREATE INDEX idx_cohort_members_wallet ON cohort_members(wallet)
CREATE INDEX idx_cohorts_scope ON cohorts(scope_kind, mint, window_kind, window_end_ts)
CREATE INDEX idx_cohorts_score ON cohorts(window_kind, window_end_ts, cohort_score DESC)
CREATE INDEX idx_phase2_runs_component ON phase2_runs(component, window_kind, window_end_ts)
CREATE INDEX idx_recycle_mint_window ON recycling_flags(mint, window_kind, window_end_ts)
CREATE INDEX idx_recycle_severity ON recycling_flags(window_kind, window_end_ts, severity DESC)
CREATE INDEX idx_recycle_wallet_window ON recycling_flags(wallet, window_kind, window_end_ts)
CREATE INDEX idx_spl_sig ON spl_transfers(signature)
CREATE INDEX idx_swaps_sig ON swaps(signature)
CREATE INDEX idx_swaps_wallet_time ON swaps(scan_wallet, block_time)
CREATE INDEX idx_tx_sig ON tx(signature)
CREATE INDEX idx_tx_wallet_time ON tx(scan_wallet, block_time)
CREATE INDEX spl2_from_time ON spl_transfers_v2(from_addr, block_time)
CREATE INDEX spl2_mint_time ON spl_transfers_v2(mint, block_time)
CREATE INDEX spl2_program ON spl_transfers_v2(program_id)
CREATE INDEX spl2_sig ON spl_transfers_v2(signature)
CREATE INDEX spl2_time ON spl_transfers_v2(block_time)
CREATE INDEX spl2_to_time ON spl_transfers_v2(to_addr, block_time)
CREATE TABLE cluster_runs (
            cluster_run_id TEXT PRIMARY KEY,
            window_kind TEXT NOT NULL,
            window_start_ts INTEGER NOT NULL,
            window_end_ts INTEGER NOT NULL,
            algo TEXT NOT NULL,
            params_json TEXT NOT NULL,
            wallet_count INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            CHECK (window_kind IN ('lifetime','24h','7d'))
        )
CREATE TABLE cohort_members (
            cohort_id TEXT NOT NULL,
            wallet TEXT NOT NULL,
            role_hint TEXT,
            inflow_raw INTEGER,
            outflow_raw INTEGER,
            degree_in INTEGER,
            degree_out INTEGER,
            PRIMARY KEY (cohort_id, wallet),
            FOREIGN KEY (cohort_id) REFERENCES cohorts(cohort_id) ON DELETE CASCADE
        )
CREATE TABLE cohorts (
            cohort_id TEXT PRIMARY KEY,
            mint TEXT,
            scope_kind TEXT NOT NULL,
            window_kind TEXT NOT NULL,
            window_start_ts INTEGER NOT NULL,
            window_end_ts INTEGER NOT NULL,
            member_count INTEGER NOT NULL,
            edge_density REAL,
            internal_flow_raw INTEGER,
            external_flow_raw INTEGER,
            cohort_score REAL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            CHECK (window_kind IN ('lifetime','24h','7d'))
        )
CREATE TABLE files (
      file_path TEXT PRIMARY KEY,
      file_name TEXT,
      file_size INTEGER,
      mtime INTEGER,
      sha256 TEXT,
      ingested_at INTEGER
    )
CREATE TABLE phase2_runs (
            run_id TEXT PRIMARY KEY,
            component TEXT NOT NULL,
            window_kind TEXT NOT NULL,
            window_start_ts INTEGER NOT NULL,
            window_end_ts INTEGER NOT NULL,
            input_counts_json TEXT NOT NULL,
            output_counts_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            CHECK (window_kind IN ('lifetime','24h','7d'))
        )
CREATE TABLE phase4_cluster_runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT,
                digest TEXT,
                rowcount INTEGER,
                code_sha256 TEXT
            )
CREATE TABLE phase4_edge_runs (
            run_id TEXT PRIMARY KEY,
            started_at INTEGER NOT NULL,
            max_time INTEGER NOT NULL,
            digest TEXT NOT NULL,
            rowcount INTEGER NOT NULL,
            code_sha256 TEXT NOT NULL
        )
CREATE TABLE phase4_features_norm (
            run_id TEXT,
            wallet TEXT,
            window TEXT,
            created_at INTEGER,
            N1_tx_rate REAL,
            N2_inflow_rate REAL,
            N3_outflow_rate REAL,
            N4_token_interaction_rate REAL,
            N5_burstiness_index REAL,
            N6_counterparty_rate REAL,
            N7_counterparty_repetition_ratio REAL,
            N8_edge_density_norm REAL,
            N9_cluster_membership_intensity REAL,
            N10_intra_cluster_flow_ratio REAL,
            N11_whale_enter_recency_sec REAL,
            N12_whale_enter_magnitude_log REAL,
            N13_flow_delta_around_enter REAL,
            N14_token_reentry_rate REAL,
            N15_capital_recycling_ratio REAL,
            N16_wallet_age_log REAL
        )
CREATE TABLE phase4_pattern_stats (
            run_id TEXT,
            window TEXT,
            lens_id TEXT,
            pattern_id TEXT,
            member_count INTEGER
        )
CREATE TABLE phase4_patterns (
            run_id TEXT,
            wallet TEXT,
            window TEXT,
            lens_id TEXT,
            pattern_id TEXT,
            is_noise INTEGER,
            created_at INTEGER
        )
CREATE TABLE phase4_runs (
            run_id TEXT PRIMARY KEY,
            started_at INTEGER,
            code_sha256 TEXT,
            window_defs_json TEXT,
            feature_defs_json TEXT,
            max_time INTEGER
        )
CREATE TABLE recycling_flags (
            wallet TEXT NOT NULL,
            mint TEXT NOT NULL,
            window_kind TEXT NOT NULL,
            window_start_ts INTEGER NOT NULL,
            window_end_ts INTEGER NOT NULL,
            pattern_kind TEXT NOT NULL,
            pattern_id TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            severity REAL NOT NULL,
            first_seen_ts INTEGER,
            last_seen_ts INTEGER,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (wallet, mint, window_kind, window_start_ts, window_end_ts, pattern_kind, pattern_id),
            CHECK (window_kind IN ('lifetime','24h','7d'))
        )
CREATE TABLE spl_transfers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      scan_wallet TEXT NOT NULL,
      signature TEXT NOT NULL,
      direction TEXT NOT NULL,           -- 'in' or 'out'
      mint TEXT,
      amount TEXT,
      from_addr TEXT,
      to_addr TEXT,
      raw_json TEXT NOT NULL,
      ingested_at INTEGER NOT NULL
    , block_time INTEGER, slot INTEGER, backfill_status TEXT, backfill_reason TEXT)
CREATE TABLE spl_transfers_v2 (
                signature TEXT NOT NULL,
                ix_index INTEGER NOT NULL,
                event_index INTEGER NOT NULL,
                scan_wallet TEXT,
                block_time INTEGER,
                slot INTEGER,
                program_id TEXT NOT NULL,
                token_program_kind TEXT NOT NULL,
                instruction_type TEXT NOT NULL,
                source_owner TEXT,
                from_addr TEXT,
                to_addr TEXT,
                mint TEXT,
                amount_raw TEXT,
                decimals INTEGER,
                authority TEXT,
                multisig_signers_json TEXT,
                accounts_json TEXT NOT NULL,
                ix_data_b64 TEXT NOT NULL,
                decode_status TEXT NOT NULL,
                decode_error TEXT,
                created_at INTEGER NOT NULL,
                UNIQUE(signature, ix_index, event_index)
            )
CREATE TABLE sqlite_sequence(name,seq)
CREATE TABLE swaps (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      scan_wallet TEXT NOT NULL,
      signature TEXT NOT NULL,
      block_time INTEGER,
      dex TEXT,
      in_mint TEXT,
      in_amount_raw TEXT,
      out_mint TEXT,
      out_amount_raw TEXT,
      has_sol_leg INTEGER NOT NULL,
      sol_direction TEXT,
      sol_amount_lamports INTEGER,
      token_mint TEXT,
      token_amount_raw TEXT,
      UNIQUE(scan_wallet, signature, in_mint, out_mint, in_amount_raw, out_amount_raw)
    )
CREATE TABLE tx (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      scan_wallet TEXT NOT NULL,
      signature TEXT NOT NULL,
      block_time INTEGER,
      slot INTEGER,
      tx_index INTEGER,
      version INTEGER,
      pre_balance_sol REAL,
      post_balance_sol REAL,
      balance_delta_sol REAL,
      spl_in_count INTEGER,
      spl_out_count INTEGER,
      err TEXT,
      source_file TEXT,
      raw_json TEXT NOT NULL,
      ingested_at INTEGER NOT NULL,
      UNIQUE(scan_wallet, signature)
    )
CREATE TABLE wallet_clusters (
                scan_wallet TEXT NOT NULL,
                window TEXT NOT NULL,
                cluster_id INTEGER NOT NULL,
                created_at_utc INTEGER NOT NULL,
                PRIMARY KEY (scan_wallet, window, cluster_id)
            )
CREATE TABLE wallet_edges (
    src_wallet TEXT NOT NULL,
    dst_wallet TEXT NOT NULL,
    edge_type TEXT NOT NULL,
    weight INTEGER NOT NULL,
    window TEXT NOT NULL,
    created_at_utc INTEGER NOT NULL,
    PRIMARY KEY (src_wallet, dst_wallet, edge_type, window)
)
CREATE TABLE wallet_features (
    scan_wallet TEXT NOT NULL,
    window TEXT NOT NULL,
    tx_count_total INTEGER NOT NULL,
    sol_volume_total INTEGER NOT NULL,
    created_at_utc INTEGER NOT NULL,
    PRIMARY KEY (scan_wallet, window)
)
CREATE TABLE wallet_token_flow (
        signature TEXT NOT NULL,
        scan_wallet TEXT NOT NULL,
        block_time INTEGER NOT NULL,
        dex TEXT,
        token_mint TEXT NOT NULL,
        token_amount_raw INTEGER NOT NULL,
        flow_direction TEXT NOT NULL,
        sol_direction TEXT NOT NULL,
        sol_amount_lamports INTEGER,
        source_table TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (signature, scan_wallet, token_mint, flow_direction)
    )
CREATE TABLE wallets (
      wallet_address TEXT PRIMARY KEY,
      wallet_label TEXT
    )
CREATE TABLE whale_events (
          wallet TEXT NOT NULL,
          window TEXT NOT NULL,
          event_time INTEGER NOT NULL,
          event_type TEXT NOT NULL,
          sol_amount_lamports INTEGER NOT NULL,
          supporting_flow_count INTEGER NOT NULL,
          flow_ref TEXT,
          created_at INTEGER NOT NULL
        )
CREATE TABLE whale_states (
            wallet TEXT NOT NULL,
            window TEXT NOT NULL,
            side TEXT NOT NULL,
            asof_time INTEGER NOT NULL,
            amount_lamports INTEGER NOT NULL,
            supporting_flow_count INTEGER NOT NULL,
            flow_ref TEXT NOT NULL,
            first_seen_time INTEGER NOT NULL,
            first_seen_flow_ref TEXT NOT NULL,
            is_whale INTEGER NOT NULL,
            PRIMARY KEY (wallet, window, side)
        )
CREATE TABLE whale_transitions (
            wallet TEXT NOT NULL,
            window TEXT NOT NULL,
            side TEXT NOT NULL,
            transition_type TEXT NOT NULL,
            event_time INTEGER NOT NULL,
            amount_lamports INTEGER NOT NULL,
            supporting_flow_count INTEGER NOT NULL,
            flow_ref TEXT NOT NULL,
            PRIMARY KEY (wallet, window, side, transition_type)
        )
