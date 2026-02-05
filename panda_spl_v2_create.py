import argparse
import sqlite3

def main() -> None:
    parser = argparse.ArgumentParser(description="Create spl_transfers_v2 table and indexes")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS spl_transfers_v2 (
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
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS spl2_sig ON spl_transfers_v2(signature)")
        conn.execute("CREATE INDEX IF NOT EXISTS spl2_time ON spl_transfers_v2(block_time)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS spl2_mint_time ON spl_transfers_v2(mint, block_time)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS spl2_from_time ON spl_transfers_v2(from_addr, block_time)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS spl2_to_time ON spl_transfers_v2(to_addr, block_time)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS spl2_program ON spl_transfers_v2(program_id)")
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
