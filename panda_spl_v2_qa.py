import argparse
import json
import sqlite3
from typing import Any, List, Optional, Tuple

SPL_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

BLOCK_TIME_CANDIDATES = ["block_time", "blockTime", "blocktime", "time", "timestamp"]
SLOT_CANDIDATES = ["slot", "block_slot", "blockSlot"]
SCAN_WALLET_CANDIDATES = ["scan_wallet", "wallet", "owner_wallet"]


def detect_columns(conn: sqlite3.Connection) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    rows = conn.execute("PRAGMA table_info(tx)").fetchall()
    columns = {row[1] for row in rows}
    block_time_col = next((c for c in BLOCK_TIME_CANDIDATES if c in columns), None)
    slot_col = next((c for c in SLOT_CANDIDATES if c in columns), None)
    scan_wallet_col = next((c for c in SCAN_WALLET_CANDIDATES if c in columns), None)
    return block_time_col, slot_col, scan_wallet_col


def normalize_account_key(entry: Any) -> Optional[str]:
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict):
        return entry.get("pubkey")
    return None


def normalize_account_keys(entries: List[Any]) -> List[str]:
    return [key for key in (normalize_account_key(entry) for entry in entries) if key]


def build_effective_account_keys(message: dict, meta: dict) -> List[str]:
    effective_keys = normalize_account_keys(message.get("accountKeys", []))
    loaded = meta.get("loadedAddresses", {})
    writable = loaded.get("writable", [])
    readonly = loaded.get("readonly", [])
    if isinstance(writable, list):
        effective_keys.extend(normalize_account_keys(writable))
    if isinstance(readonly, list):
        effective_keys.extend(normalize_account_keys(readonly))
    return effective_keys


def main() -> None:
    parser = argparse.ArgumentParser(description="QA checks for spl_transfers_v2")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    block_time_col, slot_col, scan_wallet_col = detect_columns(conn)
    select_cols = ["signature", "raw_json"]
    if scan_wallet_col:
        select_cols.append(f"{scan_wallet_col} AS scan_wallet")
    else:
        select_cols.append("NULL AS scan_wallet")
    if block_time_col:
        select_cols.append(f"{block_time_col} AS block_time")
    else:
        select_cols.append("NULL AS block_time")
    if slot_col:
        select_cols.append(f"{slot_col} AS slot")
    else:
        select_cols.append("NULL AS slot")

    query = f"SELECT {', '.join(select_cols)} FROM tx"

    total_tx = 0
    token_instructions = 0

    for row in conn.execute(query):
        total_tx += 1
        try:
            tx_json = json.loads(row["raw_json"])
        except json.JSONDecodeError:
            continue

        message = tx_json.get("transaction", {}).get("message", {})
        meta = tx_json.get("meta", {})
        account_keys = build_effective_account_keys(message, meta)
        instructions = message.get("instructions", [])
        if not isinstance(instructions, list):
            continue

        for instruction in instructions:
            if not isinstance(instruction, dict):
                continue
            program_id_index = instruction.get("programIdIndex")
            if not isinstance(program_id_index, int):
                continue
            if program_id_index < 0 or program_id_index >= len(account_keys):
                continue
            program_id = normalize_account_key(account_keys[program_id_index])
            if program_id in (SPL_TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID):
                token_instructions += 1

    total_rows = conn.execute("SELECT COUNT(*) FROM spl_transfers_v2").fetchone()[0]

    decode_rows = conn.execute(
        "SELECT decode_status, COUNT(*) AS cnt FROM spl_transfers_v2 GROUP BY decode_status"
    ).fetchall()

    coverage_row = conn.execute(
        "SELECT COUNT(*) FROM spl_transfers_v2 WHERE from_addr IS NOT NULL AND to_addr IS NOT NULL"
    ).fetchone()[0]
    coverage_percent = (coverage_row / total_rows * 100.0) if total_rows else 0.0

    transfer_like_total = conn.execute(
        "SELECT COUNT(*) FROM spl_transfers_v2 WHERE instruction_type IN "
        "('transfer','transfer_checked','close_account') AND token_program_kind IN "
        "('spl_token','token_2022')"
    ).fetchone()[0]
    transfer_like_covered = conn.execute(
        "SELECT COUNT(*) FROM spl_transfers_v2 WHERE instruction_type IN "
        "('transfer','transfer_checked','close_account') AND token_program_kind IN "
        "('spl_token','token_2022') AND from_addr IS NOT NULL AND to_addr IS NOT NULL"
    ).fetchone()[0]
    transfer_like_percent = (
        (transfer_like_covered / transfer_like_total * 100.0) if transfer_like_total else 0.0
    )

    top_instructions = conn.execute(
        "SELECT instruction_type, COUNT(*) AS cnt FROM spl_transfers_v2 "
        "GROUP BY instruction_type ORDER BY cnt DESC LIMIT 10"
    ).fetchall()

    samples = conn.execute(
        "SELECT signature, ix_index, instruction_type, decode_status, decode_error "
        "FROM spl_transfers_v2 WHERE decode_status != 'ok' LIMIT 20"
    ).fetchall()

    print(f"total_tx_rows_scanned: {total_tx}")
    print(f"total_token_instructions: {token_instructions}")
    print(f"total_rows_in_spl_transfers_v2: {total_rows}")
    print(f"total_transfer_like_rows: {transfer_like_total}")
    print("decode_status_breakdown:")
    for row in decode_rows:
        print(f"  {row['decode_status']}: {row['cnt']}")
    print(f"endpoint_coverage_percent_all_rows: {coverage_percent:.2f}")
    print(f"endpoint_coverage_percent_transfer_like: {transfer_like_percent:.2f}")
    print("top_instruction_types:")
    for row in top_instructions:
        print(f"  {row['instruction_type']}: {row['cnt']}")
    print("sample_non_ok_rows:")
    for row in samples:
        print(
            f"  {row['signature']} | {row['ix_index']} | {row['instruction_type']} | "
            f"{row['decode_status']} | {row['decode_error']}"
        )

    conn.close()


if __name__ == "__main__":
    main()
