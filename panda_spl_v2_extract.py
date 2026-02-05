import argparse
import base64
import json
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

SPL_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

BLOCK_TIME_CANDIDATES = ["block_time", "blockTime", "blocktime", "time", "timestamp"]
SLOT_CANDIDATES = ["slot", "block_slot", "blockSlot"]
SCAN_WALLET_CANDIDATES = ["scan_wallet", "wallet", "owner_wallet"]

SUPPORTED_OPCODES = {
    3: ("transfer", 1 + 8),
    12: ("transfer_checked", 1 + 8 + 1),
    7: ("mint_to", 1 + 8),
    8: ("burn", 1 + 8),
    15: ("burn_checked", 1 + 8 + 1),
    9: ("close_account", 1),
}

REQUIRED_ACCOUNTS = {
    "transfer": 3,
    "transfer_checked": 4,
    "mint_to": 3,
    "burn": 2,
    "burn_checked": 3,
    "close_account": 3,
}


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


def normalize_account_keys(entries: List[Any]) -> List[Optional[str]]:
    return [normalize_account_key(entry) for entry in entries]


def build_effective_account_keys(message: Dict[str, Any], meta: Dict[str, Any]) -> List[Optional[str]]:
    effective_keys: List[Optional[str]] = normalize_account_keys(message.get("accountKeys", []))
    loaded = meta.get("loadedAddresses", {})
    writable = loaded.get("writable", [])
    readonly = loaded.get("readonly", [])
    if isinstance(writable, list):
        effective_keys.extend(normalize_account_keys(writable))
    if isinstance(readonly, list):
        effective_keys.extend(normalize_account_keys(readonly))
    return effective_keys


def resolve_accounts(account_keys: List[Any], indices: List[Any]) -> List[Optional[str]]:
    resolved: List[Optional[str]] = []
    for idx in indices:
        if not isinstance(idx, int) or idx < 0 or idx >= len(account_keys):
            resolved.append(None)
            continue
        resolved.append(normalize_account_key(account_keys[idx]))
    return resolved


def decode_u64_le(data: bytes, offset: int) -> int:
    return int.from_bytes(data[offset : offset + 8], "little", signed=False)


def base58_decode(data: str) -> bytes:
    if not data:
        return b""
    value = 0
    for char in data:
        idx = BASE58_ALPHABET.find(char)
        if idx == -1:
            raise ValueError("invalid base58 character")
        value = value * 58 + idx
    encoded = value.to_bytes((value.bit_length() + 7) // 8, "big") if value else b""
    leading_zeros = len(data) - len(data.lstrip("1"))
    return b"\x00" * leading_zeros + encoded


def parse_instruction(
    program_id: str,
    accounts: List[Optional[str]],
    ix_data_b64: str,
) -> Tuple[str, str, Optional[str], Optional[int], str, Optional[str]]:
    instruction_type = "unknown_token_ix"
    decode_status = "unsupported_ix"
    decode_error: Optional[str] = None
    amount_raw: Optional[str] = None
    decimals: Optional[int] = None

    try:
        data = base64.b64decode(ix_data_b64, validate=True)
    except (base64.binascii.Error, ValueError) as exc:
        try:
            data = base58_decode(ix_data_b64)
        except ValueError as base58_exc:
            return (
                instruction_type,
                "malformed",
                None,
                None,
                f"base64+base58 decode fail: {exc} | {base58_exc}",
                None,
            )

    if not data:
        return instruction_type, "malformed", None, None, "empty data", None

    opcode = data[0]
    if opcode not in SUPPORTED_OPCODES:
        return instruction_type, "unsupported_ix", None, None, None, None

    instruction_type, expected_len = SUPPORTED_OPCODES[opcode]
    if len(data) < expected_len:
        return instruction_type, "malformed", None, None, "insufficient data", None

    if expected_len >= 1 + 8:
        amount_raw = str(decode_u64_le(data, 1))
    if expected_len == 1 + 8 + 1:
        decimals = data[1 + 8]

    required = REQUIRED_ACCOUNTS.get(instruction_type, 0)
    if len(accounts) < required:
        return instruction_type, "missing_accounts", amount_raw, decimals, None, None

    return instruction_type, "ok", amount_raw, decimals, None, None


def map_accounts(instruction_type: str, accounts: List[Optional[str]]) -> Dict[str, Optional[str]]:
    mapping: Dict[str, Optional[str]] = {
        "from_addr": None,
        "to_addr": None,
        "mint": None,
        "authority": None,
    }

    if instruction_type == "transfer":
        mapping["from_addr"] = accounts[0] if len(accounts) > 0 else None
        mapping["to_addr"] = accounts[1] if len(accounts) > 1 else None
        mapping["authority"] = accounts[2] if len(accounts) > 2 else None
    elif instruction_type == "transfer_checked":
        mapping["from_addr"] = accounts[0] if len(accounts) > 0 else None
        mapping["mint"] = accounts[1] if len(accounts) > 1 else None
        mapping["to_addr"] = accounts[2] if len(accounts) > 2 else None
        mapping["authority"] = accounts[3] if len(accounts) > 3 else None
    elif instruction_type == "mint_to":
        mapping["mint"] = accounts[0] if len(accounts) > 0 else None
        mapping["to_addr"] = accounts[1] if len(accounts) > 1 else None
        mapping["authority"] = accounts[2] if len(accounts) > 2 else None
    elif instruction_type == "burn":
        mapping["from_addr"] = accounts[0] if len(accounts) > 0 else None
        mapping["mint"] = accounts[1] if len(accounts) > 2 else None
        if len(accounts) > 2:
            mapping["authority"] = accounts[2]
        elif len(accounts) > 1:
            mapping["authority"] = accounts[1]
    elif instruction_type == "burn_checked":
        mapping["from_addr"] = accounts[0] if len(accounts) > 0 else None
        mapping["mint"] = accounts[1] if len(accounts) > 1 else None
        mapping["authority"] = accounts[2] if len(accounts) > 2 else None
    elif instruction_type == "close_account":
        mapping["from_addr"] = accounts[0] if len(accounts) > 0 else None
        mapping["to_addr"] = accounts[1] if len(accounts) > 1 else None
        mapping["authority"] = accounts[2] if len(accounts) > 2 else None
    return mapping


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract SPL token transfers into spl_transfers_v2")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of tx rows")
    parser.add_argument("--where", default=None, help="Optional SQL fragment for filtering tx")
    parser.add_argument("--commit-every", type=int, default=5000, help="Commit every N rows")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress output")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    block_time_col, slot_col, scan_wallet_col = detect_columns(conn)

    select_cols = [
        "signature",
        "raw_json",
    ]
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
    params: List[Any] = []
    if args.where:
        query += f" WHERE {args.where}"
    if args.limit is not None:
        query += " LIMIT ?"
        params.append(args.limit)

    insert_sql = (
        "INSERT OR REPLACE INTO spl_transfers_v2 ("
        "signature, ix_index, event_index, scan_wallet, block_time, slot, program_id, "
        "token_program_kind, instruction_type, source_owner, from_addr, to_addr, mint, "
        "amount_raw, decimals, authority, multisig_signers_json, accounts_json, ix_data_b64, "
        "decode_status, decode_error, created_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    total_rows = 0
    rows_written = 0
    conn.execute("BEGIN")
    try:
        for row in conn.execute(query, params):
            total_rows += 1
            signature = row["signature"]
            raw_json = row["raw_json"]
            scan_wallet = row["scan_wallet"]
            block_time = row["block_time"]
            slot = row["slot"]

            try:
                tx_json = json.loads(raw_json)
            except json.JSONDecodeError as exc:
                if not args.quiet:
                    print(f"Skipping signature {signature}: JSON decode error {exc}")
                continue

            message = tx_json.get("transaction", {}).get("message", {})
            meta = tx_json.get("meta", {})
            account_keys = build_effective_account_keys(message, meta)
            instructions = message.get("instructions", [])
            if not isinstance(instructions, list):
                continue

            for ix_index, instruction in enumerate(instructions):
                if not isinstance(instruction, dict):
                    conn.execute(
                        insert_sql,
                        (
                            signature,
                            ix_index,
                            0,
                            scan_wallet,
                            block_time,
                            slot,
                            "",
                            "unknown",
                            "unknown_token_ix",
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            "[]",
                            "",
                            "unknown_layout",
                            "instruction_not_dict",
                            int(time.time()),
                        ),
                    )
                    rows_written += 1
                    continue
                program_id_index = instruction.get("programIdIndex")
                if program_id_index is None:
                    accounts_idx = instruction.get("accounts")
                    accounts_json = (
                        json.dumps(resolve_accounts(account_keys, accounts_idx))
                        if isinstance(accounts_idx, list)
                        else "[]"
                    )
                    ix_data_b64 = instruction.get("data") if isinstance(instruction.get("data"), str) else ""
                    conn.execute(
                        insert_sql,
                        (
                            signature,
                            ix_index,
                            0,
                            scan_wallet,
                            block_time,
                            slot,
                            "",
                            "unknown",
                            "unknown_token_ix",
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            accounts_json,
                            ix_data_b64,
                            "unknown_layout",
                            "missing_programIdIndex",
                            int(time.time()),
                        ),
                    )
                    rows_written += 1
                    continue
                if not isinstance(program_id_index, int):
                    accounts_idx = instruction.get("accounts")
                    accounts_json = (
                        json.dumps(resolve_accounts(account_keys, accounts_idx))
                        if isinstance(accounts_idx, list)
                        else "[]"
                    )
                    ix_data_b64 = instruction.get("data") if isinstance(instruction.get("data"), str) else ""
                    conn.execute(
                        insert_sql,
                        (
                            signature,
                            ix_index,
                            0,
                            scan_wallet,
                            block_time,
                            slot,
                            "",
                            "unknown",
                            "unknown_token_ix",
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            accounts_json,
                            ix_data_b64,
                            "unknown_layout",
                            "missing_programIdIndex",
                            int(time.time()),
                        ),
                    )
                    rows_written += 1
                    continue
                if program_id_index < 0 or program_id_index >= len(account_keys):
                    accounts_idx = instruction.get("accounts")
                    accounts_json = (
                        json.dumps(resolve_accounts(account_keys, accounts_idx))
                        if isinstance(accounts_idx, list)
                        else "[]"
                    )
                    ix_data_b64 = instruction.get("data") if isinstance(instruction.get("data"), str) else ""
                    conn.execute(
                        insert_sql,
                        (
                            signature,
                            ix_index,
                            0,
                            scan_wallet,
                            block_time,
                            slot,
                            "",
                            "unknown",
                            "unknown_token_ix",
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            accounts_json,
                            ix_data_b64,
                            "unknown_layout",
                            "programIdIndex_oob",
                            int(time.time()),
                        ),
                    )
                    rows_written += 1
                    continue
                program_id = account_keys[program_id_index]
                if not program_id:
                    accounts_idx = instruction.get("accounts")
                    accounts_json = (
                        json.dumps(resolve_accounts(account_keys, accounts_idx))
                        if isinstance(accounts_idx, list)
                        else "[]"
                    )
                    ix_data_b64 = instruction.get("data") if isinstance(instruction.get("data"), str) else ""
                    conn.execute(
                        insert_sql,
                        (
                            signature,
                            ix_index,
                            0,
                            scan_wallet,
                            block_time,
                            slot,
                            "",
                            "unknown",
                            "unknown_token_ix",
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            accounts_json,
                            ix_data_b64,
                            "unknown_layout",
                            "program_id_unresolvable",
                            int(time.time()),
                        ),
                    )
                    rows_written += 1
                    continue
                if program_id not in (SPL_TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID):
                    continue

                token_program_kind = (
                    "spl_token" if program_id == SPL_TOKEN_PROGRAM_ID else "token_2022"
                )

                accounts_idx = instruction.get("accounts")
                ix_data_b64 = instruction.get("data")
                if not isinstance(accounts_idx, list) or not isinstance(ix_data_b64, str):
                    accounts_json = json.dumps(
                        resolve_accounts(account_keys, accounts_idx)
                        if isinstance(accounts_idx, list)
                        else []
                    )
                    conn.execute(
                        insert_sql,
                        (
                            signature,
                            ix_index,
                            0,
                            scan_wallet,
                            block_time,
                            slot,
                            program_id,
                            token_program_kind,
                            "unknown_token_ix",
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            accounts_json,
                            ix_data_b64 if isinstance(ix_data_b64, str) else "",
                            "unknown_layout",
                            "missing_accounts_or_data",
                            int(time.time()),
                        ),
                    )
                    rows_written += 1
                    continue

                accounts = resolve_accounts(account_keys, accounts_idx)
                accounts_json = json.dumps(accounts)

                (
                    instruction_type,
                    decode_status,
                    amount_raw,
                    decimals,
                    decode_error,
                    _,
                ) = parse_instruction(program_id, accounts, ix_data_b64)

                mapped = map_accounts(instruction_type, accounts)

                conn.execute(
                    insert_sql,
                    (
                        signature,
                        ix_index,
                        0,
                        scan_wallet,
                        block_time,
                        slot,
                        program_id,
                        token_program_kind,
                        instruction_type,
                        None,
                        mapped.get("from_addr"),
                        mapped.get("to_addr"),
                        mapped.get("mint"),
                        amount_raw,
                        decimals,
                        mapped.get("authority"),
                        None,
                        accounts_json,
                        ix_data_b64,
                        decode_status,
                        decode_error,
                        int(time.time()),
                    ),
                )
                rows_written += 1

            if total_rows % args.commit_every == 0:
                conn.commit()
                conn.execute("BEGIN")
                if not args.quiet:
                    print(f"Processed {total_rows} tx rows | rows_written {rows_written}")

        conn.commit()
        if not args.quiet:
            print(f"Completed. Total tx rows processed: {total_rows}")
            print(f"rows_written: {rows_written}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
