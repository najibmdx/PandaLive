#!/usr/bin/env python3
"""
panda_backfill_spl_transfers_from_rawjson.py

Backfill spl_transfers with chain-time and endpoints derived from tx table and raw_json.
Uses deterministic matching only - no guessing.

Usage:
    python panda_backfill_spl_transfers_from_rawjson.py --db masterwalletsdb.db --mode backfill_all
    python panda_backfill_spl_transfers_from_rawjson.py --db masterwalletsdb.db --mode backfill_missing
    python panda_backfill_spl_transfers_from_rawjson.py --db masterwalletsdb.db --mode report
"""

import sqlite3
import json
import argparse
from collections import defaultdict
from typing import Optional, Tuple, Dict, Any, List


class SPLTransferBackfiller:
    """Backfill SPL transfers with data extracted from tx table and raw_json."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.tx_signature_col = None
        self.tx_block_time_col = None
        self.tx_slot_col = None
        
    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
            
    def setup_schema(self):
        """Add new columns to spl_transfers table if they don't exist."""
        cursor = self.conn.cursor()
        
        # Check if columns exist
        cursor.execute("PRAGMA table_info(spl_transfers)")
        existing_cols = {row[1] for row in cursor.fetchall()}
        
        new_columns = [
            ('block_time', 'INTEGER'),
            ('slot', 'INTEGER'),
            ('from_addr', 'TEXT'),
            ('to_addr', 'TEXT'),
            ('backfill_status', 'TEXT'),
            ('backfill_reason', 'TEXT'),
        ]
        
        for col_name, col_type in new_columns:
            if col_name not in existing_cols:
                print(f"Adding column: {col_name}")
                cursor.execute(f"ALTER TABLE spl_transfers ADD COLUMN {col_name} {col_type}")
        
        self.conn.commit()
        print("Schema setup complete.\n")
        
    def detect_tx_schema(self):
        """Detect tx table schema and determine column mappings."""
        cursor = self.conn.cursor()
        
        # Check if tx table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='tx'
        """)
        if not cursor.fetchone():
            print("WARNING: tx table does not exist - will only use raw_json for time")
            return
            
        # Get tx columns
        cursor.execute("PRAGMA table_info(tx)")
        tx_cols = {row[1] for row in cursor.fetchall()}
        
        # Detect signature column
        for candidate in ['signature', 'sig', 'tx_signature', 'txSignature']:
            if candidate in tx_cols:
                self.tx_signature_col = candidate
                break
                
        # Detect block_time column (epoch seconds)
        for candidate in ['block_time', 'blockTime', 'blocktime', 'time', 'timestamp']:
            if candidate in tx_cols:
                self.tx_block_time_col = candidate
                break
                
        # Detect slot column
        for candidate in ['slot', 'slotNumber', 'slot_number']:
            if candidate in tx_cols:
                self.tx_slot_col = candidate
                break
                
        if self.tx_signature_col:
            print(f"TX schema detected: signature={self.tx_signature_col}, block_time={self.tx_block_time_col}, slot={self.tx_slot_col}")
        else:
            print("WARNING: Could not detect tx signature column - will only use raw_json for time")
            
    def parse_raw_json(self, raw_json_str: str) -> Optional[Dict[str, Any]]:
        """Parse raw_json string into dictionary."""
        if not raw_json_str:
            return None
        try:
            return json.loads(raw_json_str)
        except json.JSONDecodeError:
            return None
            
    def extract_time_from_raw_json(self, raw_json: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
        """
        Extract blockTime and slot from raw_json with robust detection.
        Returns (block_time, slot).
        Only extracts if truly present - no guessing.
        """
        block_time = None
        slot = None
        
        # Check top level
        if 'blockTime' in raw_json and raw_json['blockTime'] is not None:
            try:
                block_time = int(raw_json['blockTime'])
            except (ValueError, TypeError):
                pass
                
        if 'slot' in raw_json and raw_json['slot'] is not None:
            try:
                slot = int(raw_json['slot'])
            except (ValueError, TypeError):
                pass
        
        # Check result object (RPC style response)
        if 'result' in raw_json and isinstance(raw_json['result'], dict):
            result = raw_json['result']
            if 'blockTime' in result and result['blockTime'] is not None and block_time is None:
                try:
                    block_time = int(result['blockTime'])
                except (ValueError, TypeError):
                    pass
                    
            if 'slot' in result and result['slot'] is not None and slot is None:
                try:
                    slot = int(result['slot'])
                except (ValueError, TypeError):
                    pass
        
        # Check meta object
        if 'meta' in raw_json and isinstance(raw_json['meta'], dict):
            meta = raw_json['meta']
            if 'blockTime' in meta and meta['blockTime'] is not None and block_time is None:
                try:
                    block_time = int(meta['blockTime'])
                except (ValueError, TypeError):
                    pass
        
        # Check transaction object
        if 'transaction' in raw_json and isinstance(raw_json['transaction'], dict):
            tx = raw_json['transaction']
            if 'blockTime' in tx and tx['blockTime'] is not None and block_time is None:
                try:
                    block_time = int(tx['blockTime'])
                except (ValueError, TypeError):
                    pass
                    
            if 'slot' in tx and tx['slot'] is not None and slot is None:
                try:
                    slot = int(tx['slot'])
                except (ValueError, TypeError):
                    pass
        
        return block_time, slot
        
    def extract_token_transfers(self, raw_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract all token transfer instructions from raw_json.
        Returns list of dicts with: mint, amount (base units), from_addr (token account), to_addr (token account)
        
        Priority:
        1. Explicit transfer lists (tokenTransfers, parsedTokenTransfers, events.tokenTransfers)
        2. Balance-delta reconstruction with deterministic pairing
        """
        # Try explicit transfer lists first
        explicit_transfers = self._extract_explicit_transfers(raw_json)
        if explicit_transfers:
            return explicit_transfers
            
        # Fallback to balance-delta method
        return self._extract_transfers_from_balances(raw_json)
        
    def _extract_explicit_transfers(self, raw_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check for explicit token transfer lists in raw_json."""
        transfers = []
        
        # Check common locations
        candidates = [
            raw_json.get('tokenTransfers'),
            raw_json.get('parsedTokenTransfers'),
            raw_json.get('events', {}).get('tokenTransfers') if isinstance(raw_json.get('events'), dict) else None
        ]
        
        for candidate in candidates:
            if not candidate or not isinstance(candidate, list):
                continue
                
            for transfer in candidate:
                if not isinstance(transfer, dict):
                    continue
                    
                mint = transfer.get('mint')
                amount = transfer.get('amount')
                from_addr = transfer.get('fromTokenAccount') or transfer.get('from') or transfer.get('source')
                to_addr = transfer.get('toTokenAccount') or transfer.get('to') or transfer.get('destination')
                
                if not mint or amount is None:
                    continue
                    
                try:
                    amount_int = int(amount)
                except (ValueError, TypeError):
                    continue
                    
                transfers.append({
                    'mint': mint,
                    'amount': amount_int,
                    'from_addr': from_addr,
                    'to_addr': to_addr,
                })
                
        return transfers
        
    def _extract_transfers_from_balances(self, raw_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract token transfers using balance deltas.
        Uses token account addresses from transaction.message.accountKeys.
        """
        transfers = []
        
        meta = raw_json.get('meta', {})
        if not isinstance(meta, dict):
            return transfers
            
        # Get account keys for address resolution
        account_keys = []
        transaction = raw_json.get('transaction', {})
        if isinstance(transaction, dict):
            message = transaction.get('message', {})
            if isinstance(message, dict):
                account_keys = message.get('accountKeys', [])
                
        if not account_keys:
            return transfers
            
        pre_balances = meta.get('preTokenBalances', [])
        post_balances = meta.get('postTokenBalances', [])
        
        # Build mappings by account index
        pre_map = {}
        post_map = {}
        
        for pre in pre_balances:
            if isinstance(pre, dict):
                idx = pre.get('accountIndex')
                if idx is not None:
                    pre_map[idx] = pre
                    
        for post in post_balances:
            if isinstance(post, dict):
                idx = post.get('accountIndex')
                if idx is not None:
                    post_map[idx] = post
        
        # Compute deltas per token account
        all_indices = set(pre_map.keys()) | set(post_map.keys())
        
        # Group by mint to find sender/receiver pairs
        by_mint = defaultdict(lambda: {'senders': [], 'receivers': []})
        
        for idx in all_indices:
            pre = pre_map.get(idx, {})
            post = post_map.get(idx, {})
            
            mint = post.get('mint') or pre.get('mint')
            if not mint:
                continue
                
            # Get token account address
            if idx >= len(account_keys):
                continue
            token_account = account_keys[idx]
            
            # Extract amounts in base units
            pre_amount_str = None
            post_amount_str = None
            
            if 'uiTokenAmount' in pre and isinstance(pre['uiTokenAmount'], dict):
                pre_amount_str = pre['uiTokenAmount'].get('amount')
            if 'uiTokenAmount' in post and isinstance(post['uiTokenAmount'], dict):
                post_amount_str = post['uiTokenAmount'].get('amount')
                
            try:
                pre_amt = int(pre_amount_str) if pre_amount_str else 0
                post_amt = int(post_amount_str) if post_amount_str else 0
            except (ValueError, TypeError):
                continue
                
            delta = post_amt - pre_amt
            
            if delta > 0:
                by_mint[mint]['receivers'].append({
                    'token_account': token_account,
                    'amount': delta
                })
            elif delta < 0:
                by_mint[mint]['senders'].append({
                    'token_account': token_account,
                    'amount': abs(delta)
                })
        
        # Pair senders and receivers deterministically
        for mint, groups in by_mint.items():
            senders = groups['senders']
            receivers = groups['receivers']
            
            # Only create transfers when pairing is unambiguous
            if len(senders) == 1 and len(receivers) == 1:
                sender = senders[0]
                receiver = receivers[0]
                
                if sender['amount'] == receiver['amount']:
                    transfers.append({
                        'mint': mint,
                        'amount': sender['amount'],
                        'from_addr': sender['token_account'],
                        'to_addr': receiver['token_account'],
                    })
            # Multiple senders/receivers = ambiguous, produce no transfer
            # The match_transfer will fail with appropriate reason
            
        return transfers
        
    def match_transfer(
        self,
        row_mint: str,
        row_amount: int,
        transfers: List[Dict[str, Any]]
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        """
        Match a row to a transfer from raw_json by mint and amount only.
        No direction-based disambiguation.
        Returns (matched_transfer, reason)
        """
        if not transfers:
            return None, "no_transfers_in_json"
            
        # Find matches by mint and amount (exact match only)
        candidates = []
        for t in transfers:
            if t['mint'] != row_mint:
                continue
            if t['amount'] != row_amount:
                continue
            candidates.append(t)
            
        if len(candidates) == 0:
            return None, "no_matching_mint_amount"
            
        if len(candidates) > 1:
            return None, "ambiguous_multiple_matches"
                
        # Single candidate - this is our match
        return candidates[0], "ok"
        
    def lookup_time_from_tx(self, signature: str) -> Tuple[Optional[int], Optional[int]]:
        """
        Look up block_time and slot from tx table by signature.
        Returns (block_time, slot).
        """
        if not self.tx_signature_col:
            return None, None
            
        cursor = self.conn.cursor()
        
        # Build SELECT
        select_cols = []
        if self.tx_block_time_col:
            select_cols.append(self.tx_block_time_col)
        if self.tx_slot_col:
            select_cols.append(self.tx_slot_col)
            
        if not select_cols:
            return None, None
            
        query = f"SELECT {', '.join(select_cols)} FROM tx WHERE {self.tx_signature_col} = ?"
        cursor.execute(query, (signature,))
        row = cursor.fetchone()
        
        if not row:
            return None, None
            
        block_time = None
        slot = None
        
        if self.tx_block_time_col:
            block_time = row[self.tx_block_time_col]
            if block_time is not None:
                try:
                    block_time = int(block_time)
                except (ValueError, TypeError):
                    block_time = None
                    
        if self.tx_slot_col:
            slot = row[self.tx_slot_col]
            if slot is not None:
                try:
                    slot = int(slot)
                except (ValueError, TypeError):
                    slot = None
                    
        return block_time, slot
        
    def backfill_row(self, row: sqlite3.Row) -> Tuple[Dict[str, Any], str]:
        """
        Backfill a single row. Returns (update_dict, status_reason).
        Row is OK only if: (block_time OR slot present) AND (from_addr not null) AND (to_addr not null)
        """
        # Start with existing values
        block_time = row.get('block_time')
        slot = row.get('slot')
        from_addr = row.get('from_addr')
        to_addr = row.get('to_addr')
        
        # Parse raw_json
        raw_json = self.parse_raw_json(row['raw_json'])
        
        if raw_json is None:
            return {
                'block_time': block_time,
                'slot': slot,
                'from_addr': from_addr,
                'to_addr': to_addr,
                'backfill_status': 'fail',
                'backfill_reason': 'invalid_json'
            }, 'fail'
        
        # Backfill time if not already present
        if block_time is None and slot is None:
            # Try tx table first
            tx_block_time, tx_slot = self.lookup_time_from_tx(row['signature'])
            
            if tx_block_time is not None or tx_slot is not None:
                block_time = tx_block_time
                slot = tx_slot
            else:
                # Try raw_json
                json_block_time, json_slot = self.extract_time_from_raw_json(raw_json)
                block_time = json_block_time
                slot = json_slot
        
        # Check if we have time anchor
        time_ok = (block_time is not None or slot is not None)
        
        # Backfill endpoints if not already present
        if from_addr is None or to_addr is None:
            transfers = self.extract_token_transfers(raw_json)
            matched, reason = self.match_transfer(
                row['mint'],
                row['amount'],
                transfers
            )
            
            if matched is not None:
                from_addr = matched['from_addr']
                to_addr = matched['to_addr']
            else:
                # Failed to match transfer
                return {
                    'block_time': block_time,
                    'slot': slot,
                    'from_addr': from_addr,
                    'to_addr': to_addr,
                    'backfill_status': 'fail',
                    'backfill_reason': reason
                }, 'fail'
        
        # Determine final status
        endpoints_ok = (from_addr is not None and to_addr is not None)
        
        if not time_ok:
            return {
                'block_time': block_time,
                'slot': slot,
                'from_addr': from_addr,
                'to_addr': to_addr,
                'backfill_status': 'fail',
                'backfill_reason': 'missing_time_anchor'
            }, 'fail'
            
        if not endpoints_ok:
            return {
                'block_time': block_time,
                'slot': slot,
                'from_addr': from_addr,
                'to_addr': to_addr,
                'backfill_status': 'fail',
                'backfill_reason': 'missing_endpoints'
            }, 'fail'
            
        # All OK
        return {
            'block_time': block_time,
            'slot': slot,
            'from_addr': from_addr,
            'to_addr': to_addr,
            'backfill_status': 'ok',
            'backfill_reason': 'ok'
        }, 'ok'
        
    def backfill(self, mode: str):
        """
        Run the backfill process.
        mode: 'backfill_all' or 'backfill_missing'
        
        CRITICAL: SQLite cursor executing UPDATE invalidates active SELECT;
        we use two cursors to avoid processing only first batch.
        """
        print(f"Starting backfill process (mode={mode})...\n")
        
        # Setup schema
        self.setup_schema()
        
        # Detect tx schema
        self.detect_tx_schema()
        
        # Use two cursors to avoid invalidation
        read_cur = self.conn.cursor()
        write_cur = self.conn.cursor()
        
        # Build WHERE clause based on mode
        where_clause = ""
        if mode == 'backfill_missing':
            where_clause = """
                WHERE (block_time IS NULL OR slot IS NULL 
                       OR from_addr IS NULL OR to_addr IS NULL)
            """
        
        # Get total count
        count_query = f"SELECT COUNT(*) as cnt FROM spl_transfers {where_clause}"
        read_cur.execute(count_query)
        total = read_cur.fetchone()[0]
        print(f"Total rows to process: {total}\n")
        
        if total == 0:
            print("No rows to process.\n")
            return
        
        # Process in batches
        batch_size = 1000
        processed = 0
        
        # Select rows to process
        select_query = f"""
            SELECT rowid AS rid, signature, mint, amount, raw_json,
                   block_time, slot, from_addr, to_addr
            FROM spl_transfers
            {where_clause}
        """
        
        read_cur.execute(select_query)
        
        while True:
            rows = read_cur.fetchmany(batch_size)
            if not rows:
                break
                
            updates = []
            for row in rows:
                update_data, status = self.backfill_row(row)
                updates.append((
                    update_data.get('block_time'),
                    update_data.get('slot'),
                    update_data.get('from_addr'),
                    update_data.get('to_addr'),
                    update_data.get('backfill_status'),
                    update_data.get('backfill_reason'),
                    row['rid']
                ))
                
            # Batch update using separate cursor
            write_cur.executemany("""
                UPDATE spl_transfers
                SET block_time = ?,
                    slot = ?,
                    from_addr = ?,
                    to_addr = ?,
                    backfill_status = ?,
                    backfill_reason = ?
                WHERE rowid = ?
            """, updates)
            
            processed += len(rows)
            if processed % 5000 == 0:
                print(f"Processed {processed}/{total} rows...")
                self.conn.commit()
                
        self.conn.commit()
        print(f"\nBackfill complete. Processed {processed} rows.\n")
        
        # Run internal checks
        self.run_internal_checks()
        
    def run_internal_checks(self):
        """Run SQL checks on backfilled data."""
        print("=" * 60)
        print("INTERNAL DATA QUALITY CHECKS")
        print("=" * 60)
        
        cursor = self.conn.cursor()
        
        # Total rows
        cursor.execute("SELECT COUNT(*) as cnt FROM spl_transfers")
        total = cursor.fetchone()[0]
        
        # Null counts
        checks = [
            ('block_time', 'block_time IS NULL'),
            ('slot', 'slot IS NULL'),
            ('time_anchor (both)', 'block_time IS NULL AND slot IS NULL'),
            ('from_addr', 'from_addr IS NULL'),
            ('to_addr', 'to_addr IS NULL'),
        ]
        
        print(f"\nTotal rows: {total}")
        print("\nNull percentages:")
        for field, condition in checks:
            cursor.execute(f"SELECT COUNT(*) as cnt FROM spl_transfers WHERE {condition}")
            null_count = cursor.fetchone()[0]
            pct = (null_count / total * 100) if total > 0 else 0
            print(f"  {field:20s}: {null_count:8d} ({pct:6.2f}%)")
            
        # Status breakdown
        print("\nBackfill status breakdown:")
        cursor.execute("""
            SELECT backfill_status, COUNT(*) as cnt
            FROM spl_transfers
            GROUP BY backfill_status
            ORDER BY cnt DESC
        """)
        for row in cursor.fetchall():
            pct = (row['cnt'] / total * 100) if total > 0 else 0
            print(f"  {row['backfill_status'] or 'NULL':15s}: {row['cnt']:8d} ({pct:6.2f}%)")
            
        print()
        
    def report(self):
        """Generate comprehensive report."""
        print("=" * 60)
        print("BACKFILL REPORT")
        print("=" * 60)
        
        cursor = self.conn.cursor()
        
        # Get existing columns
        cursor.execute("PRAGMA table_info(spl_transfers)")
        cols = {row[1] for row in cursor.fetchall()}
        
        # Total rows
        cursor.execute("SELECT COUNT(*) as cnt FROM spl_transfers")
        total = cursor.fetchone()[0]
        print(f"\nTotal rows: {total}")
        
        if total == 0:
            print("No data to report.")
            return
            
        # Build time_ok condition
        time_condition = None
        time_label = ""
        if 'block_time' in cols and 'slot' in cols:
            time_condition = "(block_time IS NOT NULL OR slot IS NOT NULL)"
            time_label = "block_time OR slot"
        elif 'block_time' in cols:
            time_condition = "block_time IS NOT NULL"
            time_label = "block_time"
        elif 'slot' in cols:
            time_condition = "slot IS NOT NULL"
            time_label = "slot"
        
        # Rows OK for time
        if time_condition:
            cursor.execute(f"SELECT COUNT(*) as cnt FROM spl_transfers WHERE {time_condition}")
            ok_time = cursor.fetchone()[0]
            pct_time = (ok_time / total * 100) if total > 0 else 0
        else:
            ok_time = 0
            pct_time = 0.0
            print("\nWARNING: Time columns missing (block_time/slot) - cannot compute time OK count")
        
        # Rows OK for from/to (both non-null)
        if 'from_addr' in cols and 'to_addr' in cols:
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM spl_transfers 
                WHERE from_addr IS NOT NULL AND to_addr IS NOT NULL
            """)
            ok_endpoints = cursor.fetchone()[0]
            pct_endpoints = (ok_endpoints / total * 100) if total > 0 else 0
        else:
            ok_endpoints = 0
            pct_endpoints = 0.0
            missing = []
            if 'from_addr' not in cols:
                missing.append('from_addr')
            if 'to_addr' not in cols:
                missing.append('to_addr')
            print(f"\nWARNING: Endpoint columns missing ({', '.join(missing)}) - cannot compute endpoints OK count")
        
        # Rows OK for all three (time + both endpoints)
        if time_condition and 'from_addr' in cols and 'to_addr' in cols:
            cursor.execute(f"""
                SELECT COUNT(*) as cnt FROM spl_transfers 
                WHERE {time_condition}
                AND from_addr IS NOT NULL 
                AND to_addr IS NOT NULL
            """)
            ok_all = cursor.fetchone()[0]
            pct_all = (ok_all / total * 100) if total > 0 else 0
        else:
            ok_all = 0
            pct_all = 0.0
            if not time_condition:
                print("\nWARNING: Cannot compute all_ok - time columns missing")
            elif 'from_addr' not in cols or 'to_addr' not in cols:
                print("\nWARNING: Cannot compute all_ok - endpoint columns missing")
        
        if time_condition:
            print(f"\nRows OK for time ({time_label}): {ok_time} ({pct_time:.2f}%)")
        else:
            print(f"\nRows OK for time: N/A (columns not present)")
            
        if 'from_addr' in cols and 'to_addr' in cols:
            print(f"Rows OK for from/to (both non-NULL): {ok_endpoints} ({pct_endpoints:.2f}%)")
        else:
            print(f"Rows OK for from/to: N/A (columns not present)")
            
        if time_condition and 'from_addr' in cols and 'to_addr' in cols:
            print(f"Rows OK for all three (time + from + to): {ok_all} ({pct_all:.2f}%)")
        else:
            print(f"Rows OK for all three: N/A (required columns not present)")
            
        print(f"\npct_all_ok: {pct_all:.2f}%")
        
        # Top 20 backfill_reason counts
        print("\n" + "=" * 60)
        print("TOP 20 BACKFILL REASONS")
        print("=" * 60)
        
        if 'backfill_reason' in cols:
            cursor.execute("""
                SELECT backfill_reason, COUNT(*) as cnt
                FROM spl_transfers
                GROUP BY backfill_reason
                ORDER BY cnt DESC
                LIMIT 20
            """)
            
            reason_rows = cursor.fetchall()
            if reason_rows:
                for row in reason_rows:
                    reason = row['backfill_reason'] or 'NULL'
                    cnt = row['cnt']
                    pct = (cnt / total * 100) if total > 0 else 0
                    print(f"  {reason:40s}: {cnt:8d} ({pct:6.2f}%)")
            else:
                print("  No backfill_reason data found.")
        else:
            print("  WARNING: backfill_reason column does not exist - skipping")
            
        # Sample 10 failed signatures
        print("\n" + "=" * 60)
        print("SAMPLE 10 FAILED ROWS (backfill_status = 'fail')")
        print("=" * 60)
        
        if 'backfill_status' in cols and 'backfill_reason' in cols:
            # Check if signature column exists
            if 'signature' in cols:
                cursor.execute("""
                    SELECT signature, backfill_reason
                    FROM spl_transfers
                    WHERE backfill_status = 'fail'
                    LIMIT 10
                """)
                
                failed_rows = cursor.fetchall()
                if failed_rows:
                    for row in failed_rows:
                        sig = row['signature'][:20] + '...' if len(row['signature']) > 20 else row['signature']
                        reason = row['backfill_reason'] or 'NULL'
                        print(f"  {sig:25s} -> {reason}")
                else:
                    print("  No failed rows found.")
            else:
                print("  WARNING: signature column does not exist - cannot show samples")
        elif 'backfill_status' not in cols:
            print("  WARNING: backfill_status column does not exist - skipping")
        elif 'backfill_reason' not in cols:
            print("  WARNING: backfill_reason column does not exist - skipping")
            
        print("\n" + "=" * 60)
        print("END REPORT")
        print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description='Backfill spl_transfers with data from tx table and raw_json'
    )
    parser.add_argument(
        '--db',
        required=True,
        help='Path to SQLite database'
    )
    parser.add_argument(
        '--mode',
        required=True,
        choices=['backfill', 'backfill_all', 'backfill_missing', 'report'],
        help='Mode: backfill/backfill_all (process all), backfill_missing (process incomplete), or report'
    )
    
    args = parser.parse_args()
    
    # Map 'backfill' to 'backfill_all' for backwards compatibility
    mode = args.mode
    if mode == 'backfill':
        mode = 'backfill_all'
    
    with SPLTransferBackfiller(args.db) as backfiller:
        if mode in ['backfill_all', 'backfill_missing']:
            backfiller.backfill(mode)
        elif mode == 'report':
            backfiller.report()


if __name__ == '__main__':
    main()
