#!/usr/bin/env python3
"""
panda_phase3_2_accept_whale_v2.py

PANDA v4 Phase 3.2 Acceptance Test (v2)
READ-ONLY validation of whale_events and whale_states tables
"""

import sqlite3
import argparse
import sys
from collections import defaultdict, deque
from typing import Dict, Set, Tuple, Optional, List, Any

# Default thresholds (must match builder)
T_TX_LAMPORTS = 10_000_000_000
T_CUM_24H_LAMPORTS = 50_000_000_000
T_CUM_7D_LAMPORTS = 200_000_000_000

ALLOWED_WINDOWS = {"24h", "7d", "lifetime"}
ALLOWED_EVENT_TYPES = {
    "WHALE_TX_BUY",
    "WHALE_TX_SELL",
    "WHALE_CUM_24H_BUY",
    "WHALE_CUM_24H_SELL",
    "WHALE_CUM_7D_BUY",
    "WHALE_CUM_7D_SELL",
}

WINDOW_SECONDS = {
    "24h": 86400,
    "7d": 604800,
}


def discover_schema(conn: sqlite3.Connection, table: str) -> Optional[Dict[str, str]]:
    """Discover column names from wallet_token_flow using candidate mappings."""
    cur = conn.cursor()
    
    # Check if table exists first
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    if not cur.fetchone():
        return None
    
    cur.execute(f"PRAGMA table_info({table})")
    rows = cur.fetchall()
    
    if not rows:
        return None
    
    cols = {row[1].lower(): row[1] for row in rows}  # name_lower -> actual_name
    
    wallet_candidates = ["wallet", "wallet_address", "scan_wallet"]
    time_candidates = ["event_time", "block_time", "flow_time", "timestamp"]
    dir_candidates = ["sol_direction", "direction"]
    amt_candidates = ["sol_amount_lamports", "amount_lamports", "lamports", "sol_lamports"]
    flow_ref_candidates = ["flow_ref", "signature", "flow_id", "hash", "tx_signature"]
    
    def find_col(candidates: List[str]) -> Optional[str]:
        for c in candidates:
            if c.lower() in cols:
                return cols[c.lower()]
        return None
    
    wallet_col = find_col(wallet_candidates)
    time_col = find_col(time_candidates)
    dir_col = find_col(dir_candidates)
    amt_col = find_col(amt_candidates)
    flow_ref_col = find_col(flow_ref_candidates)
    
    if not all([wallet_col, time_col, dir_col, amt_col]):
        return None
    
    return {
        "wallet": wallet_col,
        "time": time_col,
        "direction": dir_col,
        "amount": amt_col,
        "flow_ref": flow_ref_col,  # May be None
    }


def normalize_direction(val: str) -> Optional[str]:
    """Normalize direction values to 'buy' or 'sell'."""
    if not val:
        return None
    v = val.lower().strip()
    if v in {"buy", "in", "receive", "received"}:
        return "buy"
    elif v in {"sell", "out", "sent", "send"}:
        return "sell"
    return None


def block_a1_schema_sanity(conn: sqlite3.Connection) -> bool:
    """A1 - Schema & Column Sanity"""
    print("\n=== A1: Schema & Column Sanity ===")
    
    cur = conn.cursor()
    
    # Check table existence
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('whale_events', 'whale_states')")
    tables = {row[0] for row in cur.fetchall()}
    
    if "whale_events" not in tables:
        print("FAIL: whale_events table not found")
        return False
    if "whale_states" not in tables:
        print("FAIL: whale_states table not found")
        return False
    
    # Expected schemas (name, type)
    expected_whale_events = [
        ("wallet", "TEXT"),
        ("window", "TEXT"),
        ("event_time", "INTEGER"),
        ("event_type", "TEXT"),
        ("sol_amount_lamports", "INTEGER"),
        ("supporting_flow_count", "INTEGER"),
        ("flow_ref", "TEXT"),
        ("created_at", "INTEGER"),
    ]
    
    expected_whale_states = [
        ("wallet", "TEXT"),
        ("window", "TEXT"),
        ("whale_tx_buy_count", "INTEGER"),
        ("whale_tx_sell_count", "INTEGER"),
        ("whale_tx_buy_max_lamports", "INTEGER"),
        ("whale_tx_sell_max_lamports", "INTEGER"),
        ("whale_cum_buy_total_lamports", "INTEGER"),
        ("whale_cum_sell_total_lamports", "INTEGER"),
        ("first_whale_time", "INTEGER"),
        ("last_whale_time", "INTEGER"),
        ("created_at", "INTEGER"),
    ]
    
    # Verify whale_events
    cur.execute("PRAGMA table_info(whale_events)")
    actual_events = [(row[1], row[2]) for row in cur.fetchall()]
    
    if actual_events != expected_whale_events:
        print(f"FAIL: whale_events schema mismatch")
        print(f"Expected: {expected_whale_events}")
        print(f"Actual: {actual_events}")
        return False
    
    # Verify whale_states
    cur.execute("PRAGMA table_info(whale_states)")
    actual_states = [(row[1], row[2]) for row in cur.fetchall()]
    
    if actual_states != expected_whale_states:
        print(f"FAIL: whale_states schema mismatch")
        print(f"Expected: {expected_whale_states}")
        print(f"Actual: {actual_states}")
        return False
    
    print("PASS: All schemas match exactly")
    return True


def block_a2_domain_validity(conn: sqlite3.Connection) -> bool:
    """A2 - Domain & Value Validity"""
    print("\n=== A2: Domain & Value Validity ===")
    
    cur = conn.cursor()
    failures = []
    
    # Check whale_events
    cur.execute("SELECT COUNT(*) FROM whale_events WHERE window NOT IN ('24h', '7d', 'lifetime')")
    if cur.fetchone()[0] > 0:
        failures.append("whale_events: invalid window values found")
    
    # Build dynamic SQL for event_type check to avoid tuple issues
    event_type_list = ', '.join([f"'{et}'" for et in ALLOWED_EVENT_TYPES])
    cur.execute(f"SELECT COUNT(*) FROM whale_events WHERE event_type NOT IN ({event_type_list})")
    if cur.fetchone()[0] > 0:
        failures.append("whale_events: invalid event_type values found")
    
    # Check NULLs in whale_events required fields
    required_fields = ["wallet", "window", "event_time", "event_type", "sol_amount_lamports", "supporting_flow_count", "created_at"]
    for field in required_fields:
        cur.execute(f"SELECT COUNT(*) FROM whale_events WHERE {field} IS NULL")
        if cur.fetchone()[0] > 0:
            failures.append(f"whale_events: NULL found in required field {field}")
    
    # Check non-negative amounts
    cur.execute("SELECT COUNT(*) FROM whale_events WHERE sol_amount_lamports < 0")
    if cur.fetchone()[0] > 0:
        failures.append("whale_events: negative sol_amount_lamports found")
    
    # Check supporting_flow_count >= 1
    cur.execute("SELECT COUNT(*) FROM whale_events WHERE supporting_flow_count < 1")
    if cur.fetchone()[0] > 0:
        failures.append("whale_events: supporting_flow_count < 1 found")
    
    # Check whale_states
    cur.execute("SELECT COUNT(*) FROM whale_states WHERE window NOT IN ('24h', '7d', 'lifetime')")
    if cur.fetchone()[0] > 0:
        failures.append("whale_states: invalid window values found")
    
    # Check NULLs in whale_states (all except first_whale_time, last_whale_time)
    state_required = ["wallet", "window", "whale_tx_buy_count", "whale_tx_sell_count",
                      "whale_tx_buy_max_lamports", "whale_tx_sell_max_lamports",
                      "whale_cum_buy_total_lamports", "whale_cum_sell_total_lamports", "created_at"]
    for field in state_required:
        cur.execute(f"SELECT COUNT(*) FROM whale_states WHERE {field} IS NULL")
        if cur.fetchone()[0] > 0:
            failures.append(f"whale_states: NULL found in required field {field}")
    
    if failures:
        print("FAIL:")
        for f in failures:
            print(f"  - {f}")
        return False
    
    print("PASS: All domain and value validations passed")
    return True


def block_a3_keys_cardinality(conn: sqlite3.Connection) -> bool:
    """A3 - Keys & Cardinality"""
    print("\n=== A3: Keys & Cardinality ===")
    
    cur = conn.cursor()
    failures = []
    
    # Check whale_events logical PK uniqueness
    cur.execute("""
        SELECT wallet, window, event_type, event_time, flow_ref, COUNT(*)
        FROM whale_events
        GROUP BY wallet, window, event_type, event_time, flow_ref
        HAVING COUNT(*) > 1
    """)
    dupes = cur.fetchall()
    if dupes:
        failures.append(f"whale_events: {len(dupes)} duplicate keys found (first 20):")
        for dup in dupes[:20]:
            failures.append(f"  {dup}")
    
    # Check whale_states PK uniqueness
    cur.execute("""
        SELECT wallet, window, COUNT(*)
        FROM whale_states
        GROUP BY wallet, window
        HAVING COUNT(*) > 1
    """)
    dupes = cur.fetchall()
    if dupes:
        failures.append(f"whale_states: {len(dupes)} duplicate keys found (first 20):")
        for dup in dupes[:20]:
            failures.append(f"  {dup}")
    
    if failures:
        print("FAIL:")
        for f in failures:
            print(f"  {f}")
        return False
    
    print("PASS: No duplicate keys found")
    return True


def block_a4_parity_events(conn: sqlite3.Connection, t_tx: int, t_cum_24h: int, t_cum_7d: int) -> bool:
    """A4 - Parity: recompute expected whale_events from wallet_token_flow"""
    print("\n=== A4: Event Parity (wallet_token_flow -> whale_events) ===")
    
    # Discover schema
    schema = discover_schema(conn, "wallet_token_flow")
    if not schema:
        print("FAIL: Cannot discover wallet_token_flow schema or table does not exist")
        return False
    
    print(f"Discovered schema: wallet={schema['wallet']}, time={schema['time']}, "
          f"direction={schema['direction']}, amount={schema['amount']}, flow_ref={schema['flow_ref']}")
    
    # Load wallet_token_flow
    cur = conn.cursor()
    flow_ref_col = schema["flow_ref"]
    
    if flow_ref_col:
        query = f"""
            SELECT {schema['wallet']}, {schema['time']}, {schema['direction']}, 
                   {schema['amount']}, {flow_ref_col}, rowid
            FROM wallet_token_flow
            ORDER BY {schema['time']}, rowid
        """
    else:
        query = f"""
            SELECT {schema['wallet']}, {schema['time']}, {schema['direction']}, 
                   {schema['amount']}, NULL, rowid
            FROM wallet_token_flow
            ORDER BY {schema['time']}, rowid
        """
    
    cur.execute(query)
    flows = cur.fetchall()
    
    print(f"Processing {len(flows)} flows from wallet_token_flow...")
    
    # Recompute expected events
    expected_events = {}  # key -> (sol_amount, supporting_flow_count)
    
    # Rolling state: (wallet, direction, window_key) -> deque[(time, amount)]
    rolling_state = defaultdict(lambda: {"deque": deque(), "total": 0})
    
    for flow in flows:
        wallet, time, direction, amount, flow_ref, rowid = flow
        
        # Handle flow_ref
        if flow_ref is None:
            flow_ref = f"rowid:{rowid}"
        
        direction = normalize_direction(direction)
        if not direction:
            continue
        
        # Handle NULL or invalid amounts
        if amount is None:
            continue
        
        amount = abs(int(amount))
        time = int(time)
        
        # Single-tx events
        if amount >= t_tx:
            event_type = "WHALE_TX_BUY" if direction == "buy" else "WHALE_TX_SELL"
            key = (wallet, "lifetime", event_type, time, str(flow_ref))
            expected_events[key] = (amount, 1)
        
        # Rolling cumulative events for 24h and 7d
        for window_key, window_secs in [("24h", 86400), ("7d", 604800)]:
            state_key = (wallet, direction, window_key)
            state = rolling_state[state_key]
            
            # Expire old entries
            while state["deque"] and (time - state["deque"][0][0]) > window_secs:
                old_time, old_amount = state["deque"].popleft()
                state["total"] -= old_amount
            
            # Add current flow
            state["deque"].append((time, amount))
            state["total"] += amount
            
            # Check threshold
            threshold = t_cum_24h if window_key == "24h" else t_cum_7d
            if state["total"] >= threshold:
                if window_key == "24h":
                    event_type = "WHALE_CUM_24H_BUY" if direction == "buy" else "WHALE_CUM_24H_SELL"
                else:
                    event_type = "WHALE_CUM_7D_BUY" if direction == "buy" else "WHALE_CUM_7D_SELL"
                
                key = (wallet, window_key, event_type, time, str(flow_ref))
                expected_events[key] = (state["total"], len(state["deque"]))
    
    print(f"Expected {len(expected_events)} whale events")
    
    # Load actual events
    cur.execute("""
        SELECT wallet, window, event_type, event_time, flow_ref, 
               sol_amount_lamports, supporting_flow_count
        FROM whale_events
    """)
    actual_events = {}
    for row in cur.fetchall():
        key = (row[0], row[1], row[2], row[3], str(row[4]))  # Ensure flow_ref is string
        actual_events[key] = (row[5], row[6])
    
    print(f"Actual {len(actual_events)} whale events in DB")
    
    # Compare
    expected_keys = set(expected_events.keys())
    actual_keys = set(actual_events.keys())
    
    phantom = actual_keys - expected_keys
    missing = expected_keys - actual_keys
    
    amount_mismatches = []
    count_mismatches = []
    
    for key in expected_keys & actual_keys:
        exp_amt, exp_cnt = expected_events[key]
        act_amt, act_cnt = actual_events[key]
        
        if exp_amt != act_amt:
            amount_mismatches.append((key, exp_amt, act_amt))
        if exp_cnt != act_cnt:
            count_mismatches.append((key, exp_cnt, act_cnt))
    
    failures = []
    
    if phantom:
        failures.append(f"Phantom events (in DB, not expected): {len(phantom)} (first 20):")
        for key in list(phantom)[:20]:
            failures.append(f"  {key} -> {actual_events[key]}")
    
    if missing:
        failures.append(f"Missing events (expected, not in DB): {len(missing)} (first 20):")
        for key in list(missing)[:20]:
            failures.append(f"  {key} -> {expected_events[key]}")
    
    if amount_mismatches:
        failures.append(f"Amount mismatches: {len(amount_mismatches)} (first 20):")
        for key, exp, act in amount_mismatches[:20]:
            failures.append(f"  {key}: expected {exp}, actual {act}")
    
    if count_mismatches:
        failures.append(f"Count mismatches: {len(count_mismatches)} (first 20):")
        for key, exp, act in count_mismatches[:20]:
            failures.append(f"  {key}: expected {exp}, actual {act}")
    
    if failures:
        print("FAIL:")
        for f in failures:
            print(f"{f}")
        return False
    
    print("PASS: All events match expected")
    return True


def block_a5_state_parity(conn: sqlite3.Connection) -> bool:
    """A5 - State Parity vs whale_events"""
    print("\n=== A5: State Parity (whale_events -> whale_states) ===")
    
    cur = conn.cursor()
    
    # Compute expected states from whale_events
    cur.execute("""
        SELECT wallet, window, event_type, event_time, sol_amount_lamports
        FROM whale_events
        ORDER BY wallet, window, event_time
    """)
    
    expected_states = {}
    
    for row in cur.fetchall():
        wallet, window, event_type, event_time, amount = row
        key = (wallet, window)
        
        if key not in expected_states:
            expected_states[key] = {
                "whale_tx_buy_count": 0,
                "whale_tx_sell_count": 0,
                "whale_tx_buy_max_lamports": 0,
                "whale_tx_sell_max_lamports": 0,
                "whale_cum_buy_total_lamports": 0,
                "whale_cum_sell_total_lamports": 0,
                "first_whale_time": event_time,
                "last_whale_time": event_time,
            }
        
        state = expected_states[key]
        
        if event_type == "WHALE_TX_BUY":
            state["whale_tx_buy_count"] += 1
            state["whale_tx_buy_max_lamports"] = max(state["whale_tx_buy_max_lamports"], amount)
        elif event_type == "WHALE_TX_SELL":
            state["whale_tx_sell_count"] += 1
            state["whale_tx_sell_max_lamports"] = max(state["whale_tx_sell_max_lamports"], amount)
        elif event_type in {"WHALE_CUM_24H_BUY", "WHALE_CUM_7D_BUY"}:
            state["whale_cum_buy_total_lamports"] += amount
        elif event_type in {"WHALE_CUM_24H_SELL", "WHALE_CUM_7D_SELL"}:
            state["whale_cum_sell_total_lamports"] += amount
        
        state["first_whale_time"] = min(state["first_whale_time"], event_time)
        state["last_whale_time"] = max(state["last_whale_time"], event_time)
    
    # Load actual states
    cur.execute("""
        SELECT wallet, window, whale_tx_buy_count, whale_tx_sell_count,
               whale_tx_buy_max_lamports, whale_tx_sell_max_lamports,
               whale_cum_buy_total_lamports, whale_cum_sell_total_lamports,
               first_whale_time, last_whale_time
        FROM whale_states
    """)
    
    actual_states = {}
    for row in cur.fetchall():
        key = (row[0], row[1])
        actual_states[key] = {
            "whale_tx_buy_count": row[2],
            "whale_tx_sell_count": row[3],
            "whale_tx_buy_max_lamports": row[4],
            "whale_tx_sell_max_lamports": row[5],
            "whale_cum_buy_total_lamports": row[6],
            "whale_cum_sell_total_lamports": row[7],
            "first_whale_time": row[8],
            "last_whale_time": row[9],
        }
    
    # Compare
    mismatches = []
    
    for key in expected_states:
        if key not in actual_states:
            mismatches.append(f"Missing state for {key}")
            continue
        
        exp = expected_states[key]
        act = actual_states[key]
        
        for field in exp:
            if exp[field] != act[field]:
                mismatches.append(f"{key} {field}: expected {exp[field]}, actual {act[field]}")
    
    # Check for extra states (should be all-zero with NULL times)
    for key in actual_states:
        if key not in expected_states:
            act = actual_states[key]
            is_empty = (
                act["whale_tx_buy_count"] == 0 and
                act["whale_tx_sell_count"] == 0 and
                act["whale_tx_buy_max_lamports"] == 0 and
                act["whale_tx_sell_max_lamports"] == 0 and
                act["whale_cum_buy_total_lamports"] == 0 and
                act["whale_cum_sell_total_lamports"] == 0 and
                act["first_whale_time"] is None and
                act["last_whale_time"] is None
            )
            if not is_empty:
                mismatches.append(f"Extra non-empty state for {key}: {act}")
    
    if mismatches:
        print(f"FAIL: {len(mismatches)} mismatches (first 20):")
        for m in mismatches[:20]:
            print(f"  {m}")
        return False
    
    print("PASS: All states match expected")
    return True


def block_a6_window_boundaries(conn: sqlite3.Connection) -> bool:
    """A6 - Window Boundary Integrity"""
    print("\n=== A6: Window Boundary Integrity ===")
    
    cur = conn.cursor()
    
    # Determine as_of_ts
    schema = discover_schema(conn, "wallet_token_flow")
    as_of_ts = None
    
    if schema:
        try:
            cur.execute(f"SELECT MAX({schema['time']}) FROM wallet_token_flow")
            row = cur.fetchone()
            if row and row[0] is not None:
                as_of_ts = row[0]
        except Exception as e:
            print(f"Warning: Could not get MAX time from wallet_token_flow: {e}")
    
    if as_of_ts is None:
        cur.execute("SELECT MAX(event_time) FROM whale_events")
        row = cur.fetchone()
        if row and row[0] is not None:
            as_of_ts = row[0]
    
    if as_of_ts is None:
        print("PASS: No data to check boundaries")
        return True
    
    print(f"as_of_ts: {as_of_ts}")
    
    violations = []
    
    # Check 24h window
    cur.execute("""
        SELECT wallet, event_time, event_type
        FROM whale_events
        WHERE window = '24h' AND (event_time < ? OR event_time > ?)
    """, (as_of_ts - 86400, as_of_ts))
    
    rows_24h = cur.fetchall()
    for row in rows_24h[:20]:
        violations.append(f"24h window violation: wallet={row[0]}, event_time={row[1]}, type={row[2]}")
    
    # Check 7d window
    cur.execute("""
        SELECT wallet, event_time, event_type
        FROM whale_events
        WHERE window = '7d' AND (event_time < ? OR event_time > ?)
    """, (as_of_ts - 604800, as_of_ts))
    
    rows_7d = cur.fetchall()
    for row in rows_7d[:20]:
        violations.append(f"7d window violation: wallet={row[0]}, event_time={row[1]}, type={row[2]}")
    
    if violations:
        print(f"FAIL: {len(violations)} boundary violations (first 20):")
        for v in violations:
            print(f"  {v}")
        return False
    
    print("PASS: All window boundaries valid")
    return True


def block_a7_determinism(conn: sqlite3.Connection) -> bool:
    """A7 - Determinism (Light)"""
    print("\n=== A7: Determinism ===")
    
    cur = conn.cursor()
    
    def snapshot():
        # whale_events snapshot
        cur.execute("""
            SELECT window, COUNT(*), COALESCE(SUM(sol_amount_lamports), 0)
            FROM whale_events
            GROUP BY window
            ORDER BY window
        """)
        events_snap = cur.fetchall()
        
        # whale_states snapshot
        cur.execute("""
            SELECT window, COUNT(*), 
                   COALESCE(SUM(whale_cum_buy_total_lamports), 0), 
                   COALESCE(SUM(whale_cum_sell_total_lamports), 0)
            FROM whale_states
            GROUP BY window
            ORDER BY window
        """)
        states_snap = cur.fetchall()
        
        return (events_snap, states_snap)
    
    snap1 = snapshot()
    snap2 = snapshot()
    
    if snap1 != snap2:
        print("FAIL: Snapshots differ")
        print(f"Snapshot 1: {snap1}")
        print(f"Snapshot 2: {snap2}")
        return False
    
    print("PASS: Determinism check passed")
    return True


def print_summary(conn: sqlite3.Connection):
    """Print row counts per window"""
    print("\n=== Summary ===")
    
    cur = conn.cursor()
    
    # Check if tables exist first
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('whale_events', 'whale_states')")
    tables = {row[0] for row in cur.fetchall()}
    
    if 'whale_events' not in tables:
        print("\nwhale_events: table not found")
    else:
        print("\nwhale_events row counts by window:")
        cur.execute("SELECT window, COUNT(*) FROM whale_events GROUP BY window ORDER BY window")
        rows = cur.fetchall()
        if rows:
            for row in rows:
                print(f"  {row[0]}: {row[1]}")
        else:
            print("  (no rows)")
        
        cur.execute("SELECT COUNT(*) FROM whale_events")
        total_events = cur.fetchone()[0]
        print(f"  TOTAL: {total_events}")
    
    if 'whale_states' not in tables:
        print("\nwhale_states: table not found")
    else:
        print("\nwhale_states row counts by window:")
        cur.execute("SELECT window, COUNT(*) FROM whale_states GROUP BY window ORDER BY window")
        rows = cur.fetchall()
        if rows:
            for row in rows:
                print(f"  {row[0]}: {row[1]}")
        else:
            print("  (no rows)")
        
        cur.execute("SELECT COUNT(*) FROM whale_states")
        total_states = cur.fetchone()[0]
        print(f"  TOTAL: {total_states}")


def main():
    parser = argparse.ArgumentParser(description="PANDA Phase 3.2 Whale Acceptance Test v2")
    parser.add_argument("--db", required=True, help="Path to database file")
    parser.add_argument("--t-tx", type=int, default=T_TX_LAMPORTS, help="Transaction threshold (lamports)")
    parser.add_argument("--t-cum-24h", type=int, default=T_CUM_24H_LAMPORTS, help="24h cumulative threshold (lamports)")
    parser.add_argument("--t-cum-7d", type=int, default=T_CUM_7D_LAMPORTS, help="7d cumulative threshold (lamports)")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print(f"PANDA Phase 3.2 Whale Acceptance Test v2")
    print("=" * 60)
    print(f"Database: {args.db}")
    print(f"Thresholds:")
    print(f"  t_tx        = {args.t_tx:,} lamports")
    print(f"  t_cum_24h   = {args.t_cum_24h:,} lamports")
    print(f"  t_cum_7d    = {args.t_cum_7d:,} lamports")
    
    try:
        conn = sqlite3.connect(args.db)
    except Exception as e:
        print(f"\nFAIL: Cannot connect to database: {e}")
        sys.exit(1)
    
    try:
        print_summary(conn)
        
        # Run acceptance blocks (fail-fast)
        blocks = [
            ("A1", lambda: block_a1_schema_sanity(conn)),
            ("A2", lambda: block_a2_domain_validity(conn)),
            ("A3", lambda: block_a3_keys_cardinality(conn)),
            ("A4", lambda: block_a4_parity_events(conn, args.t_tx, args.t_cum_24h, args.t_cum_7d)),
            ("A5", lambda: block_a5_state_parity(conn)),
            ("A6", lambda: block_a6_window_boundaries(conn)),
            ("A7", lambda: block_a7_determinism(conn)),
        ]
        
        all_passed = True
        
        for name, block_fn in blocks:
            try:
                if not block_fn():
                    all_passed = False
                    print(f"\n{name}: FAIL - Stopping further tests")
                    break
            except Exception as e:
                print(f"\n{name}: FAIL - Exception occurred: {e}")
                import traceback
                traceback.print_exc()
                all_passed = False
                break
        
        print("\n" + "=" * 60)
        if all_passed:
            print("FINAL VERDICT: PASS")
            print("=" * 60)
            sys.exit(0)
        else:
            print("FINAL VERDICT: FAIL")
            print("=" * 60)
            sys.exit(1)
    
    finally:
        conn.close()


if __name__ == "__main__":
    main()
