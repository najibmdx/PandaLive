#!/usr/bin/env python3
"""
PANDA v4 - Phase 3.2 (Whale States & Events)
"""

import argparse
import sqlite3
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Iterable, List, Optional, Sequence, Tuple

# -------------------------
# Deterministic thresholds
# -------------------------
T_TX_LAMPORTS = 10_000_000_000
T_CUM_24H_LAMPORTS = 50_000_000_000
T_CUM_7D_LAMPORTS = 200_000_000_000

WINDOW_SECS = {
    "24h": 24 * 60 * 60,
    "7d": 7 * 24 * 60 * 60,
}

BUY_VALUES = {"buy", "in", "receive", "received"}
SELL_VALUES = {"sell", "out", "sent", "send"}


@dataclass
class FlowRow:
    wallet: str
    event_time: int
    direction: str
    amount_lamports: int
    flow_ref: str


@dataclass
class WhaleEvent:
    wallet: str
    window: str
    event_time: int
    event_type: str
    sol_amount_lamports: int
    supporting_flow_count: int
    flow_ref: str
    created_at: int


class RollingWindow:
    def __init__(self, window_secs: int, threshold: int) -> None:
        self.window_secs = window_secs
        self.threshold = threshold
        self.entries: Deque[Tuple[int, int]] = deque()
        self.total = 0

    def advance(self, current_time: int) -> None:
        while self.entries and (current_time - self.entries[0][0]) > self.window_secs:
            _, amount = self.entries.popleft()
            self.total -= amount

    def add_and_check(self, current_time: int, amount: int) -> Tuple[bool, int, int]:
        self.advance(current_time)
        self.entries.append((current_time, amount))
        self.total += amount
        sum_after = self.total
        count_after = len(self.entries)
        triggered = sum_after >= self.threshold
        return triggered, sum_after, count_after


def resolve_column(columns: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in columns}
    for cand in candidates:
        col = lower_map.get(cand.lower())
        if col:
            return col
    return None


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table});")
    return [row[1] for row in cur.fetchall()]


def load_flows(conn: sqlite3.Connection) -> Iterable[FlowRow]:
    columns = table_columns(conn, "wallet_token_flow")
    wallet_col = resolve_column(columns, ["wallet", "wallet_address", "scan_wallet"])
    time_col = resolve_column(columns, ["event_time", "block_time", "flow_time", "timestamp"])
    dir_col = resolve_column(columns, ["sol_direction", "direction"])
    amount_col = resolve_column(columns, ["sol_amount_lamports", "amount_lamports", "lamports", "sol_lamports"])
    flow_ref_col = resolve_column(columns, ["flow_ref", "signature", "flow_id", "hash", "tx_signature"])

    missing = [
        name
        for name, value in [
            ("wallet", wallet_col),
            ("event_time", time_col),
            ("direction", dir_col),
            ("sol_amount_lamports", amount_col),
        ]
        if value is None
    ]
    if missing:
        missing_str = ", ".join(missing)
        raise RuntimeError(f"Missing required columns in wallet_token_flow: {missing_str}")

    select_cols = [wallet_col, time_col, dir_col, amount_col]
    if flow_ref_col:
        select_cols.append(flow_ref_col)
        flow_ref_expr = flow_ref_col
    else:
        select_cols.append("rowid")
        flow_ref_expr = "rowid"

    sql = f"""
        SELECT {', '.join(select_cols)}
        FROM wallet_token_flow
        ORDER BY {wallet_col}, {time_col}, {flow_ref_expr}
    """
    cur = conn.execute(sql)

    for row in cur.fetchall():
        wallet = str(row[0])
        event_time_raw = row[1]
        direction_raw = row[2]
        amount_raw = row[3]
        flow_ref_raw = row[4] if len(row) > 4 else None

        if event_time_raw is None or direction_raw is None or amount_raw is None:
            continue

        try:
            event_time = int(event_time_raw)
        except (TypeError, ValueError):
            continue

        try:
            amount = abs(int(amount_raw))
        except (TypeError, ValueError):
            continue

        direction = str(direction_raw).strip().lower()
        if direction in BUY_VALUES:
            direction = "buy"
        elif direction in SELL_VALUES:
            direction = "sell"
        else:
            continue

        if flow_ref_col:
            flow_ref = str(flow_ref_raw) if flow_ref_raw is not None else ""
        else:
            flow_ref = f"rowid:{flow_ref_raw}"

        yield FlowRow(
            wallet=wallet,
            event_time=event_time,
            direction=direction,
            amount_lamports=amount,
            flow_ref=flow_ref,
        )


def build_whale_events(flows: Iterable[FlowRow]) -> List[WhaleEvent]:
    events: List[WhaleEvent] = []

    current_wallet: Optional[str] = None
    windows: Dict[str, Dict[str, RollingWindow]] = {}

    def reset_windows() -> None:
        nonlocal windows
        windows = {
            "buy": {
                "24h": RollingWindow(WINDOW_SECS["24h"], T_CUM_24H_LAMPORTS),
                "7d": RollingWindow(WINDOW_SECS["7d"], T_CUM_7D_LAMPORTS),
            },
            "sell": {
                "24h": RollingWindow(WINDOW_SECS["24h"], T_CUM_24H_LAMPORTS),
                "7d": RollingWindow(WINDOW_SECS["7d"], T_CUM_7D_LAMPORTS),
            },
        }

    for flow in flows:
        if current_wallet != flow.wallet:
            current_wallet = flow.wallet
            reset_windows()

        direction = flow.direction
        amount = flow.amount_lamports

        if amount >= T_TX_LAMPORTS:
            event_type = "WHALE_TX_BUY" if direction == "buy" else "WHALE_TX_SELL"
            events.append(
                WhaleEvent(
                    wallet=flow.wallet,
                    window="lifetime",
                    event_time=flow.event_time,
                    event_type=event_type,
                    sol_amount_lamports=amount,
                    supporting_flow_count=1,
                    flow_ref=flow.flow_ref,
                    created_at=flow.event_time,
                )
            )

        for window_key in ("24h", "7d"):
            rolling = windows[direction][window_key]
            triggered, sum_after, count_after = rolling.add_and_check(flow.event_time, amount)
            if triggered:
                event_type = (
                    "WHALE_CUM_24H_BUY"
                    if window_key == "24h" and direction == "buy"
                    else "WHALE_CUM_24H_SELL"
                    if window_key == "24h"
                    else "WHALE_CUM_7D_BUY"
                    if direction == "buy"
                    else "WHALE_CUM_7D_SELL"
                )
                events.append(
                    WhaleEvent(
                        wallet=flow.wallet,
                        window=window_key,
                        event_time=flow.event_time,
                        event_type=event_type,
                        sol_amount_lamports=sum_after,
                        supporting_flow_count=count_after,
                        flow_ref=flow.flow_ref,
                        created_at=flow.event_time,
                    )
                )

    return events


def ensure_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS whale_events;")
    cur.execute("DROP TABLE IF EXISTS whale_states;")

    cur.execute(
        """
        CREATE TABLE whale_events (
          wallet TEXT NOT NULL,
          window TEXT NOT NULL,
          event_time INTEGER NOT NULL,
          event_type TEXT NOT NULL,
          sol_amount_lamports INTEGER NOT NULL,
          supporting_flow_count INTEGER NOT NULL,
          flow_ref TEXT,
          created_at INTEGER NOT NULL
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE whale_states (
          wallet TEXT NOT NULL,
          window TEXT NOT NULL,
          whale_tx_buy_count INTEGER NOT NULL,
          whale_tx_sell_count INTEGER NOT NULL,
          whale_tx_buy_max_lamports INTEGER NOT NULL,
          whale_tx_sell_max_lamports INTEGER NOT NULL,
          whale_cum_buy_total_lamports INTEGER NOT NULL,
          whale_cum_sell_total_lamports INTEGER NOT NULL,
          first_whale_time INTEGER,
          last_whale_time INTEGER,
          created_at INTEGER NOT NULL,
          PRIMARY KEY (wallet, window)
        );
        """
    )
    conn.commit()


def insert_whale_events(conn: sqlite3.Connection, events: Sequence[WhaleEvent]) -> None:
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO whale_events (
          wallet,
          window,
          event_time,
          event_type,
          sol_amount_lamports,
          supporting_flow_count,
          flow_ref,
          created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [
            (
                ev.wallet,
                ev.window,
                ev.event_time,
                ev.event_type,
                ev.sol_amount_lamports,
                ev.supporting_flow_count,
                ev.flow_ref,
                ev.created_at,
            )
            for ev in events
        ],
    )
    conn.commit()


def insert_whale_states(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        INSERT INTO whale_states (
          wallet,
          window,
          whale_tx_buy_count,
          whale_tx_sell_count,
          whale_tx_buy_max_lamports,
          whale_tx_sell_max_lamports,
          whale_cum_buy_total_lamports,
          whale_cum_sell_total_lamports,
          first_whale_time,
          last_whale_time,
          created_at
        )
        SELECT
          wallet,
          window,
          SUM(CASE WHEN event_type = 'WHALE_TX_BUY' THEN 1 ELSE 0 END) AS whale_tx_buy_count,
          SUM(CASE WHEN event_type = 'WHALE_TX_SELL' THEN 1 ELSE 0 END) AS whale_tx_sell_count,
          MAX(CASE WHEN event_type = 'WHALE_TX_BUY' THEN sol_amount_lamports ELSE 0 END)
            AS whale_tx_buy_max_lamports,
          MAX(CASE WHEN event_type = 'WHALE_TX_SELL' THEN sol_amount_lamports ELSE 0 END)
            AS whale_tx_sell_max_lamports,
          SUM(CASE WHEN event_type IN ('WHALE_CUM_24H_BUY', 'WHALE_CUM_7D_BUY')
            THEN sol_amount_lamports ELSE 0 END) AS whale_cum_buy_total_lamports,
          SUM(CASE WHEN event_type IN ('WHALE_CUM_24H_SELL', 'WHALE_CUM_7D_SELL')
            THEN sol_amount_lamports ELSE 0 END) AS whale_cum_sell_total_lamports,
          MIN(event_time) AS first_whale_time,
          MAX(event_time) AS last_whale_time,
          COALESCE(MAX(event_time), 0) AS created_at
        FROM whale_events
        GROUP BY wallet, window;
        """
    )
    conn.commit()


def print_summary(conn: sqlite3.Connection) -> None:
    cur = conn.execute("SELECT COUNT(*) FROM whale_events;")
    total_events = cur.fetchone()[0]
    cur = conn.execute("SELECT COUNT(*) FROM whale_states;")
    total_states = cur.fetchone()[0]

    print(f"whale_events rows: {total_events}")
    print(f"whale_states rows: {total_states}")

    cur = conn.execute(
        """
        SELECT window, COUNT(*)
        FROM whale_events
        GROUP BY window
        ORDER BY window;
        """
    )
    print("whale_events by window:")
    for window, count in cur.fetchall():
        print(f"  {window}: {count}")

    cur = conn.execute(
        """
        SELECT window, COUNT(*)
        FROM whale_states
        GROUP BY window
        ORDER BY window;
        """
    )
    print("whale_states by window:")
    for window, count in cur.fetchall():
        print(f"  {window}: {count}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build whale events and states.")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        ensure_schema(conn)
        flows = list(load_flows(conn))
        events = build_whale_events(flows)
        insert_whale_events(conn, events)
        insert_whale_states(conn)
        print_summary(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
