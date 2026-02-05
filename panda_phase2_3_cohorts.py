import argparse
import hashlib
import json
import sqlite3
import sys
from collections import defaultdict
from typing import Dict, List, Tuple, Set

"""
Edge Semantics:
- wallet_edges represents DIRECTED transfers from src_wallet to dst_wallet
- For cohort detection, we treat edges as UNDIRECTED (mutual relationship)
- amount_raw flows from src to dst
- tx_count represents transactions in that direction
- When aggregating stats, we track directional flows but build undirected components

Schema Mapping:
cohorts table:
  - cohort_id, mint, scope_kind, window_kind, window_start_ts, window_end_ts
  - member_count, edge_density, internal_flow_raw, external_flow_raw, cohort_score
  - created_at, updated_at

cohort_members table:
  - cohort_id, wallet, role_hint, inflow_raw, outflow_raw, degree_in, degree_out
"""

WINDOW_KINDS = ["lifetime", "24h", "7d"]
COMPONENT = "phase2_3_cohorts"


def stable_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha1_hex(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute Phase 2.3 cohorts.")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--min-members", type=int, default=3)
    parser.add_argument("--hub-topk", type=int, default=200)
    parser.add_argument("--hub-min-mints", type=int, default=3)
    parser.add_argument("--hub-min-amount-raw", type=int, default=1)
    return parser.parse_args()


def get_window_bounds(cur: sqlite3.Cursor, window_kind: str) -> Tuple[int, int]:
    cur.execute(
        """
        SELECT window_start_ts, window_end_ts
        FROM wallet_edges
        WHERE window_kind = ?
        ORDER BY window_end_ts DESC
        LIMIT 1
        """,
        (window_kind,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"No wallet_edges rows for window_kind={window_kind}")
    return int(row[0]), int(row[1])


def fetch_edges_for_mint(
    cur: sqlite3.Cursor,
    window_kind: str,
    start_ts: int,
    end_ts: int,
    mint: str,
) -> List[Tuple[str, str, int, int]]:
    cur.execute(
        """
        SELECT src_wallet, dst_wallet, amount_raw, tx_count
        FROM wallet_edges
        WHERE window_kind = ?
          AND window_start_ts = ?
          AND window_end_ts = ?
          AND mint = ?
        """,
        (window_kind, start_ts, end_ts, mint),
    )
    return [(r[0], r[1], int(r[2]), int(r[3])) for r in cur.fetchall()]


def build_components(
    edges: List[Tuple[str, str, int, int]]
) -> Tuple[List[List[str]], int]:
    """
    Build connected components from edges treating them as undirected.
    
    Returns:
        Tuple of (list of components, count of unique undirected edges with positive amounts)
    """
    adjacency: Dict[str, set] = defaultdict(set)
    nodes = set()
    edge_pairs = set()  # Track unique undirected edges
    
    for src, dst, amount_raw, tx_count in edges:
        nodes.add(src)
        nodes.add(dst)
        if amount_raw > 0 and tx_count > 0:
            # Store canonical form (sorted) for undirected edge
            edge_pairs.add(tuple(sorted([src, dst])))
            adjacency[src].add(dst)
            adjacency[dst].add(src)
    
    visited = set()
    components = []
    for node in nodes:
        if node in visited:
            continue
        stack = [node]
        visited.add(node)
        comp = []
        while stack:
            current = stack.pop()
            comp.append(current)
            for neighbor in adjacency.get(current, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    stack.append(neighbor)
        components.append(comp)
    
    return components, len(edge_pairs)


def process_co_transfer(
    cur: sqlite3.Cursor,
    window_kind: str,
    start_ts: int,
    end_ts: int,
    min_members: int,
) -> Tuple[List[Tuple], List[Tuple], int]:
    """
    Process co-transfer cohorts based on connected components.
    
    Maps to schema:
    - scope_kind = "co_transfer_cc"
    - edge_density = density calculation
    - internal_flow_raw = amount_sum
    - cohort_score = amount_sum (for now)
    """
    threshold = min_members
    
    cur.execute(
        """
        SELECT mint, COUNT(DISTINCT wallet) AS wallet_count
        FROM (
            SELECT mint, src_wallet AS wallet
            FROM wallet_edges
            WHERE window_kind = ? AND window_start_ts = ? AND window_end_ts = ?
            UNION ALL
            SELECT mint, dst_wallet AS wallet
            FROM wallet_edges
            WHERE window_kind = ? AND window_start_ts = ? AND window_end_ts = ?
        )
        GROUP BY mint
        HAVING wallet_count >= ?
        """,
        (window_kind, start_ts, end_ts, window_kind, start_ts, end_ts, threshold),
    )
    mints = [row[0] for row in cur.fetchall()]

    cohorts = []
    members = []
    
    for mint in sorted(mints):
        edges = fetch_edges_for_mint(cur, window_kind, start_ts, end_ts, mint)
        if not edges:
            continue
        
        components, total_unique_edges = build_components(edges)
        
        for comp in components:
            if len(comp) < min_members:
                continue
            
            comp_set = set(comp)
            sorted_wallets = sorted(comp_set)
            
            # Filter edges internal to this component
            comp_edges = [
                e for e in edges if e[0] in comp_set and e[1] in comp_set
            ]
            
            # Count unique undirected edges in component
            comp_edge_pairs = set()
            tx_count_sum = 0
            amount_sum = 0
            
            for src, dst, amount_raw, tx_count in comp_edges:
                if amount_raw > 0 and tx_count > 0:
                    comp_edge_pairs.add(tuple(sorted([src, dst])))
                    tx_count_sum += tx_count
                    amount_sum += amount_raw
            
            internal_edge_count = len(comp_edge_pairs)
            
            # Fixed density calculation for undirected graph
            if len(comp_set) > 1:
                max_possible_edges = len(comp_set) * (len(comp_set) - 1) / 2
                density = internal_edge_count / max_possible_edges if max_possible_edges > 0 else 0.0
            else:
                density = 0.0
            
            cohort_id = (
                f"co_transfer_cc:{window_kind}:{start_ts}:{end_ts}:{mint}:"
                f"{sha1_hex(','.join(sorted_wallets))}"
            )
            
            # Map to actual schema columns
            cohorts.append(
                (
                    cohort_id,              # cohort_id
                    mint,                   # mint
                    "co_transfer_cc",       # scope_kind
                    window_kind,            # window_kind
                    start_ts,               # window_start_ts
                    end_ts,                 # window_end_ts
                    len(comp_set),          # member_count
                    density,                # edge_density
                    amount_sum,             # internal_flow_raw
                    0,                      # external_flow_raw (not calculated)
                    float(amount_sum),      # cohort_score
                    end_ts,                 # created_at
                    end_ts,                 # updated_at
                )
            )

            # Calculate member statistics
            member_stats: Dict[str, Dict] = {
                wallet: {
                    "inflow": 0,
                    "outflow": 0,
                    "neighbors_in": set(),
                    "neighbors_out": set(),
                }
                for wallet in comp_set
            }
            
            for src, dst, amount_raw, tx_count in comp_edges:
                # Track directional flows
                member_stats[src]["outflow"] += amount_raw
                member_stats[dst]["inflow"] += amount_raw
                
                # Track neighbors for degree calculation
                member_stats[src]["neighbors_out"].add(dst)
                member_stats[dst]["neighbors_in"].add(src)

            for wallet in sorted_wallets:
                stats = member_stats[wallet]
                
                # Calculate degree based on unique neighbors
                degree_in = len(stats["neighbors_in"])
                degree_out = len(stats["neighbors_out"])
                
                # Map to actual schema columns
                members.append(
                    (
                        cohort_id,              # cohort_id
                        wallet,                 # wallet
                        "member",               # role_hint
                        stats["inflow"],        # inflow_raw
                        stats["outflow"],       # outflow_raw
                        degree_in,              # degree_in
                        degree_out,             # degree_out
                    )
                )
    
    return cohorts, members, len(mints)


def process_hub_orbit(
    cur: sqlite3.Cursor,
    window_kind: str,
    start_ts: int,
    end_ts: int,
    min_members: int,
    hub_topk: int,
    hub_min_mints: int,
    hub_min_amount_raw: int,
) -> Tuple[List[Tuple], List[Tuple], int]:
    """
    Process hub-orbit cohorts.
    
    Maps to schema:
    - scope_kind = "hub_orbit"
    - mint = NULL (multi-mint cohort)
    """
    hub_min_members = max(min_members, 4)
    
    # Query to find top hub candidates based on incident amount
    cur.execute(
        """
        SELECT wallet, SUM(amount_raw) AS incident_amount, SUM(tx_count) AS incident_tx,
               COUNT(DISTINCT mint) AS distinct_mints
        FROM (
            SELECT src_wallet AS wallet, amount_raw, tx_count, mint
            FROM wallet_edges
            WHERE window_kind = ? AND window_start_ts = ? AND window_end_ts = ?
            UNION ALL
            SELECT dst_wallet AS wallet, amount_raw, tx_count, mint
            FROM wallet_edges
            WHERE window_kind = ? AND window_start_ts = ? AND window_end_ts = ?
        )
        WHERE mint IS NOT NULL
        GROUP BY wallet
        ORDER BY incident_amount DESC, wallet ASC
        LIMIT ?
        """,
        (window_kind, start_ts, end_ts, window_kind, start_ts, end_ts, hub_topk),
    )
    
    hubs = [
        (row[0], int(row[1]), int(row[2]), int(row[3]))
        for row in cur.fetchall()
    ]
    
    print(f"hubs_passing_threshold_topk={len(hubs)}")
    
    # Filter hubs by minimum mints and amount
    hubs = [
        hub
        for hub in hubs
        if hub[3] >= hub_min_mints and hub[1] >= hub_min_amount_raw
    ]
    
    print(f"hubs_after_min_mints_and_amount={len(hubs)}")

    cohorts = []
    members = []
    debug_hubs_printed = 0
    
    for hub_wallet, incident_amount, incident_tx, distinct_mints in hubs:
        # Find all wallets that have transacted with this hub
        cur.execute(
            """
            SELECT
                CASE WHEN src_wallet = ? THEN dst_wallet ELSE src_wallet END AS wallet,
                COUNT(DISTINCT mint) AS mint_count,
                SUM(amount_raw) AS total_amount,
                SUM(tx_count) AS tx_count,
                SUM(CASE WHEN src_wallet = ? THEN amount_raw ELSE 0 END) AS from_hub_amount,
                SUM(CASE WHEN dst_wallet = ? THEN amount_raw ELSE 0 END) AS to_hub_amount
            FROM wallet_edges
            WHERE window_kind = ?
              AND window_start_ts = ?
              AND window_end_ts = ?
              AND (src_wallet = ? OR dst_wallet = ?)
              AND mint IS NOT NULL
            GROUP BY wallet
            HAVING wallet != ?
            """,
            (hub_wallet, hub_wallet, hub_wallet, window_kind, start_ts, end_ts, 
             hub_wallet, hub_wallet, hub_wallet),
        )
        
        orbit_candidates = [
            (row[0], int(row[1]), int(row[2]), int(row[3]), int(row[4]), int(row[5]))
            for row in cur.fetchall()
        ]
        
        # Filter orbit members by minimum amount
        orbit_members = [
            cand
            for cand in orbit_candidates
            if cand[2] >= hub_min_amount_raw
        ]
        
        if debug_hubs_printed < 5:
            print(
                f"hub_eval hub={hub_wallet} distinct_mints={distinct_mints} "
                f"orbit_candidates={len(orbit_candidates)} "
                f"orbit_members={len(orbit_members)}"
            )
            debug_hubs_printed += 1
        
        # Check if cohort meets minimum member requirement (hub + orbit members)
        if 1 + len(orbit_members) < hub_min_members:
            continue

        orbit_amount_sum = sum(m[2] for m in orbit_members)
        orbit_tx_sum = sum(m[3] for m in orbit_members)
        
        cohort_id = f"hub_orbit:{window_kind}:{start_ts}:{end_ts}:{hub_wallet}"
        
        # Calculate edge density (hub to all orbit members)
        # Max edges = orbit_members (each can connect to hub)
        if len(orbit_members) > 0:
            density = len(orbit_members) / len(orbit_members)  # Always 1.0 for star topology
        else:
            density = 0.0
        
        # Map to actual schema columns
        cohorts.append(
            (
                cohort_id,              # cohort_id
                None,                   # mint (NULL for multi-mint)
                "hub_orbit",            # scope_kind
                window_kind,            # window_kind
                start_ts,               # window_start_ts
                end_ts,                 # window_end_ts
                1 + len(orbit_members), # member_count
                density,                # edge_density
                orbit_amount_sum,       # internal_flow_raw
                0,                      # external_flow_raw
                float(orbit_amount_sum), # cohort_score
                end_ts,                 # created_at
                end_ts,                 # updated_at
            )
        )
        
        # Add hub as member
        members.append(
            (
                cohort_id,              # cohort_id
                hub_wallet,             # wallet
                "hub",                  # role_hint
                incident_amount,        # inflow_raw (total incident)
                incident_amount,        # outflow_raw (total incident)
                len(orbit_members),     # degree_in
                len(orbit_members),     # degree_out
            )
        )
        
        # Add orbit members
        for wallet, mint_count, total_amount, tx_count, from_hub, to_hub in sorted(
            orbit_members, key=lambda x: (x[0])
        ):
            members.append(
                (
                    cohort_id,          # cohort_id
                    wallet,             # wallet
                    "orbit",            # role_hint
                    from_hub,           # inflow_raw (from hub)
                    to_hub,             # outflow_raw (to hub)
                    1,                  # degree_in (connected to hub)
                    1,                  # degree_out (connected to hub)
                )
            )

    return cohorts, members, len(hubs)


def insert_phase2_run(
    cur: sqlite3.Cursor,
    run_id: str,
    window_kind: str,
    start_ts: int,
    end_ts: int,
    input_counts: dict,
    output_counts: dict,
) -> None:
    """Insert or update phase2_runs record."""
    cur.execute(
        """
        INSERT INTO phase2_runs (
            run_id, component, window_kind, window_start_ts, window_end_ts,
            input_counts_json, output_counts_json, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id) DO UPDATE SET
            input_counts_json=excluded.input_counts_json,
            output_counts_json=excluded.output_counts_json,
            created_at=excluded.created_at
        """,
        (
            run_id,
            COMPONENT,
            window_kind,
            start_ts,
            end_ts,
            stable_json(input_counts),
            stable_json(output_counts),
            end_ts,
        ),
    )


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db)
    conn.isolation_level = None
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    for window_kind in WINDOW_KINDS:
        try:
            start_ts, end_ts = get_window_bounds(cur, window_kind)
            
            cur.execute(
                """
                SELECT COUNT(*)
                FROM wallet_edges
                WHERE window_kind = ? AND window_start_ts = ? AND window_end_ts = ?
                """,
                (window_kind, start_ts, end_ts),
            )
            total_edges = int(cur.fetchone()[0])
            
            print(
                f"window={window_kind} start={start_ts} end={end_ts} edge_count={total_edges}"
            )

            cur.execute("BEGIN IMMEDIATE")

            # Delete existing cohorts and members for this window
            cur.execute(
                """
                DELETE FROM cohort_members
                WHERE cohort_id IN (
                    SELECT cohort_id FROM cohorts
                    WHERE window_kind = ? AND window_start_ts = ? AND window_end_ts = ?
                )
                """,
                (window_kind, start_ts, end_ts),
            )
            
            cur.execute(
                """
                DELETE FROM cohorts
                WHERE window_kind = ? AND window_start_ts = ? AND window_end_ts = ?
                """,
                (window_kind, start_ts, end_ts),
            )

            # Process co-transfer cohorts
            co_transfer_cohorts, co_transfer_members, mints_processed = process_co_transfer(
                cur, window_kind, start_ts, end_ts, args.min_members
            )
            
            print(
                f"co_transfer cohorts={len(co_transfer_cohorts)} "
                f"members={len(co_transfer_members)}"
            )
            
            # Process hub-orbit cohorts
            hub_cohorts, hub_members, hubs_evaluated = process_hub_orbit(
                cur,
                window_kind,
                start_ts,
                end_ts,
                args.min_members,
                args.hub_topk,
                args.hub_min_mints,
                args.hub_min_amount_raw,
            )
            
            print(
                f"hub_orbit cohorts={len(hub_cohorts)} members={len(hub_members)}"
            )

            # Combine results
            cohorts = co_transfer_cohorts + hub_cohorts
            members = co_transfer_members + hub_members
            
            print(
                f"cohorts_to_insert={len(cohorts)} "
                f"cohort_members_to_insert={len(members)}"
            )
            
            # Validation: if there are edges, we should have cohorts (unless thresholds filter all)
            if total_edges > 0 and len(cohorts) == 0:
                print("WARNING: edges present but zero cohorts (may be expected if all filtered by thresholds)")

            # Insert cohorts
            if cohorts:
                cur.executemany(
                    """
                    INSERT INTO cohorts (
                        cohort_id, mint, scope_kind, window_kind, window_start_ts, window_end_ts,
                        member_count, edge_density, internal_flow_raw, external_flow_raw, 
                        cohort_score, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    cohorts,
                )
            
            # Insert members
            if members:
                cur.executemany(
                    """
                    INSERT INTO cohort_members (
                        cohort_id, wallet, role_hint, inflow_raw, outflow_raw, 
                        degree_in, degree_out
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    members,
                )

            # Record run metadata
            input_counts = {
                "window_kind": window_kind,
                "window_start_ts": start_ts,
                "window_end_ts": end_ts,
                "mints_processed_co_transfer": mints_processed,
                "hubs_evaluated": hubs_evaluated,
                "total_wallet_edges_rows_in_window": total_edges,
            }
            
            output_counts = {
                "cohorts_inserted_co_transfer": len(co_transfer_cohorts),
                "cohorts_inserted_hub_orbit": len(hub_cohorts),
                "cohort_members_inserted_total": len(members),
                "status": "ok",
                "error": None,
            }
            
            run_id = f"{COMPONENT}:{window_kind}:{start_ts}:{end_ts}"
            insert_phase2_run(cur, run_id, window_kind, start_ts, end_ts, input_counts, output_counts)

            cur.execute("COMMIT")
            print("window committed OK")

            total_cohorts = len(cohorts)
            print(
                f"{window_kind} start={start_ts} end={end_ts} "
                f"cohorts={total_cohorts} members={len(members)} "
                f"co_transfer={len(co_transfer_cohorts)} hub_orbit={len(hub_cohorts)}"
            )
            
        except Exception as exc:
            # Rollback on error
            if conn.in_transaction:
                cur.execute("ROLLBACK")
            
            # Try to get window bounds for error logging
            try:
                start_ts, end_ts = get_window_bounds(cur, window_kind)
            except Exception as e:
                print(f"WARNING: Could not get window bounds in error handler: {e}")
                start_ts, end_ts = 0, 0
            
            # Record failed run
            input_counts = {
                "window_kind": window_kind,
                "window_start_ts": start_ts,
                "window_end_ts": end_ts,
                "mints_processed_co_transfer": 0,
                "hubs_evaluated": 0,
                "total_wallet_edges_rows_in_window": 0,
                "error_during": "processing",
            }
            
            output_counts = {
                "cohorts_inserted_co_transfer": 0,
                "cohorts_inserted_hub_orbit": 0,
                "cohort_members_inserted_total": 0,
                "status": "failed",
                "error": str(exc),
            }
            
            run_id = f"{COMPONENT}:{window_kind}:{start_ts}:{end_ts}"
            
            try:
                insert_phase2_run(cur, run_id, window_kind, start_ts, end_ts, input_counts, output_counts)
                conn.commit()
            except Exception as insert_exc:
                print(f"ERROR: Failed to record error in phase2_runs: {insert_exc}")
            
            print(f"ERROR processing {window_kind}: {exc}")
            import traceback
            traceback.print_exc()
            return 1

    print("All windows processed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
