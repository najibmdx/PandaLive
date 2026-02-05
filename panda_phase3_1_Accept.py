#!/usr/bin/env python3
import argparse
import datetime
import json
import sqlite3
import sys

WINDOWS = ("24h", "7d", "lifetime")
REQUIRED_COLUMNS = (
    "scan_wallet",
    "window",
    "tx_count_total",
    "sol_volume_total",
    "created_at_utc",
)
SUMMARY_FILE = "phase3_1_accept_wallet_features.summary.json"


def print_section(title):
    print("=" * 64)
    print(title)
    print("=" * 64)


def connect_read_only(db_path):
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def fetch_one_value(cursor, query, params=None):
    if params is None:
        params = ()
    cursor.execute(query, params)
    row = cursor.fetchone()
    if row is None:
        return None
    return row[0]


def format_examples(examples):
    for example in examples[:5]:
        print("  -", example)


def reference_now_utc(cursor):
    value = fetch_one_value(
        cursor, "SELECT MAX(created_at_utc) FROM wallet_features"
    )
    if value is not None:
        return int(value)
    value = fetch_one_value(
        cursor, "SELECT MAX(block_time) FROM wallet_token_flow"
    )
    if value is not None:
        return int(value)
    return int(datetime.datetime.utcnow().timestamp())


def schema_check(cursor):
    check_name = "[1] SCHEMA & TABLE SANITY"
    print_section(check_name)
    errors = []

    table_exists = fetch_one_value(
        cursor,
        "SELECT name FROM sqlite_master WHERE type='table' AND name='wallet_features'",
    )
    if not table_exists:
        errors.append("wallet_features table missing")
        print("FAIL")
        format_examples(errors)
        return False, {"errors": errors}

    cursor.execute("PRAGMA table_info(wallet_features)")
    columns = [row[1] for row in cursor.fetchall()]
    missing = [col for col in REQUIRED_COLUMNS if col not in columns]
    extra = [col for col in columns if col not in REQUIRED_COLUMNS]
    if missing or extra:
        errors.append(f"column mismatch: missing={missing}, extra={extra}")

    if errors:
        print("FAIL")
        format_examples(errors)
        return False, {"errors": errors}

    print("PASS")
    return True, {}


def row_invariant_check(cursor):
    check_name = "[2] ROW-LEVEL INVARIANTS"
    print_section(check_name)
    where_clause = (
        "scan_wallet IS NULL OR window NOT IN ('24h','7d','lifetime') "
        "OR tx_count_total IS NULL OR sol_volume_total IS NULL "
        "OR created_at_utc IS NULL "
        "OR tx_count_total <= 0 OR sol_volume_total < 0 OR created_at_utc <= 0"
    )
    count = fetch_one_value(
        cursor, f"SELECT COUNT(*) FROM wallet_features WHERE {where_clause}"
    )
    cursor.execute(
        f"SELECT scan_wallet, window, tx_count_total, sol_volume_total, created_at_utc "
        f"FROM wallet_features WHERE {where_clause} LIMIT 5"
    )
    examples = [
        {
            "scan_wallet": row[0],
            "window": row[1],
            "tx_count_total": row[2],
            "sol_volume_total": row[3],
            "created_at_utc": row[4],
        }
        for row in cursor.fetchall()
    ]
    if count and count > 0:
        print("FAIL")
        format_examples(examples)
        return False, {"violations": count, "examples": examples}
    print("PASS")
    return True, {}


def primary_key_check(cursor):
    check_name = "[3] PRIMARY KEY UNIQUENESS"
    print_section(check_name)
    cursor.execute(
        "SELECT scan_wallet, window, COUNT(*) AS c "
        "FROM wallet_features GROUP BY scan_wallet, window HAVING c > 1 LIMIT 5"
    )
    examples = [
        {"scan_wallet": row[0], "window": row[1], "count": row[2]}
        for row in cursor.fetchall()
    ]
    count = fetch_one_value(
        cursor,
        "SELECT COUNT(*) FROM ("
        "SELECT 1 FROM wallet_features GROUP BY scan_wallet, window HAVING COUNT(*) > 1"
        ")",
    )
    if count and count > 0:
        print("FAIL")
        format_examples(examples)
        return False, {"duplicates": count, "examples": examples}
    print("PASS")
    return True, {}


def fetch_wallet_features(cursor):
    cursor.execute(
        "SELECT scan_wallet, window, tx_count_total, sol_volume_total, created_at_utc "
        "FROM wallet_features"
    )
    rows = cursor.fetchall()
    data = {window: {} for window in WINDOWS}
    for scan_wallet, window, tx_count_total, sol_volume_total, created_at_utc in rows:
        data.setdefault(window, {})[scan_wallet] = (
            tx_count_total,
            sol_volume_total,
            created_at_utc,
        )
    return data


def window_monotonicity_check(features):
    check_name = "[4] WINDOW MONOTONICITY"
    print_section(check_name)
    violations = []
    wallets = set()
    for window_map in features.values():
        wallets.update(window_map.keys())
    for wallet in sorted(wallets):
        lifetime = features.get("lifetime", {}).get(wallet)
        seven = features.get("7d", {}).get(wallet)
        day = features.get("24h", {}).get(wallet)
        if lifetime and seven:
            if lifetime[0] < seven[0] or lifetime[1] < seven[1]:
                violations.append(
                    {
                        "scan_wallet": wallet,
                        "pair": "lifetime>=7d",
                        "lifetime": lifetime[:2],
                        "7d": seven[:2],
                    }
                )
        if seven and day:
            if seven[0] < day[0] or seven[1] < day[1]:
                violations.append(
                    {
                        "scan_wallet": wallet,
                        "pair": "7d>=24h",
                        "7d": seven[:2],
                        "24h": day[:2],
                    }
                )
        if lifetime and day:
            if lifetime[0] < day[0] or lifetime[1] < day[1]:
                violations.append(
                    {
                        "scan_wallet": wallet,
                        "pair": "lifetime>=24h",
                        "lifetime": lifetime[:2],
                        "24h": day[:2],
                    }
                )
    if violations:
        print("FAIL")
        format_examples(violations)
        return False, {"violations": len(violations), "examples": violations[:5]}
    print("PASS")
    return True, {}


def recompute_wallet_token_flow(cursor, now_utc):
    recomputed = {}
    for window in WINDOWS:
        if window == "24h":
            threshold = now_utc - 86400
            query = (
                "SELECT scan_wallet, COUNT(*) AS tx_count_total, "
                "SUM(ABS(sol_amount_lamports)) AS sol_volume_total "
                "FROM wallet_token_flow WHERE block_time >= ? GROUP BY scan_wallet"
            )
            params = (threshold,)
        elif window == "7d":
            threshold = now_utc - 604800
            query = (
                "SELECT scan_wallet, COUNT(*) AS tx_count_total, "
                "SUM(ABS(sol_amount_lamports)) AS sol_volume_total "
                "FROM wallet_token_flow WHERE block_time >= ? GROUP BY scan_wallet"
            )
            params = (threshold,)
        else:
            query = (
                "SELECT scan_wallet, COUNT(*) AS tx_count_total, "
                "SUM(ABS(sol_amount_lamports)) AS sol_volume_total "
                "FROM wallet_token_flow GROUP BY scan_wallet"
            )
            params = ()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        recomputed[window] = {
            row[0]: (row[1], row[2])
            for row in rows
        }
    return recomputed


def parity_recompute_check(features, recomputed):
    check_name = "[5] PARITY RECOMPUTATION (CRITICAL)"
    print_section(check_name)
    mismatches = []
    for window in WINDOWS:
        feature_rows = features.get(window, {})
        recomputed_rows = recomputed.get(window, {})
        for wallet in sorted(set(feature_rows.keys()) & set(recomputed_rows.keys())):
            f_tx, f_sol, _ = feature_rows[wallet]
            r_tx, r_sol = recomputed_rows[wallet]
            if f_tx != r_tx or f_sol != r_sol:
                mismatches.append(
                    {
                        "scan_wallet": wallet,
                        "window": window,
                        "feature": {"tx_count_total": f_tx, "sol_volume_total": f_sol},
                        "recomputed": {
                            "tx_count_total": r_tx,
                            "sol_volume_total": r_sol,
                        },
                    }
                )
    if mismatches:
        print("FAIL")
        format_examples(mismatches)
        return False, {"mismatches": len(mismatches), "examples": mismatches[:5]}
    print("PASS")
    return True, {}


def phantom_missing_check(features, recomputed):
    check_name = "[6] PHANTOM / MISSING WALLET CHECK"
    print_section(check_name)
    issues = []
    for window in WINDOWS:
        feature_wallets = set(features.get(window, {}).keys())
        recomputed_wallets = set(recomputed.get(window, {}).keys())
        phantom = sorted(feature_wallets - recomputed_wallets)
        missing = sorted(recomputed_wallets - feature_wallets)
        if phantom:
            issues.append(
                {
                    "window": window,
                    "type": "phantom",
                    "wallets": phantom[:5],
                    "count": len(phantom),
                }
            )
        if missing:
            issues.append(
                {
                    "window": window,
                    "type": "missing",
                    "wallets": missing[:5],
                    "count": len(missing),
                }
            )
    if issues:
        print("FAIL")
        format_examples(issues)
        return False, {"issues": issues}
    print("PASS")
    return True, {}


def determinism_check(features, recomputed):
    check_name = "[7] DETERMINISM CHECK (LIGHT)"
    print_section(check_name)
    issues = []
    for window in WINDOWS:
        feature_count = len(features.get(window, {}))
        recomputed_count = len(recomputed.get(window, {}))
        if feature_count != recomputed_count:
            issues.append(
                {
                    "window": window,
                    "type": "count_mismatch",
                    "features": feature_count,
                    "recomputed": recomputed_count,
                }
            )
        for wallet, values in recomputed.get(window, {}).items():
            if values[0] is None or values[1] is None:
                issues.append(
                    {
                        "window": window,
                        "type": "null_aggregate",
                        "scan_wallet": wallet,
                        "values": values,
                    }
                )
                if len(issues) >= 5:
                    break
        if len(issues) >= 5:
            break
    if issues:
        print("FAIL")
        format_examples(issues)
        return False, {"issues": issues}
    print("PASS")
    return True, {}


def print_counts(features, recomputed):
    print("Counts per window:")
    for window in WINDOWS:
        feature_count = len(features.get(window, {}))
        recomputed_count = len(recomputed.get(window, {}))
        print(
            f"  {window}: wallet_features={feature_count}, recomputed={recomputed_count}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="PANDA v4 Phase 3.1 Wallet Feature Matrix Acceptance Inspector"
    )
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    args = parser.parse_args()

    summary = {
        "checks": {},
        "counts": {},
        "overall_pass": False,
    }

    try:
        conn = connect_read_only(args.db)
    except sqlite3.Error as exc:
        print(f"Failed to open database: {exc}")
        sys.exit(1)

    try:
        cursor = conn.cursor()
        schema_pass, schema_details = schema_check(cursor)
        summary["checks"]["schema"] = {
            "pass": schema_pass,
            **schema_details,
        }
        if not schema_pass:
            summary["overall_pass"] = False
            with open(SUMMARY_FILE, "w", encoding="utf-8") as handle:
                json.dump(summary, handle, indent=2, sort_keys=True)
            sys.exit(1)

        now_utc = reference_now_utc(cursor)

        row_pass, row_details = row_invariant_check(cursor)
        summary["checks"]["row_invariants"] = {
            "pass": row_pass,
            **row_details,
        }

        pk_pass, pk_details = primary_key_check(cursor)
        summary["checks"]["primary_key_uniqueness"] = {
            "pass": pk_pass,
            **pk_details,
        }

        features = fetch_wallet_features(cursor)
        window_pass, window_details = window_monotonicity_check(features)
        summary["checks"]["window_monotonicity"] = {
            "pass": window_pass,
            **window_details,
        }

        recomputed = recompute_wallet_token_flow(cursor, now_utc)
        parity_pass, parity_details = parity_recompute_check(features, recomputed)
        summary["checks"]["parity_recomputation"] = {
            "pass": parity_pass,
            **parity_details,
        }

        phantom_pass, phantom_details = phantom_missing_check(features, recomputed)
        summary["checks"]["phantom_missing"] = {
            "pass": phantom_pass,
            **phantom_details,
        }

        deterministic_pass, deterministic_details = determinism_check(
            features, recomputed
        )
        summary["checks"]["determinism"] = {
            "pass": deterministic_pass,
            **deterministic_details,
        }

        print_counts(features, recomputed)
        summary["counts"] = {
            window: {
                "wallet_features": len(features.get(window, {})),
                "recomputed": len(recomputed.get(window, {})),
            }
            for window in WINDOWS
        }

        summary["overall_pass"] = all(
            details["pass"] for details in summary["checks"].values()
        )

        with open(SUMMARY_FILE, "w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2, sort_keys=True)

        if summary["overall_pass"]:
            sys.exit(0)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
