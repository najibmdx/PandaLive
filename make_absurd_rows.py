import argparse
import pandas as pd
import numpy as np

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tsv", required=True)
    ap.add_argument("--out", default="absurd_rows.tsv")
    ap.add_argument("--abs-threshold", type=float, default=20.0)
    args = ap.parse_args()

    df = pd.read_csv(args.tsv, sep="\t", dtype=str)

    # ensure required columns exist
    req = ["wallet_address", "token", "first_seen_time", "realized_profit_sol"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing columns: {missing}")

    # numeric convert
    df["realized_profit_sol"] = pd.to_numeric(df["realized_profit_sol"], errors="coerce")
    df["first_seen_time"] = pd.to_numeric(df["first_seen_time"], errors="coerce")

    # filter absurd
    m = df["realized_profit_sol"].notna() & (df["realized_profit_sol"].abs() >= args.abs_threshold)
    out = df.loc[m, ["wallet_address", "token", "first_seen_time"]].dropna()

    # drop duplicates for clean merge keys
    out = out.drop_duplicates()

    out.to_csv(args.out, sep="\t", index=False)
    print(f"Wrote {len(out)} rows to {args.out} using abs-threshold={args.abs_threshold}")

if __name__ == "__main__":
    main()
