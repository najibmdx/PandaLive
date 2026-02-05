"""Step 2 archetype validation.

Example usage:
python step2_validate_archetypes.py --tsv profit_situations_all.tsv --outdir step2_out --absurd-tsv absurd_rows.tsv
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REQUIRED_COLUMNS = [
    "wallet_address",
    "wallet_label",
    "token",
    "realized_profit_sol",
    "wallet_total_profit_sol",
    "trade_count",
    "first_buy_time",
    "first_sell_time",
    "first_seen_time",
    "cohort_60s",
    "cohort_300s",
    "positive_total_wallet_profit_cohort_300s",
    "positive_token_profit_cohort_300s",
    "profit_source",
    "profit_note",
]

NUMERIC_COLUMNS = [
    "realized_profit_sol",
    "wallet_total_profit_sol",
    "trade_count",
    "first_buy_time",
    "first_sell_time",
    "first_seen_time",
    "cohort_60s",
    "cohort_300s",
    "positive_total_wallet_profit_cohort_300s",
    "positive_token_profit_cohort_300s",
]

ARCHETYPES = [
    "A_ELITE_SOLO",
    "B_ELITE_MICRO_PACK",
    "C_ELITE_SWARM",
    "D_CROWD_DEPENDENT_RIDER",
    "E_TOKEN_SPECIALIST",
    "F_DENSE_LOW_SKILL_CROWD",
    "G_SOLO_NOISE",
    "H_OTHER",
]

SLICE_HEADER = [
    "slice_id",
    "speed_bucket",
    "skill_bucket",
    "commitment_bucket",
    "n",
    "win_rate",
    "median_profit",
    "early_median_profit",
    "late_median_profit",
    "p25_profit",
    "p75_profit",
    "survives_slice",
]

SOLO_SLICE_HEADER = [
    "slice_id",
    "solo_type",
    "commitment_bucket",
    "timing_bucket",
    "profit_source_bucket",
    "n",
    "win_rate",
    "median_profit",
    "early_median_profit",
    "late_median_profit",
    "p25_profit",
    "p75_profit",
    "survives_slice",
]

TOKEN_SLICE_HEADER = [
    "slice_id",
    "timing_bucket",
    "speed_bucket",
    "commitment_bucket",
    "n",
    "win_rate",
    "median_profit",
    "early_median_profit",
    "late_median_profit",
    "p25_profit",
    "p75_profit",
    "survives_slice",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Step 2 archetype validation")
    parser.add_argument("--tsv", required=True, help="Path to profit_situations_all.tsv")
    parser.add_argument("--outdir", required=True, help="Output directory")
    parser.add_argument("--absurd-tsv", default=None, help="Optional absurd rows TSV")
    parser.add_argument("--absurd-note-substr", default="", help="Substring to flag absurd rows")
    parser.add_argument("--n-min", type=int, default=200, help="Minimum n for survival")
    parser.add_argument("--delta-win", type=float, default=0.05, help="Win rate delta")
    parser.add_argument("--cap-pct", type=float, default=0.995, help="Profit cap percentile")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    return parser.parse_args()


def log(msg: str, verbose: bool) -> None:
    if verbose:
        print(msg)


def load_tsv(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path, sep="\t", engine="python")
    except FileNotFoundError:
        print(f"ERROR: cannot read file: {path}")
        sys.exit(3)
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: cannot read file: {path} ({exc})")
        sys.exit(3)


def enforce_columns(df: pd.DataFrame) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        print(f"ERROR: missing required columns: {', '.join(missing)}")
        sys.exit(2)


def coerce_numeric(df: pd.DataFrame) -> pd.DataFrame:
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df[NUMERIC_COLUMNS] = df[NUMERIC_COLUMNS].replace([np.inf, -np.inf], np.nan)
    return df


def drop_nan_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    before = len(df)
    df = df.dropna(subset=["realized_profit_sol", "first_seen_time"])
    dropped = before - len(df)
    return df, dropped


def apply_absurd_filters(
    df: pd.DataFrame,
    absurd_tsv: str | None,
    absurd_note_substr: str,
    verbose: bool,
) -> tuple[pd.DataFrame, int]:
    excluded = 0

    if absurd_tsv:
        absurd_df = load_tsv(absurd_tsv)
        expected_cols = ["wallet_address", "token", "first_seen_time"]
        if not set(expected_cols).issubset(absurd_df.columns):
            print(
                "ERROR: absurd TSV missing required columns: "
                + ", ".join(sorted(set(expected_cols) - set(absurd_df.columns)))
            )
            sys.exit(2)
        absurd_df = absurd_df[expected_cols].copy()
        absurd_df["wallet_address"] = absurd_df["wallet_address"].astype(str)
        absurd_df["token"] = absurd_df["token"].astype(str)
        absurd_df["first_seen_time"] = pd.to_numeric(
            absurd_df["first_seen_time"], errors="coerce"
        )
        absurd_df = absurd_df.dropna(subset=["first_seen_time"])
        df = df.merge(
            absurd_df,
            on=["wallet_address", "token", "first_seen_time"],
            how="left",
            indicator=True,
        )
        removed = (df["_merge"] == "both").sum()
        excluded += int(removed)
        df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])
        log(f"Absurd TSV excluded rows: {removed}", verbose)

    if absurd_note_substr:
        note_mask = df["profit_note"].fillna("").str.contains(
            absurd_note_substr, case=False, na=False
        )
        removed = int(note_mask.sum())
        excluded += removed
        df = df[~note_mask]
        log(f"Absurd note excluded rows: {removed}", verbose)

    return df, excluded


def assign_skill_tier(df: pd.DataFrame, s_p50: float, s_p90: float) -> pd.Series:
    wallet_profit = df["wallet_total_profit_sol"]
    conditions = [
        wallet_profit <= 0,
        (wallet_profit > 0) & (wallet_profit <= s_p50),
        (wallet_profit > s_p50) & (wallet_profit <= s_p90),
        wallet_profit > s_p90,
    ]
    choices = ["S0", "S1", "S2", "S3"]
    return pd.Series(np.select(conditions, choices, default="S0"), index=df.index)


def assign_density_tier(df: pd.DataFrame) -> pd.Series:
    cohort = df["cohort_300s"]
    conditions = [
        cohort == 0,
        (cohort >= 1) & (cohort <= 2),
        (cohort >= 3) & (cohort <= 7),
        cohort >= 8,
    ]
    choices = ["D0", "D1", "D2", "D3"]
    return pd.Series(np.select(conditions, choices, default="D0"), index=df.index)


def assign_quality_tier(ratio: pd.Series) -> pd.Series:
    conditions = [
        ratio == 0,
        (ratio > 0) & (ratio <= 0.33),
        (ratio > 0.33) & (ratio <= 0.66),
        ratio > 0.66,
    ]
    choices = ["Q0", "Q1", "Q2", "Q3"]
    return pd.Series(np.select(conditions, choices, default="Q0"), index=ratio.index)


def assign_archetype(
    skill: pd.Series, density: pd.Series, q_wallet: pd.Series, q_token: pd.Series
) -> pd.Series:
    archetype = pd.Series("H_OTHER", index=skill.index)

    mask = (archetype == "H_OTHER") & (skill == "S3") & (density == "D0")
    archetype.loc[mask] = "A_ELITE_SOLO"

    mask = (
        (archetype == "H_OTHER")
        & (skill == "S3")
        & (density == "D1")
        & (q_wallet.isin(["Q2", "Q3"]))
    )
    archetype.loc[mask] = "B_ELITE_MICRO_PACK"

    mask = (
        (archetype == "H_OTHER")
        & skill.isin(["S2", "S3"])
        & (density == "D3")
        & (q_wallet.isin(["Q2", "Q3"]))
    )
    archetype.loc[mask] = "C_ELITE_SWARM"

    mask = (
        (archetype == "H_OTHER")
        & skill.isin(["S0", "S1"])
        & density.isin(["D2", "D3"])
        & (q_wallet.isin(["Q2", "Q3"]))
    )
    archetype.loc[mask] = "D_CROWD_DEPENDENT_RIDER"

    mask = (archetype == "H_OTHER") & q_token.isin(["Q2", "Q3"])
    archetype.loc[mask] = "E_TOKEN_SPECIALIST"

    mask = (archetype == "H_OTHER") & (density == "D3") & (q_wallet.isin(["Q0", "Q1"]))
    archetype.loc[mask] = "F_DENSE_LOW_SKILL_CROWD"

    mask = (
        (archetype == "H_OTHER")
        & skill.isin(["S0", "S1"])
        & density.isin(["D0", "D1"])
    )
    archetype.loc[mask] = "G_SOLO_NOISE"

    return archetype


def median_or_nan(series: pd.Series) -> float:
    if series.empty:
        return float("nan")
    return float(series.median())


def build_archetype_stats(
    df: pd.DataFrame,
    baseline_win_rate: float,
) -> pd.DataFrame:
    rows = []
    for archetype in ARCHETYPES:
        subset = df[df["archetype"] == archetype]
        n = int(len(subset))
        win_rate = float((subset["realized_profit_sol"] > 0).mean()) if n else float("nan")
        median_profit = median_or_nan(subset["profit_capped"])
        early_median = median_or_nan(subset.loc[subset["half"] == "EARLY", "profit_capped"])
        late_median = median_or_nan(subset.loc[subset["half"] == "LATE", "profit_capped"])
        p25_profit = float(subset["profit_capped"].quantile(0.25)) if n else float("nan")
        p75_profit = float(subset["profit_capped"].quantile(0.75)) if n else float("nan")
        edgescore = (
            median_profit * (win_rate - baseline_win_rate) * math.log10(n)
            if n > 0
            else 0.0
        )
        rows.append(
            {
                "archetype": archetype,
                "n": n,
                "win_rate": win_rate,
                "median_profit": median_profit,
                "early_median_profit": early_median,
                "late_median_profit": late_median,
                "p25_profit": p25_profit,
                "p75_profit": p75_profit,
                "edgescore": edgescore,
            }
        )

    return pd.DataFrame(rows)


def build_c_elite_swarm_slices(df: pd.DataFrame) -> pd.DataFrame:
    df_swarm = df[df["archetype"] == "C_ELITE_SWARM"].copy()
    if df_swarm.empty:
        return pd.DataFrame(columns=SLICE_HEADER)

    df_swarm["speed_bucket"] = np.select(
        [
            df_swarm["cohort_60s"] >= 3,
            df_swarm["cohort_60s"].isin([1, 2]),
            df_swarm["cohort_60s"] == 0,
        ],
        ["FAST", "MED", "SLOW"],
        default="SLOW",
    )
    df_swarm["skill_bucket"] = np.select(
        [df_swarm["skill_tier"] == "S3", df_swarm["skill_tier"] == "S2"],
        ["PURE_ELITE", "MIXED"],
        default="MIXED",
    )
    df_swarm["commitment_bucket"] = np.select(
        [df_swarm["trade_count"] >= 2, df_swarm["trade_count"] == 1],
        ["COMMITTED", "DRIVE_BY"],
        default="OTHER_TC",
    )

    slices = []

    def add_slice(slice_df: pd.DataFrame, speed: str, skill: str, commitment: str) -> None:
        n = int(len(slice_df))
        win_rate = float((slice_df["realized_profit_sol"] > 0).mean()) if n else float("nan")
        median_profit = median_or_nan(slice_df["profit_capped"])
        early_median = median_or_nan(
            slice_df.loc[slice_df["half"] == "EARLY", "profit_capped"]
        )
        late_median = median_or_nan(
            slice_df.loc[slice_df["half"] == "LATE", "profit_capped"]
        )
        p25_profit = float(slice_df["profit_capped"].quantile(0.25)) if n else float("nan")
        p75_profit = float(slice_df["profit_capped"].quantile(0.75)) if n else float("nan")
        survives_slice = (
            n >= 150
            and median_profit > 0
            and early_median > 0
            and late_median > 0
        )
        slices.append(
            {
                "slice_id": f"C|{speed}|{skill}|{commitment}",
                "speed_bucket": speed,
                "skill_bucket": skill,
                "commitment_bucket": commitment,
                "n": n,
                "win_rate": win_rate,
                "median_profit": median_profit,
                "early_median_profit": early_median,
                "late_median_profit": late_median,
                "p25_profit": p25_profit,
                "p75_profit": p75_profit,
                "survives_slice": bool(survives_slice),
            }
        )

    speed_values = ["FAST", "MED", "SLOW"]
    skill_values = ["PURE_ELITE", "MIXED"]
    commitment_values = ["COMMITTED", "DRIVE_BY", "OTHER_TC"]

    for speed in speed_values:
        slice_speed = df_swarm[df_swarm["speed_bucket"] == speed]
        add_slice(slice_speed, speed, "ALL", "ALL")

        for skill in skill_values:
            slice_speed_skill = slice_speed[slice_speed["skill_bucket"] == skill]
            add_slice(slice_speed_skill, speed, skill, "ALL")

        for commitment in commitment_values:
            slice_speed_commit = slice_speed[
                slice_speed["commitment_bucket"] == commitment
            ]
            add_slice(slice_speed_commit, speed, "ALL", commitment)

            for skill in skill_values:
                slice_full = slice_speed_commit[
                    slice_speed_commit["skill_bucket"] == skill
                ]
                add_slice(slice_full, speed, skill, commitment)

    return pd.DataFrame(slices, columns=SLICE_HEADER)


def build_a_elite_solo_slices(df: pd.DataFrame) -> pd.DataFrame:
    df_solo = df[df["archetype"] == "A_ELITE_SOLO"].copy()
    df_true_solo = df_solo[df_solo["cohort_60s"] == 0].copy()

    def assign_solo_buckets(solo_df: pd.DataFrame, solo_type: str) -> pd.DataFrame:
        if solo_df.empty:
            return solo_df.assign(
                solo_type=solo_type,
                commitment_bucket=pd.Series(dtype=str),
                timing_bucket=pd.Series(dtype=str),
                profit_source_bucket=pd.Series(dtype=str),
            )
        delta_buy = solo_df["first_buy_time"] - solo_df["first_seen_time"]
        timing_bucket = np.select(
            [
                delta_buy <= 30,
                (delta_buy > 30) & (delta_buy <= 120),
                delta_buy > 120,
                delta_buy.isna(),
            ],
            ["INSTANT", "QUICK", "LATE", "UNKNOWN"],
            default="UNKNOWN",
        )
        solo_df = solo_df.assign(
            solo_type=solo_type,
            commitment_bucket=np.select(
                [solo_df["trade_count"] >= 2, solo_df["trade_count"] == 1],
                ["COMMITTED", "ONE_SHOT"],
                default="OTHER_TC",
            ),
            timing_bucket=timing_bucket,
            profit_source_bucket=np.where(
                solo_df["profit_source"]
                .fillna("")
                .str.contains("sol", case=False, na=False),
                "SOL_ONLY",
                "MIXED",
            ),
        )
        return solo_df

    df_solo = assign_solo_buckets(df_solo, "SOLO_ALL")
    df_true_solo = assign_solo_buckets(df_true_solo, "SOLO_TRUE")

    slices = []

    def add_slice(
        slice_df: pd.DataFrame,
        solo_type: str,
        commitment: str,
        timing: str,
        profit_source: str,
    ) -> None:
        n = int(len(slice_df))
        win_rate = float((slice_df["realized_profit_sol"] > 0).mean()) if n else float("nan")
        median_profit = median_or_nan(slice_df["profit_capped"])
        early_median = median_or_nan(
            slice_df.loc[slice_df["half"] == "EARLY", "profit_capped"]
        )
        late_median = median_or_nan(
            slice_df.loc[slice_df["half"] == "LATE", "profit_capped"]
        )
        p25_profit = float(slice_df["profit_capped"].quantile(0.25)) if n else float("nan")
        p75_profit = float(slice_df["profit_capped"].quantile(0.75)) if n else float("nan")
        survives_slice = (
            n >= 120
            and median_profit > 0
            and early_median > 0
            and late_median > 0
        )
        slices.append(
            {
                "slice_id": f"A|{solo_type}|{timing}|{commitment}|{profit_source}",
                "solo_type": solo_type,
                "commitment_bucket": commitment,
                "timing_bucket": timing,
                "profit_source_bucket": profit_source,
                "n": n,
                "win_rate": win_rate,
                "median_profit": median_profit,
                "early_median_profit": early_median,
                "late_median_profit": late_median,
                "p25_profit": p25_profit,
                "p75_profit": p75_profit,
                "survives_slice": bool(survives_slice),
            }
        )

    def build_for_df(solo_df: pd.DataFrame, solo_type: str) -> None:
        commitment_values = ["COMMITTED", "ONE_SHOT", "OTHER_TC"]
        timing_values = ["INSTANT", "QUICK", "LATE", "UNKNOWN"]
        profit_values = ["SOL_ONLY", "MIXED"]

        add_slice(solo_df, solo_type, "ALL", "ALL", "ALL")

        for timing in timing_values:
            slice_timing = solo_df[solo_df["timing_bucket"] == timing]
            add_slice(slice_timing, solo_type, "ALL", timing, "ALL")

            for commitment in commitment_values:
                slice_timing_commit = slice_timing[
                    slice_timing["commitment_bucket"] == commitment
                ]
                add_slice(slice_timing_commit, solo_type, commitment, timing, "ALL")

        for commitment in commitment_values:
            slice_commit = solo_df[solo_df["commitment_bucket"] == commitment]
            add_slice(slice_commit, solo_type, commitment, "ALL", "ALL")

        for timing in timing_values:
            for commitment in commitment_values:
                for profit_source in profit_values:
                    slice_full = solo_df[
                        (solo_df["timing_bucket"] == timing)
                        & (solo_df["commitment_bucket"] == commitment)
                        & (solo_df["profit_source_bucket"] == profit_source)
                    ]
                    add_slice(slice_full, solo_type, commitment, timing, profit_source)

    build_for_df(df_solo, "SOLO_ALL")
    build_for_df(df_true_solo, "SOLO_TRUE")

    return pd.DataFrame(slices, columns=SOLO_SLICE_HEADER)


def build_e_token_specialist_slices(df: pd.DataFrame) -> pd.DataFrame:
    df_token = df[df["archetype"] == "E_TOKEN_SPECIALIST"].copy()
    if df_token.empty:
        return pd.DataFrame(columns=TOKEN_SLICE_HEADER)

    delta_entry = df_token["first_buy_time"] - df_token["first_seen_time"]
    df_token["timing_bucket"] = np.select(
        [
            delta_entry <= 60,
            (delta_entry > 60) & (delta_entry <= 300),
            delta_entry > 300,
            delta_entry.isna(),
        ],
        ["EARLY", "MID", "LATE", "UNKNOWN"],
        default="UNKNOWN",
    )
    df_token["speed_bucket"] = np.where(df_token["cohort_60s"] >= 3, "FAST", "SLOW")
    df_token["commitment_bucket"] = np.select(
        [df_token["trade_count"] >= 2, df_token["trade_count"] == 1],
        ["COMMITTED", "ONE_SHOT"],
        default="OTHER_TC",
    )

    slices = []

    def add_slice(slice_df: pd.DataFrame, timing: str, speed: str, commitment: str) -> None:
        n = int(len(slice_df))
        win_rate = float((slice_df["realized_profit_sol"] > 0).mean()) if n else float("nan")
        median_profit = median_or_nan(slice_df["profit_capped"])
        early_median = median_or_nan(
            slice_df.loc[slice_df["half"] == "EARLY", "profit_capped"]
        )
        late_median = median_or_nan(
            slice_df.loc[slice_df["half"] == "LATE", "profit_capped"]
        )
        p25_profit = float(slice_df["profit_capped"].quantile(0.25)) if n else float("nan")
        p75_profit = float(slice_df["profit_capped"].quantile(0.75)) if n else float("nan")
        survives_slice = (
            n >= 150
            and median_profit > 0
            and early_median > 0
            and late_median > 0
        )
        slices.append(
            {
                "slice_id": f"E|{timing}|{speed}|{commitment}",
                "timing_bucket": timing,
                "speed_bucket": speed,
                "commitment_bucket": commitment,
                "n": n,
                "win_rate": win_rate,
                "median_profit": median_profit,
                "early_median_profit": early_median,
                "late_median_profit": late_median,
                "p25_profit": p25_profit,
                "p75_profit": p75_profit,
                "survives_slice": bool(survives_slice),
            }
        )

    timing_values = ["EARLY", "MID", "LATE", "UNKNOWN"]
    speed_values = ["FAST", "SLOW"]
    commitment_values = ["COMMITTED", "ONE_SHOT", "OTHER_TC"]

    for timing in timing_values:
        slice_timing = df_token[df_token["timing_bucket"] == timing]
        add_slice(slice_timing, timing, "ALL", "ALL")

        for speed in speed_values:
            slice_timing_speed = slice_timing[slice_timing["speed_bucket"] == speed]
            add_slice(slice_timing_speed, timing, speed, "ALL")

        for commitment in commitment_values:
            slice_timing_commit = slice_timing[
                slice_timing["commitment_bucket"] == commitment
            ]
            add_slice(slice_timing_commit, timing, "ALL", commitment)

            for speed in speed_values:
                slice_full = slice_timing_commit[
                    slice_timing_commit["speed_bucket"] == speed
                ]
                add_slice(slice_full, timing, speed, commitment)

    return pd.DataFrame(slices, columns=TOKEN_SLICE_HEADER)


def write_baseline_stats(
    outdir: Path,
    stats: dict,
) -> None:
    baseline_path = outdir / "baseline_stats.json"
    with baseline_path.open("w", encoding="utf-8") as handle:
        json.dump(stats, handle, indent=2)


def write_archetype_text(outdir: Path, baseline_stats: dict, stats: pd.DataFrame) -> None:
    lines = []
    lines.append("Baseline:")
    for key, value in baseline_stats.items():
        lines.append(f"  {key}: {value}")
    lines.append("")
    lines.append("Archetypes (sorted by EdgeScore desc):")
    sorted_stats = stats.sort_values(by="edgescore", ascending=False)
    for _, row in sorted_stats.iterrows():
        tags = []
        if row["survives"]:
            tags.append("SURVIVES")
        if row["anti_pattern"]:
            tags.append("ANTI_PATTERN")
        tag_str = f" [{' '.join(tags)}]" if tags else ""
        lines.append(
            f"  {row['archetype']}: n={row['n']} win_rate={row['win_rate']:.4f} "
            f"median={row['median_profit']:.4f} EdgeScore={row['edgescore']:.4f}{tag_str}"
        )
    text_path = outdir / "archetype_stats.txt"
    text_path.write_text("\n".join(lines), encoding="utf-8")


def write_c_elite_swarm_slices(outdir: Path, slices: pd.DataFrame) -> None:
    slice_path = outdir / "c_elite_swarm_slices.tsv"
    slices.to_csv(slice_path, sep="\t", index=False)

    text_path = outdir / "c_elite_swarm_slices.txt"
    if slices.empty:
        text_path.write_text("", encoding="utf-8")
        return

    sorted_slices = slices.sort_values(
        by=["median_profit", "n"], ascending=[False, False]
    )
    lines = []
    for _, row in sorted_slices.iterrows():
        lines.append(
            f"{row['slice_id']}: n={row['n']} win_rate={row['win_rate']:.4f} "
            f"median={row['median_profit']:.4f} "
            f"early={row['early_median_profit']:.4f} "
            f"late={row['late_median_profit']:.4f} "
            f"p25={row['p25_profit']:.4f} p75={row['p75_profit']:.4f} "
            f"survives={row['survives_slice']}"
        )
    text_path.write_text("\n".join(lines), encoding="utf-8")


def write_a_elite_solo_slices(outdir: Path, slices: pd.DataFrame) -> None:
    slice_path = outdir / "a_elite_solo_slices.tsv"
    slices.to_csv(slice_path, sep="\t", index=False)

    text_path = outdir / "a_elite_solo_slices.txt"
    if slices.empty:
        text_path.write_text("", encoding="utf-8")
        return

    sorted_slices = slices.sort_values(
        by=["median_profit", "n"], ascending=[False, False]
    )
    lines = []
    for _, row in sorted_slices.iterrows():
        lines.append(
            f"{row['slice_id']}: n={row['n']} win_rate={row['win_rate']:.4f} "
            f"median={row['median_profit']:.4f} "
            f"early={row['early_median_profit']:.4f} "
            f"late={row['late_median_profit']:.4f} "
            f"p25={row['p25_profit']:.4f} p75={row['p75_profit']:.4f} "
            f"survives={row['survives_slice']}"
        )
    text_path.write_text("\n".join(lines), encoding="utf-8")


def write_e_token_specialist_slices(outdir: Path, slices: pd.DataFrame) -> None:
    slice_path = outdir / "e_token_specialist_slices.tsv"
    slices.to_csv(slice_path, sep="\t", index=False)

    text_path = outdir / "e_token_specialist_slices.txt"
    if slices.empty:
        text_path.write_text("", encoding="utf-8")
        return

    sorted_slices = slices.sort_values(
        by=["median_profit", "n"], ascending=[False, False]
    )
    lines = []
    for _, row in sorted_slices.iterrows():
        lines.append(
            f"{row['slice_id']}: n={row['n']} win_rate={row['win_rate']:.4f} "
            f"median={row['median_profit']:.4f} "
            f"early={row['early_median_profit']:.4f} "
            f"late={row['late_median_profit']:.4f} "
            f"p25={row['p25_profit']:.4f} p75={row['p75_profit']:.4f} "
            f"survives={row['survives_slice']}"
        )
    text_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df = load_tsv(args.tsv)
    total_rows_read = len(df)
    enforce_columns(df)

    df = coerce_numeric(df)
    df["wallet_address"] = df["wallet_address"].astype(str)
    df["token"] = df["token"].astype(str)

    df, dropped_nan_count = drop_nan_rows(df)

    df["cohort_300s"] = df["cohort_300s"].fillna(0).astype(int)
    df["cohort_60s"] = df["cohort_60s"].fillna(0).astype(int)
    df["positive_total_wallet_profit_cohort_300s"] = (
        df["positive_total_wallet_profit_cohort_300s"].fillna(0).astype(int)
    )
    df["positive_token_profit_cohort_300s"] = (
        df["positive_token_profit_cohort_300s"].fillna(0).astype(int)
    )

    df, excluded_absurd_count = apply_absurd_filters(
        df,
        args.absurd_tsv,
        args.absurd_note_substr.strip(),
        args.verbose,
    )

    if df.empty:
        print("ERROR: empty dataset after filters")
        sys.exit(4)

    cap_value = float(df["realized_profit_sol"].abs().quantile(args.cap_pct))
    df["profit_capped"] = df["realized_profit_sol"].clip(-cap_value, cap_value)

    n_total = int(len(df))
    baseline_win_rate = float((df["realized_profit_sol"] > 0).mean())
    baseline_median_profit = float(df["profit_capped"].median())
    t_median = float(df["first_seen_time"].median())
    df["half"] = np.where(df["first_seen_time"] <= t_median, "EARLY", "LATE")
    baseline_early_median_profit = float(
        df.loc[df["half"] == "EARLY", "profit_capped"].median()
    )
    baseline_late_median_profit = float(
        df.loc[df["half"] == "LATE", "profit_capped"].median()
    )

    s_p50 = float(df["wallet_total_profit_sol"].quantile(0.5))
    s_p90 = float(df["wallet_total_profit_sol"].quantile(0.9))

    df["skill_tier"] = assign_skill_tier(df, s_p50, s_p90)
    df["density_tier"] = assign_density_tier(df)

    df["cohort_wallet_skill_ratio"] = (
        df["positive_total_wallet_profit_cohort_300s"]
        / df["cohort_300s"].clip(lower=1)
    )
    df["cohort_token_skill_ratio"] = (
        df["positive_token_profit_cohort_300s"] / df["cohort_300s"].clip(lower=1)
    )

    df["q_wallet"] = assign_quality_tier(df["cohort_wallet_skill_ratio"])
    df["q_token"] = assign_quality_tier(df["cohort_token_skill_ratio"])

    df["archetype"] = assign_archetype(
        df["skill_tier"], df["density_tier"], df["q_wallet"], df["q_token"]
    )

    stats = build_archetype_stats(df, baseline_win_rate)
    stats["survives"] = (
        (stats["n"] >= args.n_min)
        & (stats["median_profit"] > 0)
        & (stats["early_median_profit"] > 0)
        & (stats["late_median_profit"] > 0)
        & (stats["win_rate"] >= baseline_win_rate + args.delta_win)
    )
    stats["anti_pattern"] = (
        (stats["n"] >= args.n_min)
        & (stats["median_profit"] < 0)
        & (stats["early_median_profit"] < 0)
        & (stats["late_median_profit"] < 0)
    )

    stats = stats.set_index("archetype").loc[ARCHETYPES].reset_index()

    baseline_stats = {
        "n_total": n_total,
        "baseline_win_rate": baseline_win_rate,
        "baseline_median_profit": baseline_median_profit,
        "baseline_early_median_profit": baseline_early_median_profit,
        "baseline_late_median_profit": baseline_late_median_profit,
        "T_MEDIAN": t_median,
        "S_P50": s_p50,
        "S_P90": s_p90,
        "cap_pct": args.cap_pct,
        "cap_value_P": cap_value,
        "excluded_absurd_count": excluded_absurd_count,
        "dropped_nan_count": dropped_nan_count,
    }

    write_baseline_stats(outdir, baseline_stats)

    archetype_tsv_path = outdir / "archetype_stats.tsv"
    stats.to_csv(archetype_tsv_path, sep="\t", index=False)

    write_archetype_text(outdir, baseline_stats, stats)

    slices = build_c_elite_swarm_slices(df)
    write_c_elite_swarm_slices(outdir, slices)

    solo_slices = build_a_elite_solo_slices(df)
    write_a_elite_solo_slices(outdir, solo_slices)

    token_slices = build_e_token_specialist_slices(df)
    write_e_token_specialist_slices(outdir, token_slices)

    sample_cols = [
        "wallet_address",
        "token",
        "realized_profit_sol",
        "profit_capped",
        "wallet_total_profit_sol",
        "skill_tier",
        "cohort_300s",
        "density_tier",
        "cohort_wallet_skill_ratio",
        "q_wallet",
        "cohort_token_skill_ratio",
        "q_token",
        "half",
        "archetype",
    ]
    sample_path = outdir / "situations_with_archetype_sample.tsv"
    df[sample_cols].head(200).to_csv(sample_path, sep="\t", index=False)

    top3 = stats.sort_values(by="edgescore", ascending=False).head(3)
    anti_patterns = stats[stats["anti_pattern"]]

    print("Step 2 validation summary")
    print(
        f"total rows read: {total_rows_read} | "
        f"dropped_nan_count: {dropped_nan_count} | "
        f"excluded_absurd_count: {excluded_absurd_count} | "
        f"final rows used: {len(df)}"
    )
    print(
        "baseline win/med: "
        f"{baseline_win_rate:.4f} / {baseline_median_profit:.4f}"
    )
    print("top 3 archetypes by EdgeScore:")
    for _, row in top3.iterrows():
        print(
            f"  {row['archetype']}: n={row['n']} win_rate={row['win_rate']:.4f} "
            f"median={row['median_profit']:.4f} EdgeScore={row['edgescore']:.4f}"
        )
    if not anti_patterns.empty:
        print("anti_pattern archetypes:")
        for _, row in anti_patterns.iterrows():
            print(f"  {row['archetype']}")

    if slices.empty:
        print("C_ELITE_SWARM: no rows, skipping slices")
    else:
        top5_slices = slices.sort_values(
            by=["median_profit", "n"], ascending=[False, False]
        ).head(5)
        print("C_ELITE_SWARM conditional slices (top 5 by median_profit):")
        for _, row in top5_slices.iterrows():
            print(
                f"  {row['speed_bucket']} | {row['skill_bucket']} | "
                f"{row['commitment_bucket']} | n={row['n']} "
                f"win_rate={row['win_rate']:.4f} "
                f"median_profit={row['median_profit']:.4f} "
                f"survives_slice={row['survives_slice']}"
            )

    if solo_slices.empty:
        print("A_ELITE_SOLO: no rows, skipping slices")
    else:
        top5_solo = solo_slices.sort_values(
            by=["median_profit", "n"], ascending=[False, False]
        ).head(5)
        print("A_ELITE_SOLO conditional slices (top 5 by median_profit):")
        for _, row in top5_solo.iterrows():
            print(
                f"  {row['solo_type']} | {row['timing_bucket']} | "
                f"{row['commitment_bucket']} | {row['profit_source_bucket']} | "
                f"n={row['n']} win_rate={row['win_rate']:.4f} "
                f"median_profit={row['median_profit']:.4f} "
                f"survives_slice={row['survives_slice']}"
            )

    if token_slices.empty:
        print("E_TOKEN_SPECIALIST: no rows")
    else:
        top5_token = token_slices.sort_values(
            by=["median_profit", "n"], ascending=[False, False]
        ).head(5)
        print("E_TOKEN_SPECIALIST conditional slices (top 5 by median_profit):")
        for _, row in top5_token.iterrows():
            print(
                f"  {row['timing_bucket']} | {row['speed_bucket']} | "
                f"{row['commitment_bucket']} | n={row['n']} "
                f"win_rate={row['win_rate']:.4f} "
                f"median_profit={row['median_profit']:.4f} "
                f"survives_slice={row['survives_slice']}"
            )


if __name__ == "__main__":
    main()
