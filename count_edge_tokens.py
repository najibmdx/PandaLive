import pandas as pd

df = pd.read_csv("profit_situations_all.tsv", sep="\t")

# safety coercions
df["cohort_60s"] = pd.to_numeric(df["cohort_60s"], errors="coerce").fillna(0)
df["q_wallet"] = df["q_wallet"].astype(str)
df["archetype"] = df["archetype"].astype(str)
df["token"] = df["token"].astype(str)

# FAST MIXED SKILL SWARM condition
edge_df = df[
    (df["archetype"] == "C_ELITE_SWARM") &
    (df["cohort_60s"] >= 3) &
    (df["q_wallet"].isin(["Q2", "Q3"])) &
    (df["skill_bucket"] == "MIXED")
]

edge_tokens = edge_df["token"].unique()

print("Number of tokens where edge occurred:", len(edge_tokens))
