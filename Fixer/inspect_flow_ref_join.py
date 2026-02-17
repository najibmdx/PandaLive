import sqlite3

db = "masterwalletsdb.db"
conn = sqlite3.connect(db)
cur = conn.cursor()

tables = [r[0] for r in cur.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
).fetchall()]

def table_info(t):
    return cur.execute(f"PRAGMA table_info({t});").fetchall()

flow_ref_tables = []
mintish_tables = []

for t in tables:
    cols = [c[1].lower() for c in table_info(t)]
    if "flow_ref" in cols:
        flow_ref_tables.append((t, cols))
    if "mint" in cols or "token_mint" in cols or "mint_address" in cols:
        mintish_tables.append((t, cols))

print("=== TABLES WITH flow_ref ===")
for t, cols in flow_ref_tables:
    print(t)

print("\n=== TABLES WITH mint-ish column ===")
for t, cols in mintish_tables:
    mint_cols = [c for c in cols if "mint" in c]
    print(t, "->", mint_cols)

# Also check if any table has BOTH flow_ref and mint-ish
print("\n=== TABLES WITH BOTH flow_ref AND mint-ish ===")
both = 0
for t, cols in flow_ref_tables:
    mint_cols = [c for c in cols if "mint" in c]
    if mint_cols:
        both += 1
        print(t, "->", mint_cols)
if both == 0:
    print("(none)")

conn.close()
