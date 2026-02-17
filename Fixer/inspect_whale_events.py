import sqlite3
import json

db = "masterwalletsdb.db"

conn = sqlite3.connect(db)
cur = conn.cursor()

result = {}

result["table_sql"] = cur.execute(
    "SELECT sql FROM sqlite_master WHERE type='table' AND name='whale_events';"
).fetchall()

result["columns"] = cur.execute(
    "PRAGMA table_info(whale_events);"
).fetchall()

result["row_count"] = cur.execute(
    "SELECT COUNT(*) FROM whale_events;"
).fetchone()[0]

result["sample_rows"] = cur.execute(
    "SELECT * FROM whale_events LIMIT 5;"
).fetchall()

print(json.dumps(result, indent=2))

conn.close()
