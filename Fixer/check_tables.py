import sqlite3

conn = sqlite3.connect("masterwalletsdb.db")
cur = conn.cursor()

tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print("TABLES:")
for t in tables:
    print("-", t[0])

conn.close()
