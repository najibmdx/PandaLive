#!/usr/bin/env python3
"""
Quick diagnostic to inspect table schemas in masterwalletsdb.db
"""

import sqlite3
import sys

def inspect_tables(db_path):
    """Inspect and display table schemas."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]
        
        print("=" * 80)
        print("DATABASE TABLES")
        print("=" * 80)
        print(f"\nFound {len(tables)} tables: {', '.join(tables)}\n")
        
        # Inspect each relevant table
        for table_name in ['whale_events', 'wallet_token_flow']:
            if table_name not in tables:
                print(f"\n⚠️  Table '{table_name}' NOT FOUND")
                continue
            
            print("=" * 80)
            print(f"TABLE: {table_name}")
            print("=" * 80)
            
            # Get column info
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            print("\nColumns:")
            for col in columns:
                col_id, name, dtype, notnull, default, pk = col
                pk_str = " [PRIMARY KEY]" if pk else ""
                notnull_str = " NOT NULL" if notnull else ""
                default_str = f" DEFAULT {default}" if default else ""
                print(f"  {col_id}: {name:30s} {dtype:15s}{pk_str}{notnull_str}{default_str}")
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"\nRow count: {count:,}")
            
            # Show sample row
            if count > 0:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
                row = cursor.fetchone()
                print("\nSample row:")
                for i, col in enumerate(columns):
                    value = row[i] if i < len(row) else None
                    print(f"  {col[1]:30s} = {value}")
        
        conn.close()
        
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python inspect_db.py <database_path>")
        sys.exit(1)
    
    inspect_tables(sys.argv[1])
