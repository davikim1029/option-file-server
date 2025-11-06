#!/usr/bin/env python3
import sqlite3
from pathlib import Path
import sys
import textwrap

DB_PATH = Path("database/options.db")
SQL_PATH = Path("analytics/sql/")

def list_sql_files(sql_path: Path):
    """Return a sorted list of .sql files."""
    return sorted(sql_path.glob("*.sql"))

def display_menu(sql_files):
    """Display a menu of available SQL files."""
    print("\nAvailable SQL queries:\n")
    for i, f in enumerate(sql_files, 1):
        print(f"  {i}. {f.name}")
    print("  0. Exit\n")

def choose_query(sql_files):
    """Prompt user to select a query."""
    while True:
        try:
            choice = int(input("Select a query number to run: "))
            if choice == 0:
                sys.exit(0)
            if 1 <= choice <= len(sql_files):
                return sql_files[choice - 1]
            print("Invalid choice. Try again.")
        except ValueError:
            print("Please enter a number.")

def read_query(file_path: Path) -> str:
    """Read SQL content from file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read().strip()

def run_query(db_path: Path, query: str):
    """Run SQL query and return rows."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        if rows:
            print_table(rows)
        else:
            print("\n(No rows returned.)\n")
    except sqlite3.Error as e:
        print(f"\nError running query: {e}\n")
    finally:
        conn.close()

def print_table(rows):
    """Nicely format and print results."""
    headers = rows[0].keys()
    col_widths = [max(len(str(h)), max(len(str(row[h])) for row in rows)) for h in headers]

    print()
    print(" | ".join(h.ljust(w) for h, w in zip(headers, col_widths)))
    print("-+-".join("-" * w for w in col_widths))
    for row in rows:
        print(" | ".join(str(row[h]).ljust(w) for h, w in zip(headers, col_widths)))
    print()

def stats():
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        return

    if not SQL_PATH.exists():
        print(f"SQL path not found at {SQL_PATH}")
        return

    sql_files = list_sql_files(SQL_PATH)
    if not sql_files:
        print(f"No .sql files found in {SQL_PATH}")
        return

    while True:
        display_menu(sql_files)
        selected_file = choose_query(sql_files)
        print(f"\nRunning: {selected_file.name}")
        query = read_query(selected_file)
        print(f"\n{ textwrap.indent(query, '    ') }\n")
        confirm = input("Run this query? [y/N]: ").strip().lower()
        if confirm == 'y':
            run_query(DB_PATH, query)
        input("\nPress Enter to continue...")