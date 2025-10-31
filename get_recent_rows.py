import sqlite3
from pathlib import Path


DB_PATH = Path("database/options.db")

def get_last_30_rows(db_path: Path = None):
    if db_path is None:
        db_path=DB_PATH
    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
    SELECT *
    FROM option_snapshots
    ORDER BY timestamp DESC
    LIMIT 30
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            print(row)
    else:
        print("No rows found.")
