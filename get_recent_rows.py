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

    conn.close()
    return rows

if __name__ == "__main__":
    last_rows = get_last_30_rows(DB_PATH)
    if last_rows:
        for row in last_rows:
            print(row)
    else:
        print("No rows found.")
