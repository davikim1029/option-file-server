import sqlite3

def get_conn(self) -> sqlite3.Connection:
    """
    Return a connection tuned for concurrency and reasonable timeouts.
    - sets WAL
    - sets busy_timeout (in milliseconds)
    - returns a connection object to be used for short-lived transactions
    """
    self.db_path.parent.mkdir(parents=True, exist_ok=True)
    # keep a generous timeout so connection attempts wait rather than error
    conn = sqlite3.connect(str(self.db_path), timeout=30.0, isolation_level=None)
    try:
        # enable WAL and set busy timeout (30s)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=30000;")  # milliseconds
        conn.execute("PRAGMA foreign_keys=ON;")
    except Exception:
        # ignore on constrained SQLite builds
        pass
    return conn
