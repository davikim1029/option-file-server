# processors/permutation_processor.py
"""
Responsibilities:
- Read completed option lifetimes from `option_lifetimes`
- Generate ALL buy/sell permutations per OSI (buy at snapshot i, sell at j>i)
- Produce rows with:
    - osiKey, buy_timestamp, sell_timestamp
    - buy_<col>, sell_<col> for non-reserved columns
    - delta_<numeric_col> for numeric columns
    - hold_seconds, profit, return_pct
- Insert into `option_permutations` with resilient retry logic
- Safe for large datasets (batching by OSI)
- Periodic ANALYZE and occasional VACUUM (skip if busy)
- Avoid duplicate column creation by inferring existing schema and adding missing columns only
- Robust logging with status counters

"""

import sqlite3
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from shared_options.log.logger_singleton import getLogger

# -------------------------------------------------------
# Configuration defaults (tune as needed at instantiation)
# -------------------------------------------------------
DEFAULT_BATCH_COMMIT_SIZE = 50
DEFAULT_CHECK_INTERVAL = 60
DEFAULT_ANALYZE_AFTER_COMMITS = 1
DEFAULT_VACUUM_INTERVAL_HOURS = 24
DEFAULT_INSERT_RETRIES = 5
DEFAULT_INSERT_RETRY_BACKOFF = 0.1  # base seconds

# Reserved snapshot columns we do NOT want duplicated as buy_/sell_ columns
_RESERVED_COLUMNS = {"osiKey", "timestamp", "buy_timestamp", "sell_timestamp", "processed"}

logger = getLogger()


def _normalize_sql_type(t: Optional[str]) -> str:
    """Map PRAGMA type string to one of: INTEGER, REAL, TEXT"""
    if not t:
        return "REAL"
    tt = t.strip().upper()
    if "INT" in tt:
        return "INTEGER"
    if "CHAR" in tt or "CLOB" in tt or "TEXT" in tt:
        return "TEXT"
    # numeric affinity fallback
    if "REAL" in tt or "FLOA" in tt or "DOUB" in tt:
        return "REAL"
    return "REAL"


class OptionPermutationProcessor:
    """
    See module docstring above for behavior.
    """

    LIFETIME_TABLE = "option_lifetimes"
    PERM_TABLE = "option_permutations"

    def __init__(
        self,
        db_path: str,
        check_interval: int = DEFAULT_CHECK_INTERVAL,
        batch_commit_size: int = DEFAULT_BATCH_COMMIT_SIZE,
        analyze_after_commits: int = DEFAULT_ANALYZE_AFTER_COMMITS,
        vacuum_interval_hours: int = DEFAULT_VACUUM_INTERVAL_HOURS,
        insert_retries: int = DEFAULT_INSERT_RETRIES,
    ):
        self.db_path = Path(db_path)
        self.check_interval = check_interval
        self.batch_commit_size = batch_commit_size
        self.analyze_after_commits = max(1, analyze_after_commits)
        self.vacuum_interval = timedelta(hours=vacuum_interval_hours)
        self.insert_retries = max(1, int(insert_retries))

        self.running = False
        self.thread: Optional[threading.Thread] = None

        # state counters
        self._total_rows_inserted = 0
        self._total_osis_processed = 0
        self._last_run: Optional[datetime] = None
        self._last_error: Optional[str] = None
        self._last_vacuum: Optional[datetime] = None
        self._commit_count = 0

        # load schema (column name + type) from lifetime table
        self.snapshot_schema: List[Tuple[str, str]] = self._load_snapshot_schema()
        # snapshot column names (preserve order)
        self.snapshot_columns: List[str] = [c for c, _ in self.snapshot_schema]
        # numeric columns for delta calculation
        self.numeric_columns: List[str] = [c for c, t in self.snapshot_schema if _normalize_sql_type(t) in ("REAL", "INTEGER")]

        # init DB (creates permutation table if missing or adds missing columns)
        self._init_perm_table()

        logger.logMessage("[Permutation] Processor initialized.")

    # ------------------------
    # DB helpers
    # ------------------------
    def _get_conn(self) -> sqlite3.Connection:
        """Return a connection tuned for concurrency and reasonable timeouts."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path), timeout=30.0, isolation_level=None)
        # Use WAL to reduce lock contention when reading + writing
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
        except Exception:
            pass
        return conn

    def _load_snapshot_schema(self) -> List[Tuple[str, str]]:
        """
        Inspect the lifetimes table and return list of (column_name, column_type).
        Falls back to sensible defaults if the table is not present.
        """
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()
        schema = []
        try:
            cur.execute(f"PRAGMA table_info({self.LIFETIME_TABLE});")
            rows = cur.fetchall()
            if rows:
                schema = [(r[1], r[2]) for r in rows]
        except sqlite3.OperationalError:
            # table might not exist yet
            schema = []
        finally:
            conn.close()

        if not schema:
            # sensible default schema (keeps parity with your earlier lists)
            schema = [
                ("osiKey", "TEXT"), ("timestamp", "TEXT"), ("symbol", "TEXT"), ("optionType", "INTEGER"),
                ("strikePrice", "REAL"), ("lastPrice", "REAL"), ("bid", "REAL"), ("ask", "REAL"),
                ("bidSize", "REAL"), ("askSize", "REAL"), ("volume", "REAL"), ("openInterest", "REAL"),
                ("nearPrice", "REAL"), ("inTheMoney", "INTEGER"), ("delta", "REAL"), ("gamma", "REAL"),
                ("theta", "REAL"), ("vega", "REAL"), ("rho", "REAL"), ("iv", "REAL"),
                ("daysToExpiration", "REAL"), ("spread", "REAL"), ("midPrice", "REAL"), ("moneyness", "REAL"),
                ("processed", "INTEGER")
            ]
        return schema

    # ------------------------
    # Permutation table management
    # ------------------------
    def _init_perm_table(self):
        """
        Create permutation table if missing. If table exists, ensure missing columns are added.
        This avoids 'duplicate column' issues and preserves historical permutations.
        """
        conn = self._get_conn()
        cur = conn.cursor()

        # Gather desired columns (names and types)
        dynamic_cols = [col for col, _ in self.snapshot_schema if col not in _RESERVED_COLUMNS]
        # buy/sell typed columns: use same affinity as source column
        buy_sell_defs = []
        for col, col_type in self.snapshot_schema:
            if col in _RESERVED_COLUMNS:
                continue
            sql_type = _normalize_sql_type(col_type)
            buy_sell_defs.append((f"buy_{col}", sql_type))
            buy_sell_defs.append((f"sell_{col}", sql_type))

        # delta columns for numeric columns
        delta_defs = []
        for col, col_type in self.snapshot_schema:
            if col in _RESERVED_COLUMNS:
                continue
            if _normalize_sql_type(col_type) in ("REAL", "INTEGER"):
                delta_defs.append((f"delta_{col}", "REAL"))

        # static computed metrics
        computed_defs = [("hold_seconds", "REAL"), ("profit", "REAL"), ("return_pct", "REAL")]

        # Build a map of desired column -> type
        desired_columns: Dict[str, str] = {"osiKey": "TEXT", "buy_timestamp": "TEXT", "sell_timestamp": "TEXT"}
        for name, t in buy_sell_defs + delta_defs + computed_defs:
            desired_columns[name] = t

        # Check if perm table exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (self.PERM_TABLE,))
        exists = cur.fetchone() is not None

        if not exists:
            # create table fresh
            defs_sql = ",\n    ".join([f"{col} {typ}" for col, typ in desired_columns.items()])
            ddl = f"""
                CREATE TABLE {self.PERM_TABLE} (
                    {defs_sql},
                    PRIMARY KEY (osiKey, buy_timestamp, sell_timestamp)
                );
            """
            cur.execute(ddl)
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.PERM_TABLE}_osi ON {self.PERM_TABLE}(osiKey);")
            conn.commit()
            conn.close()
            logger.logMessage("[Permutation] option_permutations table created.")
            return

        # If exists: inspect existing columns and add missing ones
        cur.execute(f"PRAGMA table_info({self.PERM_TABLE});")
        existing = {r[1]: r[2] for r in cur.fetchall()}  # name -> type

        # Add columns that are missing
        to_add = []
        for col, typ in desired_columns.items():
            if col not in existing:
                to_add.append((col, typ))

        for col, typ in to_add:
            try:
                cur.execute(f"ALTER TABLE {self.PERM_TABLE} ADD COLUMN {col} {typ};")
            except sqlite3.OperationalError as e:
                # If there's a race or the column now exists, ignore
                logger.logMessage(f"[Permutation] ALTER TABLE failed for {col}: {e}")
        # Ensure index exists
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.PERM_TABLE}_osi ON {self.PERM_TABLE}(osiKey);")
        conn.commit()
        conn.close()
        if to_add:
            logger.logMessage(f"[Permutation] Added {len(to_add)} missing columns to {self.PERM_TABLE}.")

    # ------------------------
    # Lifecycle control
    # ------------------------
    def start(self):
        if self.running:
            logger.logMessage("[Permutation] Processor already running.")
            return
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        logger.logMessage("[Permutation] Processor started.")

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=10)
        logger.logMessage("[Permutation] Processor stopped.")

    def get_status(self) -> Dict:
        return {
            "total_rows_inserted": int(self._total_rows_inserted),
            "total_osis_processed": int(self._total_osis_processed),
            "last_run": self._last_run.isoformat() if self._last_run else None,
            "last_error": str(self._last_error) if self._last_error else None,
            "last_vacuum": self._last_vacuum.isoformat() if self._last_vacuum else None,
            "commit_count": int(self._commit_count)
        }

    # ------------------------
    # Main loop
    # ------------------------
    def _loop(self):
        while self.running:
            try:
                self._last_run = datetime.utcnow()
                self._process_batch()
            except Exception as e:
                self._last_error = str(e)
                logger.logMessage(f"[Permutation] Loop error: {e}")
            time.sleep(self.check_interval)

    # ------------------------
    # Processing helpers
    # ------------------------
    def _fetch_osi_batch(self, conn) -> List[str]:
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT osiKey FROM {self.LIFETIME_TABLE} LIMIT ?;", (self.batch_commit_size,))
        return [r[0] for r in cur.fetchall()]

    def _process_batch(self):
        try:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            osi_batch = self._fetch_osi_batch(conn)
            if not osi_batch:
                return

            for osi in osi_batch:
                try:
                    self._process_single_osi(conn, osi)
                    self._total_osis_processed += 1
                except Exception as e:
                    self._last_error = str(e)
                    logger.logMessage(f"[Permutation] Error processing OSI={osi}: {e}")

        finally:
            try:
                conn.close()
            except Exception as e:
                logger.logMessage(f"Error Processing batch: {e}")
                pass

    def _process_single_osi(self, conn: sqlite3.Connection, osi: str):
        """
        Fetch snapshots for a single OSI from LIFETIME_TABLE (ordered by timestamp),
        build permutations, insert them, and delete the lifetime rows.
        """
        cur = conn.cursor()
        # ensure ordered snapshots; convert Row -> dict for safe .get usage
        cur.execute(f"SELECT * FROM {self.LIFETIME_TABLE} WHERE osiKey = ? ORDER BY timestamp ASC;", (osi,))
        raw_snaps = cur.fetchall()
        snaps = [dict(r) for r in raw_snaps]

        if len(snaps) < 2:
            # nothing useful — remove to keep DB clean
            try:
                conn.execute("BEGIN;")
                conn.execute(f"DELETE FROM {self.LIFETIME_TABLE} WHERE osiKey = ?;", (osi,))
                conn.execute("COMMIT;")
            except Exception:
                try:
                    conn.execute("ROLLBACK;")
                except Exception as e:
                    logger.logMessage(f"Error Processing single OSI: {e}")
                    pass
            return

        # Build rows (list of dict)
        rows_to_insert: List[Dict] = []
        for i in range(len(snaps) - 1):
            buy = snaps[i]
            for j in range(i + 1, len(snaps)):
                sell = snaps[j]
                rows_to_insert.append(self._build_perm_row(osi, buy, sell))

        # Insert and then delete lifetime rows atomically
        if rows_to_insert:
            self._insert_with_retries(conn, rows_to_insert)

        # delete consumed lifetimes
        try:
            conn.execute("BEGIN;")
            conn.execute(f"DELETE FROM {self.LIFETIME_TABLE} WHERE osiKey = ?;", (osi,))
            conn.execute("COMMIT;")
        except Exception:
            try:
                conn.execute("ROLLBACK;")
            except Exception as e:
                logger.logMessage(f"Error Processing single OSI: {e}")
                pass

    def _build_perm_row(self, osi: str, buy_row: Dict, sell_row: Dict) -> Dict:
        """Build a single permutation row (dict) from two snapshot dicts."""
        buy_ts = buy_row.get("timestamp")
        sell_ts = sell_row.get("timestamp")
        # parse ISO timestamps defensively
        try:
            dt_buy = datetime.fromisoformat(buy_ts)
            dt_sell = datetime.fromisoformat(sell_ts)
            hold_seconds = (dt_sell - dt_buy).total_seconds()
        except Exception:
            hold_seconds = 0.0

        def _to_float(v):
            try:
                return float(v) if v is not None else 0.0
            except Exception:
                return 0.0

        buy_price = _to_float(buy_row.get("lastPrice", 0.0))
        sell_price = _to_float(sell_row.get("lastPrice", 0.0))
        profit = sell_price - buy_price
        return_pct = profit / buy_price if buy_price else 0.0

        row: Dict = {
            "osiKey": osi,
            "buy_timestamp": buy_ts,
            "sell_timestamp": sell_ts,
            "hold_seconds": hold_seconds,
            "profit": profit,
            "return_pct": return_pct,
        }

        # add buy_/sell_ columns for dynamic (non-reserved) snapshot columns
        for col, _ in self.snapshot_schema:
            if col in _RESERVED_COLUMNS:
                continue
            row[f"buy_{col}"] = buy_row.get(col)
            row[f"sell_{col}"] = sell_row.get(col)

        # delta for numeric columns only
        for col in self.numeric_columns:
            if col in _RESERVED_COLUMNS:
                continue
            b = _to_float(buy_row.get(col))
            s = _to_float(sell_row.get(col))
            try:
                row[f"delta_{col}"] = float(s) - float(b)
            except Exception:
                row[f"delta_{col}"] = None

        return row

    def _insert_with_retries(self, conn: sqlite3.Connection, rows: List[Dict]):
        """
        Insert rows into PERM_TABLE using parameterized executemany.
        Retries on sqlite3.OperationalError with exponential backoff.
        """
        if not rows:
            return

        columns = list(rows[0].keys())
        placeholders = ",".join(["?"] * len(columns))
        sql = f"INSERT OR REPLACE INTO {self.PERM_TABLE} ({','.join(columns)}) VALUES ({placeholders});"
        values = [tuple(row.get(c) for c in columns) for row in rows]

        attempt = 0
        while attempt < self.insert_retries:
            try:
                conn.execute("BEGIN;")
                conn.executemany(sql, values)
                conn.execute("COMMIT;")
                self._total_rows_inserted += len(values)
                self._commit_count += 1

                # ANALYZE occasionally to keep planner stats fresh
                if (self._commit_count % self.analyze_after_commits) == 0:
                    try:
                        conn.execute("ANALYZE;")
                    except Exception:
                        pass

                # VACUUM rarely (skip if busy)
                if not self._last_vacuum or (datetime.utcnow() - self._last_vacuum) > self.vacuum_interval:
                    try:
                        conn.execute("VACUUM;")
                        self._last_vacuum = datetime.utcnow()
                    except Exception:
                        # skip if DB is busy or VACUUM can't acquire lock
                        pass

                return
            except sqlite3.OperationalError as e:
                # rollback if transaction left open
                try:
                    conn.execute("ROLLBACK;")
                except Exception:
                    pass
                attempt += 1
                sleep_time = DEFAULT_INSERT_RETRY_BACKOFF * attempt
                logger.logMessage(f"[Permutation] SQLite OperationalError (attempt {attempt}): {e}; retrying in {sleep_time:.2f}s")
                time.sleep(sleep_time)
            except Exception as e:
                try:
                    conn.execute("ROLLBACK;")
                except Exception:
                    pass
                self._last_error = str(e)
                logger.logMessage(f"[Permutation] Unexpected insert error: {e}")
                raise

        # If we fell out of loop, raise
        raise RuntimeError("Failed to insert permutation rows after retries.")
