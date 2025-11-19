# processors/permutation_processor.py
"""
OptionPermutationProcessor
- Reads completed option lifetimes and generates permutations (buy at i, sell at j>i).
- Uses short-lived per-OSI DB connections and transactions with BEGIN IMMEDIATE.
- Avoids VACUUM during normal operation. Uses ANALYZE occasionally (safe).
"""

import sqlite3
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Tuple

from shared_options.log.logger_singleton import getLogger

logger = getLogger()

# Configuration defaults (adjust when instantiating)
DEFAULT_BATCH_COMMIT_SIZE = 50
DEFAULT_CHECK_INTERVAL = 60
DEFAULT_ANALYZE_AFTER_COMMITS = 1
DEFAULT_VACUUM_INTERVAL_HOURS = 24
DEFAULT_INSERT_RETRIES = 5
DEFAULT_INSERT_RETRY_BACKOFF = 0.1  # base seconds

_RESERVED_COLUMNS = {"osiKey", "timestamp", "buy_timestamp", "sell_timestamp", "processed"}


def _normalize_sql_type(t: Optional[str]) -> str:
    if not t:
        return "REAL"
    tt = t.strip().upper()
    if "INT" in tt:
        return "INTEGER"
    if "CHAR" in tt or "CLOB" in tt or "TEXT" in tt:
        return "TEXT"
    if "REAL" in tt or "FLOA" in tt or "DOUB" in tt:
        return "REAL"
    return "REAL"


class OptionPermutationProcessor:
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
        self.check_interval = int(check_interval)
        self.batch_commit_size = int(batch_commit_size)
        self.analyze_after_commits = max(1, int(analyze_after_commits))
        self.vacuum_interval = timedelta(hours=int(vacuum_interval_hours))
        self.insert_retries = max(1, int(insert_retries))

        self.running = False
        self.thread: Optional[threading.Thread] = None

        # metrics
        self._total_rows_inserted = 0
        self._total_osis_processed = 0
        self._last_run: Optional[datetime] = None
        self._last_error: Optional[str] = None
        self._last_vacuum: Optional[datetime] = None
        self._commit_count = 0

        # load snapshot schema (reads lifetime table schema)
        self.snapshot_schema: List[Tuple[str, str]] = self._load_snapshot_schema()
        self.snapshot_columns: List[str] = [c for c, _ in self.snapshot_schema]
        self.numeric_columns: List[str] = [c for c, t in self.snapshot_schema if _normalize_sql_type(t) in ("REAL", "INTEGER")]

        # init permutation table (add missing cols)
        self._init_perm_table()

        logger.logMessage("[Permutation] Processor initialized.")

    # -------------------------
    # DB helper (short-lived connections only)
    # -------------------------
    def _get_conn(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path), timeout=30.0, isolation_level=None)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout=30000;")
            conn.execute("PRAGMA foreign_keys=ON;")
        except Exception:
            pass
        return conn

    def _load_snapshot_schema(self) -> List[Tuple[str, str]]:
        conn = self._get_conn()
        cur = conn.cursor()
        schema = []
        try:
            cur.execute(f"PRAGMA table_info({self.LIFETIME_TABLE});")
            rows = cur.fetchall()
            if rows:
                schema = [(r[1], r[2]) for r in rows]
        except sqlite3.OperationalError:
            schema = []
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if not schema:
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

    # -------------------------
    # create/alter perm table
    # -------------------------
    def _init_perm_table(self):
        conn = self._get_conn()
        cur = conn.cursor()

        dynamic_cols = [col for col, _ in self.snapshot_schema if col not in _RESERVED_COLUMNS]

        buy_sell_defs = []
        for col, col_type in self.snapshot_schema:
            if col in _RESERVED_COLUMNS:
                continue
            sql_type = _normalize_sql_type(col_type)
            buy_sell_defs.append((f"buy_{col}", sql_type))
            buy_sell_defs.append((f"sell_{col}", sql_type))

        delta_defs = []
        for col, col_type in self.snapshot_schema:
            if col in _RESERVED_COLUMNS:
                continue
            if _normalize_sql_type(col_type) in ("REAL", "INTEGER"):
                delta_defs.append((f"delta_{col}", "REAL"))

        computed_defs = [("hold_seconds", "REAL"), ("profit", "REAL"), ("return_pct", "REAL")]

        desired_columns: Dict[str, str] = {"osiKey": "TEXT", "buy_timestamp": "TEXT", "sell_timestamp": "TEXT"}
        for name, t in buy_sell_defs + delta_defs + computed_defs:
            desired_columns[name] = t

        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (self.PERM_TABLE,))
        exists = cur.fetchone() is not None

        if not exists:
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

        cur.execute(f"PRAGMA table_info({self.PERM_TABLE});")
        existing = {r[1]: r[2] for r in cur.fetchall()}

        to_add = []
        for col, typ in desired_columns.items():
            if col not in existing:
                to_add.append((col, typ))

        for col, typ in to_add:
            try:
                cur.execute(f"ALTER TABLE {self.PERM_TABLE} ADD COLUMN {col} {typ};")
            except sqlite3.OperationalError as e:
                logger.logMessage(f"[Permutation] ALTER TABLE failed for {col}: {e}")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.PERM_TABLE}_osi ON {self.PERM_TABLE}(osiKey);")
        conn.commit()
        conn.close()
        if to_add:
            logger.logMessage(f"[Permutation] Added {len(to_add)} missing columns to {self.PERM_TABLE}.")

    # -------------------------
    # lifecycle
    # -------------------------
    def start(self):
        if self.running:
            logger.logMessage("[Permutation] Processor already running.")
            return
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True, name="OptionPermutationProcessor")
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

    # -------------------------
    # main loop
    # -------------------------
    def _loop(self):
        while self.running:
            try:
                self._last_run = datetime.utcnow()
                self._process_batch()
            except Exception as e:
                self._last_error = str(e)
                logger.logMessage(f"[Permutation] Loop error: {e}")
            time.sleep(self.check_interval)

    # -------------------------
    # fetch & process batches
    # -------------------------
    def _fetch_osi_batch(self, conn) -> List[str]:
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT osiKey FROM {self.LIFETIME_TABLE} LIMIT ?;", (self.batch_commit_size,))
        return [r[0] for r in cur.fetchall()]

    def _process_batch(self):
        conn = None
        try:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            osi_batch = self._fetch_osi_batch(conn)
            if not osi_batch:
                return

            for osi in osi_batch:
                try:
                    self._process_single_osi(osi)
                    self._total_osis_processed += 1
                except Exception as e:
                    self._last_error = str(e)
                    logger.logMessage(f"[Permutation] Error processing OSI={osi}: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _process_single_osi(self, osi: str):
        # Use a fresh connection for the entire per-OSI work
        conn = self._get_conn()
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM {self.LIFETIME_TABLE} WHERE osiKey = ? ORDER BY timestamp ASC;", (osi,))
            raw_snaps = cur.fetchall()
            snaps = [dict(r) for r in raw_snaps]

            if len(snaps) < 2:
                # nothing useful — remove to keep DB clean in short transaction
                try:
                    conn.execute("BEGIN IMMEDIATE;")
                    cur.execute(f"DELETE FROM {self.LIFETIME_TABLE} WHERE osiKey = ?;", (osi,))
                    conn.execute("COMMIT;")
                except Exception:
                    try:
                        conn.execute("ROLLBACK;")
                    except Exception:
                        pass
                return

            rows_to_insert: List[Dict] = []
            for i in range(len(snaps) - 1):
                buy = snaps[i]
                for j in range(i + 1, len(snaps)):
                    sell = snaps[j]
                    rows_to_insert.append(self._build_perm_row(osi, buy, sell))

            if rows_to_insert:
                self._insert_with_retries(conn, rows_to_insert)

            # delete consumed lifetimes
            try:
                conn.execute("BEGIN IMMEDIATE;")
                cur.execute(f"DELETE FROM {self.LIFETIME_TABLE} WHERE osiKey = ?;", (osi,))
                conn.execute("COMMIT;")
            except Exception:
                try:
                    conn.execute("ROLLBACK;")
                except Exception:
                    pass

        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _build_perm_row(self, osi: str, buy_row: Dict, sell_row: Dict) -> Dict:
        buy_ts = buy_row.get("timestamp")
        sell_ts = sell_row.get("timestamp")
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

        for col, _ in self.snapshot_schema:
            if col in _RESERVED_COLUMNS:
                continue
            row[f"buy_{col}"] = buy_row.get(col)
            row[f"sell_{col}"] = sell_row.get(col)

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
        if not rows:
            return

        columns = list(rows[0].keys())
        placeholders = ",".join(["?"] * len(columns))
        sql = f"INSERT OR REPLACE INTO {self.PERM_TABLE} ({','.join(columns)}) VALUES ({placeholders});"
        values = [tuple(row.get(c) for c in columns) for row in rows]

        attempt = 0
        while attempt < self.insert_retries:
            try:
                # Acquire write reservation early to avoid mid-commit failures
                conn.execute("BEGIN IMMEDIATE;")
                conn.executemany(sql, values)
                conn.execute("COMMIT;")
                self._total_rows_inserted += len(values)
                self._commit_count += 1

                # ANALYZE occasionally (cheap)
                if (self._commit_count % self.analyze_after_commits) == 0:
                    try:
                        conn.execute("ANALYZE;")
                    except Exception:
                        pass

                # NOTE: VACUUM is intentionally removed from normal path.
                # If you need VACUUM, run it from a maintenance script while server is down.

                return
            except sqlite3.OperationalError as e:
                try:
                    conn.execute("ROLLBACK;")
                except Exception:
                    pass
                attempt += 1
                sleep_time = DEFAULT_INSERT_RETRY_BACKOFF * attempt
                logger.logMessage(f"[Permutation] SQLite OperationalError (attempt {attempt}) in thread={threading.current_thread().name}: {e}; retrying in {sleep_time:.2f}s")
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
