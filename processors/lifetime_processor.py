# processors/option_lifetime_processor.py
"""
OptionLifetimeProcessor
- Moves completed OSI snapshot groups from option_snapshots -> option_lifetimes.
- Uses WAL, busy_timeout, short-lived per-OSI transactions with BEGIN IMMEDIATE.
- Designed to minimize lock contention with brief transactions and retries.
"""

import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

from shared_options.log.logger_singleton import getLogger

logger = getLogger()

class LifetimeProcessorStatus:
    def __init__(self):
        self.last_run: Optional[datetime] = None
        self.last_processed_batch_size: int = 0
        self.total_archived: int = 0
        self.total_deleted_small: int = 0
        self.last_error: Optional[str] = None


class OptionLifetimeProcessor:
    SNAPSHOT_TABLE = "option_snapshots"
    LIFETIME_TABLE = "option_lifetimes"

    SNAPSHOT_COLUMNS = [
        "osiKey", "timestamp", "symbol", "optionType", "strikePrice", "lastPrice",
        "bid", "ask", "bidSize", "askSize", "volume", "openInterest", "nearPrice",
        "inTheMoney", "delta", "gamma", "theta", "vega", "rho", "iv", "daysToExpiration",
        "spread", "midPrice", "moneyness"
    ]

    def __init__(
        self,
        db_path: str,
        check_interval: int = 60,
        batch_size: int = 500,
        min_snapshots: int = 5,
        sqlite_timeout: float = 30.0,
        max_retries_per_osi: int = 3,
    ):
        self.db_path = Path(db_path)
        self.check_interval = int(check_interval)
        self.batch_size = int(batch_size)
        self.min_snapshots = int(min_snapshots)
        self.sqlite_timeout = float(sqlite_timeout)
        self.max_retries_per_osi = int(max_retries_per_osi)

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._status = LifetimeProcessorStatus()
        self._status_lock = threading.Lock()

        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._init_lifetime_table()
        self._ensure_indexes()

    # -------------------------
    # DB helper
    # -------------------------
    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=self.sqlite_timeout, isolation_level=None)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout=30000;")
        except Exception:
            pass
        return conn

    # -------------------------
    # init
    # -------------------------
    def _init_lifetime_table(self) -> None:
        try:
            conn = self._get_conn()
            c = conn.cursor()
            c.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.LIFETIME_TABLE} (
                    osiKey TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    symbol TEXT,
                    optionType INTEGER,
                    strikePrice REAL,
                    lastPrice REAL,
                    bid REAL,
                    ask REAL,
                    bidSize REAL,
                    askSize REAL,
                    volume REAL,
                    openInterest REAL,
                    nearPrice REAL,
                    inTheMoney INTEGER,
                    delta REAL,
                    gamma REAL,
                    theta REAL,
                    vega REAL,
                    rho REAL,
                    iv REAL,
                    daysToExpiration REAL,
                    spread REAL,
                    midPrice REAL,
                    moneyness REAL,
                    PRIMARY KEY (osiKey, timestamp)
                );
            """)
            c.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.LIFETIME_TABLE}_osi ON {self.LIFETIME_TABLE}(osiKey);")
            conn.commit()
            conn.close()
            logger.logMessage("[LifetimeProcessor] Lifetime table initialized.")
        except Exception as e:
            logger.logMessage("[LifetimeProcessor] Error initializing lifetime table:")
            logger.logMessage(str(e))
            with self._status_lock:
                self._status.last_error = str(e)

    def _ensure_indexes(self) -> None:
        try:
            conn = self._get_conn()
            c = conn.cursor()
            c.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.SNAPSHOT_TABLE}_osi ON {self.SNAPSHOT_TABLE}(osiKey);")
            c.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.SNAPSHOT_TABLE}_exp ON {self.SNAPSHOT_TABLE}(daysToExpiration);")
            c.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.SNAPSHOT_TABLE}_ts ON {self.SNAPSHOT_TABLE}(timestamp);")
            conn.commit()
            conn.close()
            logger.logMessage("[LifetimeProcessor] Snapshot indexes ensured.")
        except Exception as e:
            logger.logMessage("[LifetimeProcessor] Error ensuring indexes:")
            logger.logMessage(str(e))
            with self._status_lock:
                self._status.last_error = str(e)

    # -------------------------
    # lifecycle
    # -------------------------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            logger.logMessage("[LifetimeProcessor] Processor already running.")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="OptionLifetimeProcessor")
        self._thread.start()
        logger.logMessage("[LifetimeProcessor] Processor started.")

    def stop(self, join_timeout: float = 10.0) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=join_timeout)
        logger.logMessage("[LifetimeProcessor] Processor stopped.")

    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive() and not self._stop_event.is_set())

    def get_status(self) -> Dict[str, Any]:
        with self._status_lock:
            return {
                "last_run": self._status.last_run.isoformat() if self._status.last_run else None,
                "last_processed_batch_size": int(self._status.last_processed_batch_size),
                "total_archived": int(self._status.total_archived),
                "total_deleted_small": int(self._status.total_deleted_small),
                "last_error": self._status.last_error
            }

    # -------------------------
    # main loop
    # -------------------------
    def _run_loop(self) -> None:
        logger.logMessage("[LifetimeProcessor] Run loop entered.")
        while not self._stop_event.is_set():
            try:
                processed = self._process_one_batch()
                with self._status_lock:
                    self._status.last_run = datetime.utcnow()
                    self._status.last_processed_batch_size = processed
                if processed == 0:
                    time.sleep(self.check_interval)
                else:
                    time.sleep(0.1)
            except Exception as e:
                logger.logMessage(f"[LifetimeProcessor] Loop error: {e}")
                with self._status_lock:
                    self._status.last_error = str(e)
                time.sleep(5)
        logger.logMessage("[LifetimeProcessor] Run loop exited.")

    # -------------------------
    # batch processing (select candidates using short connection)
    # -------------------------
    def _process_one_batch(self) -> int:
        conn = self._get_conn()
        c = conn.cursor()
        try:
            c.execute(f"""
                SELECT osiKey
                FROM {self.SNAPSHOT_TABLE}
                GROUP BY osiKey
                HAVING MAX(daysToExpiration) <= 0
                LIMIT ?
            """, (self.batch_size,))
            rows = c.fetchall()
            osi_keys = [r[0] for r in rows]
        except sqlite3.OperationalError as e:
            logger.logMessage(f"[LifetimeProcessor] SQLite error selecting OSIs: {e}")
            conn.close()
            return 0
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if not osi_keys:
            return 0

        archived = 0
        deleted_small = 0

        placeholders = ", ".join(["?"] * len(self.SNAPSHOT_COLUMNS))
        insert_sql = f"INSERT OR REPLACE INTO {self.LIFETIME_TABLE} ({', '.join(self.SNAPSHOT_COLUMNS)}) VALUES ({placeholders})"

        for osi in osi_keys:
            attempts = 0
            success = False
            while attempts < self.max_retries_per_osi and not success:
                attempts += 1
                conn2 = None
                try:
                    conn2 = self._get_conn()
                    cur2 = conn2.cursor()
                    # fetch snapshots for this OSI
                    cur2.execute(f"""
                        SELECT {', '.join(self.SNAPSHOT_COLUMNS)}
                        FROM {self.SNAPSHOT_TABLE}
                        WHERE osiKey = ?
                        ORDER BY timestamp ASC
                    """, (osi,))
                    snaps = cur2.fetchall()

                    if not snaps:
                        success = True
                        break

                    if len(snaps) < self.min_snapshots:
                        # delete snapshots with too few rows -- short transaction
                        conn2.execute("BEGIN IMMEDIATE;")
                        cur2.execute(f"DELETE FROM {self.SNAPSHOT_TABLE} WHERE osiKey = ?", (osi,))
                        conn2.execute("COMMIT;")
                        deleted_small += 1
                        success = True
                        break

                    # Archive within a short transaction (INSERT then DELETE)
                    conn2.execute("BEGIN IMMEDIATE;")
                    cur2.executemany(insert_sql, snaps)
                    cur2.execute(f"DELETE FROM {self.SNAPSHOT_TABLE} WHERE osiKey = ?", (osi,))
                    conn2.execute("COMMIT;")
                    archived += 1
                    success = True

                except sqlite3.OperationalError as e:
                    try:
                        if conn2:
                            conn2.execute("ROLLBACK;")
                    except Exception:
                        pass
                    logger.logMessage(f"[LifetimeProcessor] OperationalError osi={osi}: {e} (attempt {attempts})")
                    time.sleep(0.1 * attempts)
                except Exception as e:
                    try:
                        if conn2:
                            conn2.execute("ROLLBACK;")
                    except Exception:
                        pass
                    logger.logMessage(f"[LifetimeProcessor] Error processing osi={osi}: {e}")
                    with self._status_lock:
                        self._status.last_error = str(e)
                    break
                finally:
                    if conn2:
                        try:
                            conn2.close()
                        except Exception:
                            pass

        with self._status_lock:
            self._status.total_archived += archived
            self._status.total_deleted_small += deleted_small

        if archived:
            logger.logMessage(f"[LifetimeProcessor] Archived {archived} OSIs (batch_size={self.batch_size}).")
        if deleted_small:
            logger.logMessage(f"[LifetimeProcessor] Deleted {deleted_small} small OSIs (<{self.min_snapshots}).")

        return len(osi_keys)
