# option_lifetime_processor.py
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field

from shared_options.log.logger_singleton import getLogger

logger = getLogger()


@dataclass
class LifetimeProcessorStatus:
    last_run: Optional[datetime] = None
    last_processed_batch_size: int = 0
    total_archived: int = 0
    total_deleted_small: int = 0
    last_error: Optional[str] = None


class OptionLifetimeProcessor:
    """
    Background processor that archives completed option_snapshots into option_lifetimes.

    Key features:
      - Event-driven start/stop
      - Batch-based processing (safe for very large DBs)
      - WAL mode for improved SQLite concurrency
      - Per-OSI atomic insert+delete (transaction)
      - Simple metrics available via get_status()
    """

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
        """
        Args:
            db_path: path to SQLite DB file
            check_interval: seconds to sleep when no work found
            batch_size: number of OSIs to select and try to archive per loop
            min_snapshots: minimum number of snapshots required to archive an OSI
            sqlite_timeout: sqlite3 connection timeout (seconds)
            max_retries_per_osi: if processing an OSI fails, retry up to this many times
        """
        self.db_path = Path(db_path)
        self.check_interval = check_interval
        self.batch_size = int(batch_size)
        self.min_snapshots = int(min_snapshots)
        self.sqlite_timeout = float(sqlite_timeout)
        self.max_retries_per_osi = int(max_retries_per_osi)

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._status = LifetimeProcessorStatus()
        self._lock = threading.Lock()  # internal state guard

        # ensure db parent exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # init tables/indexes
        self._init_lifetime_table()
        self._ensure_indexes()

    # -------------------------
    # SQLite helpers
    # -------------------------
    def _get_conn(self) -> sqlite3.Connection:
        """Open a connection with sensible pragmas."""
        conn = sqlite3.connect(str(self.db_path), timeout=self.sqlite_timeout, isolation_level=None)
        try:
            # Use WAL to reduce locking contention (safe for most workloads)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
        except Exception:
            # ignore pragma errors on constrained SQLite builds
            pass
        return conn

    def _init_lifetime_table(self) -> None:
        """Create lifetime table if missing and ensure basic index."""
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
            with self._lock:
                self._status.last_error = str(e)

    def _ensure_indexes(self) -> None:
        """Ensure helpful indexes on snapshot table exist for performance."""
        try:
            conn = self._get_conn()
            c = conn.cursor()
            # index on osiKey (for grouping) and daysToExpiration (for quick expired filter)
            c.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.SNAPSHOT_TABLE}_osi ON {self.SNAPSHOT_TABLE}(osiKey);")
            c.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.SNAPSHOT_TABLE}_exp ON {self.SNAPSHOT_TABLE}(daysToExpiration);")
            c.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.SNAPSHOT_TABLE}_ts ON {self.SNAPSHOT_TABLE}(timestamp);")
            conn.commit()
            conn.close()
            logger.logMessage("[LifetimeProcessor] Snapshot indexes ensured.")
        except Exception as e:
            logger.logMessage("[LifetimeProcessor] Error ensuring indexes:")
            logger.logMessage(str(e))
            with self._lock:
                self._status.last_error = str(e)

    # -------------------------
    # Lifecycle control
    # -------------------------
    def start(self) -> None:
        """Start background thread (idempotent)."""
        if self._thread and self._thread.is_alive():
            logger.logMessage("[LifetimeProcessor] Processor already running.")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="OptionLifetimeProcessor")
        self._thread.start()
        logger.logMessage("[LifetimeProcessor] Processor started.")

    def stop(self, join_timeout: float = 10.0) -> None:
        """Signal the thread to stop and join."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=join_timeout)
        logger.logMessage("[LifetimeProcessor] Processor stopped.")

    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive() and not self._stop_event.is_set())

    # -------------------------
    # Status / metrics
    # -------------------------
    def get_status(self) -> Dict[str, Any]:
        """Return current status snapshot (safe to call from other threads)."""
        with self._lock:
            return {
                "last_run": self._status.last_run.isoformat() if self._status.last_run else None,
                "last_processed_batch_size": int(self._status.last_processed_batch_size),
                "total_archived": int(self._status.total_archived),
                "total_deleted_small": int(self._status.total_deleted_small),
                "last_error": self._status.last_error
            }

    # -------------------------
    # Main loop
    # -------------------------
    def _run_loop(self) -> None:
        logger.logMessage("[LifetimeProcessor] Run loop entered.")
        while not self._stop_event.is_set():
            try:
                processed = self._process_one_batch()
                with self._lock:
                    self._status.last_run = datetime.utcnow()
                    self._status.last_processed_batch_size = processed

                if processed == 0:
                    # nothing to do – sleep full interval
                    time.sleep(self.check_interval)
                else:
                    # short pause to allow other threads to run
                    time.sleep(0.1)
            except Exception as e:
                logger.logMessage(f"[LifetimeProcessor] Loop error: {e}")
                with self._lock:
                    self._status.last_error = str(e)
                time.sleep(5)
        logger.logMessage("[LifetimeProcessor] Run loop exited.")

    # -------------------------
    # Batch processing
    # -------------------------
    def _process_one_batch(self) -> int:
        """
        Select up to batch_size expired OSIs and archive them.
        Returns number of OSIs examined (selected).
        """
        conn = self._get_conn()
        c = conn.cursor()

        # 1) select candidate osiKeys (LIMIT batch_size)
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
            # table might not exist yet
            logger.logMessage(f"[LifetimeProcessor] SQLite error selecting OSIs: {e}")
            conn.close()
            return 0

        if not osi_keys:
            conn.close()
            return 0

        archived = 0
        deleted_small = 0

        placeholders = ", ".join(["?"] * len(self.SNAPSHOT_COLUMNS))
        insert_sql = f"INSERT OR REPLACE INTO {self.LIFETIME_TABLE} ({', '.join(self.SNAPSHOT_COLUMNS)}) VALUES ({placeholders})"

        for osi in osi_keys:
            # Retry loop in case of transient DB lock/errors
            attempts = 0
            success = False
            while attempts < self.max_retries_per_osi and not success:
                attempts += 1
                try:
                    # Fetch snapshots for this osi (chronological)
                    c.execute(f"""
                        SELECT {', '.join(self.SNAPSHOT_COLUMNS)}
                        FROM {self.SNAPSHOT_TABLE}
                        WHERE osiKey = ?
                        ORDER BY timestamp ASC
                    """, (osi,))
                    snaps = c.fetchall()

                    if not snaps:
                        success = True  # nothing to do
                        break

                    if len(snaps) < self.min_snapshots:
                        # delete snapshots with too few rows
                        conn.execute("BEGIN;")
                        c.execute(f"DELETE FROM {self.SNAPSHOT_TABLE} WHERE osiKey = ?", (osi,))
                        conn.execute("COMMIT;")
                        deleted_small += 1
                        success = True
                        break

                    # Archive within transaction to ensure atomicity
                    conn.execute("BEGIN;")
                    c.executemany(insert_sql, snaps)
                    c.execute(f"DELETE FROM {self.SNAPSHOT_TABLE} WHERE osiKey = ?", (osi,))
                    conn.execute("COMMIT;")
                    archived += 1
                    success = True

                except sqlite3.OperationalError as e:
                    # likely a transient lock or busy state; back off and retry
                    try:
                        conn.execute("ROLLBACK;")
                    except Exception:
                        pass
                    logger.logMessage(f"[LifetimeProcessor] OperationalError for osi={osi}: {e} (attempt {attempts})")
                    time.sleep(0.1 * attempts)
                except Exception as e:
                    try:
                        conn.execute("ROLLBACK;")
                    except Exception:
                        pass
                    logger.logMessage(f"[LifetimeProcessor] Error processing osi={osi}: {e}")
                    with self._lock:
                        self._status.last_error = str(e)
                    # do not retry on unexpected errors
                    break

        # update metrics and close
        conn.commit()
        conn.close()

        with self._lock:
            self._status.total_archived += archived
            self._status.total_deleted_small += deleted_small

        if archived:
            logger.logMessage(f"[LifetimeProcessor] Archived {archived} OSIs (batch_size={self.batch_size}).")
        if deleted_small:
            logger.logMessage(f"[LifetimeProcessor] Deleted {deleted_small} small OSIs (<{self.min_snapshots}).")

        return len(osi_keys)
