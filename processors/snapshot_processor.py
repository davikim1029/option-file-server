# processors/snapshot_processor.py
"""
OptionSnapshotProcessor
- Watches a folder for JSON snapshot files and ingests them into option_snapshots.
- Uses short-lived DB connections with WAL and busy_timeout to reduce lock contention.
- Inserts per-file are run inside a single short transaction (BEGIN IMMEDIATE).
"""

import json
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Union, Optional

from shared_options.log.logger_singleton import getLogger
from logging import FileHandler

logger = getLogger()

# resolve log file for context (used by old code; kept for compatibility)
LOG_FILE = Path("option_server.log")
for handler in logger.logger.handlers:
    if isinstance(handler, FileHandler):
        LOG_FILE = Path(handler.baseFilename)
        break


class OptionSnapshotProcessor(threading.Thread):
    def __init__(
        self,
        db_path: Union[str, Path],
        incoming_folder: Union[str, Path],
        check_interval: int = 30,
        sqlite_timeout: float = 30.0
    ):
        super().__init__(daemon=True, name="OptionSnapshotProcessor")
        self.db_path = Path(db_path)
        self.folder = Path(incoming_folder)
        self.check_interval = int(check_interval)
        self.sqlite_timeout = float(sqlite_timeout)

        self._stop_event = threading.Event()

        # ensure necessary paths exist
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.folder.mkdir(parents=True, exist_ok=True)

        # initialize schema
        self._init_db()

    # -------------------------
    # DB helper (short-lived connections)
    # -------------------------
    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=self.sqlite_timeout, isolation_level=None)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout=30000;")  # 30 seconds
        except Exception:
            # ignore on constrained builds
            pass
        return conn

    # -------------------------
    # Table init
    # -------------------------
    def _init_db(self) -> None:
        try:
            logger.logMessage("[SnapshotProcessor] Checking/creating option_snapshots table")
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS option_snapshots (
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
            cur.execute("CREATE INDEX IF NOT EXISTS idx_option_snapshots_osi ON option_snapshots(osiKey);")
            conn.commit()
            conn.close()
            logger.logMessage("[SnapshotProcessor] option_snapshots table ready")
        except Exception as e:
            logger.logMessage("[SnapshotProcessor] DB init error")
            logger.logMessage(str(e))

    # -------------------------
    # Thread control
    # -------------------------
    def stop(self):
        self._stop_event.set()

    def run(self):
        logger.logMessage("[SnapshotProcessor] Started")
        while not self._stop_event.is_set():
            try:
                for file in list(self.folder.glob("*.json")):
                    try:
                        logger.logMessage(f"[SnapshotProcessor] Processing {file.name}")
                        self.ingest_file(file)
                        try:
                            file.unlink(missing_ok=True)
                        except Exception:
                            logger.logMessage(f"[SnapshotProcessor] Failed to unlink {file.name}")
                    except Exception as e:
                        logger.logMessage(f"[SnapshotProcessor] Error processing {file.name}: {e}")
                time.sleep(self.check_interval)
            except Exception as e:
                logger.logMessage("[SnapshotProcessor] Loop crash (recovering)")
                logger.logMessage(str(e))
                time.sleep(5)
        logger.logMessage("[SnapshotProcessor] Stopped")

    # -------------------------
    # Ingest single file with its own short transaction
    # -------------------------
    def ingest_file(self, file_path: Union[str, Path]) -> None:
        file_path = Path(file_path)
        if not file_path.exists():
            logger.logMessage(f"[SnapshotProcessor] File not found: {file_path}")
            return

        try:
            with file_path.open("r") as f:
                data = json.load(f)
        except Exception as e:
            logger.logMessage(f"[SnapshotProcessor] JSON load error for {file_path.name}: {e}")
            return

        if not data:
            logger.logMessage(f"[SnapshotProcessor] Empty JSON file: {file_path.name}")
            return

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = self._get_conn()
            cur = conn.cursor()

            cols = (
                "osiKey", "timestamp", "symbol", "optionType", "strikePrice",
                "lastPrice", "bid", "ask", "bidSize", "askSize", "volume", "openInterest",
                "nearPrice", "inTheMoney", "delta", "gamma", "theta", "vega", "rho", "iv",
                "daysToExpiration", "spread", "midPrice", "moneyness"
            )
            placeholders = ", ".join(["?"] * len(cols))
            sql = f"INSERT OR REPLACE INTO option_snapshots ({', '.join(cols)}) VALUES ({placeholders});"

            # Do the entire file insert in a short transaction
            conn.execute("BEGIN IMMEDIATE;")
            for entry in data:
                # Defensive timestamp handling — mirror previous behavior
                ts = entry.get("timestamp")
                if ts is None:
                    ts = datetime.utcnow().isoformat()
                elif isinstance(ts, datetime):
                    ts = ts.isoformat()

                params = (
                    entry.get("osiKey"),
                    ts,
                    entry.get("symbol"),
                    entry.get("optionType"),
                    entry.get("strikePrice"),
                    entry.get("lastPrice"),
                    entry.get("bid"),
                    entry.get("ask"),
                    entry.get("bidSize"),
                    entry.get("askSize"),
                    entry.get("volume"),
                    entry.get("openInterest"),
                    entry.get("nearPrice"),
                    entry.get("inTheMoney"),
                    entry.get("delta"),
                    entry.get("gamma"),
                    entry.get("theta"),
                    entry.get("vega"),
                    entry.get("rho"),
                    entry.get("iv"),
                    entry.get("daysToExpiration"),
                    entry.get("spread"),
                    entry.get("midPrice"),
                    entry.get("moneyness"),
                )
                cur.execute(sql, params)
            conn.execute("COMMIT;")
            logger.logMessage(f"[SnapshotProcessor] {file_path.name} added to DB (thread={threading.current_thread().name})")
        except sqlite3.OperationalError as e:
            try:
                if conn:
                    conn.execute("ROLLBACK;")
            except Exception:
                pass
            logger.logMessage(f"[SnapshotProcessor] DB OperationalError ingesting {file_path.name}: {e}")
        except Exception as e:
            try:
                if conn:
                    conn.execute("ROLLBACK;")
            except Exception:
                pass
            logger.logMessage(f"[SnapshotProcessor] DB insert error for {file_path.name}: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
