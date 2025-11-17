import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
from shared_options.log.logger_singleton import getLogger


@dataclass
class PermutationProcessorStatus:
    last_run: Optional[datetime] = None
    last_processed_batch: int = 0
    total_permuted_osis: int = 0
    total_rows_inserted: int = 0
    last_error: Optional[str] = None


class OptionPermutationProcessor:
    """
    Background processor that creates buy→sell permutations for each OSI in option_lifetimes.
    Produces fully labeled rows for AI training.
    """

    LIFETIME_TABLE = "option_lifetimes"
    PERM_TABLE = "option_permutations"

    # Full column set carried through from lifetime table
    COLUMNS = [
        "osiKey", "timestamp", "symbol", "optionType", "strikePrice", "lastPrice",
        "bid", "ask", "bidSize", "askSize", "volume", "openInterest", "nearPrice",
        "inTheMoney", "delta", "gamma", "theta", "vega", "rho", "iv",
        "daysToExpiration", "spread", "midPrice", "moneyness"
    ]

    def __init__(
        self,
        db_path: str,
        check_interval: int = 60,
        batch_size: int = 200,
        sqlite_timeout: float = 30.0,
        max_retries_per_osi: int = 3,
    ):
        self.db_path = Path(db_path)
        self.check_interval = check_interval
        self.batch_size = batch_size
        self.sqlite_timeout = sqlite_timeout
        self.max_retries = max_retries_per_osi

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.logger = getLogger()

        self._status = PermutationProcessorStatus()
        self._lock = threading.Lock()

        self._init_permutation_table()

    # --------------------------------------------------------
    # DB Helper
    # --------------------------------------------------------
    def _get_conn(self):
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=self.sqlite_timeout,
            isolation_level=None
        )
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
        except Exception:
            pass
        return conn

    # --------------------------------------------------------
    # Table init
    # --------------------------------------------------------
    def _init_permutation_table(self):
        try:
            conn = self._get_conn()
            c = conn.cursor()

            c.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.PERM_TABLE} (
                    osiKey TEXT NOT NULL,
                    buy_timestamp TEXT NOT NULL,
                    sell_timestamp TEXT NOT NULL,
                    hold_seconds REAL,
                    buy_price REAL,
                    sell_price REAL,
                    profit REAL,
                    return_pct REAL,

                    symbol TEXT,
                    optionType INTEGER,
                    strikePrice REAL,
                    bid REAL,
                    ask REAL,
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

                    PRIMARY KEY (osiKey, buy_timestamp, sell_timestamp)
                )
            """)

            c.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_perm_osi ON {self.PERM_TABLE}(osiKey)
            """)

            conn.commit()
            conn.close()
            self.logger.logMessage("[Permutation] Permutation table initialized.")
        except Exception as e:
            self.logger.logMessage(f"[Permutation] Error initializing perm table: {e}")

    # --------------------------------------------------------
    # Lifecycle
    # --------------------------------------------------------
    def start(self):
        if self._thread and self._thread.is_alive():
            self.logger.logMessage("[Permutation] Already running.")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        self.logger.logMessage("[Permutation] Processor started.")

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
        self.logger.logMessage("[Permutation] Processor stopped.")

    def get_status(self):
        with self._lock:
            return {
                "last_run": self._status.last_run.isoformat() if self._status.last_run else None,
                "last_processed_batch": self._status.last_processed_batch,
                "total_permuted_osis": self._status.total_permuted_osis,
                "total_rows_inserted": self._status.total_rows_inserted,
                "last_error": self._status.last_error
            }

    # --------------------------------------------------------
    # Main loop
    # --------------------------------------------------------
    def _loop(self):
        while not self._stop_event.is_set():
            try:
                processed = self._process_batch()
                with self._lock:
                    self._status.last_run = datetime.utcnow()
                    self._status.last_processed_batch = processed

                if processed == 0:
                    time.sleep(self.check_interval)
                else:
                    time.sleep(0.1)

            except Exception as e:
                self.logger.logMessage(f"[Permutation] Loop error: {e}")
                with self._lock:
                    self._status.last_error = str(e)
                time.sleep(2)

        self.logger.logMessage("[Permutation] Loop exited.")

    # --------------------------------------------------------
    # Batch Processing
    # --------------------------------------------------------
    def _process_batch(self) -> int:
        conn = self._get_conn()
        c = conn.cursor()

        # 1) Get OSIs to process
        try:
            c.execute(f"""
                SELECT DISTINCT osiKey
                FROM {self.LIFETIME_TABLE}
                LIMIT ?
            """, (self.batch_size,))
            rows = c.fetchall()
            osi_list = [r[0] for r in rows]
        except sqlite3.OperationalError:
            conn.close()
            return 0

        if not osi_list:
            conn.close()
            return 0

        total_inserted = 0
        perm_sql = f"""
            INSERT OR REPLACE INTO {self.PERM_TABLE} (
                osiKey, buy_timestamp, sell_timestamp,
                hold_seconds, buy_price, sell_price, profit, return_pct,
                symbol, optionType, strikePrice, bid, ask,
                delta, gamma, theta, vega, rho, iv,
                daysToExpiration, spread, midPrice, moneyness
            )
            VALUES ({",".join("?" * 24)})
        """

        for osi in osi_list:
            attempts = 0
            while attempts < self.max_retries:
                attempts += 1
                try:
                    # Fetch lifetime snapshots
                    c.execute(f"""
                        SELECT {",".join(self.COLUMNS)}
                        FROM {self.LIFETIME_TABLE}
                        WHERE osiKey = ?
                        ORDER BY timestamp ASC
                    """, (osi,))
                    snaps = c.fetchall()

                    if len(snaps) < 2:
                        # Just delete small sequences
                        conn.execute("BEGIN;")
                        c.execute(f"DELETE FROM {self.LIFETIME_TABLE} WHERE osiKey = ?", (osi,))
                        conn.execute("COMMIT;")
                        break

                    perm_rows = []
                    for i in range(len(snaps)):
                        buy = snaps[i]
                        buy_ts = buy[1]
                        buy_price = float(buy[5])

                        dt_buy = datetime.fromisoformat(buy_ts)

                        for j in range(i + 1, len(snaps)):
                            sell = snaps[j]
                            sell_ts = sell[1]
                            sell_price = float(sell[5])

                            dt_sell = datetime.fromisoformat(sell_ts)
                            hold_seconds = (dt_sell - dt_buy).total_seconds()
                            profit = sell_price - buy_price
                            return_pct = profit / buy_price if buy_price != 0 else None

                            # Shared feature columns (after price columns)
                            feature_cols = sell[2:]  # symbol → moneyness

                            perm_rows.append((
                                osi, buy_ts, sell_ts,
                                hold_seconds, buy_price, sell_price,
                                profit, return_pct,
                                *feature_cols
                            ))

                    # Insert + delete atomically
                    conn.execute("BEGIN;")
                    c.executemany(perm_sql, perm_rows)
                    c.execute(f"DELETE FROM {self.LIFETIME_TABLE} WHERE osiKey = ?", (osi,))
                    conn.execute("COMMIT;")

                    total_inserted += len(perm_rows)
                    break

                except sqlite3.OperationalError as e:
                    try: conn.execute("ROLLBACK;")
                    except: pass
                    self.logger.logMessage(f"[Permutation] SQLite busy OSI={osi}: {e}")
                    time.sleep(0.1 * attempts)
                except Exception as e:
                    try: conn.execute("ROLLBACK;")
                    except: pass
                    self.logger.logMessage(f"[Permutation] Fatal OSI={osi}: {e}")
                    with self._lock:
                        self._status.last_error = str(e)
                    break

        conn.commit()
        conn.close()

        with self._lock:
            self._status.total_permuted_osis += len(osi_list)
            self._status.total_rows_inserted += total_inserted

        return len(osi_list)
