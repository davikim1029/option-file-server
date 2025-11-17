import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from shared_options.log.logger_singleton import getLogger
import sqlite3
from datetime import datetime
from itertools import combinations
import time


class OptionPermutationProcessor:
    """
    Generates all buy/sell permutations for each option lifetime
    to create a fully labeled table for AI training.
    Consumes OSI keys from option_lifetimes and snapshots once processed.
    """

    LIFETIME_TABLE = "option_lifetimes"
    SNAPSHOT_TABLE = "option_snapshots"
    PERMUTATION_TABLE = "option_permutations"

    # Columns from snapshot table to carry into permutations
    SNAPSHOT_COLUMNS = [
        "osiKey", "timestamp", "symbol", "optionType", "strikePrice", "lastPrice",
        "bid", "ask", "bidSize", "askSize", "volume", "openInterest", "nearPrice",
        "inTheMoney", "delta", "gamma", "theta", "vega", "rho", "iv", "daysToExpiration",
        "spread", "midPrice", "moneyness"
    ]

    def __init__(self, db_path: str, check_interval: int = 60):
        self.db_path = Path(db_path)
        self.check_interval = check_interval
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.logger = getLogger()

        self._init_permutation_table()

    # ---------------------------------------
    # Initialization
    # ---------------------------------------
    def _init_permutation_table(self):
        """Create option_permutations table if it doesn't exist."""
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()

            c.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.PERMUTATION_TABLE} (
                    osiKey TEXT NOT NULL,
                    buy_timestamp TEXT NOT NULL,
                    sell_timestamp TEXT NOT NULL,
                    hold_time REAL,
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
            c.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.PERMUTATION_TABLE}_osi ON {self.PERMUTATION_TABLE} (osiKey)")
            conn.commit()
            conn.close()
            self.logger.logMessage("[Permutations] Permutation table initialized.")
        except Exception as e:
            self.logger.logMessage("[Permutations] Error initializing permutation table:")
            self.logger.logMessage(str(e))

    # ---------------------------------------
    # Thread control
    # ---------------------------------------
    def start(self):
        if self.running:
            self.logger.logMessage("[Permutations] Processor already running.")
            return
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        self.logger.logMessage("[Permutations] Processor started.")

    def stop(self):
        if not self.running:
            self.logger.logMessage("[Permutations] Processor not running.")
            return
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        self.logger.logMessage("[Permutations] Processor stopped.")

    # ---------------------------------------
    # Background loop
    # ---------------------------------------
    def _run_loop(self):
        while self.running:
            try:
                self.process_permutations()
            except Exception as e:
                self.logger.logMessage(f"[Permutations] Error in loop: {e}")
            time.sleep(self.check_interval)

    # ---------------------------------------
    # Core logic
    # ---------------------------------------


    def permutation_processor():
        DB_PATH = "options.db"
        BATCH_COMMIT_SIZE = 50  # commit every X OSIs

        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Create target table if missing
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS option_permutations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                osi TEXT NOT NULL,
                buy_timestamp TEXT NOT NULL,
                sell_timestamp TEXT NOT NULL,
                buy_price REAL NOT NULL,
                sell_price REAL NOT NULL,
                profit REAL NOT NULL,
                hold_seconds INTEGER NOT NULL
            )
        """)

        # Fetch OSIs one at a time to avoid loading full table
        cursor.execute("SELECT DISTINCT osi FROM option_lifetimes ORDER BY osi ASC")
        all_osis = cursor.fetchall()

        processed = 0

        for row in all_osis:
            osi = row["osi"]

            # Fetch all snapshots for this OSI
            cursor.execute("""
                SELECT timestamp, last_price
                FROM option_lifetimes
                WHERE osi = ?
                ORDER BY timestamp ASC
            """, (osi,))
            snapshots = cursor.fetchall()

            # Skip OSIs with <2 snapshots (no permutations possible)
            if len(snapshots) < 2:
                cursor.execute("DELETE FROM option_lifetimes WHERE osi = ?", (osi,))
                continue

            # Generate permutations (i = buy, j = sell)
            perm_rows = []
            for i in range(len(snapshots)):
                buy = snapshots[i]
                buy_ts = buy["timestamp"]
                buy_price = float(buy["last_price"])

                for j in range(i + 1, len(snapshots)):
                    sell = snapshots[j]
                    sell_ts = sell["timestamp"]
                    sell_price = float(sell["last_price"])

                    # Compute metrics
                    dt_buy = datetime.fromisoformat(buy_ts)
                    dt_sell = datetime.fromisoformat(sell_ts)
                    hold_seconds = int((dt_sell - dt_buy).total_seconds())
                    profit = sell_price - buy_price

                    perm_rows.append((
                        osi,
                        buy_ts,
                        sell_ts,
                        buy_price,
                        sell_price,
                        profit,
                        hold_seconds
                    ))

            # Insert all permutations in a single batch
            cursor.executemany("""
                INSERT INTO option_permutations (
                    osi, buy_timestamp, sell_timestamp,
                    buy_price, sell_price, profit, hold_seconds
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, perm_rows)

            # Delete the OSI from lifetimes
            cursor.execute("DELETE FROM option_lifetimes WHERE osi = ?", (osi,))

            processed += 1
            if processed % BATCH_COMMIT_SIZE == 0:
                conn.commit()
                print(f"[INFO] Processed {processed} OSIs...")

        # Final commit
        conn.commit()
        conn.close()
        print("[DONE] All OSIs processed!")
