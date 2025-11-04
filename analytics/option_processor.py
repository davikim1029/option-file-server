import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from logger.logger_singleton import getLogger


class OptionAnalyticsProcessor:
    """
    Archives completed option snapshots into option_lifetimes preserving the
    full snapshot schema so AI training can consume exact historic rows.
    """

    SNAPSHOT_TABLE = "option_snapshots"
    LIFETIME_TABLE = "option_lifetimes"

    # List of columns expected in the snapshot table (kept in same order)
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

        self._init_lifetime_table()

    # ---------------------------------------
    # Initialization: create lifetime table
    # ---------------------------------------
    def _init_lifetime_table(self):
        """Create option_lifetimes using the same schema as option_snapshots."""
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()

            # Build CREATE TABLE statement using the same columns and types from snapshots.
            # We choose TEXT for osiKey/timestamp/symbol, INTEGER for ints, REAL for floats.
            # Keep same names so downstream AI code can be reused without remapping.
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
                )
            """)
            # performance index for lookup by osiKey
            c.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.LIFETIME_TABLE}_osi ON {self.LIFETIME_TABLE} (osiKey)")
            conn.commit()
            conn.close()
            self.logger.logMessage("[Analytics] Lifetime table initialized (same schema as snapshots).")
        except Exception as e:
            self.logger.logMessage("[Analytics] Error initializing lifetime table:")
            self.logger.logMessage(str(e))

    # ---------------------------------------
    # Thread control
    # ---------------------------------------
    def start(self):
        if self.running:
            self.logger.logMessage("[Analytics] Processor already running.")
            return
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        self.logger.logMessage("[Analytics] Processor started.")

    def stop(self):
        if not self.running:
            self.logger.logMessage("[Analytics] Processor not running.")
            return
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        self.logger.logMessage("[Analytics] Processor stopped.")

    def _run_loop(self):
        """Background loop: periodically archive completed lifetimes."""
        while self.running:
            try:
                self.archive_completed_lifetimes()
            except Exception as e:
                self.logger.logMessage(f"[Analytics] Error in loop: {e}")
            time.sleep(self.check_interval)

    # ---------------------------------------
    # Core archiving logic
    # ---------------------------------------
    def archive_completed_lifetimes(self, min_lifespan_days: int = 0):
        """
        Find completed options (MAX(daysToExpiration) <= 0), and for each:
            - copy all snapshot rows into option_lifetimes (same schema)
            - delete those snapshot rows from option_snapshots
        Optionally skip very short-lived options by min_lifespan_days.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()

            # 1) Find osiKeys that have completed (max(daysToExpiration) <= 0)
            c.execute(f"""
                SELECT osiKey
                FROM {self.SNAPSHOT_TABLE}
                GROUP BY osiKey
                HAVING MAX(daysToExpiration) <= 0
            """)
            completed = [row[0] for row in c.fetchall()]
            archived_count = 0

            for osiKey in completed:
                # If already archived, clear and replace to ensure completeness
                c.execute(f"DELETE FROM {self.LIFETIME_TABLE} WHERE osiKey = ?", (osiKey,))
                
                # Verify truly expired
                c.execute(f"""
                    SELECT COUNT(*) FROM {self.SNAPSHOT_TABLE}
                    WHERE osiKey = ? AND daysToExpiration > 0
                """, (osiKey,))
                still_active = c.fetchone()[0]
                if still_active > 0:
                    # skip if some data shows it's still active
                    continue



                # fetch all snapshot rows for this osiKey in chronological order
                c.execute(f"""
                    SELECT {', '.join(self.SNAPSHOT_COLUMNS)}
                    FROM {self.SNAPSHOT_TABLE}
                    WHERE osiKey = ?
                    ORDER BY timestamp ASC
                """, (osiKey,))
                rows = c.fetchall()
                if not rows:
                    continue

                # validate lifespan length if desired
                try:
                    start_ts = datetime.fromisoformat(rows[0][1])
                    end_ts = datetime.fromisoformat(rows[-1][1])
                except Exception:
                    # If timestamps are malformed, still proceed but skip min lifespan check
                    start_ts = None
                    end_ts = None

                if start_ts and end_ts:
                    lifespan_days = (end_ts - start_ts).days
                    if min_lifespan_days and lifespan_days < min_lifespan_days:
                        # delete snapshots that are too short-lived to save space
                        c.execute(f"DELETE FROM {self.SNAPSHOT_TABLE} WHERE osiKey = ?", (osiKey,))
                        conn.commit()
                        continue

                # Insert all snapshot rows into lifetime table
                # build placeholders and execute many
                placeholders = ", ".join(["?"] * len(self.SNAPSHOT_COLUMNS))
                insert_sql = f"""
                    INSERT OR REPLACE INTO {self.LIFETIME_TABLE} ({', '.join(self.SNAPSHOT_COLUMNS)})
                    VALUES ({placeholders})
                """
                # ensure rows are tuples that align with column order (they already are from SELECT)
                c.executemany(insert_sql, rows)

                # delete from snapshot table after successful insert
                c.execute(f"DELETE FROM {self.SNAPSHOT_TABLE} WHERE osiKey = ?", (osiKey,))

                conn.commit()
                archived_count += 1

            conn.close()
            if archived_count:
                self.logger.logMessage(f"[Analytics] Archived lifetimes for {archived_count} options.")
        except Exception as e:
            self.logger.logMessage("[Analytics] Error archiving completed lifetimes:")
            self.logger.logMessage(str(e))


# ---------------------------------------
# Reporting helper
# ---------------------------------------
def get_summary(db_path: str):
    """
    Print summary counts for snapshot and lifetime tables.
    """
    logger = getLogger()
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()

        # Snapshots
        c.execute(f"SELECT COUNT(*) FROM {OptionAnalyticsProcessor.SNAPSHOT_TABLE}")
        total_snapshots = c.fetchone()[0]

        c.execute(f"SELECT COUNT(DISTINCT osiKey) FROM {OptionAnalyticsProcessor.SNAPSHOT_TABLE}")
        unique_active_options = c.fetchone()[0]

        c.execute(f"SELECT COUNT(DISTINCT symbol) FROM {OptionAnalyticsProcessor.SNAPSHOT_TABLE}")
        unique_active_symbols = c.fetchone()[0]

        # Lifetimes
        c.execute(f"SELECT COUNT(*) FROM {OptionAnalyticsProcessor.LIFETIME_TABLE}")
        total_lifetime_rows = c.fetchone()[0]

        c.execute(f"SELECT COUNT(DISTINCT osiKey) FROM {OptionAnalyticsProcessor.LIFETIME_TABLE}")
        archived_options = c.fetchone()[0]

        c.execute(f"SELECT COUNT(DISTINCT symbol) FROM {OptionAnalyticsProcessor.LIFETIME_TABLE}")
        archived_symbols = c.fetchone()[0]

        conn.close()

        print("======= DB SUMMARY =======")
        print(f"Total snapshot rows:      {total_snapshots}")
        print(f"Unique active options:    {unique_active_options}")
        print(f"Symbols in snapshots:     {unique_active_symbols}")
        print(f"Total lifetime rows:      {total_lifetime_rows}  (each snapshot preserved)")
        print(f"Archived options (osi):   {archived_options}")
        print(f"Symbols in archives:      {archived_symbols}")
        print("==========================")
    except Exception as e:
        logger.logMessage("[Analytics] Error during summary reporting:")
        logger.logMessage(str(e))
