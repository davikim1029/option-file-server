# processors/permutation_processor.py
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from shared_options.log.logger_singleton import getLogger

class OptionPermutationProcessor:
    """
    Generates buy/sell permutations for each OSI from option_snapshots (or option_lifetimes).
    Stores buy_*, sell_*, delta_* for all snapshot columns.
    Includes status reporting and DB maintenance (ANALYZE + periodic VACUUM).
    """

    LIFETIME_TABLE = "option_lifetimes"   # or point to option_snapshots if you prefer
    SNAPSHOT_TABLE = "option_snapshots"
    PERM_TABLE = "option_permutations"

    def __init__(self, db_path: str, check_interval: int = 60, batch_commit_size: int = 50,
                 analyze_after_commits: int = 1, vacuum_interval_hours: int = 24):
        self.db_path = Path(db_path)
        self.check_interval = check_interval
        self.batch_commit_size = batch_commit_size
        self.analyze_after_commits = max(1, analyze_after_commits)
        self.vacuum_interval = timedelta(hours=vacuum_interval_hours)

        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.logger = getLogger()

        # status counters
        self._total_rows_inserted = 0
        self._total_osis_processed = 0
        self._last_run: Optional[datetime] = None
        self._last_error: Optional[str] = None
        self._last_vacuum: Optional[datetime] = None
        self._commit_count = 0

        # load schema
        self.snapshot_columns = self._load_snapshot_columns()
        self.numeric_columns = self._filter_numeric_columns()

        # init DB table
        self._init_perm_table()

    # ------------------------
    # DB helpers
    # ------------------------
    def _get_conn(self):
        # timeout helps with transient locks
        conn = sqlite3.connect(str(self.db_path), timeout=30.0, isolation_level=None)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
        except Exception:
            pass
        return conn

    def _load_snapshot_columns(self):
        # Read actual snapshot table columns so this auto-adapts
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({self.SNAPSHOT_TABLE});")
        cols = [r[1] for r in cur.fetchall()]
        conn.close()
        if not cols:
            # sensible default if table not created yet
            return [
                "osiKey", "timestamp", "symbol", "optionType", "strikePrice", "lastPrice",
                "bid", "ask", "bidSize", "askSize", "volume", "openInterest", "nearPrice",
                "inTheMoney", "delta", "gamma", "theta", "vega", "rho", "iv",
                "daysToExpiration", "spread", "midPrice", "moneyness"
            ]
        return cols

    def _filter_numeric_columns(self):
        numeric = {
            "lastPrice", "bid", "ask", "bidSize", "askSize", "volume", "openInterest",
            "nearPrice", "inTheMoney", "delta", "gamma", "theta", "vega", "rho",
            "iv", "daysToExpiration", "spread", "midPrice", "moneyness"
        }
        return [c for c in self.snapshot_columns if c in numeric]

    # ------------------------
    # Table init
    # ------------------------
    def _init_perm_table(self):
        conn = self._get_conn()
        c = conn.cursor()

        column_defs = []
        # buy_* and sell_* columns for every snapshot column
        for col in self.snapshot_columns:
            # use REAL for numbers, TEXT OK for strings; using REAL for simplicity and compatibility
            column_defs.append(f"buy_{col} REAL")
        for col in self.snapshot_columns:
            column_defs.append(f"sell_{col} REAL")
        # delta for numeric columns only
        for col in self.numeric_columns:
            column_defs.append(f"delta_{col} REAL")

        # add computed metrics
        column_defs.append("hold_seconds REAL")
        column_defs.append("profit REAL")
        column_defs.append("return_pct REAL")

        ddl = f"""
            CREATE TABLE IF NOT EXISTS {self.PERM_TABLE} (
                osiKey TEXT NOT NULL,
                buy_timestamp TEXT NOT NULL,
                sell_timestamp TEXT NOT NULL,
                {', '.join(column_defs)},
                PRIMARY KEY (osiKey, buy_timestamp, sell_timestamp)
            );
        """
        c.execute(ddl)
        c.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.PERM_TABLE}_osi ON {self.PERM_TABLE}(osiKey);")
        conn.commit()
        conn.close()
        self.logger.logMessage("[Permutation] Permutation table initialized (dynamic columns).")

    # ------------------------
    # Lifecycle
    # ------------------------
    def start(self):
        if self.running:
            self.logger.logMessage("[Permutation] Processor already running.")
            return
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        self.logger.logMessage("[Permutation] Processor started.")

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=10)
        self.logger.logMessage("[Permutation] Processor stopped.")

    def get_status(self):
        return {
            "total_rows_inserted": int(self._total_rows_inserted),
            "total_osis_processed": int(self._total_osis_processed),
            "last_run": self._last_run.isoformat() if self._last_run else None,
            "last_error": str(self._last_error) if self._last_error else None,
            "last_vacuum": self._last_vacuum.isoformat() if self._last_vacuum else None
        }

    # ------------------------
    # Loop
    # ------------------------
    def _loop(self):
        while self.running:
            try:
                self._last_run = datetime.utcnow()
                self._process_all_osis()
            except Exception as e:
                self._last_error = str(e)
                self.logger.logMessage(f"[Permutation] Loop error: {e}")
            time.sleep(self.check_interval)

    # ------------------------
    # Core processing
    # ------------------------
    def _fetch_osi_list(self, conn):
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT osiKey FROM {self.LIFETIME_TABLE} LIMIT ?;", (self.batch_commit_size,))
        return [r[0] for r in cur.fetchall()]

    def _fetch_snapshots_for_osi(self, conn, osi):
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {self.SNAPSHOT_TABLE} WHERE osiKey = ? ORDER BY timestamp ASC;", (osi,))
        return cur.fetchall()

    def _process_all_osis(self):
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row

        osi_list = self._fetch_osi_list(conn)
        if not osi_list:
            conn.close()
            return

        for osi in osi_list:
            try:
                self._process_single_osi(conn, osi)
                self._total_osis_processed += 1
            except Exception as e:
                self._last_error = str(e)
                self.logger.logMessage(f"[Permutation] Error processing OSI={osi}: {e}")

        conn.close()

    def _process_single_osi(self, conn, osi):
        cur = conn.cursor()
        cur.row_factory = sqlite3.Row
        cur.execute(f"SELECT * FROM {self.SNAPSHOT_TABLE} WHERE osiKey = ? ORDER BY timestamp ASC;", (osi,))
        snaps = cur.fetchall()

        if len(snaps) < 2:
            # nothing useful — delete
            conn.execute("BEGIN;")
            conn.execute(f"DELETE FROM {self.LIFETIME_TABLE} WHERE osiKey = ?;", (osi,))
            conn.execute("COMMIT;")
            return

        rows_to_insert = []
        for i in range(len(snaps)):
            buy = snaps[i]
            for j in range(i + 1, len(snaps)):
                sell = snaps[j]
                rows_to_insert.append(self._build_row_from_snapshots(osi, buy, sell))

        if rows_to_insert:
            self._insert_rows(conn, rows_to_insert)

        # delete consumed lifetimes (atomic)
        conn.execute("BEGIN;")
        conn.execute(f"DELETE FROM {self.LIFETIME_TABLE} WHERE osiKey = ?;", (osi,))
        conn.execute("COMMIT;")

    def _build_row_from_snapshots(self, osi, buy_row, sell_row):
        buy_ts = buy_row["timestamp"]
        sell_ts = sell_row["timestamp"]
        dt_buy = datetime.fromisoformat(buy_ts)
        dt_sell = datetime.fromisoformat(sell_ts)
        hold_seconds = (dt_sell - dt_buy).total_seconds()

        # safe numeric conversion
        def _to_float(v):
            try:
                return float(v) if v is not None else 0.0
            except Exception:
                return 0.0

        buy_price = _to_float(buy_row.get("lastPrice", 0.0))
        sell_price = _to_float(sell_row.get("lastPrice", 0.0))
        profit = sell_price - buy_price
        return_pct = profit / buy_price if buy_price else 0.0

        row = {
            "osiKey": osi,
            "buy_timestamp": buy_ts,
            "sell_timestamp": sell_ts,
            "hold_seconds": hold_seconds,
            "profit": profit,
            "return_pct": return_pct
        }

        # buy_* and sell_* for all snapshot columns
        for col in self.snapshot_columns:
            row[f"buy_{col}"] = buy_row.get(col)
        for col in self.snapshot_columns:
            row[f"sell_{col}"] = sell_row.get(col)

        # delta_* only for numeric columns
        for col in self.numeric_columns:
            b = buy_row.get(col) or 0.0
            s = sell_row.get(col) or 0.0
            try:
                row[f"delta_{col}"] = float(s) - float(b)
            except Exception:
                row[f"delta_{col}"] = None

        return row

    def _insert_rows(self, conn, rows):
        if not rows:
            return

        columns = list(rows[0].keys())
        placeholders = ",".join(["?"] * len(columns))
        sql = f"INSERT OR REPLACE INTO {self.PERM_TABLE} ({','.join(columns)}) VALUES ({placeholders});"

        values = [tuple(row[c] for c in columns) for row in rows]

        attempts = 5
        for attempt in range(attempts):
            try:
                conn.executemany(sql, values)
                conn.commit()
                self._total_rows_inserted += len(values)
                self._commit_count += 1
                # run ANALYZE occasionally to keep query planner stats fresh
                if (self._commit_count % self.analyze_after_commits) == 0:
                    try:
                        conn.execute("ANALYZE;")
                    except Exception:
                        pass
                # attempt VACUUM if it's been long enough (rarely)
                if not self._last_vacuum or (datetime.utcnow() - self._last_vacuum) > self.vacuum_interval:
                    try:
                        # VACUUM takes a lock; only run if database small or during quiet hours
                        conn.execute("VACUUM;")
                        self._last_vacuum = datetime.utcnow()
                    except Exception:
                        # if busy, skip and try later
                        pass
                return
            except sqlite3.OperationalError as e:
                self.logger.logMessage(f"[Permutation] SQLite operational error on insert (attempt {attempt+1}): {e}")
                time.sleep(0.1 * (attempt + 1))
            except Exception as e:
                self._last_error = str(e)
                self.logger.logMessage(f"[Permutation] Unexpected error during insert: {e}")
                raise

        raise RuntimeError("Failed to insert permutation rows after retries.")
