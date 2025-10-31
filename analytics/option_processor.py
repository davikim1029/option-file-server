import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from logger.logger_singleton import getLogger

class OptionAnalyticsProcessor:
    def __init__(self, db_path: str, check_interval: int = 60):
        self.db_path = Path(db_path)
        self.check_interval = check_interval
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.logger = getLogger()

        self._init_lifespan_table()

    def _init_lifespan_table(self):
        """Create the derived analytics table if it doesn’t exist."""
        self.logger.logMessage("Creating Lifespan database if doesn't exist")
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("""
                CREATE TABLE IF NOT EXISTS option_lifespans (
                    osiKey TEXT PRIMARY KEY,
                    symbol TEXT,
                    optionType INTEGER,
                    strikePrice REAL,
                    startDate TEXT,
                    endDate TEXT,
                    startPrice REAL,
                    endPrice REAL,
                    totalChange REAL,
                    avgIV REAL,
                    maxIV REAL,
                    minIV REAL,
                    totalSnapshots INTEGER
                )
            """)
            conn.commit()
            conn.close()
            self.logger.logMessage("Lifespan Database created/exists")
        except Exception as e:
            self.logger.logMessage("Error creating lifespan database")
            self.logger.logMessage(e)
            


    # -------------------------------
    # Core Background Loop
    # -------------------------------
    def start(self):
        if self.running:
            self.logger.logMessage("[Analytics] Processor is already running.")
            return
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        self.logger.logMessage("[Analytics] Processor started.")

    def stop(self):
        if not self.running:
            self.logger.logMessage("[Analytics] Processor is not running.")
            return
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        self.logger.logMessage("[Analytics] Processor stopped.")

    def _run_loop(self):
        while self.running:
            try:
                self.process_completed_options()
            except Exception as e:
                self.logger.logMessage(f"[Analytics] Error in analytics loop: {e}")
            time.sleep(self.check_interval)

    # -------------------------------
    # Core Analytics Logic
    # -------------------------------
    def process_completed_options(self):
        try:
            self.logger.logMessage("Processing completed options")
            """Find options where daysToExpiration <= 0 and archive their lifespan data."""
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()

            # Get all osiKeys that have expired in all their snapshots
            c.execute("""
                SELECT osiKey, symbol, optionType, strikePrice
                FROM option_snapshots
                GROUP BY osiKey
                HAVING MAX(daysToExpiration) <= 0
            """)
            completed = c.fetchall()

            for osiKey, symbol, optionType, strike in completed:
                # Skip if already archived
                c.execute("SELECT 1 FROM option_lifespans WHERE osiKey = ?", (osiKey,))
                if c.fetchone():
                    continue

                # Fetch full history for this option
                c.execute("""
                    SELECT timestamp, lastPrice, iv
                    FROM option_snapshots
                    WHERE osiKey = ?
                    ORDER BY timestamp ASC
                """, (osiKey,))
                rows = c.fetchall()
                if not rows:
                    continue

                startDate = rows[0][0]
                endDate = rows[-1][0]
                startPrice = rows[0][1]
                endPrice = rows[-1][1]
                totalChange = endPrice - startPrice

                iv_values = [r[2] for r in rows if r[2] is not None]
                avgIV = sum(iv_values) / len(iv_values) if iv_values else None
                maxIV = max(iv_values) if iv_values else None
                minIV = min(iv_values) if iv_values else None

                c.execute("""
                    INSERT OR REPLACE INTO option_lifespans (
                        osiKey, symbol, optionType, strikePrice,
                        startDate, endDate, startPrice, endPrice,
                        totalChange, avgIV, maxIV, minIV, totalSnapshots
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    osiKey, symbol, optionType, strike, startDate, endDate,
                    startPrice, endPrice, totalChange, avgIV, maxIV, minIV, len(rows)
                ))

                self.logger.logMessage(f"Archived lifespan for {osiKey}")

            conn.commit()
            conn.close()
            self.logger.logMessage("Processed all completed options")
        except Exception as e:
            self.logger.logMessage("Error processing completed options")
            self.logger.logMessage(e)


# -------------------------------
# Reporting
# -------------------------------
def get_summary(db_path):
    """Return summary stats for both tables."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM option_snapshots")
    total_snapshots = c.fetchone()[0]

    c.execute("SELECT COUNT(DISTINCT osiKey) FROM option_snapshots")
    unique_options = c.fetchone()[0]

    c.execute("SELECT COUNT(DISTINCT symbol) FROM option_snapshots")
    unique_symbols = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM option_lifespans")
    completed_count = c.fetchone()[0]

    conn.close()

    return {
        "total_snapshots": total_snapshots,
        "unique_options": unique_options,
        "unique_symbols": unique_symbols,
        "completed_lifespans": completed_count,
        "last_check": datetime.utcnow().isoformat()
    }
