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
        self.logger.logMessage("Creating Lifespan database if it doesn't exist")
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
                    avgDelta REAL,
                    avgGamma REAL,
                    avgTheta REAL,
                    avgVega REAL,
                    avgRho REAL,
                    avgBidAskSpread REAL,
                    avgVolume REAL,
                    avgOpenInterest REAL,
                    avgMidPrice REAL,
                    avgMoneyness REAL,
                    totalSnapshots INTEGER
                )
            """)
            conn.commit()
            conn.close()
            self.logger.logMessage("Lifespan Database created / exists")
        except Exception as e:
            self.logger.logMessage("Error creating lifespan database")
            self.logger.logMessage(str(e))

            


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
    def process_completed_options(self, min_lifespan_days: int = 5):
        try:
            self.logger.logMessage("Processing completed options")
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()

            # Step 1: Find all fully expired options
            c.execute("""
                SELECT osiKey
                FROM option_snapshots
                GROUP BY osiKey
                HAVING MAX(daysToExpiration) <= 0
            """)
            completed = [row[0] for row in c.fetchall()]

            for osiKey in completed:
                # Skip if already archived
                c.execute("SELECT 1 FROM option_lifespans WHERE osiKey = ?", (osiKey,))
                if c.fetchone():
                    continue

                # Step 2: Fetch all snapshots
                c.execute("""
                    SELECT timestamp, lastPrice, iv, delta, gamma, theta, vega, rho, bid, ask,
                        volume, openInterest, midPrice, moneyness
                    FROM option_snapshots
                    WHERE osiKey = ?
                    ORDER BY timestamp ASC
                """, (osiKey,))
                rows = c.fetchall()
                if not rows:
                    continue

                start_ts = datetime.fromisoformat(rows[0][0])
                end_ts = datetime.fromisoformat(rows[-1][0])
                lifespan_days = (end_ts - start_ts).days

                # Step 3: Skip / delete options shorter than minimum lifespan
                if lifespan_days < min_lifespan_days:
                    self.logger.logMessage(f"Skipping short-lived option {osiKey} ({lifespan_days} days)")
                    c.execute("DELETE FROM option_snapshots WHERE osiKey = ?", (osiKey,))
                    continue

                # Step 4: Aggregate statistics
                last_prices = [r[1] for r in rows]
                iv_values = [r[2] for r in rows if r[2] is not None]
                deltas = [r[3] for r in rows]
                gammas = [r[4] for r in rows]
                thetas = [r[5] for r in rows]
                vegas = [r[6] for r in rows]
                rhos = [r[7] for r in rows]
                bids = [r[8] for r in rows]
                asks = [r[9] for r in rows]
                volumes = [r[10] for r in rows]
                open_interests = [r[11] for r in rows]
                mid_prices = [r[12] for r in rows if r[12] is not None]
                moneyness = [r[13] for r in rows if r[13] is not None]

                avg_bid_ask_spread = sum([a - b for a, b in zip(asks, bids)]) / len(rows)

                lifespan_record = (
                    osiKey,
                    rows[0][1],  # symbol
                    rows[0][2],  # optionType
                    rows[0][3],  # strikePrice
                    start_ts.isoformat(),
                    end_ts.isoformat(),
                    last_prices[0],  # startPrice
                    last_prices[-1],  # endPrice
                    last_prices[-1] - last_prices[0],  # totalChange
                    sum(iv_values)/len(iv_values) if iv_values else None,
                    max(iv_values) if iv_values else None,
                    min(iv_values) if iv_values else None,
                    sum(deltas)/len(deltas),
                    sum(gammas)/len(gammas),
                    sum(thetas)/len(thetas),
                    sum(vegas)/len(vegas),
                    sum(rhos)/len(rhos),
                    avg_bid_ask_spread,
                    sum(volumes)/len(volumes),
                    sum(open_interests)/len(open_interests),
                    sum(mid_prices)/len(mid_prices) if mid_prices else None,
                    sum(moneyness)/len(moneyness) if moneyness else None,
                    len(rows)
                )

                # Step 5: Insert aggregated record into lifespan table
                c.execute("""
                    INSERT OR REPLACE INTO option_lifespans (
                        osiKey, symbol, optionType, strikePrice,
                        startDate, endDate, startPrice, endPrice, totalChange,
                        avgIV, maxIV, minIV, avgDelta, avgGamma, avgTheta, avgVega, avgRho,
                        avgBidAskSpread, avgVolume, avgOpenInterest, avgMidPrice, avgMoneyness,
                        totalSnapshots
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, lifespan_record)
                
                # Step 5b: Delete snapshots after archiving
                c.execute("DELETE FROM option_snapshots WHERE osiKey = ?", (osiKey,))


            conn.commit()
            conn.close()
            self.logger.logMessage("Processed all completed options successfully")

        except Exception as e:
            self.logger.logMessage("Error processing completed options")
            self.logger.logMessage(str(e))

# -------------------------------
# Reporting
# -------------------------------
def get_summary(db_path):
    logger = getLogger()
    try:
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
        
        c.execute("SELECT COUNT(DISTINCT symbol) FROM option_lifespans")
        distinct_life_opts = c.fetchone()[0]

        conn.close()

        print(f"total snapshots: {total_snapshots}")
        print(f"unique snapshot options: {unique_options}")
        print(f"unique snapshot symbols: {unique_symbols}")
        print(f"completed opt lifespans: {completed_count}")
        print(f"unique option symbols: {distinct_life_opts}")
    except Exception as e:
        logger.logMessage("Error getting option summary data")
