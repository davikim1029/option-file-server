import sqlite3
from pathlib import Path
import json
from datetime import datetime
from shared_options import OptionFeature
from typing import Union
from logger.logger_singleton import getLogger
from logging import FileHandler

LOG_FILE = Path("option_server.log")

import sys
from pathlib import Path

sys.stdout = open(LOG_FILE, "a", buffering=1)  # line-buffered
sys.stderr = sys.stdout

# -----------------------------
# Setup Logger
# -----------------------------
logger = getLogger()
LOG_FILE = Path("option_server.log")
for handler in logger.logger.handlers:
    if isinstance(handler, FileHandler):
        LOG_FILE = Path(handler.baseFilename)
        break # Assuming you only care about the first FileHandler found


class OptionDataProcessor:
    def __init__(self, db_path: Union[str, Path], incoming_folder: Union[str, Path]):
        self.db_path = Path(db_path)
        self.incoming_folder = Path(incoming_folder)
        self.db_path.parent.mkdir(exist_ok=True, parents=True)
        self.logger = logger
        self._init_db()

    def _init_db(self):
        """Create options table if it doesn't exist."""
        try:
            self.logger.logMessage("[Processor] Attempting to create option database if does not exist")
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("""
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
                )
            """)
            conn.commit()
            conn.close()
            self.logger.logMessage("[Processor] Option Database created or exists")
        except Exception as e:
            self.logger.logMessage("[Processor] Failed to create option database")
            self.logger.logMessage(e)

    def ingest_file(self, file_path: Union[str, Path]):
        """Read a JSON file of OptionFeature objects and store in DB."""
        self.logger.logMessage(f"[Processor] Attempting to ingest file {file_path.name}")
        file_path = Path(file_path)
        if not file_path.exists():
            self.logger.logMessage(f"[Processor] File path {file_path.name} does not exist")
            return

        try:
            with file_path.open("r") as f:
                data = json.load(f)
        except Exception as e:
            self.logger.logMessage(f"[Processor] Could not load file {file_path.name}")
            self.logger.logMessage(f"Exception: {e}")

        if data:
            try:
                conn = sqlite3.connect(self.db_path)
                c = conn.cursor()

                for entry in data:
                    # Create OptionFeature instance for validation / typing
                    option = OptionFeature(**entry)

                    # Ensure timestamp exists, fallback to now
                    ts = getattr(option, "timestamp", None)
                    if ts is None:
                        ts = datetime.utcnow().isoformat()
                    elif isinstance(ts, datetime):
                        ts = ts.isoformat()

                    c.execute("""
                        INSERT OR REPLACE INTO option_snapshots (
                            osiKey, timestamp, symbol, optionType, strikePrice,
                            lastPrice, bid, ask, bidSize, askSize, volume, openInterest,
                            nearPrice, inTheMoney, delta, gamma, theta, vega, rho, iv,
                            daysToExpiration, spread, midPrice, moneyness
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                    """, (
                        option.osiKey, ts, option.symbol, option.optionType, option.strikePrice,
                        option.lastPrice, option.bid, option.ask, option.bidSize, option.askSize,
                        option.volume, option.openInterest, option.nearPrice, option.inTheMoney,
                        option.delta, option.gamma, option.theta, option.vega, option.rho, option.iv,
                        option.daysToExpiration, option.spread, option.midPrice, option.moneyness
                    ))

                conn.commit()
                conn.close()
                self.logger.logMessage(f"[Processor] File {file_path.name} added to database")
            except Exception as e:
                self.logger.logMessage(f"[Processor] Error loading file {file_path.name} into DB")
                self.logger.logMessage(e)


