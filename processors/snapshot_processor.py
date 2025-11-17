import threading
import time
import sqlite3
from pathlib import Path
import json
from datetime import datetime
from typing import Union

from shared_options.models.OptionFeature import OptionFeature
from shared_options.log.logger_singleton import getLogger
from logging import FileHandler


logger = getLogger()

# Resolve actual log file from logger
LOG_FILE = Path("option_server.log")
for handler in logger.logger.handlers:
    if isinstance(handler, FileHandler):
        LOG_FILE = Path(handler.baseFilename)
        break


class OptionSnapshotProcessor(threading.Thread):
    """
    Background processor that watches a folder and ingests snapshot files
    into the option_snapshots database.
    """

    def __init__(
        self,
        db_path: Union[str, Path],
        incoming_folder: Union[str, Path],
        check_interval: int = 30
    ):
        super().__init__(daemon=True)

        self.db_path = Path(db_path)
        self.folder = Path(incoming_folder)
        self.check_interval = check_interval

        # ensure necessary paths exist
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.folder.mkdir(parents=True, exist_ok=True)

        self._stop_event = threading.Event()
        self.logger = logger

        self._init_db()

    # ----------------------------------------------------------------------
    # Database Init
    # ----------------------------------------------------------------------
    def _init_db(self):
        """Creates the option_snapshots table if missing."""
        try:
            self.logger.logMessage("[SnapshotProcessor] Checking/creating option_snapshots table")
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
            self.logger.logMessage("[SnapshotProcessor] option_snapshots table ready")

        except Exception as e:
            self.logger.logMessage("[SnapshotProcessor] DB init error")
            self.logger.logMessage(e)

    # ----------------------------------------------------------------------
    # Thread Control
    # ----------------------------------------------------------------------
    def stop(self):
        """Signal the background thread to stop cleanly."""
        self._stop_event.set()

    # ----------------------------------------------------------------------
    # Thread Run Loop
    # ----------------------------------------------------------------------
    def run(self):
        self.logger.logMessage("[SnapshotProcessor] Started")

        while not self._stop_event.is_set():
            try:
                # process files
                for file in self.folder.glob("*.json"):
                    try:
                        self.logger.logMessage(f"[SnapshotProcessor] Processing {file.name}")
                        self.ingest_file(file)
                        file.unlink(missing_ok=True)
                    except Exception as e:
                        self.logger.logMessage(f"[SnapshotProcessor] Error processing {file.name}")
                        self.logger.logMessage(e)

                # sleep between cycles
                time.sleep(self.check_interval)

            except Exception as e:
                self.logger.logMessage("[SnapshotProcessor] Loop crash (recovering)")
                self.logger.logMessage(e)
                time.sleep(5)

        self.logger.logMessage("[SnapshotProcessor] Stopped")

    # ----------------------------------------------------------------------
    # Ingestion Logic
    # ----------------------------------------------------------------------
    def ingest_file(self, file_path: Union[str, Path]):
        """Read JSON snapshot file and store in the DB."""
        file_path = Path(file_path)
        self.logger.logMessage(f"[SnapshotProcessor] Ingesting {file_path.name}")

        if not file_path.exists():
            self.logger.logMessage(f"[SnapshotProcessor] File not found: {file_path.name}")
            return

        try:
            with file_path.open("r") as f:
                data = json.load(f)
        except Exception as e:
            self.logger.logMessage(f"[SnapshotProcessor] JSON load error for {file_path.name}")
            self.logger.logMessage(e)
            return

        if not data:
            self.logger.logMessage(f"[SnapshotProcessor] Empty JSON file: {file_path.name}")
            return

        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()

            for entry in data:
                option = OptionFeature(**entry)

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
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    option.osiKey, ts, option.symbol, option.optionType, option.strikePrice,
                    option.lastPrice, option.bid, option.ask, option.bidSize, option.askSize,
                    option.volume, option.openInterest, option.nearPrice, option.inTheMoney,
                    option.delta, option.gamma, option.theta, option.vega, option.rho, option.iv,
                    option.daysToExpiration, option.spread, option.midPrice, option.moneyness
                ))

            conn.commit()
            conn.close()
            self.logger.logMessage(f"[SnapshotProcessor] {file_path.name} added to DB")

        except Exception as e:
            self.logger.logMessage(f"[SnapshotProcessor] DB insert error for {file_path.name}")
            self.logger.logMessage(e)
