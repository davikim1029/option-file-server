import sqlite3
from pathlib import Path
import json
from datetime import datetime
from shared_options import OptionFeature
from typing import Union

class OptionDataProcessor:
    def __init__(self, db_path: Union[str, Path], incoming_folder: Union[str, Path]):
        self.db_path = Path(db_path)
        self.incoming_folder = Path(incoming_folder)
        self.db_path.parent.mkdir(exist_ok=True, parents=True)
        self._init_db()

    def _init_db(self):
        """Create options table if it doesn't exist."""
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

    def ingest_file(self, file_path: Union[str, Path]):
        """Read a JSON file of OptionFeature objects and store in DB."""
        file_path = Path(file_path)
        if not file_path.exists():
            return

        with file_path.open("r") as f:
            data = json.load(f)

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

    def get_lifetime_for_osi(self, osi_key: str):
        """Return all snapshots for a given OSI key, ordered by timestamp."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            SELECT * FROM option_snapshots
            WHERE osiKey = ?
            ORDER BY timestamp ASC
        """, (osi_key,))
        rows = c.fetchall()
        conn.close()
        return rows
