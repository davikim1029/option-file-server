from fastapi import FastAPI, UploadFile, File
from pathlib import Path
import shutil
import threading
import time
from contextlib import asynccontextmanager
from typing import List
from pydantic import BaseModel
from shared_options import OptionFeature
from processor import OptionDataProcessor
from analytics.option_processor import OptionAnalyticsProcessor


# -----------------------------
# Configuration
# -----------------------------
SAVE_DIR = Path("data")
SAVE_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = Path("database/options.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

CHECK_INTERVAL = 5  # seconds
LOG_FILE = Path("option_server.log")
MAX_LOG_LINES = 10000  # Keep last 10k lines

from logger.logger_singleton import getLogger
logger = getLogger()

# -----------------------------
# FastAPI app
# -----------------------------
class OptionFeatureBatch(BaseModel):
    options: List[OptionFeature]

# -----------------------------
# File watcher function
# -----------------------------
def file_watcher():
    processor = OptionDataProcessor(
        db_path=str(DB_PATH),
        incoming_folder=SAVE_DIR
    )

    while True:
        for file in SAVE_DIR.glob("*.json"):
            logger.logMessage(f"[File Watcher] Processing {file}")
            try:
                processor.ingest_file(file)
                # Delete after ingestion
                file.unlink()
            except Exception as e:
                logger.logMessage(f"[File Watcher] Error processing {file}: {e}")
        time.sleep(CHECK_INTERVAL)

# -----------------------------
# Log trimming function
# -----------------------------
def trim_log():
    if not LOG_FILE.exists():
        return
    with LOG_FILE.open("r") as f:
        lines = f.readlines()
    if len(lines) > MAX_LOG_LINES:
        # Keep only the last MAX_LOG_LINES
        with LOG_FILE.open("w") as f:
            f.writelines(lines[-MAX_LOG_LINES:])

def log_monitor():
    while True:
        trim_log()
        time.sleep(60)  # check every 60 seconds

# -----------------------------
# Lifespan for FastAPI
# -----------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start file watcher thread
    watcher_thread = threading.Thread(target=file_watcher, daemon=True)
    watcher_thread.start()
    logger.logMessage("File watcher started")

    # Start log monitor thread
    log_thread = threading.Thread(target=log_monitor, daemon=True)
    log_thread.start()
    logger.logMessage("Log monitor started")
    
    # Start analytics processor
    try:
        logger.logMessage("Attempting to create analytics processor")
        db_path = Path("database/options.db")
        analytics_processor = OptionAnalyticsProcessor(db_path=db_path, check_interval=60)
        analytics_processor.start()
        logger.logMessage("Analytics processor started")
    except Exception as e:
        logger.logMessage("Error creating analtics processor")
        logger.logMessage(e)

    yield  # FastAPI app runs here

    # Cleanup on shutdown
    logger.logMessage("Server shutting down...")
    if analytics_processor:
        analytics_processor.stop()
        logger.logMessage("Analytics processor stopped")
    logger.logMessage("Server shutting down...")

app = FastAPI(title="Option Data Ingest Server", lifespan=lifespan)

# -----------------------------
# API endpoint
# -----------------------------
@app.post("/api/upload_file")
async def upload_file(file: UploadFile = File(...)):
    try:
        filepath = SAVE_DIR / file.filename
        with filepath.open("wb") as f:
            shutil.copyfileobj(file.file, f)
        return {"status": "ok", "filename": str(file.filename)}
    except Exception as e:
        return {"status": "error", "message": str(e)}
