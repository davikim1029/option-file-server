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

import sys
from pathlib import Path

sys.stdout = open(LOG_FILE, "a", buffering=1)  # line-buffered
sys.stderr = sys.stdout


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
            print(f"[File Watcher] Processing {file}")
            try:
                processor.ingest_file(file)
                # Delete after ingestion
                file.unlink()
            except Exception as e:
                print(f"[File Watcher] Error processing {file}: {e}")
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
    print("File watcher started")

    # Start log monitor thread
    log_thread = threading.Thread(target=log_monitor, daemon=True)
    log_thread.start()
    print("Log monitor started")

    yield  # FastAPI app runs here

    # Optional cleanup on shutdown
    print("Server shutting down...")

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
