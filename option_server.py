from fastapi import FastAPI, UploadFile, File
from pathlib import Path
import shutil
import threading
import time
from shared_options import OptionFeature
from pydantic import BaseModel
from typing import List
from processor import OptionDataProcessor

# -----------------------------
# Configuration
# -----------------------------
SAVE_DIR = Path("data")
SAVE_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = "database/options.db"
CHECK_INTERVAL = 5  # seconds

# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI(title="Option Data Ingest Server")

class OptionFeatureBatch(BaseModel):
    options: List[OptionFeature]

# -----------------------------
# File Watcher
# -----------------------------
def file_watcher():
    processor = OptionDataProcessor(
        db_path=DB_PATH,
        incoming_folder=SAVE_DIR
    )

    while True:
        # Find all JSON files in the folder
        for file in SAVE_DIR.glob("*.json"):
            print(f"Processing {file}")
            try:
                processor.ingest_file(file)
                # Delete after ingestion to save space
                file.unlink()
            except Exception as e:
                print(f"Error processing {file}: {e}")
        time.sleep(CHECK_INTERVAL)

@app.on_event("startup")
def startup_event():
    watcher_thread = threading.Thread(target=file_watcher, daemon=True)
    watcher_thread.start()
    print("File watcher started")

# -----------------------------
# API Endpoint
# -----------------------------
@app.post("/api/upload_file")
async def upload_file(file: UploadFile = File(...)):
    try:
        filepath = SAVE_DIR / file.filename

        # Write uploaded file to SAVE_DIR
        with filepath.open("wb") as f:
            shutil.copyfileobj(file.file, f)

        return {"status": "ok", "filename": str(file.filename)}
    except Exception as e:
        return {"status": "error", "message": str(e)}
