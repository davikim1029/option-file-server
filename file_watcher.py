import threading
import time
from pathlib import Path
from fastapi import FastAPI
from processor import OptionDataProcessor

app = FastAPI()
WATCH_FOLDER = Path("data")
DB_PATH = "database/options.db"

# Initialize processor once
processor = OptionDataProcessor(
    db_path=DB_PATH,
    incoming_folder=WATCH_FOLDER
)

def file_watcher():
    while True:
        processor.ingest_all_files()  # ingest everything currently in folder
        time.sleep(5)  # check every 5 seconds

@app.on_event("startup")
def startup_event():
    watcher_thread = threading.Thread(target=file_watcher, daemon=True)
    watcher_thread.start()
    print("File watcher started")

@app.get("/")
def root():
    return {"status": "server running"}
