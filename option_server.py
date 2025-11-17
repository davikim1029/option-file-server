from fastapi import FastAPI, UploadFile, File
from pathlib import Path
import shutil
import threading
import time
from contextlib import asynccontextmanager
from typing import List
from pydantic import BaseModel
from logging import FileHandler

# Processors
from processors.snapshot_processor import OptionSnapshotProcessor
from processors.lifetime_processor import OptionLifetimeProcessor
from processors.permutation_processor import OptionPermutationProcessor
from shared_options.models.OptionFeature import OptionFeature
from shared_options.log.logger_singleton import getLogger


# -----------------------------
# Configuration
# -----------------------------
SAVE_DIR = Path("data")
SAVE_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = Path("database/options.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

CHECK_INTERVAL = 5  # seconds
MAX_LOG_LINES = 10000

# Logger
logger = getLogger()
LOG_FILE = Path("option_server.log")
for handler in logger.logger.handlers:
    if isinstance(handler, FileHandler):
        LOG_FILE = Path(handler.baseFilename)
        break


# -----------------------------
# Pydantic
# -----------------------------
class OptionFeatureBatch(BaseModel):
    options: List[OptionFeature]


# -----------------------------
# Log trimming
# -----------------------------
def trim_log():
    if not LOG_FILE.exists():
        return
    with LOG_FILE.open("r") as f:
        lines = f.readlines()
    if len(lines) > MAX_LOG_LINES:
        with LOG_FILE.open("w") as f:
            f.writelines(lines[-MAX_LOG_LINES:])


def log_monitor():
    while True:
        trim_log()
        time.sleep(60)


# -----------------------------
# FastAPI lifespan
# -----------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.logMessage("Starting FastAPI lifespan...")

    # ----------------------------------------
    # Start: Snapshot Processor
    # ----------------------------------------
    snapshot_processor = None
    try:
        logger.logMessage("Initializing snapshot processor...")

        snapshot_processor = OptionSnapshotProcessor(
            db_path=str(DB_PATH),
            incoming_folder=SAVE_DIR,
            check_interval=CHECK_INTERVAL
        )
        snapshot_processor.start()

        logger.logMessage("Snapshot processor started")
    except Exception as e:
        logger.logMessage(f"Error starting snapshot processor: {e}")

    # ----------------------------------------
    # Start: Lifetime Processor
    # ----------------------------------------
    lifetime_processor = None
    try:
        logger.logMessage("Initializing lifetime processor...")

        lifetime_processor = OptionLifetimeProcessor(
            db_path=DB_PATH,
            check_interval=60
        )
        lifetime_processor.start()

        logger.logMessage("Lifetime processor started")
    except Exception as e:
        logger.logMessage(f"Error starting lifetime processor: {e}")

    # ----------------------------------------
    # Start: Permutation Processor
    # ----------------------------------------
    permutation_processor = None
    try:
        logger.logMessage("Initializing permutation processor...")

        permutation_processor = OptionPermutationProcessor(
            check_interval=45
        )
        permutation_processor.start()

        logger.logMessage("Permutation processor started")
    except Exception as e:
        logger.logMessage(f"Error starting permutation processor: {e}")

    # ----------------------------------------
    # Start: Log monitor
    # ----------------------------------------
    log_thread = threading.Thread(target=log_monitor, daemon=True)
    log_thread.start()
    logger.logMessage("Log monitor started")

    # ----------------------------------------
    # Yield to FastAPI
    # ----------------------------------------
    yield

    # ----------------------------------------
    # Shutdown
    # ----------------------------------------
    logger.logMessage("Server shutdown started...")

    for proc, name in [
        (snapshot_processor, "Snapshot"),
        (lifetime_processor, "Lifetime"),
        (permutation_processor, "Permutation")
    ]:
        if proc:
            try:
                proc.stop()
                logger.logMessage(f"{name} processor stopped")
            except Exception as e:
                logger.logMessage(f"Error stopping {name} processor: {e}")

    logger.logMessage("Shutdown complete.")


# -----------------------------
# FastAPI App
# -----------------------------
app = FastAPI(lifespan=lifespan)


# -----------------------------
# API endpoint
# -----------------------------
@app.post("/api/upload_file")
async def upload_file(file: UploadFile = File(...)):
    try:
        filepath = SAVE_DIR / file.filename
        with filepath.open("wb") as f:
            shutil.copyfileobj(file.file, f)
        return {"status": "ok", "filename": file.filename}
    except Exception as e:
        return {"status": "error", "message": str(e)}
