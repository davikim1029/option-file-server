# server (e.g. api_server.py) — only the modified pieces shown

from fastapi import FastAPI, UploadFile, File
from pathlib import Path
from contextlib import asynccontextmanager
import threading, time, shutil
from logging import FileHandler
from shared_options.log.logger_singleton import getLogger
import traceback
from processors.snapshot_processor import OptionSnapshotProcessor
from processors.lifetime_processor import OptionLifetimeProcessor
from processors.permutation_processor import OptionPermutationProcessor

# ... your existing config ...
SAVE_DIR = Path("data")
DB_PATH = Path("database/options.db")
# ensure dirs exist...
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

logger = getLogger()
LOG_FILE = Path("option_server.log")
for handler in logger.logger.handlers:
    if isinstance(handler, FileHandler):
        LOG_FILE = Path(handler.baseFilename)
        break

app = FastAPI()  # we will pass lifespan below

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.logMessage("Starting FastAPI lifespan...")

    # snapshot processor (example: using same constructor you had)
    snapshot_processor = OptionSnapshotProcessor(
        db_path=str(DB_PATH),
        incoming_folder=SAVE_DIR,
        check_interval=5
    )
    snapshot_processor.start()
    logger.logMessage("Snapshot processor started")

    # lifetime processor
    lifetime_processor = OptionLifetimeProcessor(
        db_path=str(DB_PATH),
        check_interval=60
    )
    lifetime_processor.start()
    logger.logMessage("Lifetime processor started")

    # permutation processor - ensure we pass DB_PATH
    permutation_processor = OptionPermutationProcessor(
        db_path=str(DB_PATH),
        check_interval=45,
        batch_commit_size=100,
        analyze_after_commits=1,
        vacuum_interval_hours=24
    )
    permutation_processor.start()
    logger.logMessage("Permutation processor started")

    # attach to app.state so handlers can access
    app.state.snapshot_processor = snapshot_processor
    app.state.lifetime_processor = lifetime_processor
    app.state.permutation_processor = permutation_processor

    # start log monitor thread (unchanged)
    def log_monitor():
        while True:
            # trim log file here...
            time.sleep(60)
    log_thread = threading.Thread(target=log_monitor, daemon=True)
    log_thread.start()

    try:
        yield
    except Exception as e:
        logger.logMessage(f"Lifespan crash: {e}\n{traceback.format_exc()}")
    finally:
        logger.logMessage("Server shutdown started...")
        for proc in (snapshot_processor, lifetime_processor, permutation_processor):
            try:
                if proc:
                    proc.stop()
            except Exception as e:
                logger.logMessage(f"Error stopping processor: {e}")
        logger.logMessage("Server shutdown complete.")

# assign lifespan to app
app = FastAPI(lifespan=lifespan)

# -----------------------------
# Permutation status endpoints
# -----------------------------
@app.get("/perm/status")
def permutation_status():
    proc = getattr(app.state, "permutation_processor", None)
    if not proc:
        return {"status": "error", "message": "Permutation processor not running"}
    return proc.get_status()

@app.get("/perm/counts")
def permutation_counts():
    proc = getattr(app.state, "permutation_processor", None)
    if not proc:
        return {"status": "error", "message": "Permutation processor not running"}
    return {
        "rows_inserted": proc.get_status()["total_rows_inserted"],
        "osis_processed": proc.get_status()["total_osis_processed"]
    }
