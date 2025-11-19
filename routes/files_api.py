# routes/files_api.py

from fastapi import APIRouter, UploadFile, File, HTTPException
from pathlib import Path
import shutil

router = APIRouter(prefix="/api", tags=["file-ingest"])

SAVE_DIR = Path("data")

@router.post("/upload_file")
async def upload_file(file: UploadFile = File(...)):
    """
    Accept a JSON file upload and save it to SAVE_DIR.
    SnapshotProcessor will auto-ingest it on its next cycle.
    """
    try:
        SAVE_DIR.mkdir(parents=True, exist_ok=True)
        dest = SAVE_DIR / file.filename

        with dest.open("wb") as f:
            shutil.copyfileobj(file.file, f)

        return {"status": "ok", "filename": file.filename}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
