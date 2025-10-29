from fastapi import FastAPI
from datetime import datetime
from fastapi import FastAPI, UploadFile, File
from pathlib import Path
import shutil
import uvicorn
from shared_options import OptionFeature
from pydantic import BaseModel
from typing import List

# Where JSON files will be saved
SAVE_DIR = Path("/Users/daviskim/Documents/GitHub/options/Server/data")
SAVE_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Option Data Ingest Server")

class OptionFeatureBatch(BaseModel):
    options: List[OptionFeature]

app = FastAPI()

@app.post("/api/upload_file")
async def upload_file(file: UploadFile = File(...)):
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{file.filename.split('.')[0]}_{timestamp}.json"
        filepath = SAVE_DIR / filename

        # Write uploaded file to target folder
        with filepath.open("wb") as f:
            shutil.copyfileobj(file.file, f)

        return {"status": "ok", "filename": str(filename)}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
