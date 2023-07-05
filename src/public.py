from fastapi import APIRouter
from fastapi.openapi.models import Response
from starlette.responses import JSONResponse

import src
from src import db
from src.commons import settings

router = APIRouter()

@router.get("/progress-state/{metadata_id}")
async def process_deposit(metadata_id: str):
    result = db.find_progress_state_by_metadata_id(settings.DATA_DB_FILE, metadata_id)
    progress_state, pid, urn_nbn, error = result[0]
    if error is None:
        return {"status": progress_state, "pid": pid, "urn-nbn": urn_nbn}
    else:
        return {"metadata-id": metadata_id, "status": progress_state, "error": error}