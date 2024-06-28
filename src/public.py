import json

from fastapi import APIRouter
from starlette.responses import Response

# from src import db
from src.commons import logger, data, db_manager, LOG_LEVEL_DEBUG, LOG_NAME_PS

# import logging

router = APIRouter()


@router.get("/available-modules")
async def get_modules_list():
    return sorted(list(data.keys()))


@router.get("/progress-state/{owner_id}")
async def progress_state(owner_id: str):
    rows = db_manager.find_owner_assets(owner_id)
    if rows:
        return rows
    return []


@router.get("/dataset/{datasetId}")
async def find_dataset(datasetId: str):
    # logging.debug(f'find_metadata_by_metadata_id - metadata_id: {metadata_id}')
    logger(f'find_metadata_by_metadata_id - metadata_id: {datasetId}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    dataset = db_manager.find_dataset_and_targets(datasetId)
    # metadata_json = json.loads(db_manager.find_dataset_by_id(datasetId))
    # files_metadata = metadata_json.get("file-metadata")
    # if files_metadata:
    #     for file_metadata in files_metadata:
    #         filename = file_metadata["name"]
    #         uploaded_status = db_manager.find_file_upload_status_by_dataset_id_and_filename(metadata_id, filename)
    #         if uploaded_status:
    #             file_metadata.update({"uploaded": True})
    #         else:
    #             # logging.error(f"No file for metadata_id: {metadata_id} and filename: {filename}")
    #             logger(f'input: {input}', 'error', LOG_NAME_PS)
    #

    # y =
    if dataset.dataset_id:
        dataset.md = json.loads(dataset.md)
        return Response(content=dataset.model_dump_json(by_alias=True), media_type="application/json")
    return {}
