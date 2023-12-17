import json
# import logging

from fastapi import APIRouter

# from src import db
from src.commons import logger, settings, data, db_manager

router = APIRouter()


@router.get("/available-modules")
async def get_modules_list():
    return sorted(list(data.keys()))

@router.get("/progress-state/{owner_id}")
async def process_deposit(owner_id: str):
    rows = db_manager.find_progress_state_by_owner_id(owner_id)
    results = []
    for row in rows:
        results.append(json.loads(row[0]))
    # logging.debug(f'results: {results}')
    return results


@router.get("/dataset/{datasetId}")
async def find_dataset(datasetId: str):
    # logging.debug(f'find_metadata_by_metadata_id - metadata_id: {metadata_id}')
    logger(f'find_metadata_by_metadata_id - metadata_id: {datasetId}', 'debug', 'ps')
    metadata_json = json.loads(db_manager.find_dataset_by_id(datasetId))
    files_metadata = metadata_json.get("file-metadata")
    if files_metadata:
        for file_metadata in files_metadata:
            filename = file_metadata["name"]
            uploaded_status = db_manager.find_file_upload_status_by_dataset_id_and_filename(metadata_id, filename)
            if uploaded_status:
                file_metadata.update({"uploaded": True})
            else:
                # logging.error(f"No file for metadata_id: {metadata_id} and filename: {filename}")
                logger(f'input: {input}', 'error', 'ps')
                file_metadata.update({"uploaded": False})
    return metadata_json
