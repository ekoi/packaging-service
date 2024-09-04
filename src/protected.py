# Import necessary modules and packages
# Import necessary libraries and modules
import hashlib
import json
import mimetypes
import os
import shutil
from pathlib import Path
import threading
import time
from datetime import datetime
from typing import Callable, Awaitable

import jmespath
import requests
from fastapi import APIRouter, Request, UploadFile, Form, File, HTTPException
from fastapi.responses import JSONResponse
from starlette.responses import FileResponse

from src.commons import settings, logger, data, db_manager, get_class, assistant_repo_headers, handle_ps_exceptions, \
    send_mail, LOG_LEVEL_DEBUG, LOG_NAME_PS, delete_symlink_and_target
from src.dbz import TargetRepo, DataFile, Dataset, ReleaseVersion, DepositStatus, FilePermissions, \
    DatasetWorkState, DataFileWorkState
from src.models.app_model import ResponseDataModel, InboxDatasetDataModel
# Import custom modules and classes
from src.models.assistant_datamodel import RepoAssistantDataModel, Target
from src.models.target_datamodel import TargetsCredentialsModel

# Create an API router instance
router = APIRouter()


# Endpoint to register a bridge module
@router.post("/register-bridge-module/{name}/{overwrite}")
async def register_module(name: str, bridge_file: Request, overwrite: bool | None = False) -> {}:
    logger(f'Registering {name}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    if not overwrite and name in data["bridge-modules"]:
        raise HTTPException(status_code=400,
                            detail=f'The {name} is already exist. Consider /register-bridge-module/{name}/true')

    if bridge_file.headers['Content-Type'] != 'text/x-python':
        raise HTTPException(status_code=400, detail="Unsupported content type")

    m_file = await bridge_file.body()
    bridge_path = os.path.join(settings.MODULES_DIR, name)
    with open(bridge_path, "w+") as file:
        file.write(m_file.decode())

    if mimetypes.guess_type(bridge_path)[0] != 'text/x-python':
        os.remove(bridge_path)
        raise HTTPException(status_code=400, detail='Unsupported file type')

    return {"status": "ok", "bridge-module-name": name}


# Helper function to process inbox dataset metadata
@handle_ps_exceptions
async def get_inbox_dataset_dc(request: Request, release_version: ReleaseVersion) -> (
        Callable)[[Request, ReleaseVersion], Awaitable[InboxDatasetDataModel]]:
    req_body = await request.json()
    # logger(f'METADATA: \n{req_body}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    return InboxDatasetDataModel(assistant_name=request.headers.get('assistant-config-name'),
                                 release_version=release_version, owner_id=request.headers.get('user-id'),
                                 title=jmespath.search('title', req_body),
                                 target_creds=request.headers.get('targets-credentials'), metadata=req_body)


# Endpoint to process inbox dataset metadata
@router.post("/inbox/dataset/{release_version}")
async def process_inbox_dataset_metadata(request: Request, release_version: ReleaseVersion) -> {}:
    logger(f'######## Process inbox dataset metadata for release version: {release_version}', LOG_LEVEL_DEBUG,
           LOG_NAME_PS)
    idh = await get_inbox_dataset_dc(request, release_version)
    file_metadata = jmespath.search('"file-metadata"[*]', idh.metadata)
    logger(f'--- Number of file_metadata: {len(file_metadata)}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    datasetId = jmespath.search("id", idh.metadata)

    logger(f'Start inbox for metadata id: {datasetId} - release version: {release_version} - assistant name: '
           f'{idh.assistant_name}', LOG_LEVEL_DEBUG, LOG_NAME_PS)

    if db_manager.is_dataset_published(datasetId):
        raise HTTPException(status_code=400, detail='Dataset is already published.')
    repo_config = retrieve_targets_configuration(idh.assistant_name)
    repo_assistant = RepoAssistantDataModel.model_validate_json(repo_config)

    dataset_folder = os.path.join(settings.DATA_TMP_BASE_DIR, repo_assistant.app_name, datasetId)
    if not os.path.exists(dataset_folder):
        os.makedirs(dataset_folder)

    db_recs_target_repo = process_target_repos(repo_assistant, idh.target_creds)
    db_record_metadata, registered_files = process_metadata_record(datasetId, idh, repo_assistant, dataset_folder)
    process_db_records(datasetId, db_record_metadata, db_recs_target_repo, registered_files)
    x = db_manager.is_dataset_ready(datasetId)
    logger(f'***************  Dataset READY:  {x}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    y = db_manager.are_files_uploaded(datasetId)
    logger(f'-------- ALL Files are UPLOADED:  {y}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    z = count_files_in_directory(dataset_folder)
    logger(f'======= Number files in DIRECTORY: {z}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    k = len(db_manager.find_uploaded_files(datasetId))
    logger(f'+++++++ Number files is DATABASE: {k}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    tus_files = list_files_with_suffix(settings.DATA_TMP_BASE_TUS_FILES_DIR, f'{datasetId}.{repo_assistant.app_name}')
    number_of_tus_files = len(tus_files)
    logger(f'!!!!! Number of TUS files: {number_of_tus_files}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    if z == k:
        logger(f'>>>>>>>> FILES IN DATASET ARE READY TO INGEST ---- ', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    else:
        logger(f'<<<<<<<< FILES IN Dataset are NOT ready TO INGEST ---- Number files MISMATCH !!!!!!! ',
               LOG_LEVEL_DEBUG, LOG_NAME_PS)

    if db_manager.is_dataset_ready(datasetId) and db_manager.are_files_uploaded(datasetId):
        logger(f'SUBMIT DATASET with version {release_version.name} is_dataset_ready {datasetId}', LOG_LEVEL_DEBUG,
               LOG_NAME_PS)
        bridge_task(datasetId, f"/inbox/dataset/{idh.release_version}")
    else:
        logger(f'NOT READY to submit dataset with version {release_version.name} datasetId: {datasetId} '
               f'\nNumber still registered: {len(db_manager.find_registered_files(datasetId))}', LOG_LEVEL_DEBUG,
               LOG_NAME_PS)
    # registerd_files = db_manager.find_registered_files(datasetId)
    # if release_version == ReleaseVersion.PUBLISH and (len(registerd_files) > 0):
    #     rfs = [f.name for f in registerd_files]
    #     logger(f'NOT READY to publish dataset with version {release_version.name} datasetId: {datasetId} '
    #            f'\n!!!! --- Not yet uploaded: {rfs}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    #
    #     raise HTTPException(status_code=400, detail=f'Not all files are uploaded. Missing files: {rfs}')

    rdm = ResponseDataModel(status="OK")
    rdm.dataset_id = datasetId
    rdm.start_process = db_manager.is_dataset_ready(datasetId)
    return rdm.model_dump(by_alias=True)


@router.delete("/inbox/dataset/{metadata_id}")
def delete_dataset_metadata(request: Request, metadata_id: str):
    logger(f'Delete dataset: {metadata_id}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    user_id = request.headers.get('user-id')
    if not user_id:
        raise HTTPException(status_code=401, detail='No user id provided')
    if metadata_id not in db_manager.find_dataset_ids_by_owner(user_id):
        raise HTTPException(status_code=404, detail='No Dataset found')
    target_repo = db_manager.find_target_repos_by_dataset_id(metadata_id)
    if not target_repo or target_repo[0].deposit_status not in (DepositStatus.ACCEPTED, DepositStatus.DEPOSITED,
                                                                DepositStatus.FINISH):
        dataset_folder = os.path.join(settings.DATA_TMP_BASE_DIR, db_manager.find_dataset(metadata_id).app_name,
                                      metadata_id)
        logger(f'Delete dataset folder: {dataset_folder}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        if os.path.exists(dataset_folder):
            delete_symlink_and_target(dataset_folder)
        else:
            logger(f'Dataset folder: {dataset_folder} not found', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        db_manager.delete_by_dataset_id(metadata_id)
        if os.path.exists(dataset_folder):
            logger(f'Delete dataset folder: {dataset_folder}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
            shutil.rmtree(dataset_folder)
        else:
            logger(f'Dataset folder: {dataset_folder} not found', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        return {"status": "ok", "metadata-id": metadata_id}
    raise HTTPException(status_code=404, detail=f'Delete of {metadata_id} is not allowed.')


@handle_ps_exceptions
def process_db_records(datasetId, db_record_metadata, db_recs_target_repo, registered_files) -> type(None):
    if not db_manager.is_dataset_exist(datasetId):
        logger(f'Insert dataset and target repo records for {datasetId}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        db_manager.insert_dataset_and_target_repo(db_record_metadata, db_recs_target_repo)
    else:
        logger(f'Update dataset and target repo records for {datasetId}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        db_manager.update_metadata(db_record_metadata)
        db_manager.replace_targets_record(datasetId, db_recs_target_repo)
    if registered_files:
        logger(f'********** INSERT datafiles records for {datasetId}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        logger(f' ===== NUMBER registered_files: {len(registered_files)}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        try:
            db_manager.insert_datafiles(registered_files)
            logger(f'SUCCESSFUL  INSERT datafiles records for {datasetId}, number of files: {len(registered_files)}',
                   LOG_LEVEL_DEBUG, LOG_NAME_PS)
        except ValueError as e:
            logger(f'Error inserting datafiles: {e}', 'error', LOG_NAME_PS)


@handle_ps_exceptions
def process_metadata_record(datasetId, idh, repo_assistant, tmp_dir):
    logger(f'Processing metadata record for {datasetId}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    registered_files = []
    file_names_from_input = jmespath.search('"file-metadata"[*].name', idh.metadata)
    file_names = []
    for file_name in file_names_from_input:
        data_file = db_manager.find_file_by_name(datasetId, file_name)
        if data_file:
            # logger(f'+++++ File {file_name} already exist', LOG_LEVEL_DEBUG, LOG_NAME_PS)
            escaped_file_name = file_name.replace('"', '\\"')
            f_permission = jmespath.search(f'"file-metadata"[?name == `{escaped_file_name}`].private', idh.metadata)
            permission = FilePermissions.PRIVATE if f_permission[0] else FilePermissions.PUBLIC
            db_manager.update_file_permission(datasetId, file_name, permission)
            continue
        else:
            file_names.append(file_name)

    logger(f'NUMBER OF file_names: {len(file_names)}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    already_uploaded_files_name = db_manager.execute_l(datasetId)
    # already_uploaded_files_name = [f.name for f in x]
    logger(f'NUMBER already_uploaded_files: {len(already_uploaded_files_name)}', LOG_LEVEL_DEBUG, LOG_NAME_PS)

    files_name_to_be_deleted = set(already_uploaded_files_name) - set(file_names_from_input)
    logger(
        f'>>> DELETE -- files_name_to_be_deleted: {len(files_name_to_be_deleted)} --LIST:  {files_name_to_be_deleted}',
        LOG_LEVEL_DEBUG, LOG_NAME_PS)
    files_name_to_be_added = set(file_names) - set(already_uploaded_files_name)
    logger(f'--- ADDED---- number of files_name_to_be_added: {len(files_name_to_be_added)}', LOG_LEVEL_DEBUG,
           LOG_NAME_PS)

    for f_name in files_name_to_be_deleted:
        file_path = os.path.join(tmp_dir, f_name)
        if os.path.exists(file_path):
            delete_symlink_and_target(file_path)
            logger(f'{file_path} is deleted', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        else:
            logger(f'{file_path} not found', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        db_manager.delete_datafile(datasetId, f_name)

    for f_name in files_name_to_be_added:
        file_path = os.path.join(tmp_dir, f_name)
        # Escape special characters in the filename
        escaped_filename = f_name.replace('"', '\\"')

        # JMESPath expression
        expression = f"[?name == '{escaped_filename}'].private"
        f_permission = jmespath.search(f'"file-metadata"[?name == `{escaped_filename}`].private', idh.metadata)
        permission = FilePermissions.PRIVATE if f_permission[0] else FilePermissions.PUBLIC
        registered_files.append(DataFile(name=f_name, path=file_path, ds_id=datasetId, permissions=permission))

    logger(f'registered_files: {registered_files}', LOG_LEVEL_DEBUG, LOG_NAME_PS)

    # Update file permission
    already_uploaded_files = db_manager.find_uploaded_files(datasetId)
    logger(f'>>>>>Number of already_uploaded_files: {len(already_uploaded_files)}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    # for f in already_uploaded_files:
    #     f_permission = jmespath.search(f'"file-metadata"[?name == `{f.name}`].private', idh.metadata)
    #     permission = FilePermissions.PRIVATE if f_permission[0] else FilePermissions.PUBLIC
    #     db_manager.update_file_permission(datasetId, f.name, permission)

    dataset_state = DatasetWorkState.READY if not files_name_to_be_added else DatasetWorkState.NOT_READY
    db_record_metadata = Dataset(id=datasetId, title=idh.title, owner_id=idh.owner_id,
                                 app_name=repo_assistant.app_name, release_version=idh.release_version,
                                 state=dataset_state, md=json.dumps(idh.metadata))
    return db_record_metadata, registered_files


@handle_ps_exceptions
def process_target_repos(repo_assistant, target_creds) -> [TargetRepo]:
    db_recs_target_repo = []
    tgc = {"targets-credentials": json.loads(target_creds)}
    input_target_cred_model = TargetsCredentialsModel.model_validate(tgc)
    for repo_target in repo_assistant.targets:
        if repo_target.bridge_module_class not in data.keys():
            raise HTTPException(status_code=404, detail=f'Module "{repo_target.bridge_module_class}" not found.',
                                headers={})
        target_repo_name = repo_target.repo_name
        logger(f'target_repo_name: {target_repo_name}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        for depositor_cred in input_target_cred_model.targets_credentials:
            if depositor_cred.target_repo_name == repo_target.repo_name and depositor_cred.credentials.username:
                repo_target.username = depositor_cred.credentials.username
            if depositor_cred.target_repo_name == repo_target.repo_name and depositor_cred.credentials.password:
                repo_target.password = depositor_cred.credentials.password

        db_recs_target_repo.append(TargetRepo(name=repo_target.repo_name, url=repo_target.target_url,
                                              display_name=repo_target.repo_display_name,
                                              config=repo_target.model_dump_json(by_alias=True, exclude_none=True)))
    return db_recs_target_repo


def count_files_in_directory(directory: str) -> int:
    try:
        # List all items in the directory
        items = os.listdir(directory)
        # Filter the list to include only files
        files = [item for item in items if os.path.isfile(os.path.join(directory, item))]
        # Return the count of files
        return len(files)
    except FileNotFoundError:
        print(f"The directory {directory} does not exist.")
        return 0
    except Exception as e:
        print(f"An error occurred: {e}")
        return 0


def list_files_with_suffix(directory: str, suffix: str) -> list:
    """
    List all files in the given directory that end with the specified suffix.

    Parameters:
    directory (str): The directory to search in.
    suffix (str): The suffix to filter files by.

    Returns:
    list: A list of file names that end with the given suffix.
    """
    return [file for file in os.listdir(directory) if
            os.path.isfile(os.path.join(directory, file)) and file.endswith(suffix)]


# @router.post("/inbox/file")
# async def process_inbox_dataset_file(datasetId: str = Form(), fileName: str = Form(),
#                                      file: UploadFile = File(...)) -> {}:
#     files_name = []
#     submitted_filename = fileName
#     logger(f"submitted_filename: {submitted_filename} from metadataid: {datasetId}", LOG_LEVEL_DEBUG, LOG_NAME_PS)
#     db_record_metadata = db_manager.find_dataset(datasetId)
#     dataset_folder = os.path.join(settings.DATA_TMP_BASE_DIR, db_record_metadata.app_name, datasetId)
#
#     db_records_uploaded_file = db_manager.find_files(datasetId)
#     # Process the files
#     f_upload_st = []
#     for db_rec_uploaded_f in db_records_uploaded_file:
#         # Save the uploaded file
#         if submitted_filename == db_rec_uploaded_f.name:
#             file_contents = await file.read()
#             md5_hash = hashlib.md5(file_contents).hexdigest()
#             file_path = os.path.join(str(dataset_folder), submitted_filename)
#             with open(file_path, "wb") as f:
#                 f.write(file_contents)
#
#             # NOTES: There are many way to get the mime type of file, This rather the simplest then the best.
#             file_mimetype = mimetypes.guess_type(file_path)[0]
#             db_manager.update_file(df=DataFile(ds_id=datasetId, name=submitted_filename, checksum_value=md5_hash,
#                                                size=os.path.getsize(file_path), mime_type=file_mimetype, path=file_path,
#                                                date_added=datetime.utcnow(), state=DataFileWorkState.UPLOADED))
#             f_upload_st.append({"name": db_rec_uploaded_f.name, "uploaded": True})
#         else:
#             f_upload_st.append(
#                 {"name": db_rec_uploaded_f.name, "uploaded": False}) if db_rec_uploaded_f.date_added is None \
#                 else f_upload_st.append({"name": db_rec_uploaded_f.name, "uploaded": True})
#
#     all_files_uploaded = True
#     for fus in f_upload_st:
#         if fus['uploaded'] is False:
#             all_files_uploaded = False
#             break
#     # Update record
#     if all_files_uploaded:
#         db_manager.set_dataset_ready_for_ingest(datasetId)
#
#     start_process = db_manager.is_dataset_ready(datasetId)
#
#     if start_process:
#         bridge_task(datasetId, f'/inbox/file/{fileName}')
#
#     rdm = ResponseDataModel(status="OK")
#     rdm.dataset_id = datasetId
#     rdm.start_process = start_process
#     return rdm.model_dump(by_alias=True)

import httpx


async def delete_file(file_id: str):
    logger(f"#####-----------------Deleting file {file_id}", LOG_LEVEL_DEBUG, LOG_NAME_PS)
    url = f'{settings.TUS_BASE_URL}/files/{file_id}'
    headers = {"accept": "application/json"}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.delete(url, headers=headers, timeout=10)
            if response.status_code == 204:
                logger(f"-----------------TUS File  successfully deleted. {file_id}", LOG_LEVEL_DEBUG, LOG_NAME_PS)
            else:
                logger(f"------- {file_id}--------------Failed to delete file. Status code: {response.status_code}",
                       LOG_LEVEL_DEBUG, LOG_NAME_PS)
        except Exception as e:
            logger(f"------------- {file_id}-------Error deleting file: {e}", "error", LOG_NAME_PS)


@router.patch("/inbox/files/{metadata_id}/{file_uuid}")
async def update_file_metadata(metadata_id: str, file_uuid: str) -> {}:
    logger(f'>>>>>>> PATCH file metadata for metadata_id: {metadata_id} and file_uuid: {file_uuid}', LOG_LEVEL_DEBUG,
           LOG_NAME_PS)
    tus_file = os.path.join(settings.DATA_TMP_BASE_TUS_FILES_DIR, file_uuid)
    file_info_path = f'{tus_file}.info'
    if not os.path.exists(file_info_path):
        logger(f'!!!FILE INFO NOT FOUND for {file_uuid}: {file_info_path}', "error", LOG_NAME_PS)
        raise HTTPException(status_code=404, detail='File not found')
    if not os.path.exists(tus_file):
        logger(f'!!!FILE NOT FOUND for {file_uuid}: {tus_file}', "error", LOG_NAME_PS)
        raise HTTPException(status_code=404, detail='File not found')
    if os.path.getsize(tus_file) == 0:
        logger(f'!!!FILE SIZE IS 0 for {file_uuid}: {tus_file}', "error", LOG_NAME_PS)
        raise HTTPException(status_code=400, detail='File size is 0')

    with open(file_info_path, "r") as file:
        file_metadata = json.load(file)
    file_name = file_metadata['metadata']['fileName']
    if file_metadata.get('size', 0) == 0:
        logger(f'!!!FILE SIZE IS 0 for {file_uuid}: {tus_file}', "error", LOG_NAME_PS)
        raise HTTPException(status_code=400, detail='File size is 0')

    logger(f'file_name: {file_name}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    if os.path.getsize(tus_file) != file_metadata.get('size', 0):
        logger(f'!!!FILE SIZE MISMATCH for {file_uuid}: {tus_file}', "error", LOG_NAME_PS)
        raise HTTPException(status_code=400, detail='File size mismatch')

    db_record_metadata = db_manager.find_dataset(metadata_id)
    dataset_folder = os.path.join(settings.DATA_TMP_BASE_DIR, db_record_metadata.app_name, metadata_id)
    source_file_path = os.path.join(settings.DATA_TMP_BASE_TUS_FILES_DIR, file_uuid)
    dest_file_path = os.path.join(dataset_folder, file_name)
    # Process the files
    logger(f'Processing using symlink {source_file_path} to {dest_file_path}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    target = source_file_path
    link_name = dest_file_path
    try:
        # with open(source_file_path, 'rb') as file:
        #     md5_hash = hashlib.md5(file.read()).hexdigest()
        # with open(source_file_path, "rb") as f:
        #     file_hash = hashlib.md5()
        #     while chunk := f.read(8192):
        #         file_hash.update(chunk)
        # md5_hash = file_hash.hexdigest()
        md5_hash = ""
        file_type = file_metadata['metadata'].get('filetype', mimetypes.guess_type(dest_file_path)[0])
        db_manager.update_file(DataFile(ds_id=metadata_id, name=file_name, checksum_value=md5_hash,
                                        size=os.path.getsize(source_file_path), mime_type=file_type,
                                        path=dest_file_path, date_added=datetime.utcnow(),
                                        state=DataFileWorkState.UPLOADED))
        new_name = f'{target}-{metadata_id}.{db_record_metadata.app_name}'
        os.rename(target, new_name)
        os.symlink(new_name, link_name)
        logger(f'Symlink created: {link_name} -> {target}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        logger(f'Deleting {source_file_path}.info', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        await delete_file(file_uuid)
    except FileExistsError:
        logger(f'The symlink {link_name} already exists.', "error", LOG_NAME_PS)
    except FileNotFoundError:
        logger(f'The target {target} does not exist.', "error", LOG_NAME_PS)
    except OSError as e:
        logger(f'Error creating symlink: {e}', "error", LOG_NAME_PS)
    all_files_uploaded = len(db_manager.find_registered_files(metadata_id)) == 0
    if all_files_uploaded:
        logger(f'ALL FILE UPLOADED for {metadata_id}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        k = len(db_manager.find_uploaded_files(metadata_id))
        logger(f'+++++++ find_uploaded_files {k}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        z = count_files_in_directory(dataset_folder)
        logger(f'======= count_files_in_directory {z}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        db_manager.set_dataset_ready_for_ingest(metadata_id)
    else:
        db_manager.set_dataset_ready_for_ingest(metadata_id, DatasetWorkState.NOT_READY)
        logger(f'Not all files uploaded for {metadata_id}', LOG_LEVEL_DEBUG, LOG_NAME_PS)

    start_process = db_manager.is_dataset_ready(metadata_id)
    if start_process:
        logger(f'Start Bridge task for {metadata_id} from the PATCH file endpoint', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        bridge_task(metadata_id, f'/inbox/files/{metadata_id}/{file_uuid}')
        logger(f'Bridge task for {metadata_id} started successfully', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    else:
        logger(f'Bridge task for {metadata_id} NOT started', LOG_LEVEL_DEBUG, LOG_NAME_PS)

    registerd_files = db_manager.find_registered_files(metadata_id)
    logger(f'!!!!!!==== Number of registered files: {len(registerd_files)}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    rdm = ResponseDataModel(status="OK")
    rdm.dataset_id = metadata_id
    rdm.start_process = start_process
    return rdm.model_dump(by_alias=True)


def bridge_task(datasetId: str, msg: str) -> None:
    logger(f"Starting threading for {msg} with datasetId: {datasetId}", LOG_LEVEL_DEBUG, LOG_NAME_PS)
    try:
        threading.Thread(target=follow_bridge, args=(datasetId,)).start()
        logger(f"Threading for {datasetId} started successfully.", LOG_LEVEL_DEBUG, LOG_NAME_PS)
    except Exception as e:
        logger(f"Error starting thread for {datasetId}: {e}", 'error', LOG_NAME_PS)


def follow_bridge(datasetId) -> type(None):
    # Log the start time of the thread
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger(f"Thread for datasetId: {datasetId} started at {start_time}", LOG_LEVEL_DEBUG, LOG_NAME_PS)

    logger("Follow bridge", LOG_LEVEL_DEBUG, LOG_NAME_PS)
    logger(f"-------------- EXECUTE follow_bridge for datasetId: {datasetId}", LOG_LEVEL_DEBUG, LOG_NAME_PS)
    db_manager.submitted_now(datasetId)
    target_repo_recs = db_manager.find_target_repos_by_dataset_id(datasetId)
    execute_bridges(datasetId, target_repo_recs)


def execute_bridges(datasetId, targets) -> None:
    logger("execute_bridges", LOG_LEVEL_DEBUG, LOG_NAME_PS)
    results = []
    for target_repo_rec in targets:
        bridge_class = data[Target(**json.loads(target_repo_rec.config)).bridge_module_class]
        logger(f'EXECUTING {bridge_class} for target_repo_id: {target_repo_rec.id}', LOG_LEVEL_DEBUG, LOG_NAME_PS)

        start = time.perf_counter()
        bridge_instance = get_class(bridge_class)(dataset_id=datasetId,
                                                  target=Target(**json.loads(target_repo_rec.config)))
        deposit_result = bridge_instance.deposit()
        deposit_result.response.duration = round(time.perf_counter() - start, 2)

        logger(f'Result from Deposit: {deposit_result.model_dump_json()}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        bridge_instance.save_state(deposit_result)

        if deposit_result.deposit_status in [DepositStatus.FINISH, DepositStatus.ACCEPTED, DepositStatus.SUCCESS]:
            results.append(deposit_result)
        else:
            send_mail(f'Executing {bridge_class} is FAILED.', f'Resp:\n {deposit_result.model_dump_json()}')
            break

    if len(results) == len(targets):
        dataset_folder = os.path.join(settings.DATA_TMP_BASE_DIR, db_manager.find_dataset(ds_id=datasetId).app_name,
                                      datasetId)
        logger(f'Ingest SUCCESSFULL, DELETE {dataset_folder}', LOG_LEVEL_DEBUG, LOG_NAME_PS)

        for file in Path(dataset_folder).glob('*'):
            if file.is_file():
                delete_symlink_and_target(file)
        if os.path.exists(dataset_folder):
            shutil.rmtree(dataset_folder)
        logger(f'DELETED successfully: {dataset_folder}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    else:
        logger(f'Ingest FAILED for datasetId: {datasetId}', LOG_LEVEL_DEBUG, LOG_NAME_PS)


@handle_ps_exceptions
def retrieve_targets_configuration(assistant_config_name: str) -> str:
    repo_url = f'{settings.ASSISTANT_CONFIG_URL}/{assistant_config_name}'
    logger(f'Retrieve targets configuration from {repo_url}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    rsp = requests.get(repo_url, headers=assistant_repo_headers)
    if rsp.status_code != 200:
        raise HTTPException(status_code=404, detail=f"{repo_url} not found")
    return rsp.json()


@router.post("/inbox/resubmit/{datasetId}")
async def resubmit(datasetId: str):
    logger(f'Resubmit {datasetId}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    targets = db_manager.find_unfinished_target_repo(datasetId)
    if not targets:
        return 'No targets'

    logger(f'Resubmitting {len(targets)}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    try:
        execute_bridges_task = threading.Thread(target=execute_bridges, args=(datasetId, targets,))
        execute_bridges_task.start()
        print(f'follow_bridge_task: {execute_bridges_task}')
    except Exception as e:
        logger(f"ERROR: Follow bridge: {targets}. For datasetId: {datasetId}. Exception: "
               f"{e.with_traceback(e.__traceback__)}", 'error', 'ps')


#
@router.delete("/inbox/{datasetId}", include_in_schema=False)
def delete_inbox(datasetId: str):
    num_rows_deleted = db_manager.delete_metadata_record(datasetId)
    return {"Deleted": "OK", "num-row-deleted": num_rows_deleted}


def remove_files_and_directories(dir_path):
    for item in os.listdir(dir_path):
        item_path = os.path.join(dir_path, item)
        if os.path.isfile(item_path):
            os.remove(item_path)
            logger(f"File {item_path} has been removed", LOG_LEVEL_DEBUG, LOG_NAME_PS)
        elif os.path.isdir(item_path):
            shutil.rmtree(item_path)
            logger(f"Directory {item_path} and all its contents have been removed", LOG_LEVEL_DEBUG, LOG_NAME_PS)


@router.delete("/delete-dir/{dir}", include_in_schema=False)
def delete_inbox(dir: str):
    directory = f"{settings.DATA_TMP_BASE_DIR}/{dir}"
    if not os.path.exists(directory):
        return HTTPException(status_code=404, detail=f"{directory} not found")
    logger(f'Delete directory: {directory}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    remove_files_and_directories(directory)
    return {"Deleted": "OK", "directory": directory}


# Endpoint to retrieve application settings
@router.get("/settings-reload", include_in_schema=False)
async def get_settings():
    logger(f"Getting settings Before Load: {settings.as_dict()}", "debug", "ps")
    logger("Reload settings", "debug", "ps")
    settings.reload()
    logger(f"Getting settings After Load: {settings.as_dict()}", "debug", "ps")
    return settings.as_dict()


@router.get('/logs/{app_name}', include_in_schema=False)
def get_log(app_name: str):
    logger('logs', LOG_LEVEL_DEBUG, 'ps')
    return FileResponse(path=f"{os.environ['BASE_DIR']}/logs/{app_name}.log", filename=f"{app_name}.log",
                        media_type='text/plain')


@router.get("/logs-list", include_in_schema=False)
def get_log_list():
    logger('logs-list', LOG_LEVEL_DEBUG, 'ps')
    return os.listdir(path=f"{os.environ['BASE_DIR']}/logs")


@router.get("/db-download", include_in_schema=False)
def get_db():
    logger('db-download', LOG_LEVEL_DEBUG, 'ps')
    return FileResponse(path=settings.DB_URL, filename="dans_packaging.db",
                        media_type='application/octet-stream')


@router.delete("/db-delete-all", include_in_schema=False)
def delete_all_recs():
    logger('Deleting all', LOG_LEVEL_DEBUG, 'ps')
    return db_manager.delete_all()


@router.get("/datasets", include_in_schema=False)
async def get_db():
    logger("Finding datasets", "debug", "ps")
    return JSONResponse(content=db_manager.execute_raw_sql())
