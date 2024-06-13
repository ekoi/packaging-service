# Import necessary modules and packages
# Import necessary libraries and modules
import time
from datetime import datetime
import hashlib
import json
import mimetypes
import os
import threading
from typing import Callable, Awaitable, Any

import jmespath
import requests
from fastapi import APIRouter, Request, UploadFile, Form, File, HTTPException
from starlette.responses import FileResponse

from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

# Import custom modules and classes
from src.models.assistant_datamodel import RepoAssistantDataModel, Target
from src.commons import settings, logger, data, db_manager, get_class, assistant_repo_headers, handle_ps_exceptions, \
    send_mail
from src.dbz import TargetRepo, DataFile, Dataset, ReleaseVersion, DepositStatus, FilePermissions, \
    DatasetWorkState, DataFileWorkState
from src.models.app_model import ResponseDataModel, InboxDatasetDataModel
from src.models.target_datamodel import TargetsCredentialsModel

# Create an API router instance
router = APIRouter()


# Endpoint to register a bridge module
@router.post("/register-bridge-module/{name}/{overwrite}")
async def register_module(name: str, bridge_file: Request, overwrite: bool | None = False) -> {}:
    logger(f'Registering {name}', 'debug', 'ps')
    if not overwrite and name in data["bridge-modules"]:
        raise HTTPException(status_code=400,
                            detail=f'The {name} is already exist. Consider /register-bridge-module/{name}/true')

    if bridge_file.headers['Content-Type'] != 'text/x-python':
        raise HTTPException(status_code=400, detail=f"{bridge_file.headers['Content-Type']} supported content type")

    m_file = await bridge_file.body()
    bridge_path = os.path.join(settings.MODULES_DIR, name)
    with open(bridge_path, mode="w+") as file:
        file.write(str(m_file))
    # check real mimetype
    if mimetypes.guess_type(bridge_path)[0] == 'text/x-python':
        pass
    else:
        os.remove(bridge_path)
        raise HTTPException(status_code=400, detail=f'{name} is not supported file type')

    return {"status": "ok", "bridge-module-name": name}


# Helper function to process inbox dataset metadata
@handle_ps_exceptions
async def get_inbox_dataset_dc(request: Request, release_version: ReleaseVersion) -> (
        Callable)[[Request, ReleaseVersion], Awaitable[InboxDatasetDataModel]]:
    req_body = await request.json()
    logger(f'METADATA: \n{req_body}', 'debug', 'ps')
    title = jmespath.search('title', req_body)
    return InboxDatasetDataModel(assistant_name=request.headers.get('assistant-config-name'),
                                 release_version=release_version, owner_id=request.headers.get('user-id'),
                                 title=title,
                                 target_creds=request.headers.get('targets-credentials'), metadata=req_body)


# Endpoint to process inbox dataset metadata
@router.post("/inbox/dataset/{release_version}")
async def process_inbox_dataset_metadata(request: Request, release_version: ReleaseVersion) -> {}:
    idh = await get_inbox_dataset_dc(request, release_version)
    datasetId = jmespath.search("id", idh.metadata)
    logger(f'Start inbox for metadata id: {datasetId}- release version: {release_version}   - assistant name: {idh.assistant_name}', 'debug', 'ps')
    if db_manager.is_dataset_published(datasetId):
        raise HTTPException(status_code=400, detail='Dataset is already published.')
    repo_config = retrieve_targets_configuration(idh.assistant_name)
    try:
        repo_assistant = RepoAssistantDataModel.model_validate_json(
            repo_config)  # TODO: Check the given transformer exist.
        # Create temp folder
        tmp_dir = os.path.join(settings.DATA_TMP_BASE_DIR, repo_assistant.app_name, datasetId)
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)

        db_recs_target_repo = process_target_repos(repo_assistant, idh.target_creds)

        db_record_metadata, registered_files = process_metadata_record(datasetId, idh, repo_assistant, tmp_dir)
        process_db_records(datasetId, db_record_metadata, db_recs_target_repo, registered_files)

    except HTTPException as ex:
        return ex
    except Exception as ex:
        logger(f'Errors during dataset ingest: {ex.with_traceback(ex.__traceback__)}', 'debug', 'ps')
        raise HTTPException(status_code=400, detail='Error occurred. Please contact application owner.')
    # Check whether all files uploaded
    start_process = db_manager.is_dataset_ready(datasetId)
    if start_process and db_manager.are_files_uploaded(datasetId):
        bridge_task(datasetId, f"/inbox/dataset/{idh.release_version}")
        # logger(f"------------- START THREADING from inbox/dataset  {datasetId}-------------------", 'debug', 'ps')
        #
        # t1 = threading.Thread(target=follow_bridge, args=(datasetId,))
        # t1.start()
    rdm = ResponseDataModel(status="OK")
    rdm.dataset_id = datasetId
    rdm.start_process = start_process
    return rdm.model_dump(by_alias=True)


@router.delete("/inbox/dataset/{metadata_id}")
def delete_dataset_metadata(request: Request, metadata_id: str):
    user_id = request.headers.get('user-id')
    if user_id is None:
        raise HTTPException(status_code=401, detail='No user id provided')

    if metadata_id not in db_manager.find_dataset_ids_by_owner(user_id):
        raise HTTPException(status_code=404, detail='No Dataset found')

    target_repos = db_manager.find_target_repos_by_dataset_id(metadata_id)
    if target_repos is None:
        raise HTTPException(status_code=404, detail='No target found')

    #TThe delete mechanism will be executed when an error is occurred in the first target.
    logger(f'Deposit Status: {target_repos[0].deposit_status}', 'debug', 'ps')
    if target_repos[0].deposit_status not in (DepositStatus.ACCEPTED, DepositStatus.DEPOSITED, DepositStatus.FINISH):
        db_manager.delete_by_dataset_id(dataset_id=metadata_id)
        return {"status": "ok", "metadata-id": metadata_id}

    raise HTTPException(status_code=404, detail=f'Delete of {metadata_id} is not allowed.')


@handle_ps_exceptions
def process_db_records(datasetId, db_record_metadata, db_recs_target_repo, registered_files) -> type(None):
    if not db_manager.is_dataset_exist(datasetId):
        db_manager.insert_dataset_and_target_repo(db_record_metadata, db_recs_target_repo)
    else:
        db_manager.update_metadata(db_record_metadata)
        db_manager.replace_targets_record(datasetId, db_recs_target_repo)
    if registered_files:
        db_manager.insert_datafiles(registered_files)


@handle_ps_exceptions
def process_metadata_record(datasetId, idh, repo_assistant, tmp_dir):
    registered_files = []
    file_names = jmespath.search('"file-metadata"[*].name', idh.metadata)
    already_uploaded_files = [f.name for f in db_manager.find_files(datasetId)]
    list_files_from_metadata = file_names
    files_name_to_be_deleted = list(set(already_uploaded_files).difference(list_files_from_metadata))
    logger(f'files_name_to_be_deleted: {files_name_to_be_deleted}', 'debug', 'ps')
    files_name_to_be_added = list(set(list_files_from_metadata).difference(already_uploaded_files))
    logger(f'files_name_to_be_added: {files_name_to_be_added}', 'debug', 'ps')

    for f_name_to_be_deleted in files_name_to_be_deleted:
        f = os.path.join(str(tmp_dir), f_name_to_be_deleted)
        # if os.path.exists(f):
        #     os.remove(f)
        os.remove(f)
        logger(f'{f} ---- {f} is deleted', 'debug', 'ps')
        db_manager.delete_datafile(datasetId, f_name_to_be_deleted)

    for f_name_tobe_added in files_name_to_be_added:
        file_path = os.path.join(str(tmp_dir), f_name_tobe_added)
        f_permission = jmespath.search(f'"file-metadata"[?name == \'{f_name_tobe_added}\'].private',
                                       idh.metadata)

        fp = FilePermissions.PRIVATE if f_permission[0] else FilePermissions.PUBLIC
        registered_files.append(DataFile(name=f_name_tobe_added, path=file_path, ds_id=datasetId, permissions=fp))
    ready_for_ingest = DatasetWorkState.READY if len(files_name_to_be_added) == 0 else DatasetWorkState.NOT_READY
    db_record_metadata = Dataset(id=datasetId, title=idh.title, owner_id=idh.owner_id,
                                 app_name=repo_assistant.app_name, release_version=idh.release_version,
                                 state=ready_for_ingest, md=json.dumps(idh.metadata))
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
        logger(f'target_repo_name: {target_repo_name}', 'debug', 'ps')
        for depositor_cred in input_target_cred_model.targets_credentials:
            if depositor_cred.target_repo_name == repo_target.repo_name and depositor_cred.credentials.username:
                repo_target.username = depositor_cred.credentials.username
            if depositor_cred.target_repo_name == repo_target.repo_name and depositor_cred.credentials.password:
                repo_target.password = depositor_cred.credentials.password

        db_recs_target_repo.append(TargetRepo(name=repo_target.repo_name, url=repo_target.target_url,
                                              display_name=repo_target.repo_display_name,
                                              config=repo_target.model_dump_json(by_alias=True, exclude_none=True)))
    return db_recs_target_repo


@router.post("/inbox/file")
async def process_inbox_dataset_file(datasetId: str = Form(), fileName: str = Form(),
                                     file: UploadFile = File(...)) -> {}:
    files_name = []
    submitted_filename = fileName
    logger(f"submitted_filename: {submitted_filename} from metadataid: {datasetId}", 'debug', 'ps')
    db_record_metadata = db_manager.find_dataset(datasetId)
    tmp_dir = os.path.join(settings.DATA_TMP_BASE_DIR, db_record_metadata.app_name, datasetId)

    db_records_uploaded_file = db_manager.find_files(datasetId)
    # Process the files
    f_upload_st = []
    for db_rec_uploaded_f in db_records_uploaded_file:
        # Save the uploaded file
        if submitted_filename == db_rec_uploaded_f.name:
            file_contents = await file.read()
            md5_hash = hashlib.md5(file_contents).hexdigest()
            file_path = os.path.join(str(tmp_dir), submitted_filename)
            with open(file_path, "wb") as f:
                f.write(file_contents)

            # NOTES: There are many way to get the mime type of file, This rather the simplest then the best.
            file_mimetype = mimetypes.guess_type(file_path)[0]
            db_manager.update_file(df=DataFile(ds_id=datasetId, name=submitted_filename, checksum_value=md5_hash,
                                               size=os.path.getsize(file_path), mime_type=file_mimetype, path=file_path,
                                               date_added=datetime.utcnow(), state=DataFileWorkState.UPLOADED))
            f_upload_st.append({"name": db_rec_uploaded_f.name, "uploaded": True})
        else:
            f_upload_st.append(
                {"name": db_rec_uploaded_f.name, "uploaded": False}) if db_rec_uploaded_f.date_added is None \
                else f_upload_st.append({"name": db_rec_uploaded_f.name, "uploaded": True})

    all_files_uploaded = True
    for fus in f_upload_st:
        if fus['uploaded'] is False:
            all_files_uploaded = False
            break
    # Update record
    if all_files_uploaded:
        db_manager.set_ready_for_ingest(datasetId)

    start_process = db_manager.is_dataset_ready(datasetId)

    if start_process:
        # func_name = inspect.currentframe().f_code.co_name # Slow
        # func_name = sys._getframe().f_code.co_name  # Faster 3x, but it uses 'private' method _getframe()
        bridge_task(datasetId, f'/inbox/file/{fileName}')

    rdm = ResponseDataModel(status="OK")
    rdm.dataset_id = datasetId
    rdm.start_process = start_process
    return rdm.model_dump(by_alias=True)


def bridge_task(datasetId: str, msg: str) -> type(None):
    print("bridge_task")
    logger(f">> START THREADING from {msg} for datasetId: {datasetId}-------------------", 'debug', 'ps')
    # Start the threads
    print(f"Thread executed: {msg}")
    try:
        follow_bridge_task = threading.Thread(target=follow_bridge, args=(datasetId,))
        follow_bridge_task.start()
        print(f'follow_bridge_task: {follow_bridge_task}')
    except Exception as e:
        logger(f"ERROR: Follow bridge: {msg}. For datasetId: {datasetId}. Exception: "
               f"{e.with_traceback(e.__traceback__)}", 'error', 'ps')
    print(f"Thread execution completed: {msg}")
    logger(f">> Thread execution for {datasetId} is completed. {msg}", 'debug', 'ps')


# def follow_bridge(datasetId) -> type(None):
#     logger("Follow bridge", 'debug', 'ps')
#     logger(f"-------------- EXECUTE follow_bridge for datasetId: {datasetId}", 'debug', 'ps')
#     db_manager.submitted_now(datasetId)
#     target_repo_recs = db_manager.find_target_repos_by_ds_id(datasetId)
#     result = []
#     rsp = {"dataset-id": datasetId, "targets-result": result}
#
#     for target_repo_rec in target_repo_recs:
#         target_repo_id = target_repo_rec.id
#         target_repo_json = json.loads(target_repo_rec.config)
#         tg = Target(**target_repo_json)
#         bridge_class = data[tg.bridge_module_class]
#
#         logger(f'EXECUTING {bridge_class} for target_repo_id: {target_repo_id}', 'debug', 'ps')
#         start = time.perf_counter()
#         a = get_class(bridge_class)
#         k = a(dataset_id=datasetId, target=tg)
#         m = k.deposit()  # deposit return type: s BridgeOutputModel
#         finish = time.perf_counter()
#         m.response.duration = round(finish - start, 2)
#         logger(f'Result from Deposit: {m.model_dump_json()}', 'debug', 'ps')
#         k.save_state(m)
#         if m.deposit_status in [DepositStatus.FINISH, DepositStatus.ACCEPTED, DepositStatus.SUCCESS]:
#             logger(f'Finish deposit for {bridge_class} to target_repo_id: {target_repo_id}. Result: {m}', 'debug', 'ps')
#             result.append(m)
#         else:
#             logger(f'Executing {bridge_class} is FAILED. Resp: {m.model_dump_json()}', 'debug', 'ps')
#             send_mail(f'Executing {bridge_class} is FAILED.', f'Resp:\n {m.model_dump_json()}')
#             break
#
#     logger(f"----------- END follow_bridge for datasetId: {datasetId}", 'debug', 'ps')
#     logger(f'>>>>Executed: {len(result)} of {len(target_repo_recs)}', 'debug', 'ps')

def follow_bridge(datasetId) -> type(None):
    logger("Follow bridge", 'debug', 'ps')
    logger(f"-------------- EXECUTE follow_bridge for datasetId: {datasetId}", 'debug', 'ps')
    db_manager.submitted_now(datasetId)
    target_repo_recs = db_manager.find_target_repos_by_dataset_id(datasetId)
    execute_bridges(datasetId, target_repo_recs)


def execute_bridges(datasetId, targets) -> type(None):
    logger("execute_bridges", 'debug', 'ps')
    logger(f"-------------- EXECUTE execute_bridges for datasetId: {datasetId}", 'debug', 'ps')
    result = []
    rsp = {"dataset-id": datasetId, "targets-result": result}
    for target_repo_rec in targets:
        target_repo_id = target_repo_rec.id
        target_repo_json = json.loads(target_repo_rec.config)
        tg = Target(**target_repo_json)
        bridge_class = data[tg.bridge_module_class]

        logger(f'EXECUTING {bridge_class} for target_repo_id: {target_repo_id}', 'debug', 'ps')
        start = time.perf_counter()
        a = get_class(bridge_class)
        k = a(dataset_id=datasetId, target=tg)
        m = k.deposit()  # deposit return type: s BridgeOutputModel
        finish = time.perf_counter()
        m.response.duration = round(finish - start, 2)
        logger(f'Result from Deposit: {m.model_dump_json()}', 'debug', 'ps')
        k.save_state(m)
        if m.deposit_status in [DepositStatus.FINISH, DepositStatus.ACCEPTED, DepositStatus.SUCCESS]:
            logger(f'Finish deposit for {bridge_class} to target_repo_id: {target_repo_id}. Result: {m}', 'debug', 'ps')
            result.append(m)
        else:
            logger(f'Executing {bridge_class} is FAILED. Resp: {m.model_dump_json()}', 'debug', 'ps')
            send_mail(f'Executing {bridge_class} is FAILED.', f'Resp:\n {m.model_dump_json()}')
            break

    logger(f"----------- END follow_bridge for datasetId: {datasetId}", 'debug', 'ps')
    logger(f'>>>>Executed: {len(result)} of {len(targets)}', 'debug', 'ps')


@handle_ps_exceptions
def retrieve_targets_configuration(assistant_config_name: str) -> str:
    repo_url = f'{settings.ASSISTANT_CONFIG_URL}/{assistant_config_name}'
    logger('>>>>>', 'debug', 'ps')
    logger(f'repo_url: {repo_url}', 'debug', 'ps')
    logger('<<<<<', 'debug', 'ps')
    rsp = requests.get(repo_url, headers=assistant_repo_headers)
    if rsp.status_code != 200:
        logger(f'repo_url: {repo_url} NOT FOUND!', 'error', 'ps')
        raise HTTPException(status_code=404, detail=f"{repo_url} not found")
    repo_config = rsp.json()
    logger(f'Given repo config: {json.dumps(repo_config)}', 'debug', 'ps')
    return repo_config


@router.post("/inbox/resubmit/{datasetId}")
async def resubmit(datasetId: str):
    logger(f'Resubmit {datasetId}', 'debug', 'ps')
    targets = db_manager.find_unfinished_target_repo(datasetId)
    if not targets:
        return 'No targets'

    logger(f'Resubmitting {len(targets)}', 'debug', 'ps')
    try:
        execute_bridges_task = threading.Thread(target=execute_bridges, args=(datasetId, targets, ))
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


# Endpoint to retrieve application settings
@router.get("/settings", include_in_schema=False)
async def get_settings():
    return settings


@router.get('/logs/{app_name}', include_in_schema=False)
def get_log(app_name: str):
    logger('logs', 'debug', 'ps')
    return FileResponse(path=f"{os.environ['BASE_DIR']}/logs/{app_name}.log", filename=f"{app_name}.log",
                        media_type='text/plain')


@router.get("/logs-list", include_in_schema=False)
def get_log_list():
    logger('logs-list', 'debug', 'ps')
    return os.listdir(path=f"{os.environ['BASE_DIR']}/logs")


@router.get("/db-download", include_in_schema=False)
def get_db():
    logger('db-download', 'debug', 'ps')
    return FileResponse(path=settings.DB_URL, filename="dans_packaging.db",
                        media_type='application/octet-stream')


@router.delete("/db-delete-all", include_in_schema=False)
def delete_all_recs():
    logger('Deleting all', 'debug', 'ps')
    return db_manager.delete_all()


@router.get("/datasets", include_in_schema=False)
async def get_db():
    logger("Finding datasets", "debug", "ps")
    return JSONResponse(content=db_manager.execute_raw_sql())
