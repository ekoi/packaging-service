import json
import math
import mimetypes
import os
import re
import time
import uuid
import zipfile
from datetime import datetime

import subprocess

import jmespath
import requests
from simple_file_checksum import get_checksum
from starlette import status

from src.bridge import Bridge, BridgeOutputDataModel
from src.commons import (
    settings,
    db_manager,
    transform,
    logger,
    handle_deposit_exceptions, dmz_dataverse_headers, LOG_LEVEL_DEBUG, upload_large_file, zip_with_progress,
    compress_zip_file, zip_a_zipfile_with_progress, escape_invalid_json_characters,
)
from src.dbz import ReleaseVersion, DataFile, DepositStatus, FilePermissions, DataFileWorkState
from src.models.bridge_output_model import IdentifierItem, IdentifierProtocol, TargetResponse, ResponseContentType


class DataverseIngester(Bridge):

    @handle_deposit_exceptions
    def deposit(self) -> BridgeOutputDataModel:
        md_json = json.loads(self.metadata_rec.md)
        if self.target.input:
            input_from_prev_target = db_manager.find_target_repo(self.dataset_id, self.target.input.from_target_name)
            md_json.update({"input_from_prev_target":
                                json.loads(input_from_prev_target.target_output)['response']['identifiers'][0][
                                    'value']})

        # logger(f"md_json - after update (input_from_prev_target): {json.dumps(md_json)}", LOG_LEVEL_DEBUG,
        #        self.app_name)

        files_metadata = jmespath.search('"file-metadata"[*]', md_json)
        generated_files = self.__create_generated_files()
        for gf in generated_files:
            files_metadata.append({"name": gf.name, "mimetype": gf.mime_type,
                                   "private": True if gf.permissions == FilePermissions.PRIVATE else False})
        if generated_files:
            db_manager.insert_datafiles(generated_files)
        # Update the file-metadata: added some attributes
        md_json.update({"file-metadata": files_metadata})
        # updating mimetype of user's uploaded files since no mimetype in the form-metadata submission
        for _ in db_manager.find_non_generated_files(dataset_id=self.dataset_id):
            # Escape single and double quotes in the file name
            escaped_file_name = _.name.replace('"', '\\"')
            f_json = jmespath.search(f'[?name == `{escaped_file_name}`]', files_metadata)
            # logger(f'{self.__class__.__name__} f_json: {f_json}', LOG_LEVEL_DEBUG, self.app_name)
            f_json[0].update({"mimetype": _.mime_type})

        str_updated_metadata_json = json.dumps(md_json)
        logger(f"*******str_updated_metadata_json: {str_updated_metadata_json}", LOG_LEVEL_DEBUG, self.app_name)
        str_dv_metadata = transform(
            transformer_url=self.target.metadata.transformed_metadata[0].transformer_url,
            str_tobe_transformed=str_updated_metadata_json
        )
        # Validate json
        try:
            json.loads(str_dv_metadata)
        except json.JSONDecodeError as e:
            logger(f"Error: {e}", "error", self.app_name)
            str_dv_metadata = re.sub(r'[\x00-\x1F\x7F]', '', str_dv_metadata)
            try:
                json.loads(str_dv_metadata)
            except json.JSONDecodeError as e:
                logger(f"Error: {e}", "error", self.app_name)
                return BridgeOutputDataModel(notes="Error", deposit_status=DepositStatus.ERROR)

        logger(f'deposit to "{self.target.target_url}"', "debug", self.app_name)
        dv_response = requests.post(
            f"{self.target.target_url}", headers=dmz_dataverse_headers('API_KEY', self.target.password),
            data=str_dv_metadata
        )
        logger(
            f"dv_response.status_code: {dv_response.status_code} dv_response.text: {dv_response.text}",
            "debug",
            self.app_name,
        )

        ingest_status = DepositStatus.ERROR
        message = "Error"
        dataset_id = None
        identifier_items = []
        logger(f'Ingesting metadata {self.dataset_id} to {self.target.target_url}', "debug", self.app_name)
        if dv_response.status_code == 201:
            dv_response_json = dv_response.json()
            dataset_id = dv_response_json["data"]["id"]
            logger(f"Data ingest successfully! {json.dumps(dv_response_json)}", "debug", self.app_name)
            pid = dv_response_json["data"]["persistentId"]
            identifier_items.append(
                IdentifierItem(value=pid, url=f'{self.target.base_url}/dataset.xhtml?persistentId={pid}',
                               protocol=IdentifierProtocol('doi')))
            logger(f"pid: {pid}", "debug", self.app_name)

            ingest_file = self.__ingest_files(pid, str_updated_metadata_json)
            if ingest_file.get("status") == status.HTTP_200_OK:
                ingest_status, message = DepositStatus.FINISH, "The dataset and its file is successfully ingested"
                logger(f'Ingest FILE(s) successfully! {json.dumps(ingest_file)}', LOG_LEVEL_DEBUG, self.app_name)
                if self.target.initial_release_version == ReleaseVersion.PUBLISHED:
                    logger(f'Publish the dataset', "debug", self.app_name)
                    publish_status = self.__publish_dataset(pid)
                    message = "The dataset is successfully published" if publish_status == status.HTTP_200_OK else "The dataset is unsuccessfully published"
            else:
                ingest_status, message = DepositStatus.ERROR, ingest_file.get("message")
        else:
            logger(f"Ingest failed with status code {dv_response.status_code}:", "debug", self.app_name)
            logger(f'Response:  {dv_response.text}', "debug", self.app_name)
            logger(f"Ingest metadata - str_dv_metadata {str_dv_metadata}", "debug", self.app_name)
            logger(f"Ingest metadata - str_updated_metadata_json {str_updated_metadata_json}", "debug", self.app_name)
            # TODO: DELETE DRAFT Dataverse dataset
            ingest_status, message = DepositStatus.ERROR, "Error"

        if ingest_status == DepositStatus.ERROR:
            # Delete Dataverse dataset
            delete_response = requests.delete(f"{self.target.base_url}/api/datasets/{dataset_id}/versions/:draft",
                            headers=dmz_dataverse_headers('API_KEY', self.target.password))
            logger(f"delete_response.status_code: {delete_response.status_code} delete_response.text: {delete_response.text}", "debug", self.app_name)

        bridge_output_model = BridgeOutputDataModel(notes=message, deposit_status=ingest_status)
        current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
        bridge_output_model.deposit_time = current_time
        target_repo = TargetResponse(url=self.target.target_url, status=DepositStatus.FINISH, message=message,
                                     identifiers=identifier_items, content=dv_response.text)
        target_repo.content_type = ResponseContentType.JSON
        target_repo.status_code = dv_response.status_code
        bridge_output_model = BridgeOutputDataModel(notes=message, response=target_repo)
        bridge_output_model.deposit_time = current_time
        bridge_output_model.deposit_status = ingest_status
        return bridge_output_model

    def __create_generated_files(self) -> [DataFile]:
        generated_files = []
        for gnr_file in self.target.metadata.transformed_metadata:
            if not gnr_file.target_dir:  # Skip if target-dir is "metadata"
                gf_path = os.path.join(self.dataset_dir, gnr_file.name)
                content = transform(gnr_file.transformer_url,
                                    self.metadata_rec.md) if gnr_file.transformer_url else self.metadata_rec.md
                with open(gf_path, "wt") as f:
                    f.write(content)
                gf_mimetype = mimetypes.guess_type(gf_path)[0]
                permissions = FilePermissions.PRIVATE if gnr_file.restricted else FilePermissions.PUBLIC
                generated_files.append(DataFile(
                    ds_id=self.dataset_id, name=gnr_file.name, path=gf_path,
                    size=os.path.getsize(gf_path), mime_type=gf_mimetype,
                    checksum_value=get_checksum(gf_path, algorithm="MD5"),
                    date_added=datetime.utcnow(), permissions=permissions,
                    state=DataFileWorkState.GENERATED))
        return generated_files

    def __ingest_files(self, pid: str, str_updated_metadata_json: str) -> dict:
        logger(f'Ingesting files to {pid}', "debug", self.app_name)
        str_dv_file = transform(
            transformer_url=self.target.metadata.transformed_metadata[1].transformer_url,
            str_tobe_transformed=str_updated_metadata_json
        )

        for file in db_manager.find_non_generated_files(dataset_id=self.dataset_id):
            logger(f'Ingesting file {file.name}. Size: {file.size} Path: {file.path} ', "debug", self.app_name)
            jsonData = json.loads(str_dv_file).get(file.name)
            if jsonData:
                start = time.perf_counter()
                data = {"jsonData": json.dumps(jsonData)}
                if file.mime_type == "application/zip":
                    real_file_path = os.readlink(file.path)
                    zip_file_name = f'{os.path.dirname(real_file_path)}/{file.name}'
                    # remove the symlink
                    os.remove(file.path)
                    os.rename(real_file_path, zip_file_name)
                    logger(f'Start zipping file {file.name}. Real path: {zip_file_name}', LOG_LEVEL_DEBUG
                           , self.app_name)
                    zip_a_zipfile_with_progress(zip_file_name, file.path)
                    os.remove(zip_file_name)
                    logger(f'Finished zipping file {file.name} to {real_file_path} in '
                           f'{round(time.perf_counter() - start, 2)} seconds', LOG_LEVEL_DEBUG, self.app_name)

                url_base = f"{self.target.base_url}/api/datasets/:persistentId/add?persistentId={pid}"
                headers = dmz_dataverse_headers('API_KEY', self.target.password)
                timeout_seconds = settings.get("DATAVERSE_RESPONSE_TIMEOUT", 360000)
                # file_path = file.path + '.zip' if file.mime_type == "application/zip" else file.path

                response_ingest_file = None
                logger(f'>>>> Start ingesting file {file.name}. Size: {file.size}. Ingest to {url_base}', "debug", self.app_name)
                if file.size < settings.get("MAX_INGEST_SIZE_USING_PYTHON", 100000000):
                    logger(f'++++ Ingest SMALL FILE using python: {file.name}', "debug", self.app_name)
                    with open(file.path, 'rb') as f:
                        files = {'file': (file.name, f)}
                        response_ingest_file = requests.post(url_base, files=files, data=data, headers=headers, timeout= timeout_seconds)
                        response_ingest_file = response_ingest_file.json()
                        logger(f'>>>>>>>File {file.name} is successfully ingested', "debug", self.app_name)
                else:
                    logger(f'####### Ingest LARGE FILE using script: {file.name}', "debug", self.app_name)
                    # Convert jsonData and headers to strings
                    jsonData_str = json.dumps(jsonData)
                    try:
                        output = f'{settings.DATA_TMP_BASE_DIR}/{self.app_name}/{self.dataset_id}/{str(uuid.uuid4().int)}.txt'
                        logger(f'>>>>>>>Output: {output}', "debug", self.app_name)
                        # Execute the shell script with the parameters
                        result = subprocess.run(
                            [settings.SHELL_SCRIPT_PATH, file.path, url_base, jsonData_str, self.target.password, output],
                            check=True,  # Raises a CalledProcessError if the command exits with a non-zero status
                            text=True,  # Interprets stdout and stderr as text strings
                            capture_output=True  # Captures the output (stdout and stderr)
                        )
                        logger(f'>>>>>>>File {file.name} is successfully ingested', "debug", self.app_name)
                        logger(f'>>>>>>>Response: {result.stdout}', "debug", self.app_name)
                        response_ingest_file = json.loads(result.stdout)
                    except subprocess.CalledProcessError as e:
                        logger(f'>>>>>>>File {file.name} is FAIL ingested', "error", self.app_name)
                        logger(f'>>>>>>>Response: {e.stderr}', "error", self.app_name)
                        return {"status": "error", "message": e.stderr}
                    except Exception as e:
                        logger(f'>>>>>>>File {file.name} is FAIL ingested', "error", self.app_name)
                        logger(f'>>>>>>>Response: {e}', "error", self.app_name)
                        return {"status": "error", "message": e}

                logger(f'Finish ingesting file {file.name} to {pid} in {round(time.perf_counter() - start, 2)}'
                       f' seconds.',"debug", self.app_name)

                if jsonData.get('embargo'):
                    json_data = {
                        'dateAvailable': jsonData.get('embargo'),
                        'reason': '',
                        'fileIds': [response_ingest_file['data']['files'][0]['dataFile']['id']],
                    }
                    response_embargo = requests.post(
                        f'{self.target.base_url}/api/datasets/:persistentId/files/actions/:set-embargo?persistentId={pid}',
                        headers=dmz_dataverse_headers('API_KEY', self.target.password), json=json_data)
                    if response_embargo.status_code != status.HTTP_200_OK:
                        return {"status": "error", "message": response_embargo.text}

        return {"status": status.HTTP_200_OK}

    def __publish_dataset(self, pid) -> int:
        return requests.post(
            f"{self.target.base_url}/api/datasets/:persistentId/actions/:publish?persistentId={pid}&type=major",
            headers={"Content-Type": "application/json", "X-Dataverse-key": self.target.password},
        ).status_code
