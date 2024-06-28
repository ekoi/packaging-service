import json
import mimetypes
import os
from datetime import datetime

import jmespath
import requests
from simple_file_checksum import get_checksum
from starlette import status

from src.bridge import Bridge, BridgeOutputDataModel
from src.commons import (
    db_manager,
    transform,
    logger,
    handle_deposit_exceptions, dmz_dataverse_headers, LOG_LEVEL_DEBUG,
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
                            json.loads(input_from_prev_target.target_output)['response']['identifiers'][0]['value']})

        logger(f"md_json - after update (input_from_prev_target): {json.dumps(md_json)}", LOG_LEVEL_DEBUG,
               self.app_name)

        files_metadata = jmespath.search('"file-metadata"[*]', md_json)
        generated_files = self.__create_generated_files()
        for gf in generated_files:
            files_metadata.append({"name": gf.name, "mimetype": gf.mime_type,
                                   "private": True if gf.permissions == FilePermissions.PRIVATE else False})
        if generated_files: db_manager.insert_datafiles(generated_files)
        # Update the file-metadata: added some attributes
        md_json.update({"file-metadata": files_metadata})
        # updating mimetype of user's uploaded files since no mimetype in the form-metadata submission
        for _ in db_manager.find_non_generated_files(dataset_id=self.dataset_id):
            f_json = jmespath.search(f'[?name == \'{_.name}\']', files_metadata)
            logger(f'{self.__class__.__name__} f_json: {f_json}', LOG_LEVEL_DEBUG, self.app_name)
            f_json[0].update({"mimetype": _.mime_type})

        str_updated_metadata_json = json.dumps(md_json)
        str_dv_metadata = transform(
            transformer_url=self.target.metadata.transformed_metadata[0].transformer_url,
            str_tobe_transformed=str_updated_metadata_json
        )
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

        identifier_items = []
        if dv_response.status_code == 201:
            dv_response_json = dv_response.json()
            logger(f"Data ingest successfully! {json.dumps(dv_response_json)}", "debug", self.app_name)
            pid = dv_response_json["data"]["persistentId"]
            ideni = IdentifierItem(value=pid, url=f'{self.target.base_url}/dataset.xhtml?persistentId={pid}',
                                   protocol=IdentifierProtocol('doi'))
            identifier_items.append(ideni)
            logger(f"pid: {pid}", "debug", self.app_name)

            ingest_file = self.__ingest_files(pid, str_updated_metadata_json)
            if ingest_file.get("status") == status.HTTP_200_OK:
                logger(f'Ingest FILE(s) successfully! {json.dumps(ingest_file)}', LOG_LEVEL_DEBUG, self.app_name)
                if self.target.initial_release_version == ReleaseVersion.PUBLISHED:
                    logger(f'Publish the dataset', "debug", self.app_name)
                    publish_status = self.__publish_dataset(pid)
                    if publish_status == status.HTTP_200_OK:
                        ingest_status = DepositStatus.FINISH
                        message = "The dataset is successfully published"
                    else:
                        ingest_status = DepositStatus.ERROR
                        message = "The dataset is unsuccessfully published"
                else:
                    ingest_status = DepositStatus.FINISH
                    message = "The dataset and its file is successfully ingested"
            else:
                ingest_status = DepositStatus.ERROR
                message = ingest_file.get("message")

        else:
            logger(
                f"Ingest failed with status code {dv_response.status_code}:",
                "debug",
                self.app_name,
            )
        bridge_output_model = BridgeOutputDataModel(
                notes=message, deposit_status=ingest_status
        )
        bridge_output_model.deposit_time = datetime.utcnow().strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )
        target_repo = TargetResponse(url=self.target.target_url, status=DepositStatus.FINISH, message=message,
                                     identifiers=identifier_items, content=dv_response.text)
        target_repo.content_type = ResponseContentType.JSON
        target_repo.status_code = dv_response.status_code
        bridge_output_model = BridgeOutputDataModel(notes=message, response=target_repo)
        bridge_output_model.deposit_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
        bridge_output_model.deposit_status = ingest_status
        return bridge_output_model

    def __create_generated_files(self) -> [DataFile]:
        generated_files = []
        # Create generated file in target-dir. The base directory is the app_name/target-dir
        for gnr_file in self.target.metadata.transformed_metadata:
            gf_dir = gnr_file.target_dir
            if gf_dir:
                continue  # exclude target-dir="metadata"
            else:
                gf_filename = gnr_file.name
                gf_restricted = gnr_file.restricted
                gf_path = os.path.join(self.dataset_dir, gf_filename)
                if gnr_file.transformer_url:
                    # generate file content
                    gf_str = transform(gnr_file.transformer_url, self.metadata_rec.md)
                    # write file
                    with open(gf_path, mode="wt") as f:
                        f.write(gf_str)
                else:
                    # No transformer-url, so the file content is from the original input metadata
                    with open(gf_path, mode="wt") as f:
                        f.write(self.metadata_rec.md)

                gf_mimetype = mimetypes.guess_type(gf_path)[0]
                fp = FilePermissions.PRIVATE if gf_restricted else FilePermissions.PUBLIC
                generated_files.append(DataFile(ds_id=self.dataset_id, name=gf_filename, path=gf_path,
                                                size=os.path.getsize(gf_path), mime_type=gf_mimetype,
                                                checksum_value=get_checksum(gf_path, algorithm="MD5"),
                                                date_added=datetime.utcnow(), permissions=fp,
                                                state=DataFileWorkState.GENERATED))
        return generated_files

    def __ingest_files(self, pid: str, str_updated_metadata_json: str) -> {}:
        logger(f'Ingesting files to {pid}', "debug", self.app_name)
        str_dv_file = transform(
            transformer_url=self.target.metadata.transformed_metadata[1].transformer_url,
            str_tobe_transformed=str_updated_metadata_json
        )

        for _ in db_manager.find_non_generated_files(dataset_id=self.dataset_id):
            jsonData = json.loads(str_dv_file).get(_.name)
            if jsonData:
                data = {
                    "jsonData": f'{json.dumps(jsonData)}'
                }
                files = {
                    "file": (_.name, open(_.path, "rb")),
                }

                response_ingest_file = requests.post(
                    f"{self.target.base_url}/api/datasets/:persistentId/add?persistentId={pid}",
                    headers=dmz_dataverse_headers('API_KEY', self.target.password), data=data, files=files)
                # Print the response
                logger(
                    f'Adding file {_.name}. Response.status_code: {response_ingest_file.status_code}. Response text: '
                    f'{response_ingest_file.text}', LOG_LEVEL_DEBUG, self.app_name)

                if response_ingest_file.status_code != status.HTTP_200_OK:
                    return {"status": "error", "message": response_ingest_file.text}
                dv_resp = response_ingest_file.json()
                logger(f"file ID: {dv_resp['data']['files'][0]['dataFile']['id']}", LOG_LEVEL_DEBUG, self.app_name)
                if jsonData.get('embargo'):
                    json_data = {
                        'dateAvailable': jsonData.get('embargo'),
                        'reason': '',
                        'fileIds': [
                            dv_resp['data']['files'][0]['dataFile']['id'],
                        ],
                    }
                    logger(f'Set EMBARGO to pid {pid} with data is {json.dumps(json_data)}', LOG_LEVEL_DEBUG,
                           self.app_name)
                    response_embargo = requests.post(
                        f'{self.target.base_url}/api/datasets/:persistentId/files/actions/:set-embargo?persistentId='
                        f'{pid}', headers=dmz_dataverse_headers('API_KEY', self.target.password),
                        json=json_data)

                    logger(
                        f'Set EMBARGO file {_.name}. Response.status_code: {response_embargo.status_code}. Response'
                        f'text: {response_embargo.text}', LOG_LEVEL_DEBUG, self.app_name)
                    if response_embargo.status_code != status.HTTP_200_OK:
                        return {"status": "error", "message": response_embargo.text}

        return {"status": status.HTTP_200_OK}

    def __publish_dataset(self, pid) -> int:
        url = f"{self.target.base_url}/api/datasets/:persistentId/actions/:publish?persistentId={pid}&type=major"
        headers = {
            "Content-Type": "application/json",
            "X-Dataverse-key": self.target.password,
        }
        logger(f"published url: {url}", "debug", self.app_name)
        response = requests.post(url, headers=headers)
        logger(
            f"response code for published: {response.status_code} with response.text: {response.text}",
            "debug",
            self.app_name,
        )
        return response.status_code
