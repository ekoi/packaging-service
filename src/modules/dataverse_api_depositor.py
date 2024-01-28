import json
from datetime import datetime

import requests

from src.commons import (
    settings,
    db_manager,
    DepositStatus,
    transform,
    logger,
    handle_deposit_exceptions,
)
from src.bridge import Bridge, BridgeOutputDataModel
from src.dbz import ReleaseVersion
from src.models.bridge_output_model import IdentifierItem, IdentifierProtocol, TargetResponse, ResponseContentType


class DataverseIngester(Bridge):

    @handle_deposit_exceptions
    def deposit(self) -> BridgeOutputDataModel:
        swh_form_md = json.loads(self.metadata_rec.md)
        swhid = db_manager.find_target_repo(self.dataset_id, self.target.input.from_target_name)
        if swhid:
            swh_form_md.update({"swhid": json.loads(swhid.target_output)['response']['identifiers'][0]['value']})

        logger(f"swh_form_md - after update (swhid): {json.dumps(swh_form_md)}", 'debug', self.app_name)
        str_dv_metadata = transform(
            transformer_url=self.target.metadata.transformed_metadata[0].transformer_url,
            str_tobe_transformed=json.dumps(swh_form_md)
        )
        logger(f'deposit to "{self.target.target_url}"', "debug", self.app_name)
        dv_response = requests.post(
            f"{self.target.target_url}", headers=self.__header_dv(), data=str_dv_metadata
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
            logger("Data ingest successfully!", "debug", self.app_name)
            pid = dv_response.json()["data"]["persistentId"]
            ideni = IdentifierItem(value=pid, url=f'{self.target.base_url}/dataset.xhtml?persistentId={pid}',
                                   protocol=IdentifierProtocol('doi'))
            identifier_items.append(ideni)
            logger(f"pid: {pid}", "debug", self.app_name)
            self.ingest_files(pid)
            if self.target.initial_release_version == ReleaseVersion.PUBLISHED:
                self.publish_dataset(pid)

            ingest_status = DepositStatus.FINISH
            message = "The dataset is successfully ingested"

        else:
            logger(
                f"Ingest failed with status code {dv_response.status_code}:",
                "debug",
                self.app_name,
            )
        bridge_output_model = BridgeOutputDataModel(
            message=message, deposit_status=ingest_status
        )
        bridge_output_model.deposit_time = datetime.utcnow().strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )
        target_repo = TargetResponse(url=self.target.target_url, status=DepositStatus.FINISH, message=message,
                                     identifiers=identifier_items, content=dv_response.text)
        target_repo.content_type = ResponseContentType.JSON
        target_repo.status_code = dv_response.status_code
        bridge_output_model = BridgeOutputDataModel(message=message, response=target_repo)
        bridge_output_model.deposit_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
        bridge_output_model.deposit_status = ingest_status
        return bridge_output_model

    def __header_dv(self):
        headers = {
            "X-Dataverse-key": self.target.password,
        }
        return headers

    def ingest_files(self, pid):
        str_dv_file = transform(
            transformer_url=self.target.metadata.transformed_metadata[1].transformer_url,
            str_tobe_transformed=self.metadata_rec.md
        )

        for _ in db_manager.find_non_generated_files(ds_id=self.dataset_id):
            jsonData = json.loads(str_dv_file).get(_.name)
            if jsonData:
                data = {
                    "jsonData": f'{json.dumps(jsonData)}'
                }
                files = {
                    "file": (_.name, open(_.path, "rb")),
                }

                response = requests.post(f"{self.target.base_url}/api/datasets/:persistentId/add?persistentId={pid}",
                                         headers=self.__header_dv(), data=data, files=files)
                # Print the response
                logger(f'Adding file {_.name}. Response.status_code: {response.status_code}. Response text: '
                       f'{response.text}', 'debug', self.app_name)

    def publish_dataset(self, pid):
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
        # TODO: exception when response is not 200
        return response.status_code == 200
