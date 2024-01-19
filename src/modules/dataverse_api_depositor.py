import json
import jmespath
import requests
from datetime import datetime
from src.commons import (
    settings,
    db_manager,
    DepositStatus,
    transform,
    logger,
    handle_deposit_exceptions,
)
from src.bridge import Bridge, BridgeOutputDataModel


class DataverseIngester(Bridge):
    def __init__(self, metadata_id, app_name, target_repo_json):
        self.transformer_dv_dataset_url = jmespath.search(
            f"transformer[?name=='{settings.TRANSFORMER_NAME_DATAVERSE_DATASET}'].url",
            target_repo_json,
        )[0]
        self.transformer_dv_file_url = jmespath.search(
            f"transformer[?name=='{settings.TRANSFORMER_NAME_DATAVERSE_FILE}'].url",
            target_repo_json,
        )[0]

        input_from_previous_repo = jmespath.search("in put", target_repo_json)
        if input_from_previous_repo:
            input_from_repo_name = jmespath.search("from", input_from_previous_repo)
            d = db_manager.find_target_repo_output_by_metadata_id_and_repo_name(
                metadata_id, input_from_repo_name
            )
            input_object_name = jmespath.search(
                '"object-name"', input_from_previous_repo
            )
            z = json.loads(self.str_metadata)
            z[input_object_name].append(json.loads(d))
            self.str_metadata = json.dumps(z)
            logger(self.str_metadata, "debug", app_name)
            logger(z, "debug", app_name)
        db_manager.update_ingest_status_by_id(
            DepositStatus.PROGRESS, self.metadataId, self.target_repo_name
        )

    @handle_deposit_exceptions
    def deposit(self) -> BridgeOutputDataModel:
        str_dv_metadata = transform(
            transformer_url=self.transformer_dv_dataset_url, input=self.str_metadata
        )
        headers = {
            "Content-Type": "application/json",
            "X-Dataverse-key": self.dv_api_key,
        }
        logger(f'deposit to "{self.dv_api_url}"', "debug", self.app_name)
        dv_response = requests.post(
            f"{self.dv_api_url}", headers=headers, data=str_dv_metadata
        )
        logger(
            f"dv_response.status_code: {dv_response.status_code} dv_response.text: {dv_response.text}",
            "debug",
            self.app_name,
        )
        ingest_status = DepositStatus.ERROR
        message = "Error"
        if dv_response.status_code == 201:
            logger("Data ingest successfully!", "debug", self.app_name)
            pid = dv_response.json()["data"]["persistentId"]
            logger(f"pid: {pid}", "debug", self.app_name)
            self.ingest_files(pid)
            if self.direct_publish:
                self.publish_dataset(pid)

            ingest_status = DepositStatus.FINISH
            message = "The dataset is successfully ingested"

        else:
            logger(
                f"Ingest failed with status code {dv_response.status_code}:",
                "debug",
                self.app_name,
            )
        db_manager.update_ingest_status_by_id(
            ingest_status, self.metadataId, self.target_repo_name
        )

        bridge_output_model = BridgeOutputDataModel(
            message=message, deposit_status=ingest_status
        )
        bridge_output_model.deposit_time = datetime.utcnow().strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )
        bridge_output_model.deposit_status = ingest_status
        return bridge_output_model

    def ingest_files(self, pid):
        str_dv_file = transform(
            transformer_url=self.transformer_dv_file_url, input=self.str_metadata
        )
        logger(f"str_dv_file: {str_dv_file}", "debug", self.app_name)
        dv_file_json = json.loads(str_dv_file)
        if not any(dv_file_json):
            return
        headers = {
            "X-Dataverse-key": self.dv_api_key,
        }

        for file in self.files:
            file_name = file[0]
            file_path = file[1]
            file_json = dv_file_json.get(file_name)
            if file_json:
                data = {"jsonData": f"{json.dumps(file_json)}"}
                f = {"file": (file_name, open(file_path, "rb"))}
                dv_deposit_file_url = f"{self.dv_base_url}/api/datasets/:persistentId/add?persistentId={pid}"
                logger(
                    f"dv_deposit_file_url: {dv_deposit_file_url}",
                    "debug",
                    self.app_name,
                )
                response = requests.post(
                    dv_deposit_file_url, headers=headers, data=data, files=f
                )
                # Print the response
                logger(
                    f"response.status_code: {response.status_code} response.text: {response.text}",
                    "debug",
                    self.app_name,
                )
            else:
                logger(
                    f'File "{file_name}" with path "{file_path}" is not ingested to dataverse.',
                    "debug",
                    self.app_name,
                )

    def publish_dataset(self, pid):
        url = f"{self.dv_base_url}/api/datasets/:persistentId/actions/:publish?persistentId={pid}&type=major"
        headers = {
            "Content-Type": "application/json",
            "X-Dataverse-key": self.dv_api_key,
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
