from __future__ import annotations

import json
from datetime import datetime
from typing import List

import requests
from pydantic import BaseModel
from starlette import status

from src.bridge import Bridge
from src.commons import transform, logger, handle_deposit_exceptions, db_manager, LOG_LEVEL_DEBUG
from src.dbz import DepositStatus
from src.models.bridge_output_model import BridgeOutputDataModel, TargetResponse, ResponseContentType, IdentifierItem


class ZenodoApiDepositor(Bridge):
    @handle_deposit_exceptions
    def deposit(self) -> BridgeOutputDataModel:
        zenodo_resp = self.__create_initial_dataset()
        if zenodo_resp is None:
            return BridgeOutputDataModel(notes="Error occurs: status code: 500", deposit_status=DepositStatus.ERROR)
        zenodo_id = zenodo_resp.get("id")
        str_zenodo_dataset_metadata = transform(self.target.metadata.transformed_metadata[0].transformer_url,
                                                self.metadata_rec.md)

        url = f'{self.target.target_url}/{zenodo_id}?{self.target.username}={self.target.password}'
        logger(f"Send to {url}", LOG_LEVEL_DEBUG, self.app_name)
        zen_resp = requests.put(url, data=str_zenodo_dataset_metadata, headers={"Content-Type": "application/json"})
        logger(f'Zenodo response status code: {zen_resp.status_code}. Zenodo response: {zen_resp.text}',
               LOG_LEVEL_DEBUG, self.app_name)
        bridge_output_model = BridgeOutputDataModel()
        if zen_resp.status_code != status.HTTP_200_OK:
            logger(f'Error occurs: status code: {zen_resp.status_code}', 'error', self.app_name)
            bridge_output_model.notes = "Error occurs: status code: " + str(zen_resp.status_code)
            bridge_output_model.response = zen_resp.text
            bridge_output_model = BridgeOutputDataModel(notes=zen_resp.text, response=zen_resp)
            bridge_output_model.deposit_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
            bridge_output_model.deposit_status = DepositStatus.ERROR
            return bridge_output_model
        zm = ZenodoModel(**zen_resp.json())
        self.__ingest_files(zm.links.bucket)
        bridge_output_model.deposit_status = DepositStatus.SUCCESS
        bridge_output_model.notes = "Successfully deposited to Zenodo."
        bridge_output_model.deposit_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
        target_resp = TargetResponse(url=f'{self.target.target_url}/{zenodo_id}', status=DepositStatus.SUCCESS,
                                     content=json.dumps(zen_resp.json()), message="Successfully deposited to Zenodo")
        target_resp.status_code = zen_resp.status_code
        target_resp.identifiers = [IdentifierItem(value=zm.metadata.prereserve_doi.doi, url=zm.links.html)]
        target_resp.content_type = ResponseContentType.JSON
        bridge_output_model.response = target_resp
        logger(f"Successfully deposited to Zenodo. Zenodo response: {zen_resp.text}", LOG_LEVEL_DEBUG, self.app_name)
        return bridge_output_model

    @handle_deposit_exceptions
    def __create_initial_dataset(self) -> dict | None:
        logger('Create an initial zenodo dataset', LOG_LEVEL_DEBUG, self.app_name)
        response = requests.post(f"{self.target.target_url}?{self.target.username}={self.target.password}",
                                 data="{}", headers={"Content-Type": "application/json"})
        logger(f"Response status code: {response.status_code}", LOG_LEVEL_DEBUG, self.app_name)
        return response.json() if response.status_code == 201 else None

    def __ingest_files(self, bucket_url: str) -> dict:
        logger(f'Ingesting files to {bucket_url}', "debug", self.app_name)
        params = {'access_token': self.target.password, 'access_right': 'restricted'}
        for file in db_manager.find_non_generated_files(dataset_id=self.dataset_id):
            file_path = f"{file.path}/{file.name}"
            logger(f'Ingesting file {file_path}', "debug", self.app_name)
            with open(file_path, "rb") as fp:
                response = requests.put(f"{bucket_url}/{file.name}", data=fp, params=params)
            logger(f"Response status code: {response.status_code} and message: {response.text}", LOG_LEVEL_DEBUG, self.app_name)
        return {"status": status.HTTP_200_OK}


class PrereserveDoi(BaseModel):
    doi: str
    recid: int


class Metadata(BaseModel):
    access_right: str
    prereserve_doi: PrereserveDoi


class Links(BaseModel):
    self: str
    html: str
    badge: str
    files: str
    bucket: str
    latest_draft: str
    latest_draft_html: str
    publish: str
    edit: str
    discard: str
    newversion: str
    registerconceptdoi: str


class ZenodoModel(BaseModel):
    created: str
    modified: str
    id: int
    conceptrecid: str
    metadata: Metadata
    title: str
    links: Links
    record_id: int
    owner: int
    files: List
    state: str
    submitted: bool


json_data_zenodo_model = '''{
    "created": "2023-12-11T17:50:54.342124+00:00",
    "modified": "2023-12-11T17:50:54.380509+00:00",
    "id": 10358181,
    "conceptrecid": "10358180",
    "metadata": {
        "access_right": "open",
        "prereserve_doi": {
            "doi": "10.5281/zenodo.10358181",
            "recid": 10358181
        }
    },
    "title": "",
    "links": {
        "self": "https://zenodo.org/api/deposit/depositions/10358181",
        "html": "https://zenodo.org/deposit/10358181",
        "badge": "https://zenodo.org/badge/doi/.svg",
        "files": "https://zenodo.org/api/deposit/depositions/10358181/files",
        "bucket": "https://zenodo.org/api/files/b40b73d8-7550-415d-b91e-b981b13e61be",
        "latest_draft": "https://zenodo.org/api/deposit/depositions/10358181",
        "latest_draft_html": "https://zenodo.org/deposit/10358181",
        "publish": "https://zenodo.org/api/deposit/depositions/10358181/actions/publish",
        "edit": "https://zenodo.org/api/deposit/depositions/10358181/actions/edit",
        "discard": "https://zenodo.org/api/deposit/depositions/10358181/actions/discard",
        "newversion": "https://zenodo.org/api/deposit/depositions/10358181/actions/newversion",
        "registerconceptdoi": "https://zenodo.org/api/deposit/depositions/10358181/actions/registerconceptdoi"
    },
    "record_id": 10358181,
    "owner": 548524,
    "files": [],
    "state": "unsubmitted",
    "submitted": false
}
'''
# x = json.loads(json_data_zenodo_model)
# zm = ZenodoModel(**x)
# print(zm.links.self)
