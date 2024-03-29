from __future__ import annotations

from typing import List

from pydantic import BaseModel
import json

import requests

from src.bridge import Bridge
from src.models.bridge_output_model import BridgeOutputDataModel, TargetResponse, ResponseContentType, IdentifierItem
from src.commons import transform, logger, handle_deposit_exceptions
from src.dbz import DepositStatus


class ZenodoApiDepositor(Bridge):
    @handle_deposit_exceptions
    def deposit(self) -> BridgeOutputDataModel:
        zenodo_resp = self.__create_initial_dataset()  # TODO if zenodo_resp return {}
        zenodo_id = zenodo_resp.get("id")
        str_zenodo_dataset_metadata = transform(self.target.metadata.transformed_metadata[0].transformer_url,
                                                self.metadata_rec.md)

        url = f'{self.target.target_url}/{zenodo_id}?{self.target.username}={self.target.password}'
        logger(f"Send to {url}", 'debug', self.app_name)
        zen_resp = requests.put(url, data=str_zenodo_dataset_metadata, headers={"Content-Type": "application/json"})
        bridge_output_model = BridgeOutputDataModel()
        if zen_resp.status_code == 200:
            zm = ZenodoModel(**zen_resp.json())
            bridge_output_model.deposit_status = DepositStatus.SUCCESS
            bridge_output_model.message = "This is a success"
            target_resp = TargetResponse(url=f'{self.target.target_url}/{zenodo_id}', status=DepositStatus.SUCCESS,
                                         content=json.dumps(zen_resp.json()), message="")
            target_resp.status_code = zen_resp.status_code
            target_resp.identifiers = [IdentifierItem(value=zm.metadata.prereserve_doi.doi, url=zm.links.html)]
            target_resp.content_type = ResponseContentType.JSON
            bridge_output_model.response = target_resp
        return bridge_output_model

    @handle_deposit_exceptions
    def __create_initial_dataset(self) -> json:
        logger('Create an initial zenodo dataset', 'debug', self.app_name)
        url = f"{self.target.target_url}?{self.target.username}={self.target.password}"
        logger(f"Send to {url}", 'debug', self.app_name)
        r = requests.post(url, data="{}", headers={"Content-Type": "application/json"})
        logger(f"Response status code: {r.status_code}", 'debug', self.app_name)
        if r.status_code == 201:
            r_json = r.json()
            return r_json
        return json.loads('{"":""}')


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
