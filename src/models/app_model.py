from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field


class ResponseDataModel(BaseModel):
    status: str = ''
    dataset_id: str = Field('', alias='dataset-id')
    start_process: Optional[bool] = Field(False, alias='start-process')


@dataclass(frozen=True, kw_only=True)
class InboxDatasetDataModel:
    assistant_name: str
    target_creds: str
    owner_id: str
    title: str = ''
    metadata: dict
    release_version: str


class TargetApp(BaseModel):
    repo_name: str = Field(None, alias='repo-name')
    display_name: str = Field(None, alias='display-name')
    deposit_status: str = Field(None, alias='deposit-status')
    deposit_time: str = Field(None, alias='deposit-time')
    duration: str = ''
    output_response: Dict[str, Any] = Field('', alias='output-response')


class Asset(BaseModel):
    dataset_id: str = Field(None, alias='dataset-id')
    title: str = ''
    md: str = ''
    created_date: str = Field(None, alias='created-date')
    saved_date: str = Field(None, alias='saved-date')
    submitted_date: str = Field(None, alias='submitted-date')
    release_version: str = Field(None, alias='release-version')
    version: str = ''
    targets: List[TargetApp] = []


# x = Asset()
# y = Target()
# x.targets.append(y)
class OwnerAssetsModel(BaseModel):
    owner_id: str = Field(None, alias='owner-id')
    assets: List[Asset] = []


json_data = {
    "owner-id": "",
    "assets": [
        {
            "dataset-id": "",
            "title": "ds.title",
            "created-date": "ds.created_date",
            "saved-date": "ds.saved_date",
            "submitted-date": "ds.submitted_date",
            "release-version": "ds.release_version",
            "version": "ds.version",
            "targets": [
                {
                    "repo-name": "",
                    "display-name": "",
                    "deposit-status": "",
                    "deposit-time": "",
                    "duration": "",
                    "output-response": {}
                }
            ]
        }
    ]
}
