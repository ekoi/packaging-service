from __future__ import annotations

from dataclasses import dataclass

from pydantic import BaseModel, Field


class ResponseModel(BaseModel):
    status: str = ''
    dataset_id: str = Field('', alias='dataset-id')
    start_process: bool = Field(False, alias='start-process')


@dataclass(frozen=True, kw_only=True)
class InboxDatasetDC:
    assistant_name: str
    target_creds: str
    owner_id: str
    title: str = ''
    metadata: dict
    release_version: str
