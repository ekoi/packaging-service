from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

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
