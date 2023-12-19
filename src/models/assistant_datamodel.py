# def taskX_A():
#     logger_a = logger.bind(task="AX")
#     logger_a.info("Starting task AX")
#     #do_something()
#     logger_a.success("End of task AX")
# def taskX_B():
#     logger_b = logger.bind(task="BY")
#     logger_b.info("Starting task BY")
#     #do_something_else()
#     logger_b.success("End of task BY")
# logger.add("file_A.log", filter=lambda record: record["extra"]["task"] == "AX")
# logger.add("file_B.log", filter=lambda record: record["extra"]["task"] == "BY")
# taskX_A()
# taskX_B()


from __future__ import annotations

import json
from typing import List, Optional

from pydantic import BaseModel, Field

from typing import List, Optional

from pydantic import BaseModel, Field


class TransformedMetadata(BaseModel):
    name: str
    transformer_url: Optional[str] = Field(None, alias='transformer-url')
    target_dir: Optional[str] = Field(None, alias='target-dir')
    restricted: Optional[bool] = None


class Metadata(BaseModel):
    specification: List[str]
    transformed_metadata: List[TransformedMetadata] = Field(
        ..., alias='transformed-metadata'
    )


class Target(BaseModel):
    repo_name: str = Field(..., alias='repo-name')
    repo_display_name: str = Field(..., alias='repo-display-name')
    bridge_module_class: str = Field(..., alias='bridge-module-class')
    base_url: str = Field(..., alias='base-url')
    target_url: str = Field(..., alias='target-url')
    username: str
    password: str
    metadata: Metadata


class FileConversion(BaseModel):
    origin_type: str = Field(..., alias='origin-type')
    target_type: str = Field(..., alias='target-type')
    conversion_url: str = Field(..., alias='conversion-url')


class RepoAssistantDataModel(BaseModel):
    assistant_config_name: str = Field(..., alias='assistant-config-name')
    description: str
    app_name: str = Field(..., alias='app-name')
    app_config_url: str = Field(..., alias='app-config-url')
    targets: List[Target]
    file_conversions: Optional[List[FileConversion]] = Field(None, alias='file-conversions')


