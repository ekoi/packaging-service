from __future__ import annotations

from datetime import datetime
from enum import StrEnum, auto
from typing import List, Optional

from pydantic import BaseModel, Field

from src.dbz import DepositStatus


class TargetResponseType(StrEnum):
    XML = auto()
    JSON = auto()
    TEXT = auto()
    RDF = auto()
    UNDEFINED = auto()


class IdentifierProtocol(StrEnum):
    DOI = auto()
    HANDLE = auto()
    URN_NBN = 'urn:nbn'
    UNDEFINED = auto()


class IdentifierItem(BaseModel):
    value: str = None
    protocol: IdentifierProtocol = IdentifierProtocol.DOI
    url: Optional[str] = None


# ii = IdentifierProtocol("urn:nbn")
# print(ii)
# i = IdentifierItem(value="eko", protocol=IdentifierProtocol.URN_NBN, url="https://googl" )
# print(i.protocol.value)

class TargetResponse(BaseModel):
    url: Optional[str] = None
    status_code: int = Field(default=-10122004, alias='status-code')
    status: Optional[str] = None
    error: Optional[str] = None
    duration: int = 0
    message: str = None
    identifiers: Optional[List[IdentifierItem]] = None
    content: str = None
    content_type: TargetResponseType = Field(None, alias='content-type')


# # it = IdentifierItem()
#
# y = TargetResponse(content="eko", message="", identifiers=[])
# print(y.model_dump_json())


class BridgeOutputModel(BaseModel):
    timestamp: Optional[str] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
    deposit_status: DepositStatus = Field(DepositStatus.UNDEFINED, alias='deposit-status')
    message: str = None
    response: TargetResponse = None


# z = BridgeOutputModel(message="ekoindart", response=y)
# print(type(z.deposit_status.value))
# print(z)
#
json_output_model = {
    "timestamp": "",
    "deposit-status": "",
    "message": "Any message from Bridge. e.g: Sword ingest is accepted. It can be the same as Target Response message.",
    "response": {
        "url": "",
        "status-code": 200,
        "status": "",
        "error": "",
        "message": "Any message from response.",
        "identifiers": [
            {
                "value": "doi",
                "protocol": "doi",
                "url": ""
            },
            {
                "value": "doi",
                "protocol": IdentifierProtocol.DOI
            }
        ],
        "content": "",
        "content-type": TargetResponseType.XML
    }
}
response_json = {
    "url": "",
    "status-code": 200,
    "error": "",
    "message": "Any message from response.",
    "identifiers": [],
    "content": "",
    "content-type": TargetResponseType.XML
}
# z = BridgeOutputModel(response=TargetResponse())
# z = TargetResponse.model_validate(response_json)
# url: Optional[str]
# status_code: Optional[int] = Field(..., alias='status-code')
# error: Optional[str]
# message: str
# identifiers: Optional[List[IdentifierItem]]
# content: str
# content_type: TargetResponseType = Field(..., alias='content-type')
# y = TargetResponse(url="", status_code=123, error="", message="ll", identifiers=[], content='xxx', content_type=TargetResponseType.XML)
# x = BridgeOutputModel.model_validate(json_output_model)
# print(x.model_dump_json(by_alias=True, indent=4))
