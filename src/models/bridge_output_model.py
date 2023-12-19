from __future__ import annotations

from datetime import datetime
from enum import StrEnum, auto
from typing import List, Optional

from pydantic import BaseModel, Field

from src.dbz import DepositStatus


class ResponseContentType(StrEnum):
    XML = auto()
    JSON = auto()
    TEXT = auto()
    RDF = auto()
    UNDEFINED = auto()


class IdentifierProtocol(StrEnum):
    DOI = auto()
    HANDLE = auto()
    URN_NBN = 'urn:nbn'
    URN_UUID = 'urn:uuid'
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
    duration: float = 0.0
    status: Optional[str] = None
    error: Optional[str] = None
    message: str = None
    identifiers: Optional[List[IdentifierItem]] = None
    content: str = None
    content_type: ResponseContentType = Field(None, alias='content-type')


# # it = IdentifierItem()
#
# y = TargetResponse(content="eko", message="", identifiers=[])
# print(y.model_dump_json())


class BridgeOutputDataModel(BaseModel):
    deposit_time: Optional[str] = Field(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), alias='deposit-time')
    deposit_status: DepositStatus = Field(DepositStatus.UNDEFINED, alias='deposit-status')
    notes: str = None  # This is for any message/text
    response: TargetResponse = None


json_output_model = {
    "deposit-time": "",
    "deposit-status": "initial",
    "duration": 0.0,
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
        "content-type": ResponseContentType.XML
    }
}
response_json = {
    "url": "",
    "status-code": 200,
    "error": "",
    "message": "Any message from response.",
    "identifiers": [],
    "content": "",
    "content-type": ResponseContentType.XML
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
