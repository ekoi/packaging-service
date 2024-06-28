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
    URN_UUID = 'urn:uuid',
    SWHID = auto()
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
    notes: Optional[str] = "" # This is for any message/text
    response: TargetResponse = None


json_output_model = {
    "deposit-time": "",
    "deposit-status": "initial",
    "duration": 0.0,
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


