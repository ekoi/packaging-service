from __future__ import annotations

from typing import List

from pydantic import BaseModel



class Metadata(BaseModel):
    relativePath: str
    name: str
    type: str
    filetype: str
    filename: str


class Storage(BaseModel):
    type: str
    path: str


class Model(BaseModel):
    uuid: str
    offset: int
    size: int
    is_size_deferred: bool
    metadata: Metadata
    is_partial: bool
    is_final: bool
    partial_uploads: List
    expires: str
    storage: Storage
    created_at: str


json_data = '''{
  "uuid": "fd9ac8fbe56b4d6f931be890e5d0eb0b",
  "offset": 53668,
  "size": 53668,
  "is_size_deferred": false,
  "metadata": {
    "relativePath": "null",
    "name": "amalin.jpeg",
    "type": "image/jpeg",
    "filetype": "image/jpeg",
    "filename": "amalin.jpeg"
  },
  "is_partial": false,
  "is_final": true,
  "partial_uploads": [],
  "expires": "2023-12-13T05:29:02.880462",
  "storage": {
    "type": "filestore",
    "path": "./files/fd9ac8fbe56b4d6f931be890e5d0eb0b"
  },
  "created_at": "2023-12-12T04:29:02.865944"
}'''