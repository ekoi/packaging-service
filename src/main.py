import json
import logging
import os

import requests
import uvicorn
from fastapi import FastAPI, Request, HTTPException, Body, UploadFile, File, Form, Depends
from dynaconf import Dynaconf

import importlib.metadata

from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
__version__ = importlib.metadata.metadata("packaging-service")["version"]

from starlette import status
from starlette.middleware.cors import CORSMiddleware

from starlette.responses import Response

from src import public, protected, db
from src.commons import settings

logging.basicConfig(filename=settings.LOG_FILE, level=settings.LOG_LEVEL,
                    format=settings.LOG_FORMAT)

app = FastAPI(title=settings.FASTAPI_TITLE, description=settings.FASTAPI_DESCRIPTION,
              version=__version__)

data = {}


api_keys = [
    settings.DANS_PACKAGING_SERVICE_API_KEY
]  # Todo: This is encrypted in the .secrets.toml

# Authorization Form: It doesn't matter what you type in the form, it won't work yet. But we'll get there.
# See: https://fastapi.tiangolo.com/tutorial/security/first-steps/
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")  # use token authentication


def api_key_auth(api_key: str = Depends(oauth2_scheme)):
    if api_key not in api_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Forbidden"
        )

log = logging.getLogger(__name__)

app.include_router(
    public.router,
    tags=["Public"],
    prefix=""
)

app.include_router(
    protected.router,
    tags=["Protected"],
    prefix="",
    dependencies=[Depends(api_key_auth)]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
@app.get('/')
def info():
    logging.info("packaging service")
    logging.debug("info")
    return {"name": "packaging-service", "version": __version__}


@app.on_event('startup')
def common_data():
    logging.debug("startup")


if __name__ == "__main__":
    logging.info("Start")
    #
    # sql_create_form_metadata_table = """
    #         CREATE TABLE `form_metadata` (`id` uuid PRIMARY KEY,`created_time` datetime NOT NULL,
    #         `metadata_id` text UNIQUE NOT NULL, `metadata` text NOT NULL,
    #         `list_files` text);"""
    # # todo: if not found, creates one.
    #
    #
    #
    #
    # # create a database connection
    # conn = db.create_sqlite3_connection(settings.DATA_DB_FILE)
    #
    # # create tables
    # if conn is not None:
    #     # create inbox table
    #     db.create_table(conn, sql_create_form_metadata_table)
    # else:
    #     print("Error! cannot create the database connection.")
    db.create_tables(settings.DATA_DB_FILE)

    uvicorn.run("src.main:app", host="0.0.0.0", port=2005, reload=False)
