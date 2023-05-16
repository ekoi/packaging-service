import json
import logging
import os

import requests
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from dynaconf import Dynaconf

import importlib.metadata
from pydantic import BaseModel
__version__ = importlib.metadata.metadata("packaging-service")["version"]

from starlette.responses import Response

settings = Dynaconf(settings_files=["conf/settings.toml"],
                    environments=True)
logging.basicConfig(filename=settings.LOG_FILE, level=settings.LOG_LEVEL,
                    format=settings.LOG_FORMAT)

app = FastAPI(title=settings.FASTAPI_TITLE, description=settings.FASTAPI_DESCRIPTION,
              version=__version__)

data = {}


@app.get('/')
def info():
    logging.info("packaging service")
    logging.debug("info")
    return {"name": "packaging-service", "version": __version__}


@app.on_event('startup')
def common_data():
    logging.debug("startup")


class Item(BaseModel):
    name: str



@app.post('/sendbox')
async def buildScaffolding(request: Request):
    form_data = await request.form()
    print(form_data)
    return form_data
    # return {"checked": input_text, "error": True, "message": "No data found from 'https://raw.githubusercontent.com/ekoi/DANS-File-Formats/draft/dans-file-formats.json'"}


if __name__ == "__main__":
    logging.info("Start")
    uvicorn.run("src.main:app", host="0.0.0.0", port=2005, reload=False)
