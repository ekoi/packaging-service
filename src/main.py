import importlib.metadata
import logging

import jinja2
import uvicorn
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer

__version__ = importlib.metadata.metadata("packaging-service")["version"]

from starlette import status
from starlette.middleware.cors import CORSMiddleware

from src import public, protected, db
from src.commons import settings, data

logging.basicConfig(filename=settings.LOG_FILE, level=settings.LOG_LEVEL,
                    format=settings.LOG_FORMAT)

app = FastAPI(title=settings.FASTAPI_TITLE, description=settings.FASTAPI_DESCRIPTION,
              version=__version__)

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
    # Initiate jinja template (for file.xml)
    data.update({'template-env': jinja2.Environment(loader=jinja2.FileSystemLoader(
        searchpath=settings.jinja_template_dir))})


if __name__ == "__main__":
    logging.info("Start")
    db.create_tables(settings.DATA_DB_FILE)

    uvicorn.run("src.main:app", host="0.0.0.0", port=2005, reload=False)
