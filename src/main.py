import importlib.metadata
import logging
import os
from datetime import datetime

import emoji
import uvicorn
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from keycloak import KeycloakOpenID, KeycloakAuthenticationError
from contextlib import asynccontextmanager

__version__ = importlib.metadata.metadata("packaging-service")["version"]

from starlette import status
from starlette.middleware.cors import CORSMiddleware

from src import public, protected, tus_files
from src.commons import settings, setup_logger, logger, data, InspectBridgeModule, db_manager


from fastapi_tusd import TusRouter

from src.tus_files import upload_files


# logging.basicConfig(filename=settings.LOG_FILE, level=settings.LOG_LEVEL,
#                     format=settings.LOG_FORMAT)

@asynccontextmanager
async def lifespan(application: FastAPI):
    print('start up')
    db_manager.create_db_and_tables()
    iterate_saved_bridge_module_dir()
    print(f'Available bridge classes: {sorted(list(data.keys()))}')
    print(emoji.emojize(':thumbs_up:'))

    yield


app = FastAPI(title=settings.FASTAPI_TITLE, description=settings.FASTAPI_DESCRIPTION,
              version=__version__, lifespan=lifespan)



app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

api_keys = [
    settings.DANS_PACKAGING_SERVICE_API_KEY
]  # Todo: This is encrypted in the .secrets.toml

# Authorization Form: It doesn't matter what you type in the form, it won't work yet. But we'll get there.
# See: https://fastapi.tiangolo.com/tutorial/security/first-steps/
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")  # use token authentication


# oauth2_scheme = OAuth2AuthorizationCodeBearer(
#     tokenUrl="http://localhost:9090/realms/ekoi/protocol/openid-connect/token",
#     authorizationUrl="http://localhost:9090/auth/realms/ekoi/protocol/openid-connect/auth")


def auth_header(request: Request, api_key: str = Depends(oauth2_scheme)):
    # first check whether the api_key exist in the settings.toml
    if api_key not in api_keys:
        # using token from keycloak
        keycloak_env_name = f"keycloak_{request.headers['auth-env-name']}"
        keycloak_env = settings.get(keycloak_env_name)
        # if keycloak_env
        if keycloak_env is not None:
            try:
                keycloak_openid = KeycloakOpenID(server_url=keycloak_env.URL,
                                                 client_id=keycloak_env.CLIENT_ID,
                                                 realm_name=keycloak_env.REALMS)
                user_info = keycloak_openid.userinfo(api_key)
                logging.debug(user_info.items())
                return
            except KeycloakAuthenticationError as e:
                logging.debug(e.response_code)

            except BaseException as e:
                logging.debug(e)

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Forbidden"
        )


app.include_router(upload_files,
    tags=["Upload Files"],
    prefix="/files",
)

app.include_router(tus_files.router,
    tags=["Upload Files"],
    prefix="",
)

app.include_router(
    public.router,
    tags=["Public"],
    prefix=""
)

app.include_router(
    protected.router,
    tags=["Protected"],
    prefix="",
    dependencies=[Depends(auth_header)]
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


def iterate_saved_bridge_module_dir():
    for filename in os.listdir(settings.MODULES_DIR):
        if filename.endswith(".py") and not filename.startswith('__'):
            module_path = os.path.join(settings.MODULES_DIR, filename)
            cls_name = InspectBridgeModule.get_bridge_sub_class(path=module_path)
            if cls_name:
                data.update(cls_name)

        else:
            continue


if __name__ == "__main__":
    import platform
    print(f'Python version: {platform.python_version()}')
    setup_logger()


    uvicorn.run("src.main:app", host="0.0.0.0", port=2005, reload=False)
