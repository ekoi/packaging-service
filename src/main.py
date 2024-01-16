"""
Packaging Service FastAPI Application

This FastAPI application provides endpoints for file uploads, public access, and protected access.
It integrates Keycloak for OAuth2-based authentication and supports token-based authentication with API keys.

Modules:
- `public`: Contains public access routes.
- `protected`: Contains protected access routes.
- `tus_files`: Contains routes for handling file uploads using the Tus protocol.
- `commons`: Contains common settings, logger setup, and utility functions.
- `InspectBridgeModule`: Provides a utility for inspecting bridge module classes.
- `db_manager`: Manages the creation of the database and tables.

Dependencies:
- `fastapi`: Web framework for building APIs with Python.
- `starlette`: Asynchronous framework for building APIs.
- `uvicorn`: ASGI server for running the FastAPI application.
- `keycloak`: Provides integration with Keycloak for authentication.
- `emoji`: Library for adding emoji support to Python applications.

"""

import importlib.metadata
import os
from contextlib import asynccontextmanager

import multiprocessing

from gunicorn.app.wsgiapp import WSGIApplication

from fastapi_events.dispatcher import dispatch
from fastapi_events.middleware import EventHandlerASGIMiddleware
from fastapi_events.handlers.local import local_handler

from datetime import datetime

import emoji
import uvicorn
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from keycloak import KeycloakOpenID, KeycloakAuthenticationError

__version__ = importlib.metadata.metadata("packaging-service")["version"]

from starlette import status
from starlette.middleware.cors import CORSMiddleware

from src import public, protected, tus_files
from src.commons import settings, setup_logger, data, InspectBridgeModule, db_manager, logger, send_mail

from src.tus_files import upload_files

from fastapi_events.handlers.local import local_handler
from fastapi_events.typing import Event


@asynccontextmanager
async def lifespan(application: FastAPI):
    """
    Lifespan event handler for the FastAPI application.

    This function is executed during the startup of the FastAPI application.
    It initializes the database, iterates through saved bridge module directories,
    and prints available bridge classes.

    Args:
        application (FastAPI): The FastAPI application.

    Yields:
        None: The context manager does not yield any value.

    """
    print('start up')
    if not os.path.exists(settings.DB_URL):
        logger('Creating database', 'debug', 'ps')
        db_manager.create_db_and_tables()
    else:
        logger('Database already exists', 'debug', 'ps')
    iterate_saved_bridge_module_dir()
    print(f'Available bridge classes: {sorted(list(data.keys()))}')
    print(emoji.emojize(':thumbs_up:'))

    yield


app = FastAPI(
    title=settings.FASTAPI_TITLE,
    description=settings.FASTAPI_DESCRIPTION,
    version=__version__,
    lifespan=lifespan
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(EventHandlerASGIMiddleware,
                   handlers=[local_handler])   # registering handler(s)

# Todo: This is encrypted in the .secrets.toml
api_keys = [settings.DANS_PACKAGING_SERVICE_API_KEY]

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")  # use token authentication


# oauth2_scheme = OAuth2AuthorizationCodeBearer(
#     tokenUrl="http://localhost:9090/realms/ekoi/protocol/openid-connect/token",
#     authorizationUrl="http://localhost:9090/auth/realms/ekoi/protocol/openid-connect/auth")


def auth_header(request: Request, api_key: str = Depends(oauth2_scheme)):
    """
    Authentication header dependency function.

    This function is used as a dependency for protected routes.
    It checks the provided API key against a list of valid keys.
    If the key is not valid, it attempts to authenticate using Keycloak.

    Args:
        request (Request): The FastAPI request object.
        api_key (str): The API key provided in the request.

    Raises:
        HTTPException: Raised if authentication fails.

    Returns:
        None: No return value if authentication is successful.

    """
    if api_key not in api_keys:
        keycloak_env_name = f"keycloak_{request.headers['auth-env-name']}"
        keycloak_env = settings.get(keycloak_env_name)
        if keycloak_env is not None:
            try:
                keycloak_openid = KeycloakOpenID(
                    server_url=keycloak_env.URL,
                    client_id=keycloak_env.CLIENT_ID,
                    realm_name=keycloak_env.REALMS
                )
                user_info = keycloak_openid.userinfo(api_key)
                logger(str(user_info.items()), 'debug', "ps")

                return
            except KeycloakAuthenticationError as e:
                logger(e.response_code, 'error', 'ps')
            except BaseException as e:
                logger(e, 'error', 'ps')

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Forbidden"
        )


app.include_router(public.router, tags=["Public"], prefix="")
app.include_router(protected.router, tags=["Protected"], prefix="", dependencies=[Depends(auth_header)])

if settings.DEPLOYMENT != "production":
    app.include_router(upload_files, prefix="/files", dependencies=[Depends(auth_header)])
app.include_router(tus_files.router, prefix="")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# @app.get("/eko")
# def eko():
#
#     return ""
@app.get('/')
def info():
    """
    Root endpoint to retrieve information about the packaging service.

    Returns:
        dict: A dictionary containing the name and version of the packaging service.

    """
    # dispatch("cat ok", payload={"name": "EKO INDarto"})
    return {"name": "packaging-service", "version": __version__}


def iterate_saved_bridge_module_dir():
    """
    Iterates through saved bridge module directories.

    For each Python file in the modules directory, it inspects the file for bridge classes
    and updates the data dictionary with the class name.

    """
    for filename in os.listdir(settings.MODULES_DIR):
        if filename.endswith(".py") and not filename.startswith('__'):
            module_path = os.path.join(settings.MODULES_DIR, filename)
            cls_name = InspectBridgeModule.get_bridge_sub_class(path=module_path)
            if cls_name:
                data.update(cls_name)


class PackagingServiceApplication(WSGIApplication):
    def __init__(self, app_uri, options=None):
        self.options = options or {}
        self.app_uri = app_uri
        super().__init__()

    def load_config(self):
        config = {
            key: value
            for key, value in self.options.items()
            if key in self.cfg.settings and value is not None
        }
        for key, value in config.items():
            self.cfg.set(key.lower(), value)


def run():
    logger('MULTIPLE WORKERS', 'debug', 'ps')
    options = {
        "bind": "0.0.0.0:10124",
        "workers": (multiprocessing.cpu_count() * 2) + 1,
        "worker_class": "uvicorn.workers.UvicornWorker",
    }
    PackagingServiceApplication("src.main:app", options).run()


@local_handler.register(event_name="cat*")
def handle_all_cat_events(event: Event):
    event_name, payload = event
    print(f'event_name: {event_name}, payload: {payload}')

if __name__ == "__main__":
    send_mail(f'{settings.DEPLOYMENT}: Starting the packaging service',
              f'Started at {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")}')
    setup_logger()
    logger('START Packaging Service', 'debug', 'ps')

    import platform

    print(f'Python version: {platform.python_version()}')
    logger(f'Python version: {platform.python_version()}', 'debug', 'ps')

    if os.environ.get('run-local'):
        logger('SINGLE WORKER', 'debug', 'ps')
        uvicorn.run("src.main:app", host="0.0.0.0", port=10124, reload=False, workers=1)

    else:
        run()
