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
import importlib
# import importlib.metadata
import multiprocessing
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated

import emoji
import uvicorn
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer, HTTPBearer, HTTPAuthorizationCredentials
from fastapi_events.middleware import EventHandlerASGIMiddleware
from gunicorn.app.wsgiapp import WSGIApplication
from keycloak import KeycloakOpenID, KeycloakAuthenticationError

import multiprocessing
import platform
from datetime import datetime, timezone


__version__ = importlib.metadata.metadata("packaging-service")["version"]

from starlette import status
from starlette.middleware.cors import CORSMiddleware

from src import public, protected, tus_files
from src.commons import settings, setup_logger, data, db_manager, logger, send_mail, inspect_bridge_module, \
    LOG_LEVEL_DEBUG, LOG_NAME_PS

from src.tus_files import upload_files

from fastapi_events.handlers.local import local_handler
from fastapi_events.typing import Event

from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource


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
        logger('Creating database', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        db_manager.create_db_and_tables()
    else:
        logger('Database already exists', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    iterate_saved_bridge_module_dir()
    print(f'Available bridge classes: {sorted(list(data.keys()))}')
    print(emoji.emojize(':thumbs_up:'))

    yield


api_keys = [settings.DANS_PACKAGING_SERVICE_API_KEY]

security = HTTPBearer()


def auth_header(request: Request, auth_cred: Annotated[HTTPAuthorizationCredentials, Depends(security)]):
    """
    Simplified authentication header dependency function.

    This function checks the provided API key against a list of valid keys or attempts to authenticate using Keycloak.

    Args:
        request (Request): The FastAPI request object.
        auth_cred: The authorization credentials from the request.

    Raises:
        HTTPException: Raised if authentication fails.
    """
    api_key = auth_cred.credentials
    if api_key in api_keys:
        return

    keycloak_env = settings.get(f"keycloak_{request.headers['auth-env-name']}")
    if not keycloak_env:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Forbidden")

    try:
        keycloak_openid = KeycloakOpenID(server_url=keycloak_env.URL, client_id=keycloak_env.CLIENT_ID, realm_name=keycloak_env.REALMS)
        keycloak_openid.userinfo(api_key)
    except KeycloakAuthenticationError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Forbidden")

def pre_startup_routine(app: FastAPI) -> None:
    setup_logger()
    logger(f'MELT_ENABLE = {settings.get("MELT_ENABLE")}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    # add middlewares
    if settings.get("MELT_ENABLE", False):
        enable_otel(app)

    # Enable CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["Upload-Offset", "Location", "Upload-Length", "Tus-Version", "Tus-Resumable", "Tus-Max-Size",
                        "Tus-Extension", "Upload-Metadata", "Upload-Defer-Length", "Upload-Concat", "Upload-Incomplete",
                        "Upload-Complete", "Upload-Draft-Interop-Version"],

    )

    app.add_middleware(EventHandlerASGIMiddleware,
                       handlers=[local_handler])  # registering handler(s)

    # register routers
    app.include_router(public.router, tags=["Public"], prefix="")
    app.include_router(protected.router, tags=["Protected"], prefix="", dependencies=[Depends(auth_header)])

    # if settings.DEPLOYMENT in ['demo', 'local']:
    app.include_router(upload_files, prefix="/files", include_in_schema=True, dependencies=[Depends(auth_header)])
    # app.include_router(tus_files.router, prefix="", include_in_schema=False)


def enable_otel(app):
    melt_agent_host_name = settings.get("MELT_AGENT_HOST_NAME", "localhost")
    # Set up the tracer provider
    trace.set_tracer_provider(
        TracerProvider(resource=Resource.create({SERVICE_NAME: "Packaging Service"}))
    )
    tracer_provider = trace.get_tracer_provider()
    # Configure Jaeger exporter
    jaeger_exporter = JaegerExporter(
        agent_host_name=melt_agent_host_name,
        agent_port=6831,
        udp_split_oversized_batches=True,
    )
    # Add the Jaeger exporter to the tracer provider
    tracer_provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))
    FastAPIInstrumentor.instrument_app(app)


# create FastAPI app instance
app = FastAPI(
    title=settings.FASTAPI_TITLE,
    description=settings.FASTAPI_DESCRIPTION,
    version=__version__,
    lifespan=lifespan
)

pre_startup_routine(app)


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
            for cls_name in inspect_bridge_module(module_path):
                data.update(cls_name)


def run_server():
    """Configures and runs the server based on the environment settings."""
    if settings.get("MULTIPLE_WORKERS_ENABLE", False):
        uvicorn.run("src.main:app", host="0.0.0.0", port=10124, reload=False,
                    workers=(multiprocessing.cpu_count() * 2) + 1,
                    # worker_class="uvicorn.workers.UvicornWorker",
                    timeout_keep_alive= 300,
                    # preload=True
                    )
    else:
        uvicorn.run("src.main:app", host="0.0.0.0", port=10124, reload=False, workers=1)

if __name__ == "__main__":
    logger('START Packaging Service', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    if settings.get("SENDMAIL_ENABLE"):
        send_mail('Starting the packaging service',
                  f'Started at {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")}')
    run_server()