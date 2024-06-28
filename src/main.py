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
import multiprocessing
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import emoji
import uvicorn
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from fastapi_events.middleware import EventHandlerASGIMiddleware
from gunicorn.app.wsgiapp import WSGIApplication
from keycloak import KeycloakOpenID, KeycloakAuthenticationError

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

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")  # use token authentication


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
                logger(str(user_info.items()), LOG_LEVEL_DEBUG, "ps")

                return
            except KeycloakAuthenticationError as e:
                logger(e.response_code, 'error', LOG_NAME_PS)
            except BaseException as e:
                logger(e, 'error', LOG_NAME_PS)

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Forbidden"
        )


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
    )

    app.add_middleware(EventHandlerASGIMiddleware,
                       handlers=[local_handler])  # registering handler(s)

    # register routers
    app.include_router(public.router, tags=["Public"], prefix="")
    app.include_router(protected.router, tags=["Protected"], prefix="", dependencies=[Depends(auth_header)])

    if settings.DEPLOYMENT in ['demo', 'local']:
        app.include_router(upload_files, prefix="/files", dependencies=[Depends(auth_header)])
        app.include_router(tus_files.router, prefix="")


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
    logger('MULTIPLE WORKERS', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    options = {
        "bind": "0.0.0.0:10124",
        "workers": (multiprocessing.cpu_count() * 2) + 1,
        "worker_class": "uvicorn.workers.UvicornWorker",
        "--preload": True
    }
    PackagingServiceApplication("src.main:app", options).run()


@local_handler.register(event_name="cat*")
def handle_all_cat_events(event: Event):
    event_name, payload = event
    print(f'event_name: {event_name}, payload: {payload}')


if __name__ == "__main__":
    logger('START Packaging Service', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    if settings.get("SENDMAIL_ENABLE"):
        send_mail(f'Starting the packaging service',
                  f'Started at {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")}')
    import platform
    logger(f'Python version: {platform.python_version()}', LOG_LEVEL_DEBUG, 'ps')

    if settings.get("MULTIPLE_WORKERS_ENABLE", False):
        logger('MULTIPLE WORKERS', LOG_LEVEL_DEBUG, 'ps')
        run()
    else:
        logger('SINGLE WORKER', LOG_LEVEL_DEBUG, 'ps')
        uvicorn.run("src.main:app", host="0.0.0.0", port=10124, reload=False, workers=1)