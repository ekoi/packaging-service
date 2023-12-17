import logging
import os
import platform
import re
import time
from datetime import datetime
from functools import wraps
from logging.handlers import TimedRotatingFileHandler
from typing import Any, Callable, Tuple, Dict

import requests
from dynaconf import Dynaconf
from fastapi import HTTPException

from src.dbz import DatabaseManager, DepositStatus
from src.models.bridge_output_model import BridgeOutputModel, TargetResponse

settings = Dynaconf(root_path=f'{os.getenv("BASE_DIR")}/conf', settings_files=["*.toml"],
                    environments=True)

data = {}

db_manager = DatabaseManager(db_dialect=settings.DB_DIALECT, db_url=settings.DB_URL)

transformer_headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {settings.DANS_TRANSFORMER_SERVICE_API_KEY}'
}

assistant_repo_headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {settings.DANS_REPO_ASSISTANT_SERVICE_API_KEY}'
}


def setup_logger():
    now = datetime.utcnow()
    for log in settings.LOGGERS:
        log_setup = logging.getLogger(log.get('name'))
        formatter = logging.Formatter(log.get('log_format'))
        fileHandler = logging.FileHandler(log.get('log_file'), mode='a')
        fileHandler.setFormatter(formatter)
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)
        rotatingHandler = TimedRotatingFileHandler(log.get('log_file'), when="H", interval=8, backupCount=10)
        log_setup.addHandler(rotatingHandler)
        log_setup.setLevel(log.get('log_level'))
        log_setup.addHandler(fileHandler)
        log_setup.addHandler(streamHandler)
        logger(f"Start {log.get('name')} at {now} Pyton version: {platform.python_version()}",
               'debug', log.get('name'))


def logger(msg, level, logfile):
    log = logging.getLogger(logfile)
    if level == 'info': log.info(msg)
    if level == 'warning': log.warning(msg)
    if level == 'error': log.error(msg)
    if level == 'debug': log.debug(msg)


def get_class(kls) -> Any:
    parts = kls.split('.')
    module = ".".join(parts[:-1])
    import src.modules.dans_sword_depositor
    try:
        m = __import__(module)
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m
    except ModuleNotFoundError as e:
        print(f'error: {kls}')
        logger(f'ModuleNotFoundError: {e}', 'error', 'ps')
    return None


def transform(transformer_url: str, input: str) -> str:
    logger(f'transformer_url: {transformer_url}', 'debug', 'ps')
    logger(f'input: {input}', 'debug', 'ps')
    transformer_response = requests.post(transformer_url, headers=transformer_headers, data=input)
    if transformer_response.status_code == 200:
        transformed_metadata = transformer_response.json()
        str_transformed_metadata = transformed_metadata.get('result')
        logger(f'Transformer result: {str_transformed_metadata}', 'debug', 'ps')
        return str_transformed_metadata

    logger(f'transformer_response.status_code: {transformer_response.status_code}', 'error', 'ps')
    raise ValueError(f"Error - Transfomer response status code: {transformer_response.status_code}")


# def transform(transformer_url: str, input: str) -> str:
#     logger(f'transformer_url: {transformer_url}', 'debug', 'ps')
#     logger(f'input: {input}', 'debug', 'ps')
#     try:
#         transformer_response = requests.post(transformer_url, headers=transformer_headers, data=input)
#         if transformer_response.status_code == 200:
#             transformed_metadata = transformer_response.json()
#             str_transformed_metadata = transformed_metadata.get('result')
#             logger(f'Transformer result: {str_transformed_metadata}', 'debug', 'ps')
#             return str_transformed_metadata
#         logger(f'transformer_response.status_code: {transformer_response.status_code}', 'error', 'ps')
#         raise ValueError(f"Error - Transfomer response status code: {transformer_response.status_code}")
#     except ConnectionError as ce:
#         logger(f'Errors during transformer: {ce.with_traceback(ce.__traceback__)}', 'debug', 'ps')
#         raise ValueError(f"Error - {ce.with_traceback(ce.__traceback__)}")
#     except Exception as ex:
#         raise ValueError(f"Error - {ex.with_traceback(ex.__traceback__)}")


# def handle_deposit_exceptions(bridge_output_model: BridgeOutputModel) -> Callable[
#     [Any], Callable[[tuple[Any, ...], dict[str, Any]], BridgeOutputModel | Any]]:
#     def decorator(func):
#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             try:
#                 print("start")
#                 print(f'kwargs: {kwargs}')
#                 print(f'args: {args}')
#                 # Call the original function
#                 rv = func(*args, **kwargs)
#                 print("end")
#                 return rv
#             except Exception as ex:
#                 # Handle the exception and provide the default response
#                 logger(f'Errors in {func.__name__}: {ex.with_traceback(ex.__traceback__)}',
#                        'debug', 'ps')
#                 bridge_output_model.deposit_status = DepositStatus.ERROR
#                 target_response = TargetResponse()
#                 target_response.duration=10100
#                 target_response.error="hello error"
#                 bridge_output_model.message = "this is bridge message"
#                 target_response.message = "TARGET MESSAGE"
#                 bridge_output_model.response = target_response
#                 return bridge_output_model
#
#         return wrapper
#
#     return decorator


def save_duration(target_id: int) -> type(None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            print(f'kwargs: {kwargs}')
            print(f'args: {args}')
            rv = func(*args, **kwargs)
            duration = time.time() - start
            print(duration)
            return rv

        return wrapper

    return decorator


def handle_deposit_exceptions(func) -> Callable[[tuple[Any, ...], dict[str, Any]], BridgeOutputModel | Any]:
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger(f'handle_deposit_exceptions for {func.__name__}. args: {args}', 'debug', 'ps')
        try:
            rv = func(*args, **kwargs)
            return rv
        except Exception as ex:
            logger(f'Errors in {func.__name__}: {ex} - {ex.with_traceback(ex.__traceback__)}',
                   'debug', 'ps')
            target = args[0].target
            bom = BridgeOutputModel()
            bom.deposit_status = DepositStatus.ERROR
            bom.message = f'Errors in {func.__name__}: {ex.with_traceback(ex.__traceback__)}'
            tr = TargetResponse()
            tr.url = target.target_url
            tr.status = DepositStatus.ERROR
            tr.error = f'error: {ex.with_traceback(ex.__traceback__)}'
            tr.message = f"Error {func.__name__}. Causes: {ex.__class__.__name__} {ex}"
            bom.response = tr
            return bom

    return wrapper


def handle_ps_exceptions(func) -> Any:
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            logger(f'handle_ps_exceptions:: {func.__name__}', 'debug', 'ps')
            rv = func(*args, **kwargs)
            return rv
        except HTTPException as ex:
            print("INDARTO1")
            logger(
                f'handle_ps_exceptions: Errors in {func.__name__}. status code: {ex.status_code}. Details: {ex.detail}. '
                f'args: {args}', 'debug', 'ps')
            raise ex
        except Exception as ex:
            print("INDARTO222")
            logger(f'handle_ps_exceptions: Errors in {func.__name__}: {ex} - {ex.with_traceback(ex.__traceback__)}',
                   'debug', 'ps')
            raise ex
        except BaseException as ex:
            logger(f'handle_ps_exceptions: Errors in {func.__name__}:  {ex} - {ex.with_traceback(ex.__traceback__)}',
                   'debug', 'ps')
            raise ex

    return wrapper


class InspectBridgeModule:

    @staticmethod
    def get_bridge_sub_class(path: str) -> Any:
        pattern = re.compile(r'class\s+(.*?)\(Bridge\):', re.DOTALL)
        if os.path.isfile(path):
            with open(path) as f:
                for line in f:
                    match = pattern.search(line)
                    if match:
                        words_between = match.group(1).split()
                        module_name = path.replace(f'{os.getenv("BASE_DIR")}/', '').replace('/', '.')
                        subclass_name = words_between[0] if len(words_between) == 1 else None
                        if subclass_name:
                            bridge_subclass = module_name[:-len('.py')] + '.' + subclass_name
                            from src.bridge import Bridge
                            cls = get_class(bridge_subclass)
                            if cls and issubclass(cls, Bridge):
                                # deposit_func = getattr(cls, 'deposit', None)
                                # print(f'k: {callable(deposit_func)}')
                                return {subclass_name: bridge_subclass}

        return None

    class PackagingServiceException(Exception):
        def __init__(self, bom: BridgeOutputModel, message: str):
            self.bom = bom
            self.message = message
            super().__init__(self.message)
