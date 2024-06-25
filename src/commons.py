import ast
import logging
import os
import platform
import re
import smtplib
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functools import wraps
from logging.handlers import TimedRotatingFileHandler
from typing import Any, Callable, List

import requests
from dynaconf import Dynaconf
from fastapi import HTTPException
from fastapi_mail import ConnectionConfig, MessageSchema
from pydantic import EmailStr, BaseModel
import os

from src.dbz import DatabaseManager, DepositStatus
from src.models.bridge_output_model import BridgeOutputDataModel, TargetResponse

conf_path = os.getenv("BASE_DIR") if os.getenv("BASE_DIR") is not None else os.getcwd()

settings = Dynaconf(root_path=f'{os.getenv("BASE_DIR", os.getcwd())}/conf', settings_files=["*.toml"],
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
        file_handler = logging.FileHandler(log.get('log_file'), mode='a')
        file_handler.setFormatter(formatter)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        rotating_handler = TimedRotatingFileHandler(log.get('log_file'), when="H", interval=8, backupCount=10)
        log_setup.addHandler(rotating_handler)
        log_setup.setLevel(log.get('log_level'))
        log_setup.addHandler(file_handler)
        log_setup.addHandler(stream_handler)
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
    try:
        m = __import__(module)
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m
    except ModuleNotFoundError as e:
        print(f'error: {kls}')
        logger(f'ModuleNotFoundError: {e}', 'error', 'ps')
    return None


def transform(transformer_url: str, str_tobe_transformed: str) -> str:
    logger(f'transformer_url: {transformer_url}', 'debug', 'ps')
    logger(f'str_tobe_transformed: {str_tobe_transformed}', 'debug', 'ps')
    if type(str_tobe_transformed) is not str:
        raise ValueError(f"Error - str_tobe_transformed is not a string. It is : {type(str_tobe_transformed)}")

    transformer_response = requests.post(transformer_url, headers=transformer_headers, data=str_tobe_transformed)
    if transformer_response.status_code == 200:
        transformed_metadata = transformer_response.json()
        str_transformed_metadata = transformed_metadata.get('result')
        logger(f'Transformer result: {str_transformed_metadata}', 'debug', 'ps')
        return str_transformed_metadata

    logger(f'transformer_response.status_code: {transformer_response.status_code}', 'error', 'ps')
    raise ValueError(f"Error - Transformer response status code: {transformer_response.status_code}")


# def transform(transformer_url: str, input: str) -> str:
#     logger(transformer_url: {transformer_url}', 'debug', 'ps')
#     logger(f'input: {input}', 'debug', 'ps')
#     try:
#         transformer_response = requests.post(transformer_url, headers=transformer_headers, data=input)
#         if transformer_response.status_code == 200:
#             transformed_metadata = transformer_response.json()
#             str_transformed_metadata = transformed_metadata.get('result')
#             logger(f'Transformer result: {str_transformed_metadata}', 'debug', 'ps')
#             return str_transformed_metadata
#         logger(transformer_response.status_code: {transformer_response.status_code}', 'error', 'ps')
#         raise ValueError(f"Error - Transformer response status code: {transformer_response.status_code}")
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


def handle_deposit_exceptions(func) -> Callable[[tuple[Any, ...], dict[str, Any]], BridgeOutputDataModel | Any]:
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger(f'Enter to handle_deposit_exceptions for {func.__name__}. args: {args}', 'debug', 'ps')
        try:
            rv = func(*args, **kwargs)
            return rv
        except Exception as ex:
            logger(f'Errors in {func.__name__}: {ex} - {ex.with_traceback(ex.__traceback__)}',
                   'debug', 'ps')
            target = args[0].target
            bom = BridgeOutputDataModel()
            bom.deposit_status = DepositStatus.ERROR
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
            logger(f'Enter to handle_ps_exceptions:: {func.__name__}', 'debug', 'ps')
            rv = func(*args, **kwargs)
            return rv
        except HTTPException as ex:
            # send_mail(f'handle_ps_exceptions: Errors in {func.__name__}', f'status code: {ex.status_code}.'
            #                                                               f'\nDetails: {ex.detail}.')
            logger(
                f'handle_ps_exceptions: Errors in {func.__name__}. status code: {ex.status_code}. Details: {ex.detail}. '
                f'args: {args}', 'debug', 'ps')
            raise ex
        except Exception as ex:
            send_mail(f'handle_ps_exceptions: Errors in {func.__name__}', f'{ex} - '
                                                                          f'{ex.with_traceback(ex.__traceback__)}.')
            logger(f'handle_ps_exceptions: Errors in {func.__name__}: {ex} - {ex.with_traceback(ex.__traceback__)}',
                   'debug', 'ps')
            raise ex
        except BaseException as ex:
            send_mail(f'handle_ps_exceptions: Errors in {func.__name__}', f'{ex} - '
                                                                          f'{ex.with_traceback(ex.__traceback__)}.')
            logger(f'handle_ps_exceptions: Errors in {func.__name__}:  {ex} - {ex.with_traceback(ex.__traceback__)}',
                   'debug', 'ps')
            raise ex

    return wrapper


def inspect_bridge_module(py_file_path: str):
    with open(py_file_path, 'r') as f:
        bridge_mdl = ast.parse(f.read())
    results = []
    if isinstance(bridge_mdl, ast.Module):
        for node in bridge_mdl.body:
            if isinstance(node, ast.ClassDef):
                class_name = node.name
                super_class = ""
                if len(node.bases):
                    for base in node.bases:
                        root = base
                        max_depth = 100
                        while not isinstance(root, ast.Name) and max_depth:
                            if "value" in root.__dir__():
                                super_class = f".{root.attr}" + super_class
                                root = root.value
                        if root.id != 'Bridge':
                            continue
                        module_name = py_file_path.replace(f'{os.getenv("BASE_DIR", os.getcwd())}/', '').replace('/', '.')
                        name_of_bridge_subclass = module_name[:-len('.py')] + '.' + class_name
                        results.append({class_name: name_of_bridge_subclass})

    return results


# class PackagingServiceException(Exception):
#     def __init__(self, bom: BridgeOutputDataModel, message: str):
#         self.bom = bom
#         self.message = message
#         super().__init__(self.message)

def send_mail(subject: str, text: str):
    sender_email = settings.MAIL_USR
    app_password = settings.MAIL_PASS  #
    # recipient_emails = ["eko.indarto@dans.knaw.nl", "eko.indarto.huc@di.huc.knaw.nl", "umar.fth@gmail.com"]
    recipient_email = settings.MAIL_TO
    # Create the email message
    subject = subject
    body = text
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = recipient_email
    message['Subject'] = f'{settings.DEPLOYMENT}: {subject}'
    message.attach(MIMEText(body, 'plain'))

    if settings.get('send_mail', True):
        # Establish a connection to the SMTP server
        try:
            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login(sender_email, app_password)
                text = message.as_string()
                server.sendmail(sender_email,  recipient_email, text)
            print("Email sent successfully!")
            logger(f"Email sent successfully to {recipient_email}", "debug", "ps")
        except Exception as e:
            print(f"Error: {e}")
            logger(f"Unsuccessful sent email to {recipient_email}", "error", "ps")

    else:
        logger(f"{settings.get('send_mail', False)} - Sending email is disabled.", 'debug', 'ps')


def dmz_dataverse_headers(username, password) -> {}:
    headers = {}
    if settings.exists("dmz_x_authorization_value", fresh=False):
        headers.update({'X-Authorization': settings.dmz_x_authorization_value})

    if username == 'API_KEY':
        headers.update({"X-Dataverse-key": password})

    return headers
