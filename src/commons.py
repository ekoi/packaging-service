import ast
import logging
import os
import platform
import smtplib
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functools import wraps
from logging.handlers import TimedRotatingFileHandler
from typing import Any, Callable

import requests
from dynaconf import Dynaconf
from fastapi import HTTPException

from src.dbz import DatabaseManager, DepositStatus
from src.models.bridge_output_model import BridgeOutputDataModel, TargetResponse

LOG_NAME_PS = 'ps'
LOG_LEVEL_DEBUG = 'debug'

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
    """
    This function sets up the logger for the application.

    It iterates over the list of loggers specified in the settings, and for each logger, it:
    - Gets or creates a logger with the specified name.
    - Creates a formatter with the specified format.
    - Creates a file handler that writes to the specified log file in append mode, and sets its formatter.
    - Creates a stream handler (which writes to stdout by default) and sets its formatter.
    - Creates a timed rotating file handler that rotates the log file every 8 hours and keeps the last 10 log files, and adds it to the logger.
    - Sets the log level of the logger.
    - Adds the file handler and the stream handler to the logger.
    - Logs a startup message at the debug level, which includes the current time and the Python version.

    The logger settings (name, format, log file, and log level) are read from the `LOGGERS` setting in the application's configuration.

    The startup message is logged using the `logger` function defined elsewhere in this module.
    """
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
    """
    This function dynamically imports a class from a module.

    It takes a string `kls` as input, which should be the fully qualified name of a class (i.e., including its module path).
    The string is split into parts, and the module path is reconstructed by joining all parts except the last one.
    The module is then imported using the `__import__` function, and the class is retrieved using `getattr`.

    If the module cannot be found, a `ModuleNotFoundError` is caught and logged, and the function returns `None`.

    Parameters:
    kls (str): The fully qualified name of a class to import.

    Returns:
    Any: The class if it can be imported, or `None` otherwise.
    """
    parts = kls.split('.')
    module = ".".join(parts[:-1])
    try:
        m = __import__(module)
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m
    except ModuleNotFoundError as e:
        print(f'error: {kls}')
        logger(f'ModuleNotFoundError: {e}', 'error', LOG_NAME_PS)
    return None


def transform(transformer_url: str, str_tobe_transformed: str) -> str:
    logger(f'transformer_url: {transformer_url}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    logger(f'str_tobe_transformed: {str_tobe_transformed}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
    if type(str_tobe_transformed) is not str:
        raise ValueError(f"Error - str_tobe_transformed is not a string. It is : {type(str_tobe_transformed)}")

    transformer_response = requests.post(transformer_url, headers=transformer_headers, data=str_tobe_transformed)
    if transformer_response.status_code == 200:
        transformed_metadata = transformer_response.json()
        str_transformed_metadata = transformed_metadata.get('result')
        logger(f'Transformer result: {str_transformed_metadata}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        return str_transformed_metadata

    logger(f'transformer_response.status_code: {transformer_response.status_code}', 'error', LOG_NAME_PS)
    raise ValueError(f"Error - Transformer response status code: {transformer_response.status_code}")


# def transform(transformer_url: str, input: str) -> str:
#     logger(transformer_url: {transformer_url}', LOGGER_LEVEL_DEBUG, LOG_NAME_PS)
#     logger(f'input: {input}', LOGGER_LEVEL_DEBUG, LOG_NAME_PS)
#     try:
#         transformer_response = requests.post(transformer_url, headers=transformer_headers, data=input)
#         if transformer_response.status_code == 200:
#             transformed_metadata = transformer_response.json()
#             str_transformed_metadata = transformed_metadata.get('result')
#             logger(f'Transformer result: {str_transformed_metadata}', LOGGER_LEVEL_DEBUG, LOG_NAME_PS)
#             return str_transformed_metadata
#         logger(transformer_response.status_code: {transformer_response.status_code}', 'error', LOG_NAME_PS)
#         raise ValueError(f"Error - Transformer response status code: {transformer_response.status_code}")
#     except ConnectionError as ce:
#         logger(f'Errors during transformer: {ce.with_traceback(ce.__traceback__)}', LOGGER_LEVEL_DEBUG, LOG_NAME_PS)
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
#                        LOGGER_LEVEL_DEBUG, LOG_NAME_PS)
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


def handle_deposit_exceptions(
        func) -> Callable[[tuple[Any, ...], dict[str, Any]], BridgeOutputDataModel | Any]:
    """
    This function is a decorator that wraps around a function to handle exceptions during the deposit process.

    It logs the entry into the function it is decorating, then attempts to execute the function.
    If an exception is raised during the execution of the function, it logs the error and creates a BridgeOutputDataModel
    instance with an error status and a TargetResponse instance containing the error details.

    The decorated function should take a BridgeOutputDataModel instance as its first argument.

    Parameters:
    func (Callable): The function to be decorated.

    Returns:
    Callable: The decorated function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger(f'Enter to handle_deposit_exceptions for {func.__name__}. args: {args}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
        try:
            rv = func(*args, **kwargs)
            return rv
        except Exception as ex:
            logger(f'Errors in {func.__name__}: {ex} - {ex.with_traceback(ex.__traceback__)}',
                   LOG_LEVEL_DEBUG, LOG_NAME_PS)
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
    """
    This function is a decorator that wraps around a function to handle exceptions during the execution of the function.

    It logs the entry into the function it is decorating, then attempts to execute the function.
    If an HTTPException is raised during the execution of the function, it logs the error and re-raises the exception.
    If any other exception is raised, it sends an email with the error details, logs the error, and re-raises the exception.

    The decorated function can take any number of positional and keyword arguments.

    Parameters:
    func (Callable): The function to be decorated.

    Returns:
    Callable: The decorated function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            logger(f'Enter to handle_ps_exceptions:: {func.__name__}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
            rv = func(*args, **kwargs)
            return rv
        except HTTPException as ex:
            # send_mail(f'handle_ps_exceptions: Errors in {func.__name__}', f'status code: {ex.status_code}.'
            #                                                               f'\nDetails: {ex.detail}.')
            logger(
                f'handle_ps_exceptions: Errors in {func.__name__}. status code: {ex.status_code}. Details: {ex.detail}. '
                f'args: {args}', LOG_LEVEL_DEBUG, LOG_NAME_PS)
            raise ex
        except Exception as ex:
            send_mail(f'handle_ps_exceptions: Errors in {func.__name__}', f'{ex} - '
                                                                          f'{ex.with_traceback(ex.__traceback__)}.')
            logger(f'handle_ps_exceptions: Errors in {func.__name__}: {ex} - {ex.with_traceback(ex.__traceback__)}',
                   LOG_LEVEL_DEBUG, LOG_NAME_PS)
            raise ex
        except BaseException as ex:
            send_mail(f'handle_ps_exceptions: Errors in {func.__name__}', f'{ex} - '
                                                                          f'{ex.with_traceback(ex.__traceback__)}.')
            logger(f'handle_ps_exceptions: Errors in {func.__name__}:  {ex} - {ex.with_traceback(ex.__traceback__)}',
                   LOG_LEVEL_DEBUG, LOG_NAME_PS)
            raise ex

    return wrapper


def inspect_bridge_module(py_file_path: str):
    """
    This function inspects a Python module and returns a list of classes that inherit from the 'Bridge' class.

    It opens the Python file at the given path and parses it into an AST (Abstract Syntax Tree) using the `ast.parse` function.
    It then iterates over the nodes in the AST, and for each class definition, it checks if it inherits from the 'Bridge' class.
    If it does, it constructs the fully qualified name of the class and adds it to the results list.

    The fully qualified name of a class is constructed by replacing the base directory path in the file path with an empty string,
    replacing all slashes with dots, and appending the class name.

    Parameters:
    py_file_path (str): The path to the Python file to inspect.

    Returns:
    list[dict[str, str]]: A list of dictionaries, where each dictionary has one key-value pair.
                           The key is the name of a class that inherits from the 'Bridge' class,
                           and the value is the fully qualified name of the class.
    """
    with open(py_file_path, 'r') as f:
        bridge_mdl = ast.parse(f.read())
    results = []
    for node in bridge_mdl.body:
        if isinstance(node, ast.ClassDef) and any(
                isinstance(base, ast.Name) and base.id == 'Bridge' for base in node.bases):
            module_name = py_file_path.replace(f'{os.getenv("BASE_DIR", os.getcwd())}/', '').replace('/', '.')
            name_of_bridge_subclass = f"{module_name[:-3]}.{node.name}"
            results.append({node.name: name_of_bridge_subclass})
    return results


# class PackagingServiceException(Exception):
#     def __init__(self, bom: BridgeOutputDataModel, message: str):
#         self.bom = bom
#         self.message = message
#         super().__init__(self.message)

def send_mail(subject: str, text: str):
    sender_email = settings.MAIL_USR
    app_password = settings.MAIL_PASS
    recipient_email = settings.MAIL_TO
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = recipient_email
    message['Subject'] = f'{settings.DEPLOYMENT}: {subject}'
    message.attach(MIMEText(text, 'plain'))

    if settings.get('send_mail', True):
        try:
            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login(sender_email, app_password)
                server.sendmail(sender_email, recipient_email, message.as_string())
            print("Email sent successfully!")
            logger(f"Email sent successfully to {recipient_email}", "debug", "ps")
        except Exception as e:
            print(f"Error: {e}")
            logger(f"Unsuccessful sent email to {recipient_email}", "error", "ps")
    else:
        logger("Sending email is disabled.", LOG_LEVEL_DEBUG, "ps")


def dmz_dataverse_headers(username, password) -> dict:
    headers = {'X-Authorization': settings.dmz_x_authorization_value} if settings.exists("dmz_x_authorization_value",
                                                                                         fresh=False) else {}
    if username == 'API_KEY':
        headers["X-Dataverse-key"] = password
    return headers
