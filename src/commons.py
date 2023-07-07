import logging

from dynaconf import Dynaconf

settings = Dynaconf(settings_files=["conf/settings.toml", "conf/.secrets.toml"],
                    environments=True)

logging.basicConfig(filename=settings.LOG_FILE, level=settings.LOG_LEVEL,
                    format=settings.LOG_FORMAT)

metadata_status_initial = "initial"
metadata_status_all_files_uploaded = "all-files-uploaded"
reserved_filename = "form-metadata.json"
data = {}

def dmz_headers(username, password):
    dmz_headers = {}
    if settings.exists("dmz_x_authorization_value", fresh=False):
        dmz_headers.update({'X-Authorization': settings.dmz_x_authorization_value})

    if username == 'API_KEY':
        dmz_headers.update({"X-Dataverse-key": password})

    return dmz_headers

