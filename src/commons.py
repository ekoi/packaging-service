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
