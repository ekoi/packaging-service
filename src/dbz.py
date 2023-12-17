import json
import sqlite3
from contextlib import closing
from enum import StrEnum, auto

from jinja2 import Environment, BaseLoader
from sqlalchemy import text
from sqlalchemy.exc import NoResultFound
from sqlmodel import SQLModel, Field, create_engine, Session, select
from datetime import datetime

from typing import List, Optional
from pydantic import BaseModel

'''
import logging
logging.basicConfig()
logger = logging.getLogger('sqlalchemy.engine')
logger.setLevel(logging.DEBUG)
# run sqlmodel code after this
'''


class ReleaseVersion(StrEnum):
    DRAFT = 'DRAFT'
    PUBLISH = 'PUBLISH'
    PUBLISHED = 'PUBLISHED'
    PUBLISHING = 'PUBLISHING'


class DepositStatus(StrEnum):
    INITIAL = auto()
    PROGRESS = auto()
    FINISH = auto()
    REJECTED = auto()
    FAILED = auto()
    ERROR = auto()
    SUCCESS = auto()
    ACCEPTED = auto()
    FINALIZING = auto()
    SUBMITTED = auto()
    PUBLISHED = auto()
    UNDEFINED = auto()


class DataFileWorkState(StrEnum):
    GENERATED = auto()
    UPLOADED = auto()
    REGISTERED = auto()


class DatasetWorkState(StrEnum):
    NOT_READY = 'not-ready'
    READY = auto()
    RELEASED = auto()


class FilePermissions(StrEnum):
    PUBLIC = auto()
    PRIVATE = auto()


# Define the Metadata model
class Dataset(SQLModel, table=True):
    id: str = Field(primary_key=True, index=True)
    title: Optional[str] = Field(nullable=True)
    owner_id: str = Field(index=True)
    created_date: datetime = datetime.utcnow()
    saved_date: datetime = datetime.utcnow()
    submitted_date: Optional[datetime]
    app_name: str = Field(index=True)
    md: str  # https://www.sqlite.org/fasterthanfs.html
    release_version: ReleaseVersion = ReleaseVersion.DRAFT
    version: Optional[str]
    state: DatasetWorkState = DatasetWorkState.NOT_READY


# Define the TargetRepo model
class TargetRepo(SQLModel, table=True):
    __tablename__ = "target_repo"
    id: int = Field(default=None, primary_key=True)
    ds_id: str = Field(foreign_key="dataset.id")
    name: str = Field(index=True)
    display_name: str = Field(index=True)
    config: str
    url: str
    deposit_status: Optional[DepositStatus]
    deposit_time: Optional[datetime]
    duration: float = 0.0
    output: Optional[str]
    # Optional since some repo uses the same uername/password
    # e.g. dataverse username is always API_KEY, SWH API uses the same username/password for every user.
    # username: Optional[str]
    # password: Optional[str]


# Define the Files model
class DataFile(SQLModel, table=True):
    __tablename__ = "data_file"
    id: int = Field(primary_key=True)
    ds_id: str = Field(foreign_key="dataset.id")
    name: str = Field(index=True)
    path: Optional[str]
    size: Optional[int]
    mime_type: Optional[str]
    checksum_value: Optional[str]
    date_added: Optional[datetime]
    permissions: FilePermissions = FilePermissions.PRIVATE
    state: DataFileWorkState = DataFileWorkState.REGISTERED


class DatabaseManager:

    def __init__(self, db_dialect: str, db_url: str):
        self.conn_url = f'{db_dialect}:{db_url}'
        self.engine = create_engine(self.conn_url, pool_size=10)
        # TODO: VERY IMPORTANT!!! REFACTOR - Using sqlmodel.
        # TODO: Remove db_file = self.conn_url.split("///")[1]
        # TODO use self.engine
        self.db_file = self.conn_url.split("///")[1]  # sqlite:////
        # self.engine = create_engine("sqlite:////Users/akmi/git/ekoi/poc-4-wim/packaging-service/data/db/abc.db")

    def create_db_and_tables(self):
        SQLModel.metadata.create_all(self.engine)

    def insert_dataset_and_target_repo(self, ds_record: Dataset, repo_records: [TargetRepo]) -> type(None):
        with Session(self.engine) as session:
            session.add(ds_record)
            session.commit()
            for tr in repo_records:
                tr.ds_id = ds_record.id
                session.add(tr)
            session.commit()

    def insert_datafiles(self, file_records: [DataFile]) -> type(None):
        with Session(self.engine) as session:
            for file_record in file_records:
                session.add(file_record)
                session.commit()
                session.refresh(file_record)

    def delete_datafile(self, ds_id: str, filename: str) -> type(None):
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == ds_id, DataFile.name == filename)
            results = session.exec(statement)
            file_record = results.one_or_none()
            if file_record:
                session.delete(file_record)
                session.commit()

    def is_dataset_exist(self, dataset_id: str) -> bool:
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.id == dataset_id)
            results = session.exec(statement)
            result = results.one_or_none()
        return result is not None

    def is_dataset_published(self, dataset_id: str) -> bool:
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.id == dataset_id,
                                              Dataset.release_version == ReleaseVersion.PUBLISH)
            results = session.exec(statement)
            result = results.one_or_none()
        return result is not None

    def find_dataset(self, ds_id: str) -> Dataset:
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.id == ds_id)
            results = session.exec(statement)
            result = results.one_or_none()
        return result

    def find_target_repos_by_ds_id(self, ds_id: str) -> [TargetRepo]:
        with Session(self.engine) as session:
            statement = select(TargetRepo).where(TargetRepo.ds_id == ds_id)
            results = session.exec(statement)
            result = results.all()
        # or the compact version: session.exec(select(TargetRepo)).all()
        return result

    def find_files(self, ds_id: str) -> [DataFile]:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == ds_id)
            results = session.exec(statement)
            result = results.all()
        return result

    def find_non_generated_files(self, ds_id: str) -> [DataFile]:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == ds_id, DataFile.state != DataFileWorkState.GENERATED)
            results = session.exec(statement)
            result = results.all()
        return result

    def execute_raw_sql(self) -> [DataFile]:
        with Session(self.engine) as session:
            statement = text("select json_object('metadata-id', ds.id, 'title', ds.title) from dataset ds")
            results = session.execute(statement)
            result = results.all()
        return result

    def find_file_by_dataset_id_and_name(self, ds_id: str, file_name: str) -> DataFile:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == ds_id,
                                               DataFile.name == file_name)
            results = session.exec(statement)
            result = results.one()
        return result

    def find_progress_state_by_owner_id(self, owner_id: str):
        with closing(sqlite3.connect(self.db_file)) as connection:
            with connection:
                cursor = connection.cursor()
                sql_query = '''
                            SELECT json_object('metadata-id', ds.id, 'title', ds.title, 
                            'created-date', ds.created_date, 'saved-date', ds.saved_date,
                            'submitted-date', ds.submitted_date,
                            'release-version', ds.release_version,'targets', 
                            json_group_array(json_object('target-repo-name', tr.name, 
                                'target-repo-display-name', tr.display_name, 'target-url', tr.url, 
                                'ingest-status', tr.deposit_status, 'target-output', json(tr.output) ))) 
                                FROM target_repo tr, dataset ds WHERE tr.ds_id = ds.id and ds.owner_id=?  
                                GROUP BY ds.id
                        '''
                cursor.execute(sql_query, (owner_id,))
                rows = cursor.fetchall()
                return rows

    # TODO: REFACTOR - Using sqlmodel
    def find_dataset_by_id(self, id):
        with closing(sqlite3.connect(self.db_file)) as connection:
            with connection:
                cursor = connection.cursor()
                cursor.execute('SELECT json(md) FROM dataset WHERE id = ?', (id,))
                results = cursor.fetchall()
            if len(results) != 1:
                return '{}'
            return (results[0])[0]

    # TODO: REFACTOR - Using sqlmodel
    def find_file_upload_status_by_dataset_id_and_filename(self, ds_id, filename):
        with closing(sqlite3.connect(self.conn_url)) as connection:
            with connection:
                cursor = connection.cursor()
                cursor.execute('SELECT json(date_added) FROM data_file WHERE ds_id = ? and name=?',
                               (ds_id, filename,))
                results = cursor.fetchall()
            if len(results) != 1:
                return None

            return (results[0])[0]

    def update_metadata(self, dataset: Dataset) -> type(None):
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.id == dataset.id)
            results = session.exec(statement)
            ds_record = results.one_or_none()
            if ds_record:
                ds_record.md = dataset.md
                ds_record.title = dataset.title
                ds_record.release_version = dataset.release_version
                ds_record.saved_date = datetime.utcnow()
                ds_record.state = dataset.state
                session.add(ds_record)
                session.commit()
                session.refresh(ds_record)

    def set_ready_for_ingest(self, id: str) -> type(None):
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.id == id)
            results = session.exec(statement)
            md_record = results.one_or_none()
            if md_record:
                # md_record.release_version = ReleaseVersion.PUBLISH
                md_record.state = DatasetWorkState.READY
                session.add(md_record)
                session.commit()
                session.refresh(md_record)

    def update_target_repo_deposit_status(self, target_repo: TargetRepo) -> type(None):
        with Session(self.engine) as session:
            statement = select(TargetRepo).where(TargetRepo.ds_id == target_repo.ds_id,
                                                 TargetRepo.name == target_repo.name)
            results = session.exec(statement)
            target_repo_record = results.one_or_none()
            if target_repo:
                target_repo_record.deposit_status = target_repo.deposit_status
                target_repo_record.output = target_repo.output
                target_repo_record.deposit_time = datetime.utcnow()
                session.add(target_repo_record)
                session.commit()
                session.refresh(target_repo_record)

    def update_target_output_by_id(self, target_repo=TargetRepo) -> type(None):
        with Session(self.engine) as session:
            statement = select(TargetRepo).where(TargetRepo.id == target_repo)
            results = session.exec(statement)
            target_repo_record = results.one_or_none()
            if target_repo_record:
                target_repo_record.output = target_repo.output
                session.add(target_repo_record)
                session.commit()
                session.refresh(target_repo_record)

    def submitted_now(self, id: str) -> type(None):
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.id == id)
            results = session.exec(statement)
            md_record = results.one_or_none()
            if md_record:
                md_record.submitted_date = datetime.utcnow()
                md_record.saved_date = datetime.utcnow()
                session.add(md_record)
                session.commit()
                session.refresh(md_record)

    def update_file(self, df: DataFile) -> type(None):
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == df.ds_id, DataFile.name == df.name)
            results = session.exec(statement)
            f_record = results.one_or_none()
            if f_record:
                f_record.date_added = datetime.utcnow()
                f_record.path = df.path
                f_record.mime_type = df.mime_type
                f_record.size = df.size
                f_record.checksum_value = df.checksum_value
                f_record.state = df.state
                session.add(f_record)
                session.commit()
                session.refresh(f_record)

    def replace_targets_record(self, ds_id: str, target_repo_records: [TargetRepo]) -> type(None):
        with Session(self.engine) as session:
            statement = select(TargetRepo).where(TargetRepo.ds_id == ds_id)
            results = session.exec(statement)
            trs = results.fetchall()
            for tr in trs:
                session.delete(tr)
            session.commit()
            for tr in target_repo_records:
                tr.ds_id = ds_id
                session.add(tr)
            session.commit()

    def is_dataset_ready(self, dataset_id: str) -> bool:
        with Session(self.engine) as session:
            dataset_id_rec = session.exec(
                select(Dataset.id).where((Dataset.id == dataset_id) & (Dataset.state == DatasetWorkState.READY) &
                                         (Dataset.release_version == ReleaseVersion.PUBLISH))).one_or_none()
            return dataset_id_rec is not None

    def are_files_uploaded(self, ds_id: str) -> bool:
        with Session(self.engine) as session:
            results = session.exec(select(DataFile).where(DataFile.ds_id == ds_id,
                                                          DataFile.state == DataFileWorkState.REGISTERED)).all()

        return len(results) == 0


# import decimal, datetime
#
#
# def alchemyencoder(obj):
#     """JSON encoder function for SQLAlchemy special classes."""
#     if isinstance(obj, datetime.date):
#         return obj.isoformat()
#     elif isinstance(obj, decimal.Decimal):
#         return float(obj)


class ProgressTarget(BaseModel):
    target_repo_name: str = Field(..., alias='target-repo-name')
    target_repo_display_name: str = Field(..., alias='target-repo-display-name')
    target_url: str = Field(..., alias='target-url')
    ingest_status: str = Field(..., alias='ingest-status')
    target_output: Optional[str] = Field(..., alias='target-output')


class ProgressModel(BaseModel):
    metadata_id: str = Field(..., alias='metadata-id')
    title: str
    created_date: str = Field(..., alias='created-date')
    submitted_date: str = Field(..., alias='submitted-date')
    saved_date: str = Field(..., alias='saved-date')
    release_version: str = Field(..., alias='release-version')
    targets: List[ProgressTarget]


template_deposit_response_json = Environment(loader=BaseLoader()).from_string('''
 {
        "metadata-id": "{{mid}}",
        "title": "Amalin Title",
        "created-date": "2023-11-26 16:15:48.238064",
        "submitted-date": "2023-11-27 12:14:23.755198",
        "saved-date": "2023-11-27 12:14:23.755198",
        "release-version": "PUBLISH",
        "targets": [
            {
                "target-repo-name": "demo.ssh.datastations.nl",
                "target-repo-display-name": "SSH Datastation",
                "target-url": "https://demo.sword2.ssh.datastations.nl/collection/1",
                "ingest-status": "processing",
                "target-output": None
            }
        ]
    }
''')

json_data = [
    {
        "metadata-id": "cc66cd92-7e46-455c-b18b-7e84ef6ab797",
        "eko": "indarto",
        "title": "Amalin Title",
        "created-date": "2023-11-26 16:15:48.238064",
        "submitted-date": "2023-11-27 12:14:23.755198",
        "saved-date": "2023-11-27 12:14:23.755198",
        "release-version": "PUBLISH",
        "targets": [
            {
                "target-repo-name": "demo.ssh.datastations.nl",
                "target-repo-display-name": "SSH Datastation",
                "target-url": "https://demo.sword2.ssh.datastations.nl/collection/1",
                "ingest-status": "processing",
                "target-output": None
            }
        ]
    }
]


# Convert JSON to Pydantic model


def main():
    #
    db_manager = DatabaseManager('sqlite',
                                 '////Users/akmi/git/ekoi/poc-4-wim/packaging-service/data/db/dans_packaging.db')
    try:
        s = db_manager.are_files_uploaded('jad2db079-ad2b-44c3-afb8-989bf9c551bc')
        print(s)
    except NoResultFound as e:
        print(e)

#     import inspect
#     print(inspect.getmembers(db_manager))
#     x = db_manager.execute_raw_sql()
# print(x)
# #     db_manager.create_db_and_tables()
# db_manager.insert_metadata_and_target_repo_record(md_record=Metadata(md_id="x1", md="abac", owner_id="eko"
#                                                                  , app_name="ohsmart"),
#                                               target_repo_records=[TargetRepo(target_repo_name="trm",
#                                                                               target_repo_display_name="disp",
#                                                                               target_url="the url",
#                                                                               target_repo_config="mmmm")])
# db_manager.insert_file_records([Files(md_id="x1", file_name="xyz")])
# db_manager.delete_file_by_md_id_and_filename("x1", "abc")
# db_manager.update_metadata(md_id='x1', release_version="hello", title="EKO TITLE", md="HELLLLLOOOOOOO")
# db_manager.replace_targets_record(md_id="x1", target_repo_records=[TargetRepo(target_repo_name="tlskfdljrm",
#                                                                             target_repo_display_name="di---sp",
#                                                                             target_url="t---he url",
#                                                                             target_repo_config="m----mmm")])

# s = db_manager.are_files_uploaded("x1")
# s = db_manager.find_files_by_metadata_id("x1")
# x=[i.file_name for i in db_manager.find_files_by_metadata_id("x1")]
# zz = db_manager.is_ready_for_ingest_by_metadata_id(md_id="28f2143d-2d43-4146-8164-bd09d1d2b842")

# print(type(zz))
# print(zz)
#
# for z in zz:
#     # u = [dict(r) for r in z]
#     v = json.dumps([dict(r) for r in z], default=alchemyencoder)
#     print(v)
#     # print(u)
# print(type(u))
# pydantic_models = [ProgressModel(**item) for item in json_data]
#
# print(pydantic_models)
# print("-----")
# for model in pydantic_models:
#     # print(model.metadata_id, model.title, model.created_date, model.release_version)
#     print(model.json())
#     for target in model.targets:
#         print(target.target_repo_name, target.target_repo_display_name, target.target_url, target.ingest_status,
#               target.target_output)

#
# klm = []

#
# #
# if __name__ == "__main__":
#     main()
