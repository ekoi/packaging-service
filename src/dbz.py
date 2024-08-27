import base64
import json
import sqlite3
from contextlib import closing
from datetime import datetime
from enum import StrEnum, auto
from typing import List, Optional, Sequence, Any

from cryptography.fernet import Fernet

from pydantic import BaseModel
from sqlalchemy import text, delete, inspect, UniqueConstraint, desc, asc
from sqlalchemy.exc import IntegrityError
from sqlmodel import SQLModel, Field, create_engine, Session, select

from src.models.app_model import OwnerAssetsModel, Asset, TargetApp

'''
import logging
logging.basicConfig()
logger = logging.getLogger('sqlalchemy.engine')
logger.setLevel(logging.DEBUG)
# run sqlmodel code after this
'''

LOG_NAME_PS = 'ps'
LOG_LEVEL_DEBUG = 'debug'

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
    DEPOSITED = auto()


class DataFileWorkState(StrEnum):
    GENERATED = "GENERATED"
    UPLOADED = "UPLOADED"
    REGISTERED = "REGISTERED"


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

    def encrypt_md(self, cipher_suite):
        self.md = cipher_suite.encrypt(self.md.encode()).decode()

    def decrypt_md(self, cipher_suite):
        self.md = cipher_suite.decrypt(self.md.encode()).decode()


# Define the TargetRepo model
class TargetRepo(SQLModel, table=True):
    __tablename__ = "target_repo"
    __table_args__ = (
        UniqueConstraint("ds_id", "name", name="unique_dataset_id_target_repo_name"),
    )
    id: int = Field(default=None, primary_key=True)
    ds_id: str = Field(foreign_key="dataset.id")
    name: str = Field(index=True)
    display_name: str = Field(index=True)
    config: str
    url: str
    deposit_status: Optional[DepositStatus]
    deposit_time: Optional[datetime]
    duration: float = 0.0
    target_output: Optional[str]

    def encrypt_config(self, cipher_suite):
        self.config = cipher_suite.encrypt(self.config.encode()).decode()

    def decrypt_config(self, cipher_suite):
        self.config = cipher_suite.decrypt(self.config.encode()).decode()
    # Optional since some repo uses the same uername/password
    # e.g. dataverse username is always API_KEY, SWH API uses the same username/password for every user.
    # username: Optional[str]
    # password: Optional[str]


# Define the Files model
class DataFile(SQLModel, table=True):
    __tablename__ = "data_file"
    __table_args__ = (
        UniqueConstraint("ds_id", "name", name="unique_ds_id_name"),
    )
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
    cipher_suite = None
    def __init__(self, db_dialect: str, db_url: str, encryption_key: str):
        self.conn_url = f'{db_dialect}:{db_url}'
        self.engine = create_engine(self.conn_url, pool_size=10)
        # TODO: Remove db_file = self.conn_url.split("///")[1]
        # TODO use self.engine
        self.db_file = self.conn_url.split("///")[1]  # sqlite:////
        self.cipher_suite = Fernet(base64.urlsafe_b64encode(encryption_key.encode()))
        # self.engine = create_engine("sqlite:////Users/akmi/git/ekoi/poc-4-wim/packaging-service/data/db/abc.db")
        # self.session_local = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    # def get_db(self):
    #     database = self.session_local()
    #     try:
    #         yield database
    #     finally:
    #         database.close()

    # def encrypt_data(self, data):
    #     return self.cipher_suite.encrypt(data.encode()).decode()
    #
    # # Function to decrypt data
    # def decrypt_data(self, data):
    #     return  self.cipher_suite.decrypt(data.encode()).decode()

    def create_db_and_tables(self):
        # checkfirst=True means if not exist create one, otherwise skip it.
        # But it doesn't work in multiple uvicorn workers
        if not inspect(self.engine).has_table("Dataset"):
            SQLModel.metadata.create_all(self.engine, checkfirst=True)
        else:
            from src.commons import logger
            logger('TABLES ALREADY CREATED', LOG_LEVEL_DEBUG, LOG_NAME_PS)

    def insert_dataset_and_target_repo(self, ds_record: Dataset, repo_records: List[TargetRepo]) -> None:
        # Encrypt the md field of the Dataset
        ds_record.encrypt_md(self.cipher_suite)

        # Encrypt the config field of each TargetRepo
        for tr in repo_records:
            tr.encrypt_config(self.cipher_suite)

        with Session(self.engine) as session:
            session.add(ds_record)
            for tr in repo_records:
                tr.ds_id = ds_record.id
                session.add(tr)
            session.commit()

    def insert_datafiles(self, file_records: [DataFile]) -> None:
        try:
            with Session(self.engine) as session:
                for file_record in file_records:
                    session.add(file_record)
                    session.commit()
                    session.refresh(file_record)
        except IntegrityError as e:
            # Handle the unique constraint violation
            print(f"IntegrityError: {e.orig}")
            # Optionally, you can re-raise the exception or handle it as needed
            raise ValueError(f"------- IntegrityError: {e.orig}")


    def delete_datafile(self, dataset_id: str, filename: str) -> None:
        with Session(self.engine) as session:
            file_record = session.exec(select(DataFile).where(DataFile.ds_id == dataset_id, DataFile.name == filename)).one_or_none()
            if file_record:
                session.delete(file_record)
                session.commit()

    def delete_all(self) -> dict:
        with Session(self.engine) as session:
            tabs = {cls.__qualname__: session.exec(delete(cls)).rowcount for cls in [DataFile, TargetRepo, Dataset]}
            session.commit()
        return tabs

    def delete_by_dataset_id(self, dataset_id) -> type(None):
        with Session(self.engine) as session:
            # Delete DataFiles and TargetRepos in a single transaction
            for model in [DataFile, TargetRepo]:
                session.exec(delete(model).where(model.ds_id == dataset_id))
            session.commit()

            # Delete Dataset
            session.delete(session.exec(select(Dataset).where(Dataset.id == dataset_id)).one_or_none())
            session.commit()

    def is_dataset_exist(self, dataset_id: str) -> bool:
        return Session(self.engine).exec(select(Dataset).where(Dataset.id == dataset_id)).first() is not None

    def is_dataset_published(self, dataset_id: str) -> bool:
        return Session(self.engine).exec(select(Dataset).where(Dataset.id == dataset_id,
                                                               Dataset.release_version == ReleaseVersion.PUBLISHED)).first() is not None

    def find_dataset(self, ds_id: str) -> Dataset:
        with Session(self.engine) as session:
            dataset = session.exec(select(Dataset).where(Dataset.id == ds_id)).one_or_none()
            if dataset:
                dataset.decrypt_md(self.cipher_suite)
            return dataset

    def find_target_repo(self, dataset_id: str, target_name: str) -> TargetRepo:
        with Session(self.engine) as session:
            target_repo = session.exec(
                select(TargetRepo).where(TargetRepo.ds_id == dataset_id, TargetRepo.name == target_name)).one_or_none()
            if target_repo:
                target_repo.decrypt_config(self.cipher_suite)
            return target_repo

    def find_unfinished_target_repo(self, dataset_id: str) -> Sequence[TargetRepo]:
        with Session(self.engine) as session:
            return session.exec(select(TargetRepo).where(TargetRepo.ds_id == dataset_id,
                                                         TargetRepo.deposit_status != DepositStatus.FINISH).
                                order_by(TargetRepo.id)).all()

    def find_all_datasets(self) -> Sequence[Dataset]:
        with Session(self.engine) as session:
            results = session.exec(select(Dataset))
            result = results.all()
        return result

    def find_dataset_and_targets(self, dataset_id: str) -> Asset:
        with Session(self.engine) as session:
            dataset = session.exec(select(Dataset).where(Dataset.id == dataset_id)).one_or_none()
            if dataset:
                dataset.decrypt_md(self.cipher_suite)
                asset = Asset()
                asset.dataset_id = dataset.id
                asset.release_version = dataset.release_version
                asset.title = dataset.title
                asset.md = dataset.md
                asset.created_date = dataset.created_date
                asset.saved_date = dataset.saved_date
                asset.submitted_date = dataset.submitted_date
                asset.release_version = dataset.release_version
                asset.version = dataset.version
                # Fetch TargetRepo objects associated with the Dataset and order them
                targets_repo = session.exec(
                    select(TargetRepo).where(TargetRepo.ds_id == dataset.id).order_by(TargetRepo.id)).all()
                for target_repo in targets_repo:
                    target_repo.decrypt_config(self.cipher_suite)
                    target = TargetApp()
                    target.repo_name = target_repo.name
                    target.display_name = target_repo.display_name
                    target.deposit_status = target_repo.deposit_status
                    target.deposit_time = target_repo.deposit_time
                    target.duration = target_repo.duration
                    if target_repo.target_output is not None and target_repo.target_output != '':
                        target.output_response = json.loads(target_repo.target_output)
                    asset.targets.append(target)
                return asset
            return Asset()

    def find_dataset_ids_by_owner(self, owner_id: str) -> [TargetRepo]:
        with Session(self.engine) as session:
            statement = select(Dataset.id).where(Dataset.owner_id == owner_id)
            results = session.exec(statement)
            result = results.all()
        # or the compact version: session.exec(select(TargetRepo)).all()
        return result

    def find_target_repos_by_dataset_id(self, dataset_id: str) -> [TargetRepo]:
        with Session(self.engine) as session:
            statement = select(TargetRepo).where(TargetRepo.ds_id == dataset_id).order_by(TargetRepo.id)
            results = session.exec(statement)
            target_repos = results.all()
            for target_repo in target_repos:
                target_repo.decrypt_config(self.cipher_suite)
            return target_repos

    def find_files(self, dataset_id: str) -> [DataFile]:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == dataset_id)
            results = session.exec(statement)
            result = results.all()
        return result

    def find_uploaded_files(self, dataset_id: str) -> [DataFile]:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == dataset_id, DataFile.state == DataFileWorkState.UPLOADED)
            results = session.exec(statement)
            result = results.all()
        return result

    def find_file_by_name(self, dataset_id: str, file_name: str) -> [DataFile]:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == dataset_id, DataFile.name == file_name)
            results = session.exec(statement)
            result = results.one_or_none()
            print(dataset_id)
            print(file_name)
        return result

    def find_registered_files(self, dataset_id: str) -> [DataFile]:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == dataset_id, DataFile.state == DataFileWorkState.REGISTERED)
            results = session.exec(statement)
            result = results.all()
        return result

    def find_non_generated_files(self, dataset_id: str) -> [DataFile]:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == dataset_id, DataFile.state != DataFileWorkState.REGISTERED)
            results = session.exec(statement)
            result = results.all()
        return result

    def execute_raw_sql(self) -> Any:
        rst = []
        with Session(self.engine) as session:
            results = session.execute(text("select json_object('metadata-id', ds.id, 'owner', ds.owner_id,  'title', "
                                           "ds.title, 'release-version', ds.release_version) from dataset ds")).all()
            for result in results:
                rst.append(json.loads(result[0]))
        return rst

    def execute_l(self, dataset_id: str) -> Any:
        rst = []
        t = text("SELECT name from data_file where ds_id = '" + dataset_id + "' and state = 'UPLOADED'")
        print(t)
        with Session(self.engine) as session:
            results = session.execute(t).all()
            for result in results:
                rst.append(result[0])
        return rst


    def find_file_by_dataset_id_and_name(self, ds_id: str, file_name: str) -> DataFile:
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == ds_id,
                                               DataFile.name == file_name)
            results = session.exec(statement)
            result = results.one_or_none()
        return result

    def find_owner_assets(self, owner_id: str) -> OwnerAssetsModel | None:
        with Session(self.engine) as session:
            datasets = session.exec(select(Dataset).where(Dataset.owner_id == owner_id).order_by(desc(Dataset.created_date))).all()
            if datasets:
                oam = OwnerAssetsModel()
                oam.owner_id = owner_id
                for dataset in datasets:
                    asset = Asset()
                    asset.dataset_id = str(dataset.id)
                    asset.release_version = dataset.release_version.name
                    asset.title = dataset.title
                    asset.created_date = dataset.created_date.strftime('%Y-%m-%d %H:%M:%S')
                    asset.saved_date = dataset.saved_date.strftime('%Y-%m-%d %H:%M:%S')
                    asset.submitted_date = dataset.submitted_date.strftime('%Y-%m-%d %H:%M:%S') if dataset.submitted_date else ''
                    asset.release_version = dataset.release_version.name
                    asset.version = dataset.version if dataset.version else ''
                    targets_repo = session.exec(
                        select(TargetRepo).where(TargetRepo.ds_id == dataset.id).order_by(TargetRepo.id)).all()
                    for target_repo in targets_repo:
                        target = TargetApp()
                        target.repo_name = target_repo.name
                        target.display_name = target_repo.display_name
                        target.deposit_status = target_repo.deposit_status
                        target.deposit_time = target_repo.deposit_time.strftime('%Y-%m-%d %H:%M:%S') if target_repo.deposit_time else ''
                        target.duration = str(target_repo.duration)
                        target.output_response = json.loads(target_repo.target_output) if target_repo.target_output else {}
                        asset.targets.append(target)
                    oam.assets.append(asset)

                return oam
            return None

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
    def find_file_upload_status_by_dataset_id_and_filename(self, dataset_id, filename):
        with closing(sqlite3.connect(self.conn_url)) as connection:
            with connection:
                cursor = connection.cursor()
                cursor.execute('SELECT json(date_added) FROM data_file WHERE ds_id = ? and name=?',
                               (dataset_id, filename,))
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
                ds_record.encrypt_md(self.cipher_suite)
                session.add(ds_record)
                session.commit()
                session.refresh(ds_record)

    def set_dataset_ready_for_ingest(self, id: str, status: DatasetWorkState= DatasetWorkState.READY) -> type(None):
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.id == id)
            results = session.exec(statement)
            md_record = results.one_or_none()
            if md_record:
                # md_record.release_version = ReleaseVersion.PUBLISH
                md_record.state = status
                session.add(md_record)
                session.commit()
                session.refresh(md_record)

    def set_dataset_published(self, id: str) -> type(None):
        with Session(self.engine) as session:
            statement = select(Dataset).where(Dataset.id == id)
            results = session.exec(statement)
            md_record = results.one_or_none()
            if md_record:
                # md_record.release_version = ReleaseVersion.PUBLISH
                md_record.release_version = ReleaseVersion.PUBLISHED
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
                target_repo_record.target_output = target_repo.target_output
                target_repo_record.deposit_time = datetime.utcnow()
                target_repo_record.duration = target_repo.duration
                target_repo_record.encrypt_config(self.cipher_suite)
                session.add(target_repo_record)
                session.commit()
                session.refresh(target_repo_record)

    def update_target_output_by_id(self, target_repo=TargetRepo) -> type(None):
        with Session(self.engine) as session:
            statement = select(TargetRepo).where(TargetRepo.id == target_repo)
            results = session.exec(statement)
            target_repo_record = results.one_or_none()
            if target_repo_record:
                target_repo_record.target_output = target_repo.target_output
                target_repo_record.encrypt_config(self.cipher_suite)
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

    def update_file_permission(self, dataset_id: str, filename: str, permission: FilePermissions) -> type(None):
        with Session(self.engine) as session:
            statement = select(DataFile).where(DataFile.ds_id == dataset_id, DataFile.name == filename)
            results = session.exec(statement)
            f_record = results.one_or_none()
            if f_record:
                f_record.permissions = permission
                session.add(f_record)
                session.commit()
                session.refresh(f_record)

    def replace_targets_record(self, dataset_id: str, target_repo_records: [TargetRepo]) -> type(None):
        with Session(self.engine) as session:
            statement = select(TargetRepo).where(TargetRepo.ds_id == dataset_id)
            results = session.exec(statement)
            trs = results.fetchall()
            for tr in trs:
                session.delete(tr)
            session.commit()
            for tr in target_repo_records:
                tr.ds_id = dataset_id
                tr.encrypt_config(self.cipher_suite)
                session.add(tr)
            session.commit()

    def is_dataset_ready(self, dataset_id: str) -> bool:
        with Session(self.engine) as session:
            dataset_id_rec = session.exec(
                select(Dataset.id).where((Dataset.id == dataset_id) & (Dataset.state == DatasetWorkState.READY) &
                                         (Dataset.release_version == ReleaseVersion.PUBLISH))).one_or_none()
            return dataset_id_rec is not None

    def are_files_uploaded(self, dataset_id: str) -> bool:
        with Session(self.engine) as session:
            results = session.exec(select(DataFile).where(DataFile.ds_id == dataset_id,
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
