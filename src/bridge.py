from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from src.commons import settings, db_manager, logger, LOG_LEVEL_DEBUG
from src.dbz import TargetRepo, DepositStatus, DatabaseManager, Dataset, DataFile
from src.models.assistant_datamodel import Target
from src.models.bridge_output_model import BridgeOutputDataModel


@dataclass(frozen=True, kw_only=True, slots=True)
class Bridge(ABC):
    """
    Abstract base class representing a bridge between the Assistant and a specific target repository.

    Attributes:
        dataset_id (str): Identifier for the dataset.
        target (Target): Information about the target repository.
        db_manager (DatabaseManager): Database manager for interacting with the data store.
        metadata_rec (Dataset): Record representing the dataset metadata.
        app_name (str): Name of the application associated with the dataset.
        data_file_rec (DataFile): Record representing the data file associated with the dataset.
        dataset_dir (str): Directory path for the dataset.

    Methods:
        __post_init__(): Initializes the Bridge object after its creation.
        deposit() -> BridgeOutputModel: Abstract method to deposit data into the target repository.
        save_state(bridge_output_model: BridgeOutputModel = None) -> type(None): Saves the state of the deposit
        process, updating the deposit status in the database.

    Note:
        This class is expected to be subclassed with a concrete implementation of the `deposit` method.
    """

    dataset_id: str
    target: Target
    db_manager: DatabaseManager = field(init=False)
    metadata_rec: Dataset = field(init=False)
    app_name: str = field(init=False)
    data_file_rec: DataFile = field(init=False)
    dataset_dir: str = field(init=False)

    def __post_init__(self):
        """
        Initializes the Bridge object after its creation.

        The method sets up various attributes by querying the database using the provided dataset_id.
        It also sets default values for attributes like `db_manager`, `metadata_rec`, `app_name`, `data_file_rec`,
        and `dataset_dir`.

        Note:
            This method is automatically called by the dataclasses module after the object is created.
        """
        object.__setattr__(self, 'db_manager', db_manager)
        object.__setattr__(self, 'metadata_rec', self.db_manager.find_dataset(self.dataset_id))
        object.__setattr__(self, 'app_name', self.metadata_rec.app_name)
        object.__setattr__(self, 'data_file_rec', self.db_manager.find_files(self.dataset_id))
        object.__setattr__(self, 'dataset_dir', os.path.join(settings.DATA_TMP_BASE_DIR,
                                                             self.app_name, self.dataset_id))
        self.save_state()

    @classmethod
    @abstractmethod
    def deposit(cls) -> BridgeOutputDataModel:
        """
        Abstract method to deposit data into the target repository.

        Subclasses must provide a concrete implementation of this method.

        Returns:
            BridgeOutputDataModel: An instance of BridgeOutputModel representing the output of the deposit process.
        """
        ...

    def save_state(self, output_data_model: BridgeOutputDataModel = None) -> type(None):
        """
        Saves the state of the deposit process, updating the deposit status in the database.

        Args:
            output_data_model (BridgeOutputModel, optional): An instance of BridgeOutputModel representing the
                output of the deposit process. Defaults to None.
        """
        deposit_status = DepositStatus.PROGRESS
        output = ''
        duration = 0.0
        if output_data_model:
            deposit_status = output_data_model.deposit_status
            duration = output_data_model.response.duration
            output = output_data_model.model_dump_json()
            logger(f'Save state for dataset_id: {self.dataset_id}. Target: {self.target.repo_name}', LOG_LEVEL_DEBUG,
                   self.app_name)
        db_manager.update_target_repo_deposit_status(TargetRepo(ds_id=self.dataset_id, name=self.target.repo_name,
                                                                deposit_status=deposit_status, target_output=output,
                                                                duration=duration))

    def deposit_files(self):
        pass
