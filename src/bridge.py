from __future__ import annotations

import os
from abc import ABC, abstractmethod

from src.models.assistant_datamodel import Target
from src.models.bridge_output_model import BridgeOutputModel
from src.commons import settings, db_manager, handle_deposit_exceptions

from src.dbz import TargetRepo, DepositStatus, DatabaseManager, Dataset, DataFile

from dataclasses import dataclass, field


@dataclass(frozen=True, kw_only=True, slots=True)
class Bridge(ABC):
    # No need to check empty database result since it has been checked  in the earlier process (see protected.py).
    dataset_id: str
    target: Target
    db_manager: DatabaseManager = field(init=False)
    metadata_rec: Dataset = field(init=False)  # TODO: change to dataset_rec
    app_name: str = field(init=False)
    data_file_rec: DataFile = field(init=False)
    dataset_dir: str = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, 'db_manager', db_manager)
        object.__setattr__(self, 'metadata_rec', self.db_manager.find_dataset(self.dataset_id))
        object.__setattr__(self, 'app_name', self.metadata_rec.app_name)
        object.__setattr__(self, 'data_file_rec', self.db_manager.find_files(self.dataset_id))
        object.__setattr__(self, 'dataset_dir', os.path.join(settings.DATA_TMP_BASE_DIR,
                                                             self.app_name, self.dataset_id))
        self.save_state()

    @classmethod
    @abstractmethod
    def deposit(cls) -> BridgeOutputModel:
        ...

    def save_state(self, bridge_output_model: BridgeOutputModel = None) -> type(None):
        deposit_status = DepositStatus.PROGRESS
        output = ''
        if bridge_output_model:
            print(bridge_output_model)
            deposit_status = bridge_output_model.deposit_status
            output = bridge_output_model.model_dump_json()
        db_manager.update_target_repo_deposit_status(TargetRepo(ds_id=self.dataset_id, name=self.target.repo_name,
                                                                deposit_status=deposit_status, output=output))
