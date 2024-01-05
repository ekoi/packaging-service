from __future__ import annotations

from src.bridge import Bridge
from src.models.bridge_output_model import BridgeOutputDataModel


class Mail(Bridge):

    def deposit(self) -> BridgeOutputDataModel:

        bridge_output_model = BridgeOutputDataModel()

        return bridge_output_model
