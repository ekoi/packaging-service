from __future__ import annotations

from src.bridge import Bridge
from src.models.bridge_output_model import BridgeOutputModel


class SwhSwordDepositor(Bridge):

    def deposit(self) -> BridgeOutputModel:

        bridge_output_model = BridgeOutputModel()

        return bridge_output_model



