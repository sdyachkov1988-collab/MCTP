from abc import ABC, abstractmethod

from mctp.core.types import Intent

from .models import StrategyInput


class StrategyBase(ABC):
    @abstractmethod
    def on_candle(self, input: StrategyInput) -> Intent:
        raise NotImplementedError
