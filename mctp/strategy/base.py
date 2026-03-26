from abc import ABC, abstractmethod

from mctp.core.types import Intent

from .models import StrategyInput


class StrategyBase(ABC):
    requires_mtf_warmup: bool = False

    @abstractmethod
    def on_candle(self, input: StrategyInput) -> Intent:
        raise NotImplementedError
