from abc import ABC, abstractmethod
from decimal import Decimal

from .types import Symbol
from .enums import ExecutionResult
from .order import Fill


class ExecutionInterface(ABC):
    @abstractmethod
    async def submit_order(self, order) -> ExecutionResult:
        ...

    @abstractmethod
    async def cancel_order(self, client_order_id: str) -> ExecutionResult:
        ...

    @abstractmethod
    async def get_balances(self) -> dict[str, Decimal]:
        ...

    @abstractmethod
    async def get_instrument_info(self, symbol: Symbol) -> dict:
        ...

    @abstractmethod
    async def get_fills(self, client_order_id: str) -> list[Fill]:
        ...
