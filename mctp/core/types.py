from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, timezone
from typing import Optional

from .enums import Market, IntentType, QuantityMode


@dataclass(frozen=True)
class Symbol:
    base: str
    quote: str
    market: Market

    def to_exchange_str(self) -> str:
        return f"{self.base}{self.quote}"

    def __str__(self) -> str:
        return f"{self.base}/{self.quote} ({self.market.value})"


@dataclass
class PortfolioSnapshot:
    symbol: Symbol
    held_qty: Decimal
    avg_cost_basis: Decimal
    free_quote: Decimal
    quote_asset: str
    is_in_position: bool
    meaningful_position: bool
    scale_in_count: int = 0
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self):
        if self.timestamp.tzinfo is None:
            raise ValueError("PortfolioSnapshot.timestamp must be UTC-aware")
        if not isinstance(self.held_qty, Decimal):
            raise AssertionError("held_qty must be Decimal")
        if not isinstance(self.avg_cost_basis, Decimal):
            raise AssertionError("avg_cost_basis must be Decimal")
        if not isinstance(self.free_quote, Decimal):
            raise AssertionError("free_quote must be Decimal")


@dataclass
class Intent:
    type: IntentType
    symbol: Symbol
    quantity_mode: Optional[QuantityMode] = None
    partial_fraction: Optional[Decimal] = None
    reason: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
