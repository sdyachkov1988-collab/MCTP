from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Optional

from mctp.core.enums import AlertSeverity, ExecutionResult, Timeframe
from mctp.core.order import Fill
from mctp.core.types import Symbol
from mctp.indicators.models import Candle


@dataclass(frozen=True)
class KlineEvent:
    timeframe: Timeframe
    candle: Candle


@dataclass(frozen=True)
class BookTickerEvent:
    timestamp: datetime
    bid: Decimal
    ask: Decimal


@dataclass(frozen=True)
class BnbTickerEvent:
    timestamp: datetime
    price: Decimal


@dataclass(frozen=True)
class MockExecutionReportEvent:
    fill: Fill


@dataclass(frozen=True)
class OutboundAccountPositionEvent:
    timestamp: datetime
    balances: dict[str, Decimal]
    locked_balances: dict[str, Decimal] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutionReportEvent:
    timestamp: datetime
    symbol: Symbol
    client_order_id: str
    execution_result: ExecutionResult
    order_status: str
    fill: Optional[Fill] = None


@dataclass(frozen=True)
class OCOListStatusEvent:
    timestamp: datetime
    symbol: Symbol
    list_order_id: str
    list_status_type: str
    list_order_status: str
    contingency_type: str


@dataclass(frozen=True)
class RuntimeAlertEvent:
    timestamp: datetime
    severity: AlertSeverity
    code: str
    message: str


@dataclass(frozen=True)
class DelistingSignalEvent:
    symbol: Symbol
    listed: bool
    source: str
    details: str
    days_until_delisting: Optional[int] = None
