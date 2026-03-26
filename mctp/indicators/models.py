from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from mctp.core.enums import Timeframe


@dataclass(frozen=True)
class Candle:
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    closed: bool = True

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None:
            raise ValueError("Candle.timestamp must be UTC-aware")
        for name, value in [
            ("open", self.open),
            ("high", self.high),
            ("low", self.low),
            ("close", self.close),
            ("volume", self.volume),
        ]:
            if not isinstance(value, Decimal):
                raise AssertionError(f"Candle.{name} must be Decimal")
        if not isinstance(self.closed, bool):
            raise AssertionError("Candle.closed must be bool")


@dataclass(frozen=True)
class CandlestickPatternSignal:
    name: str
    timeframe: Timeframe
    direction: str
    weight: Decimal
    score: Decimal


@dataclass(frozen=True)
class FibonacciLevels:
    level_0: Decimal
    level_236: Decimal
    level_382: Decimal
    level_500: Decimal
    level_618: Decimal
    level_786: Decimal
    level_1000: Decimal


@dataclass(frozen=True)
class PivotPoints:
    pivot: Decimal
    resistance_1: Decimal
    resistance_2: Decimal
    support_1: Decimal
    support_2: Decimal


@dataclass(frozen=True)
class WarmupRequirement:
    timeframe: Timeframe
    bars_required: int


@dataclass(frozen=True)
class IndicatorSnapshot:
    ema: Optional[Decimal] = None
    sma: Optional[Decimal] = None
    hull_ma: Optional[Decimal] = None
    rsi: Optional[Decimal] = None
    stochastic_k: Optional[Decimal] = None
    stochastic_d: Optional[Decimal] = None
    cci: Optional[Decimal] = None
    atr: Optional[Decimal] = None
    bollinger_mid: Optional[Decimal] = None
    bollinger_upper: Optional[Decimal] = None
    bollinger_lower: Optional[Decimal] = None
    keltner_mid: Optional[Decimal] = None
    keltner_upper: Optional[Decimal] = None
    keltner_lower: Optional[Decimal] = None
    obv: Optional[Decimal] = None
    vwap: Optional[Decimal] = None
    cmf: Optional[Decimal] = None
