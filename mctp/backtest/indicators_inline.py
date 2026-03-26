"""Legacy compatibility helpers for pre-v0.11 inline indicators.

The active backtest/runtime path must use Indicator Engine v1.
These helpers remain only to preserve narrow historical tests and should not be
used for new code. They are intentionally not re-exported from ``mctp.backtest``.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
import warnings

from .market_replay import BacktestCandle

__all__ = ["ema_next", "true_range", "InlineIndicatorState"]


def ema_next(previous_ema: Optional[Decimal], close: Decimal, period: int) -> Decimal:
    warnings.warn(
        "mctp.backtest.indicators_inline is deprecated; use IndicatorEngine instead",
        DeprecationWarning,
        stacklevel=2,
    )
    if previous_ema is None:
        return close
    alpha = Decimal("2") / Decimal(period + 1)
    return (close - previous_ema) * alpha + previous_ema


def true_range(current: BacktestCandle, previous_close: Optional[Decimal]) -> Decimal:
    if previous_close is None:
        return current.high - current.low
    return max(
        current.high - current.low,
        abs(current.high - previous_close),
        abs(current.low - previous_close),
    )


@dataclass
class InlineIndicatorState:
    ema_period: int
    atr_period: int
    ema: Optional[Decimal] = None
    atr: Optional[Decimal] = None
    _prev_close: Optional[Decimal] = None
    _tr_count: int = 0
    _tr_sum: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        warnings.warn(
            "InlineIndicatorState is deprecated; use IndicatorEngine snapshot/ATR helpers instead",
            DeprecationWarning,
            stacklevel=2,
        )

    def update(self, candle: BacktestCandle) -> tuple[Decimal, Optional[Decimal]]:
        self.ema = ema_next(self.ema, candle.close, self.ema_period)
        tr = true_range(candle, self._prev_close)
        if self._tr_count < self.atr_period:
            self._tr_sum += tr
            self._tr_count += 1
            if self._tr_count == self.atr_period:
                self.atr = self._tr_sum / Decimal(self.atr_period)
        else:
            assert self.atr is not None
            self.atr = ((self.atr * Decimal(self.atr_period - 1)) + tr) / Decimal(self.atr_period)
        self._prev_close = candle.close
        return self.ema, self.atr
