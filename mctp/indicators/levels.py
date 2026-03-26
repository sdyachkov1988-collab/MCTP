from decimal import Decimal
from typing import Sequence

from .models import Candle, FibonacciLevels, PivotPoints


def fibonacci_levels(candles: Sequence[Candle]) -> FibonacciLevels:
    if not candles:
        raise ValueError("candles must not be empty")
    highest_high = max(c.high for c in candles)
    lowest_low = min(c.low for c in candles)
    span = highest_high - lowest_low
    return FibonacciLevels(
        level_0=highest_high,
        level_236=highest_high - (span * Decimal("0.236")),
        level_382=highest_high - (span * Decimal("0.382")),
        level_500=highest_high - (span * Decimal("0.5")),
        level_618=highest_high - (span * Decimal("0.618")),
        level_786=highest_high - (span * Decimal("0.786")),
        level_1000=lowest_low,
    )


def pivot_points(candle: Candle) -> PivotPoints:
    pivot = (candle.high + candle.low + candle.close) / Decimal("3")
    return PivotPoints(
        pivot=pivot,
        resistance_1=(Decimal("2") * pivot) - candle.low,
        resistance_2=pivot + (candle.high - candle.low),
        support_1=(Decimal("2") * pivot) - candle.high,
        support_2=pivot - (candle.high - candle.low),
    )
