from decimal import Decimal
from typing import Sequence

from mctp.core.enums import Timeframe

from .engine import IndicatorEngine
from .models import Candle, CandlestickPatternSignal


def detect_weighted_patterns(
    candles: Sequence[Candle],
    timeframe: Timeframe,
    engine: IndicatorEngine | None = None,
) -> tuple[CandlestickPatternSignal, ...]:
    if len(candles) < 2:
        return tuple()
    engine = engine or IndicatorEngine()
    previous = candles[-2]
    current = candles[-1]
    signals: list[CandlestickPatternSignal] = []
    weight = engine.timeframe_weight(timeframe)

    prev_bearish = previous.close < previous.open
    current_bullish = current.close > current.open
    prev_bullish = previous.close > previous.open
    current_bearish = current.close < current.open

    if prev_bearish and current_bullish and current.open <= previous.close and current.close >= previous.open:
        signals.append(
            CandlestickPatternSignal(
                name="bullish_engulfing",
                timeframe=timeframe,
                direction="bullish",
                weight=weight,
                score=weight,
            )
        )
    if prev_bullish and current_bearish and current.open >= previous.close and current.close <= previous.open:
        signals.append(
            CandlestickPatternSignal(
                name="bearish_engulfing",
                timeframe=timeframe,
                direction="bearish",
                weight=weight,
                score=weight * Decimal("-1"),
            )
        )
    return tuple(signals)
