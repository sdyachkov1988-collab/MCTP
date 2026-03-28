import logging
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal

from mctp.core.enums import Timeframe
from mctp.indicators.models import Candle
from mctp.strategy.mtf import _bucket_end_for_timeframe, _bucket_start_for_timeframe


_logger = logging.getLogger(__name__)
_M15_INTERVAL = timedelta(minutes=15)
_DERIVED_TIMEFRAMES: tuple[Timeframe, ...] = (
    Timeframe.H1,
    Timeframe.H4,
    Timeframe.D1,
    Timeframe.W1,
    Timeframe.MONTHLY,
)


@dataclass
class _BucketState:
    timeframe: Timeframe
    bucket_start: object
    bucket_end: object
    candles: list[Candle]
    last_timestamp: object
    started_at_boundary: bool
    contiguous: bool = True
    finalized: bool = False


class IncrementalMtfBacktestBuilder:
    def __init__(self) -> None:
        self._m15: list[Candle] = []
        self._derived: dict[Timeframe, list[Candle]] = {timeframe: [] for timeframe in _DERIVED_TIMEFRAMES}
        self._active: dict[Timeframe, _BucketState | None] = {timeframe: None for timeframe in _DERIVED_TIMEFRAMES}

    def append(self, candle: Candle) -> None:
        if not candle.closed:
            return
        self._m15.append(candle)
        for timeframe in _DERIVED_TIMEFRAMES:
            self._append_to_timeframe(timeframe, candle)

    def candle_map(self) -> dict[Timeframe, list[Candle]]:
        candle_map = {Timeframe.M15: self._m15}
        candle_map.update(self._derived)
        return candle_map

    def _append_to_timeframe(self, timeframe: Timeframe, candle: Candle) -> None:
        bucket_start = _bucket_start_for_timeframe(candle.timestamp, timeframe)
        state = self._active[timeframe]
        if state is None or state.bucket_start != bucket_start:
            self._finalize_incomplete_bucket(state)
            state = _BucketState(
                timeframe=timeframe,
                bucket_start=bucket_start,
                bucket_end=_bucket_end_for_timeframe(bucket_start, timeframe),
                candles=[],
                last_timestamp=candle.timestamp,
                started_at_boundary=candle.timestamp == bucket_start,
            )
            self._active[timeframe] = state
        elif state.last_timestamp != candle.timestamp and candle.timestamp != state.last_timestamp + _M15_INTERVAL:
            state.contiguous = False

        if state.candles and candle.timestamp != state.candles[-1].timestamp + _M15_INTERVAL:
            state.contiguous = False
        state.candles.append(candle)
        state.last_timestamp = candle.timestamp
        if state.finalized:
            return
        if not state.started_at_boundary:
            return
        if candle.timestamp + _M15_INTERVAL != state.bucket_end:
            return
        if not state.contiguous:
            return
        self._derived[timeframe].append(
            Candle(
                timestamp=state.bucket_start,
                open=state.candles[0].open,
                high=max(item.high for item in state.candles),
                low=min(item.low for item in state.candles),
                close=state.candles[-1].close,
                volume=sum((item.volume for item in state.candles), Decimal("0")),
                closed=True,
            )
        )
        state.finalized = True

    def _finalize_incomplete_bucket(self, state: _BucketState | None) -> None:
        if state is None or state.finalized:
            return
        expected_bucket_size = int((state.bucket_end - state.bucket_start).total_seconds() // (15 * 60))
        if not state.contiguous:
            _logger.warning(
                "Dropping %s bucket at %s: M15 candles are not contiguous",
                state.timeframe.value,
                state.bucket_start.isoformat(),
            )
            return
        _logger.warning(
            "Dropping %s bucket at %s: expected %d M15 candles, got %d",
            state.timeframe.value,
            state.bucket_start.isoformat(),
            expected_bucket_size,
            len(state.candles),
        )
