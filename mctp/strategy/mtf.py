import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Sequence

from mctp.core.constants import (
    V20_MTF_BASE_TIMEFRAME_MINUTES,
    V20_MTF_M15_PER_D1,
    V20_MTF_M15_PER_H1,
    V20_MTF_M15_PER_H4,
    V20_MTF_REQUIRED_M15_CANDLES,
)
from mctp.core.enums import Timeframe
from mctp.indicators.models import Candle


_logger = logging.getLogger(__name__)
_BASE_CANDLE_INTERVAL = timedelta(minutes=V20_MTF_BASE_TIMEFRAME_MINUTES)
_EXPECTED_BUCKET_SIZES: dict[Timeframe, int] = {
    Timeframe.H1: V20_MTF_M15_PER_H1,
    Timeframe.H4: V20_MTF_M15_PER_H4,
    Timeframe.D1: V20_MTF_M15_PER_D1,
}
_DERIVED_MTF_TIMEFRAMES: tuple[Timeframe, ...] = (
    Timeframe.H1,
    Timeframe.H4,
    Timeframe.D1,
    Timeframe.W1,
    Timeframe.MONTHLY,
)


def required_m15_history_for_v20_btcusdt_mtf() -> int:
    return V20_MTF_REQUIRED_M15_CANDLES


def build_closed_mtf_candle_map_from_m15(base_candles: Sequence[Candle]) -> dict[Timeframe, list[Candle]]:
    closed_base = [candle for candle in base_candles if candle.closed]
    candle_map = {
        Timeframe.M15: list(closed_base),
    }
    for timeframe in _DERIVED_MTF_TIMEFRAMES:
        candle_map[timeframe] = aggregate_closed_m15_candles(closed_base, timeframe)
    return candle_map


def aggregate_closed_m15_candles(base_candles: Sequence[Candle], timeframe: Timeframe) -> list[Candle]:
    if timeframe == Timeframe.M15:
        return [candle for candle in base_candles if candle.closed]
    if timeframe not in _DERIVED_MTF_TIMEFRAMES:
        raise ValueError(f"Unsupported timeframe for M15 aggregation: {timeframe.value}")
    buckets: dict[datetime, list[Candle]] = {}
    for candle in sorted((c for c in base_candles if c.closed), key=lambda item: item.timestamp):
        bucket_start = _bucket_start_for_timeframe(candle.timestamp, timeframe)
        buckets.setdefault(bucket_start, []).append(candle)
    aggregated: list[Candle] = []
    latest_bucket_start = max(buckets) if buckets else None
    for bucket_start in sorted(buckets):
        bucket = buckets[bucket_start]
        bucket_end = _bucket_end_for_timeframe(bucket_start, timeframe)
        expected_bucket_size = int((bucket_end - bucket_start) / _BASE_CANDLE_INTERVAL)
        if len(bucket) != expected_bucket_size:
            if bucket_start != latest_bucket_start:
                _logger.warning(
                    "Dropping %s bucket at %s: expected %d M15 candles, got %d",
                    timeframe.value,
                    bucket_start.isoformat(),
                    expected_bucket_size,
                    len(bucket),
            )
            continue
        if not _is_complete_bucket(bucket, bucket_start, bucket_end):
            _logger.warning(
                "Dropping %s bucket at %s: M15 candles are not contiguous",
                timeframe.value,
                bucket_start.isoformat(),
            )
            continue
        aggregated.append(
            Candle(
                timestamp=bucket_start,
                open=bucket[0].open,
                high=max(candle.high for candle in bucket),
                low=min(candle.low for candle in bucket),
                close=bucket[-1].close,
                volume=sum((candle.volume for candle in bucket), Decimal("0")),
                closed=True,
            )
        )
    return aggregated


def _bucket_start_for_timeframe(timestamp: datetime, timeframe: Timeframe) -> datetime:
    if timestamp.tzinfo is None:
        raise ValueError("timestamp must be UTC-aware")
    normalized = timestamp.astimezone(timezone.utc).replace(second=0, microsecond=0)
    if timeframe == Timeframe.H1:
        return normalized.replace(minute=0)
    if timeframe == Timeframe.H4:
        return normalized.replace(hour=(normalized.hour // 4) * 4, minute=0)
    if timeframe == Timeframe.D1:
        return normalized.replace(hour=0, minute=0)
    if timeframe == Timeframe.W1:
        week_start = normalized - timedelta(days=normalized.weekday())
        return week_start.replace(hour=0, minute=0)
    if timeframe == Timeframe.MONTHLY:
        return normalized.replace(day=1, hour=0, minute=0)
    raise ValueError(f"Unsupported timeframe for M15 aggregation: {timeframe.value}")


def _bucket_end_for_timeframe(bucket_start: datetime, timeframe: Timeframe) -> datetime:
    if timeframe == Timeframe.H1:
        return bucket_start + timedelta(hours=1)
    if timeframe == Timeframe.H4:
        return bucket_start + timedelta(hours=4)
    if timeframe == Timeframe.D1:
        return bucket_start + timedelta(days=1)
    if timeframe == Timeframe.W1:
        return bucket_start + timedelta(days=7)
    if timeframe == Timeframe.MONTHLY:
        if bucket_start.month == 12:
            return bucket_start.replace(year=bucket_start.year + 1, month=1)
        return bucket_start.replace(month=bucket_start.month + 1)
    raise ValueError(f"Unsupported timeframe for M15 aggregation: {timeframe.value}")


def _is_complete_bucket(bucket: Sequence[Candle], bucket_start: datetime, bucket_end: datetime) -> bool:
    expected_time = bucket_start
    for candle in bucket:
        if candle.timestamp != expected_time:
            return False
        expected_time += _BASE_CANDLE_INTERVAL
    return expected_time == bucket_end
