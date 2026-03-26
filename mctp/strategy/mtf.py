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


_BASE_CANDLE_INTERVAL = timedelta(minutes=V20_MTF_BASE_TIMEFRAME_MINUTES)
_EXPECTED_BUCKET_SIZES: dict[Timeframe, int] = {
    Timeframe.H1: V20_MTF_M15_PER_H1,
    Timeframe.H4: V20_MTF_M15_PER_H4,
    Timeframe.D1: V20_MTF_M15_PER_D1,
}


def required_m15_history_for_v20_btcusdt_mtf() -> int:
    return V20_MTF_REQUIRED_M15_CANDLES


def build_closed_mtf_candle_map_from_m15(base_candles: Sequence[Candle]) -> dict[Timeframe, list[Candle]]:
    closed_base = [candle for candle in base_candles if candle.closed]
    return {
        Timeframe.M15: list(closed_base),
        Timeframe.H1: aggregate_closed_m15_candles(closed_base, Timeframe.H1),
        Timeframe.H4: aggregate_closed_m15_candles(closed_base, Timeframe.H4),
        Timeframe.D1: aggregate_closed_m15_candles(closed_base, Timeframe.D1),
    }


def aggregate_closed_m15_candles(base_candles: Sequence[Candle], timeframe: Timeframe) -> list[Candle]:
    if timeframe == Timeframe.M15:
        return [candle for candle in base_candles if candle.closed]
    expected_bucket_size = _EXPECTED_BUCKET_SIZES.get(timeframe)
    if expected_bucket_size is None:
        raise ValueError(f"Unsupported timeframe for M15 aggregation: {timeframe.value}")
    buckets: dict[datetime, list[Candle]] = {}
    for candle in sorted((c for c in base_candles if c.closed), key=lambda item: item.timestamp):
        bucket_start = _bucket_start_for_timeframe(candle.timestamp, timeframe)
        buckets.setdefault(bucket_start, []).append(candle)
    aggregated: list[Candle] = []
    for bucket_start in sorted(buckets):
        bucket = buckets[bucket_start]
        if len(bucket) != expected_bucket_size:
            continue
        if not _is_complete_bucket(bucket, bucket_start):
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
    raise ValueError(f"Unsupported timeframe for M15 aggregation: {timeframe.value}")


def _is_complete_bucket(bucket: Sequence[Candle], bucket_start: datetime) -> bool:
    expected_time = bucket_start
    for candle in bucket:
        if candle.timestamp != expected_time:
            return False
        expected_time += _BASE_CANDLE_INTERVAL
    return True
