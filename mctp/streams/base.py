"""
Contract 04: structure of 4 independent websocket streams.
Real connections are for v1.0 (SpotLiveAdapter).
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class StreamType(Enum):
    KLINE = "KLINE"
    BOOK_TICKER = "BOOK_TICKER"
    BNB_TICKER = "BNB_TICKER"
    USER_DATA = "USER_DATA"


@dataclass
class StreamState:
    stream_type: StreamType
    is_connected: bool
    last_update_at: Optional[datetime]
    is_stale: bool

    def __post_init__(self) -> None:
        if self.last_update_at is not None and self.last_update_at.tzinfo is None:
            raise ValueError("StreamState.last_update_at must be UTC-aware")

    def is_stale_check(self, threshold_seconds: int) -> bool:
        return self.stale_at(datetime.now(timezone.utc), threshold_seconds)

    def stale_at(self, now: datetime, threshold_seconds: int) -> bool:
        if now.tzinfo is None:
            raise ValueError("now must be UTC-aware")
        if self.last_update_at is None:
            return True
        delta = (now - self.last_update_at).total_seconds()
        return delta > threshold_seconds


@dataclass(frozen=True)
class StreamStaleFlags:
    kline: bool
    book_ticker: bool
    bnb_ticker: bool
    user_data: bool

    @classmethod
    def from_states(
        cls,
        states: dict[StreamType, StreamState],
        thresholds_seconds: dict[StreamType, int],
        now: datetime,
    ) -> "StreamStaleFlags":
        if now.tzinfo is None:
            raise ValueError("now must be UTC-aware")
        required = (
            StreamType.KLINE,
            StreamType.BOOK_TICKER,
            StreamType.BNB_TICKER,
            StreamType.USER_DATA,
        )
        for stream_type in required:
            if stream_type not in states:
                raise KeyError(f"Missing stream state for {stream_type}")
            if stream_type not in thresholds_seconds:
                raise KeyError(f"Missing stale threshold for {stream_type}")
        return cls(
            kline=states[StreamType.KLINE].stale_at(now, thresholds_seconds[StreamType.KLINE]),
            book_ticker=states[StreamType.BOOK_TICKER].stale_at(
                now, thresholds_seconds[StreamType.BOOK_TICKER]
            ),
            bnb_ticker=states[StreamType.BNB_TICKER].stale_at(
                now, thresholds_seconds[StreamType.BNB_TICKER]
            ),
            user_data=states[StreamType.USER_DATA].stale_at(
                now, thresholds_seconds[StreamType.USER_DATA]
            ),
        )


def refresh_stale_flags(
    states: dict[StreamType, StreamState],
    thresholds_seconds: dict[StreamType, int],
    now: datetime,
) -> StreamStaleFlags:
    flags = StreamStaleFlags.from_states(states, thresholds_seconds, now)
    states[StreamType.KLINE].is_stale = flags.kline
    states[StreamType.BOOK_TICKER].is_stale = flags.book_ticker
    states[StreamType.BNB_TICKER].is_stale = flags.bnb_ticker
    states[StreamType.USER_DATA].is_stale = flags.user_data
    return flags
