from __future__ import annotations

from datetime import datetime
from typing import Any

from mctp.core.constants import (
    CRITICAL_STALE_USER_DATA_CODE,
    INFO_STALE_BNBUSDT_CODE,
    INFO_STALE_BOOK_TICKER_CODE,
    WARNING_STALE_KLINE_CODE,
)
from mctp.core.enums import AlertSeverity
from mctp.runtime.events import ExecutionReportEvent, KlineEvent, OCOListStatusEvent, OutboundAccountPositionEvent
from mctp.streams.base import StreamType, refresh_stale_flags


class TestnetStreamHealthHelper:
    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime

    async def evaluate_staleness(self, now: datetime, *, enforce_user_data_fail_safe: bool = True) -> None:
        runtime = self.runtime
        flags = refresh_stale_flags(
            {stream_type: channel.state for stream_type, channel in runtime.channels.items()},
            runtime.config.stale_thresholds_seconds,
            now,
        )
        effective_kline = flags.kline and runtime.channels[StreamType.KLINE].state.last_update_at is not None
        effective_book = flags.book_ticker and runtime.channels[StreamType.BOOK_TICKER].state.last_update_at is not None
        runtime.channels[StreamType.KLINE].state.is_stale = effective_kline
        runtime.channels[StreamType.BOOK_TICKER].state.is_stale = effective_book
        runtime.channels[StreamType.BNB_TICKER].state.is_stale = (
            flags.bnb_ticker and runtime.channels[StreamType.BNB_TICKER].state.last_update_at is not None
        )
        runtime.channels[StreamType.USER_DATA].state.is_stale = (
            flags.user_data and runtime.channels[StreamType.USER_DATA].state.last_update_at is not None
        )
        if effective_kline:
            runtime._raise_alert(AlertSeverity.WARNING, WARNING_STALE_KLINE_CODE, "Kline stream is stale")
            runtime.status = runtime._status_enum.HALT
        if effective_book:
            runtime._raise_alert(AlertSeverity.INFO, INFO_STALE_BOOK_TICKER_CODE, "Book ticker stream is stale")
        if runtime.channels[StreamType.BNB_TICKER].state.is_stale:
            runtime._raise_alert(AlertSeverity.INFO, INFO_STALE_BNBUSDT_CODE, "BNBUSDT ticker stream is stale")
        if runtime.channels[StreamType.USER_DATA].state.is_stale and enforce_user_data_fail_safe:
            self.trigger_user_data_stale_fail_safe(now)

    async def stale_checkpoint(self, event: object) -> None:
        runtime = self.runtime
        timestamp = getattr(event, "timestamp", None)
        if isinstance(event, KlineEvent):
            timestamp = event.candle.timestamp
        if timestamp is not None:
            runtime.current_runtime_time = timestamp
            runtime._observe_clock_drift(timestamp)
            await self.evaluate_staleness(
                timestamp,
                enforce_user_data_fail_safe=isinstance(
                    event,
                    (OutboundAccountPositionEvent, OCOListStatusEvent, ExecutionReportEvent),
                ),
            )

    def user_data_stream_is_stale_at(self, observed_at: datetime) -> bool:
        runtime = self.runtime
        channel = runtime.channels.get(StreamType.USER_DATA)
        if channel is None or channel.state.last_update_at is None:
            return False
        threshold_seconds = runtime.config.stale_thresholds_seconds[StreamType.USER_DATA]
        return (observed_at - channel.state.last_update_at).total_seconds() > threshold_seconds

    def trigger_user_data_stale_fail_safe(self, timestamp: datetime) -> None:
        runtime = self.runtime
        if runtime._user_data_stale_fail_safe_active:
            return
        runtime._user_data_stale_fail_safe_active = True
        runtime._raise_alert(
            AlertSeverity.CRITICAL,
            CRITICAL_STALE_USER_DATA_CODE,
            "USER_DATA stream is stale; runtime halted until exchange execution state is revalidated",
            timestamp=timestamp,
        )
        runtime._emit_runtime_event("stale_user_data_fail_safe", audit=True)
        runtime.status = runtime._status_enum.HALT
