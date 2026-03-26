from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from mctp.core.enums import Timeframe
from mctp.indicators.models import Candle
from mctp.runtime.events import BnbTickerEvent, BookTickerEvent, KlineEvent
from mctp.streams.base import StreamType


def adapt_binance_payload(
    stream_type: StreamType,
    payload: object,
    timeframe: Timeframe | None = None,
) -> object:
    if stream_type == StreamType.USER_DATA:
        return payload
    if not isinstance(payload, dict):
        return payload
    if stream_type == StreamType.KLINE:
        return _adapt_kline(payload, timeframe)
    if stream_type == StreamType.BOOK_TICKER:
        return _adapt_book_ticker(payload)
    if stream_type == StreamType.BNB_TICKER:
        return _adapt_bnb_ticker(payload)
    raise ValueError(f"Unsupported stream type for adapter: {stream_type}")


def _adapt_kline(payload: dict[str, Any], timeframe: Timeframe | None) -> KlineEvent:
    kline = payload.get("k")
    if not isinstance(kline, dict):
        raise ValueError("Binance kline payload must contain object 'k'")
    if timeframe is None:
        timeframe_raw = kline.get("i")
        if timeframe_raw is None:
            raise ValueError("Binance kline payload missing timeframe")
        timeframe = Timeframe(str(timeframe_raw))
    return KlineEvent(
        timeframe=timeframe,
        candle=Candle(
            timestamp=_from_millis(kline.get("T")),
            open=Decimal(str(kline["o"])),
            high=Decimal(str(kline["h"])),
            low=Decimal(str(kline["l"])),
            close=Decimal(str(kline["c"])),
            volume=Decimal(str(kline["v"])),
            closed=bool(kline.get("x", True)),
        ),
    )


def _adapt_book_ticker(payload: dict[str, Any]) -> BookTickerEvent:
    return BookTickerEvent(
        timestamp=_from_millis(payload.get("E")),
        bid=Decimal(str(payload["b"])),
        ask=Decimal(str(payload["a"])),
    )


def _adapt_bnb_ticker(payload: dict[str, Any]) -> BnbTickerEvent:
    bid = Decimal(str(payload["b"]))
    ask = Decimal(str(payload["a"]))
    return BnbTickerEvent(
        timestamp=_from_millis(payload.get("E")),
        price=(bid + ask) / Decimal("2"),
    )


def _from_millis(value: object) -> datetime:
    if value is None:
        raise ValueError("Binance payload missing event timestamp")
    return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
