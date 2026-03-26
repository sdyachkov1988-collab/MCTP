import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from mctp.core.constants import BINANCE_WS_BASE_URL
from mctp.core.enums import Timeframe
from mctp.core.types import Symbol
from mctp.runtime.adapters import adapt_binance_payload
from mctp.streams.base import StreamState, StreamType


class QueueStreamTransport:
    def __init__(self) -> None:
        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        self.connected = False
        self.ping_count = 0
        self.pong_count = 0

    async def connect(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.connected = False

    async def publish(self, event: Any) -> None:
        await self._queue.put(event)

    async def receive(self) -> Any | None:
        if self._queue.empty():
            return None
        return await self._queue.get()

    async def ping(self) -> None:
        self.ping_count += 1

    async def pong(self) -> None:
        self.pong_count += 1


class WebSocketJsonTransport:
    def __init__(self) -> None:
        self._connection = None
        self.connected = False
        self.ping_count = 0
        self.pong_count = 0

    async def connect(self, endpoint: str) -> None:
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("websockets package is required for real Binance stream transport") from exc
        self._connection = await websockets.connect(endpoint, ping_interval=None)
        self.connected = True

    async def disconnect(self) -> None:
        if self._connection is not None:
            await self._connection.close()
        self.connected = False

    async def publish(self, event: Any) -> None:
        if self._connection is None:
            raise RuntimeError("transport is not connected")
        await self._connection.send(json.dumps(event))

    async def receive(self) -> Any | None:
        if self._connection is None:
            return None
        payload = await self._connection.recv()
        return json.loads(payload)

    async def ping(self) -> None:
        if self._connection is not None:
            waiter = await self._connection.ping()
            await waiter
        self.ping_count += 1

    async def pong(self) -> None:
        self.pong_count += 1


@dataclass
class StreamChannel:
    stream_type: StreamType
    endpoint: str
    transport: QueueStreamTransport
    timeframe: Optional[Timeframe] = None
    state: StreamState = field(init=False)
    last_ping_at: Optional[datetime] = None
    last_pong_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        self.state = StreamState(
            stream_type=self.stream_type,
            is_connected=False,
            last_update_at=None,
            is_stale=False,
        )

    async def connect(self) -> None:
        if isinstance(self.transport, WebSocketJsonTransport):
            await self.transport.connect(self.endpoint)
        else:
            await self.transport.connect()
        self.state.is_connected = True

    async def disconnect(self) -> None:
        await self.transport.disconnect()
        self.state.is_connected = False

    async def ping(self, now: Optional[datetime] = None) -> None:
        stamp = now if now is not None else datetime.now(timezone.utc)
        await self.transport.ping()
        self.last_ping_at = stamp

    async def pong(self, now: Optional[datetime] = None) -> None:
        stamp = now if now is not None else datetime.now(timezone.utc)
        await self.transport.pong()
        self.last_pong_at = stamp

    async def publish(self, event: Any) -> None:
        await self.transport.publish(event)

    async def receive(self) -> Any | None:
        payload = await self.transport.receive()
        if payload is None:
            return None
        return adapt_binance_payload(self.stream_type, payload, self.timeframe)

    def touch(self, at: datetime) -> None:
        self.state.last_update_at = at
        self.state.is_stale = False


class BinanceKlineStreamChannel(StreamChannel):
    def __init__(self, symbol: Symbol, timeframe: Timeframe, transport: QueueStreamTransport):
        endpoint = f"{BINANCE_WS_BASE_URL}/{symbol.to_exchange_str().lower()}@kline_{timeframe.value}"
        super().__init__(stream_type=StreamType.KLINE, endpoint=endpoint, transport=transport, timeframe=timeframe)


class BinanceBookTickerStreamChannel(StreamChannel):
    def __init__(self, symbol: Symbol, transport: QueueStreamTransport):
        endpoint = f"{BINANCE_WS_BASE_URL}/{symbol.to_exchange_str().lower()}@bookTicker"
        super().__init__(stream_type=StreamType.BOOK_TICKER, endpoint=endpoint, transport=transport)


class BinanceBnbTickerStreamChannel(StreamChannel):
    def __init__(self, transport: QueueStreamTransport):
        endpoint = f"{BINANCE_WS_BASE_URL}/bnbusdt@bookTicker"
        super().__init__(stream_type=StreamType.BNB_TICKER, endpoint=endpoint, transport=transport)


class MockUserDataStreamChannel(StreamChannel):
    def __init__(self, transport: QueueStreamTransport):
        super().__init__(stream_type=StreamType.USER_DATA, endpoint="mock://user-data", transport=transport)
