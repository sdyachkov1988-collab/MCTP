from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Optional
from uuid import uuid4

from mctp.adapters import BinanceSpotTestnetAdapterV1

from dataclasses import dataclass, field

from mctp.core.constants import BINANCE_SPOT_TESTNET_WS_API_BASE_URL, BINANCE_SPOT_TESTNET_WS_BASE_URL
from mctp.core.enums import Timeframe
from mctp.core.types import Symbol
from mctp.runtime.streams import WebSocketJsonTransport
from mctp.streams.base import StreamState, StreamType


AdapterFn = Callable[[Any], Any]


@dataclass
class ReconnectableStreamChannel:
    stream_type: StreamType
    endpoint: str
    transport: Any
    adapter: AdapterFn
    state: StreamState = field(init=False)
    reconnect_count: int = 0
    last_ping_at: Optional[datetime] = None
    last_pong_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        self.state = StreamState(self.stream_type, False, None, False)

    async def connect(self) -> None:
        if isinstance(self.transport, WebSocketJsonTransport):
            await self.transport.connect(self.endpoint)
        else:
            await self.transport.connect()
        self.state.is_connected = True

    async def disconnect(self) -> None:
        await self.transport.disconnect()
        self.state.is_connected = False

    async def reconnect(self) -> None:
        self.reconnect_count += 1
        await self.disconnect()
        await self.connect()

    async def receive(self) -> Any | None:
        try:
            payload = await self.transport.receive()
        except Exception:
            await self.reconnect()
            return None
        if payload is None:
            return None
        return self.adapter(payload)

    async def publish(self, event: Any) -> None:
        await self.transport.publish(event)

    async def ping(self, now: Optional[datetime] = None) -> None:
        stamp = now if now is not None else datetime.now(timezone.utc)
        await self.transport.ping()
        self.last_ping_at = stamp

    async def pong(self, now: Optional[datetime] = None) -> None:
        stamp = now if now is not None else datetime.now(timezone.utc)
        await self.transport.pong()
        self.last_pong_at = stamp

    def touch(self, at: datetime) -> None:
        self.state.last_update_at = at
        self.state.is_stale = False


class BinanceSpotTestnetKlineChannel(ReconnectableStreamChannel):
    def __init__(self, symbol: Symbol, timeframe: Timeframe, transport: Any, adapter: AdapterFn):
        endpoint = f"{BINANCE_SPOT_TESTNET_WS_BASE_URL}/{symbol.to_exchange_str().lower()}@kline_{timeframe.value}"
        super().__init__(StreamType.KLINE, endpoint, transport, adapter)


class BinanceSpotTestnetBookTickerChannel(ReconnectableStreamChannel):
    def __init__(self, symbol: Symbol, transport: Any, adapter: AdapterFn):
        endpoint = f"{BINANCE_SPOT_TESTNET_WS_BASE_URL}/{symbol.to_exchange_str().lower()}@bookTicker"
        super().__init__(StreamType.BOOK_TICKER, endpoint, transport, adapter)


class BinanceSpotTestnetBnbTickerChannel(ReconnectableStreamChannel):
    def __init__(self, transport: Any, adapter: AdapterFn):
        endpoint = f"{BINANCE_SPOT_TESTNET_WS_BASE_URL}/bnbusdt@bookTicker"
        super().__init__(StreamType.BNB_TICKER, endpoint, transport, adapter)


class BinanceSpotTestnetUserDataChannel(ReconnectableStreamChannel):
    def __init__(self, executor: BinanceSpotTestnetAdapterV1, transport: Any, adapter: AdapterFn):
        endpoint = BINANCE_SPOT_TESTNET_WS_API_BASE_URL
        super().__init__(StreamType.USER_DATA, endpoint, transport, adapter)
        self._executor = executor
        self.subscription_id: Optional[Any] = None

    async def connect(self) -> None:
        if isinstance(self.transport, WebSocketJsonTransport):
            await self.transport.connect(self.endpoint)
        else:
            await self.transport.connect()
        self.state.is_connected = True
        if isinstance(self.transport, WebSocketJsonTransport) or getattr(self.transport, "ws_api_user_data", False):
            await self._authenticate_and_subscribe()

    async def disconnect(self) -> None:
        if self.subscription_id is not None:
            unsubscribe_request_id = uuid4().hex
            try:
                await self.transport.publish(
                    self._executor.build_user_data_ws_unsubscribe_request(
                        unsubscribe_request_id,
                        self.subscription_id,
                    )
                )
                response = await self.transport.receive()
                if not self._executor.is_user_data_ws_success_response(response, unsubscribe_request_id):
                    raise RuntimeError("user-data WebSocket API unsubscribe failed")
            except Exception:
                pass
            self.subscription_id = None
        await self.transport.disconnect()
        self.state.is_connected = False

    async def receive(self) -> Any | None:
        try:
            payload = await self.transport.receive()
        except Exception:
            await self.reconnect()
            return None
        if payload is None:
            return None
        normalized_payload = self._executor.unwrap_user_data_ws_payload(payload)
        if normalized_payload is None:
            return None
        return self.adapter(normalized_payload)

    async def _authenticate_and_subscribe(self) -> None:
        subscribe_request_id = uuid4().hex
        await self.transport.publish(self._executor.build_user_data_ws_subscribe_signature_request(subscribe_request_id))
        subscribe_response = await self.transport.receive()
        if not self._executor.is_user_data_ws_success_response(subscribe_response, subscribe_request_id):
            raise RuntimeError("user-data WebSocket API subscribe.signature failed")
        self.subscription_id = self._executor.extract_user_data_ws_subscription_id(subscribe_response)
