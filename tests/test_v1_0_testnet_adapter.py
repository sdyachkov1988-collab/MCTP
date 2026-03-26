import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mctp.adapters import (
    BinanceCredentials,
    BinanceDelistingDetectorV1,
    BinanceSpotTestnetAdapterV1,
    BinanceSpotTestnetConfigV1,
)
from mctp.core.constants import (
    ASSET_FDUSD,
    EXCHANGE_WS_API_METHOD_USER_DATA_SUBSCRIBE_SIGNATURE,
    EXCHANGE_WS_API_METHOD_USER_DATA_UNSUBSCRIBE,
    EXCHANGE_USER_DATA_EVENT_EXECUTION_REPORT,
)
from mctp.core.enums import ExecutionResult, Market, OrderType, Side, Timeframe
from mctp.core.order import Order
from mctp.core.types import Symbol
from mctp.execution.oco import OCOOrder
from mctp.runtime.events import ExecutionReportEvent
from mctp.runtime.strategy_smoke import EmaCrossSmokeStrategy
from mctp.runtime.streams import QueueStreamTransport, WebSocketJsonTransport
from mctp.runtime.testnet_adapters import adapt_binance_testnet_payload
from mctp.runtime.testnet_exchange_boundary import parse_exchange_spot_symbol
from mctp.runtime.testnet import TestnetRuntime, TestnetRuntimeConfig
from mctp.storage.order_store import OrderStore
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.snapshot_store import SnapshotStore
from mctp.streams.base import StreamType


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
START = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone.utc)


class FakeRestClient:
    def __init__(self) -> None:
        self.calls = []
        self.account_balances = {"BTC": Decimal("0.25"), "USDT": Decimal("1000")}
        self.order_submit_status = "NEW"

    async def request_json(self, method, path, *, params=None, signed=False):
        params = params or {}
        self.calls.append((method, path, dict(params), signed))
        if path == "/api/v3/order" and method == "POST":
            return {"status": self.order_submit_status}
        if path == "/api/v3/order" and method == "DELETE":
            return {"status": "CANCELED"}
        if path == "/api/v3/userDataStream" and method == "POST":
            return {"listenKey": "listen-key"}
        if path == "/api/v3/userDataStream" and method in {"PUT", "DELETE"}:
            return {}
        if path == "/api/v3/orderList/oco":
            return {"orderListId": "oco-1"}
        if path == "/api/v3/openOrderList":
            return []
        if path == "/api/v3/account":
            return {
                "balances": [
                    {"asset": asset, "free": str(amount)}
                    for asset, amount in self.account_balances.items()
                ]
            }
        if path == "/api/v3/exchangeInfo":
            return {
                "symbols": [
                    {
                        "status": "TRADING",
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "1000"},
                            {"filterType": "MIN_NOTIONAL", "minNotional": "10"},
                        ],
                    }
                ]
            }
        raise AssertionError(f"Unhandled fake REST call: {method} {path}")


class FailingOnceTransport(QueueStreamTransport):
    def __init__(self, payload):
        super().__init__()
        self._payload = payload
        self._failed = False

    async def receive(self):
        if not self._failed:
            self._failed = True
            raise RuntimeError("transient disconnect")
        if self._payload is None:
            return None
        payload = self._payload
        self._payload = None
        return payload


class RecordingWsApiTransport(QueueStreamTransport):
    ws_api_user_data = True

    def __init__(self, responses):
        super().__init__()
        self._responses = list(responses)
        self.published_messages = []

    async def publish(self, event):
        self.published_messages.append(event)

    async def receive(self):
        if not self._responses:
            return None
        response = self._responses.pop(0)
        if (
            isinstance(response, dict)
            and response.get("id") == "__LAST_REQUEST_ID__"
            and self.published_messages
            and isinstance(self.published_messages[-1], dict)
        ):
            response = dict(response)
            response["id"] = self.published_messages[-1]["id"]
        return response


def _config():
    return BinanceSpotTestnetConfigV1(
        credentials=BinanceCredentials(api_key="k", api_secret="s")
    )


def _runtime(tmp_path, rest_client: FakeRestClient, **transports):
    config = TestnetRuntimeConfig(
        symbol=BTCUSDT,
        timeframe=Timeframe.M15,
        instrument_info={
            "lot_size": Decimal("0.001"),
            "min_qty": Decimal("0.001"),
            "max_qty": Decimal("1000"),
            "min_notional": Decimal("10"),
        },
        initial_balances={"BTC": Decimal("0"), "USDT": Decimal("1000")},
        listen_key_keepalive_seconds=transports.get("listen_key_keepalive_seconds", 1800),
    )
    adapter = BinanceSpotTestnetAdapterV1(
        _config(),
        rest_client=rest_client,
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        order_store=OrderStore(str(tmp_path / "orders.json")),
    )
    runtime = TestnetRuntime(
        config=config,
        strategy=EmaCrossSmokeStrategy(),
        executor=adapter,
        snapshot_store=SnapshotStore(str(tmp_path / "snapshot.json")),
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        accounting_store=AccountingStore(str(tmp_path / "accounting.json")),
        kline_transport=transports.get("kline_transport"),
        book_transport=transports.get("book_transport"),
        bnb_transport=transports.get("bnb_transport"),
        user_transport=transports.get("user_transport"),
    )
    return adapter, runtime


@pytest.mark.asyncio
async def test_real_adapter_boundary_is_introduced_without_breaking_paper_path():
    adapter = BinanceSpotTestnetAdapterV1(_config(), rest_client=FakeRestClient())
    order = Order(symbol=BTCUSDT, side=Side.BUY, order_type=OrderType.MARKET, quantity=Decimal("0.01"), created_at=START)
    result = await adapter.submit_order(order)
    assert result == ExecutionResult.ACCEPTED
    from mctp.execution.paper import SpotPaperExecutor

    paper = SpotPaperExecutor(initial_balances={"BTC": Decimal("0"), "USDT": Decimal("1000")})
    assert paper is not None


def test_real_testnet_runtime_defaults_to_real_websocket_transports(tmp_path):
    _, runtime = _runtime(tmp_path, FakeRestClient())
    assert isinstance(runtime._kline_transport, WebSocketJsonTransport)
    assert isinstance(runtime._book_transport, WebSocketJsonTransport)
    assert isinstance(runtime._bnb_transport, WebSocketJsonTransport)
    assert isinstance(runtime._user_transport, WebSocketJsonTransport)


@pytest.mark.asyncio
async def test_typed_symbol_is_preserved_above_adapter_boundary():
    rest = FakeRestClient()
    adapter = BinanceSpotTestnetAdapterV1(_config(), rest_client=rest)
    await adapter.get_instrument_info(BTCUSDT)
    assert rest.calls[-1][2]["symbol"] == "BTCUSDT"
    assert isinstance(BTCUSDT, Symbol)


def test_authenticated_private_stream_path_is_wired():
    event = adapt_binance_testnet_payload(
        StreamType.USER_DATA,
        {
            "e": EXCHANGE_USER_DATA_EVENT_EXECUTION_REPORT,
            "E": 1774310400000,
            "T": 1774310400000,
            "s": "BTCUSDT",
            "c": "order-1",
            "X": "FILLED",
            "S": "BUY",
            "l": "0.01",
            "L": "100000",
            "n": "1",
            "N": "QUOTE",
            "i": "exchange-order-1",
            "t": "trade-1",
        },
    )
    assert isinstance(event, ExecutionReportEvent)
    assert event.fill is not None
    assert event.symbol == BTCUSDT


def test_exchange_symbol_boundary_supports_configured_quote_suffixes():
    parsed = parse_exchange_spot_symbol(f"ETH{ASSET_FDUSD}")
    assert parsed == Symbol("ETH", ASSET_FDUSD, Market.SPOT)


def test_exchange_symbol_boundary_rejects_unsupported_symbols_explicitly():
    with pytest.raises(ValueError, match="Unsupported exchange symbol"):
        parse_exchange_spot_symbol("BTCXYZ")


@pytest.mark.asyncio
async def test_four_streams_have_independent_lifecycle_and_reconnect_behavior(tmp_path):
    rest = FakeRestClient()
    adapter, runtime = _runtime(
        tmp_path,
        rest,
        kline_transport=FailingOnceTransport(
            {"e": "kline", "E": 1774310400000, "k": {"t": 1774310400000, "i": "15m", "o": "100", "h": "101", "l": "99", "c": "100", "v": "10", "x": True}}
        ),
        book_transport=QueueStreamTransport(),
        bnb_transport=QueueStreamTransport(),
        user_transport=QueueStreamTransport(),
    )
    await runtime.start()
    try:
        await runtime.process_all_available()
        reconnect_calls = [call for call in rest.calls if call[1] == "/api/v3/account"]
        assert len(reconnect_calls) == 2
        assert runtime.channels[StreamType.BOOK_TICKER].state.is_connected is True
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_user_data_ws_api_subscription_replaces_legacy_listen_key_startup(tmp_path):
    rest = FakeRestClient()
    user_transport = RecordingWsApiTransport(
        [
            {"id": "__LAST_REQUEST_ID__", "status": 200, "result": {"subscriptionId": "sub-1"}},
        ]
    )
    _, runtime = _runtime(
        tmp_path,
        rest,
        kline_transport=QueueStreamTransport(),
        book_transport=QueueStreamTransport(),
        bnb_transport=QueueStreamTransport(),
        user_transport=user_transport,
    )
    await runtime.start()
    try:
        listen_key_calls = [call for call in rest.calls if call[1] == "/api/v3/userDataStream"]
        assert listen_key_calls == []
        methods = [message["method"] for message in user_transport.published_messages[:1]]
        assert methods == [EXCHANGE_WS_API_METHOD_USER_DATA_SUBSCRIBE_SIGNATURE]
        subscribe_request = user_transport.published_messages[0]
        assert subscribe_request["params"]["apiKey"] == "k"
        assert "timestamp" in subscribe_request["params"]
        assert "signature" in subscribe_request["params"]
        assert runtime.channels[StreamType.USER_DATA].subscription_id == "sub-1"
    finally:
        await runtime.shutdown()
    shutdown_methods = [message["method"] for message in user_transport.published_messages[1:]]
    assert shutdown_methods == [EXCHANGE_WS_API_METHOD_USER_DATA_UNSUBSCRIBE]


@pytest.mark.asyncio
async def test_user_data_ws_api_event_wrapper_still_routes_into_existing_runtime_handler(tmp_path):
    rest = FakeRestClient()
    adapter, runtime = _runtime(
        tmp_path,
        rest,
        kline_transport=QueueStreamTransport(),
        book_transport=QueueStreamTransport(),
        bnb_transport=QueueStreamTransport(),
        user_transport=RecordingWsApiTransport(
            [
                {"id": "__LAST_REQUEST_ID__", "status": 200, "result": {"subscriptionId": "sub-7"}},
                {
                    "subscriptionId": "sub-7",
                    "event": {
                        "e": "outboundAccountPosition",
                        "E": 1774310400000,
                        "B": [{"a": "BTC", "f": "0.25"}, {"a": "USDT", "f": "950"}],
                    },
                },
            ]
        ),
    )
    await runtime.start()
    try:
        await runtime.process_all_available()
        balances = await adapter.get_balances()
        assert balances["BTC"] == Decimal("0.25")
        assert balances["USDT"] == Decimal("950")
    finally:
        await runtime.shutdown()


def test_hmac_user_data_subscription_request_shape_uses_subscribe_signature():
    adapter = BinanceSpotTestnetAdapterV1(_config(), rest_client=FakeRestClient())

    request = adapter.build_user_data_ws_subscribe_signature_request("req-1")

    assert request["id"] == "req-1"
    assert request["method"] == EXCHANGE_WS_API_METHOD_USER_DATA_SUBSCRIBE_SIGNATURE
    assert request["params"]["apiKey"] == "k"
    assert "timestamp" in request["params"]
    assert "signature" in request["params"]
    assert "recvWindow" not in request["params"]


@pytest.mark.asyncio
async def test_oco_path_uses_real_adapter_api_and_preserves_tp_limit_rule():
    rest = FakeRestClient()
    adapter = BinanceSpotTestnetAdapterV1(_config(), rest_client=rest)
    await adapter.submit_oco(
        OCOOrder(
            symbol=BTCUSDT,
            tp_price=Decimal("110"),
            sl_stop_price=Decimal("95"),
            sl_limit_price=Decimal("94"),
            quantity=Decimal("0.5"),
            created_at=START,
            updated_at=START,
        )
    )
    _, path, params, _ = rest.calls[-1]
    assert path == "/api/v3/orderList/oco"
    assert params["aboveType"] == "LIMIT"


@pytest.mark.asyncio
async def test_account_rest_usage_is_bounded_to_allowed_scenarios():
    rest = FakeRestClient()
    adapter = BinanceSpotTestnetAdapterV1(_config(), rest_client=rest)
    await adapter.refresh_account_snapshot("startup")
    await adapter.refresh_account_snapshot("reconnect")
    with pytest.raises(ValueError):
        await adapter.refresh_account_snapshot("loop")
    account_calls = [call for call in rest.calls if call[1] == "/api/v3/account"]
    assert len(account_calls) == 2


@pytest.mark.asyncio
async def test_open_order_list_request_shape_omits_symbol_and_filters_locally():
    rest = FakeRestClient()
    rest.account_balances["FDUSD"] = Decimal("0")

    async def _request_json(method, path, *, params=None, signed=False):
        rest.calls.append((method, path, dict(params or {}), signed))
        if path == "/api/v3/openOrderList":
            return [
                {"symbol": "BTCUSDT", "orderListId": "btc-oco-1"},
                {"symbol": f"ETH{ASSET_FDUSD}", "orderListId": "eth-oco-1"},
                {"symbol": "BTCUSDT", "orderListId": "btc-oco-2"},
            ]
        raise AssertionError(f"Unhandled fake REST call: {method} {path}")

    rest.request_json = _request_json  # type: ignore[method-assign]
    adapter = BinanceSpotTestnetAdapterV1(_config(), rest_client=rest)

    order_list_ids = await adapter.get_open_oco_order_ids(BTCUSDT)

    assert order_list_ids == ["btc-oco-1", "btc-oco-2"]
    assert rest.calls == [("GET", "/api/v3/openOrderList", {}, True)]


@pytest.mark.asyncio
async def test_open_order_list_empty_or_malformed_response_returns_empty_list():
    rest = FakeRestClient()
    adapter = BinanceSpotTestnetAdapterV1(_config(), rest_client=rest)

    async def _non_list(method, path, *, params=None, signed=False):
        return {"unexpected": True}

    rest.request_json = _non_list  # type: ignore[method-assign]
    assert await adapter.get_open_oco_order_ids(BTCUSDT) == []

    async def _malformed_list(method, path, *, params=None, signed=False):
        return ["bad", {"symbol": "ETHUSDT"}, {"symbol": "ETHUSDT", "orderListId": "other"}]

    rest.request_json = _malformed_list  # type: ignore[method-assign]
    assert await adapter.get_open_oco_order_ids(BTCUSDT) == []


@pytest.mark.asyncio
async def test_open_order_list_legacy_payload_without_symbol_remains_usable_for_recovery_fixtures():
    rest = FakeRestClient()

    async def _legacy_payload(method, path, *, params=None, signed=False):
        rest.calls.append((method, path, dict(params or {}), signed))
        if path == "/api/v3/openOrderList":
            return [{"orderListId": "legacy-oco-1"}]
        raise AssertionError(f"Unhandled fake REST call: {method} {path}")

    rest.request_json = _legacy_payload  # type: ignore[method-assign]
    adapter = BinanceSpotTestnetAdapterV1(_config(), rest_client=rest)

    assert await adapter.get_open_oco_order_ids(BTCUSDT) == ["legacy-oco-1"]


@pytest.mark.asyncio
async def test_delisting_detection_path_exists_and_is_test_covered():
    class DelistingRestClient(FakeRestClient):
        async def request_json(self, method, path, *, params=None, signed=False):
            if path == "/api/v3/exchangeInfo":
                return {"symbols": [{"status": "BREAK"}]}
            return await super().request_json(method, path, params=params, signed=signed)

    detector = BinanceDelistingDetectorV1(_config(), rest_client=DelistingRestClient(), rss_fetcher=lambda: "BTCUSDT delist")
    signal = await detector.check_symbol(BTCUSDT)
    assert signal.listed is False
    assert "delist" in signal.details.lower() or "status" in signal.details.lower()


@pytest.mark.asyncio
async def test_delisting_detection_is_invoked_through_runtime_plumbing(tmp_path):
    class FakeDetector:
        def __init__(self) -> None:
            self.calls = 0

        async def check_symbol(self, symbol):
            self.calls += 1
            return await BinanceDelistingDetectorV1(_config(), rest_client=FakeRestClient(), rss_fetcher=lambda: "").check_symbol(symbol)

    rest = FakeRestClient()
    detector = FakeDetector()
    _, runtime = _runtime(
        tmp_path,
        rest,
        kline_transport=QueueStreamTransport(),
        book_transport=QueueStreamTransport(),
        bnb_transport=QueueStreamTransport(),
        user_transport=QueueStreamTransport(),
    )
    runtime.detector = detector
    await runtime.start()
    try:
        assert detector.calls == 1
        assert runtime.last_delisting_signal is not None
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_private_balance_events_flow_into_adapter_cache(tmp_path):
    rest = FakeRestClient()
    adapter, runtime = _runtime(
        tmp_path,
        rest,
        kline_transport=QueueStreamTransport(),
        book_transport=QueueStreamTransport(),
        bnb_transport=QueueStreamTransport(),
        user_transport=QueueStreamTransport(),
    )
    await runtime.start()
    try:
        await runtime.channels[StreamType.USER_DATA].publish(
            {
                "e": "outboundAccountPosition",
                "E": 1774310400000,
                "B": [{"a": "BTC", "f": "0.25"}, {"a": "USDT", "f": "950"}],
            }
        )
        await runtime.process_all_available()
        balances = await adapter.get_balances()
        assert balances["BTC"] == Decimal("0.25")
        assert balances["USDT"] == Decimal("950")
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_rejected_orders_are_not_retained_as_active_local_orders(tmp_path):
    rest = FakeRestClient()
    rest.order_submit_status = "REJECTED"
    order_store = OrderStore(str(tmp_path / "orders.json"))
    adapter = BinanceSpotTestnetAdapterV1(
        _config(),
        rest_client=rest,
        order_store=order_store,
    )
    order = Order(symbol=BTCUSDT, side=Side.BUY, order_type=OrderType.MARKET, quantity=Decimal("0.01"), created_at=START)
    result = await adapter.submit_order(order)
    active_orders, _ = order_store.load()
    assert result == ExecutionResult.REJECTED
    assert active_orders == {}
