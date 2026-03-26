from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from mctp.adapters import BinanceCredentials, BinanceSpotTestnetAdapterV1, BinanceSpotTestnetConfigV1
from mctp.core.constants import (
    BNB_TICKER_STALE_SECONDS,
    BOOK_TICKER_STALE_SECONDS,
    CRITICAL_RESTART_OUTSTANDING_ORDER_CODE,
    INFO_STALE_BNBUSDT_CODE,
    INFO_STALE_BOOK_TICKER_CODE,
    KLINE_STALE_SECONDS,
    USER_DATA_STALE_SECONDS,
    WARNING_STALE_KLINE_CODE,
)
from mctp.core.enums import AlertSeverity, ExecutionResult, IntentType, Market, ProtectionMode, QuantityMode, Timeframe
from mctp.core.types import Intent, Symbol
from mctp.runtime.streams import QueueStreamTransport
from mctp.runtime.strategy_smoke import EmaCrossSmokeStrategy
from mctp.runtime.testnet import TestnetRuntime, TestnetRuntimeConfig, TestnetRuntimeStatus as RuntimeStatus
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.order_store import OrderStore
from mctp.storage.snapshot_store import SnapshotStore
from mctp.streams.base import StreamType


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
START = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone.utc)


class ChaosRestClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, object], bool]] = []
        self.account_balances = {"BTC": Decimal("0"), "USDT": Decimal("1000"), "BNB": Decimal("1")}
        self.locked_balances = {"BTC": Decimal("0"), "USDT": Decimal("0"), "BNB": Decimal("0")}
        self.order_submit_status = "NEW"
        self.order_snapshots: dict[str, dict[str, object]] = {}
        self.open_oco_order_ids: list[str] = []
        self.open_orders: list[dict[str, object]] = []

    async def request_json(self, method, path, *, params=None, signed=False):
        safe_params = dict(params or {})
        self.calls.append((method, path, safe_params, signed))
        if path == "/api/v3/order" and method == "POST":
            return {"status": self.order_submit_status}
        if path == "/api/v3/order" and method == "DELETE":
            return {"status": "CANCELED"}
        if path == "/api/v3/order" and method == "GET":
            client_order_id = str(safe_params.get("origClientOrderId", ""))
            return dict(self.order_snapshots.get(client_order_id, {}))
        if path == "/api/v3/orderList/oco":
            return {"orderListId": "oco-chaos-1"}
        if path == "/api/v3/orderList" and method == "DELETE":
            return {"listStatusType": "ALL_DONE"}
        if path == "/api/v3/userDataStream" and method == "POST":
            return {"listenKey": "listen-key"}
        if path == "/api/v3/userDataStream" and method in {"PUT", "DELETE"}:
            return {}
        if path == "/api/v3/openOrderList":
            return [{"orderListId": value} for value in self.open_oco_order_ids]
        if path == "/api/v3/openOrders":
            return list(self.open_orders)
        if path == "/api/v3/account":
            return {
                "balances": [
                    {
                        "asset": asset,
                        "free": str(amount),
                        "locked": str(self.locked_balances.get(asset, Decimal("0"))),
                    }
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
        raise AssertionError(f"Unhandled chaos REST call: {method} {path}")


def _runtime(tmp_path, rest_client: ChaosRestClient, *, root_name: str) -> TestnetRuntime:
    root = tmp_path / root_name
    root.mkdir(exist_ok=True)
    adapter = BinanceSpotTestnetAdapterV1(
        BinanceSpotTestnetConfigV1(BinanceCredentials(api_key="k", api_secret="s")),
        rest_client=rest_client,
        balance_cache_store=BalanceCacheStore(str(root / "balances.json")),
        order_store=OrderStore(str(root / "orders.json")),
    )
    return TestnetRuntime(
        config=TestnetRuntimeConfig(
            symbol=BTCUSDT,
            timeframe=Timeframe.M15,
            instrument_info={
                "lot_size": Decimal("0.001"),
                "min_qty": Decimal("0.001"),
                "max_qty": Decimal("1000"),
                "min_notional": Decimal("10"),
            },
            initial_balances={"BTC": Decimal("0"), "USDT": Decimal("1000"), "BNB": Decimal("1")},
        ),
        strategy=EmaCrossSmokeStrategy(),
        executor=adapter,
        snapshot_store=SnapshotStore(str(root / "snapshot.json")),
        balance_cache_store=BalanceCacheStore(str(root / "balances.json")),
        accounting_store=AccountingStore(str(root / "accounting.json")),
        kline_transport=QueueStreamTransport(),
        book_transport=QueueStreamTransport(),
        bnb_transport=QueueStreamTransport(),
        user_transport=QueueStreamTransport(),
    )


def _millis(timestamp: datetime) -> int:
    return int(timestamp.timestamp() * 1000)


def _kline_payload(timestamp: datetime, close: Decimal) -> dict[str, object]:
    return {
        "k": {
            "T": _millis(timestamp),
            "i": Timeframe.M15.value,
            "o": str(close),
            "h": str(close),
            "l": str(close),
            "c": str(close),
            "v": "1",
            "x": True,
        }
    }


def _book_payload(timestamp: datetime, *, bid: Decimal, ask: Decimal) -> dict[str, object]:
    return {"E": _millis(timestamp), "b": str(bid), "a": str(ask)}


def _bnb_payload(timestamp: datetime, *, bid: Decimal, ask: Decimal) -> dict[str, object]:
    return {"E": _millis(timestamp), "b": str(bid), "a": str(ask)}


def _account_payload(
    timestamp: datetime,
    *,
    free_btc: Decimal = Decimal("0"),
    free_usdt: Decimal = Decimal("1000"),
    free_bnb: Decimal = Decimal("1"),
    locked_btc: Decimal = Decimal("0"),
) -> dict[str, object]:
    return {
        "e": "outboundAccountPosition",
        "E": _millis(timestamp),
        "B": [
            {"a": "BTC", "f": str(free_btc), "l": str(locked_btc)},
            {"a": "USDT", "f": str(free_usdt), "l": "0"},
            {"a": "BNB", "f": str(free_bnb), "l": "0"},
        ],
    }


def _execution_report_payload(
    timestamp: datetime,
    *,
    client_order_id: str,
    status: str,
    side: str = "BUY",
    last_qty: Decimal = Decimal("0"),
    last_price: Decimal = Decimal("0"),
) -> dict[str, object]:
    return {
        "e": "executionReport",
        "E": _millis(timestamp),
        "T": _millis(timestamp),
        "s": BTCUSDT.to_exchange_str(),
        "c": client_order_id,
        "S": side,
        "X": status,
        "l": str(last_qty),
        "L": str(last_price),
        "n": "0",
        "N": "QUOTE",
        "t": client_order_id,
    }


async def _publish_baseline(runtime: TestnetRuntime) -> None:
    await runtime.channels[StreamType.KLINE].publish(_kline_payload(START, Decimal("100")))
    await runtime.channels[StreamType.BOOK_TICKER].publish(
        _book_payload(START, bid=Decimal("99"), ask=Decimal("100"))
    )
    await runtime.channels[StreamType.BNB_TICKER].publish(
        _bnb_payload(START, bid=Decimal("299"), ask=Decimal("301"))
    )
    await runtime.channels[StreamType.USER_DATA].publish(_account_payload(START))
    await runtime.process_all_available()


@pytest.mark.asyncio
async def test_v17_chaos_kline_staleness_is_detected_independently_from_other_live_streams(tmp_path):
    runtime = _runtime(tmp_path, ChaosRestClient(), root_name="kline_independence")
    await runtime.start()
    try:
        await _publish_baseline(runtime)
        stale_now = START + timedelta(seconds=KLINE_STALE_SECONDS + 1)
        await runtime.channels[StreamType.BOOK_TICKER].publish(
            _book_payload(stale_now, bid=Decimal("101"), ask=Decimal("102"))
        )
        await runtime.channels[StreamType.BNB_TICKER].publish(
            _bnb_payload(stale_now, bid=Decimal("310"), ask=Decimal("312"))
        )
        await runtime.channels[StreamType.USER_DATA].publish(_account_payload(stale_now))
        await runtime.process_all_available()
        await runtime.evaluate_staleness(stale_now)

        assert runtime.channels[StreamType.KLINE].state.is_stale is True
        assert runtime.channels[StreamType.BOOK_TICKER].state.is_stale is False
        assert runtime.channels[StreamType.BNB_TICKER].state.is_stale is False
        assert runtime.channels[StreamType.USER_DATA].state.is_stale is False
        assert runtime.status == RuntimeStatus.HALT
        assert runtime.last_alert is not None
        assert runtime.last_alert.code == WARNING_STALE_KLINE_CODE
        assert runtime.current_bid == Decimal("101")
        assert runtime.current_bnb_price == Decimal("311")
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_chaos_book_ticker_staleness_does_not_contaminate_kline_bnb_or_user_data_state(tmp_path):
    runtime = _runtime(tmp_path, ChaosRestClient(), root_name="book_independence")
    await runtime.start()
    try:
        await _publish_baseline(runtime)
        stale_now = START + timedelta(seconds=BOOK_TICKER_STALE_SECONDS + 1)
        await runtime.channels[StreamType.KLINE].publish(_kline_payload(stale_now, Decimal("101")))
        await runtime.channels[StreamType.BNB_TICKER].publish(
            _bnb_payload(stale_now, bid=Decimal("320"), ask=Decimal("322"))
        )
        await runtime.channels[StreamType.USER_DATA].publish(_account_payload(stale_now))
        await runtime.process_all_available()
        before_snapshot = runtime.portfolio.snapshot
        await runtime.evaluate_staleness(stale_now)

        assert runtime.channels[StreamType.BOOK_TICKER].state.is_stale is True
        assert runtime.channels[StreamType.KLINE].state.is_stale is False
        assert runtime.channels[StreamType.BNB_TICKER].state.is_stale is False
        assert runtime.channels[StreamType.USER_DATA].state.is_stale is False
        assert runtime.last_alert is not None
        assert runtime.last_alert.code == INFO_STALE_BOOK_TICKER_CODE
        assert runtime.portfolio.snapshot == before_snapshot
        assert runtime.protection_mode == ProtectionMode.NONE
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_chaos_bnb_ticker_staleness_is_isolated_and_does_not_corrupt_accounting_or_position_state(tmp_path):
    runtime = _runtime(tmp_path, ChaosRestClient(), root_name="bnb_independence")
    await runtime.start()
    try:
        await _publish_baseline(runtime)
        stale_now = START + timedelta(seconds=BNB_TICKER_STALE_SECONDS + 1)
        await runtime.channels[StreamType.KLINE].publish(_kline_payload(stale_now, Decimal("102")))
        await runtime.channels[StreamType.BOOK_TICKER].publish(
            _book_payload(stale_now, bid=Decimal("102"), ask=Decimal("103"))
        )
        await runtime.channels[StreamType.USER_DATA].publish(_account_payload(stale_now))
        await runtime.process_all_available()
        before_snapshot = runtime.portfolio.snapshot
        before_history = list(runtime.portfolio.accounting.fill_history)
        await runtime.evaluate_staleness(stale_now)

        assert runtime.channels[StreamType.BNB_TICKER].state.is_stale is True
        assert runtime.channels[StreamType.KLINE].state.is_stale is False
        assert runtime.channels[StreamType.BOOK_TICKER].state.is_stale is False
        assert runtime.channels[StreamType.USER_DATA].state.is_stale is False
        assert runtime.last_alert is not None
        assert runtime.last_alert.code == INFO_STALE_BNBUSDT_CODE
        assert runtime.portfolio.snapshot == before_snapshot
        assert runtime.portfolio.accounting.fill_history == before_history
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_chaos_user_data_interruption_is_not_masked_by_healthy_market_streams_and_clears_cleanly_on_resume(tmp_path):
    runtime = _runtime(tmp_path, ChaosRestClient(), root_name="user_data_independence")
    await runtime.start()
    try:
        await _publish_baseline(runtime)
        runtime.current_bid = Decimal("99")
        runtime.current_ask = Decimal("100")
        runtime._order_quantity = lambda intent: (Decimal("0.1"), None)  # type: ignore[method-assign]
        await runtime._execute_intent(
            Intent(type=IntentType.BUY, symbol=BTCUSDT, quantity_mode=QuantityMode.FULL, reason="pending"),
            START,
        )
        pending_id = runtime.pending_order_client_id
        assert pending_id is not None
        assert pending_id in runtime.executor.load_local_active_orders()

        stale_now = START + timedelta(seconds=USER_DATA_STALE_SECONDS + 1)
        await runtime.channels[StreamType.KLINE].publish(_kline_payload(stale_now, Decimal("103")))
        await runtime.channels[StreamType.BOOK_TICKER].publish(
            _book_payload(stale_now, bid=Decimal("103"), ask=Decimal("104"))
        )
        await runtime.channels[StreamType.BNB_TICKER].publish(
            _bnb_payload(stale_now, bid=Decimal("330"), ask=Decimal("332"))
        )
        await runtime.process_all_available()
        await runtime.evaluate_staleness(stale_now)

        assert runtime.channels[StreamType.USER_DATA].state.is_stale is True
        assert runtime.channels[StreamType.KLINE].state.is_stale is False
        assert runtime.channels[StreamType.BOOK_TICKER].state.is_stale is False
        assert runtime.channels[StreamType.BNB_TICKER].state.is_stale is False
        assert runtime.pending_order_client_id == pending_id
        assert pending_id in runtime.executor.load_local_active_orders()
        assert runtime.active_oco_order_id is None
        assert runtime.protection_mode == ProtectionMode.NONE

        resume_at = stale_now + timedelta(seconds=1)
        await runtime.channels[StreamType.USER_DATA].publish(
            _execution_report_payload(
                resume_at,
                client_order_id=pending_id,
                status="CANCELED",
                last_qty=Decimal("0"),
                last_price=Decimal("0"),
            )
        )
        await runtime.process_all_available()
        await runtime.evaluate_staleness(resume_at)

        assert runtime.channels[StreamType.USER_DATA].state.is_stale is False
        assert runtime.pending_order_client_id is None
        assert runtime.pending_order_side is None
        assert runtime.executor.load_local_active_orders() == {}
        assert runtime.portfolio.snapshot.held_qty == Decimal("0")
        assert runtime.portfolio.accounting.fill_history == []
        assert runtime.active_oco_order_id is None
        assert runtime.protection_mode == ProtectionMode.NONE
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_chaos_isolated_book_ticker_recovery_clears_only_its_own_stale_flag_without_false_cross_stream_recovery(tmp_path):
    runtime = _runtime(tmp_path, ChaosRestClient(), root_name="mixed_recovery")
    await runtime.start()
    try:
        await _publish_baseline(runtime)
        stale_now = START + timedelta(seconds=BOOK_TICKER_STALE_SECONDS + 1)
        await runtime.channels[StreamType.KLINE].publish(_kline_payload(stale_now, Decimal("104")))
        await runtime.channels[StreamType.BNB_TICKER].publish(
            _bnb_payload(stale_now, bid=Decimal("340"), ask=Decimal("342"))
        )
        await runtime.channels[StreamType.USER_DATA].publish(_account_payload(stale_now))
        await runtime.process_all_available()
        await runtime.evaluate_staleness(stale_now)
        assert runtime.channels[StreamType.BOOK_TICKER].state.is_stale is True

        resume_at = stale_now + timedelta(seconds=1)
        await runtime.channels[StreamType.BOOK_TICKER].publish(
            _book_payload(resume_at, bid=Decimal("104"), ask=Decimal("105"))
        )
        await runtime.process_all_available()
        await runtime.evaluate_staleness(resume_at)

        assert runtime.channels[StreamType.BOOK_TICKER].state.is_stale is False
        assert runtime.channels[StreamType.KLINE].state.is_stale is False
        assert runtime.channels[StreamType.BNB_TICKER].state.is_stale is False
        assert runtime.channels[StreamType.USER_DATA].state.is_stale is False
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_chaos_user_data_gap_then_restart_keeps_outstanding_order_reconciliation_conservative(tmp_path):
    rest = ChaosRestClient()
    runtime_a = _runtime(tmp_path, rest, root_name="restart_after_user_gap")
    await runtime_a.start()
    try:
        await _publish_baseline(runtime_a)
        runtime_a.current_bid = Decimal("99")
        runtime_a.current_ask = Decimal("100")
        runtime_a._order_quantity = lambda intent: (Decimal("0.1"), None)  # type: ignore[method-assign]
        await runtime_a._execute_intent(
            Intent(type=IntentType.BUY, symbol=BTCUSDT, quantity_mode=QuantityMode.FULL, reason="restart-gap"),
            START,
        )
        pending_id = runtime_a.pending_order_client_id
        assert pending_id is not None
        rest.order_snapshots[pending_id] = {
            "status": "NEW",
            "side": "BUY",
            "executedQty": "0",
            "cummulativeQuoteQty": "0",
            "price": "100",
            "updateTime": _millis(START),
        }
    finally:
        await runtime_a.shutdown()

    runtime_b = _runtime(tmp_path, rest, root_name="restart_after_user_gap")
    await runtime_b.start()
    try:
        assert runtime_b.status == RuntimeStatus.HALT
        assert runtime_b.pending_order_client_id == pending_id
        assert runtime_b.last_alert is not None
        assert runtime_b.last_alert.severity == AlertSeverity.CRITICAL
        assert runtime_b.last_alert.code == CRITICAL_RESTART_OUTSTANDING_ORDER_CODE
        assert runtime_b.executor.load_local_active_orders().get(pending_id) is not None
        assert runtime_b.active_oco_order_id is None
        assert runtime_b.protection_mode == ProtectionMode.NONE
    finally:
        await runtime_b.shutdown()
