import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from mctp.adapters import BinanceCredentials, BinanceSpotTestnetAdapterV1, BinanceSpotTestnetConfigV1
from mctp.core.constants import (
    CRITICAL_BACKGROUND_TASK_FAILURE_CODE,
    CRITICAL_DIRECT_SELL_OCO_CANCEL_FAILED_CODE,
    CRITICAL_RESTART_OUTSTANDING_ORDER_CODE,
    CRITICAL_RESTART_PARTIAL_FILL_CODE,
    CRITICAL_STALE_USER_DATA_CODE,
    CRITICAL_STARTUP_OCO_AMBIGUITY_CODE,
    EXECUTION_STATE_RETENTION_SECONDS,
    USER_DATA_STALE_SECONDS,
)
from mctp.core.enums import AlertSeverity, CommissionAsset, ExchangeOrderStatus, ExecutionResult, IntentType, Market, OrderType, ProtectionMode, QuantityMode, Side, Timeframe
from mctp.core.order import Fill, Order
from mctp.core.types import Intent, Symbol
from mctp.execution.oco import OCOOrder
from mctp.runtime.events import ExecutionReportEvent, OutboundAccountPositionEvent
from mctp.runtime.strategy_smoke import EmaCrossSmokeStrategy
from mctp.runtime.streams import QueueStreamTransport
from mctp.runtime.testnet import TestnetRuntime, TestnetRuntimeConfig, TestnetRuntimeStatus as RuntimeStatus
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.order_store import OrderStore
from mctp.storage.snapshot_store import SnapshotStore
from mctp.streams.base import StreamType


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
START = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone.utc)


class FakeRestClient:
    def __init__(self) -> None:
        self.calls = []
        self.account_balances = {"BTC": Decimal("0.5"), "USDT": Decimal("1000"), "BNB": Decimal("1")}
        self.locked_balances = {"BTC": Decimal("0"), "USDT": Decimal("0"), "BNB": Decimal("0")}
        self.open_oco_order_ids: list[str] = []
        self.open_orders: list[dict[str, object]] = []
        self.order_snapshots: dict[str, dict[str, object]] = {}
        self.order_trades: dict[str, list[dict[str, object]]] = {}
        self.order_submit_status = "NEW"
        self.order_list_cancel_status = "ALL_DONE"

    async def request_json(self, method, path, *, params=None, signed=False):
        safe_params = dict(params or {})
        self.calls.append((method, path, safe_params, signed))
        if path == "/api/v3/order" and method == "POST":
            return {"status": self.order_submit_status}
        if path == "/api/v3/order" and method == "GET":
            client_order_id = str(safe_params.get("origClientOrderId", ""))
            return dict(self.order_snapshots.get(client_order_id, {}))
        if path == "/api/v3/myTrades":
            client_order_id = str(safe_params.get("origClientOrderId", ""))
            return list(self.order_trades.get(client_order_id, []))
        if path == "/api/v3/order" and method == "DELETE":
            return {"status": "CANCELED"}
        if path == "/api/v3/userDataStream" and method == "POST":
            return {"listenKey": "listen-key"}
        if path == "/api/v3/userDataStream" and method in {"PUT", "DELETE"}:
            return {}
        if path == "/api/v3/orderList/oco":
            return {"orderListId": "oco-1"}
        if path == "/api/v3/orderList" and method == "DELETE":
            return {"listStatusType": self.order_list_cancel_status}
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
        raise AssertionError(f"Unhandled fake REST call: {method} {path}")


class FakeDelistingDetector:
    def __init__(self, days_until_delisting: int | None) -> None:
        self.days_until_delisting = days_until_delisting

    async def check_symbol(self, symbol: Symbol):
        from mctp.runtime.events import DelistingSignalEvent

        return DelistingSignalEvent(
            symbol=symbol,
            listed=False,
            source="fake",
            details="delisting scheduled",
            days_until_delisting=self.days_until_delisting,
        )


class _ChannelStub:
    def __init__(self) -> None:
        self.state = type("_State", (), {"is_stale": False})()

    def touch(self, _timestamp) -> None:
        return


def _adapter(rest_client: FakeRestClient, tmp_path) -> BinanceSpotTestnetAdapterV1:
    return BinanceSpotTestnetAdapterV1(
        BinanceSpotTestnetConfigV1(BinanceCredentials(api_key="k", api_secret="s")),
        rest_client=rest_client,
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        order_store=OrderStore(str(tmp_path / "orders.json")),
    )


def _runtime(tmp_path, rest_client: FakeRestClient, **config_overrides) -> TestnetRuntime:
    adapter = _adapter(rest_client, tmp_path)
    runtime = TestnetRuntime(
        config=TestnetRuntimeConfig(
            symbol=BTCUSDT,
            timeframe=Timeframe.M15,
            instrument_info={
                "lot_size": Decimal("0.001"),
                "min_qty": Decimal("0.001"),
                "max_qty": Decimal("1000"),
                "min_notional": Decimal("10"),
            },
            initial_balances={"BTC": Decimal("0"), "USDT": Decimal("1000")},
            heartbeat_interval_seconds=config_overrides.get("heartbeat_interval_seconds", 1),
            heartbeat_watchdog_interval_seconds=config_overrides.get("heartbeat_watchdog_interval_seconds", 1),
            listen_key_keepalive_seconds=config_overrides.get("listen_key_keepalive_seconds", 1800),
        ),
        strategy=EmaCrossSmokeStrategy(),
        executor=adapter,
        snapshot_store=SnapshotStore(str(tmp_path / "snapshot.json")),
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        accounting_store=AccountingStore(str(tmp_path / "accounting.json")),
        kline_transport=QueueStreamTransport(),
        book_transport=QueueStreamTransport(),
        bnb_transport=QueueStreamTransport(),
        user_transport=QueueStreamTransport(),
    )
    runtime.portfolio.replace_snapshot(avg_cost_basis=Decimal("100"))
    return runtime


def _attach_user_channel(runtime: TestnetRuntime) -> None:
    runtime.channels[StreamType.USER_DATA] = _ChannelStub()  # type: ignore[assignment]


@pytest.mark.asyncio
async def test_websocket_filled_status_is_not_regressed_by_weaker_rest_submit_state(tmp_path):
    rest = FakeRestClient()
    adapter = _adapter(rest, tmp_path)
    order = Order(
        client_order_id="cid-1",
        symbol=BTCUSDT,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.01"),
        created_at=START,
    )
    adapter.handle_user_data_event(
        ExecutionReportEvent(
            timestamp=START,
            symbol=BTCUSDT,
            client_order_id="cid-1",
            execution_result=ExecutionResult.FILLED,
            order_status=ExchangeOrderStatus.FILLED,
            fill=None,
        )
    )
    rest.order_submit_status = "NEW"
    result = await adapter.submit_order(order)
    assert result == ExecutionResult.ACCEPTED
    assert await adapter.get_order_status("cid-1") == "FILLED"


def test_terminal_execution_state_entries_are_pruned_after_retention_window(tmp_path):
    rest = FakeRestClient()
    adapter = _adapter(rest, tmp_path)
    event_time = START
    adapter.handle_user_data_event(
        ExecutionReportEvent(
            timestamp=event_time,
            symbol=BTCUSDT,
            client_order_id="filled-1",
            execution_result=ExecutionResult.FILLED,
            order_status=ExchangeOrderStatus.FILLED,
            fill=None,
        )
    )
    assert adapter._order_statuses["filled-1"] == "FILLED"
    adapter.prune_execution_state(now=event_time + timedelta(seconds=EXECUTION_STATE_RETENTION_SECONDS + 1))
    assert "filled-1" not in adapter._order_statuses
    assert "filled-1" not in adapter._fills_by_client_order_id


@pytest.mark.asyncio
async def test_background_task_failure_is_surfaced_and_halts_runtime(tmp_path):
    rest = FakeRestClient()
    rest.account_balances["BTC"] = Decimal("0")
    runtime = _runtime(tmp_path, rest, heartbeat_interval_seconds=0)

    async def _boom() -> None:
        raise RuntimeError("boom")

    runtime.emit_heartbeat_observability = _boom  # type: ignore[method-assign]
    await runtime.start()
    try:
        await asyncio.sleep(0)
        await asyncio.sleep(0.05)
        assert runtime.status == RuntimeStatus.HALT
        assert runtime.last_alert is not None
        assert runtime.last_alert.code == CRITICAL_BACKGROUND_TASK_FAILURE_CODE
        assert runtime.last_alert.severity == AlertSeverity.CRITICAL
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_user_data_stale_triggers_fail_safe_halt(tmp_path):
    rest = FakeRestClient()
    runtime = _runtime(tmp_path, rest)
    await runtime.start()
    try:
        evaluation_time = START + timedelta(seconds=USER_DATA_STALE_SECONDS + 1)
        runtime.channels[StreamType.KLINE].touch(evaluation_time)
        runtime.channels[StreamType.BOOK_TICKER].touch(evaluation_time)
        runtime.channels[StreamType.BNB_TICKER].touch(evaluation_time)
        runtime.channels[StreamType.USER_DATA].touch(START)

        await runtime.evaluate_staleness(evaluation_time)

        assert runtime.channels[StreamType.USER_DATA].state.is_stale is True
        assert runtime.channels[StreamType.KLINE].state.is_stale is False
        assert runtime.status == RuntimeStatus.HALT
        assert runtime.last_alert is not None
        assert runtime.last_alert.code == CRITICAL_STALE_USER_DATA_CODE
        assert runtime.last_alert.severity == AlertSeverity.CRITICAL
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_failed_delisting_forced_sell_does_not_mark_submission_done_and_can_retry(tmp_path):
    rest = FakeRestClient()
    rest.order_submit_status = "REJECTED"
    runtime = _runtime(tmp_path, rest)
    runtime.detector = FakeDelistingDetector(days_until_delisting=2)
    runtime.portfolio.replace_snapshot(
        held_qty=Decimal("0.2"),
        is_in_position=True,
        meaningful_position=True,
        avg_cost_basis=Decimal("100"),
    )
    await runtime.start()
    try:
        assert runtime._delisting_sell_submitted is False
        first_order_calls = [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
        assert len(first_order_calls) == 1

        rest.order_submit_status = "NEW"
        await runtime._check_delisting()
        second_order_calls = [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
        assert len(second_order_calls) == 2
        assert runtime._delisting_sell_submitted is True
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_startup_oco_ambiguity_is_handled_conservatively(tmp_path):
    rest = FakeRestClient()
    rest.account_balances["BTC"] = Decimal("0")
    rest.open_oco_order_ids = ["oco-a", "oco-b"]
    runtime = _runtime(tmp_path, rest)
    await runtime.start()
    try:
        assert runtime.status == RuntimeStatus.HALT
        assert runtime.last_alert is not None
        assert runtime.last_alert.code == CRITICAL_STARTUP_OCO_AMBIGUITY_CODE
        assert runtime.active_oco_order_id is None
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_single_unknown_startup_oco_is_handled_conservatively(tmp_path):
    rest = FakeRestClient()
    rest.account_balances["BTC"] = Decimal("0")
    rest.open_oco_order_ids = ["oco-a"]
    runtime = _runtime(tmp_path, rest)
    await runtime.start()
    try:
        assert runtime.status == RuntimeStatus.HALT
        assert runtime.last_alert is not None
        assert runtime.last_alert.code == CRITICAL_STARTUP_OCO_AMBIGUITY_CODE
        assert runtime.active_oco_order_id is None
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_account_position_and_fill_do_not_double_apply_same_economic_event(tmp_path):
    rest = FakeRestClient()
    runtime = _runtime(tmp_path, rest)
    _attach_user_channel(runtime)
    fill = Fill(
        order_id="buy-1",
        symbol=BTCUSDT,
        side=Side.BUY,
        base_qty_filled=Decimal("0.1"),
        quote_qty_filled=Decimal("10"),
        fill_price=Decimal("100"),
        commission=Decimal("0"),
        commission_asset=CommissionAsset.QUOTE,
        filled_at=START,
    )
    await runtime._handle_user(
        OutboundAccountPositionEvent(
            timestamp=START,
            balances={"BTC": Decimal("0.1"), "USDT": Decimal("990")},
            locked_balances={"BTC": Decimal("0"), "USDT": Decimal("0")},
        )
    )
    await runtime._handle_user(
        ExecutionReportEvent(
            timestamp=START,
            symbol=BTCUSDT,
            client_order_id="buy-1",
            execution_result=ExecutionResult.FILLED,
            order_status=ExchangeOrderStatus.FILLED,
            fill=fill,
        )
    )
    assert runtime.portfolio.snapshot.held_qty == Decimal("0.1")
    assert runtime.portfolio.snapshot.free_quote == Decimal("990")


@pytest.mark.asyncio
async def test_partial_account_position_update_merges_assets_and_updates_runtime_portfolio_truth_immediately(tmp_path):
    rest = FakeRestClient()
    runtime = _runtime(tmp_path, rest)
    _attach_user_channel(runtime)
    await runtime.start()
    try:
        await runtime._handle_user(
            OutboundAccountPositionEvent(
                timestamp=START,
                balances={"BTC": Decimal("0.6")},
                locked_balances={"BTC": Decimal("0.1")},
            )
        )
        balances, locked_balances = runtime.executor.get_cached_balance_state()
        assert balances["BTC"] == Decimal("0.6")
        assert locked_balances["BTC"] == Decimal("0.1")
        assert balances["USDT"] == Decimal("1000")
        assert balances["BNB"] == Decimal("1")
        assert runtime.portfolio.snapshot.held_qty == Decimal("0.7")
        assert runtime.portfolio.snapshot.free_quote == Decimal("1000")
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_duplicate_execution_report_does_not_double_apply_fill_or_history(tmp_path):
    rest = FakeRestClient()
    runtime = _runtime(tmp_path, rest)
    _attach_user_channel(runtime)
    fill = Fill(
        order_id="buy-dup",
        symbol=BTCUSDT,
        side=Side.BUY,
        base_qty_filled=Decimal("0.1"),
        quote_qty_filled=Decimal("10"),
        fill_price=Decimal("100"),
        commission=Decimal("0"),
        commission_asset=CommissionAsset.QUOTE,
        trade_id="trade-dup-1",
        filled_at=START,
    )
    event = ExecutionReportEvent(
        timestamp=START,
        symbol=BTCUSDT,
        client_order_id="buy-dup",
        execution_result=ExecutionResult.PARTIAL_FILL,
        order_status=ExchangeOrderStatus.PARTIALLY_FILLED,
        fill=fill,
    )
    await runtime._handle_user(event)
    await runtime._handle_user(event)
    assert runtime.portfolio.snapshot.held_qty == Decimal("0.1")
    assert runtime.portfolio.snapshot.free_quote == Decimal("990")
    assert len(runtime.portfolio.accounting.fill_history) == 1
    assert runtime.portfolio.accounting.fill_history[0].trade_id == "trade-dup-1"


@pytest.mark.asyncio
async def test_distinct_partial_fills_of_same_order_are_all_applied_once_each(tmp_path):
    rest = FakeRestClient()
    runtime = _runtime(tmp_path, rest)
    _attach_user_channel(runtime)
    first_fill = Fill(
        order_id="buy-split",
        symbol=BTCUSDT,
        side=Side.BUY,
        base_qty_filled=Decimal("0.1"),
        quote_qty_filled=Decimal("10"),
        fill_price=Decimal("100"),
        commission=Decimal("0"),
        commission_asset=CommissionAsset.QUOTE,
        trade_id="trade-split-1",
        filled_at=START,
    )
    second_fill = Fill(
        order_id="buy-split",
        symbol=BTCUSDT,
        side=Side.BUY,
        base_qty_filled=Decimal("0.15"),
        quote_qty_filled=Decimal("15"),
        fill_price=Decimal("100"),
        commission=Decimal("0"),
        commission_asset=CommissionAsset.QUOTE,
        trade_id="trade-split-2",
        filled_at=START + timedelta(seconds=1),
    )
    await runtime._handle_user(
        ExecutionReportEvent(
            timestamp=START,
            symbol=BTCUSDT,
            client_order_id="buy-split",
            execution_result=ExecutionResult.PARTIAL_FILL,
            order_status=ExchangeOrderStatus.PARTIALLY_FILLED,
            fill=first_fill,
        )
    )
    await runtime._handle_user(
        ExecutionReportEvent(
            timestamp=START + timedelta(seconds=1),
            symbol=BTCUSDT,
            client_order_id="buy-split",
            execution_result=ExecutionResult.FILLED,
            order_status=ExchangeOrderStatus.FILLED,
            fill=second_fill,
        )
    )
    assert runtime.portfolio.snapshot.held_qty == Decimal("0.25")
    assert runtime.portfolio.snapshot.free_quote == Decimal("975")
    assert len(runtime.portfolio.accounting.fill_history) == 2
    assert [record.trade_id for record in runtime.portfolio.accounting.fill_history] == [
        "trade-split-1",
        "trade-split-2",
    ]


@pytest.mark.asyncio
async def test_duplicate_weaker_execution_state_does_not_reopen_pending_after_terminal_fill(tmp_path):
    rest = FakeRestClient()
    runtime = _runtime(tmp_path, rest)
    _attach_user_channel(runtime)
    runtime._set_pending_order(
        Order(
            client_order_id="buy-term",
            symbol=BTCUSDT,
            side=Side.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.1"),
            created_at=START,
        )
    )
    await runtime._handle_user(
        ExecutionReportEvent(
            timestamp=START,
            symbol=BTCUSDT,
            client_order_id="buy-term",
            execution_result=ExecutionResult.FILLED,
            order_status=ExchangeOrderStatus.FILLED,
            fill=None,
        )
    )
    await runtime._handle_user(
        ExecutionReportEvent(
            timestamp=START + timedelta(seconds=1),
            symbol=BTCUSDT,
            client_order_id="buy-term",
            execution_result=ExecutionResult.PARTIAL_FILL,
            order_status=ExchangeOrderStatus.PARTIALLY_FILLED,
            fill=None,
        )
    )
    assert runtime.pending_order_client_id is None
    assert runtime.pending_order_side is None


@pytest.mark.asyncio
async def test_pending_order_blocks_duplicate_submit_until_resolution(tmp_path):
    rest = FakeRestClient()
    runtime = _runtime(tmp_path, rest)
    _attach_user_channel(runtime)
    runtime.status = RuntimeStatus.READY
    runtime._set_pending_order(
        Order(
            symbol=BTCUSDT,
            side=Side.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.01"),
            created_at=START,
        )
    )
    await runtime._execute_intent(
        Intent(
            type=IntentType.BUY,
            symbol=BTCUSDT,
            quantity_mode=QuantityMode.FULL,
            reason="duplicate_buy",
            timestamp=START,
        ),
        START,
    )
    assert not [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]

    await runtime._handle_user(
        ExecutionReportEvent(
            timestamp=START,
            symbol=BTCUSDT,
            client_order_id=runtime.pending_order_client_id or "pending-1",
            execution_result=ExecutionResult.CANCELLED,
            order_status=ExchangeOrderStatus.CANCELED,
            fill=None,
        )
    )
    runtime.portfolio.replace_snapshot(
        held_qty=Decimal("0.2"),
        is_in_position=True,
        meaningful_position=True,
        avg_cost_basis=Decimal("100"),
    )
    await runtime._execute_intent(
        Intent(
            type=IntentType.SELL,
            symbol=BTCUSDT,
            quantity_mode=QuantityMode.FULL,
            reason="exit_after_cancel",
            timestamp=START,
        ),
        START,
    )
    assert len([call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]) == 1


@pytest.mark.asyncio
async def test_active_oco_is_cancelled_before_direct_sell_submit(tmp_path):
    rest = FakeRestClient()
    runtime = _runtime(tmp_path, rest)
    runtime.status = RuntimeStatus.READY
    runtime.active_oco_order_id = "oco-1"
    runtime.protection_mode = ProtectionMode.EXCHANGE_OCO
    runtime.portfolio.replace_snapshot(
        held_qty=Decimal("0.2"),
        is_in_position=True,
        meaningful_position=True,
        avg_cost_basis=Decimal("100"),
    )

    await runtime._execute_intent(
        Intent(
            type=IntentType.SELL,
            symbol=BTCUSDT,
            quantity_mode=QuantityMode.FULL,
            reason="strategy_exit",
            timestamp=START,
        ),
        START,
    )

    delete_index = next(index for index, call in enumerate(rest.calls) if call[1] == "/api/v3/orderList")
    post_index = next(index for index, call in enumerate(rest.calls) if call[1] == "/api/v3/order" and call[0] == "POST")
    assert delete_index < post_index
    assert runtime.active_oco_order_id is None


@pytest.mark.asyncio
async def test_failed_oco_cancel_blocks_unsafe_direct_sell(tmp_path):
    rest = FakeRestClient()
    rest.order_list_cancel_status = "EXEC_STARTED"
    rest.open_oco_order_ids = ["oco-1"]
    runtime = _runtime(tmp_path, rest)
    runtime.status = RuntimeStatus.READY
    runtime.active_oco_order_id = "oco-1"
    runtime.portfolio.replace_snapshot(
        held_qty=Decimal("0.2"),
        is_in_position=True,
        meaningful_position=True,
        avg_cost_basis=Decimal("100"),
    )

    await runtime._execute_intent(
        Intent(
            type=IntentType.SELL,
            symbol=BTCUSDT,
            quantity_mode=QuantityMode.FULL,
            reason="unsafe_exit",
            timestamp=START,
        ),
        START,
    )

    assert not [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
    assert runtime.status == RuntimeStatus.HALT
    assert runtime.last_alert is not None
    assert runtime.last_alert.code == CRITICAL_DIRECT_SELL_OCO_CANCEL_FAILED_CODE


@pytest.mark.asyncio
async def test_oco_cancel_race_does_not_false_halt_when_exchange_oco_is_already_terminal(tmp_path):
    rest = FakeRestClient()
    rest.order_list_cancel_status = "EXEC_STARTED"
    runtime = _runtime(tmp_path, rest)
    runtime.status = RuntimeStatus.READY
    runtime.active_oco_order_id = "oco-1"
    runtime.protection_mode = ProtectionMode.EXCHANGE_OCO
    runtime.portfolio.replace_snapshot(
        held_qty=Decimal("0.2"),
        is_in_position=True,
        meaningful_position=True,
        avg_cost_basis=Decimal("100"),
    )

    await runtime._execute_intent(
        Intent(
            type=IntentType.SELL,
            symbol=BTCUSDT,
            quantity_mode=QuantityMode.FULL,
            reason="resolved_oco_exit",
            timestamp=START,
        ),
        START,
    )

    assert len([call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]) == 1
    assert runtime.status == RuntimeStatus.READY
    assert runtime.active_oco_order_id is None
    assert runtime.last_alert is None or runtime.last_alert.code != CRITICAL_DIRECT_SELL_OCO_CANCEL_FAILED_CODE


@pytest.mark.asyncio
async def test_oco_cancel_exception_does_not_false_halt_when_exchange_oco_is_already_terminal(tmp_path):
    rest = FakeRestClient()
    runtime = _runtime(tmp_path, rest)
    runtime.status = RuntimeStatus.READY
    runtime.active_oco_order_id = "oco-1"
    runtime.protection_mode = ProtectionMode.EXCHANGE_OCO
    runtime.portfolio.replace_snapshot(
        held_qty=Decimal("0.2"),
        is_in_position=True,
        meaningful_position=True,
        avg_cost_basis=Decimal("100"),
    )

    async def _boom(_list_order_id: str):
        raise RuntimeError("order list not found")

    runtime.executor.cancel_oco = _boom  # type: ignore[method-assign]

    await runtime._execute_intent(
        Intent(
            type=IntentType.SELL,
            symbol=BTCUSDT,
            quantity_mode=QuantityMode.FULL,
            reason="resolved_oco_exception_exit",
            timestamp=START,
        ),
        START,
    )

    assert len([call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]) == 1
    assert runtime.status == RuntimeStatus.READY
    assert runtime.active_oco_order_id is None
    assert runtime.last_alert is None or runtime.last_alert.code != CRITICAL_DIRECT_SELL_OCO_CANCEL_FAILED_CODE


@pytest.mark.asyncio
async def test_oco_cancel_exception_does_not_false_halt_when_filled_leg_explains_state(tmp_path):
    rest = FakeRestClient()
    rest.account_balances["BTC"] = Decimal("0")
    rest.account_balances["USDT"] = Decimal("1020")
    runtime = _runtime(tmp_path, rest)
    runtime.status = RuntimeStatus.READY
    runtime.active_oco_order_id = "oco-1"
    runtime.protection_mode = ProtectionMode.EXCHANGE_OCO
    runtime.portfolio.replace_snapshot(
        held_qty=Decimal("0.2"),
        is_in_position=True,
        meaningful_position=True,
        avg_cost_basis=Decimal("100"),
    )
    runtime.executor._order_store.save_oco(  # type: ignore[attr-defined]
        OCOOrder(
            symbol=BTCUSDT,
            tp_price=Decimal("110"),
            sl_stop_price=Decimal("95"),
            sl_limit_price=Decimal("94"),
            quantity=Decimal("0.2"),
            tp_client_order_id="tp-1",
            sl_client_order_id="sl-1",
            list_order_id="oco-1",
            created_at=START,
            updated_at=START,
        )
    )
    rest.order_snapshots["tp-1"] = {
        "status": "FILLED",
        "side": "SELL",
        "executedQty": "0.2",
        "cummulativeQuoteQty": "20",
        "price": "100",
        "updateTime": int(START.timestamp() * 1000),
    }
    rest.order_snapshots["sl-1"] = {
        "status": "NEW",
        "side": "SELL",
        "executedQty": "0",
        "cummulativeQuoteQty": "0",
        "price": "94",
        "updateTime": int(START.timestamp() * 1000),
    }
    rest.order_trades["tp-1"] = [
        {
            "id": 901,
            "price": "100",
            "qty": "0.2",
            "quoteQty": "20",
            "commission": "0",
            "commissionAsset": "QUOTE",
            "time": int(START.timestamp() * 1000),
        }
    ]

    async def _boom(_list_order_id: str):
        raise RuntimeError("oco already gone")

    runtime.executor.cancel_oco = _boom  # type: ignore[method-assign]

    await runtime._execute_intent(
        Intent(
            type=IntentType.SELL,
            symbol=BTCUSDT,
            quantity_mode=QuantityMode.FULL,
            reason="filled_leg_exception_exit",
            timestamp=START,
        ),
        START,
    )

    assert not [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
    assert runtime.status == RuntimeStatus.READY
    assert runtime.active_oco_order_id is None
    assert runtime.portfolio.snapshot.held_qty == Decimal("0")
    assert runtime.portfolio.snapshot.free_quote == Decimal("1020")
    assert runtime.last_alert is None or runtime.last_alert.code != CRITICAL_DIRECT_SELL_OCO_CANCEL_FAILED_CODE


@pytest.mark.asyncio
async def test_oco_cancel_exception_halts_when_exchange_state_is_unresolved(tmp_path):
    rest = FakeRestClient()
    rest.open_oco_order_ids = ["oco-1"]
    runtime = _runtime(tmp_path, rest)
    runtime.status = RuntimeStatus.READY
    runtime.active_oco_order_id = "oco-1"
    runtime.protection_mode = ProtectionMode.EXCHANGE_OCO
    runtime.portfolio.replace_snapshot(
        held_qty=Decimal("0.2"),
        is_in_position=True,
        meaningful_position=True,
        avg_cost_basis=Decimal("100"),
    )

    async def _boom(_list_order_id: str):
        raise RuntimeError("temporary exchange failure")

    runtime.executor.cancel_oco = _boom  # type: ignore[method-assign]

    await runtime._execute_intent(
        Intent(
            type=IntentType.SELL,
            symbol=BTCUSDT,
            quantity_mode=QuantityMode.FULL,
            reason="unresolved_oco_exception_exit",
            timestamp=START,
        ),
        START,
    )

    assert not [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
    assert runtime.status == RuntimeStatus.HALT
    assert runtime.last_alert is not None
    assert runtime.last_alert.code == CRITICAL_DIRECT_SELL_OCO_CANCEL_FAILED_CODE


@pytest.mark.asyncio
async def test_restart_reconciliation_halts_for_unknown_exchange_open_order(tmp_path):
    rest = FakeRestClient()
    rest.open_orders = [
        {
            "clientOrderId": "exchange-open-1",
            "status": "NEW",
            "side": "BUY",
            "executedQty": "0",
            "cummulativeQuoteQty": "0",
            "price": "100",
            "updateTime": int(START.timestamp() * 1000),
        }
    ]
    runtime = _runtime(tmp_path, rest)
    runtime._restart_state_loaded = True
    previous_snapshot = runtime.portfolio.snapshot

    await runtime._run_restart_reconciliation(previous_snapshot, restart_reason="startup")

    assert runtime.status == RuntimeStatus.HALT
    assert runtime.last_alert is not None
    assert runtime.last_alert.code == CRITICAL_RESTART_OUTSTANDING_ORDER_CODE


@pytest.mark.asyncio
async def test_restart_reconciliation_halts_for_local_partial_fill_state(tmp_path):
    rest = FakeRestClient()
    order = Order(
        client_order_id="partial-1",
        symbol=BTCUSDT,
        side=Side.SELL,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.2"),
        created_at=START,
    )
    rest.order_snapshots["partial-1"] = {
        "status": "PARTIALLY_FILLED",
        "side": "SELL",
        "executedQty": "0.05",
        "cummulativeQuoteQty": "5",
        "price": "100",
        "updateTime": int(START.timestamp() * 1000),
    }
    rest.order_trades["partial-1"] = [
        {
            "id": 501,
            "price": "100",
            "qty": "0.05",
            "quoteQty": "5",
            "commission": "0",
            "commissionAsset": "QUOTE",
            "time": int(START.timestamp() * 1000),
        }
    ]
    runtime = _runtime(tmp_path, rest)
    runtime.executor._order_store.save_order(order)  # type: ignore[attr-defined]
    runtime._restart_state_loaded = True

    await runtime._run_restart_reconciliation(runtime.portfolio.snapshot, restart_reason="startup")

    assert runtime.status == RuntimeStatus.HALT
    assert runtime.pending_order_client_id == "partial-1"
    assert runtime.last_alert is not None
    assert runtime.last_alert.code == CRITICAL_RESTART_PARTIAL_FILL_CODE
    assert [record.trade_id for record in runtime.portfolio.accounting.fill_history] == ["501"]
