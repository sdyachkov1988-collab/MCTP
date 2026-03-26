from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mctp.adapters import BinanceCredentials, BinanceSpotTestnetAdapterV1, BinanceSpotTestnetConfigV1
from mctp.core.constants import CRITICAL_EXTERNAL_OCO_CANCEL_CODE, T_CANCEL, WARNING_MANUAL_TRADE_DETECTED_CODE
from mctp.core.enums import AlertSeverity, CommissionAsset, ExecutionResult, Market, OrderType, ProtectionMode, Side, Timeframe
from mctp.core.order import Fill, Order
from mctp.core.types import PortfolioSnapshot, Symbol
from mctp.execution.oco import OCOOrder
from mctp.portfolio.accounting import AccountingFillRecord
from mctp.runtime.events import ExecutionReportEvent, OCOListStatusEvent
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
        self.account_balances = {"BTC": Decimal("0.25"), "USDT": Decimal("1000")}
        self.locked_balances = {"BTC": Decimal("0"), "USDT": Decimal("0")}
        self.open_oco_order_ids: list[str] = []
        self.order_submit_status = "NEW"
        self.order_snapshots: dict[str, dict] = {}
        self.order_trades: dict[str, list[dict]] = {}

    async def request_json(self, method, path, *, params=None, signed=False):
        safe_params = dict(params or {})
        self.calls.append((method, path, safe_params, signed))
        if path == "/api/v3/order" and method == "POST":
            return {"status": self.order_submit_status}
        if path == "/api/v3/order" and method == "DELETE":
            return {"status": "CANCELED"}
        if path == "/api/v3/order" and method == "GET":
            return self.order_snapshots.get(safe_params["origClientOrderId"], {})
        if path == "/api/v3/myTrades":
            return self.order_trades.get(safe_params["origClientOrderId"], [])
        if path == "/api/v3/userDataStream" and method == "POST":
            return {"listenKey": "listen-key"}
        if path == "/api/v3/userDataStream" and method in {"PUT", "DELETE"}:
            return {}
        if path == "/api/v3/orderList/oco":
            return {"orderListId": "oco-1", "orders": [{"clientOrderId": "tp-1"}, {"clientOrderId": "sl-1"}]}
        if path == "/api/v3/openOrderList":
            return [{"orderListId": order_list_id} for order_list_id in self.open_oco_order_ids]
        if path == "/api/v3/openOrders":
            return []
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


def _config(**overrides):
    config = TestnetRuntimeConfig(
        symbol=BTCUSDT,
        timeframe=Timeframe.M15,
        instrument_info={
            "lot_size": Decimal("0.001"),
            "min_qty": Decimal("0.001"),
            "max_qty": Decimal("1000"),
            "min_notional": Decimal("10"),
        },
        initial_balances={"BTC": Decimal("0"), "USDT": Decimal("0")},
    )
    for key, value in overrides.items():
        setattr(config, key, value)
    return config


def _runtime(tmp_path, rest_client: FakeRestClient, **config_overrides):
    order_store = OrderStore(str(tmp_path / "orders.json"))
    adapter = BinanceSpotTestnetAdapterV1(
        BinanceSpotTestnetConfigV1(BinanceCredentials(api_key="k", api_secret="s")),
        rest_client=rest_client,
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        order_store=order_store,
    )
    runtime = TestnetRuntime(
        config=_config(**config_overrides),
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
    return adapter, runtime, order_store


def _persist_snapshot(runtime: TestnetRuntime, held_qty: Decimal, avg_cost_basis: Decimal) -> None:
    runtime.snapshot_store.save(
        PortfolioSnapshot(
            symbol=BTCUSDT,
            held_qty=held_qty,
            avg_cost_basis=avg_cost_basis,
            free_quote=Decimal("1000"),
            quote_asset="USDT",
            is_in_position=held_qty > Decimal("0"),
            meaningful_position=held_qty > Decimal("0"),
            timestamp=START,
        )
    )


def _attach_user_channel(runtime: TestnetRuntime) -> None:
    runtime.channels[StreamType.USER_DATA] = type("_ChannelStub", (), {"touch": lambda self, _timestamp: None})()  # type: ignore[assignment]


@pytest.mark.asyncio
async def test_full_reconciliation_runs_on_every_restart(tmp_path):
    rest = FakeRestClient()
    _, runtime_a, _ = _runtime(tmp_path, rest)
    _persist_snapshot(runtime_a, Decimal("0.25"), Decimal("100"))
    await runtime_a.start()
    try:
        assert runtime_a.reconciliation_runs == 1
    finally:
        await runtime_a.shutdown()

    _, runtime_b, _ = _runtime(tmp_path, rest)
    await runtime_b.start()
    try:
        assert runtime_b.reconciliation_runs == 1
    finally:
        await runtime_b.shutdown()


@pytest.mark.asyncio
async def test_reconnect_refreshes_balance_cache_from_rest(tmp_path):
    rest = FakeRestClient()
    _, runtime, _ = _runtime(tmp_path, rest)
    _persist_snapshot(runtime, Decimal("0.25"), Decimal("100"))
    await runtime.start()
    try:
        initial_account_calls = len([call for call in rest.calls if call[1] == "/api/v3/account"])
        runtime.channels[StreamType.BOOK_TICKER].reconnect_count = 1
        await runtime.process_all_available()
        final_account_calls = len([call for call in rest.calls if call[1] == "/api/v3/account"])
        assert final_account_calls == initial_account_calls + 1
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_outage_oco_fill_is_applied_with_cached_bnb_rate_and_other_leg_cancelled(tmp_path):
    rest = FakeRestClient()
    rest.account_balances["BTC"] = Decimal("0")
    rest.account_balances["USDT"] = Decimal("1027.5")
    adapter, runtime, order_store = _runtime(tmp_path, rest)
    _persist_snapshot(runtime, Decimal("0.25"), Decimal("100"))
    runtime.accounting_store.save(
        [
            AccountingFillRecord(
                trade_id="cached-1",
                order_id="cached-order-1",
                symbol=BTCUSDT,
                filled_at=START,
                fill_price=Decimal("100"),
                commission=Decimal("0.01"),
                commission_asset=CommissionAsset.BNB,
                fee_drag_quote=Decimal("3.2"),
                bnb_rate_at_fill=Decimal("320"),
            )
        ]
    )
    order_store.save_oco(
        OCOOrder(
            symbol=BTCUSDT,
            tp_price=Decimal("110"),
            sl_stop_price=Decimal("95"),
            sl_limit_price=Decimal("94"),
            quantity=Decimal("0.25"),
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
        "executedQty": "0.25",
        "cummulativeQuoteQty": "27.5",
        "price": "110",
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
            "id": 77,
            "price": "110",
            "qty": "0.25",
            "quoteQty": "27.5",
            "commission": "0.01",
            "commissionAsset": "BNB",
            "time": int(START.timestamp() * 1000),
        }
    ]

    await runtime.start()
    try:
        assert runtime.last_reconciliation_applied_bnb_rate == Decimal("320")
        assert runtime.portfolio.snapshot.held_qty == Decimal("0")
        assert runtime.portfolio.snapshot.free_quote == Decimal("1027.5")
        assert runtime.last_cancel_code == T_CANCEL
        cancel_calls = [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "DELETE"]
        assert cancel_calls[-1][2]["origClientOrderId"] == "sl-1"
        records = runtime.accounting_store.load()
        assert records is not None
        assert records[-1].trade_id == "77"
        assert records[-1].bnb_rate_at_fill == Decimal("320")
        assert runtime.portfolio.snapshot.held_qty == Decimal("0")
        assert runtime.portfolio.snapshot.avg_cost_basis == Decimal("0")
        _, active_ocos = order_store.load()
        assert active_ocos == {}
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_outage_oco_fill_reconciliation_does_not_double_count_quote_proceeds(tmp_path):
    rest = FakeRestClient()
    rest.account_balances["BTC"] = Decimal("0")
    rest.account_balances["USDT"] = Decimal("1027.5")
    _, runtime, order_store = _runtime(tmp_path, rest)
    _persist_snapshot(runtime, Decimal("0.25"), Decimal("100"))
    order_store.save_oco(
        OCOOrder(
            symbol=BTCUSDT,
            tp_price=Decimal("110"),
            sl_stop_price=Decimal("95"),
            sl_limit_price=Decimal("94"),
            quantity=Decimal("0.25"),
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
        "executedQty": "0.25",
        "cummulativeQuoteQty": "27.5",
        "price": "110",
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
            "id": 78,
            "price": "110",
            "qty": "0.25",
            "quoteQty": "27.5",
            "commission": "0.01",
            "commissionAsset": "QUOTE",
            "time": int(START.timestamp() * 1000),
        }
    ]

    await runtime.start()
    try:
        assert runtime.portfolio.snapshot.free_quote == Decimal("1027.5")
        assert runtime.portfolio.snapshot.held_qty == Decimal("0")
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_external_oco_cancellation_still_reactivates_software_stop_and_raises_critical(tmp_path):
    rest = FakeRestClient()
    _, runtime, order_store = _runtime(tmp_path, rest)
    _persist_snapshot(runtime, Decimal("0.25"), Decimal("100"))
    rest.open_oco_order_ids = ["oco-1"]
    order_store.save_oco(
        OCOOrder(
            symbol=BTCUSDT,
            tp_price=Decimal("110"),
            sl_stop_price=Decimal("95"),
            sl_limit_price=Decimal("94"),
            quantity=Decimal("0.25"),
            list_order_id="oco-1",
            created_at=START,
            updated_at=START,
        )
    )
    await runtime.start()
    try:
        runtime._handle_oco_status(
            OCOListStatusEvent(
                timestamp=START,
                symbol=BTCUSDT,
                list_order_id="oco-1",
                list_status_type="ALL_DONE",
                list_order_status="ALL_DONE",
                contingency_type="OCO",
            )
        )
        assert runtime.protection_mode == ProtectionMode.SOFTWARE_STOP
        assert runtime.software_stop_active is True
        assert runtime.last_alert is not None
        assert runtime.last_alert.severity == AlertSeverity.CRITICAL
        assert runtime.last_alert.code == CRITICAL_EXTERNAL_OCO_CANCEL_CODE
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_manual_trade_detection_prompts_operator_and_basis_adjustment_path_works(tmp_path):
    rest = FakeRestClient()
    rest.account_balances["BTC"] = Decimal("0.40")
    _, runtime, _ = _runtime(tmp_path, rest)
    _persist_snapshot(runtime, Decimal("0.25"), Decimal("100"))
    await runtime.start()
    try:
        assert runtime.status == RuntimeStatus.HALT
        assert runtime.manual_trade_detected is True
        assert runtime.manual_trade_prompt_required is True
        assert runtime.last_alert is not None
        assert runtime.last_alert.severity == AlertSeverity.WARNING
        assert runtime.last_alert.code == WARNING_MANUAL_TRADE_DETECTED_CODE
        runtime.apply_manual_trade_basis_adjustment(Decimal("105"))
        assert runtime.portfolio.snapshot.avg_cost_basis == Decimal("105")
        assert runtime.status == RuntimeStatus.READY
        assert runtime.manual_trade_prompt_required is False
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_restart_reconciliation_reconstructs_full_fill_set_for_filled_order_and_replay_does_not_double_count(tmp_path):
    rest = FakeRestClient()
    adapter, runtime, order_store = _runtime(tmp_path, rest)
    order = Order(
        client_order_id="buy-1",
        symbol=BTCUSDT,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.25"),
        created_at=START,
    )
    order_store.save_order(order)
    runtime._restart_state_loaded = True
    previous_snapshot = replace(
        runtime.portfolio.snapshot,
        held_qty=Decimal("0"),
        avg_cost_basis=Decimal("0"),
        free_quote=Decimal("1000"),
        is_in_position=False,
        meaningful_position=False,
        scale_in_count=0,
        timestamp=START,
    )
    runtime._apply_exchange_balance_truth(
        {"BTC": Decimal("0.25"), "USDT": Decimal("975")},
        {"BTC": Decimal("0"), "USDT": Decimal("0")},
        START,
    )
    rest.order_snapshots["buy-1"] = {
        "status": "FILLED",
        "side": "BUY",
        "executedQty": "0.25",
        "cummulativeQuoteQty": "25",
        "price": "100",
        "updateTime": int(START.timestamp() * 1000),
    }
    rest.order_trades["buy-1"] = [
        {
            "id": 701,
            "price": "100",
            "qty": "0.10",
            "quoteQty": "10",
            "commission": "0",
            "commissionAsset": "QUOTE",
            "time": int(START.timestamp() * 1000),
        },
        {
            "id": 702,
            "price": "100",
            "qty": "0.15",
            "quoteQty": "15",
            "commission": "0",
            "commissionAsset": "QUOTE",
            "time": int((START.timestamp() + 1) * 1000),
        },
    ]

    await runtime._run_restart_reconciliation(previous_snapshot, restart_reason="startup")

    assert runtime.portfolio.snapshot.held_qty == Decimal("0.25")
    assert runtime.portfolio.snapshot.free_quote == Decimal("975")
    assert runtime.portfolio.snapshot.avg_cost_basis == Decimal("100")
    records = runtime.accounting_store.load()
    assert records is not None
    assert [record.trade_id for record in records[-2:]] == ["701", "702"]
    assert adapter.load_local_active_orders() == {}
    assert runtime.pending_order_client_id is None

    _attach_user_channel(runtime)
    duplicate_fill = Fill(
        order_id="buy-1",
        symbol=BTCUSDT,
        side=Side.BUY,
        base_qty_filled=Decimal("0.10"),
        quote_qty_filled=Decimal("10"),
        fill_price=Decimal("100"),
        commission=Decimal("0"),
        commission_asset=CommissionAsset.QUOTE,
        trade_id="701",
        filled_at=START,
    )
    await runtime._handle_user(
        ExecutionReportEvent(
            timestamp=START,
            symbol=BTCUSDT,
            client_order_id="buy-1",
            execution_result=ExecutionResult.FILLED,
            order_status="FILLED",
            fill=duplicate_fill,
        )
    )
    updated_records = runtime.accounting_store.load()
    assert updated_records is not None
    assert [record.trade_id for record in updated_records[-2:]] == ["701", "702"]
    assert len(updated_records) == len(records)


@pytest.mark.asyncio
async def test_repeated_restart_over_same_partial_fill_state_is_idempotent_and_preserves_basis(tmp_path):
    rest = FakeRestClient()
    adapter_a, runtime_a, order_store_a = _runtime(tmp_path, rest)
    order = Order(
        client_order_id="partial-buy-1",
        symbol=BTCUSDT,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.50"),
        created_at=START,
    )
    order_store_a.save_order(order)
    pre_fill_snapshot = PortfolioSnapshot(
        symbol=BTCUSDT,
        held_qty=Decimal("0"),
        avg_cost_basis=Decimal("0"),
        free_quote=Decimal("1000"),
        quote_asset="USDT",
        is_in_position=False,
        meaningful_position=False,
        timestamp=START,
    )
    runtime_a.snapshot_store.save(pre_fill_snapshot)
    runtime_a._hydrate_restart_state()
    runtime_a._apply_exchange_balance_truth(
        {"BTC": Decimal("0.25"), "USDT": Decimal("975")},
        {"BTC": Decimal("0"), "USDT": Decimal("0")},
        START,
    )
    rest.order_snapshots["partial-buy-1"] = {
        "status": "PARTIALLY_FILLED",
        "side": "BUY",
        "executedQty": "0.25",
        "cummulativeQuoteQty": "25",
        "price": "100",
        "updateTime": int(START.timestamp() * 1000),
    }
    rest.order_trades["partial-buy-1"] = [
        {
            "id": 801,
            "price": "100",
            "qty": "0.25",
            "quoteQty": "25",
            "commission": "0",
            "commissionAsset": "QUOTE",
            "time": int(START.timestamp() * 1000),
        }
    ]

    await runtime_a._run_restart_reconciliation(pre_fill_snapshot, restart_reason="startup")

    first_records = runtime_a.accounting_store.load()
    assert first_records is not None
    assert [record.trade_id for record in first_records[-1:]] == ["801"]
    assert runtime_a.portfolio.snapshot.held_qty == Decimal("0.25")
    assert runtime_a.portfolio.snapshot.free_quote == Decimal("975")
    assert runtime_a.portfolio.snapshot.avg_cost_basis == Decimal("100")
    assert runtime_a.pending_order_client_id == "partial-buy-1"
    assert runtime_a.status == RuntimeStatus.HALT

    adapter_b, runtime_b, _ = _runtime(tmp_path, rest)
    runtime_b._hydrate_restart_state()
    runtime_b._apply_exchange_balance_truth(
        {"BTC": Decimal("0.25"), "USDT": Decimal("975")},
        {"BTC": Decimal("0"), "USDT": Decimal("0")},
        START,
    )
    previous_snapshot_b = runtime_b.portfolio.snapshot

    await runtime_b._run_restart_reconciliation(previous_snapshot_b, restart_reason="startup")

    second_records = runtime_b.accounting_store.load()
    assert second_records is not None
    assert len(second_records) == len(first_records)
    assert [record.trade_id for record in second_records[-1:]] == ["801"]
    assert runtime_b.portfolio.snapshot.held_qty == Decimal("0.25")
    assert runtime_b.portfolio.snapshot.free_quote == Decimal("975")
    assert runtime_b.portfolio.snapshot.avg_cost_basis == Decimal("100")
    assert runtime_b.pending_order_client_id == "partial-buy-1"
    assert runtime_b.status == RuntimeStatus.HALT
    assert adapter_b.load_local_active_orders()["partial-buy-1"].client_order_id == "partial-buy-1"
