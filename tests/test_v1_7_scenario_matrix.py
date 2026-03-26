from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mctp.adapters import BinanceCredentials, BinanceSpotTestnetAdapterV1, BinanceSpotTestnetConfigV1
from mctp.core.constants import T_CANCEL
from mctp.core.enums import BasisRecoveryState, CommissionAsset, ExchangeOrderStatus, ExecutionResult, IntentType, Market, OrderType, ProtectionMode, QuantityMode, Side, SymbolChangeStage, Timeframe
from mctp.core.order import Fill, Order
from mctp.core.types import Intent, Symbol
from mctp.execution.oco import OCOOrder, OCOStatus
from mctp.runtime.events import ExecutionReportEvent, MockExecutionReportEvent, OutboundAccountPositionEvent
from mctp.runtime.paper import PaperRuntime, PaperRuntimeConfig, PaperRuntimeStatus
from mctp.runtime.strategy_smoke import EmaCrossSmokeStrategy
from mctp.runtime.streams import QueueStreamTransport
from mctp.runtime.testnet import TestnetRuntime, TestnetRuntimeConfig, TestnetRuntimeStatus as RuntimeStatus
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.order_store import OrderStore
from mctp.storage.snapshot_store import SnapshotStore
from mctp.streams.base import StreamType


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
ETHUSDT = Symbol("ETH", "USDT", Market.SPOT)
START = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone.utc)


class ScenarioRestClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, object], bool]] = []
        self.account_balances = {"BTC": Decimal("0"), "USDT": Decimal("1000"), "BNB": Decimal("1")}
        self.locked_balances = {"BTC": Decimal("0"), "USDT": Decimal("0"), "BNB": Decimal("0")}
        self.open_oco_order_ids: list[str] = []
        self.open_orders: list[dict[str, object]] = []
        self.order_submit_status = "NEW"
        self.order_snapshots: dict[str, dict[str, object]] = {}

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
        if path == "/api/v3/userDataStream" and method == "POST":
            return {"listenKey": "listen-key"}
        if path == "/api/v3/userDataStream" and method in {"PUT", "DELETE"}:
            return {}
        if path == "/api/v3/orderList/oco":
            return {"orderListId": "oco-runtime-1"}
        if path == "/api/v3/orderList" and method == "DELETE":
            return {"listStatusType": "ALL_DONE"}
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
        raise AssertionError(f"Unhandled scenario REST call: {method} {path}")


def _instrument_info() -> dict[str, Decimal]:
    return {
        "lot_size": Decimal("0.001"),
        "min_qty": Decimal("0.001"),
        "max_qty": Decimal("1000"),
        "min_notional": Decimal("10"),
    }


def _paper_runtime(
    tmp_path,
    *,
    initial_balances: dict[str, Decimal],
) -> PaperRuntime:
    return PaperRuntime(
        config=PaperRuntimeConfig(
            symbol=BTCUSDT,
            timeframe=Timeframe.M15,
            instrument_info=_instrument_info(),
            initial_balances=initial_balances,
        ),
        strategy=EmaCrossSmokeStrategy(),
        snapshot_store=SnapshotStore(str(tmp_path / "paper_snapshot.json")),
        balance_cache_store=BalanceCacheStore(str(tmp_path / "paper_balances.json")),
        accounting_store=AccountingStore(str(tmp_path / "paper_accounting.json")),
        kline_transport=QueueStreamTransport(),
        book_transport=QueueStreamTransport(),
        bnb_transport=QueueStreamTransport(),
        user_transport=QueueStreamTransport(),
    )


def _testnet_runtime(tmp_path, rest_client: ScenarioRestClient, *, root_name: str = "testnet"):
    root = tmp_path / root_name
    root.mkdir(exist_ok=True)
    order_store = OrderStore(str(root / "orders.json"))
    adapter = BinanceSpotTestnetAdapterV1(
        BinanceSpotTestnetConfigV1(BinanceCredentials(api_key="k", api_secret="s")),
        rest_client=rest_client,
        balance_cache_store=BalanceCacheStore(str(root / "balances.json")),
        order_store=order_store,
    )
    runtime = TestnetRuntime(
        config=TestnetRuntimeConfig(
            symbol=BTCUSDT,
            timeframe=Timeframe.M15,
            instrument_info=_instrument_info(),
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
    return runtime, order_store


async def _publish_paper_fill(runtime: PaperRuntime, fills: list[Fill], timestamp: datetime) -> None:
    balances = await runtime.executor.get_balances()
    for fill in fills:
        await runtime.channels[StreamType.USER_DATA].publish(MockExecutionReportEvent(fill=fill))
    await runtime.channels[StreamType.USER_DATA].publish(
        OutboundAccountPositionEvent(timestamp=timestamp, balances=balances)
    )
    await runtime.process_all_available()


def _buy_fill(order_id: str, quantity: Decimal, price: Decimal, *, timestamp: datetime) -> Fill:
    quote_qty = quantity * price
    commission = quote_qty * Decimal("0.001")
    return Fill(
        order_id=order_id,
        symbol=BTCUSDT,
        side=Side.BUY,
        base_qty_filled=quantity,
        quote_qty_filled=quote_qty,
        fill_price=price,
        commission=commission,
        commission_asset=CommissionAsset.QUOTE,
        filled_at=timestamp,
    )


def _sell_fill(order_id: str, quantity: Decimal, price: Decimal, *, timestamp: datetime) -> Fill:
    quote_qty = quantity * price
    commission = quote_qty * Decimal("0.001")
    return Fill(
        order_id=order_id,
        symbol=BTCUSDT,
        side=Side.SELL,
        base_qty_filled=quantity,
        quote_qty_filled=quote_qty,
        fill_price=price,
        commission=commission,
        commission_asset=CommissionAsset.QUOTE,
        filled_at=timestamp,
    )


def _save_owned_oco(order_store: OrderStore, list_order_id: str, quantity: Decimal) -> None:
    order_store.save_oco(
        OCOOrder(
            symbol=BTCUSDT,
            tp_price=Decimal("110"),
            sl_stop_price=Decimal("95"),
            sl_limit_price=Decimal("94"),
            quantity=quantity,
            list_order_id=list_order_id,
            created_at=START,
            updated_at=START,
        )
    )


@pytest.mark.asyncio
async def test_v17_scenario_entry_fill_then_oco_protection_creation_leaves_runtime_state_coherent(tmp_path):
    rest = ScenarioRestClient()
    runtime, _ = _testnet_runtime(tmp_path, rest, root_name="entry_to_oco")
    await runtime.start()
    try:
        runtime.status = RuntimeStatus.READY
        runtime.current_ask = Decimal("100")
        runtime.current_bid = Decimal("99")
        runtime._order_quantity = lambda intent: (Decimal("0.1"), None)  # type: ignore[method-assign]
        runtime.channels[StreamType.BOOK_TICKER].touch(START)
        runtime.channels[StreamType.BOOK_TICKER].state.is_stale = False

        await runtime._execute_intent(
            Intent(type=IntentType.BUY, symbol=BTCUSDT, quantity_mode=QuantityMode.FULL, reason="entry"),
            START,
        )
        buy_order_calls = [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
        assert len(buy_order_calls) == 1
        entry_order_id = str(buy_order_calls[-1][2]["newClientOrderId"])

        await runtime._handle_user(
            ExecutionReportEvent(
                timestamp=START,
                symbol=BTCUSDT,
                client_order_id=entry_order_id,
                execution_result=ExecutionResult.FILLED,
                order_status=ExchangeOrderStatus.FILLED,
                fill=_buy_fill(entry_order_id, Decimal("0.1"), Decimal("100"), timestamp=START),
            )
        )

        order_list_id = await runtime.submit_oco(
            OCOOrder(
                symbol=BTCUSDT,
                tp_price=Decimal("110"),
                sl_stop_price=Decimal("95"),
                sl_limit_price=Decimal("94"),
                quantity=Decimal("0.1"),
                created_at=START,
                updated_at=START,
            )
        )

        assert runtime.portfolio.snapshot.held_qty == Decimal("0.1")
        assert runtime.portfolio.snapshot.avg_cost_basis == Decimal("100.1")
        assert runtime.pending_order_client_id is None
        assert runtime.active_oco_order_id == order_list_id
        assert runtime.protection_mode == ProtectionMode.EXCHANGE_OCO
        assert runtime.software_stop_active is False
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_scenario_oco_tp_hit_flattens_position_and_resolves_protection(tmp_path):
    runtime = _paper_runtime(
        tmp_path,
        initial_balances={"BTC": Decimal("0.2"), "USDT": Decimal("990"), "BNB": Decimal("1")},
    )
    runtime.portfolio.replace_snapshot(
        held_qty=Decimal("0.2"),
        avg_cost_basis=Decimal("100"),
        free_quote=Decimal("990"),
        is_in_position=True,
        meaningful_position=True,
        timestamp=START,
    )
    await runtime.start()
    try:
        order_list_id = runtime.executor.submit_oco(
            OCOOrder(
                symbol=BTCUSDT,
                tp_price=Decimal("110"),
                sl_stop_price=Decimal("95"),
                sl_limit_price=Decimal("94"),
                quantity=Decimal("0.2"),
                created_at=START,
                updated_at=START,
            )
        )
        runtime.executor.set_event_time(START)
        results = runtime.executor.set_price(BTCUSDT, Decimal("111"))
        fills = [fill for result in results for fill in result.new_fills]
        await _publish_paper_fill(runtime, fills, START)
        oco = runtime.executor.get_oco(order_list_id)
        assert oco is not None
        assert oco.status == OCOStatus.TP_FILLED
        assert runtime.portfolio.snapshot.held_qty == Decimal("0")
        assert runtime.portfolio.snapshot.avg_cost_basis == Decimal("0")
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_scenario_oco_sl_hit_flattens_position_and_resolves_protection(tmp_path):
    runtime = _paper_runtime(
        tmp_path,
        initial_balances={"BTC": Decimal("0.2"), "USDT": Decimal("990"), "BNB": Decimal("1")},
    )
    runtime.portfolio.replace_snapshot(
        held_qty=Decimal("0.2"),
        avg_cost_basis=Decimal("100"),
        free_quote=Decimal("990"),
        is_in_position=True,
        meaningful_position=True,
        timestamp=START,
    )
    await runtime.start()
    try:
        order_list_id = runtime.executor.submit_oco(
            OCOOrder(
                symbol=BTCUSDT,
                tp_price=Decimal("110"),
                sl_stop_price=Decimal("95"),
                sl_limit_price=Decimal("94"),
                quantity=Decimal("0.2"),
                created_at=START,
                updated_at=START,
            )
        )
        runtime.executor.set_event_time(START)
        results = runtime.executor.set_price(BTCUSDT, Decimal("94"))
        fills = [fill for result in results for fill in result.new_fills]
        await _publish_paper_fill(runtime, fills, START)
        oco = runtime.executor.get_oco(order_list_id)
        assert oco is not None
        assert oco.status == OCOStatus.SL_TRIGGERED
        assert runtime.portfolio.snapshot.held_qty == Decimal("0")
        assert runtime.portfolio.snapshot.avg_cost_basis == Decimal("0")
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_scenario_oco_partial_tp_then_sl_keeps_accounting_and_position_consistent(tmp_path):
    runtime = _paper_runtime(
        tmp_path,
        initial_balances={"BTC": Decimal("0.2"), "USDT": Decimal("990"), "BNB": Decimal("1")},
    )
    runtime.portfolio.replace_snapshot(
        held_qty=Decimal("0.2"),
        avg_cost_basis=Decimal("100"),
        free_quote=Decimal("990"),
        is_in_position=True,
        meaningful_position=True,
        timestamp=START,
    )
    await runtime.start()
    try:
        order_list_id = runtime.executor.submit_oco(
            OCOOrder(
                symbol=BTCUSDT,
                tp_price=Decimal("110"),
                sl_stop_price=Decimal("95"),
                sl_limit_price=Decimal("94"),
                quantity=Decimal("0.2"),
                created_at=START,
                updated_at=START,
            )
        )
        partial_fill = runtime.executor.simulate_partial_tp_fill(order_list_id, Decimal("0.05"))
        assert partial_fill is not None
        await _publish_paper_fill(runtime, [partial_fill], START)

        runtime.executor.set_event_time(START)
        results = runtime.executor.set_price(BTCUSDT, Decimal("94"))
        fills = [fill for result in results for fill in result.new_fills]
        await _publish_paper_fill(runtime, fills, START)

        oco = runtime.executor.get_oco(order_list_id)
        assert oco is not None
        assert oco.status == OCOStatus.PARTIAL_TP_THEN_SL
        assert runtime.portfolio.snapshot.held_qty == Decimal("0")
        assert len(runtime.portfolio.accounting.fill_history) == 2
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_scenario_shutdown_restart_preserves_existing_owned_oco_protection_context(tmp_path):
    rest = ScenarioRestClient()
    rest.account_balances["BTC"] = Decimal("0.2")
    rest.account_balances["USDT"] = Decimal("990")
    rest.open_oco_order_ids = ["oco-owned-1"]

    runtime_a, order_store_a = _testnet_runtime(tmp_path, rest, root_name="restart_matrix")
    runtime_a.snapshot_store.save(
        replace(
            runtime_a.portfolio.snapshot,
            held_qty=Decimal("0.2"),
            avg_cost_basis=Decimal("100"),
            free_quote=Decimal("990"),
            is_in_position=True,
            meaningful_position=True,
            timestamp=START,
        )
    )
    _save_owned_oco(order_store_a, "oco-owned-1", Decimal("0.2"))
    await runtime_a.start()
    try:
        assert runtime_a.protection_mode == ProtectionMode.EXCHANGE_OCO
        assert runtime_a.active_oco_order_id == "oco-owned-1"
    finally:
        await runtime_a.shutdown()

    runtime_b, _ = _testnet_runtime(tmp_path, rest, root_name="restart_matrix")
    await runtime_b.start()
    try:
        assert runtime_b.status == RuntimeStatus.READY
        assert runtime_b.protection_mode == ProtectionMode.EXCHANGE_OCO
        assert runtime_b.active_oco_order_id == "oco-owned-1"
    finally:
        await runtime_b.shutdown()


@pytest.mark.asyncio
async def test_v17_scenario_runtime_t_cancel_clears_pending_state_without_ghost_order_or_accounting_drift(tmp_path):
    rest = ScenarioRestClient()
    rest.account_balances["BTC"] = Decimal("0.2")
    rest.account_balances["USDT"] = Decimal("990")
    runtime, _ = _testnet_runtime(tmp_path, rest, root_name="runtime_cancel")
    await runtime.start()
    try:
        runtime.portfolio.replace_snapshot(
            held_qty=Decimal("0.2"),
            avg_cost_basis=Decimal("100"),
            free_quote=Decimal("990"),
            is_in_position=True,
            meaningful_position=True,
            timestamp=START,
        )
        await runtime.request_symbol_change(ETHUSDT)
        pending_order_id = runtime.pending_order_client_id
        assert pending_order_id is not None
        assert runtime.symbol_change_stage == SymbolChangeStage.AWAITING_ZERO

        cancel_result = await runtime.executor.cancel_order_with_code(pending_order_id, T_CANCEL)
        assert cancel_result == ExecutionResult.CANCELLED
        await runtime._handle_user(
            ExecutionReportEvent(
                timestamp=START,
                symbol=BTCUSDT,
                client_order_id=pending_order_id,
                execution_result=ExecutionResult.CANCELLED,
                order_status=ExchangeOrderStatus.CANCELED,
                fill=None,
            )
        )

        assert runtime.pending_order_client_id is None
        assert runtime.pending_order_side is None
        assert runtime.executor.load_local_active_orders() == {}
        assert runtime.portfolio.snapshot.held_qty == Decimal("0.2")
        assert runtime.portfolio.snapshot.avg_cost_basis == Decimal("100")
        assert runtime.portfolio.accounting.fill_history == []
        assert runtime.active_oco_order_id is None
        assert runtime.protection_mode == ProtectionMode.NONE
        assert T_CANCEL == 10
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_scenario_gtc_scale_in_updates_avg_cost_basis_and_exposure_coherently(tmp_path):
    runtime = _paper_runtime(
        tmp_path,
        initial_balances={"BTC": Decimal("0.1"), "USDT": Decimal("990"), "BNB": Decimal("1")},
    )
    runtime.portfolio.replace_snapshot(
        held_qty=Decimal("0.1"),
        avg_cost_basis=Decimal("100"),
        free_quote=Decimal("990"),
        is_in_position=True,
        meaningful_position=True,
        timestamp=START,
    )
    await runtime.start()
    try:
        order = Order(
            symbol=BTCUSDT,
            side=Side.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("90"),
            created_at=START,
        )
        submitted = await runtime.executor.submit_order(order)
        assert submitted == ExecutionResult.ACCEPTED
        runtime.executor.set_event_time(START)
        runtime.executor.set_price(BTCUSDT, Decimal("90"))
        fills = await runtime.executor.get_fills(order.client_order_id)
        await _publish_paper_fill(runtime, fills, START)
        assert runtime.portfolio.snapshot.held_qty == Decimal("0.2")
        assert runtime.portfolio.snapshot.avg_cost_basis == Decimal("95.045")
        assert runtime.portfolio.snapshot.free_quote == Decimal("980.991")
        assert len(runtime.portfolio.accounting.fill_history) == 1
        assert runtime.handled_fills[-1].order_id == order.client_order_id
        assert runtime.executor._open_orders == {}  # type: ignore[attr-defined]
        assert runtime.executor._active_ocos == {}  # type: ignore[attr-defined]
        assert runtime.status == PaperRuntimeStatus.RUNNING
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_scenario_cost_basis_manual_basis_variant_recovers_and_resumes(tmp_path):
    rest = ScenarioRestClient()
    rest.account_balances["BTC"] = Decimal("0.25")
    rest.account_balances["USDT"] = Decimal("1000")
    runtime, _ = _testnet_runtime(tmp_path, rest, root_name="basis_manual")
    await runtime.start()
    try:
        assert runtime.status == RuntimeStatus.HALT
        assert runtime.basis_recovery_state == BasisRecoveryState.MISSING
        runtime.provide_manual_basis(Decimal("123.45"))
        assert runtime.status == RuntimeStatus.READY
        assert runtime.portfolio.snapshot.avg_cost_basis == Decimal("123.45")
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_scenario_cost_basis_zero_basis_variant_blocks_buy_until_confirmed(tmp_path):
    rest = ScenarioRestClient()
    rest.account_balances["BTC"] = Decimal("0.25")
    rest.account_balances["USDT"] = Decimal("1000")
    runtime, _ = _testnet_runtime(tmp_path, rest, root_name="basis_zero")
    await runtime.start()
    try:
        runtime.declare_zero_basis()
        assert runtime.basis_recovery_state == BasisRecoveryState.ZERO_DECLARED
        assert runtime.zero_basis_buy_blocked is True
        runtime.status = RuntimeStatus.READY
        runtime.current_ask = Decimal("100")
        runtime._order_quantity = lambda intent: (Decimal("0.1"), None)  # type: ignore[method-assign]
        await runtime._execute_intent(
            Intent(type=IntentType.BUY, symbol=BTCUSDT, quantity_mode=QuantityMode.FULL),
            START,
        )
        order_calls = [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
        assert order_calls == []
        runtime.confirm_zero_basis_for_new_entries()
        assert runtime.zero_basis_buy_blocked is False
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_scenario_cost_basis_immediate_close_variant_submits_exit_order(tmp_path):
    rest = ScenarioRestClient()
    rest.account_balances["BTC"] = Decimal("0.25")
    rest.account_balances["USDT"] = Decimal("1000")
    runtime, _ = _testnet_runtime(tmp_path, rest, root_name="basis_close")
    await runtime.start()
    try:
        await runtime.request_missing_basis_immediate_close()
        order_calls = [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
        assert order_calls[-1][2]["side"] == "SELL"
        assert order_calls[-1][2]["type"] == OrderType.MARKET.value
        assert runtime.basis_recovery_state == BasisRecoveryState.CLOSE_PENDING
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_scenario_symbol_change_allowed_when_flat_advances_to_restart_required(tmp_path):
    rest = ScenarioRestClient()
    runtime, _ = _testnet_runtime(tmp_path, rest, root_name="symbol_flat")
    await runtime.start()
    try:
        await runtime.request_symbol_change(ETHUSDT)
        assert runtime.pending_symbol_change == ETHUSDT
        assert runtime.symbol_change_stage == SymbolChangeStage.AWAITING_CONFIG_UPDATE
        runtime.apply_symbol_change_config()
        assert runtime.config.symbol == ETHUSDT
        assert runtime.symbol_change_stage == SymbolChangeStage.RESTART_REQUIRED
        assert runtime.restart_required is True
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_scenario_symbol_change_with_position_blocks_immediate_switch_and_enters_flatten_first_flow(tmp_path):
    rest = ScenarioRestClient()
    runtime, _ = _testnet_runtime(tmp_path, rest, root_name="symbol_position")
    await runtime.start()
    try:
        runtime.portfolio.replace_snapshot(
            held_qty=Decimal("0.2"),
            avg_cost_basis=Decimal("100"),
            free_quote=Decimal("990"),
            is_in_position=True,
            meaningful_position=True,
            timestamp=START,
        )
        await runtime.request_symbol_change(ETHUSDT)
        assert runtime.config.symbol == BTCUSDT
        assert runtime.pending_symbol_change == ETHUSDT
        assert runtime.symbol_change_stage == SymbolChangeStage.AWAITING_ZERO
        with pytest.raises(ValueError):
            runtime.apply_symbol_change_config()
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_scenario_symbol_change_blocked_when_pending_order_or_protection_exists(tmp_path):
    rest = ScenarioRestClient()
    runtime, _ = _testnet_runtime(tmp_path, rest, root_name="symbol_blocked")
    await runtime.start()
    try:
        runtime._set_pending_order(
            Order(
                symbol=BTCUSDT,
                side=Side.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("0.1"),
                created_at=START,
            )
        )
        with pytest.raises(ValueError, match="pending"):
            await runtime.request_symbol_change(ETHUSDT)
        runtime._clear_pending_order()
        runtime.active_oco_order_id = "oco-runtime-1"
        with pytest.raises(ValueError, match="protection"):
            await runtime.request_symbol_change(ETHUSDT)
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_scenario_symbol_change_progression_reaches_config_update_after_flatten_fill(tmp_path):
    rest = ScenarioRestClient()
    runtime, _ = _testnet_runtime(tmp_path, rest, root_name="symbol_progress")
    await runtime.start()
    try:
        runtime.portfolio.replace_snapshot(
            held_qty=Decimal("0.2"),
            avg_cost_basis=Decimal("100"),
            free_quote=Decimal("990"),
            is_in_position=True,
            meaningful_position=True,
            timestamp=START,
        )
        await runtime.request_symbol_change(ETHUSDT)
        sell_fill = Fill(
            order_id=runtime.pending_order_client_id or "symbol-change-sell",
            symbol=BTCUSDT,
            side=Side.SELL,
            base_qty_filled=Decimal("0.2"),
            quote_qty_filled=Decimal("20"),
            fill_price=Decimal("100"),
            commission=Decimal("0.02"),
            commission_asset=CommissionAsset.QUOTE,
            filled_at=START,
        )
        await runtime._handle_user(
            ExecutionReportEvent(
                timestamp=START,
                symbol=BTCUSDT,
                client_order_id=runtime.pending_order_client_id or "symbol-change-sell",
                execution_result=ExecutionResult.FILLED,
                order_status=ExchangeOrderStatus.FILLED,
                fill=sell_fill,
            )
        )
        assert runtime.symbol_change_stage == SymbolChangeStage.AWAITING_CONFIG_UPDATE
        assert runtime.portfolio.snapshot.held_qty == Decimal("0")
        assert runtime.portfolio.snapshot.avg_cost_basis == Decimal("0")
    finally:
        await runtime.shutdown()
