from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mctp.adapters import BinanceCredentials, BinanceSpotTestnetAdapterV1, BinanceSpotTestnetConfigV1
from mctp.core.constants import CRITICAL_STARTUP_PROTECTION_CONFLICT_CODE, WARNING_ZERO_BASIS_CODE
from mctp.core.enums import AlertSeverity, BasisRecoveryState, IntentType, Market, OrderType, ProtectionMode, Timeframe
from mctp.core.types import Intent, Symbol
from mctp.execution.oco import OCOOrder
from mctp.runtime.strategy_smoke import EmaCrossSmokeStrategy
from mctp.runtime.streams import QueueStreamTransport
from mctp.runtime.testnet import TestnetRuntime, TestnetRuntimeConfig, TestnetRuntimeStatus as RuntimeStatus
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.order_store import OrderStore
from mctp.storage.snapshot_store import SnapshotStore


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
START = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone.utc)


class FakeRestClient:
    def __init__(self) -> None:
        self.calls = []
        self.account_balances = {"BTC": Decimal("0.25"), "USDT": Decimal("1000")}
        self.locked_balances = {"BTC": Decimal("0"), "USDT": Decimal("0")}
        self.open_oco_order_ids: list[str] = []
        self.order_submit_status = "NEW"

    async def request_json(self, method, path, *, params=None, signed=False):
        safe_params = dict(params or {})
        self.calls.append((method, path, safe_params, signed))
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


class OneShotBookTransport(QueueStreamTransport):
    def __init__(self, payload):
        super().__init__()
        self._payload = payload

    async def receive(self):
        if self._payload is None:
            return None
        payload = self._payload
        self._payload = None
        return payload


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
    book_transport = config_overrides.pop("book_transport", QueueStreamTransport())
    adapter = BinanceSpotTestnetAdapterV1(
        BinanceSpotTestnetConfigV1(BinanceCredentials(api_key="k", api_secret="s")),
        rest_client=rest_client,
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        order_store=OrderStore(str(tmp_path / "orders.json")),
    )
    runtime = TestnetRuntime(
        config=_config(**config_overrides),
        strategy=EmaCrossSmokeStrategy(),
        executor=adapter,
        snapshot_store=SnapshotStore(str(tmp_path / "snapshot.json")),
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        accounting_store=AccountingStore(str(tmp_path / "accounting.json")),
        kline_transport=QueueStreamTransport(),
        book_transport=book_transport,
        bnb_transport=QueueStreamTransport(),
        user_transport=QueueStreamTransport(),
    )
    return adapter, runtime


def _save_local_oco(runtime: TestnetRuntime, list_order_id: str) -> None:
    runtime.executor._order_store.save_oco(  # type: ignore[attr-defined]
        OCOOrder(
            symbol=BTCUSDT,
            tp_price=Decimal("110"),
            sl_stop_price=Decimal("95"),
            sl_limit_price=Decimal("94"),
            quantity=Decimal("0.25"),
            list_order_id=list_order_id,
            created_at=START,
            updated_at=START,
        )
    )


@pytest.mark.asyncio
async def test_runtime_does_not_enter_ready_before_startup_checks_complete(tmp_path):
    rest = FakeRestClient()
    _, runtime = _runtime(tmp_path, rest)
    runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, avg_cost_basis=Decimal("100"))
    observed_statuses: list[RuntimeStatus] = []

    async def wrapped_startup_sync():
        observed_statuses.append(runtime.status)

    runtime._run_startup_sync = wrapped_startup_sync  # type: ignore[method-assign]

    await runtime.start()
    try:
        assert observed_statuses == [RuntimeStatus.STARTING]
        assert runtime.status == RuntimeStatus.READY
        assert runtime.startup_checks_completed is True
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_startup_refreshes_balance_cache_from_rest_before_ready(tmp_path):
    rest = FakeRestClient()
    _, runtime = _runtime(tmp_path, rest)
    runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, avg_cost_basis=Decimal("100"))
    await runtime.start()
    try:
        cached = runtime.balance_cache_store.load()
        assert cached is not None
        balances, _ = cached
        assert balances["BTC"] == Decimal("0.25")
        assert balances["USDT"] == Decimal("1000")
        assert runtime.status == RuntimeStatus.READY
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_missing_basis_with_open_position_defaults_to_halt(tmp_path):
    rest = FakeRestClient()
    _, runtime = _runtime(tmp_path, rest)
    await runtime.start()
    try:
        assert runtime.status == RuntimeStatus.HALT
        assert runtime.basis_recovery_state == BasisRecoveryState.MISSING
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_manual_basis_recovery_path_works(tmp_path):
    rest = FakeRestClient()
    _, runtime = _runtime(tmp_path, rest)
    await runtime.start()
    try:
        runtime.provide_manual_basis(Decimal("123.45"))
        assert runtime.portfolio.snapshot.avg_cost_basis == Decimal("123.45")
        assert runtime.basis_recovery_state == BasisRecoveryState.MANUAL
        assert runtime.status == RuntimeStatus.READY
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_zero_basis_declaration_warns_and_blocks_new_buy_entries_until_confirmed(tmp_path):
    rest = FakeRestClient()
    _, runtime = _runtime(tmp_path, rest)
    await runtime.start()
    try:
        runtime.declare_zero_basis()
        assert runtime.status == RuntimeStatus.READY
        assert runtime.last_alert is not None
        assert runtime.last_alert.severity == AlertSeverity.WARNING
        assert runtime.last_alert.code == WARNING_ZERO_BASIS_CODE
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_zero_basis_blocks_buy_until_explicit_confirmation(tmp_path):
    rest = FakeRestClient()
    _, runtime = _runtime(tmp_path, rest)
    await runtime.start()
    try:
        runtime.declare_zero_basis()
        await runtime._execute_intent(Intent(type=IntentType.BUY, symbol=BTCUSDT), START)
        order_calls = [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
        assert order_calls == []
        runtime.confirm_zero_basis_for_new_entries()
        assert runtime.zero_basis_buy_blocked is False
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_immediate_close_path_for_missing_basis_is_supported(tmp_path):
    rest = FakeRestClient()
    _, runtime = _runtime(tmp_path, rest)
    await runtime.start()
    try:
        await runtime.request_missing_basis_immediate_close()
        order_calls = [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
        assert order_calls[-1][2]["side"] == "SELL"
        assert order_calls[-1][2]["type"] == OrderType.MARKET.value
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_startup_oco_present_deactivates_software_stop_primary_protection(tmp_path):
    rest = FakeRestClient()
    rest.open_oco_order_ids = ["oco-exchange-1"]
    _, runtime = _runtime(tmp_path, rest, startup_software_stop_active=False)
    _save_local_oco(runtime, "oco-exchange-1")
    runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, avg_cost_basis=Decimal("100"))
    await runtime.start()
    try:
        assert runtime.protection_mode == ProtectionMode.EXCHANGE_OCO
        assert runtime.software_stop_active is False
        assert runtime.active_oco_order_id == "oco-exchange-1"
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_startup_conflict_where_both_oco_and_software_stop_are_active_raises_critical(tmp_path):
    rest = FakeRestClient()
    rest.open_oco_order_ids = ["oco-exchange-1"]
    _, runtime = _runtime(tmp_path, rest, startup_software_stop_active=True)
    _save_local_oco(runtime, "oco-exchange-1")
    runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, avg_cost_basis=Decimal("100"))
    await runtime.start()
    try:
        assert runtime.status == RuntimeStatus.HALT
        assert runtime.last_alert is not None
        assert runtime.last_alert.severity == AlertSeverity.CRITICAL
        assert runtime.last_alert.code == CRITICAL_STARTUP_PROTECTION_CONFLICT_CODE
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_restart_gap_risk_chooses_market_sell_when_best_bid_is_close_to_stop(tmp_path):
    rest = FakeRestClient()
    _, runtime = _runtime(
        tmp_path,
        rest,
        startup_software_trailing_active=True,
        startup_stop_price=Decimal("100"),
        book_transport=OneShotBookTransport(
            {"u": 1, "E": 1774310400000, "b": "99", "a": "99.1"}
        ),
    )
    runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, avg_cost_basis=Decimal("100"))
    await runtime.start()
    try:
        order_calls = [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
        assert order_calls[-1][2]["type"] == OrderType.MARKET.value
        assert runtime.status == RuntimeStatus.HALT
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_restart_gap_risk_chooses_limit_sell_at_stop_price_otherwise(tmp_path):
    rest = FakeRestClient()
    _, runtime = _runtime(
        tmp_path,
        rest,
        startup_software_trailing_active=True,
        startup_stop_price=Decimal("100"),
        book_transport=OneShotBookTransport(
            {"u": 1, "E": 1774310400000, "b": "90", "a": "90.1"}
        ),
    )
    runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, avg_cost_basis=Decimal("100"))
    await runtime.start()
    try:
        order_calls = [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
        assert order_calls[-1][2]["type"] == OrderType.LIMIT.value
        assert order_calls[-1][2]["price"] == "100"
        assert runtime.status == RuntimeStatus.HALT
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_open_position_with_startup_trailing_and_no_exchange_oco_places_immediate_oco_and_blocks_ready_when_bid_unavailable(tmp_path):
    rest = FakeRestClient()
    _, runtime = _runtime(
        tmp_path,
        rest,
        startup_software_trailing_active=True,
        startup_stop_price=Decimal("100"),
    )
    runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, avg_cost_basis=Decimal("100"))
    await runtime.start()
    try:
        oco_calls = [call for call in rest.calls if call[1] == "/api/v3/orderList/oco" and call[0] == "POST"]
        order_calls = [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
        assert len(oco_calls) == 1
        assert order_calls == []
        assert runtime.protection_mode == ProtectionMode.EXCHANGE_OCO
        assert runtime.status == RuntimeStatus.HALT
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_startup_gap_risk_uses_real_startup_book_bid_without_external_preseed(tmp_path):
    rest = FakeRestClient()
    _, runtime = _runtime(
        tmp_path,
        rest,
        startup_software_trailing_active=True,
        startup_stop_price=Decimal("100"),
        book_transport=OneShotBookTransport(
            {"u": 1, "E": 1774310400000, "b": "98.5", "a": "98.6"}
        ),
    )
    runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, avg_cost_basis=Decimal("100"))
    assert runtime.current_bid is None
    await runtime.start()
    try:
        order_calls = [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
        oco_calls = [call for call in rest.calls if call[1] == "/api/v3/orderList/oco" and call[0] == "POST"]
        assert runtime.current_bid == Decimal("98.5")
        assert len(order_calls) == 1
        assert oco_calls == []
        assert runtime.status == RuntimeStatus.HALT
    finally:
        await runtime.shutdown()
