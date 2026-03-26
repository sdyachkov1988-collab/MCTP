from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mctp.adapters import BinanceCredentials, BinanceSpotTestnetAdapterV1, BinanceSpotTestnetConfigV1
from mctp.core.constants import CRITICAL_EXTERNAL_OCO_CANCEL_CODE
from mctp.core.enums import AlertSeverity, Market, ProtectionMode, Side, SymbolChangeStage, Timeframe
from mctp.core.types import Symbol
from mctp.execution.oco import OCOOrder
from mctp.runtime.events import RuntimeAlertEvent
from mctp.runtime.strategy_smoke import EmaCrossSmokeStrategy
from mctp.runtime.streams import QueueStreamTransport
from mctp.runtime.testnet import TestnetRuntime, TestnetRuntimeConfig
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.order_store import OrderStore
from mctp.storage.snapshot_store import SnapshotStore
from mctp.streams.base import StreamType


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
ETHUSDT = Symbol("ETH", "USDT", Market.SPOT)
START = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone.utc)


class FakeRestClient:
    def __init__(self) -> None:
        self.calls = []
        self.account_balances = {"BTC": Decimal("0.25"), "USDT": Decimal("1000")}
        self.locked_balances = {"BTC": Decimal("0"), "USDT": Decimal("0")}
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


def _config():
    return BinanceSpotTestnetConfigV1(
        credentials=BinanceCredentials(api_key="k", api_secret="s")
    )


def _runtime(tmp_path, rest_client: FakeRestClient, **transports):
    adapter = BinanceSpotTestnetAdapterV1(
        _config(),
        rest_client=rest_client,
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        order_store=OrderStore(str(tmp_path / "orders.json")),
    )
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
            initial_balances={"BTC": Decimal("0"), "USDT": Decimal("5")},
        ),
        strategy=EmaCrossSmokeStrategy(),
        executor=adapter,
        snapshot_store=SnapshotStore(str(tmp_path / "snapshot.json")),
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        accounting_store=AccountingStore(str(tmp_path / "accounting.json")),
        kline_transport=transports.get("kline_transport", QueueStreamTransport()),
        book_transport=transports.get("book_transport", QueueStreamTransport()),
        bnb_transport=transports.get("bnb_transport", QueueStreamTransport()),
        user_transport=transports.get("user_transport", QueueStreamTransport()),
    )
    return adapter, runtime


@pytest.mark.asyncio
async def test_exchange_balances_authoritative_for_balance_cache_at_startup(tmp_path):
    rest = FakeRestClient()
    rest.account_balances = {"BTC": Decimal("0"), "USDT": Decimal("777")}
    rest.locked_balances = {"BTC": Decimal("0.5"), "USDT": Decimal("123")}
    adapter, runtime = _runtime(tmp_path, rest)
    await runtime.start()
    try:
        cached = runtime.balance_cache_store.load()
        assert cached is not None
        balances, _ = cached
        assert balances["BTC"] == Decimal("0")
        assert balances["USDT"] == Decimal("777")
        assert runtime.portfolio.snapshot.held_qty == Decimal("0.5")
        assert runtime.portfolio.snapshot.free_quote == Decimal("777")
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_exchange_balances_authoritative_after_reconnect_and_bounded(tmp_path):
    rest = FakeRestClient()
    adapter, runtime = _runtime(
        tmp_path,
        rest,
        kline_transport=FailingOnceTransport(None),
        book_transport=FailingOnceTransport(None),
    )
    await runtime.start()
    try:
        runtime.portfolio._snapshot = replace(
            runtime.portfolio.snapshot,
            avg_cost_basis=Decimal("100"),
            scale_in_count=2,
        )
        rest.account_balances = {"BTC": Decimal("0"), "USDT": Decimal("888")}
        rest.locked_balances = {"BTC": Decimal("0.1"), "USDT": Decimal("11")}
        await runtime.process_all_available()
        account_calls = [call for call in rest.calls if call[1] == "/api/v3/account"]
        assert len(account_calls) == 2
        assert runtime.portfolio.snapshot.held_qty == Decimal("0.1")
        assert runtime.portfolio.snapshot.free_quote == Decimal("888")
        assert runtime.portfolio.snapshot.avg_cost_basis == Decimal("100")
        assert runtime.portfolio.snapshot.scale_in_count == 2
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_basis_resets_only_when_effective_held_base_is_truly_zero(tmp_path):
    rest = FakeRestClient()
    adapter, runtime = _runtime(
        tmp_path,
        rest,
        kline_transport=FailingOnceTransport(None),
    )
    await runtime.start()
    try:
        runtime.portfolio._snapshot = replace(
            runtime.portfolio.snapshot,
            avg_cost_basis=Decimal("100"),
            scale_in_count=3,
        )
        rest.account_balances = {"BTC": Decimal("0"), "USDT": Decimal("999")}
        rest.locked_balances = {"BTC": Decimal("0"), "USDT": Decimal("0")}
        await runtime.process_all_available()
        assert runtime.portfolio.snapshot.held_qty == Decimal("0")
        assert runtime.portfolio.snapshot.avg_cost_basis == Decimal("0")
        assert runtime.portfolio.snapshot.scale_in_count == 0
        assert runtime.portfolio.snapshot.free_quote == Decimal("999")
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_external_oco_cancellation_reactivates_software_stop_and_raises_critical(tmp_path):
    rest = FakeRestClient()
    rest.account_balances = {"BTC": Decimal("0.5"), "USDT": Decimal("1000")}
    _, runtime = _runtime(tmp_path, rest)
    await runtime.start()
    try:
        runtime.current_bid = Decimal("100")
        runtime.channels[StreamType.BOOK_TICKER].touch(START)
        runtime.channels[StreamType.BOOK_TICKER].state.is_stale = False
        await runtime.submit_oco(
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
        assert runtime.protection_mode == ProtectionMode.EXCHANGE_OCO
        assert runtime.software_stop_active is False
        await runtime.channels[StreamType.USER_DATA].publish(
            {
                "e": "listStatus",
                "E": 1774310400000,
                "s": "BTCUSDT",
                "g": "oco-1",
                "l": "ALL_DONE",
                "L": "ALL_DONE",
                "c": "OCO",
            }
        )
        await runtime.process_all_available()
        assert runtime.active_oco_order_id is None
        assert runtime.protection_mode == ProtectionMode.SOFTWARE_STOP
        assert runtime.software_stop_active is True
        assert isinstance(runtime.last_alert, RuntimeAlertEvent)
        assert runtime.last_alert.severity == AlertSeverity.CRITICAL
        assert runtime.last_alert.code == CRITICAL_EXTERNAL_OCO_CANCEL_CODE
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_symbol_change_procedure_enforces_sell_zero_reset_config_restart(tmp_path):
    rest = FakeRestClient()
    rest.account_balances = {"BTC": Decimal("0.5"), "USDT": Decimal("1000")}
    _, runtime = _runtime(tmp_path, rest)
    await runtime.start()
    try:
        runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, avg_cost_basis=Decimal("100"))
        await runtime.request_symbol_change(ETHUSDT)
        order_calls = [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
        assert order_calls[-1][2]["side"] == Side.SELL.value
        assert runtime.symbol_change_stage == SymbolChangeStage.AWAITING_ZERO
        assert runtime.config.symbol == BTCUSDT
        with pytest.raises(ValueError):
            runtime.apply_symbol_change_config()
        await runtime.channels[StreamType.USER_DATA].publish(
            {
                "e": "executionReport",
                "E": 1774310400000,
                "T": 1774310400000,
                "s": "BTCUSDT",
                "c": order_calls[-1][2]["newClientOrderId"],
                "X": "FILLED",
                "S": "SELL",
                "l": "0.5",
                "L": "100",
                "n": "0",
                "N": "QUOTE",
                "i": "exchange-order-1",
                "t": "trade-1",
            }
        )
        await runtime.process_all_available()
        assert runtime.portfolio.snapshot.held_qty == Decimal("0")
        assert runtime.portfolio.snapshot.avg_cost_basis == Decimal("0")
        assert runtime.symbol_change_stage == SymbolChangeStage.AWAITING_CONFIG_UPDATE
        runtime.apply_symbol_change_config()
        assert runtime.config.symbol == ETHUSDT
        assert runtime.symbol_change_stage == SymbolChangeStage.RESTART_REQUIRED
        assert runtime.restart_required is True
    finally:
        await runtime.shutdown()
