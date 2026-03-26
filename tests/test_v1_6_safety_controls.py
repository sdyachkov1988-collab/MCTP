from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mctp.adapters import BinanceCredentials, BinanceSpotTestnetAdapterV1, BinanceSpotTestnetConfigV1
from mctp.core.constants import (
    CRITICAL_DRAWDOWN_STOP_CODE,
    DELISTING_FORCE_EXIT_REASON,
    WARNING_DAILY_LOSS_LIMIT_CODE,
    WARNING_DRAWDOWN_CODE,
    WARNING_REGIME_UNKNOWN_CODE,
)
from mctp.core.enums import AlertSeverity, IntentType, Market, OperationalMode, RecoveryMode, SymbolChangeStage, Timeframe
from mctp.core.types import Intent, PortfolioSnapshot, Symbol
from mctp.runtime.events import DelistingSignalEvent
from mctp.runtime.strategy_smoke import EmaCrossSmokeStrategy
from mctp.runtime.streams import QueueStreamTransport
from mctp.runtime.testnet import TestnetRuntime, TestnetRuntimeConfig, TestnetRuntimeStatus as RuntimeStatus
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.order_store import OrderStore
from mctp.storage.snapshot_store import SnapshotStore


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
ETHUSDT = Symbol("ETH", "USDT", Market.SPOT)
START = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone.utc)


class FakeRestClient:
    def __init__(self) -> None:
        self.calls = []
        self.account_balances = {"BTC": Decimal("0"), "USDT": Decimal("1000"), "BNB": Decimal("1")}
        self.locked_balances = {"BTC": Decimal("0"), "USDT": Decimal("0"), "BNB": Decimal("0")}
        self.open_oco_order_ids: list[str] = []
        self.order_submit_status = "NEW"

    async def request_json(self, method, path, *, params=None, signed=False):
        safe_params = dict(params or {})
        self.calls.append((method, path, safe_params, signed))
        if path == "/api/v3/order" and method == "POST":
            return {"status": self.order_submit_status}
        if path == "/api/v3/order" and method == "DELETE":
            return {"status": "CANCELED"}
        if path == "/api/v3/order" and method == "GET":
            return {
                "status": "NEW",
                "side": "SELL",
                "executedQty": "0",
                "cummulativeQuoteQty": "0",
                "price": "100",
                "updateTime": int(START.timestamp() * 1000),
            }
        if path == "/api/v3/userDataStream" and method == "POST":
            return {"listenKey": "listen-key"}
        if path == "/api/v3/userDataStream" and method in {"PUT", "DELETE"}:
            return {}
        if path == "/api/v3/orderList/oco":
            return {"orderListId": "oco-1"}
        if path == "/api/v3/openOrderList":
            return [{"orderListId": value} for value in self.open_oco_order_ids]
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


class FakeDelistingDetector:
    def __init__(self, days_until_delisting: int | None) -> None:
        self.days_until_delisting = days_until_delisting

    async def check_symbol(self, symbol: Symbol) -> DelistingSignalEvent:
        return DelistingSignalEvent(
            symbol=symbol,
            listed=False,
            source="fake",
            details="delisting scheduled",
            days_until_delisting=self.days_until_delisting,
        )


def _runtime(tmp_path, rest_client: FakeRestClient) -> tuple[TestnetRuntime, FakeRestClient]:
    adapter = BinanceSpotTestnetAdapterV1(
        BinanceSpotTestnetConfigV1(BinanceCredentials(api_key="k", api_secret="s")),
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
            initial_balances={"BTC": Decimal("0"), "USDT": Decimal("1000")},
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
    runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, avg_cost_basis=Decimal("100"))
    return runtime, rest_client

@pytest.mark.asyncio
async def test_operational_mode_run_pause_close_only_and_stop_are_enforced(tmp_path):
    assert {mode.value for mode in OperationalMode} == {"RUN", "PAUSE_NEW_ENTRIES", "CLOSE_ONLY", "STOP"}


@pytest.mark.asyncio
async def test_run_allows_entries_pause_and_close_only_block_buys_but_allow_sell_and_stop_blocks_all(tmp_path):
    runtime, rest = _runtime(tmp_path, FakeRestClient())
    await runtime.start()
    try:
        runtime.current_ask = Decimal("100")
        runtime._order_quantity = lambda intent: (Decimal("0.1"), None)  # type: ignore[method-assign]
        runtime.operational_mode = OperationalMode.RUN
        await runtime._execute_intent(Intent(type=IntentType.BUY, symbol=BTCUSDT), START)
        post_calls = [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
        assert len(post_calls) == 1
        runtime._clear_pending_order()

        rest.calls.clear()
        runtime.operational_mode = OperationalMode.PAUSE_NEW_ENTRIES
        await runtime._execute_intent(Intent(type=IntentType.BUY, symbol=BTCUSDT), START)
        assert [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"] == []

        runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, held_qty=Decimal("0.2"), is_in_position=True, meaningful_position=True)
        await runtime._execute_intent(Intent(type=IntentType.SELL, symbol=BTCUSDT), START)
        assert len([call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]) == 1
        runtime._clear_pending_order()

        rest.calls.clear()
        runtime.operational_mode = OperationalMode.CLOSE_ONLY
        await runtime._execute_intent(Intent(type=IntentType.BUY, symbol=BTCUSDT), START)
        assert [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"] == []
        await runtime._execute_intent(Intent(type=IntentType.SELL, symbol=BTCUSDT), START)
        assert len([call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]) == 1
        runtime._clear_pending_order()

        rest.calls.clear()
        runtime.operational_mode = OperationalMode.STOP
        await runtime._execute_intent(Intent(type=IntentType.SELL, symbol=BTCUSDT), START)
        assert [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"] == []
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_drawdown_warning_and_stop_controls_with_manual_resume(tmp_path):
    runtime, _ = _runtime(tmp_path, FakeRestClient())
    await runtime.start()
    try:
        runtime._peak_equity = Decimal("100")
        runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, free_quote=Decimal("89"))
        runtime._evaluate_safety_controls(START)
        assert runtime.last_alert is not None
        assert runtime.last_alert.code == WARNING_DRAWDOWN_CODE
        assert runtime._effective_risk_multipliers().loss_mult == Decimal("0.5")

        runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, free_quote=Decimal("79"))
        runtime._evaluate_safety_controls(START)
        assert runtime.last_alert is not None
        assert runtime.last_alert.code == CRITICAL_DRAWDOWN_STOP_CODE
        assert runtime.operational_mode == OperationalMode.STOP
        assert runtime.status == RuntimeStatus.HALT
        runtime.manual_resume_after_stop()
        assert runtime.operational_mode == OperationalMode.RUN
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_daily_loss_and_consecutive_losses_controls_are_enforced(tmp_path):
    runtime, _ = _runtime(tmp_path, FakeRestClient())
    await runtime.start()
    try:
        for _ in range(3):
            runtime.adaptive_risk.on_trade_result(Decimal("-1"), Decimal("99"), now=START)
        runtime._evaluate_safety_controls(START)
        assert runtime._effective_risk_multipliers().loss_mult == Decimal("0.5")

        runtime.adaptive_risk.on_trade_result(Decimal("-30"), Decimal("69"), now=START)
        runtime._evaluate_safety_controls(START)
        assert runtime.last_alert is not None
        assert runtime.last_alert.code == WARNING_DAILY_LOSS_LIMIT_CODE
        assert runtime.operational_mode == OperationalMode.PAUSE_NEW_ENTRIES

        runtime.adaptive_risk._consecutive_losses = 5
        runtime._evaluate_safety_controls(START)
        assert runtime.operational_mode == OperationalMode.PAUSE_NEW_ENTRIES
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_recovery_mode_is_present_and_logging_only_on_testnet(tmp_path):
    runtime, _ = _runtime(tmp_path, FakeRestClient())
    await runtime.start()
    try:
        runtime._peak_equity = Decimal("100")
        runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, free_quote=Decimal("70"))
        runtime._evaluate_safety_controls(START)
        assert runtime.recovery_mode_controller.mode == RecoveryMode.NORMAL
        assert runtime.recovery_mode_controller.live_activation_enabled is False
        assert runtime.recovery_mode_controller.last_logged_reason is not None
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_bnb_guard_and_symbol_change_protection_are_enforced(tmp_path):
    from mctp.core.enums import IntentType

    runtime, rest = _runtime(tmp_path, FakeRestClient())
    await runtime.start()
    try:
        runtime.balance_cache_store.save({"BNB": Decimal("0.001"), "USDT": Decimal("1000")}, START)
        runtime._evaluate_safety_controls(START)
        assert runtime.operational_mode == OperationalMode.PAUSE_NEW_ENTRIES

        runtime.current_ask = Decimal("100")
        runtime._order_quantity = lambda intent: (Decimal("0.1"), None)  # type: ignore[method-assign]
        await runtime._execute_intent(Intent(type=IntentType.BUY, symbol=BTCUSDT), START)
        assert [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"] == []

        runtime.pending_symbol_change = ETHUSDT
        runtime.symbol_change_stage = SymbolChangeStage.AWAITING_CONFIG_UPDATE
        runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, held_qty=Decimal("0.1"))
        with pytest.raises(ValueError):
            runtime.apply_symbol_change_config()
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_delisting_force_exit_triggers_market_sell_regardless_of_strategy(tmp_path):
    rest = FakeRestClient()
    rest.account_balances["BTC"] = Decimal("0.2")
    runtime, rest = _runtime(tmp_path, rest)
    runtime.detector = FakeDelistingDetector(days_until_delisting=2)
    runtime.portfolio._snapshot = replace(
        runtime.portfolio.snapshot,
        held_qty=Decimal("0.2"),
        is_in_position=True,
        meaningful_position=True,
        avg_cost_basis=Decimal("100"),
    )
    await runtime.start()
    try:
        post_calls = [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
        assert post_calls[-1][2]["side"] == "SELL"
        assert post_calls[-1][2]["type"] == "MARKET"
        assert post_calls[-1][2]["newClientOrderId"]
        assert runtime.operational_mode == OperationalMode.CLOSE_ONLY
        runtime._clear_pending_order()
        runtime.status = RuntimeStatus.READY

        runtime.portfolio._snapshot = replace(
            runtime.portfolio.snapshot,
            held_qty=Decimal("0"),
            is_in_position=False,
            meaningful_position=False,
            free_quote=Decimal("1020"),
        )
        runtime._evaluate_safety_controls(START)
        assert runtime.operational_mode == OperationalMode.CLOSE_ONLY

        rest.calls.clear()
        runtime.current_ask = Decimal("100")
        runtime._order_quantity = lambda intent: (Decimal("0.1"), None)  # type: ignore[method-assign]
        await runtime._execute_intent(Intent(type=IntentType.BUY, symbol=BTCUSDT), START)
        assert [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"] == []

        runtime.portfolio._snapshot = replace(
            runtime.portfolio.snapshot,
            held_qty=Decimal("0.1"),
            is_in_position=True,
            meaningful_position=True,
            avg_cost_basis=Decimal("100"),
        )
        await runtime._execute_intent(Intent(type=IntentType.SELL, symbol=BTCUSDT), START)
        post_calls = [call for call in rest.calls if call[1] == "/api/v3/order" and call[0] == "POST"]
        assert len(post_calls) == 1
        assert post_calls[0][2]["side"] == "SELL"
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_regime_unknown_forces_pause_and_zero_size_priority(tmp_path):
    runtime, _ = _runtime(tmp_path, FakeRestClient())
    await runtime.start()
    try:
        runtime.set_regime_state(True, regime_mult=Decimal("1"), anomaly_mult=Decimal("5"))
        assert runtime.operational_mode == OperationalMode.PAUSE_NEW_ENTRIES
        multipliers = runtime._effective_risk_multipliers()
        assert multipliers.regime_mult == Decimal("0")
        assert multipliers.combined() == Decimal("0")
        assert runtime.last_alert is not None
        assert runtime.last_alert.code == WARNING_REGIME_UNKNOWN_CODE

        runtime.current_ask = Decimal("100")
        quantity, _ = runtime._order_quantity(Intent(type=IntentType.BUY, symbol=BTCUSDT))
        assert quantity is None
    finally:
        await runtime.shutdown()
