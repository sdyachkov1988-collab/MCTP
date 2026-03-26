import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from mctp.adapters import BinanceCredentials, BinanceSpotTestnetAdapterV1, BinanceSpotTestnetConfigV1
from mctp.core.constants import (
    CRITICAL_EXTERNAL_OCO_CANCEL_CODE,
    CRITICAL_HEARTBEAT_TIMEOUT_CODE,
    CRITICAL_IP_BAN_CODE,
    CRITICAL_MISSING_BASIS_CODE,
    CRITICAL_RUNTIME_CRASH_CODE,
    CRITICAL_STARTUP_PROTECTION_CONFLICT_CODE,
    CRITICAL_STORAGE_UNAVAILABLE_CODE,
    INFO_CLOCK_DRIFT_CODE,
    INFO_DELISTING_ANNOUNCED_CODE,
    INFO_POST_ONLY_REJECTED_CODE,
    INFO_STALE_BNBUSDT_CODE,
    INFO_STALE_BOOK_TICKER_CODE,
    WARNING_BNB_NEAR_ZERO_CODE,
    WARNING_CONSECUTIVE_LOSSES_CODE,
    WARNING_DRAWDOWN_CODE,
    WARNING_PERSISTENT_DUST_CODE,
    WARNING_STALE_KLINE_CODE,
    WARNING_STRATEGY_DEGRADATION_CODE,
)
from mctp.core.enums import AlertSeverity, ContingencyType, ListOrderStatus, ListStatusType, Market, ProtectionMode, Timeframe
from mctp.core.types import PortfolioSnapshot, Symbol
from mctp.execution.oco import OCOOrder
from mctp.runtime import AlertDispatcher, MemoryAlertChannel
from mctp.runtime.alerting import AlertChannel
from mctp.runtime.events import OCOListStatusEvent
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
        self.account_balances = {"BTC": Decimal("0.25"), "USDT": Decimal("1000"), "BNB": Decimal("1")}
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


class FailingAlertChannel(AlertChannel):
    def __init__(self, name: str) -> None:
        self.name = name

    def deliver(self, alert) -> None:
        raise RuntimeError("primary delivery failed")


class FakeDelistingDetector:
    async def check_symbol(self, symbol: Symbol):
        class Signal:
            listed = False
            details = "Delisting announced"

        return Signal()


def _runtime(tmp_path, rest_client: FakeRestClient, *, alert_dispatcher: AlertDispatcher | None = None, **config_overrides) -> TestnetRuntime:
    adapter = BinanceSpotTestnetAdapterV1(
        BinanceSpotTestnetConfigV1(BinanceCredentials(api_key="k", api_secret="s")),
        rest_client=rest_client,
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        order_store=OrderStore(str(tmp_path / "orders.json")),
    )
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
    for key, value in config_overrides.items():
        setattr(config, key, value)
    runtime = TestnetRuntime(
        config=config,
        strategy=EmaCrossSmokeStrategy(),
        executor=adapter,
        snapshot_store=SnapshotStore(str(tmp_path / "snapshot.json")),
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        accounting_store=AccountingStore(str(tmp_path / "accounting.json")),
        kline_transport=QueueStreamTransport(),
        book_transport=QueueStreamTransport(),
        bnb_transport=QueueStreamTransport(),
        user_transport=QueueStreamTransport(),
        alert_dispatcher=alert_dispatcher,
    )
    return runtime


def _latest_alert_code(runtime: TestnetRuntime) -> str:
    assert runtime.last_alert is not None
    return runtime.last_alert.code


@pytest.mark.asyncio
async def test_alert_model_supports_all_v15_severities(tmp_path):
    runtime = _runtime(tmp_path, FakeRestClient())
    runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, avg_cost_basis=Decimal("100"))
    await runtime.start()
    try:
        runtime._raise_alert(AlertSeverity.INFO, INFO_CLOCK_DRIFT_CODE, "info")
        assert runtime.last_alert is not None
        assert runtime.last_alert.severity == AlertSeverity.INFO
        runtime._raise_alert(AlertSeverity.WARNING, WARNING_DRAWDOWN_CODE, "warn")
        assert runtime.last_alert.severity == AlertSeverity.WARNING
        runtime._raise_alert(AlertSeverity.CRITICAL, CRITICAL_RUNTIME_CRASH_CODE, "crit")
        assert runtime.last_alert.severity == AlertSeverity.CRITICAL
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_critical_alert_conditions_are_generated(tmp_path):
    runtime = _runtime(tmp_path, FakeRestClient())
    await runtime.start()
    try:
        runtime._handle_runtime_exception(RuntimeError("418 IP ban"))
        assert _latest_alert_code(runtime) == CRITICAL_IP_BAN_CODE

        runtime._handle_runtime_exception(RuntimeError("runtime exploded"))
        assert _latest_alert_code(runtime) == CRITICAL_RUNTIME_CRASH_CODE

        runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, held_qty=Decimal("0.25"), avg_cost_basis=Decimal("0"))
        runtime._check_missing_basis_at_startup()
        assert _latest_alert_code(runtime) == CRITICAL_MISSING_BASIS_CODE

        runtime.executor._order_store.save_oco(  # type: ignore[attr-defined]
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
        runtime._apply_startup_oco_consistency(["oco-1"])
        runtime.config.startup_software_stop_active = True
        runtime._apply_startup_oco_consistency(["oco-1"])
        assert _latest_alert_code(runtime) == CRITICAL_STARTUP_PROTECTION_CONFLICT_CODE

        runtime.active_oco_order_id = "oco-1"
        runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, held_qty=Decimal("0.25"))
        runtime._handle_oco_status(
            OCOListStatusEvent(
                timestamp=START,
                symbol=BTCUSDT,
                list_order_id="oco-1",
                list_status_type=ListStatusType.ALL_DONE,
                list_order_status=ListOrderStatus.ALL_DONE,
                contingency_type=ContingencyType.OCO,
            )
        )
        assert _latest_alert_code(runtime) == CRITICAL_EXTERNAL_OCO_CANCEL_CODE

        runtime.last_heartbeat_at = START
        runtime.check_heartbeat_timeout(START + timedelta(seconds=11))
        assert _latest_alert_code(runtime) == CRITICAL_HEARTBEAT_TIMEOUT_CODE

        def broken_save(snapshot):
            raise OSError("disk offline")

        runtime.snapshot_store.save = broken_save  # type: ignore[method-assign]
        runtime._save_snapshot_or_alert()
        assert _latest_alert_code(runtime) == CRITICAL_STORAGE_UNAVAILABLE_CODE
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_warning_alert_conditions_are_generated(tmp_path):
    runtime = _runtime(tmp_path, FakeRestClient())
    runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, avg_cost_basis=Decimal("100"))
    await runtime.start()
    try:
        runtime.current_bid = Decimal("80")
        runtime.portfolio._snapshot = replace(
            runtime.portfolio.snapshot,
            held_qty=Decimal("1"),
            free_quote=Decimal("0"),
            avg_cost_basis=Decimal("100"),
            is_in_position=True,
            meaningful_position=True,
        )
        runtime.adaptive_risk._daily_start_equity = Decimal("100")
        runtime._evaluate_warning_conditions()
        assert _latest_alert_code(runtime) == WARNING_DRAWDOWN_CODE

        runtime.adaptive_risk._consecutive_losses = 3
        runtime._evaluate_warning_conditions()
        assert _latest_alert_code(runtime) == WARNING_CONSECUTIVE_LOSSES_CODE

        runtime.channels[StreamType.KLINE].touch(START)
        await runtime.evaluate_staleness(START + timedelta(seconds=121))
        assert _latest_alert_code(runtime) == WARNING_STALE_KLINE_CODE

        runtime.balance_cache_store.save({"BNB": Decimal("0.001"), "USDT": Decimal("1000")}, START)
        runtime._evaluate_warning_conditions()
        assert _latest_alert_code(runtime) == WARNING_BNB_NEAR_ZERO_CODE

        for value in [
            Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"),
            Decimal("1"), Decimal("1"),
            Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"),
            Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"),
        ]:
            runtime.observability.performance_monitor.observe_trade(value)
        runtime._evaluate_warning_conditions()
        assert _latest_alert_code(runtime) == WARNING_STRATEGY_DEGRADATION_CODE

        runtime.portfolio._snapshot = replace(
            runtime.portfolio.snapshot,
            held_qty=Decimal("0.001"),
            avg_cost_basis=Decimal("100"),
            is_in_position=True,
            meaningful_position=False,
        )
        runtime._evaluate_warning_conditions()
        assert _latest_alert_code(runtime) == WARNING_PERSISTENT_DUST_CODE
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_info_alert_conditions_are_generated(tmp_path):
    runtime = _runtime(tmp_path, FakeRestClient())
    runtime.detector = FakeDelistingDetector()
    runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, avg_cost_basis=Decimal("100"))
    await runtime.start()
    try:
        runtime.channels[StreamType.BOOK_TICKER].touch(START)
        await runtime.evaluate_staleness(START + timedelta(seconds=16))
        assert _latest_alert_code(runtime) == INFO_STALE_BOOK_TICKER_CODE

        runtime.channels[StreamType.BNB_TICKER].touch(START)
        await runtime.evaluate_staleness(START + timedelta(seconds=31))
        assert _latest_alert_code(runtime) == INFO_STALE_BNBUSDT_CODE

        await runtime._check_delisting()
        assert _latest_alert_code(runtime) == INFO_DELISTING_ANNOUNCED_CODE

        runtime._observe_clock_drift(START, now=START + timedelta(seconds=6))
        assert _latest_alert_code(runtime) == INFO_CLOCK_DRIFT_CODE

        runtime.report_post_only_rejected("cid-1")
        assert _latest_alert_code(runtime) == INFO_POST_ONLY_REJECTED_CODE
    finally:
        await runtime.shutdown()


def test_primary_and_backup_alert_delivery_paths_are_real():
    primary = MemoryAlertChannel("primary")
    backup = MemoryAlertChannel("backup")
    dispatcher = AlertDispatcher(primary, backup)
    alert = dispatcher.dispatch(START, AlertSeverity.INFO, "X", "ok", "BTCUSDT")
    assert alert.delivered_via == ("primary",)
    assert len(primary.records) == 1
    assert len(backup.records) == 0

    fallback_dispatcher = AlertDispatcher(FailingAlertChannel("primary"), backup)
    fallback = fallback_dispatcher.dispatch(START, AlertSeverity.WARNING, "Y", "fallback", "BTCUSDT")
    assert fallback.delivered_via == ("backup",)
    assert len(backup.records) == 1


@pytest.mark.asyncio
async def test_heartbeat_timeout_is_triggered_by_real_runtime_watchdog(tmp_path):
    primary = MemoryAlertChannel("primary")
    backup = MemoryAlertChannel("backup")
    runtime = _runtime(
        tmp_path,
        FakeRestClient(),
        alert_dispatcher=AlertDispatcher(primary, backup),
    )
    runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, avg_cost_basis=Decimal("100"))
    await runtime.start()
    try:
        assert runtime._heartbeat_watchdog_task is not None
        assert runtime._heartbeat_watchdog_task.done() is False
        assert runtime._heartbeat_task is not None
        runtime._heartbeat_task.cancel()
        try:
            await runtime._heartbeat_task
        except asyncio.CancelledError:
            pass
        runtime.last_heartbeat_at = datetime.now(timezone.utc) - timedelta(seconds=11)
        await asyncio.sleep(runtime.config.heartbeat_watchdog_interval_seconds + 0.2)
        assert runtime.last_alert is not None
        assert runtime.last_alert.severity == AlertSeverity.CRITICAL
        assert runtime.last_alert.code == CRITICAL_HEARTBEAT_TIMEOUT_CODE
        assert primary.records[-1].code == CRITICAL_HEARTBEAT_TIMEOUT_CODE
        assert backup.records == []
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_heartbeat_timeout_uses_backup_delivery_when_primary_fails(tmp_path):
    backup = MemoryAlertChannel("backup")
    runtime = _runtime(
        tmp_path,
        FakeRestClient(),
        alert_dispatcher=AlertDispatcher(FailingAlertChannel("primary"), backup),
    )
    runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, avg_cost_basis=Decimal("100"))
    await runtime.start()
    try:
        assert runtime._heartbeat_task is not None
        runtime._heartbeat_task.cancel()
        try:
            await runtime._heartbeat_task
        except asyncio.CancelledError:
            pass
        runtime.last_heartbeat_at = datetime.now(timezone.utc) - timedelta(seconds=11)
        await asyncio.sleep(runtime.config.heartbeat_watchdog_interval_seconds + 0.2)
        assert backup.records[-1].code == CRITICAL_HEARTBEAT_TIMEOUT_CODE
        assert backup.records[-1].delivered_via == ("backup",)
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v14_observability_and_runtime_behaviour_do_not_regress(tmp_path):
    runtime = _runtime(tmp_path, FakeRestClient())
    runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, avg_cost_basis=Decimal("100"))
    await runtime.start()
    try:
        await runtime.emit_heartbeat_observability()
        assert runtime.observability.heartbeat_count >= 1
        assert runtime.observability.performance_monitor.smoke_only is True
        assert runtime.status in {RuntimeStatus.READY, RuntimeStatus.HALT}
        assert runtime.protection_mode in {ProtectionMode.NONE, ProtectionMode.SOFTWARE_STOP, ProtectionMode.EXCHANGE_OCO}
    finally:
        await runtime.shutdown()
