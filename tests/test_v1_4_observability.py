from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mctp.adapters import BinanceCredentials, BinanceSpotTestnetAdapterV1, BinanceSpotTestnetConfigV1
from mctp.core.enums import IntentType, Market, Timeframe
from mctp.core.types import Intent, Symbol
from mctp.runtime.observability import HashChainAuditLogger, StrategyPerformanceMonitor
from mctp.runtime.strategy_smoke import EmaCrossSmokeStrategy
from mctp.runtime.streams import QueueStreamTransport
from mctp.runtime.testnet import TestnetRuntime, TestnetRuntimeConfig
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
            return [{"orderListId": value} for value in self.open_oco_order_ids]
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


def _runtime(tmp_path, rest_client: FakeRestClient) -> TestnetRuntime:
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
            initial_balances={"BTC": Decimal("0"), "USDT": Decimal("0")},
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
    return runtime


@pytest.mark.asyncio
async def test_structured_json_logs_include_required_fields_and_utc_timestamp(tmp_path):
    runtime = _runtime(tmp_path, FakeRestClient())
    await runtime.start()
    try:
        runtime.portfolio._snapshot = replace(
            runtime.portfolio.snapshot,
            held_qty=Decimal("0"),
            is_in_position=False,
            meaningful_position=False,
            free_quote=Decimal("1"),
        )
        await runtime._execute_intent(Intent(type=IntentType.BUY, symbol=BTCUSDT), START)
        decision_records = [record for record in runtime.observability.structured_logger.records if record["event_type"] == "decision_cycle"]
        assert decision_records
        record = decision_records[-1]
        assert set(["timestamp", "event_type", "symbol", "intent", "risk_result", "sizer_result", "execution_result"]).issubset(record.keys())
        assert datetime.fromisoformat(record["timestamp"]).tzinfo is not None
        assert "portfolio_snapshot" in record["before_state"]
        assert "stale_flags" in record["before_state"]
        assert record["rejection_reason"] == "insufficient_quote"
    finally:
        await runtime.shutdown()


def test_hash_chain_audit_log_links_and_detects_tampering():
    audit = HashChainAuditLogger()
    audit.append({"timestamp": START, "event_type": "a", "symbol": "BTCUSDT", "intent": None, "risk_result": None, "sizer_result": None, "execution_result": None})
    audit.append({"timestamp": START, "event_type": "b", "symbol": "BTCUSDT", "intent": None, "risk_result": None, "sizer_result": None, "execution_result": None})
    assert audit.verify_chain() is True
    audit.records[1]["payload"]["event_type"] = "tampered"
    assert audit.verify_chain() is False


@pytest.mark.asyncio
async def test_heartbeat_latency_and_memory_observability_are_emitted(tmp_path):
    runtime = _runtime(tmp_path, FakeRestClient())
    await runtime.start()
    try:
        runtime.current_runtime_time = START
        await runtime.emit_heartbeat_observability()
        runtime.portfolio._snapshot = replace(runtime.portfolio.snapshot, free_quote=Decimal("1"))
        await runtime._execute_intent(Intent(type=IntentType.BUY, symbol=BTCUSDT), START)
        records = runtime.observability.structured_logger.records
        assert any(record["event_type"] == "heartbeat" for record in records)
        assert any(record["event_type"] == "memory_metric" for record in records)
        assert any(record["event_type"] == "latency_metric" and record["metric"] == "risk_check" for record in records)
        assert runtime.observability.last_memory_snapshot["current_bytes"] >= 0
    finally:
        await runtime.shutdown()


def test_strategy_performance_monitor_thresholds_and_smoke_only_mode():
    monitor = StrategyPerformanceMonitor()
    assert monitor.smoke_only is True
    assert monitor.snapshot().mode == "testnet_smoke"

    threshold_monitor = StrategyPerformanceMonitor(mode="live")
    for value in [
        Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"),
        Decimal("1"), Decimal("1"),
        Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"),
        Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"), Decimal("-1"),
    ]:
        threshold_monitor.observe_trade(value)

    snapshot = threshold_monitor.snapshot()
    assert snapshot.warning is True
    assert "win_rate" in snapshot.warning_reasons
    assert "profit_factor" in snapshot.warning_reasons
    assert snapshot.stop_strategy is True
    assert snapshot.consecutive_losses >= 7


@pytest.mark.asyncio
async def test_runtime_uses_smoke_only_spm_in_testnet_mode(tmp_path):
    runtime = _runtime(tmp_path, FakeRestClient())
    await runtime.start()
    try:
        assert runtime.observability.performance_monitor.smoke_only is True
    finally:
        await runtime.shutdown()
