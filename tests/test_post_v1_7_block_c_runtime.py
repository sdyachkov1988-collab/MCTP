from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mctp.adapters import BinanceCredentials, BinanceSpotTestnetAdapterV1, BinanceSpotTestnetConfigV1
from mctp.core.enums import IntentType, Market, Timeframe
from mctp.core.types import Intent, Symbol
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
    async def request_json(self, method, path, *, params=None, signed=False):
        if path == "/api/v3/account":
            return {"balances": [{"asset": "BTC", "free": "0", "locked": "0"}, {"asset": "USDT", "free": "1000", "locked": "0"}]}
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
        if path == "/api/v3/userDataStream" and method == "POST":
            return {"listenKey": "listen-key"}
        if path == "/api/v3/userDataStream" and method in {"PUT", "DELETE"}:
            return {}
        if path == "/api/v3/openOrderList":
            return []
        if path == "/api/v3/openOrders":
            return []
        raise AssertionError(f"Unhandled fake REST call: {method} {path}")


def _runtime(tmp_path) -> TestnetRuntime:
    adapter = BinanceSpotTestnetAdapterV1(
        BinanceSpotTestnetConfigV1(BinanceCredentials(api_key="k", api_secret="s")),
        rest_client=FakeRestClient(),
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        order_store=OrderStore(str(tmp_path / "orders.json")),
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


@pytest.mark.asyncio
async def test_runtime_execute_intent_delegates_to_trade_flow_helper(tmp_path):
    runtime = _runtime(tmp_path)
    called: dict[str, object] = {}

    async def _fake_execute(intent, timestamp):
        called["intent"] = intent
        called["timestamp"] = timestamp

    runtime._trade_flow_helper.execute_intent = _fake_execute  # type: ignore[method-assign]
    intent = Intent(type=IntentType.HOLD, symbol=BTCUSDT)

    await runtime._execute_intent(intent, START)

    assert called == {"intent": intent, "timestamp": START}


def test_runtime_safety_control_wrapper_delegates_to_safety_helper(tmp_path):
    runtime = _runtime(tmp_path)
    called: dict[str, object] = {}

    def _fake_evaluate(timestamp):
        called["timestamp"] = timestamp

    runtime._safety_state_helper.evaluate_safety_controls = _fake_evaluate  # type: ignore[method-assign]

    runtime._evaluate_safety_controls(START)

    assert called == {"timestamp": START}
