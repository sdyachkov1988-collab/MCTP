from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from mctp.adapters import BinanceCredentials, BinanceSpotTestnetAdapterV1, BinanceSpotTestnetConfigV1
from mctp.core.constants import ACCOUNT_SNAPSHOT_TTL_SECONDS, BALANCE_CACHE_TTL, USER_DATA_STALE_SECONDS
from mctp.core.enums import Market, ProtectionMode, Timeframe
from mctp.core.types import Symbol
from mctp.runtime.streams import QueueStreamTransport
from mctp.runtime.strategy_smoke import EmaCrossSmokeStrategy
from mctp.runtime.testnet import TestnetRuntime, TestnetRuntimeConfig
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.order_store import OrderStore
from mctp.storage.snapshot_store import SnapshotStore
from mctp.streams.base import StreamType


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
START = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone.utc)


class OperationalRestClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, object], bool]] = []
        self.account_balances = {"BTC": Decimal("0"), "USDT": Decimal("1000"), "BNB": Decimal("1")}
        self.locked_balances = {"BTC": Decimal("0"), "USDT": Decimal("0"), "BNB": Decimal("0")}

    async def request_json(self, method, path, *, params=None, signed=False):
        safe_params = dict(params or {})
        self.calls.append((method, path, safe_params, signed))
        if path == "/api/v3/userDataStream" and method == "POST":
            return {"listenKey": "listen-key"}
        if path == "/api/v3/userDataStream" and method in {"PUT", "DELETE"}:
            return {}
        if path == "/api/v3/openOrderList":
            return []
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
        raise AssertionError(f"Unhandled operational REST call: {method} {path}")


def _runtime(tmp_path, rest_client: OperationalRestClient, *, root_name: str) -> TestnetRuntime:
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


def _account_payload(timestamp: datetime, *, free_btc: Decimal = Decimal("0"), free_usdt: Decimal = Decimal("1000")) -> dict[str, object]:
    return {
        "e": "outboundAccountPosition",
        "E": int(timestamp.timestamp() * 1000),
        "B": [
            {"a": "BTC", "f": str(free_btc), "l": "0"},
            {"a": "USDT", "f": str(free_usdt), "l": "0"},
            {"a": "BNB", "f": "1", "l": "0"},
        ],
    }


def _account_calls(rest_client: OperationalRestClient) -> list[tuple[str, str, dict[str, object], bool]]:
    return [call for call in rest_client.calls if call[1] == "/api/v3/account"]


def test_v17_operational_balance_cache_ttl_constants_remain_aligned():
    assert BALANCE_CACHE_TTL == ACCOUNT_SNAPSHOT_TTL_SECONDS


@pytest.mark.asyncio
async def test_v17_operational_balance_cache_ttl_does_not_refresh_rest_while_cache_is_fresh_and_runtime_is_idle(tmp_path):
    rest = OperationalRestClient()
    runtime = _runtime(tmp_path, rest, root_name="ttl_fresh_idle")
    await runtime.start()
    try:
        assert len(_account_calls(rest)) == 1
        await runtime.process_all_available()
        assert len(_account_calls(rest)) == 1
        assert runtime.portfolio.snapshot.free_quote == Decimal("1000")
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_operational_balance_cache_ttl_refreshes_exchange_truth_when_cache_is_stale_and_runtime_is_idle(tmp_path):
    rest = OperationalRestClient()
    runtime = _runtime(tmp_path, rest, root_name="ttl_stale_idle")
    await runtime.start()
    try:
        stale_at = datetime.now(timezone.utc) - timedelta(seconds=ACCOUNT_SNAPSHOT_TTL_SECONDS + 1)
        runtime.balance_cache_store.save({"BTC": Decimal("0"), "USDT": Decimal("1000"), "BNB": Decimal("1")}, stale_at)
        rest.account_balances["BTC"] = Decimal("0.25")
        rest.locked_balances["BTC"] = Decimal("0.05")
        rest.account_balances["USDT"] = Decimal("777")

        await runtime.process_all_available()

        assert len(_account_calls(rest)) == 2
        assert runtime.portfolio.snapshot.held_qty == Decimal("0.30")
        assert runtime.portfolio.snapshot.free_quote == Decimal("777")
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_v17_operational_balance_cache_ttl_refresh_does_not_mask_user_data_staleness_or_protection_state(tmp_path):
    rest = OperationalRestClient()
    runtime = _runtime(tmp_path, rest, root_name="ttl_user_data_stale")
    await runtime.start()
    try:
        await runtime.channels[StreamType.USER_DATA].publish(_account_payload(START))
        await runtime.process_all_available()
        stale_now = START + timedelta(seconds=USER_DATA_STALE_SECONDS + 1)
        await runtime.evaluate_staleness(stale_now)
        assert runtime.channels[StreamType.USER_DATA].state.is_stale is True

        stale_at = datetime.now(timezone.utc) - timedelta(seconds=ACCOUNT_SNAPSHOT_TTL_SECONDS + 1)
        runtime.balance_cache_store.save({"BTC": Decimal("0"), "USDT": Decimal("1000"), "BNB": Decimal("1")}, stale_at)
        rest.account_balances["USDT"] = Decimal("850")

        await runtime.process_all_available()

        assert len(_account_calls(rest)) == 2
        assert runtime.portfolio.snapshot.free_quote == Decimal("850")
        assert runtime.channels[StreamType.USER_DATA].state.is_stale is True
        assert runtime.pending_order_client_id is None
        assert runtime.active_oco_order_id is None
        assert runtime.protection_mode == ProtectionMode.NONE
    finally:
        await runtime.shutdown()
