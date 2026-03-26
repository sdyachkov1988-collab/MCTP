from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from mctp.adapters import BinanceCredentials, BinanceSpotTestnetAdapterV1, BinanceSpotTestnetConfigV1
from mctp.core.enums import Market, Timeframe
from mctp.core.types import PortfolioSnapshot, Symbol
from mctp.execution.oco import OCOOrder
from mctp.indicators.models import Candle
from mctp.runtime.events import BookTickerEvent, KlineEvent
from mctp.runtime.paper import PaperRuntime, PaperRuntimeConfig
from mctp.runtime.strategy_smoke import EmaCrossSmokeStrategy
from mctp.runtime.streams import QueueStreamTransport
from mctp.runtime.testnet import TestnetRuntime, TestnetRuntimeConfig
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.order_store import OrderStore
from mctp.storage.snapshot_store import SnapshotStore
from mctp.streams.base import StreamType


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
START = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone.utc)


class FakeRestClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict, bool]] = []
        self.account_balances = {"BTC": Decimal("0"), "USDT": Decimal("1000"), "BNB": Decimal("1")}
        self.locked_balances = {"BTC": Decimal("0"), "USDT": Decimal("0"), "BNB": Decimal("0")}

    async def request_json(self, method, path, *, params=None, signed=False):
        safe_params = dict(params or {})
        self.calls.append((method, path, safe_params, signed))
        if path == "/api/v3/orderList/oco":
            return {"orderListId": "oco-1"}
        if path == "/api/v3/userDataStream" and method == "POST":
            return {"listenKey": "listen-key"}
        if path == "/api/v3/userDataStream" and method in {"PUT", "DELETE"}:
            return {}
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
        if path == "/api/v3/openOrderList":
            return []
        if path == "/api/v3/openOrders":
            return []
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
        if path == "/api/v3/order":
            return {"status": "NEW"}
        raise AssertionError(f"Unhandled fake REST call: {method} {path}")


class RecordingStrategy(EmaCrossSmokeStrategy):
    def __init__(self) -> None:
        self.calls = 0

    def on_candle(self, input):
        self.calls += 1
        return super().on_candle(input)


def _instrument_info() -> dict:
    return {
        "lot_size": Decimal("0.001"),
        "min_qty": Decimal("0.001"),
        "max_qty": Decimal("1000"),
        "min_notional": Decimal("10"),
    }


def _candle(index: int, close: Decimal, *, closed: bool = True) -> Candle:
    return Candle(
        timestamp=START + timedelta(minutes=index),
        open=close,
        high=close + Decimal("1"),
        low=close - Decimal("1"),
        close=close,
        volume=Decimal("10"),
        closed=closed,
    )


def _paper_runtime(tmp_path, strategy: RecordingStrategy) -> PaperRuntime:
    return PaperRuntime(
        config=PaperRuntimeConfig(
            symbol=BTCUSDT,
            timeframe=Timeframe.M15,
            instrument_info=_instrument_info(),
            initial_balances={"BTC": Decimal("0"), "USDT": Decimal("1000")},
        ),
        strategy=strategy,
        snapshot_store=SnapshotStore(str(tmp_path / "paper_snapshot.json")),
        balance_cache_store=BalanceCacheStore(str(tmp_path / "paper_balances.json")),
        accounting_store=AccountingStore(str(tmp_path / "paper_accounting.json")),
        kline_transport=QueueStreamTransport(),
        book_transport=QueueStreamTransport(),
        bnb_transport=QueueStreamTransport(),
        user_transport=QueueStreamTransport(),
    )


def _testnet_runtime(tmp_path, strategy: RecordingStrategy | None = None) -> tuple[TestnetRuntime, FakeRestClient]:
    rest = FakeRestClient()
    adapter = BinanceSpotTestnetAdapterV1(
        BinanceSpotTestnetConfigV1(BinanceCredentials(api_key="k", api_secret="s")),
        rest_client=rest,
        balance_cache_store=BalanceCacheStore(str(tmp_path / "testnet_balances.json")),
        order_store=OrderStore(str(tmp_path / "testnet_orders.json")),
    )
    runtime = TestnetRuntime(
        config=TestnetRuntimeConfig(
            symbol=BTCUSDT,
            timeframe=Timeframe.M15,
            instrument_info=_instrument_info(),
            initial_balances={"BTC": Decimal("0"), "USDT": Decimal("1000")},
        ),
        strategy=strategy or RecordingStrategy(),
        executor=adapter,
        snapshot_store=SnapshotStore(str(tmp_path / "testnet_snapshot.json")),
        balance_cache_store=BalanceCacheStore(str(tmp_path / "testnet_balances.json")),
        accounting_store=AccountingStore(str(tmp_path / "testnet_accounting.json")),
        kline_transport=QueueStreamTransport(),
        book_transport=QueueStreamTransport(),
        bnb_transport=QueueStreamTransport(),
        user_transport=QueueStreamTransport(),
    )
    runtime.portfolio.replace_snapshot(avg_cost_basis=Decimal("100"))
    return runtime, rest


@pytest.mark.asyncio
async def test_non_closed_candle_is_ignored_in_paper_runtime_but_closed_candle_still_triggers(tmp_path):
    strategy = RecordingStrategy()
    runtime = _paper_runtime(tmp_path, strategy)
    await runtime.start()
    try:
        await runtime.channels[StreamType.BOOK_TICKER].publish(
            BookTickerEvent(timestamp=START, bid=Decimal("100"), ask=Decimal("101"))
        )
        runtime.candles[Timeframe.M15] = [_candle(index, Decimal("100") + Decimal(index)) for index in range(20)]

        await runtime._handle_kline(KlineEvent(timeframe=Timeframe.M15, candle=_candle(20, Decimal("120"), closed=False)))
        assert strategy.calls == 0
        assert runtime.strategy_call_count == 0

        await runtime._handle_kline(KlineEvent(timeframe=Timeframe.M15, candle=_candle(20, Decimal("120"), closed=True)))
        assert strategy.calls == 1
        assert runtime.strategy_call_count == 1
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_non_closed_candle_is_ignored_in_testnet_runtime_but_closed_candle_still_triggers(tmp_path):
    strategy = RecordingStrategy()
    runtime, _ = _testnet_runtime(tmp_path, strategy)
    await runtime.start()
    try:
        runtime.candles[Timeframe.M15] = [_candle(index, Decimal("100") + Decimal(index)) for index in range(20)]
        await runtime._handle_kline(KlineEvent(timeframe=Timeframe.M15, candle=_candle(20, Decimal("120"), closed=False)))
        assert strategy.calls == 0
        assert runtime.strategy_call_count == 0

        await runtime._handle_kline(KlineEvent(timeframe=Timeframe.M15, candle=_candle(20, Decimal("120"), closed=True)))
        assert strategy.calls == 1
        assert runtime.strategy_call_count == 1
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_oco_pre_submit_validation_blocks_invalid_payloads_and_allows_valid_oco(tmp_path):
    runtime, rest = _testnet_runtime(tmp_path)
    await runtime.start()
    try:
        runtime.portfolio.replace_snapshot(
            held_qty=Decimal("0.2"),
            is_in_position=True,
            meaningful_position=True,
            avg_cost_basis=Decimal("100"),
        )
        runtime.current_bid = Decimal("100")
        runtime.channels[StreamType.BOOK_TICKER].touch(START)
        runtime.channels[StreamType.BOOK_TICKER].state.is_stale = False

        await runtime.submit_oco(
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
        assert len([call for call in rest.calls if call[1] == "/api/v3/orderList/oco"]) == 1

        with pytest.raises(ValueError, match="take-profit"):
            await runtime.submit_oco(
                OCOOrder(
                    symbol=BTCUSDT,
                    tp_price=Decimal("99"),
                    sl_stop_price=Decimal("95"),
                    sl_limit_price=Decimal("94"),
                    quantity=Decimal("0.2"),
                    created_at=START,
                    updated_at=START,
                )
            )
        assert len([call for call in rest.calls if call[1] == "/api/v3/orderList/oco"]) == 1
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_oco_pre_submit_validation_requires_fresh_market_reference(tmp_path):
    runtime, _ = _testnet_runtime(tmp_path)
    await runtime.start()
    try:
        runtime.portfolio.replace_snapshot(
            held_qty=Decimal("0.2"),
            is_in_position=True,
            meaningful_position=True,
            avg_cost_basis=Decimal("100"),
        )

        with pytest.raises(ValueError, match="unavailable"):
            await runtime.submit_oco(
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

        runtime.current_bid = Decimal("100")
        runtime.channels[StreamType.BOOK_TICKER].touch(START)
        runtime.channels[StreamType.BOOK_TICKER].state.is_stale = True
        with pytest.raises(ValueError, match="stale"):
            await runtime.submit_oco(
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
    finally:
        await runtime.shutdown()


def test_runtime_uses_portfolio_snapshot_api_for_controlled_state_transitions(tmp_path):
    runtime, _ = _testnet_runtime(tmp_path)
    replace_calls: list[dict[str, object]] = []
    restore_calls: list[PortfolioSnapshot] = []

    original_replace = runtime.portfolio.replace_snapshot
    original_restore = runtime.portfolio.restore_snapshot

    def record_replace(**changes: object):
        replace_calls.append(changes)
        return original_replace(**changes)

    def record_restore(snapshot: PortfolioSnapshot):
        restore_calls.append(snapshot)
        return original_restore(snapshot)

    runtime.portfolio.replace_snapshot = record_replace  # type: ignore[method-assign]
    runtime.portfolio.restore_snapshot = record_restore  # type: ignore[method-assign]

    runtime.provide_manual_basis(Decimal("123"))
    runtime._apply_exchange_balance_truth(
        {"BTC": Decimal("0.1"), "USDT": Decimal("900"), "BNB": Decimal("1")},
        {"BTC": Decimal("0"), "USDT": Decimal("0"), "BNB": Decimal("0")},
        START,
    )
    runtime._advance_symbol_change_to_config_update(START)
    runtime._hydrate_restart_state()

    assert len(replace_calls) >= 3
    assert len(restore_calls) == 1
    assert runtime.portfolio.snapshot.avg_cost_basis == Decimal("0")
