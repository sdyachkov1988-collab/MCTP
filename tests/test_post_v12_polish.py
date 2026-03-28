import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import warnings

import pytest

from mctp.core.enums import Market, Timeframe
from mctp.core.types import Symbol
from mctp.execution.paper import SpotPaperExecutor
from mctp.indicators.models import Candle
from mctp.runtime import BnbTickerEvent, BookTickerEvent, EmaCrossSmokeStrategy, KlineEvent, PaperRuntime, PaperRuntimeConfig
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.snapshot_store import SnapshotStore
from mctp.streams.base import StreamType
from mctp.strategy.models import StrategyInput
from run_backtest import run_demo_backtest
from run_paper_runtime import _configure_operator_logging, main as run_paper_main, run_local_demo


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
START = datetime(2026, 3, 22, 0, 0, 0, tzinfo=timezone.utc)


def _instrument_info() -> dict:
    return {
        "lot_size": Decimal("0.001"),
        "min_qty": Decimal("0.001"),
        "max_qty": Decimal("1000"),
        "min_notional": Decimal("10"),
    }


def _runtime(tmp_path) -> PaperRuntime:
    return PaperRuntime(
        config=PaperRuntimeConfig(
            symbol=BTCUSDT,
            timeframe=Timeframe.M15,
            instrument_info=_instrument_info(),
            initial_balances={"BTC": Decimal("0"), "USDT": Decimal("1000")},
            order_quantity=Decimal("0.001"),
        ),
        strategy=EmaCrossSmokeStrategy(),
        snapshot_store=SnapshotStore(str(tmp_path / "snapshot.json")),
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        accounting_store=AccountingStore(str(tmp_path / "accounting.json")),
    )


def _candle(index: int, close: Decimal) -> Candle:
    return Candle(
        timestamp=START + timedelta(minutes=index),
        open=close,
        high=close + Decimal("1"),
        low=close - Decimal("1"),
        close=close,
        volume=Decimal("10"),
    )


@pytest.mark.asyncio
async def test_runtime_uses_core_sizing_path_more_faithfully(tmp_path):
    runtime = _runtime(tmp_path)
    await runtime.start()
    for index in range(21):
        close = Decimal("100") + Decimal(index)
        timestamp = START + timedelta(minutes=index)
        await runtime.channels[StreamType.BOOK_TICKER].publish(
            BookTickerEvent(timestamp=timestamp, bid=close - Decimal("0.5"), ask=close + Decimal("0.5"))
        )
        await runtime.channels[StreamType.BNB_TICKER].publish(
            BnbTickerEvent(timestamp=timestamp, price=Decimal("300"))
        )
        await runtime.channels[StreamType.KLINE].publish(
            KlineEvent(timeframe=Timeframe.M15, candle=_candle(index, close))
        )
    await runtime.process_all_available()
    assert runtime.submitted_order_quantities
    assert runtime.submitted_order_quantities[0] > runtime.config.order_quantity


@pytest.mark.asyncio
async def test_runtime_tick_detects_quiet_kline_staleness(tmp_path):
    runtime = _runtime(tmp_path)
    await runtime.start()
    runtime.channels[StreamType.KLINE].touch(START)
    runtime.channels[StreamType.BOOK_TICKER].touch(START)
    runtime.channels[StreamType.BNB_TICKER].touch(START)
    runtime.channels[StreamType.USER_DATA].touch(START)
    await runtime.tick(START + timedelta(seconds=500))
    assert runtime.status.value == "HALT"
    assert runtime.channels[StreamType.KLINE].state.is_stale is True


@pytest.mark.asyncio
async def test_runtime_tick_marks_quiet_book_ticker_stale_without_halt(tmp_path):
    runtime = _runtime(tmp_path)
    await runtime.start()
    runtime.channels[StreamType.KLINE].touch(START + timedelta(seconds=20))
    runtime.channels[StreamType.BOOK_TICKER].touch(START)
    runtime.channels[StreamType.BNB_TICKER].touch(START + timedelta(seconds=20))
    runtime.channels[StreamType.USER_DATA].touch(START + timedelta(seconds=20))
    await runtime.tick(START + timedelta(seconds=20))
    assert runtime.channels[StreamType.BOOK_TICKER].state.is_stale is True
    assert runtime.status.value == "RUNNING"


@pytest.mark.asyncio
async def test_runtime_fill_timestamps_follow_event_timeline(tmp_path):
    runtime = _runtime(tmp_path)
    await runtime.start()
    for index in range(21):
        close = Decimal("100") + Decimal(index)
        timestamp = START + timedelta(minutes=index)
        await runtime.channels[StreamType.BOOK_TICKER].publish(
            BookTickerEvent(timestamp=timestamp, bid=close - Decimal("0.5"), ask=close + Decimal("0.5"))
        )
        await runtime.channels[StreamType.KLINE].publish(
            KlineEvent(timeframe=Timeframe.M15, candle=_candle(index, close))
        )
    await runtime.process_all_available()
    assert runtime.portfolio.accounting.fill_history
    assert runtime.portfolio.accounting.fill_history[0].filled_at == START + timedelta(minutes=20)


def test_executor_fill_uses_event_time_when_provided():
    from mctp.core.enums import OrderType, Side
    from mctp.core.order import Order

    executor = SpotPaperExecutor(initial_balances={"BTC": Decimal("0"), "USDT": Decimal("1000")})
    event_time = START + timedelta(minutes=5)
    executor.set_event_time(event_time)
    order = Order(
        symbol=BTCUSDT,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("1"),
        created_at=START,
    )
    fill = executor._make_fill(order, Decimal("100"))
    assert fill.filled_at == event_time


def test_strategy_input_nested_structures_are_read_only():
    strategy_input = StrategyInput(
        snapshot=type("Snapshot", (), {"symbol": BTCUSDT, "timestamp": START})(),
        indicators={"ema_9": Decimal("101")},
        candles={Timeframe.M15: [_candle(0, Decimal("100"))]},
        onchain=None,
    )
    with pytest.raises(TypeError):
        strategy_input.indicators["ema_9"] = Decimal("102")
    with pytest.raises(TypeError):
        strategy_input.candles[Timeframe.M15] += (_candle(1, Decimal("101")),)


def test_run_backtest_default_demo_produces_multiple_closed_trades_and_analytics():
    result = run_demo_backtest()
    assert len(result.closed_trades) >= 2
    assert result.trade_count >= 2
    assert result.analytics is not None
    assert result.analytics.profit_factor is not None
    assert result.analytics.expectancy is not None
    assert result.analytics.oco.tp_exit_share is not None


@pytest.mark.asyncio
async def test_run_paper_runtime_local_demo_exercises_more_than_one_shot_path():
    summary = await run_local_demo()
    assert summary["runtime_status_before_shutdown"] == "RUNNING"
    assert summary["runtime_status_after_shutdown"] == "STOPPED"
    assert summary["strategy_calls"] > 1
    assert summary["fill_count"] >= 2
    assert summary["submitted_order_count"] >= 2
    assert summary["last_intent"] in {"BUY", "SELL"}
    assert summary["kline_stale"] is False


def test_run_paper_runtime_websocket_demo_mode_is_honestly_disabled(monkeypatch):
    monkeypatch.setattr("sys.argv", ["run_paper_runtime.py", "websocket"])
    with pytest.raises(SystemExit, match="Only local demo mode is supported"):
        asyncio.run(run_paper_main())


def test_run_paper_runtime_configures_operator_logging(monkeypatch):
    import logging

    root_logger = logging.getLogger()
    previous_handlers = list(root_logger.handlers)
    previous_level = root_logger.level
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    try:
        _configure_operator_logging()
        assert root_logger.handlers
        assert root_logger.level == logging.INFO
        assert root_logger.handlers[0].formatter is not None
        assert "%(asctime)sZ" in root_logger.handlers[0].formatter._fmt
    finally:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
        for handler in previous_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(previous_level)


def test_legacy_inline_indicator_helpers_are_clearly_deprecated_but_compatible():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        from mctp.backtest.indicators_inline import InlineIndicatorState, ema_next

        value = ema_next(None, Decimal("10"), 3)
        state = InlineIndicatorState(ema_period=3, atr_period=3)
    assert value == Decimal("10")
    assert state.ema is None
    assert any(item.category is DeprecationWarning for item in caught)
