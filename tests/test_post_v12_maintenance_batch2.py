import asyncio
import tomllib
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from mctp.core.constants import CONSECUTIVE_LOSSES_STOP
from mctp.core.enums import CommissionAsset, Market, Side, Timeframe
from mctp.core.order import Fill
from mctp.core.types import PortfolioSnapshot, Symbol
from mctp.portfolio.equity import EquityTracker
from mctp.portfolio.tracker import PortfolioTracker
from mctp.runtime import BnbTickerEvent, BookTickerEvent, EmaCrossSmokeStrategy, KlineEvent, PaperRuntime, PaperRuntimeConfig
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.snapshot_store import SnapshotStore
from mctp.streams.base import StreamType


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
START = datetime(2026, 3, 22, 0, 0, 0, tzinfo=timezone.utc)


def _instrument_info() -> dict:
    return {
        "lot_size": Decimal("0.001"),
        "min_qty": Decimal("0.001"),
        "max_qty": Decimal("1000"),
        "min_notional": Decimal("10"),
    }


def _runtime(tmp_path, heartbeat_interval_seconds: int = 1) -> PaperRuntime:
    return PaperRuntime(
        config=PaperRuntimeConfig(
            symbol=BTCUSDT,
            timeframe=Timeframe.M15,
            instrument_info=_instrument_info(),
            initial_balances={"BTC": Decimal("0"), "USDT": Decimal("1000")},
            heartbeat_interval_seconds=heartbeat_interval_seconds,
        ),
        strategy=EmaCrossSmokeStrategy(),
        snapshot_store=SnapshotStore(str(tmp_path / "snapshot.json")),
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        accounting_store=AccountingStore(str(tmp_path / "accounting.json")),
    )


def _candle(index: int, close: Decimal):
    from mctp.indicators.models import Candle

    return Candle(
        timestamp=START + timedelta(minutes=index),
        open=close,
        high=close + Decimal("1"),
        low=close - Decimal("1"),
        close=close,
        volume=Decimal("10"),
    )


def _buy_fill(
    *,
    qty: Decimal,
    price: Decimal,
    commission: Decimal,
    commission_asset: CommissionAsset,
) -> Fill:
    return Fill(
        order_id="buy-order",
        symbol=BTCUSDT,
        side=Side.BUY,
        base_qty_filled=qty,
        quote_qty_filled=qty * price,
        fill_price=price,
        commission=commission,
        commission_asset=commission_asset,
        filled_at=START,
    )


@pytest.mark.asyncio
async def test_runtime_blocks_new_entries_when_operational_mode_pauses_entries(tmp_path):
    runtime = _runtime(tmp_path)
    await runtime.start()
    try:
        for _ in range(CONSECUTIVE_LOSSES_STOP):
            runtime.adaptive_risk.on_trade_result(Decimal("-10"), Decimal("990"), now=START)
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
        assert runtime.submitted_order_quantities == []
        assert runtime.portfolio.snapshot.held_qty == Decimal("0")
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_runtime_allows_entry_when_operational_mode_is_run(tmp_path):
    runtime = _runtime(tmp_path)
    await runtime.start()
    try:
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
        assert runtime.portfolio.snapshot.held_qty > Decimal("0")
    finally:
        await runtime.shutdown()


def test_meaningful_position_can_differ_from_is_in_position_for_dust_holdings():
    tracker = PortfolioTracker(
        initial_snapshot=PortfolioSnapshot(
            symbol=BTCUSDT,
            held_qty=Decimal("0"),
            avg_cost_basis=Decimal("0"),
            free_quote=Decimal("1000"),
            quote_asset="USDT",
            is_in_position=False,
            meaningful_position=False,
            timestamp=START,
        ),
        equity_tracker=EquityTracker(Decimal("1000")),
        lot_size_provider=lambda: Decimal("0.001"),
    )
    tracker.on_fill(
        _buy_fill(
            qty=Decimal("0.002"),
            price=Decimal("100"),
            commission=Decimal("0"),
            commission_asset=CommissionAsset.QUOTE,
        )
    )
    equity_snapshot = tracker.record_equity(Decimal("100"), now=START + timedelta(minutes=1))
    assert tracker.snapshot.is_in_position is True
    assert tracker.snapshot.meaningful_position is False
    assert equity_snapshot is not None
    assert equity_snapshot.is_in_position is True
    assert equity_snapshot.meaningful_position is False


@pytest.mark.asyncio
async def test_quiet_stream_stale_detection_triggers_autonomously_without_manual_tick(tmp_path):
    runtime = _runtime(tmp_path, heartbeat_interval_seconds=1)
    await runtime.start()
    try:
        runtime.channels[StreamType.KLINE].touch(START)
        runtime.channels[StreamType.BOOK_TICKER].touch(START)
        runtime.channels[StreamType.BNB_TICKER].touch(START)
        runtime.channels[StreamType.USER_DATA].touch(START)
        runtime.current_runtime_time = START + timedelta(seconds=499)
        await asyncio.sleep(1.1)
        assert runtime.status.value == "HALT"
        assert runtime.channels[StreamType.KLINE].state.is_stale is True
        assert runtime.last_stale_check_at is not None
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_quiet_stream_book_ticker_stale_marks_flag_without_halt(tmp_path):
    runtime = _runtime(tmp_path, heartbeat_interval_seconds=1)
    await runtime.start()
    try:
        runtime.channels[StreamType.KLINE].touch(START + timedelta(seconds=20))
        runtime.channels[StreamType.BOOK_TICKER].touch(START)
        runtime.channels[StreamType.BNB_TICKER].touch(START + timedelta(seconds=20))
        runtime.channels[StreamType.USER_DATA].touch(START + timedelta(seconds=20))
        runtime.current_runtime_time = START + timedelta(seconds=19)
        await asyncio.sleep(1.1)
        assert runtime.channels[StreamType.BOOK_TICKER].state.is_stale is True
        assert runtime.status.value == "RUNNING"
    finally:
        await runtime.shutdown()


def test_websocket_dependency_is_declared_honestly_in_project_packaging():
    pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    runtime_dependencies = pyproject["project"]["optional-dependencies"]["runtime"]
    assert any("websockets" in dependency for dependency in runtime_dependencies)


def test_legacy_inline_indicator_layer_is_not_part_of_active_backtest_api():
    import mctp.backtest as backtest_api

    assert not hasattr(backtest_api, "InlineIndicatorState")
    assert not hasattr(backtest_api, "ema_next")
