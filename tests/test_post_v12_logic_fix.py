from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from mctp.backtest.config import BacktestConfig
from mctp.backtest.engine import BacktestEngine
from mctp.backtest.market_replay import BacktestCandle
from mctp.core.enums import CommissionAsset, Market, Side, Timeframe
from mctp.core.order import Fill
from mctp.core.types import PortfolioSnapshot, Symbol
from mctp.portfolio.equity import EquityTracker
from mctp.portfolio.tracker import PortfolioTracker
from mctp.runtime import BnbTickerEvent, BookTickerEvent, EmaCrossSmokeStrategy, KlineEvent, PaperRuntime, PaperRuntimeConfig
from mctp.sizing.models import SizerResult
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


def _tp_sequence() -> list[BacktestCandle]:
    closes = [
        Decimal("100"),
        Decimal("99"),
        Decimal("98"),
        Decimal("97"),
        Decimal("96"),
        Decimal("110"),
        Decimal("128"),
    ]
    candles = []
    for index, close in enumerate(closes):
        candles.append(
            BacktestCandle(
                timestamp=START + timedelta(minutes=index),
                open=close,
                high=close + Decimal("2"),
                low=close - Decimal("2"),
                close=close,
                bnb_rate=Decimal("100"),
            )
        )
    candles[-1] = BacktestCandle(
        timestamp=START + timedelta(minutes=len(closes) - 1),
        open=Decimal("128"),
        high=Decimal("130"),
        low=Decimal("126"),
        close=Decimal("128"),
        bnb_rate=Decimal("100"),
    )
    return candles


def _config(commission_asset: CommissionAsset) -> BacktestConfig:
    return BacktestConfig(
        symbol=BTCUSDT,
        initial_quote=Decimal("10000"),
        warmup_bars=5,
        ema_period=3,
        atr_period=3,
        instrument_info=_instrument_info(),
        commission_asset=commission_asset,
    )


def _runtime(tmp_path) -> PaperRuntime:
    return PaperRuntime(
        config=PaperRuntimeConfig(
            symbol=BTCUSDT,
            timeframe=Timeframe.M15,
            instrument_info=_instrument_info(),
            initial_balances={"BTC": Decimal("0"), "USDT": Decimal("1000")},
            order_quantity=Decimal("0.500"),
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


def test_closed_trade_pnl_matches_equity_delta_for_quote_fee():
    result = BacktestEngine(_config(CommissionAsset.QUOTE)).run(_tp_sequence())
    assert len(result.closed_trades) == 1
    entry = result.executions[0]
    exit_fill = result.executions[1]
    expected = (
        exit_fill.quantity * exit_fill.fill_price
        - result.accounting_history[1].fee_drag_quote
        - entry.quantity * entry.fill_price
        - result.accounting_history[0].fee_drag_quote
    )
    assert result.closed_trades[0].net_pnl == expected
    assert result.analytics is not None
    assert result.analytics.expectancy == result.closed_trades[0].net_pnl


def test_closed_trade_pnl_matches_economic_round_trip_for_base_fee():
    result = BacktestEngine(_config(CommissionAsset.BASE)).run(_tp_sequence())
    assert len(result.closed_trades) == 1
    entry = result.executions[0]
    exit_fill = result.executions[1]
    net_entry_base = entry.quantity - entry.commission
    expected = (
        net_entry_base * exit_fill.fill_price
        - result.accounting_history[1].fee_drag_quote
        - entry.quantity * entry.fill_price
    )
    assert abs(result.closed_trades[0].net_pnl - expected) < Decimal("0.0000000001")


def test_closed_trade_pnl_matches_economic_round_trip_for_bnb_fee():
    result = BacktestEngine(_config(CommissionAsset.BNB)).run(_tp_sequence())
    assert len(result.closed_trades) == 1
    entry = result.executions[0]
    exit_fill = result.executions[1]
    expected = (
        exit_fill.quantity * exit_fill.fill_price
        - result.accounting_history[1].fee_drag_quote
        - entry.quantity * entry.fill_price
        - result.accounting_history[0].fee_drag_quote
    )
    assert result.closed_trades[0].net_pnl == expected


def test_partial_close_remains_consistent_after_entry_fee_fix():
    tracker = PortfolioTracker(
        initial_snapshot=PortfolioSnapshot(
            symbol=BTCUSDT,
            held_qty=Decimal("0"),
            avg_cost_basis=Decimal("0"),
            free_quote=Decimal("2000"),
            quote_asset="USDT",
            is_in_position=False,
            meaningful_position=False,
            timestamp=START,
        ),
        equity_tracker=EquityTracker(Decimal("2000")),
    )
    buy_fill = Fill(
        order_id="buy-1",
        symbol=BTCUSDT,
        side=Side.BUY,
        base_qty_filled=Decimal("10"),
        quote_qty_filled=Decimal("1000"),
        fill_price=Decimal("100"),
        commission=Decimal("1"),
        commission_asset=CommissionAsset.QUOTE,
        filled_at=START,
    )
    sell_fill_a = Fill(
        order_id="sell-a",
        symbol=BTCUSDT,
        side=Side.SELL,
        base_qty_filled=Decimal("5"),
        quote_qty_filled=Decimal("550"),
        fill_price=Decimal("110"),
        commission=Decimal("0.55"),
        commission_asset=CommissionAsset.QUOTE,
        filled_at=START + timedelta(minutes=1),
    )
    sell_fill_b = Fill(
        order_id="sell-b",
        symbol=BTCUSDT,
        side=Side.SELL,
        base_qty_filled=Decimal("5"),
        quote_qty_filled=Decimal("550"),
        fill_price=Decimal("110"),
        commission=Decimal("0.55"),
        commission_asset=CommissionAsset.QUOTE,
        filled_at=START + timedelta(minutes=2),
    )

    tracker.on_fill(buy_fill)
    pnl_a = tracker.realized_pnl(sell_fill_a)
    tracker.on_fill(sell_fill_a)
    pnl_b = tracker.realized_pnl(sell_fill_b)
    tracker.on_fill(sell_fill_b)

    total_trade_pnl = pnl_a.net_pnl + pnl_b.net_pnl
    assert total_trade_pnl == tracker.snapshot.free_quote - Decimal("2000")


@pytest.mark.asyncio
async def test_runtime_does_not_submit_trade_when_sizer_rejects(tmp_path):
    runtime = _runtime(tmp_path)
    await runtime.start()

    def _rejecting_calculate(*args, **kwargs):
        return SizerResult(
            approved=False,
            quantity=None,
            notional=None,
            rejection_reason="forced reject",
            risk_used=Decimal("0"),
            calculated_at=START,
        )

    runtime.position_sizer.calculate = _rejecting_calculate
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
    assert runtime.portfolio.accounting.fill_history == []
    assert runtime.portfolio.snapshot.held_qty == Decimal("0")


@pytest.mark.asyncio
async def test_runtime_no_longer_falls_back_to_default_quantity_after_sizer_rejection(tmp_path):
    runtime = _runtime(tmp_path)
    await runtime.start()

    def _rejecting_calculate(*args, **kwargs):
        return SizerResult(
            approved=False,
            quantity=None,
            notional=None,
            rejection_reason="forced reject",
            risk_used=Decimal("0"),
            calculated_at=START,
        )

    runtime.position_sizer.calculate = _rejecting_calculate
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
    assert runtime.config.order_quantity == Decimal("0.500")
    assert runtime.submitted_order_quantities != [runtime.config.order_quantity]
