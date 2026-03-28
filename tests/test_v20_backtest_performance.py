from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from mctp.backtest import BacktestCandle, BacktestConfig, BacktestEngine
from mctp.backtest.mtf_builder import IncrementalMtfBacktestBuilder
from mctp.core.constants import STRATEGY_ID_V20_BTCUSDT_MTF, V20_MTF_REQUIRED_M15_CANDLES
from mctp.core.enums import IntentType, Market
from mctp.core.types import Intent, Symbol
from mctp.strategy.mtf import build_closed_mtf_candle_map_from_m15


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _instrument_info() -> dict[str, Decimal]:
    return {
        "lot_size": Decimal("0.001"),
        "min_qty": Decimal("0.001"),
        "max_qty": Decimal("1000"),
        "min_notional": Decimal("10"),
    }


def _m15_history(count: int) -> list[BacktestCandle]:
    candles: list[BacktestCandle] = []
    for index in range(count):
        timestamp = START + timedelta(minutes=15 * index)
        close = Decimal("100") + (Decimal(index) / Decimal("1000"))
        candles.append(
            BacktestCandle(
                timestamp=timestamp,
                open=close,
                high=close + Decimal("1"),
                low=close - Decimal("1"),
                close=close,
                volume=Decimal("1"),
                bnb_rate=Decimal("300"),
            )
        )
    return candles


def test_incremental_mtf_builder_matches_reference_rebuild_at_checkpoints():
    candles = _m15_history(96 * 70)
    builder = IncrementalMtfBacktestBuilder()
    checkpoints = {96, 672, 96 * 35, len(candles)}

    for index, source in enumerate(candles, start=1):
        builder.append(
            BacktestEngine._indicator_candle(source)
        )
        if index not in checkpoints:
            continue
        incremental_map = builder.candle_map()
        reference_map = build_closed_mtf_candle_map_from_m15(
            [BacktestEngine._indicator_candle(item) for item in candles[:index]]
        )
        assert set(incremental_map) == set(reference_map)
        for timeframe in reference_map:
            assert tuple(incremental_map[timeframe]) == tuple(reference_map[timeframe])


def test_v20_backtest_path_no_longer_rebuilds_full_history_per_bar(monkeypatch):
    class HoldStrategy:
        requires_mtf_warmup = True

        def __init__(self, indicator_engine) -> None:
            self._indicator_engine = indicator_engine

        def on_candle(self, input):
            return Intent(type=IntentType.HOLD, symbol=input.snapshot.symbol, timestamp=input.snapshot.timestamp)

    def fail_if_called(*args, **kwargs):
        raise AssertionError("full-history MTF rebuild should not be used in v20 backtest path anymore")

    monkeypatch.setattr("mctp.backtest.engine.BtcUsdtMtfV20Strategy", HoldStrategy)
    monkeypatch.setattr("mctp.backtest.engine.build_closed_mtf_candle_map_from_m15", fail_if_called)

    result = BacktestEngine(
        BacktestConfig(
            symbol=BTCUSDT,
            initial_quote=Decimal("10000"),
            warmup_bars=5,
            ema_period=3,
            atr_period=14,
            instrument_info=_instrument_info(),
            strategy_id=STRATEGY_ID_V20_BTCUSDT_MTF,
        )
    ).run(_m15_history(V20_MTF_REQUIRED_M15_CANDLES + 5))

    assert result.execution_count == 0
    assert result.trade_count == 0


def test_v20_backtest_remains_deterministic_after_incremental_mtf_refactor():
    candles = _m15_history(V20_MTF_REQUIRED_M15_CANDLES + 5)
    config = BacktestConfig(
        symbol=BTCUSDT,
        initial_quote=Decimal("10000"),
        warmup_bars=5,
        ema_period=3,
        atr_period=14,
        instrument_info=_instrument_info(),
        strategy_id=STRATEGY_ID_V20_BTCUSDT_MTF,
    )

    result_a = BacktestEngine(config).run(candles)
    result_b = BacktestEngine(config).run(candles)

    assert result_a.execution_count == result_b.execution_count
    assert result_a.trade_count == result_b.trade_count
    assert result_a.realized_pnl_total == result_b.realized_pnl_total
    assert result_a.end_equity == result_b.end_equity
    assert result_a.fee_drag_quote_total == result_b.fee_drag_quote_total
    assert result_a.analytics.profit_factor == result_b.analytics.profit_factor
    assert result_a.analytics.expectancy == result_b.analytics.expectancy
