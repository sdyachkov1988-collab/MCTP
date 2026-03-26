from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from mctp.backtest import BacktestCandle, BacktestConfig, BacktestEngine
from mctp.core.constants import STRATEGY_ID_V20_BTCUSDT_MTF, V20_MTF_REQUIRED_M15_CANDLES
from mctp.core.enums import IntentType, Market, Timeframe
from mctp.core.types import PortfolioSnapshot, Symbol
from mctp.indicators.models import Candle
from mctp.runtime import KlineEvent
from mctp.runtime.paper import PaperRuntime, PaperRuntimeConfig
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.snapshot_store import SnapshotStore
from mctp.strategy import BtcUsdtMtfV20Strategy, StrategyInput, build_closed_mtf_candle_map_from_m15
from mctp.streams.base import StreamType


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
ETHUSDT = Symbol("ETH", "USDT", Market.SPOT)
START = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def _snapshot(symbol: Symbol = BTCUSDT, *, in_position: bool = False) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        symbol=symbol,
        held_qty=Decimal("1") if in_position else Decimal("0"),
        avg_cost_basis=Decimal("100") if in_position else Decimal("0"),
        free_quote=Decimal("10000"),
        quote_asset=symbol.quote,
        is_in_position=in_position,
        meaningful_position=in_position,
        timestamp=START,
    )


def _step_for_timeframe(timeframe: Timeframe) -> timedelta:
    if timeframe == Timeframe.M15:
        return timedelta(minutes=15)
    if timeframe == Timeframe.H1:
        return timedelta(hours=1)
    if timeframe == Timeframe.H4:
        return timedelta(hours=4)
    if timeframe == Timeframe.D1:
        return timedelta(days=1)
    raise ValueError(f"Unsupported timeframe {timeframe.value}")


def _candle_sequence(
    timeframe: Timeframe,
    closes: list[Decimal],
    *,
    start: datetime,
) -> list[Candle]:
    candles: list[Candle] = []
    previous_close = closes[0]
    step = _step_for_timeframe(timeframe)
    for index, close in enumerate(closes):
        timestamp = start + (step * index)
        open_price = previous_close if index > 0 else close
        if close >= open_price:
            high = close + Decimal("2")
            low = open_price - Decimal("1")
        else:
            high = open_price + Decimal("1")
            low = close - Decimal("2")
        candles.append(
            Candle(
                timestamp=timestamp,
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=Decimal("1"),
                closed=True,
            )
        )
        previous_close = close
    return candles


def _bullish_m15_trigger() -> list[Candle]:
    return [
        Candle(
            timestamp=START,
            open=Decimal("110"),
            high=Decimal("111"),
            low=Decimal("107"),
            close=Decimal("108"),
            volume=Decimal("1"),
            closed=True,
        ),
        Candle(
            timestamp=START + timedelta(minutes=15),
            open=Decimal("107"),
            high=Decimal("113"),
            low=Decimal("106"),
            close=Decimal("112"),
            volume=Decimal("1"),
            closed=True,
        ),
    ]


def _neutral_m15_trigger() -> list[Candle]:
    return [
        Candle(
            timestamp=START,
            open=Decimal("110"),
            high=Decimal("112"),
            low=Decimal("109"),
            close=Decimal("111"),
            volume=Decimal("1"),
            closed=True,
        ),
        Candle(
            timestamp=START + timedelta(minutes=15),
            open=Decimal("111"),
            high=Decimal("113"),
            low=Decimal("110"),
            close=Decimal("112"),
            volume=Decimal("1"),
            closed=True,
        ),
    ]


def _strong_bullish_d1() -> list[Candle]:
    closes = [Decimal("100") + Decimal(index) for index in range(200)]
    return _candle_sequence(Timeframe.D1, closes, start=START)


def _bearish_d1() -> list[Candle]:
    closes = [Decimal("300") - Decimal(index) for index in range(200)]
    return _candle_sequence(Timeframe.D1, closes, start=START)


def _weak_trend_d1() -> list[Candle]:
    closes = [Decimal("100") for _ in range(200)]
    return _candle_sequence(Timeframe.D1, closes, start=START)


def _bullish_h4() -> list[Candle]:
    closes = [Decimal("200") + (Decimal(index) * Decimal("2")) for index in range(21)]
    return _candle_sequence(Timeframe.H4, closes, start=START)


def _bearish_h4() -> list[Candle]:
    closes = [Decimal("260") - (Decimal(index) * Decimal("2")) for index in range(21)]
    return _candle_sequence(Timeframe.H4, closes, start=START)


def _h1_entry_ok() -> list[Candle]:
    closes = [
        Decimal("100"),
        Decimal("102"),
        Decimal("101"),
        Decimal("103"),
        Decimal("102"),
        Decimal("104"),
        Decimal("103"),
        Decimal("105"),
        Decimal("104"),
        Decimal("106"),
        Decimal("105"),
        Decimal("107"),
        Decimal("106"),
        Decimal("108"),
        Decimal("107"),
    ]
    return _candle_sequence(Timeframe.H1, closes, start=START)


def _h1_overbought() -> list[Candle]:
    closes = [
        Decimal("100"),
        Decimal("103"),
        Decimal("102"),
        Decimal("105"),
        Decimal("104"),
        Decimal("107"),
        Decimal("106"),
        Decimal("109"),
        Decimal("108"),
        Decimal("111"),
        Decimal("110"),
        Decimal("113"),
        Decimal("112"),
        Decimal("115"),
        Decimal("114"),
    ]
    return _candle_sequence(Timeframe.H1, closes, start=START)


def _instrument_info() -> dict[str, Decimal]:
    return {
        "lot_size": Decimal("0.001"),
        "min_qty": Decimal("0.001"),
        "max_qty": Decimal("1000"),
        "min_notional": Decimal("10"),
    }


def _long_backtest_history(count: int) -> list[BacktestCandle]:
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


def _legacy_backtest_sequence() -> list[BacktestCandle]:
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
                volume=Decimal("1"),
                bnb_rate=Decimal("100"),
            )
        )
    candles[-1] = BacktestCandle(
        timestamp=START + timedelta(minutes=len(closes) - 1),
        open=Decimal("128"),
        high=Decimal("130"),
        low=Decimal("126"),
        close=Decimal("128"),
        volume=Decimal("1"),
        bnb_rate=Decimal("100"),
    )
    return candles


class RecordingBtcUsdtMtfStrategy(BtcUsdtMtfV20Strategy):
    def __init__(self) -> None:
        super().__init__()
        self.inputs: list[StrategyInput] = []

    def on_candle(self, input: StrategyInput):
        self.inputs.append(input)
        return super().on_candle(input)


def test_m15_to_higher_timeframes_is_utc_aligned_and_closed_only():
    base: list[Candle] = []
    for index in range(96):
        timestamp = START + timedelta(minutes=15 * index)
        close = Decimal("100") + Decimal(index)
        base.append(
            Candle(
                timestamp=timestamp,
                open=close,
                high=close + Decimal("1"),
                low=close - Decimal("1"),
                close=close,
                volume=Decimal("1"),
                closed=True,
            )
        )
    base.append(
        Candle(
            timestamp=START + timedelta(minutes=15 * 96),
            open=Decimal("500"),
            high=Decimal("501"),
            low=Decimal("499"),
            close=Decimal("500"),
            volume=Decimal("1"),
            closed=False,
        )
    )
    candles = build_closed_mtf_candle_map_from_m15(base)
    assert len(candles[Timeframe.M15]) == 96
    assert len(candles[Timeframe.H1]) == 24
    assert len(candles[Timeframe.H4]) == 6
    assert len(candles[Timeframe.D1]) == 1
    assert candles[Timeframe.H1][0].timestamp == START
    assert candles[Timeframe.H4][1].timestamp == START + timedelta(hours=4)
    assert candles[Timeframe.D1][0].timestamp == START


def test_v20_strategy_incomplete_context_returns_hold():
    strategy = BtcUsdtMtfV20Strategy()
    intent = strategy.on_candle(
        StrategyInput(
            snapshot=_snapshot(),
            indicators={},
            candles={Timeframe.M15: _bullish_m15_trigger()},
            onchain=None,
        )
    )
    assert intent.type == IntentType.HOLD


def test_v20_strategy_bearish_daily_filter_returns_hold():
    strategy = BtcUsdtMtfV20Strategy()
    intent = strategy.on_candle(
        StrategyInput(
            snapshot=_snapshot(),
            indicators={},
            candles={
                Timeframe.M15: _bullish_m15_trigger(),
                Timeframe.H1: _h1_entry_ok(),
                Timeframe.H4: _bullish_h4(),
                Timeframe.D1: _bearish_d1(),
            },
            onchain=None,
        )
    )
    assert intent.type == IntentType.HOLD


def test_v20_strategy_weak_trend_zone_near_ema200_returns_hold():
    strategy = BtcUsdtMtfV20Strategy()
    intent = strategy.on_candle(
        StrategyInput(
            snapshot=_snapshot(),
            indicators={},
            candles={
                Timeframe.M15: _bullish_m15_trigger(),
                Timeframe.H1: _h1_entry_ok(),
                Timeframe.H4: _bullish_h4(),
                Timeframe.D1: _weak_trend_d1(),
            },
            onchain=None,
        )
    )
    assert intent.type == IntentType.HOLD


def test_v20_strategy_conflicting_h4_h1_m15_conditions_return_hold():
    strategy = BtcUsdtMtfV20Strategy()
    intent = strategy.on_candle(
        StrategyInput(
            snapshot=_snapshot(),
            indicators={},
            candles={
                Timeframe.M15: _neutral_m15_trigger(),
                Timeframe.H1: _h1_entry_ok(),
                Timeframe.H4: _bullish_h4(),
                Timeframe.D1: _strong_bullish_d1(),
            },
            onchain=None,
        )
    )
    assert intent.type == IntentType.HOLD


def test_v20_strategy_fully_aligned_bullish_case_returns_buy():
    strategy = BtcUsdtMtfV20Strategy()
    intent = strategy.on_candle(
        StrategyInput(
            snapshot=_snapshot(),
            indicators={},
            candles={
                Timeframe.M15: _bullish_m15_trigger(),
                Timeframe.H1: _h1_entry_ok(),
                Timeframe.H4: _bullish_h4(),
                Timeframe.D1: _strong_bullish_d1(),
            },
            onchain=None,
        )
    )
    assert intent.type == IntentType.BUY


def test_v20_strategy_in_position_overbought_and_bearish_h4_returns_sell():
    strategy = BtcUsdtMtfV20Strategy()
    intent = strategy.on_candle(
        StrategyInput(
            snapshot=_snapshot(in_position=True),
            indicators={},
            candles={
                Timeframe.M15: _bullish_m15_trigger(),
                Timeframe.H1: _h1_overbought(),
                Timeframe.H4: _bearish_h4(),
            },
            onchain=None,
        )
    )
    assert intent.type == IntentType.SELL


def test_v20_strategy_non_btcusdt_symbol_returns_hold():
    strategy = BtcUsdtMtfV20Strategy()
    intent = strategy.on_candle(
        StrategyInput(
            snapshot=_snapshot(symbol=ETHUSDT),
            indicators={},
            candles={
                Timeframe.M15: _bullish_m15_trigger(),
                Timeframe.H1: _h1_entry_ok(),
                Timeframe.H4: _bullish_h4(),
                Timeframe.D1: _strong_bullish_d1(),
            },
            onchain=None,
        )
    )
    assert intent.type == IntentType.HOLD


def test_backtest_v20_strategy_runs_without_breaking_legacy_default():
    legacy_result = BacktestEngine(
        BacktestConfig(
            symbol=BTCUSDT,
            initial_quote=Decimal("10000"),
            warmup_bars=5,
            ema_period=3,
            atr_period=3,
            instrument_info=_instrument_info(),
        )
    ).run(_legacy_backtest_sequence())
    v20_result = BacktestEngine(
        BacktestConfig(
            symbol=BTCUSDT,
            initial_quote=Decimal("10000"),
            warmup_bars=5,
            ema_period=3,
            atr_period=3,
            instrument_info=_instrument_info(),
            strategy_id=STRATEGY_ID_V20_BTCUSDT_MTF,
        )
    ).run(_long_backtest_history(V20_MTF_REQUIRED_M15_CANDLES))
    assert legacy_result.execution_count == 2
    assert legacy_result.trade_count == 1
    assert v20_result.warmup_bars == V20_MTF_REQUIRED_M15_CANDLES
    assert v20_result.execution_count == 0
    assert v20_result.trade_count == 0


@pytest.mark.asyncio
async def test_paper_runtime_builds_mtf_strategy_input_for_v20_strategy(tmp_path):
    strategy = RecordingBtcUsdtMtfStrategy()
    runtime = PaperRuntime(
        config=PaperRuntimeConfig(
            symbol=BTCUSDT,
            timeframe=Timeframe.M15,
            instrument_info=_instrument_info(),
            initial_balances={"BTC": Decimal("0"), "USDT": Decimal("1000")},
            warmup_bars=V20_MTF_REQUIRED_M15_CANDLES,
        ),
        strategy=strategy,
        snapshot_store=SnapshotStore(str(tmp_path / "snapshot.json")),
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        accounting_store=AccountingStore(str(tmp_path / "accounting.json")),
    )
    await runtime.start()
    preload_history = []
    for index in range(V20_MTF_REQUIRED_M15_CANDLES - 1):
        close = Decimal("100") + (Decimal(index) / Decimal("1000"))
        preload_history.append(
            Candle(
                timestamp=START + timedelta(minutes=15 * index),
                open=close,
                high=close + Decimal("1"),
                low=close - Decimal("1"),
                close=close,
                volume=Decimal("1"),
                closed=True,
            )
        )
    runtime.candles[Timeframe.M15] = preload_history
    close = Decimal("100") + (Decimal(V20_MTF_REQUIRED_M15_CANDLES - 1) / Decimal("1000"))
    candle = Candle(
        timestamp=START + timedelta(minutes=15 * (V20_MTF_REQUIRED_M15_CANDLES - 1)),
        open=close,
        high=close + Decimal("1"),
        low=close - Decimal("1"),
        close=close,
        volume=Decimal("1"),
        closed=True,
    )
    await runtime.channels[StreamType.KLINE].publish(KlineEvent(timeframe=Timeframe.M15, candle=candle))
    await runtime.process_all_available()
    assert len(strategy.inputs) == 1
    assert runtime.last_strategy_input is not None
    assert set(runtime.last_strategy_input.candles) == {
        Timeframe.M15,
        Timeframe.H1,
        Timeframe.H4,
        Timeframe.D1,
    }
    assert len(runtime.last_strategy_input.candles[Timeframe.M15]) == V20_MTF_REQUIRED_M15_CANDLES
    assert len(runtime.last_strategy_input.candles[Timeframe.H1]) == V20_MTF_REQUIRED_M15_CANDLES // 4
    assert len(runtime.last_strategy_input.candles[Timeframe.H4]) == V20_MTF_REQUIRED_M15_CANDLES // 16
    assert len(runtime.last_strategy_input.candles[Timeframe.D1]) == 200
    await runtime.shutdown()
