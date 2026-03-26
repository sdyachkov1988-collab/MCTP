from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from mctp.backtest.config import BacktestConfig
from mctp.backtest.engine import BacktestEngine
from mctp.backtest.market_replay import BacktestCandle
from mctp.core.enums import CommissionAsset, IntentType, Market, OrderType, Timeframe
from mctp.core.types import Intent, PortfolioSnapshot, Symbol
from mctp.execution.software_stop import SoftwareTrailingStop
from mctp.indicators import IndicatorEngine, compute_warmup_requirements, detect_weighted_patterns, fibonacci_levels, pivot_points
from mctp.indicators.models import Candle
from mctp.risk.adaptive import AdaptiveRiskController
from mctp.sizing.config import SizerConfig
from mctp.sizing.sizer import PositionSizer
from mctp.strategy import StrategyBase, StrategyInput


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
START = datetime(2026, 3, 22, 0, 0, 0, tzinfo=timezone.utc)


def _candle(
    index: int,
    open_price: str,
    high: str,
    low: str,
    close: str,
    volume: str = "10",
) -> Candle:
    return Candle(
        timestamp=START + timedelta(minutes=index),
        open=Decimal(open_price),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal(volume),
    )


def _bt_candle(
    index: int,
    close: Decimal,
    high: Decimal | None = None,
    low: Decimal | None = None,
    volume: Decimal = Decimal("10"),
    bnb_rate: Decimal | None = Decimal("100"),
) -> BacktestCandle:
    return BacktestCandle(
        timestamp=START + timedelta(minutes=index),
        open=close,
        high=high if high is not None else close + Decimal("2"),
        low=low if low is not None else close - Decimal("2"),
        close=close,
        volume=volume,
        bnb_rate=bnb_rate,
    )


def _instrument_info() -> dict:
    return {
        "lot_size": Decimal("0.001"),
        "min_qty": Decimal("0.001"),
        "max_qty": Decimal("1000"),
        "min_notional": Decimal("10"),
    }


def _backtest_config() -> BacktestConfig:
    return BacktestConfig(
        symbol=BTCUSDT,
        initial_quote=Decimal("10000"),
        warmup_bars=5,
        ema_period=3,
        atr_period=3,
        instrument_info=_instrument_info(),
        commission_asset=CommissionAsset.QUOTE,
        entry_order_type=OrderType.MARKET,
    )


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
    candles = [_bt_candle(index, close) for index, close in enumerate(closes)]
    candles[-1] = _bt_candle(
        len(closes) - 1,
        Decimal("128"),
        high=Decimal("130"),
        low=Decimal("126"),
    )
    return candles


def test_indicator_engine_ema_is_deterministic():
    engine = IndicatorEngine()
    candles = [
        _candle(0, "10", "10", "10", "10"),
        _candle(1, "12", "12", "12", "12"),
        _candle(2, "14", "14", "14", "14"),
    ]
    assert engine.ema(candles, period=3) == Decimal("12.5")


def test_indicator_engine_sma_is_deterministic():
    engine = IndicatorEngine()
    candles = [
        _candle(0, "10", "10", "10", "10"),
        _candle(1, "12", "12", "12", "12"),
        _candle(2, "14", "14", "14", "14"),
    ]
    assert engine.sma(candles, period=3) == Decimal("12")


def test_indicator_engine_hull_ma_is_deterministic():
    engine = IndicatorEngine()
    candles = [_candle(index, str(value), str(value), str(value), str(value)) for index, value in enumerate([1, 2, 3, 4, 5])]
    assert engine.hull_ma(candles, period=4) == Decimal("5")


def test_indicator_engine_rsi_is_deterministic():
    engine = IndicatorEngine()
    candles = [
        _candle(0, "10", "10", "10", "10"),
        _candle(1, "12", "12", "12", "12"),
        _candle(2, "11", "11", "11", "11"),
        _candle(3, "13", "13", "13", "13"),
    ]
    assert engine.rsi(candles, period=3) == Decimal("80")


def test_indicator_engine_stochastic_is_deterministic():
    engine = IndicatorEngine()
    candles = [
        _candle(0, "9", "10", "8", "9"),
        _candle(1, "10", "11", "9", "10"),
        _candle(2, "11", "12", "10", "11"),
    ]
    k_value, d_value = engine.stochastic(candles, period=3)
    assert k_value == Decimal("75.00")
    assert d_value == Decimal("75.00")


def test_indicator_engine_cci_is_deterministic():
    engine = IndicatorEngine()
    candles = [
        _candle(0, "9", "10", "8", "9"),
        _candle(1, "10", "11", "9", "10"),
        _candle(2, "11", "12", "10", "11"),
    ]
    assert engine.cci(candles, period=3).quantize(Decimal("0.1")) == Decimal("100.0")


def test_indicator_engine_atr_is_deterministic():
    engine = IndicatorEngine()
    candles = [
        _candle(0, "10", "11", "9", "10"),
        _candle(1, "11", "12", "10", "11"),
        _candle(2, "12", "13", "11", "12"),
        _candle(3, "13", "14", "12", "13"),
    ]
    assert engine.atr(candles, period=3) == Decimal("2")


def test_indicator_engine_bollinger_bands_are_deterministic():
    engine = IndicatorEngine()
    candles = [
        _candle(0, "10", "10", "10", "10"),
        _candle(1, "12", "12", "12", "12"),
        _candle(2, "14", "14", "14", "14"),
    ]
    mid, upper, lower = engine.bollinger_bands(candles, period=3, stddev_mult=Decimal("2"))
    assert mid == Decimal("12")
    assert upper is not None and upper.quantize(Decimal("0.000001")) == Decimal("15.265986")
    assert lower is not None and lower.quantize(Decimal("0.000001")) == Decimal("8.734014")


def test_indicator_engine_keltner_channels_are_deterministic():
    engine = IndicatorEngine()
    candles = [
        _candle(0, "10", "11", "9", "10"),
        _candle(1, "11", "12", "10", "11"),
        _candle(2, "12", "13", "11", "12"),
        _candle(3, "13", "14", "12", "13"),
    ]
    mid, upper, lower = engine.keltner_channels(candles, period=3, atr_mult=Decimal("2"))
    assert mid == Decimal("12.125")
    assert upper == Decimal("16.125")
    assert lower == Decimal("8.125")


def test_indicator_engine_obv_is_deterministic():
    engine = IndicatorEngine()
    candles = [
        _candle(0, "10", "10", "10", "10", volume="5"),
        _candle(1, "12", "12", "12", "12", volume="3"),
        _candle(2, "11", "11", "11", "11", volume="4"),
    ]
    assert engine.obv(candles) == Decimal("-1")


def test_indicator_engine_vwap_is_deterministic():
    engine = IndicatorEngine()
    candles = [
        _candle(0, "10", "10", "10", "10", volume="1"),
        _candle(1, "12", "12", "12", "12", volume="1"),
        _candle(2, "14", "14", "14", "14", volume="2"),
    ]
    assert engine.vwap(candles) == Decimal("12.5")


def test_indicator_engine_cmf_is_deterministic():
    engine = IndicatorEngine()
    candles = [
        _candle(0, "9", "10", "8", "9", volume="10"),
        _candle(1, "11", "11", "9", "11", volume="10"),
        _candle(2, "10", "12", "10", "10", volume="10"),
    ]
    assert engine.cmf(candles, period=3) == Decimal("0")


def test_weighted_patterns_respect_timeframe_weights():
    candles = [
        _candle(0, "12", "13", "9", "10"),
        _candle(1, "10", "14", "9", "13"),
    ]
    m15_signal = detect_weighted_patterns(candles, Timeframe.M15)[0]
    d1_signal = detect_weighted_patterns(candles, Timeframe.D1)[0]
    assert m15_signal.name == "bullish_engulfing"
    assert d1_signal.weight > m15_signal.weight
    assert d1_signal.score > m15_signal.score


def test_fibonacci_levels_are_deterministic():
    candles = [
        _candle(0, "12", "20", "10", "18"),
        _candle(1, "13", "18", "11", "12"),
    ]
    levels = fibonacci_levels(candles)
    assert levels.level_0 == Decimal("20")
    assert levels.level_500 == Decimal("15.0")
    assert levels.level_1000 == Decimal("10")


def test_pivot_points_are_deterministic():
    levels = pivot_points(_candle(0, "10", "12", "8", "10"))
    assert levels.pivot == Decimal("10")
    assert levels.resistance_1 == Decimal("12")
    assert levels.support_1 == Decimal("8")


def test_multi_timeframe_warmup_requirement_is_computed_correctly():
    requirements = compute_warmup_requirements(
        {
            Timeframe.M5: 50,
            Timeframe.M15: 55,
            Timeframe.M30: 60,
            Timeframe.H1: 80,
            Timeframe.H4: 120,
            Timeframe.D1: 200,
            Timeframe.W1: 20,
        }
    )
    req_map = {item.timeframe: item.bars_required for item in requirements}
    assert req_map[Timeframe.D1] == 200
    assert req_map[Timeframe.W1] == 20
    assert len(requirements) == 7


def test_backtest_uses_indicator_engine_instead_of_inline_path(monkeypatch: pytest.MonkeyPatch):
    from mctp.backtest import indicators_inline

    def _boom(*args, **kwargs):
        raise AssertionError("inline path must not be used")

    monkeypatch.setattr(indicators_inline.InlineIndicatorState, "update", _boom)
    result = BacktestEngine(_backtest_config()).run(_tp_sequence())
    assert result.execution_count == 2
    assert result.latest_indicators is not None
    assert result.indicator_source == "indicator_engine_v1"


def test_atr_mult_changes_sizing_deterministically_when_atr_changes():
    controller = AdaptiveRiskController(initial_equity=Decimal("10000"))
    snapshot = PortfolioSnapshot(
        symbol=BTCUSDT,
        held_qty=Decimal("0"),
        avg_cost_basis=Decimal("0"),
        free_quote=Decimal("10000"),
        quote_asset="USDT",
        is_in_position=False,
        meaningful_position=False,
        timestamp=START,
    )
    sizer = PositionSizer(SizerConfig(risk_pct=Decimal("0.01"), use_early_risk_cap=False))

    controller.update_atr_context(Decimal("1"), Decimal("100"))
    low_vol = sizer.calculate(
        snapshot=snapshot,
        stop_distance_pct=Decimal("0.01"),
        instrument_info=_instrument_info(),
        current_price=Decimal("100"),
        risk_multipliers=controller.get_risk_multipliers(),
    )

    controller.update_atr_context(Decimal("4"), Decimal("100"))
    high_vol = sizer.calculate(
        snapshot=snapshot,
        stop_distance_pct=Decimal("0.01"),
        instrument_info=_instrument_info(),
        current_price=Decimal("100"),
        risk_multipliers=controller.get_risk_multipliers(),
    )

    assert controller.regime_mult == Decimal("1.0")
    assert controller.anomaly_mult == Decimal("1.0")
    assert low_vol.quantity is not None and high_vol.quantity is not None
    assert low_vol.quantity > high_vol.quantity


def test_trailing_stop_core_behaves_correctly():
    trailing = SoftwareTrailingStop()
    state = trailing.initialize(entry_price=Decimal("100"), quantity=Decimal("1"), atr=Decimal("10"))
    action = trailing.update(
        state=state,
        highest_price=Decimal("120"),
        current_price=Decimal("118"),
        atr=Decimal("10"),
    )
    assert state.stop_price == Decimal("80")
    assert action.new_stop_price == Decimal("100")


def test_partial_close_core_behaves_correctly():
    trailing = SoftwareTrailingStop()
    state = trailing.initialize(entry_price=Decimal("100"), quantity=Decimal("2"), atr=Decimal("10"))
    action = trailing.update(
        state=state,
        highest_price=Decimal("120"),
        current_price=Decimal("116"),
        atr=Decimal("10"),
    )
    assert action.partial_close_triggered is True
    assert action.partial_close_qty == Decimal("1.00")


def test_strategy_contract_exists_and_is_read_only():
    class DummyStrategy(StrategyBase):
        def on_candle(self, input: StrategyInput) -> Intent:
            return Intent(type=input.indicators["intent_type"], symbol=input.snapshot.symbol, timestamp=START)

    snapshot = PortfolioSnapshot(
        symbol=BTCUSDT,
        held_qty=Decimal("0"),
        avg_cost_basis=Decimal("0"),
        free_quote=Decimal("1000"),
        quote_asset="USDT",
        is_in_position=False,
        meaningful_position=False,
        timestamp=START,
    )
    strategy_input = StrategyInput(
        snapshot=snapshot,
        indicators={"intent_type": IntentType.HOLD},
        candles={Timeframe.M15: [_candle(0, "10", "11", "9", "10")]},
        onchain=None,
    )
    strategy = DummyStrategy()
    intent = strategy.on_candle(strategy_input)
    assert intent.symbol == BTCUSDT
    assert not hasattr(strategy, "submit_order")
    with pytest.raises(FrozenInstanceError):
        strategy_input.snapshot = snapshot
