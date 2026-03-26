from datetime import datetime, timedelta, timezone
from decimal import Decimal

from mctp.backtest.config import BacktestConfig
from mctp.backtest.engine import BacktestEngine
from mctp.backtest.indicators_inline import InlineIndicatorState, ema_next
from mctp.backtest.market_replay import BacktestCandle
from mctp.core.constants import T_CANCEL
from mctp.core.enums import CommissionAsset, Market, OrderType, Side
from mctp.core.types import Symbol
from mctp.execution.oco import OCOOrder, OCOStatus


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
START = datetime(2026, 3, 22, 0, 0, 0, tzinfo=timezone.utc)


def _instrument_info() -> dict:
    return {
        "lot_size": Decimal("0.001"),
        "min_qty": Decimal("0.001"),
        "max_qty": Decimal("1000"),
        "min_notional": Decimal("10"),
    }


def _make_candle(
    index: int,
    close: Decimal,
    high: Decimal | None = None,
    low: Decimal | None = None,
    bnb_rate: Decimal | None = Decimal("100"),
) -> BacktestCandle:
    return BacktestCandle(
        timestamp=START + timedelta(minutes=index),
        open=close,
        high=high if high is not None else close + Decimal("2"),
        low=low if low is not None else close - Decimal("2"),
        close=close,
        bnb_rate=bnb_rate,
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
    candles = [_make_candle(index, close) for index, close in enumerate(closes)]
    candles[-1] = _make_candle(
        len(closes) - 1,
        Decimal("128"),
        high=Decimal("130"),
        low=Decimal("126"),
    )
    return candles


def _cancel_sequence() -> list[BacktestCandle]:
    closes = [
        Decimal("100"),
        Decimal("99"),
        Decimal("98"),
        Decimal("97"),
        Decimal("96"),
        Decimal("110"),
        Decimal("111"),
    ]
    candles = [_make_candle(index, close) for index, close in enumerate(closes)]
    candles[-1] = _make_candle(
        len(closes) - 1,
        Decimal("111"),
        high=Decimal("113"),
        low=Decimal("108"),
    )
    return candles


def _ambiguous_oco_sequence() -> list[BacktestCandle]:
    closes = [
        Decimal("100"),
        Decimal("99"),
        Decimal("98"),
        Decimal("97"),
        Decimal("96"),
        Decimal("110"),
        Decimal("110"),
    ]
    candles = [_make_candle(index, close) for index, close in enumerate(closes)]
    candles[-1] = _make_candle(
        len(closes) - 1,
        Decimal("110"),
        high=Decimal("150"),
        low=Decimal("50"),
    )
    return candles


def _config(
    commission_asset: CommissionAsset = CommissionAsset.QUOTE,
    spread_bps: Decimal = Decimal("0"),
    entry_order_type: OrderType = OrderType.MARKET,
    entry_limit_discount_pct: Decimal = Decimal("0"),
) -> BacktestConfig:
    return BacktestConfig(
        symbol=BTCUSDT,
        initial_quote=Decimal("10000"),
        warmup_bars=5,
        ema_period=3,
        atr_period=3,
        instrument_info=_instrument_info(),
        spread_bps=spread_bps,
        commission_asset=commission_asset,
        entry_order_type=entry_order_type,
        entry_limit_discount_pct=entry_limit_discount_pct,
        cancel_after_seconds=T_CANCEL,
    )


def test_backtest_identical_input_produces_identical_results():
    candles = _tp_sequence()
    config = _config()
    result_a = BacktestEngine(config).run(candles)
    result_b = BacktestEngine(config).run(candles)
    assert result_a.end_equity == result_b.end_equity
    assert result_a.realized_pnl_total == result_b.realized_pnl_total
    assert result_a.fee_drag_quote_total == result_b.fee_drag_quote_total
    assert result_a.execution_count == result_b.execution_count
    assert [(e.reason, e.fill_price, e.quantity) for e in result_a.executions] == [
        (e.reason, e.fill_price, e.quantity) for e in result_b.executions
    ]
    assert [(r.bnb_rate_at_fill, r.fee_drag_quote) for r in result_a.accounting_history] == [
        (r.bnb_rate_at_fill, r.fee_drag_quote) for r in result_b.accounting_history
    ]


def test_backtest_warmup_blocks_trading_before_required_bars_exist():
    candles = _tp_sequence()[:4]
    result = BacktestEngine(_config()).run(candles)
    assert result.warmup_bars == 5
    assert result.execution_count == 0
    assert result.trade_count == 0


def test_inline_ema_is_deterministic_on_known_sequence():
    ema = None
    for close in [Decimal("10"), Decimal("12"), Decimal("14")]:
        ema = ema_next(ema, close, period=3)
    assert ema == Decimal("12.5")


def test_inline_atr_is_deterministic_on_known_sequence():
    state = InlineIndicatorState(ema_period=3, atr_period=3)
    candles = [
        _make_candle(0, Decimal("10"), high=Decimal("11"), low=Decimal("9")),
        _make_candle(1, Decimal("11"), high=Decimal("12"), low=Decimal("10")),
        _make_candle(2, Decimal("12"), high=Decimal("13"), low=Decimal("11")),
        _make_candle(3, Decimal("13"), high=Decimal("14"), low=Decimal("12")),
    ]
    atr_values = []
    for candle in candles:
        _, atr = state.update(candle)
        atr_values.append(atr)
    assert atr_values[0] is None
    assert atr_values[1] is None
    assert atr_values[2] == Decimal("2")
    assert atr_values[3] == Decimal("2")


def test_backtest_quote_fee_case():
    result = BacktestEngine(_config(commission_asset=CommissionAsset.QUOTE)).run(_tp_sequence())
    assert result.execution_count == 2
    assert result.fee_drag_quote_total == Decimal("1.4750000")


def test_backtest_base_fee_case():
    result = BacktestEngine(_config(commission_asset=CommissionAsset.BASE)).run(_tp_sequence())
    assert result.execution_count == 2
    assert result.fee_drag_quote_total == Decimal("1.4742125000")


def test_backtest_bnb_fee_case_with_simulated_bnb_rate():
    result = BacktestEngine(_config(commission_asset=CommissionAsset.BNB)).run(_tp_sequence())
    assert result.execution_count == 2
    assert result.fee_drag_quote_total == Decimal("1.4750000")
    assert result.accounting_history[0].bnb_rate_at_fill == Decimal("100")
    assert result.accounting_history[1].bnb_rate_at_fill == Decimal("100")


def test_backtest_local_oco_simulation():
    result = BacktestEngine(_config()).run(_tp_sequence())
    assert [execution.reason for execution in result.executions] == ["MARKET_ENTRY", "OCO_TP"]
    assert result.trade_count == 1


def test_backtest_ambiguous_same_candle_long_oco_resolves_conservatively_to_stop_loss():
    result = BacktestEngine(_config()).run(_ambiguous_oco_sequence())
    assert [execution.reason for execution in result.executions] == ["MARKET_ENTRY", "OCO_SL"]
    assert result.trade_count == 1
    assert len(result.closed_trades) == 1
    assert result.closed_trades[0].exit_reason == "OCO_SL"
    assert result.realized_pnl_total == result.closed_trades[0].net_pnl
    assert result.analytics is not None
    assert result.analytics.oco.tp_exit_count == 0
    assert result.analytics.oco.sl_exit_count == 1


def test_backtest_ambiguous_same_candle_short_policy_is_conservative_stop_first():
    assert BacktestEngine._resolve_intrabar_protective_exit_leg(
        position_side=Side.SELL,
        tp_hit=True,
        sl_hit=True,
    ) == "SL"


def test_backtest_non_ambiguous_tp_case_remains_take_profit():
    engine = BacktestEngine(_config())
    active_oco = OCOOrder(
        symbol=BTCUSDT,
        tp_price=Decimal("110"),
        sl_stop_price=Decimal("95"),
        sl_limit_price=Decimal("94"),
        quantity=Decimal("1"),
        list_order_id="bt-oco-tp",
        created_at=START,
        updated_at=START,
    )
    updated = engine._process_oco(
        active_oco,
        _make_candle(0, Decimal("100"), high=Decimal("111"), low=Decimal("96")),
        [],
        Decimal("100"),
    )
    assert updated is not None
    assert updated.status == OCOStatus.TP_FILLED


def test_backtest_non_ambiguous_sl_case_remains_stop_loss():
    engine = BacktestEngine(_config())
    active_oco = OCOOrder(
        symbol=BTCUSDT,
        tp_price=Decimal("110"),
        sl_stop_price=Decimal("95"),
        sl_limit_price=Decimal("94"),
        quantity=Decimal("1"),
        list_order_id="bt-oco-sl",
        created_at=START,
        updated_at=START,
    )
    updated = engine._process_oco(
        active_oco,
        _make_candle(0, Decimal("100"), high=Decimal("109"), low=Decimal("94")),
        [],
        Decimal("100"),
    )
    assert updated is not None
    assert updated.status == OCOStatus.SL_TRIGGERED


def test_backtest_t_cancel_simulation():
    result = BacktestEngine(
        _config(
            entry_order_type=OrderType.LIMIT,
            entry_limit_discount_pct=Decimal("0.10"),
        )
    ).run(_cancel_sequence())
    assert result.cancelled_order_count == 1
    assert result.execution_count == 0


def test_backtest_spread_model_affects_execution_consistently():
    no_spread = BacktestEngine(_config(spread_bps=Decimal("0"))).run(_tp_sequence())
    with_spread = BacktestEngine(_config(spread_bps=Decimal("100"))).run(_tp_sequence())
    assert with_spread.executions[0].fill_price > no_spread.executions[0].fill_price
    assert with_spread.end_equity < no_spread.end_equity
