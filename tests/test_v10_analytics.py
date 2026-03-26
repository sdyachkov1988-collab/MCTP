from datetime import datetime, timedelta, timezone
from decimal import Decimal

from mctp.backtest.analytics import (
    analyze_backtest,
    drawdown_stats,
    expectancy,
    profit_factor,
    sharpe_ratio,
    sortino_ratio,
)
from mctp.backtest.config import BacktestConfig
from mctp.backtest.engine import BacktestEngine
from mctp.backtest.market_replay import BacktestCandle
from mctp.backtest.results import (
    BacktestExecution,
    BacktestResult,
    ClosedTrade,
    EquityCurvePoint,
)
from mctp.core.enums import CommissionAsset, Market, Side
from mctp.core.types import PortfolioSnapshot, Symbol
from mctp.portfolio.accounting import AccountingFillRecord
from mctp.portfolio.equity import EquitySnapshot


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


def _engine_result() -> BacktestResult:
    config = BacktestConfig(
        symbol=BTCUSDT,
        initial_quote=Decimal("10000"),
        warmup_bars=5,
        ema_period=3,
        atr_period=3,
        instrument_info=_instrument_info(),
    )
    return BacktestEngine(config).run(_tp_sequence())


def _manual_result(
    *,
    equity_curve: list[EquityCurvePoint],
    closed_trades: list[ClosedTrade],
    executions: list[BacktestExecution] | None = None,
    fee_drag_quote_total: Decimal = Decimal("0"),
    accounting_history: list[AccountingFillRecord] | None = None,
) -> BacktestResult:
    final_snapshot = PortfolioSnapshot(
        symbol=BTCUSDT,
        held_qty=Decimal("0"),
        avg_cost_basis=Decimal("0"),
        free_quote=equity_curve[-1].equity if equity_curve else Decimal("0"),
        quote_asset="USDT",
        is_in_position=False,
        meaningful_position=False,
        timestamp=equity_curve[-1].timestamp if equity_curve else START,
    )
    final_equity = EquitySnapshot(
        timestamp=final_snapshot.timestamp,
        total_equity=final_snapshot.free_quote,
        free_quote=final_snapshot.free_quote,
        held_qty=Decimal("0"),
        held_value=Decimal("0"),
        current_price=Decimal("0"),
        is_in_position=False,
        meaningful_position=False,
    )
    result = BacktestResult(
        start_equity=equity_curve[0].equity if equity_curve else Decimal("0"),
        end_equity=equity_curve[-1].equity if equity_curve else Decimal("0"),
        realized_pnl_total=sum((trade.net_pnl for trade in closed_trades), Decimal("0")),
        unrealized_pnl=Decimal("0"),
        fee_drag_quote_total=fee_drag_quote_total,
        execution_count=len(executions or []),
        trade_count=len(closed_trades),
        cancelled_order_count=0,
        warmup_bars=0,
        final_snapshot=final_snapshot,
        final_equity_snapshot=final_equity,
        executions=executions or [],
        accounting_history=accounting_history or [],
        equity_curve=equity_curve,
        closed_trades=closed_trades,
        latest_ema=None,
        latest_atr=None,
    )
    result.analytics = analyze_backtest(result)
    return result


def test_continuous_equity_curve_is_produced_deterministically():
    result_a = _engine_result()
    result_b = _engine_result()
    assert [(point.timestamp, point.equity, point.point_type) for point in result_a.equity_curve] == [
        (point.timestamp, point.equity, point.point_type) for point in result_b.equity_curve
    ]
    assert result_a.equity_curve


def test_equity_curve_reflects_hold_snapshots_and_fills():
    result = _engine_result()
    point_types = [point.point_type for point in result.equity_curve]
    assert "HOLD" in point_types
    assert "FILL" in point_types
    assert result.equity_curve[-1].equity == result.end_equity


def test_drawdown_metrics_behave_correctly_on_known_sequence():
    equity_curve = [
        EquityCurvePoint(START, Decimal("100"), "HOLD"),
        EquityCurvePoint(START + timedelta(days=1), Decimal("120"), "HOLD"),
        EquityCurvePoint(START + timedelta(days=2), Decimal("90"), "HOLD"),
        EquityCurvePoint(START + timedelta(days=3), Decimal("150"), "HOLD"),
    ]
    stats = drawdown_stats(_manual_result(equity_curve=equity_curve, closed_trades=[]))
    assert stats.absolute_drawdown == Decimal("30")
    assert stats.max_drawdown_pct == Decimal("0.25")
    assert stats.peak_equity == Decimal("120")
    assert stats.trough_equity == Decimal("90")


def test_profit_factor_behaves_correctly_on_known_trade_set():
    trades = [
        ClosedTrade(START, START, Decimal("1"), Decimal("100"), Decimal("110"), Decimal("10"), Decimal("10"), Decimal("0.1"), "OCO_TP"),
        ClosedTrade(START, START, Decimal("1"), Decimal("100"), Decimal("95"), Decimal("-5"), Decimal("-5"), Decimal("-0.05"), "OCO_SL"),
        ClosedTrade(START, START, Decimal("1"), Decimal("100"), Decimal("120"), Decimal("20"), Decimal("20"), Decimal("0.2"), "OCO_TP"),
    ]
    result = _manual_result(equity_curve=[EquityCurvePoint(START, Decimal("100"), "HOLD")], closed_trades=trades)
    assert profit_factor(result) == Decimal("6")


def test_expectancy_behaves_correctly_on_known_trade_set():
    trades = [
        ClosedTrade(START, START, Decimal("1"), Decimal("100"), Decimal("110"), Decimal("10"), Decimal("10"), Decimal("0.1"), "OCO_TP"),
        ClosedTrade(START, START, Decimal("1"), Decimal("100"), Decimal("95"), Decimal("-5"), Decimal("-5"), Decimal("-0.05"), "OCO_SL"),
        ClosedTrade(START, START, Decimal("1"), Decimal("100"), Decimal("98"), Decimal("-2"), Decimal("-2"), Decimal("-0.02"), "OCO_SL"),
    ]
    result = _manual_result(equity_curve=[EquityCurvePoint(START, Decimal("100"), "HOLD")], closed_trades=trades)
    assert expectancy(result) == Decimal("1")


def test_per_trade_sharpe_behaves_deterministically_on_known_trade_set():
    value = sharpe_ratio((Decimal("0.1"), Decimal("-0.05"), Decimal("0.15")))
    assert value is not None
    assert abs(value - Decimal("0.640512615")) < Decimal("0.000001")


def test_daily_sharpe_behaves_deterministically_on_known_daily_sequence():
    equity_curve = [
        EquityCurvePoint(START, Decimal("100"), "HOLD"),
        EquityCurvePoint(START + timedelta(days=1), Decimal("110"), "HOLD"),
        EquityCurvePoint(START + timedelta(days=2), Decimal("104.5"), "HOLD"),
        EquityCurvePoint(START + timedelta(days=3), Decimal("125.4"), "HOLD"),
    ]
    result = _manual_result(equity_curve=equity_curve, closed_trades=[])
    assert result.analytics is not None
    assert result.analytics.sharpe_daily is not None
    assert abs(result.analytics.sharpe_daily - Decimal("0.662266178")) < Decimal("0.000001")


def test_sortino_behaves_deterministically_on_known_sample():
    value = sortino_ratio((Decimal("0.1"), Decimal("-0.05"), Decimal("0.15")))
    assert value is not None
    assert abs(value - Decimal("2.309401076")) < Decimal("0.000001")


def test_oco_diagnostics_correctly_classify_tp_vs_sl_share():
    trades = [
        ClosedTrade(START, START, Decimal("1"), Decimal("100"), Decimal("110"), Decimal("10"), Decimal("10"), Decimal("0.1"), "OCO_TP"),
        ClosedTrade(START, START, Decimal("1"), Decimal("100"), Decimal("95"), Decimal("-5"), Decimal("-5"), Decimal("-0.05"), "OCO_SL"),
        ClosedTrade(START, START, Decimal("1"), Decimal("100"), Decimal("112"), Decimal("12"), Decimal("12"), Decimal("0.12"), "OCO_TP"),
    ]
    result = _manual_result(equity_curve=[EquityCurvePoint(START, Decimal("100"), "HOLD")], closed_trades=trades)
    assert result.analytics is not None
    assert result.analytics.oco.tp_exit_share == Decimal("0.6666666666666666666666666667")
    assert result.analytics.oco.sl_exit_share == Decimal("0.3333333333333333333333333333")


def test_fee_drag_diagnostics_match_accounting_totals():
    result = _engine_result()
    assert result.analytics is not None
    assert result.analytics.fee_drag.total_fee_drag_quote == result.fee_drag_quote_total


def test_slippage_diagnostics_behave_correctly_on_known_scenario():
    executions = [
        BacktestExecution(
            timestamp=START,
            side=Side.BUY,
            quantity=Decimal("1"),
            fill_price=Decimal("101"),
            commission=Decimal("0"),
            commission_asset=CommissionAsset.QUOTE,
            reason="ENTRY",
            order_id="o1",
            trade_id="t1",
            reference_price=Decimal("100"),
            slippage_quote=Decimal("1"),
        ),
        BacktestExecution(
            timestamp=START + timedelta(minutes=1),
            side=Side.SELL,
            quantity=Decimal("1"),
            fill_price=Decimal("99"),
            commission=Decimal("0"),
            commission_asset=CommissionAsset.QUOTE,
            reason="EXIT",
            order_id="o2",
            trade_id="t2",
            reference_price=Decimal("100"),
            slippage_quote=Decimal("1"),
        ),
    ]
    result = _manual_result(
        equity_curve=[EquityCurvePoint(START, Decimal("100"), "HOLD")],
        closed_trades=[],
        executions=executions,
    )
    assert result.analytics is not None
    assert result.analytics.slippage.total_slippage_quote == Decimal("2")
    assert result.analytics.slippage.average_slippage_quote == Decimal("1")
    assert result.analytics.slippage.total_unfavorable_slippage_quote == Decimal("2")


def test_consecutive_loss_diagnostics_detect_correct_max_streak():
    trades = [
        ClosedTrade(START, START, Decimal("1"), Decimal("100"), Decimal("110"), Decimal("10"), Decimal("10"), Decimal("0.1"), "OCO_TP"),
        ClosedTrade(START, START, Decimal("1"), Decimal("100"), Decimal("95"), Decimal("-5"), Decimal("-5"), Decimal("-0.05"), "OCO_SL"),
        ClosedTrade(START, START, Decimal("1"), Decimal("100"), Decimal("97"), Decimal("-3"), Decimal("-3"), Decimal("-0.03"), "OCO_SL"),
        ClosedTrade(START, START, Decimal("1"), Decimal("100"), Decimal("107"), Decimal("7"), Decimal("7"), Decimal("0.07"), "OCO_TP"),
        ClosedTrade(START, START, Decimal("1"), Decimal("100"), Decimal("99"), Decimal("-1"), Decimal("-1"), Decimal("-0.01"), "OCO_SL"),
    ]
    result = _manual_result(equity_curve=[EquityCurvePoint(START, Decimal("100"), "HOLD")], closed_trades=trades)
    assert result.analytics is not None
    assert result.analytics.consecutive_losses.max_consecutive_losses == 2
