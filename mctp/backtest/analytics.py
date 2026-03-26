from collections import defaultdict
from decimal import Decimal, getcontext
from typing import Optional

from mctp.backtest.results import (
    BacktestAnalytics,
    BacktestResult,
    ConsecutiveLossDiagnostics,
    DrawdownStats,
    FeeDragDiagnostics,
    OcoDiagnostics,
    SlippageDiagnostics,
)


def analyze_backtest(result: BacktestResult) -> BacktestAnalytics:
    per_trade_returns = tuple(trade.return_pct for trade in result.closed_trades)
    daily_returns = _daily_returns(result)
    return BacktestAnalytics(
        drawdown=drawdown_stats(result),
        profit_factor=profit_factor(result),
        expectancy=expectancy(result),
        sharpe_per_trade=sharpe_ratio(per_trade_returns),
        sharpe_daily=sharpe_ratio(daily_returns),
        sortino=sortino_ratio(per_trade_returns),
        oco=oco_diagnostics(result),
        fee_drag=fee_drag_diagnostics(result),
        slippage=slippage_diagnostics(result),
        consecutive_losses=consecutive_loss_diagnostics(result),
        daily_returns=daily_returns,
        per_trade_returns=per_trade_returns,
    )


def drawdown_stats(result: BacktestResult) -> DrawdownStats:
    if not result.equity_curve:
        return DrawdownStats(
            absolute_drawdown=Decimal("0"),
            max_drawdown_pct=Decimal("0"),
            peak_equity=result.end_equity,
            trough_equity=result.end_equity,
            peak_at=None,
            trough_at=None,
        )
    peak_equity = result.equity_curve[0].equity
    peak_at = result.equity_curve[0].timestamp
    max_abs = Decimal("0")
    max_pct = Decimal("0")
    drawdown_peak_equity = peak_equity
    drawdown_peak_at = peak_at
    trough_equity = peak_equity
    trough_at = peak_at
    for point in result.equity_curve:
        if point.equity > peak_equity:
            peak_equity = point.equity
            peak_at = point.timestamp
        abs_dd = peak_equity - point.equity
        pct_dd = abs_dd / peak_equity if peak_equity > Decimal("0") else Decimal("0")
        if abs_dd > max_abs:
            max_abs = abs_dd
            max_pct = pct_dd
            drawdown_peak_equity = peak_equity
            drawdown_peak_at = peak_at
            trough_equity = point.equity
            trough_at = point.timestamp
    return DrawdownStats(
        absolute_drawdown=max_abs,
        max_drawdown_pct=max_pct,
        peak_equity=drawdown_peak_equity,
        trough_equity=trough_equity,
        peak_at=drawdown_peak_at,
        trough_at=trough_at,
    )


def profit_factor(result: BacktestResult) -> Optional[Decimal]:
    if not result.closed_trades:
        return None
    gross_profit = sum((trade.net_pnl for trade in result.closed_trades if trade.net_pnl > Decimal("0")), Decimal("0"))
    gross_loss = sum((-trade.net_pnl for trade in result.closed_trades if trade.net_pnl < Decimal("0")), Decimal("0"))
    if gross_loss == Decimal("0"):
        return Decimal("Infinity") if gross_profit > Decimal("0") else None
    return gross_profit / gross_loss


def expectancy(result: BacktestResult) -> Optional[Decimal]:
    if not result.closed_trades:
        return None
    total = sum((trade.net_pnl for trade in result.closed_trades), Decimal("0"))
    return total / Decimal(len(result.closed_trades))


def sharpe_ratio(samples: tuple[Decimal, ...]) -> Optional[Decimal]:
    if len(samples) < 2:
        return None
    mean = sum(samples, Decimal("0")) / Decimal(len(samples))
    variance_sum = sum(((sample - mean) ** 2 for sample in samples), Decimal("0"))
    if variance_sum == Decimal("0"):
        return None
    variance = variance_sum / Decimal(len(samples) - 1)
    stddev = variance.sqrt()
    if stddev == Decimal("0"):
        return None
    return mean / stddev


def sortino_ratio(samples: tuple[Decimal, ...]) -> Optional[Decimal]:
    if len(samples) < 2:
        return None
    mean = sum(samples, Decimal("0")) / Decimal(len(samples))
    downside = [sample for sample in samples if sample < Decimal("0")]
    if not downside:
        return None
    downside_sq = sum((sample ** 2 for sample in downside), Decimal("0")) / Decimal(len(samples))
    if downside_sq == Decimal("0"):
        return None
    downside_dev = downside_sq.sqrt()
    if downside_dev == Decimal("0"):
        return None
    return mean / downside_dev


def oco_diagnostics(result: BacktestResult) -> OcoDiagnostics:
    tp_count = sum(1 for trade in result.closed_trades if trade.exit_reason == "OCO_TP")
    sl_count = sum(1 for trade in result.closed_trades if trade.exit_reason == "OCO_SL")
    total = tp_count + sl_count
    if total == 0:
        return OcoDiagnostics(tp_count, sl_count, None, None)
    total_dec = Decimal(total)
    return OcoDiagnostics(
        tp_exit_count=tp_count,
        sl_exit_count=sl_count,
        tp_exit_share=Decimal(tp_count) / total_dec,
        sl_exit_share=Decimal(sl_count) / total_dec,
    )


def fee_drag_diagnostics(result: BacktestResult) -> FeeDragDiagnostics:
    pct = None
    if result.end_equity > Decimal("0"):
        pct = result.fee_drag_quote_total / result.end_equity
    return FeeDragDiagnostics(
        total_fee_drag_quote=result.fee_drag_quote_total,
        fee_drag_pct_of_end_equity=pct,
    )


def slippage_diagnostics(result: BacktestResult) -> SlippageDiagnostics:
    if not result.executions:
        return SlippageDiagnostics(
            total_slippage_quote=Decimal("0"),
            average_slippage_quote=None,
            total_unfavorable_slippage_quote=Decimal("0"),
        )
    total = sum((execution.slippage_quote for execution in result.executions), Decimal("0"))
    unfavorable = sum(
        (execution.slippage_quote for execution in result.executions if execution.slippage_quote > Decimal("0")),
        Decimal("0"),
    )
    return SlippageDiagnostics(
        total_slippage_quote=total,
        average_slippage_quote=total / Decimal(len(result.executions)),
        total_unfavorable_slippage_quote=unfavorable,
    )


def consecutive_loss_diagnostics(result: BacktestResult) -> ConsecutiveLossDiagnostics:
    streaks: list[int] = []
    current = 0
    max_streak = 0
    for trade in result.closed_trades:
        if trade.net_pnl < Decimal("0"):
            current += 1
            max_streak = max(max_streak, current)
            streaks.append(current)
        else:
            current = 0
            streaks.append(0)
    return ConsecutiveLossDiagnostics(
        max_consecutive_losses=max_streak,
        streak_series=tuple(streaks),
    )


def _daily_returns(result: BacktestResult) -> tuple[Decimal, ...]:
    if not result.equity_curve:
        return tuple()
    last_per_day: dict[object, Decimal] = {}
    for point in result.equity_curve:
        last_per_day[point.timestamp.date()] = point.equity
    ordered_dates = sorted(last_per_day)
    returns: list[Decimal] = []
    previous_equity: Optional[Decimal] = None
    for day in ordered_dates:
        equity = last_per_day[day]
        if previous_equity is not None and previous_equity > Decimal("0"):
            returns.append((equity - previous_equity) / previous_equity)
        previous_equity = equity
    return tuple(returns)
