from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from mctp.core.enums import CommissionAsset, Side
from mctp.core.types import PortfolioSnapshot
from mctp.indicators.models import IndicatorSnapshot
from mctp.portfolio.accounting import AccountingFillRecord
from mctp.portfolio.equity import EquitySnapshot


@dataclass(frozen=True)
class BacktestExecution:
    timestamp: datetime
    side: Side
    quantity: Decimal
    fill_price: Decimal
    commission: Decimal
    commission_asset: CommissionAsset
    reason: str
    order_id: str
    trade_id: str
    reference_price: Decimal
    slippage_quote: Decimal


@dataclass(frozen=True)
class EquityCurvePoint:
    timestamp: datetime
    equity: Decimal
    point_type: str


@dataclass(frozen=True)
class ClosedTrade:
    entry_timestamp: datetime
    exit_timestamp: datetime
    quantity: Decimal
    entry_price: Decimal
    exit_price: Decimal
    gross_pnl: Decimal
    net_pnl: Decimal
    return_pct: Decimal
    exit_reason: str
    trade_id: str = ""


@dataclass(frozen=True)
class DrawdownStats:
    absolute_drawdown: Decimal
    max_drawdown_pct: Decimal
    peak_equity: Decimal
    trough_equity: Decimal
    peak_at: Optional[datetime]
    trough_at: Optional[datetime]


@dataclass(frozen=True)
class OcoDiagnostics:
    tp_exit_count: int
    sl_exit_count: int
    tp_exit_share: Optional[Decimal]
    sl_exit_share: Optional[Decimal]


@dataclass(frozen=True)
class FeeDragDiagnostics:
    total_fee_drag_quote: Decimal
    fee_drag_pct_of_end_equity: Optional[Decimal]


@dataclass(frozen=True)
class SlippageDiagnostics:
    total_slippage_quote: Decimal
    average_slippage_quote: Optional[Decimal]
    total_unfavorable_slippage_quote: Decimal


@dataclass(frozen=True)
class ConsecutiveLossDiagnostics:
    max_consecutive_losses: int
    streak_series: tuple[int, ...]


@dataclass(frozen=True)
class BacktestAnalytics:
    drawdown: DrawdownStats
    profit_factor: Optional[Decimal]
    expectancy: Optional[Decimal]
    sharpe_per_trade: Optional[Decimal]
    sharpe_daily: Optional[Decimal]
    sortino: Optional[Decimal]
    oco: OcoDiagnostics
    fee_drag: FeeDragDiagnostics
    slippage: SlippageDiagnostics
    consecutive_losses: ConsecutiveLossDiagnostics
    daily_returns: tuple[Decimal, ...]
    per_trade_returns: tuple[Decimal, ...]


@dataclass
class BacktestResult:
    start_equity: Decimal
    end_equity: Decimal
    realized_pnl_total: Decimal
    unrealized_pnl: Decimal
    fee_drag_quote_total: Decimal
    execution_count: int
    trade_count: int
    cancelled_order_count: int
    warmup_bars: int
    final_snapshot: PortfolioSnapshot
    final_equity_snapshot: EquitySnapshot
    executions: list[BacktestExecution] = field(default_factory=list)
    accounting_history: list[AccountingFillRecord] = field(default_factory=list)
    equity_curve: list[EquityCurvePoint] = field(default_factory=list)
    closed_trades: list[ClosedTrade] = field(default_factory=list)
    latest_ema: Optional[Decimal] = None
    latest_atr: Optional[Decimal] = None
    latest_indicators: Optional[IndicatorSnapshot] = None
    indicator_source: str = "indicator_engine_v1"
    analytics: Optional[BacktestAnalytics] = None
