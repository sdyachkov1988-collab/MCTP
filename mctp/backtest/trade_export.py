import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from mctp.backtest.results import BacktestResult, ClosedTrade
from mctp.core.types import Symbol


TRADE_EXPORT_HEADERS = (
    "trade_id",
    "symbol",
    "side",
    "entry_time_utc",
    "exit_time_utc",
    "entry_price",
    "exit_price",
    "quantity",
    "holding_minutes",
    "exit_reason",
    "gross_pnl_quote",
    "fees_quote_total",
    "net_pnl_quote",
    "entry_notional_quote",
    "exit_notional_quote",
    "executed_quantity",
    "was_tp_exit",
    "was_sl_exit",
    "was_forced_exit",
    "trade_won",
)


@dataclass(frozen=True)
class ExportedTradeRow:
    trade_id: str
    symbol: str
    side: str
    entry_time_utc: str
    exit_time_utc: str
    entry_price: str
    exit_price: str
    quantity: str
    holding_minutes: str
    exit_reason: str
    gross_pnl_quote: str
    fees_quote_total: str
    net_pnl_quote: str
    entry_notional_quote: str
    exit_notional_quote: str
    executed_quantity: str
    was_tp_exit: str
    was_sl_exit: str
    was_forced_exit: str
    trade_won: str


def export_closed_trades_csv(
    result: BacktestResult,
    symbol: Symbol,
    output_path: str | Path,
) -> Path:
    path = Path(output_path)
    rows = [build_exported_trade_row(trade, symbol) for trade in result.closed_trades]
    if path.parent != Path(""):
        path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=TRADE_EXPORT_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.__dict__)
    return path


def build_exported_trade_row(trade: ClosedTrade, symbol: Symbol) -> ExportedTradeRow:
    holding_minutes = (trade.exit_timestamp - trade.entry_timestamp).total_seconds() / 60
    fees_quote_total = trade.gross_pnl - trade.net_pnl
    normalized_exit_reason = normalize_exit_reason(trade.exit_reason)
    return ExportedTradeRow(
        trade_id=trade.trade_id,
        symbol=f"{symbol.base}{symbol.quote}",
        side="BUY",
        entry_time_utc=format_utc_iso(trade.entry_timestamp),
        exit_time_utc=format_utc_iso(trade.exit_timestamp),
        entry_price=str(trade.entry_price),
        exit_price=str(trade.exit_price),
        quantity=str(trade.quantity),
        holding_minutes=str(Decimal(str(holding_minutes))),
        exit_reason=normalized_exit_reason,
        gross_pnl_quote=str(trade.gross_pnl),
        fees_quote_total=str(fees_quote_total),
        net_pnl_quote=str(trade.net_pnl),
        entry_notional_quote=str(trade.entry_price * trade.quantity),
        exit_notional_quote=str(trade.exit_price * trade.quantity),
        executed_quantity=str(trade.quantity),
        was_tp_exit=str(normalized_exit_reason == "TAKE_PROFIT"),
        was_sl_exit=str(normalized_exit_reason == "STOP_LOSS"),
        was_forced_exit=str(normalized_exit_reason == "END_OF_BACKTEST"),
        trade_won=str(trade.net_pnl > Decimal("0")),
    )


def normalize_exit_reason(exit_reason: str) -> str:
    if exit_reason == "OCO_TP":
        return "TAKE_PROFIT"
    if exit_reason == "OCO_SL":
        return "STOP_LOSS"
    return exit_reason if exit_reason else "OTHER"


def format_utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("trade export timestamps must be UTC-aware")
    return value.astimezone(timezone.utc).isoformat()
