import csv
from datetime import datetime, timezone
from decimal import Decimal

from mctp.backtest.results import (
    BacktestResult,
    ClosedTrade,
    EquityCurvePoint,
)
from mctp.backtest.trade_export import TRADE_EXPORT_HEADERS, build_exported_trade_row, export_closed_trades_csv
from mctp.core.enums import Market
from mctp.core.types import PortfolioSnapshot, Symbol
from mctp.portfolio.equity import EquitySnapshot


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
START = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone.utc)


def _result_with_trades(closed_trades: list[ClosedTrade]) -> BacktestResult:
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
    equity_snapshot = EquitySnapshot(
        timestamp=START,
        total_equity=Decimal("10000"),
        free_quote=Decimal("10000"),
        held_qty=Decimal("0"),
        held_value=Decimal("0"),
        current_price=Decimal("100"),
        is_in_position=False,
        meaningful_position=False,
    )
    return BacktestResult(
        start_equity=Decimal("10000"),
        end_equity=Decimal("10010"),
        realized_pnl_total=Decimal("10"),
        unrealized_pnl=Decimal("0"),
        fee_drag_quote_total=Decimal("1"),
        execution_count=2,
        trade_count=len(closed_trades),
        cancelled_order_count=0,
        warmup_bars=5,
        final_snapshot=snapshot,
        final_equity_snapshot=equity_snapshot,
        equity_curve=[EquityCurvePoint(timestamp=START, equity=Decimal("10000"), point_type="HOLD")],
        closed_trades=closed_trades,
    )


def test_completed_trades_are_exported(tmp_path):
    trade = ClosedTrade(
        trade_id="bt-trade-1",
        entry_timestamp=START,
        exit_timestamp=START.replace(hour=1),
        quantity=Decimal("0.5"),
        entry_price=Decimal("100"),
        exit_price=Decimal("105"),
        gross_pnl=Decimal("2.7"),
        net_pnl=Decimal("2.5"),
        return_pct=Decimal("0.05"),
        exit_reason="OCO_TP",
    )
    result = _result_with_trades([trade])
    output_path = tmp_path / "trades.csv"
    export_closed_trades_csv(result, BTCUSDT, output_path)
    rows = list(csv.DictReader(output_path.open("r", encoding="utf-8")))
    assert len(rows) == 1
    assert rows[0]["trade_id"] == "bt-trade-1"
    assert rows[0]["symbol"] == "BTCUSDT"


def test_trade_export_headers_are_stable_and_readable(tmp_path):
    result = _result_with_trades([])
    output_path = tmp_path / "empty_trades.csv"
    export_closed_trades_csv(result, BTCUSDT, output_path)
    with output_path.open("r", encoding="utf-8") as handle:
        header = handle.readline().strip().split(",")
    assert tuple(header) == TRADE_EXPORT_HEADERS


def test_trade_export_timestamps_are_utc_iso():
    trade = ClosedTrade(
        trade_id="bt-trade-1",
        entry_timestamp=START,
        exit_timestamp=START.replace(hour=2),
        quantity=Decimal("1"),
        entry_price=Decimal("100"),
        exit_price=Decimal("99"),
        gross_pnl=Decimal("-0.8"),
        net_pnl=Decimal("-1"),
        return_pct=Decimal("-0.01"),
        exit_reason="OCO_SL",
    )
    row = build_exported_trade_row(trade, BTCUSDT)
    assert row.entry_time_utc == "2026-03-24T00:00:00+00:00"
    assert row.exit_time_utc == "2026-03-24T02:00:00+00:00"


def test_empty_trade_set_is_handled_cleanly(tmp_path):
    result = _result_with_trades([])
    output_path = tmp_path / "trades.csv"
    export_closed_trades_csv(result, BTCUSDT, output_path)
    rows = list(csv.DictReader(output_path.open("r", encoding="utf-8")))
    assert rows == []


def test_tp_sl_classification_is_exported_correctly():
    tp_trade = ClosedTrade(
        trade_id="tp-1",
        entry_timestamp=START,
        exit_timestamp=START.replace(hour=1),
        quantity=Decimal("1"),
        entry_price=Decimal("100"),
        exit_price=Decimal("110"),
        gross_pnl=Decimal("10.2"),
        net_pnl=Decimal("10"),
        return_pct=Decimal("0.1"),
        exit_reason="OCO_TP",
    )
    sl_trade = ClosedTrade(
        trade_id="sl-1",
        entry_timestamp=START,
        exit_timestamp=START.replace(hour=1),
        quantity=Decimal("1"),
        entry_price=Decimal("100"),
        exit_price=Decimal("95"),
        gross_pnl=Decimal("-4.8"),
        net_pnl=Decimal("-5"),
        return_pct=Decimal("-0.05"),
        exit_reason="OCO_SL",
    )
    tp_row = build_exported_trade_row(tp_trade, BTCUSDT)
    sl_row = build_exported_trade_row(sl_trade, BTCUSDT)
    assert tp_row.exit_reason == "TAKE_PROFIT"
    assert tp_row.was_tp_exit == "True"
    assert tp_row.was_sl_exit == "False"
    assert sl_row.exit_reason == "STOP_LOSS"
    assert sl_row.was_tp_exit == "False"
    assert sl_row.was_sl_exit == "True"
