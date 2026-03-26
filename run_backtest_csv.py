import argparse
from decimal import Decimal
from pathlib import Path

from mctp.backtest import BacktestConfig, BacktestEngine
from mctp.backtest.csv_loader import load_binance_spot_kline_csv, parse_cli_datetime
from mctp.backtest.trade_export import export_closed_trades_csv
from mctp.core.enums import Market
from mctp.core.types import Symbol


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MCTP backtest on a local Binance spot kline CSV file")
    parser.add_argument("--csv", required=True, help="Path to Binance spot kline CSV")
    parser.add_argument("--symbol", required=True, help="Symbol such as BTCUSDT")
    parser.add_argument("--start", help="Optional UTC start datetime, for example 2025-01-01 or 2025-01-01T00:00:00Z")
    parser.add_argument("--end", help="Optional UTC end datetime, for example 2025-01-31 or 2025-01-31T23:59:59Z")
    parser.add_argument("--initial-quote", default="10000", help="Initial quote balance, default 10000")
    parser.add_argument("--warmup-bars", type=int, default=21, help="Warmup bars, default 21")
    parser.add_argument("--spread-bps", default="10", help="Bid/ask spread in bps, default 10")
    parser.add_argument("--export-trades", help="Optional path to export completed trades as CSV")
    return parser.parse_args()


def parse_symbol(raw_symbol: str) -> Symbol:
    normalized = raw_symbol.strip().upper()
    quote_candidates = ("USDT", "FDUSD", "USDC", "BUSD", "BTC", "ETH", "BNB", "TRY")
    for quote in quote_candidates:
        if normalized.endswith(quote) and len(normalized) > len(quote):
            return Symbol(normalized[: -len(quote)], quote, Market.SPOT)
    raise ValueError(f"Unsupported symbol format: {raw_symbol}")


def build_instrument_info() -> dict[str, Decimal]:
    return {
        "lot_size": Decimal("0.001"),
        "min_qty": Decimal("0.001"),
        "max_qty": Decimal("1000"),
        "min_notional": Decimal("10"),
    }


def main() -> None:
    args = parse_args()
    symbol = parse_symbol(args.symbol)
    start = parse_cli_datetime(args.start) if args.start else None
    end = parse_cli_datetime(args.end) if args.end else None
    load_result = load_binance_spot_kline_csv(args.csv, start=start, end=end)
    config = BacktestConfig(
        symbol=symbol,
        initial_quote=Decimal(args.initial_quote),
        warmup_bars=args.warmup_bars,
        ema_period=9,
        atr_period=14,
        instrument_info=build_instrument_info(),
        spread_bps=Decimal(args.spread_bps),
    )
    result = BacktestEngine(config).run(load_result.candles)
    print(f"csv_source={load_result.source}")
    print(f"symbol={symbol.base}{symbol.quote}")
    print(f"candles_loaded={len(load_result.candles)}")
    print(f"candles_used={len(load_result.candles)}")
    print(f"start_time={load_result.candles[0].timestamp.isoformat()}")
    print(f"end_time={load_result.candles[-1].timestamp.isoformat()}")
    print(f"start_equity={result.start_equity}")
    print(f"end_equity={result.end_equity}")
    print(f"realized_pnl_total={result.realized_pnl_total}")
    print(f"unrealized_pnl={result.unrealized_pnl}")
    print(f"fee_drag_quote_total={result.fee_drag_quote_total}")
    print(f"execution_count={result.execution_count}")
    print(f"trade_count={result.trade_count}")
    print(f"indicator_source={result.indicator_source}")
    if args.export_trades:
        export_path = export_closed_trades_csv(result, symbol, Path(args.export_trades))
        print(f"trades_export={export_path}")
    if result.analytics is not None:
        print(f"max_drawdown_pct={result.analytics.drawdown.max_drawdown_pct}")
        print(f"profit_factor={result.analytics.profit_factor}")
        print(f"expectancy={result.analytics.expectancy}")
        print(f"sharpe_per_trade={result.analytics.sharpe_per_trade}")
        print(f"sharpe_daily={result.analytics.sharpe_daily}")
        print(f"sortino={result.analytics.sortino}")
        print(f"tp_exit_share={result.analytics.oco.tp_exit_share}")
        print(f"sl_exit_share={result.analytics.oco.sl_exit_share}")


if __name__ == "__main__":
    main()
