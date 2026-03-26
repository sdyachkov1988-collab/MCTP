from datetime import datetime, timedelta, timezone
from decimal import Decimal

from mctp.backtest import BacktestCandle, BacktestConfig, BacktestEngine
from mctp.core.enums import CommissionAsset, Market, OrderType
from mctp.core.types import Symbol


def build_demo_candles() -> list[BacktestCandle]:
    symbol_start = datetime(2026, 3, 22, 0, 0, 0, tzinfo=timezone.utc)
    raw_candles = [
        ("100", "102", "98"),
        ("99", "101", "97"),
        ("98", "100", "96"),
        ("97", "99", "95"),
        ("96", "98", "94"),
        ("110", "112", "108"),
        ("128", "130", "126"),
        ("105", "107", "103"),
        ("95", "97", "93"),
        ("112", "114", "110"),
        ("132", "134", "130"),
        ("108", "110", "106"),
        ("92", "94", "90"),
        ("111", "113", "109"),
        ("89", "91", "87"),
    ]
    return [
        BacktestCandle(
            timestamp=symbol_start + timedelta(minutes=index),
            open=Decimal(close),
            high=Decimal(high),
            low=Decimal(low),
            close=Decimal(close),
            bnb_rate=Decimal("300"),
        )
        for index, (close, high, low) in enumerate(raw_candles)
    ]


def run_demo_backtest():
    symbol = Symbol("BTC", "USDT", Market.SPOT)
    config = BacktestConfig(
        symbol=symbol,
        initial_quote=Decimal("10000"),
        warmup_bars=5,
        ema_period=3,
        atr_period=3,
        instrument_info={
            "lot_size": Decimal("0.001"),
            "min_qty": Decimal("0.001"),
            "max_qty": Decimal("1000"),
            "min_notional": Decimal("10"),
        },
        spread_bps=Decimal("10"),
        commission_asset=CommissionAsset.BNB,
        entry_order_type=OrderType.MARKET,
    )
    return BacktestEngine(config).run(build_demo_candles())


def main() -> None:
    result = run_demo_backtest()
    print(f"start_equity={result.start_equity}")
    print(f"end_equity={result.end_equity}")
    print(f"realized_pnl_total={result.realized_pnl_total}")
    print(f"fee_drag_quote_total={result.fee_drag_quote_total}")
    print(f"execution_count={result.execution_count}")
    print(f"trade_count={result.trade_count}")
    print(f"closed_trade_count={len(result.closed_trades)}")
    print(f"indicator_source={result.indicator_source}")
    print(f"latest_ema={result.latest_ema}")
    print(f"latest_atr={result.latest_atr}")
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
