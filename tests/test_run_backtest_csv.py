from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

import run_backtest_csv
from mctp.core.constants import STRATEGY_ID_LEGACY_EMA_CROSS, STRATEGY_ID_V20_BTCUSDT_MTF
from mctp.core.enums import CommissionAsset, Side
from mctp.core.types import Symbol


UTC_NOW = datetime(2026, 3, 28, 0, 0, 0, tzinfo=timezone.utc)


def test_parse_args_default_strategy_is_legacy():
    args = run_backtest_csv.parse_args(["--csv", "sample.csv", "--symbol", "BTCUSDT"])
    assert args.strategy == STRATEGY_ID_LEGACY_EMA_CROSS


def test_parse_args_supports_explicit_v20_strategy():
    args = run_backtest_csv.parse_args(
        [
            "--csv",
            "sample.csv",
            "--symbol",
            "BTCUSDT",
            "--strategy",
            STRATEGY_ID_V20_BTCUSDT_MTF,
        ]
    )
    assert args.strategy == STRATEGY_ID_V20_BTCUSDT_MTF


def test_parse_args_rejects_unknown_strategy_with_clear_error(capsys):
    with pytest.raises(SystemExit) as exc_info:
        run_backtest_csv.parse_args(
            [
                "--csv",
                "sample.csv",
                "--symbol",
                "BTCUSDT",
                "--strategy",
                "unknown_strategy",
            ]
        )
    assert exc_info.value.code == 2
    assert "invalid choice" in capsys.readouterr().err


def test_main_uses_legacy_strategy_by_default(monkeypatch, capsys):
    _install_main_stubs(monkeypatch)
    run_backtest_csv.main()
    captured = capsys.readouterr().out
    assert "strategy_id=legacy_ema_cross" in captured


def test_main_supports_explicit_v20_strategy(monkeypatch, capsys):
    _install_main_stubs(monkeypatch, strategy_id=STRATEGY_ID_V20_BTCUSDT_MTF)
    run_backtest_csv.main()
    captured = capsys.readouterr().out
    assert "strategy_id=v20_btcusdt_mtf" in captured


def test_main_prints_backtest_progress_to_stderr(monkeypatch, capsys):
    _install_main_stubs(monkeypatch, emit_progress=True)
    run_backtest_csv.main()
    captured = capsys.readouterr()
    assert "backtest_progress" in captured.err
    assert "processed=10/100" in captured.err
    assert "execution_count=1" in captured.err
    assert "trade_count=1" in captured.err


def test_backtest_progress_is_emitted_during_warmup_heavy_v20_path():
    from mctp.backtest import BacktestCandle, BacktestConfig, BacktestEngine

    symbol = Symbol("BTC", "USDT", run_backtest_csv.Market.SPOT)
    candles = []
    for index in range(19205):
        candles.append(
            BacktestCandle(
                timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=15 * index),
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100"),
                volume=Decimal("1"),
                bnb_rate=Decimal("300"),
            )
        )
    progress_events = []
    engine = BacktestEngine(
        BacktestConfig(
            symbol=symbol,
            initial_quote=Decimal("10000"),
            warmup_bars=5,
            ema_period=3,
            atr_period=14,
            instrument_info=run_backtest_csv.build_instrument_info(),
            strategy_id=STRATEGY_ID_V20_BTCUSDT_MTF,
        )
    )

    engine.run(candles, progress_callback=progress_events.append)

    assert progress_events
    assert progress_events[0].processed_candles <= 1920
    assert progress_events[0].percent_complete >= 9
    assert progress_events[0].processed_candles < 19200


def _install_main_stubs(
    monkeypatch,
    *,
    strategy_id: str = STRATEGY_ID_LEGACY_EMA_CROSS,
    emit_progress: bool = False,
) -> None:
    symbol = Symbol("BTC", "USDT", run_backtest_csv.Market.SPOT)
    candle_count = 100 if emit_progress else 2
    candles = [SimpleNamespace(timestamp=UTC_NOW) for _ in range(candle_count)]
    load_result = SimpleNamespace(source="sample.csv", candles=candles)

    monkeypatch.setattr(
        run_backtest_csv,
        "parse_args",
        lambda argv=None: SimpleNamespace(
            csv="sample.csv",
            symbol="BTCUSDT",
            start=None,
            end=None,
            initial_quote="10000",
            warmup_bars=21,
            spread_bps="10",
            strategy=strategy_id,
            export_trades=None,
        ),
    )
    monkeypatch.setattr(run_backtest_csv, "parse_symbol", lambda raw_symbol: symbol)
    monkeypatch.setattr(run_backtest_csv, "load_binance_spot_kline_csv", lambda *args, **kwargs: load_result)

    class FakeEngine:
        def __init__(self, config):
            self._config = config

        def run(self, candles, *, progress_callback=None):
            assert candles == load_result.candles
            if emit_progress and progress_callback is not None:
                progress_callback(
                    SimpleNamespace(
                        processed_candles=10,
                        total_candles=100,
                        percent_complete=10,
                        candle_timestamp=UTC_NOW,
                        execution_count=1,
                        trade_count=1,
                    )
                )
            return SimpleNamespace(
                start_equity=Decimal("10000"),
                end_equity=Decimal("10010"),
                realized_pnl_total=Decimal("10"),
                unrealized_pnl=Decimal("0"),
                fee_drag_quote_total=Decimal("0"),
                execution_count=1,
                trade_count=1,
                indicator_source="indicator_engine",
                analytics=SimpleNamespace(
                    drawdown=SimpleNamespace(max_drawdown_pct=Decimal("0")),
                    profit_factor=Decimal("1"),
                    expectancy=Decimal("1"),
                    sharpe_per_trade=Decimal("1"),
                    sharpe_daily=Decimal("1"),
                    sortino=Decimal("1"),
                    oco=SimpleNamespace(tp_exit_share=Decimal("1"), sl_exit_share=Decimal("0")),
                ),
            )

    monkeypatch.setattr(run_backtest_csv, "BacktestEngine", FakeEngine)

    original_config = run_backtest_csv.BacktestConfig

    def checking_config(*args, **kwargs):
        assert kwargs["strategy_id"] == strategy_id
        return original_config(*args, **kwargs)

    monkeypatch.setattr(run_backtest_csv, "BacktestConfig", checking_config)
