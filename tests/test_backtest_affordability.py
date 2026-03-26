from datetime import datetime, timedelta, timezone
from decimal import Decimal

from mctp.backtest.config import BacktestConfig
from mctp.backtest.engine import BacktestEngine
from mctp.backtest.market_replay import BacktestCandle
from mctp.core.enums import CommissionAsset, Market, OrderType
from mctp.core.types import Symbol


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
START = datetime(2026, 3, 22, 0, 0, 0, tzinfo=timezone.utc)


def _make_candle(
    index: int,
    close: Decimal,
    *,
    high: Decimal | None = None,
    low: Decimal | None = None,
) -> BacktestCandle:
    return BacktestCandle(
        timestamp=START + timedelta(minutes=index),
        open=close,
        high=high if high is not None else close + Decimal("0.01"),
        low=low if low is not None else close - Decimal("0.01"),
        close=close,
        volume=Decimal("10"),
    )


def _oversized_buy_sequence() -> list[BacktestCandle]:
    return [
        _make_candle(0, Decimal("100.00")),
        _make_candle(1, Decimal("99.99")),
        _make_candle(2, Decimal("99.98")),
        _make_candle(3, Decimal("99.97")),
        _make_candle(4, Decimal("99.96")),
        _make_candle(5, Decimal("100.05")),
        _make_candle(6, Decimal("100.06"), high=Decimal("100.07"), low=Decimal("100.05")),
    ]


def _tp_sequence_after_buy() -> list[BacktestCandle]:
    candles = _oversized_buy_sequence()
    candles[-1] = _make_candle(6, Decimal("100.18"), high=Decimal("100.22"), low=Decimal("100.16"))
    return candles


def _config(
    *,
    initial_quote: Decimal = Decimal("10000"),
    commission_asset: CommissionAsset = CommissionAsset.QUOTE,
    instrument_info: dict | None = None,
) -> BacktestConfig:
    return BacktestConfig(
        symbol=BTCUSDT,
        initial_quote=initial_quote,
        warmup_bars=5,
        ema_period=3,
        atr_period=3,
        instrument_info=instrument_info
        or {
            "lot_size": Decimal("0.001"),
            "min_qty": Decimal("0.001"),
            "max_qty": Decimal("1000"),
            "min_notional": Decimal("10"),
        },
        commission_asset=commission_asset,
        entry_order_type=OrderType.MARKET,
    )


def test_buy_size_is_capped_by_available_quote():
    result = BacktestEngine(_config()).run(_oversized_buy_sequence())
    entry = result.executions[0]
    max_affordable = Decimal("10000") / (entry.fill_price * Decimal("1.001"))
    expected_quantity = (max_affordable // Decimal("0.001")) * Decimal("0.001")
    assert entry.quantity == expected_quantity


def test_fee_aware_affordability_caps_quote_fee_buy():
    result = BacktestEngine(_config(commission_asset=CommissionAsset.QUOTE)).run(_oversized_buy_sequence())
    entry = result.executions[0]
    total_quote_outflow = entry.quantity * entry.fill_price + result.accounting_history[0].fee_drag_quote
    assert total_quote_outflow <= Decimal("10000")


def test_quantization_after_affordability_cap_can_reject_too_small_order():
    config = _config(
        initial_quote=Decimal("10"),
        instrument_info={
            "lot_size": Decimal("0.01"),
            "min_qty": Decimal("0.01"),
            "max_qty": Decimal("1000"),
            "min_notional": Decimal("10"),
        },
    )
    candles = [
        _make_candle(0, Decimal("1000.00")),
        _make_candle(1, Decimal("999.99")),
        _make_candle(2, Decimal("999.98")),
        _make_candle(3, Decimal("999.97")),
        _make_candle(4, Decimal("999.96")),
        _make_candle(5, Decimal("1000.05")),
        _make_candle(6, Decimal("1000.06")),
    ]
    result = BacktestEngine(config).run(candles)
    assert result.execution_count == 0
    assert result.final_snapshot.held_qty == Decimal("0")


def test_after_buy_fill_free_quote_does_not_go_negative():
    result = BacktestEngine(_config()).run(_oversized_buy_sequence())
    assert result.final_snapshot.held_qty > Decimal("0")
    assert result.final_snapshot.free_quote >= Decimal("0")


def test_existing_sell_path_is_unchanged_after_affordability_cap():
    result = BacktestEngine(_config()).run(_tp_sequence_after_buy())
    assert [execution.reason for execution in result.executions] == ["MARKET_ENTRY", "OCO_TP"]
    assert result.executions[1].quantity == result.executions[0].quantity
