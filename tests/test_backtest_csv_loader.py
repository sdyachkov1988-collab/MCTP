from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mctp.backtest.csv_loader import (
    load_binance_spot_kline_csv,
    normalize_binance_timestamp,
    parse_binance_kline_row,
)


def test_parse_binance_csv_row_into_backtest_candle():
    candle = parse_binance_kline_row(
        ["1735689600000", "100.0", "101.0", "99.0", "100.5", "12.3"],
        row_index=1,
    )
    assert candle.timestamp == datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert candle.open == Decimal("100.0")
    assert candle.high == Decimal("101.0")
    assert candle.low == Decimal("99.0")
    assert candle.close == Decimal("100.5")
    assert candle.volume == Decimal("12.3")
    assert candle.closed is True


def test_normalize_microseconds_timestamp_correctly():
    timestamp = normalize_binance_timestamp("1735689600000000", row_index=1)
    assert timestamp == datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)


def test_normalize_milliseconds_timestamp_correctly():
    timestamp = normalize_binance_timestamp("1735689600000", row_index=1)
    assert timestamp == datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)


def test_filter_by_start_end_works(tmp_path):
    csv_path = tmp_path / "btcusdt.csv"
    csv_path.write_text(
        "\n".join(
            [
                "Open time,Open,High,Low,Close,Volume",
                "1735689600000,100,101,99,100,10",
                "1735689660000,101,102,100,101,11",
                "1735689720000,102,103,101,102,12",
            ]
        ),
        encoding="utf-8",
    )
    result = load_binance_spot_kline_csv(
        csv_path,
        start=datetime(2025, 1, 1, 0, 1, tzinfo=timezone.utc),
        end=datetime(2025, 1, 1, 0, 1, tzinfo=timezone.utc),
    )
    assert len(result.candles) == 1
    assert result.candles[0].timestamp == datetime(2025, 1, 1, 0, 1, tzinfo=timezone.utc)


def test_invalid_csv_fails_cleanly(tmp_path):
    csv_path = tmp_path / "invalid.csv"
    csv_path.write_text("Open time,Open,High,Low,Close,Volume\n1735689600000,100,boom,99,100,10\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid High"):
        load_binance_spot_kline_csv(csv_path)


def test_duplicate_timestamps_raise_clear_error(tmp_path):
    csv_path = tmp_path / "duplicate.csv"
    csv_path.write_text(
        "\n".join(
            [
                "Open time,Open,High,Low,Close,Volume",
                "1735689600000,100,101,99,100,10",
                "1735689600000,101,102,100,101,11",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Duplicate candle timestamp"):
        load_binance_spot_kline_csv(csv_path)
