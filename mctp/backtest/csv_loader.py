import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

from mctp.backtest.market_replay import BacktestCandle


BINANCE_SPOT_KLINE_MIN_COLUMNS = 6


@dataclass(frozen=True)
class CsvLoadResult:
    source: Path
    candles: list[BacktestCandle]


def load_binance_spot_kline_csv(
    csv_path: str | Path,
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    bnb_rate: Optional[Decimal] = None,
) -> CsvLoadResult:
    path = Path(csv_path)
    if not path.exists():
        raise ValueError(f"CSV file does not exist: {path}")
    if not path.is_file():
        raise ValueError(f"CSV path is not a file: {path}")

    normalized_start = normalize_optional_utc_datetime(start)
    normalized_end = normalize_optional_utc_datetime(end)
    if (
        normalized_start is not None
        and normalized_end is not None
        and normalized_start > normalized_end
    ):
        raise ValueError("start must be <= end")

    candles: list[BacktestCandle] = []
    seen_timestamps: set[datetime] = set()

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for row_index, row in enumerate(reader, start=1):
            if not row:
                continue
            if _is_header_row(row):
                continue
            candle = parse_binance_kline_row(
                row,
                row_index=row_index,
                bnb_rate=bnb_rate,
            )
            if candle.timestamp in seen_timestamps:
                raise ValueError(f"Duplicate candle timestamp at row {row_index}: {candle.timestamp.isoformat()}")
            seen_timestamps.add(candle.timestamp)
            if normalized_start is not None and candle.timestamp < normalized_start:
                continue
            if normalized_end is not None and candle.timestamp > normalized_end:
                continue
            candles.append(candle)

    if not candles:
        if normalized_start is not None or normalized_end is not None:
            raise ValueError("CSV load produced no candles for the requested date range")
        raise ValueError("CSV file produced no candles")

    for previous, current in zip(candles, candles[1:]):
        if current.timestamp <= previous.timestamp:
            raise ValueError("CSV candles must be strictly sorted by ascending time")

    return CsvLoadResult(source=path, candles=candles)


def parse_binance_kline_row(
    row: list[str],
    *,
    row_index: int,
    bnb_rate: Optional[Decimal] = None,
) -> BacktestCandle:
    if len(row) < BINANCE_SPOT_KLINE_MIN_COLUMNS:
        raise ValueError(f"CSV row {row_index} has insufficient columns")

    timestamp = normalize_binance_timestamp(row[0], row_index=row_index)
    return BacktestCandle(
        timestamp=timestamp,
        open=_parse_decimal_field(row[1], row_index, "Open"),
        high=_parse_decimal_field(row[2], row_index, "High"),
        low=_parse_decimal_field(row[3], row_index, "Low"),
        close=_parse_decimal_field(row[4], row_index, "Close"),
        volume=_parse_decimal_field(row[5], row_index, "Volume"),
        bnb_rate=bnb_rate,
        closed=True,
    )


def normalize_binance_timestamp(raw_timestamp: str | int, *, row_index: int = 0) -> datetime:
    try:
        timestamp_int = int(str(raw_timestamp).strip())
    except ValueError as exc:
        raise ValueError(f"Invalid Open time at row {row_index}") from exc

    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    if timestamp_int >= 10**15:
        return epoch + timedelta(microseconds=timestamp_int)
    return epoch + timedelta(milliseconds=timestamp_int)


def normalize_optional_utc_datetime(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_cli_datetime(raw_value: str) -> datetime:
    normalized = raw_value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid datetime value: {raw_value}") from exc
    return normalize_optional_utc_datetime(parsed)  # type: ignore[return-value]


def _is_header_row(row: list[str]) -> bool:
    return row[0].strip().lower() == "open time"


def _parse_decimal_field(raw_value: str, row_index: int, field_name: str) -> Decimal:
    try:
        return Decimal(str(raw_value).strip())
    except InvalidOperation as exc:
        raise ValueError(f"Invalid {field_name} at row {row_index}") from exc
