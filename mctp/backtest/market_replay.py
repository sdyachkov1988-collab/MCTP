from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional


@dataclass(frozen=True)
class BacktestCandle:
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Decimal("0")
    bnb_rate: Optional[Decimal] = None
    closed: bool = True

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None:
            raise ValueError("BacktestCandle.timestamp must be UTC-aware")
        for name, value in [
            ("open", self.open),
            ("high", self.high),
            ("low", self.low),
            ("close", self.close),
            ("volume", self.volume),
        ]:
            if not isinstance(value, Decimal):
                raise AssertionError(f"BacktestCandle.{name} must be Decimal")
        if self.bnb_rate is not None and not isinstance(self.bnb_rate, Decimal):
            raise AssertionError("BacktestCandle.bnb_rate must be Decimal or None")
        if not self.closed:
            raise ValueError("BacktestCandle must represent a closed candle")


@dataclass(frozen=True)
class ReplayQuote:
    bid: Decimal
    ask: Decimal
    mid: Decimal


class MarketReplay:
    def __init__(self, spread_bps: Decimal) -> None:
        if not isinstance(spread_bps, Decimal):
            raise AssertionError("spread_bps must be Decimal")
        self._spread_bps = spread_bps

    def quote_for_candle(self, candle: BacktestCandle) -> ReplayQuote:
        half_spread = candle.close * self._spread_bps / Decimal("20000")
        return ReplayQuote(
            bid=candle.close - half_spread,
            ask=candle.close + half_spread,
            mid=candle.close,
        )

    @staticmethod
    def limit_buy_hit(candle: BacktestCandle, limit_price: Decimal) -> bool:
        return candle.low <= limit_price

    @staticmethod
    def tp_hit(candle: BacktestCandle, tp_price: Decimal) -> bool:
        return candle.high >= tp_price

    @staticmethod
    def sl_hit(candle: BacktestCandle, sl_stop_price: Decimal) -> bool:
        return candle.low <= sl_stop_price
