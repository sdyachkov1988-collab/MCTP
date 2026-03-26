from decimal import Decimal

from mctp.core.enums import IntentType, QuantityMode
from mctp.core.types import Intent
from mctp.strategy import StrategyBase, StrategyInput


class EmaCrossSmokeStrategy(StrategyBase):
    def on_candle(self, input: StrategyInput) -> Intent:
        ema_fast = input.indicators.get("ema_9")
        ema_slow = input.indicators.get("ema_21")
        if ema_fast is None or ema_slow is None:
            return Intent(type=IntentType.HOLD, symbol=input.snapshot.symbol, timestamp=input.snapshot.timestamp)
        if ema_fast > ema_slow:
            return Intent(
                type=IntentType.BUY,
                symbol=input.snapshot.symbol,
                quantity_mode=QuantityMode.FULL,
                reason="ema9_above_ema21",
                timestamp=input.snapshot.timestamp,
            )
        if ema_fast < ema_slow:
            return Intent(
                type=IntentType.SELL,
                symbol=input.snapshot.symbol,
                quantity_mode=QuantityMode.FULL,
                reason="ema9_below_ema21",
                timestamp=input.snapshot.timestamp,
            )
        return Intent(type=IntentType.HOLD, symbol=input.snapshot.symbol, timestamp=input.snapshot.timestamp)
