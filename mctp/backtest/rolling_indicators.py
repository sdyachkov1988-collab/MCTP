from collections import deque
from decimal import Decimal

from mctp.indicators.models import Candle, IndicatorSnapshot


class RollingBacktestIndicators:
    def __init__(self, *, ema_period: int, atr_period: int) -> None:
        self._ema_period = ema_period
        self._atr_period = atr_period
        self._ema_seed_closes: deque[Decimal] = deque(maxlen=ema_period)
        self._ema: Decimal | None = None
        self._prev_close: Decimal | None = None
        self._tr_values: deque[Decimal] = deque(maxlen=atr_period)
        self._tr_sum = Decimal("0")
        self._latest_snapshot = IndicatorSnapshot()

    def update(self, candle: Candle) -> IndicatorSnapshot:
        ema = self._update_ema(candle.close)
        atr = self._update_atr(candle)
        self._latest_snapshot = IndicatorSnapshot(ema=ema, atr=atr)
        return self._latest_snapshot

    @property
    def latest_snapshot(self) -> IndicatorSnapshot:
        return self._latest_snapshot

    def _update_ema(self, close: Decimal) -> Decimal | None:
        if self._ema is None:
            self._ema_seed_closes.append(close)
            if len(self._ema_seed_closes) < self._ema_period:
                return None
            self._ema = sum(self._ema_seed_closes, Decimal("0")) / Decimal(self._ema_period)
            return self._ema
        multiplier = Decimal("2") / Decimal(self._ema_period + 1)
        self._ema = ((close - self._ema) * multiplier) + self._ema
        return self._ema

    def _update_atr(self, candle: Candle) -> Decimal | None:
        atr = None
        if self._prev_close is not None:
            tr = max(
                candle.high - candle.low,
                abs(candle.high - self._prev_close),
                abs(candle.low - self._prev_close),
            )
            if len(self._tr_values) == self._atr_period:
                self._tr_sum -= self._tr_values[0]
            self._tr_values.append(tr)
            self._tr_sum += tr
            if len(self._tr_values) == self._atr_period:
                atr = self._tr_sum / Decimal(self._atr_period)
        self._prev_close = candle.close
        return atr
