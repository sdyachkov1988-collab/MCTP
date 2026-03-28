from decimal import Decimal
from typing import Iterable, Sequence

from mctp.core.constants import (
    CCI_SCALING_CONSTANT,
    PATTERN_WEIGHT_D1,
    PATTERN_WEIGHT_H1,
    PATTERN_WEIGHT_H4,
    PATTERN_WEIGHT_M15,
    PATTERN_WEIGHT_M30,
    PATTERN_WEIGHT_M5,
    PATTERN_WEIGHT_MONTHLY,
    PATTERN_WEIGHT_W1,
)
from mctp.core.enums import Timeframe

from .models import Candle, IndicatorSnapshot


_TIMEFRAME_WEIGHTS: dict[Timeframe, Decimal] = {
    Timeframe.M5: PATTERN_WEIGHT_M5,
    Timeframe.M15: PATTERN_WEIGHT_M15,
    Timeframe.M30: PATTERN_WEIGHT_M30,
    Timeframe.H1: PATTERN_WEIGHT_H1,
    Timeframe.H4: PATTERN_WEIGHT_H4,
    Timeframe.D1: PATTERN_WEIGHT_D1,
    Timeframe.W1: PATTERN_WEIGHT_W1,
    Timeframe.MONTHLY: PATTERN_WEIGHT_MONTHLY,
}


class IndicatorEngine:
    def ema(self, candles: Sequence[Candle], period: int) -> Decimal | None:
        if len(candles) < period:
            return None
        multiplier = Decimal("2") / Decimal(period + 1)
        ema_value = sum((candle.close for candle in candles[:period]), Decimal("0")) / Decimal(period)
        for candle in candles[period:]:
            ema_value = ((candle.close - ema_value) * multiplier) + ema_value
        return ema_value

    def sma(self, candles: Sequence[Candle], period: int) -> Decimal | None:
        if len(candles) < period:
            return None
        window = candles[-period:]
        return sum((c.close for c in window), Decimal("0")) / Decimal(period)

    def wma(self, values: Sequence[Decimal], period: int) -> Decimal | None:
        if len(values) < period:
            return None
        weights = list(range(1, period + 1))
        total_weight = Decimal(sum(weights))
        weighted = sum((value * Decimal(weight) for value, weight in zip(values[-period:], weights)), Decimal("0"))
        return weighted / total_weight

    def hull_ma(self, candles: Sequence[Candle], period: int) -> Decimal | None:
        if len(candles) < period:
            return None
        half_period = max(1, period // 2)
        sqrt_period = max(1, int(Decimal(period).sqrt()))
        closes = [c.close for c in candles]
        series: list[Decimal] = []
        for index in range(period - 1, len(closes)):
            slice_values = closes[: index + 1]
            wma_half = self.wma(slice_values, half_period)
            wma_full = self.wma(slice_values, period)
            if wma_half is None or wma_full is None:
                continue
            series.append((Decimal("2") * wma_half) - wma_full)
        return self.wma(series, sqrt_period)

    def rsi(self, candles: Sequence[Candle], period: int) -> Decimal | None:
        if len(candles) < period + 1:
            return None
        gains = Decimal("0")
        losses = Decimal("0")
        for previous, current in zip(candles[-(period + 1) : -1], candles[-period:]):
            change = current.close - previous.close
            if change > Decimal("0"):
                gains += change
            else:
                losses += abs(change)
        avg_gain = gains / Decimal(period)
        avg_loss = losses / Decimal(period)
        if avg_loss == Decimal("0"):
            return Decimal("100")
        rs = avg_gain / avg_loss
        return Decimal("100") - (Decimal("100") / (Decimal("1") + rs))

    def stochastic(self, candles: Sequence[Candle], period: int, smooth_k: int = 1) -> tuple[Decimal | None, Decimal | None]:
        if len(candles) < period:
            return None, None
        raw_values: list[Decimal] = []
        for index in range(period - 1, len(candles)):
            window = candles[index - period + 1 : index + 1]
            highest_high = max(c.high for c in window)
            lowest_low = min(c.low for c in window)
            if highest_high == lowest_low:
                raw_values.append(Decimal("0"))
            else:
                raw_values.append(((window[-1].close - lowest_low) / (highest_high - lowest_low)) * Decimal("100"))
        if len(raw_values) < smooth_k:
            return None, None
        k_value = sum(raw_values[-smooth_k:], Decimal("0")) / Decimal(smooth_k)
        return k_value, k_value

    def cci(self, candles: Sequence[Candle], period: int) -> Decimal | None:
        if len(candles) < period:
            return None
        window = candles[-period:]
        typical_prices = [(c.high + c.low + c.close) / Decimal("3") for c in window]
        sma_tp = sum(typical_prices, Decimal("0")) / Decimal(period)
        mean_deviation = sum((abs(tp - sma_tp) for tp in typical_prices), Decimal("0")) / Decimal(period)
        if mean_deviation == Decimal("0"):
            return Decimal("0")
        return (typical_prices[-1] - sma_tp) / (CCI_SCALING_CONSTANT * mean_deviation)

    def atr(self, candles: Sequence[Candle], period: int) -> Decimal | None:
        if len(candles) < period + 1:
            return None
        tr_values: list[Decimal] = []
        for previous, current in zip(candles[-(period + 1) : -1], candles[-period:]):
            tr_values.append(
                max(
                    current.high - current.low,
                    abs(current.high - previous.close),
                    abs(current.low - previous.close),
                )
            )
        return sum(tr_values, Decimal("0")) / Decimal(period)

    def bollinger_bands(self, candles: Sequence[Candle], period: int, stddev_mult: Decimal) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
        sma_value = self.sma(candles, period)
        if sma_value is None:
            return None, None, None
        closes = [c.close for c in candles[-period:]]
        variance = sum(((close - sma_value) ** 2 for close in closes), Decimal("0")) / Decimal(period)
        stddev = variance.sqrt()
        offset = stddev * stddev_mult
        return sma_value, sma_value + offset, sma_value - offset

    def keltner_channels(self, candles: Sequence[Candle], period: int, atr_mult: Decimal) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
        mid = self.ema(candles, period)
        atr_value = self.atr(candles, period)
        if mid is None or atr_value is None:
            return None, None, None
        offset = atr_value * atr_mult
        return mid, mid + offset, mid - offset

    def obv(self, candles: Sequence[Candle]) -> Decimal | None:
        if not candles:
            return None
        total = Decimal("0")
        previous_close = candles[0].close
        for candle in candles[1:]:
            if candle.close > previous_close:
                total += candle.volume
            elif candle.close < previous_close:
                total -= candle.volume
            previous_close = candle.close
        return total

    def vwap(self, candles: Sequence[Candle]) -> Decimal | None:
        total_volume = sum((c.volume for c in candles), Decimal("0"))
        if total_volume == Decimal("0"):
            return None
        total_value = sum((((c.high + c.low + c.close) / Decimal("3")) * c.volume for c in candles), Decimal("0"))
        return total_value / total_volume

    def cmf(self, candles: Sequence[Candle], period: int) -> Decimal | None:
        if len(candles) < period:
            return None
        window = candles[-period:]
        total_volume = sum((c.volume for c in window), Decimal("0"))
        if total_volume == Decimal("0"):
            return None
        money_flow = Decimal("0")
        for candle in window:
            if candle.high == candle.low:
                continue
            multiplier = ((candle.close - candle.low) - (candle.high - candle.close)) / (candle.high - candle.low)
            money_flow += multiplier * candle.volume
        return money_flow / total_volume

    def snapshot(self, candles: Sequence[Candle], ema_period: int, atr_period: int) -> IndicatorSnapshot:
        stochastic_k, stochastic_d = self.stochastic(candles, atr_period)
        boll_mid, boll_upper, boll_lower = self.bollinger_bands(candles, ema_period, Decimal("2"))
        kelt_mid, kelt_upper, kelt_lower = self.keltner_channels(candles, atr_period, Decimal("2"))
        return IndicatorSnapshot(
            ema=self.ema(candles, ema_period),
            sma=self.sma(candles, ema_period),
            hull_ma=self.hull_ma(candles, ema_period),
            rsi=self.rsi(candles, ema_period),
            stochastic_k=stochastic_k,
            stochastic_d=stochastic_d,
            cci=self.cci(candles, ema_period),
            atr=self.atr(candles, atr_period),
            bollinger_mid=boll_mid,
            bollinger_upper=boll_upper,
            bollinger_lower=boll_lower,
            keltner_mid=kelt_mid,
            keltner_upper=kelt_upper,
            keltner_lower=kelt_lower,
            obv=self.obv(candles),
            vwap=self.vwap(candles),
            cmf=self.cmf(candles, atr_period),
        )

    @staticmethod
    def timeframe_weight(timeframe: Timeframe) -> Decimal:
        return _TIMEFRAME_WEIGHTS[timeframe]
