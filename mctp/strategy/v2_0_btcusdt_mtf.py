from decimal import Decimal

from mctp.core.constants import (
    ASSET_BTC,
    ASSET_USDT,
    V20_MTF_BUY_REASON,
    V20_MTF_D1_EMA_BUFFER_PCT,
    V20_MTF_D1_EMA_PERIOD,
    V20_MTF_H1_RSI_ENTRY_MAX,
    V20_MTF_H1_RSI_ENTRY_MIN,
    V20_MTF_H1_RSI_EXIT_MIN,
    V20_MTF_H1_RSI_PERIOD,
    V20_MTF_H4_EMA_FAST_PERIOD,
    V20_MTF_H4_EMA_SLOW_PERIOD,
    V20_MTF_LATE_OVERSTRETCH_D1_DISTANCE_PCT,
    V20_MTF_LATE_OVERSTRETCH_H4_EXHAUST_BLOCK_REASON,
    V20_MTF_LATE_OVERSTRETCH_H4_EXHAUST_SPREAD_MIN_PCT,
    V20_MTF_LATE_OVERSTRETCH_H4_FLAT_BLOCK_REASON,
    V20_MTF_LATE_OVERSTRETCH_H4_FLAT_SPREAD_MAX_PCT,
    V20_MTF_LATE_OVERSTRETCH_H4_WEAK_BLOCK_REASON,
    V20_MTF_LATE_OVERSTRETCH_H4_WEAK_SPREAD_MAX_PCT,
    V20_MTF_LATE_OVERSTRETCH_H4_WEAK_SPREAD_MIN_PCT,
    V20_MTF_MACRO_CONTEXT_MIN_CANDLES,
    V20_MTF_M15_ATR_PERIOD,
    V20_MTF_M15_TRIGGER_MIN_BODY_ATR_RATIO,
    V20_MTF_MID_OVERSTRETCH_H4_CONTINUATION_BLOCK_REASON,
    V20_MTF_MID_OVERSTRETCH_D1_DISTANCE_MAX_PCT,
    V20_MTF_MID_OVERSTRETCH_D1_DISTANCE_MIN_PCT,
    V20_MTF_MID_OVERSTRETCH_H4_WEAK_BLOCK_REASON,
    V20_MTF_SELL_REASON,
)
from mctp.core.enums import IntentType, Market, QuantityMode, Timeframe
from mctp.core.types import Intent
from mctp.indicators import IndicatorEngine
from mctp.indicators.levels import pivot_points
from mctp.indicators.models import IndicatorSnapshot
from mctp.indicators.patterns import detect_weighted_patterns

from .base import StrategyBase
from .models import StrategyInput


class BtcUsdtMtfV20Strategy(StrategyBase):
    requires_mtf_warmup: bool = True

    def __init__(self, indicator_engine: IndicatorEngine | None = None) -> None:
        self._indicator_engine = indicator_engine or IndicatorEngine()
        self.mid_overstretch_h4_continuation_blocked = 0
        self.mid_overstretch_h4_weak_blocked = 0
        self.late_overstretch_h4_flat_blocked = 0
        self.late_overstretch_h4_weak_blocked = 0
        self.late_overstretch_h4_exhaust_blocked = 0

    def on_candle(self, input: StrategyInput) -> Intent:
        symbol = input.snapshot.symbol
        latest_timestamp = self._latest_timestamp(input)
        hold = Intent(type=IntentType.HOLD, symbol=symbol, timestamp=latest_timestamp)
        if symbol.market != Market.SPOT or symbol.base != ASSET_BTC or symbol.quote != ASSET_USDT:
            return hold

        m15 = input.candles.get(Timeframe.M15, [])
        h1 = input.candles.get(Timeframe.H1, [])
        h4 = input.candles.get(Timeframe.H4, [])
        d1 = input.candles.get(Timeframe.D1, [])
        w1 = input.candles.get(Timeframe.W1, [])
        monthly = input.candles.get(Timeframe.MONTHLY, [])
        if len(m15) < 2 or not h1 or not h4:
            return hold

        h4_ema_fast = self._indicator_engine.ema(h4, V20_MTF_H4_EMA_FAST_PERIOD)
        h4_ema_slow = self._indicator_engine.ema(h4, V20_MTF_H4_EMA_SLOW_PERIOD)
        h1_rsi = self._indicator_engine.rsi(h1, V20_MTF_H1_RSI_PERIOD)
        if h4_ema_fast is None or h4_ema_slow is None or h1_rsi is None:
            return hold

        if input.snapshot.is_in_position:
            if h1_rsi > V20_MTF_H1_RSI_EXIT_MIN and h4_ema_fast < h4_ema_slow:
                return Intent(
                    type=IntentType.SELL,
                    symbol=symbol,
                    quantity_mode=QuantityMode.FULL,
                    reason=V20_MTF_SELL_REASON,
                    timestamp=latest_timestamp,
                )
            return hold

        d1_ema = self._indicator_engine.ema(d1, V20_MTF_D1_EMA_PERIOD)
        if d1_ema is None or d1_ema <= Decimal("0"):
            return hold
        if not self._macro_context_allows_long(monthly, w1):
            return hold
        latest_d1 = d1[-1]
        if latest_d1.close <= d1_ema:
            return hold
        if abs(latest_d1.close - d1_ema) / d1_ema <= V20_MTF_D1_EMA_BUFFER_PCT:
            return hold

        latest_h4 = h4[-1]
        latest_h4_pivot = pivot_points(latest_h4).pivot
        if h4_ema_fast <= h4_ema_slow or latest_h4.close <= latest_h4_pivot:
            return hold
        if not (h1_rsi > V20_MTF_H1_RSI_ENTRY_MIN and h1_rsi < V20_MTF_H1_RSI_ENTRY_MAX):
            return hold
        late_overstretch_reason = self._late_overstretch_block_reason(
            latest_d1_close=latest_d1.close,
            d1_ema=d1_ema,
            h4_ema_fast=h4_ema_fast,
            h4_ema_slow=h4_ema_slow,
        )
        if late_overstretch_reason is not None:
            self._increment_late_overstretch_counter(late_overstretch_reason)
            return hold

        m15_atr = self._resolve_m15_atr(input, m15)
        if m15_atr is None or m15_atr <= Decimal("0"):
            return hold

        latest_m15 = m15[-1]
        trigger_body = abs(latest_m15.close - latest_m15.open)
        if trigger_body < (m15_atr * V20_MTF_M15_TRIGGER_MIN_BODY_ATR_RATIO):
            return hold

        latest_patterns = detect_weighted_patterns(m15, Timeframe.M15, engine=self._indicator_engine)
        if not any(signal.name == "bullish_engulfing" for signal in latest_patterns):
            return hold
        return Intent(
            type=IntentType.BUY,
            symbol=symbol,
            quantity_mode=QuantityMode.FULL,
            reason=V20_MTF_BUY_REASON,
            timestamp=latest_timestamp,
        )

    @staticmethod
    def _latest_timestamp(input: StrategyInput):
        m15 = input.candles.get(Timeframe.M15, [])
        if m15:
            return m15[-1].timestamp
        return input.snapshot.timestamp

    def _resolve_m15_atr(self, input: StrategyInput, m15: list) -> Decimal | None:
        snapshot = input.indicators.get("snapshot")
        if isinstance(snapshot, IndicatorSnapshot) and snapshot.atr is not None:
            return snapshot.atr
        return self._indicator_engine.atr(m15, V20_MTF_M15_ATR_PERIOD)

    def late_overstretch_block_counters(self) -> dict[str, int]:
        return {
            V20_MTF_MID_OVERSTRETCH_H4_CONTINUATION_BLOCK_REASON: self.mid_overstretch_h4_continuation_blocked,
            V20_MTF_MID_OVERSTRETCH_H4_WEAK_BLOCK_REASON: self.mid_overstretch_h4_weak_blocked,
            V20_MTF_LATE_OVERSTRETCH_H4_FLAT_BLOCK_REASON: self.late_overstretch_h4_flat_blocked,
            V20_MTF_LATE_OVERSTRETCH_H4_WEAK_BLOCK_REASON: self.late_overstretch_h4_weak_blocked,
            V20_MTF_LATE_OVERSTRETCH_H4_EXHAUST_BLOCK_REASON: self.late_overstretch_h4_exhaust_blocked,
        }

    def _increment_late_overstretch_counter(self, reason: str) -> None:
        if reason == V20_MTF_MID_OVERSTRETCH_H4_CONTINUATION_BLOCK_REASON:
            self.mid_overstretch_h4_continuation_blocked += 1
        elif reason == V20_MTF_MID_OVERSTRETCH_H4_WEAK_BLOCK_REASON:
            self.mid_overstretch_h4_weak_blocked += 1
        elif reason == V20_MTF_LATE_OVERSTRETCH_H4_FLAT_BLOCK_REASON:
            self.late_overstretch_h4_flat_blocked += 1
        elif reason == V20_MTF_LATE_OVERSTRETCH_H4_WEAK_BLOCK_REASON:
            self.late_overstretch_h4_weak_blocked += 1
        elif reason == V20_MTF_LATE_OVERSTRETCH_H4_EXHAUST_BLOCK_REASON:
            self.late_overstretch_h4_exhaust_blocked += 1

    @staticmethod
    def _late_overstretch_block_reason(
        *,
        latest_d1_close: Decimal,
        d1_ema: Decimal,
        h4_ema_fast: Decimal,
        h4_ema_slow: Decimal,
    ) -> str | None:
        if d1_ema <= Decimal("0") or h4_ema_slow <= Decimal("0"):
            return None
        d1_distance_pct = ((latest_d1_close - d1_ema) / d1_ema) * Decimal("100")
        if (
            d1_distance_pct >= V20_MTF_MID_OVERSTRETCH_D1_DISTANCE_MIN_PCT
            and d1_distance_pct < V20_MTF_MID_OVERSTRETCH_D1_DISTANCE_MAX_PCT
        ):
            h4_spread_pct = ((h4_ema_fast - h4_ema_slow) / h4_ema_slow) * Decimal("100")
            if (
                h4_spread_pct >= V20_MTF_LATE_OVERSTRETCH_H4_WEAK_SPREAD_MIN_PCT
                and h4_spread_pct < V20_MTF_LATE_OVERSTRETCH_H4_WEAK_SPREAD_MAX_PCT
            ):
                return V20_MTF_MID_OVERSTRETCH_H4_WEAK_BLOCK_REASON
            if (
                h4_spread_pct >= V20_MTF_LATE_OVERSTRETCH_H4_WEAK_SPREAD_MAX_PCT
                and h4_spread_pct < V20_MTF_LATE_OVERSTRETCH_H4_EXHAUST_SPREAD_MIN_PCT
            ):
                return V20_MTF_MID_OVERSTRETCH_H4_CONTINUATION_BLOCK_REASON
            return None
        if d1_distance_pct < V20_MTF_LATE_OVERSTRETCH_D1_DISTANCE_PCT:
            return None
        h4_spread_pct = ((h4_ema_fast - h4_ema_slow) / h4_ema_slow) * Decimal("100")
        if h4_spread_pct < V20_MTF_LATE_OVERSTRETCH_H4_FLAT_SPREAD_MAX_PCT:
            return V20_MTF_LATE_OVERSTRETCH_H4_FLAT_BLOCK_REASON
        if (
            h4_spread_pct >= V20_MTF_LATE_OVERSTRETCH_H4_WEAK_SPREAD_MIN_PCT
            and h4_spread_pct < V20_MTF_LATE_OVERSTRETCH_H4_WEAK_SPREAD_MAX_PCT
        ):
            return V20_MTF_LATE_OVERSTRETCH_H4_WEAK_BLOCK_REASON
        if h4_spread_pct >= V20_MTF_LATE_OVERSTRETCH_H4_EXHAUST_SPREAD_MIN_PCT:
            return V20_MTF_LATE_OVERSTRETCH_H4_EXHAUST_BLOCK_REASON
        return None

    @staticmethod
    def _macro_context_allows_long(monthly: list, weekly: list) -> bool:
        if len(monthly) < V20_MTF_MACRO_CONTEXT_MIN_CANDLES or len(weekly) < V20_MTF_MACRO_CONTEXT_MIN_CANDLES:
            return False
        latest_monthly = monthly[-1]
        previous_monthly = monthly[-2]
        latest_weekly = weekly[-1]
        previous_weekly = weekly[-2]
        monthly_bullish = (
            latest_monthly.close > latest_monthly.open
            and latest_monthly.close >= previous_monthly.close
        )
        weekly_bullish = (
            latest_weekly.close > latest_weekly.open
            and latest_weekly.close >= previous_weekly.close
        )
        return monthly_bullish and weekly_bullish
