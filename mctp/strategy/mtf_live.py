"""
Incremental live MTF aggregation adapter.

Receives individual closed candles from live WS streams (M15/H1/H4/D1)
and maintains rolling windows for each timeframe. On each new M15 candle,
builds the full candle map expected by BtcUsdtMtfV20Strategy.

Also supports batch priming from REST historical data.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional, Sequence

from mctp.core.constants import (
    V20_MTF_BASE_TIMEFRAME_MINUTES,
    V20_MTF_M15_PER_D1,
    V20_MTF_M15_PER_H1,
    V20_MTF_M15_PER_H4,
    V20_MTF_REQUIRED_M15_CANDLES,
)
from mctp.core.enums import Timeframe
from mctp.indicators.models import Candle
from mctp.strategy.mtf import aggregate_closed_m15_candles, build_closed_mtf_candle_map_from_m15

_logger = logging.getLogger(__name__)

# Maximum candles to retain per timeframe in rolling windows.
_MAX_M15_WINDOW = V20_MTF_REQUIRED_M15_CANDLES + 200  # small buffer
_MAX_H1_WINDOW = V20_MTF_REQUIRED_M15_CANDLES // V20_MTF_M15_PER_H1 + 50
_MAX_H4_WINDOW = V20_MTF_REQUIRED_M15_CANDLES // V20_MTF_M15_PER_H4 + 50
_MAX_D1_WINDOW = V20_MTF_REQUIRED_M15_CANDLES // V20_MTF_M15_PER_D1 + 50

# Valid UTC boundaries for H4 candle starts
_H4_VALID_HOURS = frozenset({0, 4, 8, 12, 16, 20})


class LiveMtfAggregator:
    """Incremental multi-timeframe candle aggregator for live data.

    Maintains separate rolling windows for M15, H1, H4, D1.
    Supports two data paths:
      1. Batch priming: prime_from_m15_history() for REST startup data
      2. Incremental: on_candle() for live WS closed candles

    The strategy receives the full candle map via build_strategy_candles().
    """

    def __init__(self) -> None:
        self._candles: dict[Timeframe, list[Candle]] = {
            Timeframe.M15: [],
            Timeframe.H1: [],
            Timeframe.H4: [],
            Timeframe.D1: [],
        }
        self._last_m15_timestamp: Optional[datetime] = None
        self._warmup_complete = False
        self._stale_timeframes: set[Timeframe] = set()

    # ── Public API ──────────────────────────────────────────────────────

    @property
    def warmup_complete(self) -> bool:
        return self._warmup_complete

    @property
    def stale_timeframes(self) -> frozenset[Timeframe]:
        return frozenset(self._stale_timeframes)

    def mark_stale(self, timeframe: Timeframe) -> None:
        self._stale_timeframes.add(timeframe)

    def clear_stale(self, timeframe: Timeframe) -> None:
        self._stale_timeframes.discard(timeframe)

    @property
    def any_stale(self) -> bool:
        return len(self._stale_timeframes) > 0

    def candle_counts(self) -> dict[Timeframe, int]:
        return {tf: len(candles) for tf, candles in self._candles.items()}

    def prime_from_m15_history(self, m15_candles: Sequence[Candle]) -> None:
        """Load historical M15 candles and derive all higher timeframes.

        Used at startup for REST priming.
        """
        closed = [c for c in m15_candles if c.closed]
        closed.sort(key=lambda c: c.timestamp)

        mtf_map = build_closed_mtf_candle_map_from_m15(closed)

        self._candles[Timeframe.M15] = mtf_map[Timeframe.M15][-_MAX_M15_WINDOW:]
        self._candles[Timeframe.H1] = mtf_map[Timeframe.H1][-_MAX_H1_WINDOW:]
        self._candles[Timeframe.H4] = mtf_map[Timeframe.H4][-_MAX_H4_WINDOW:]
        self._candles[Timeframe.D1] = mtf_map[Timeframe.D1][-_MAX_D1_WINDOW:]

        if self._candles[Timeframe.M15]:
            self._last_m15_timestamp = self._candles[Timeframe.M15][-1].timestamp

        self._check_warmup()

        _logger.info(
            "MTF aggregator primed from M15 history: %s",
            json.dumps({
                "m15_loaded": len(self._candles[Timeframe.M15]),
                "h1_derived": len(self._candles[Timeframe.H1]),
                "h4_derived": len(self._candles[Timeframe.H4]),
                "d1_derived": len(self._candles[Timeframe.D1]),
                "warmup_complete": self._warmup_complete,
            }),
        )

    def prime_higher_tf_candles(
        self,
        timeframe: Timeframe,
        candles: Sequence[Candle],
    ) -> None:
        """Load historical candles for a specific higher timeframe directly.

        Used when REST returns H1/H4/D1 candles directly (not derived from M15).
        """
        if timeframe == Timeframe.M15:
            raise ValueError("Use prime_from_m15_history for M15 data")
        closed = [c for c in candles if c.closed]
        closed.sort(key=lambda c: c.timestamp)
        max_window = {
            Timeframe.H1: _MAX_H1_WINDOW,
            Timeframe.H4: _MAX_H4_WINDOW,
            Timeframe.D1: _MAX_D1_WINDOW,
        }.get(timeframe, 500)
        self._candles[timeframe] = closed[-max_window:]
        self._check_warmup()

    def on_candle(self, timeframe: Timeframe, candle: Candle) -> None:
        """Process a single closed candle from live WS.

        For M15: checks for gaps and appends.
        For H1/H4/D1: validates UTC alignment and appends.
        """
        if not candle.closed:
            return

        self.clear_stale(timeframe)

        if timeframe == Timeframe.M15:
            self._on_m15_candle(candle)
        elif timeframe == Timeframe.H4:
            self._on_h4_candle(candle)
        else:
            self._append_candle(timeframe, candle)

        self._check_warmup()

    def build_strategy_candles(self) -> dict[Timeframe, list[Candle]]:
        """Return current candle map for strategy consumption."""
        return {tf: list(candles) for tf, candles in self._candles.items()}

    # ── Internal ────────────────────────────────────────────────────────

    def _on_m15_candle(self, candle: Candle) -> None:
        """Handle M15 candle with gap detection."""
        if self._last_m15_timestamp is not None:
            expected_ts = self._last_m15_timestamp + timedelta(minutes=V20_MTF_BASE_TIMEFRAME_MINUTES)
            if candle.timestamp != expected_ts:
                _logger.warning(
                    json.dumps({
                        "event_type": "MTF_M15_GAP_DETECTED",
                        "symbol": "BTCUSDT",
                        "expected_ts": expected_ts.isoformat(),
                        "received_ts": candle.timestamp.isoformat(),
                    }),
                )
        self._last_m15_timestamp = candle.timestamp
        self._append_candle(Timeframe.M15, candle)

    def _on_h4_candle(self, candle: Candle) -> None:
        """Handle H4 candle with UTC alignment validation."""
        utc_ts = candle.timestamp.astimezone(timezone.utc)
        if utc_ts.hour not in _H4_VALID_HOURS or utc_ts.minute != 0:
            _logger.warning(
                "H4 candle at %s does not align to valid UTC boundary (expected hours: %s)",
                utc_ts.isoformat(),
                sorted(_H4_VALID_HOURS),
            )
        self._append_candle(Timeframe.H4, candle)

    def _append_candle(self, timeframe: Timeframe, candle: Candle) -> None:
        window = self._candles.setdefault(timeframe, [])
        window.append(candle)
        max_size = {
            Timeframe.M15: _MAX_M15_WINDOW,
            Timeframe.H1: _MAX_H1_WINDOW,
            Timeframe.H4: _MAX_H4_WINDOW,
            Timeframe.D1: _MAX_D1_WINDOW,
        }.get(timeframe, _MAX_M15_WINDOW)
        if len(window) > max_size:
            excess = len(window) - max_size
            del window[:excess]

    def _check_warmup(self) -> None:
        m15_count = len(self._candles.get(Timeframe.M15, []))
        h1_count = len(self._candles.get(Timeframe.H1, []))
        h4_count = len(self._candles.get(Timeframe.H4, []))
        d1_count = len(self._candles.get(Timeframe.D1, []))

        # Minimum: strategy needs M15 >= 2, H1 >= 1, H4 >= 1
        # Full warmup: enough M15 for D1 EMA-200 derivation
        self._warmup_complete = (
            m15_count >= V20_MTF_REQUIRED_M15_CANDLES
            and h1_count >= 1
            and h4_count >= 1
            and d1_count >= 1
        )
