"""
Multi-timeframe kline subscription manager for testnet runtime.

Manages independent WS channels for M15/H1/H4/D1, REST priming at startup,
per-TF staleness tracking, and routing closed candles to LiveMtfAggregator.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Optional, Sequence

from mctp.core.constants import (
    BINANCE_SPOT_TESTNET_REST_BASE_URL,
    KLINE_STALE_SECONDS,
    V20_MTF_M15_PER_D1,
    V20_MTF_M15_PER_H1,
    V20_MTF_M15_PER_H4,
    V20_MTF_REQUIRED_M15_CANDLES,
)
from mctp.core.enums import Timeframe
from mctp.core.types import Symbol
from mctp.indicators.models import Candle
from mctp.runtime.events import KlineEvent
from mctp.runtime.testnet_adapters import adapt_binance_testnet_payload
from mctp.runtime.testnet_streams import BinanceSpotTestnetKlineChannel, ReconnectableStreamChannel
from mctp.runtime.streams import QueueStreamTransport, WebSocketJsonTransport
from mctp.strategy.mtf_live import LiveMtfAggregator
from mctp.streams.base import StreamType

_logger = logging.getLogger(__name__)

# All 4 timeframes the V2.0 MTF strategy requires
MTF_TIMEFRAMES: tuple[Timeframe, ...] = (Timeframe.M15, Timeframe.H1, Timeframe.H4, Timeframe.D1)

# Minimum candles to fetch per timeframe at startup via REST
_REST_HISTORY_LIMITS: dict[Timeframe, int] = {
    Timeframe.M15: V20_MTF_REQUIRED_M15_CANDLES,  # 19200
    Timeframe.H1: V20_MTF_REQUIRED_M15_CANDLES // V20_MTF_M15_PER_H1,  # 4800
    Timeframe.H4: V20_MTF_REQUIRED_M15_CANDLES // V20_MTF_M15_PER_H4,  # 1200
    Timeframe.D1: V20_MTF_REQUIRED_M15_CANDLES // V20_MTF_M15_PER_D1,  # 200
}

# Binance REST klines max per request
_BINANCE_MAX_KLINES_PER_REQUEST = 1000

# Stale thresholds per timeframe (seconds)
_STALE_THRESHOLDS: dict[Timeframe, int] = {
    Timeframe.M15: KLINE_STALE_SECONDS,       # 120s = 2 min (miss one candle = stale)
    Timeframe.H1: 3900,                        # 65 min
    Timeframe.H4: 14700,                       # ~4h 5min
    Timeframe.D1: 87300,                       # ~24h 15min
}


def _timeframe_to_binance_interval(tf: Timeframe) -> str:
    """Convert Timeframe enum to Binance REST interval string."""
    return tf.value


class MtfKlineManager:
    """Manages multi-TF kline WS channels and REST priming for testnet."""

    def __init__(
        self,
        symbol: Symbol,
        aggregator: LiveMtfAggregator,
        *,
        kline_transports: Optional[dict[Timeframe, Any]] = None,
        rest_client: Optional[Any] = None,
        primary_kline_transport: Optional[Any] = None,
    ) -> None:
        self.symbol = symbol
        self.aggregator = aggregator
        self._rest_client = rest_client
        self._kline_transports = kline_transports or {}
        self._primary_kline_transport = primary_kline_transport
        self.channels: dict[Timeframe, ReconnectableStreamChannel] = {}
        self._last_touch: dict[Timeframe, Optional[datetime]] = {tf: None for tf in MTF_TIMEFRAMES}

    # ── Channel lifecycle ───────────────────────────────────────────────

    def _make_default_transport(self) -> Any:
        """Create a default transport matching the primary kline transport type."""
        if isinstance(self._primary_kline_transport, QueueStreamTransport):
            return QueueStreamTransport()
        return WebSocketJsonTransport()

    def build_channels(self) -> dict[Timeframe, ReconnectableStreamChannel]:
        """Create independent kline WS channels for each timeframe."""
        for tf in MTF_TIMEFRAMES:
            transport = self._kline_transports.get(tf) or self._make_default_transport()
            channel = BinanceSpotTestnetKlineChannel(
                self.symbol,
                tf,
                transport,
                lambda payload, _tf=tf: adapt_binance_testnet_payload(
                    StreamType.KLINE, payload, timeframe=_tf,
                ),
            )
            self.channels[tf] = channel
        return self.channels

    async def connect_all(self) -> None:
        """Connect all kline channels."""
        for channel in self.channels.values():
            await channel.connect()

    async def disconnect_all(self) -> None:
        """Disconnect all kline channels."""
        for channel in self.channels.values():
            await channel.disconnect()

    async def ping_all(self, now: Optional[datetime] = None) -> None:
        """Ping/pong all kline channels."""
        for channel in self.channels.values():
            await channel.ping(now)
            await channel.pong(now)

    # ── Live event processing ───────────────────────────────────────────

    async def receive_and_process(self) -> list[KlineEvent]:
        """Poll all TF channels, return processed closed-candle events."""
        events: list[KlineEvent] = []
        for tf, channel in self.channels.items():
            event = await channel.receive()
            if channel.reconnect_count > 0:
                channel.reconnect_count = 0
            if event is None:
                continue
            if not isinstance(event, KlineEvent):
                continue
            channel.touch(event.candle.timestamp)
            self._last_touch[tf] = event.candle.timestamp
            self.aggregator.clear_stale(tf)
            if not event.candle.closed:
                continue
            self.aggregator.on_candle(tf, event.candle)
            events.append(event)
        return events

    # ── Staleness ───────────────────────────────────────────────────────

    def evaluate_staleness(self, now: datetime) -> dict[Timeframe, bool]:
        """Check per-TF staleness and update aggregator stale flags."""
        result: dict[Timeframe, bool] = {}
        for tf in MTF_TIMEFRAMES:
            threshold = _STALE_THRESHOLDS[tf]
            last = self._last_touch.get(tf)
            if last is None:
                is_stale = False  # not yet received = not stale (startup grace)
            else:
                is_stale = (now - last).total_seconds() > threshold
            result[tf] = is_stale
            if is_stale:
                self.aggregator.mark_stale(tf)
            else:
                self.aggregator.clear_stale(tf)
        return result

    # ── REST priming ────────────────────────────────────────────────────

    async def prime_from_rest(self) -> dict[Timeframe, int]:
        """Fetch historical klines via REST for all 4 TF and prime aggregator.

        Returns dict of {Timeframe: candles_loaded}.
        """
        if self._rest_client is None:
            _logger.warning("No REST client provided; skipping REST priming")
            return {tf: 0 for tf in MTF_TIMEFRAMES}

        loaded: dict[Timeframe, int] = {}

        # Prime M15 first (the base), then higher TFs directly
        m15_candles = await self._fetch_historical_klines(Timeframe.M15)
        if m15_candles:
            self.aggregator.prime_from_m15_history(m15_candles)
        loaded[Timeframe.M15] = len(m15_candles)

        # Higher TFs: fetch directly and prime
        for tf in (Timeframe.H1, Timeframe.H4, Timeframe.D1):
            candles = await self._fetch_historical_klines(tf)
            if candles:
                self.aggregator.prime_higher_tf_candles(tf, candles)
            loaded[tf] = len(candles)

        # Log startup priming summary
        counts = self.aggregator.candle_counts()
        m15_count = counts.get(Timeframe.M15, 0)
        days_loaded = m15_count * 15 / (60 * 24)  # M15 candles * 15min / minutes_per_day
        _logger.info(
            "MTF REST priming complete: %s",
            json.dumps({
                "event_type": "MTF_REST_PRIMING_COMPLETE",
                "symbol": self.symbol.to_exchange_str(),
                "candles_per_tf": {tf.value: count for tf, count in counts.items()},
                "warmup_complete": self.aggregator.warmup_complete,
                "days_of_history_loaded": round(days_loaded, 1),
            }),
        )
        return loaded

    async def _fetch_historical_klines(self, timeframe: Timeframe) -> list[Candle]:
        """Fetch historical klines from Binance REST with pagination."""
        limit = _REST_HISTORY_LIMITS.get(timeframe, 1000)
        interval = _timeframe_to_binance_interval(timeframe)
        exchange_symbol = self.symbol.to_exchange_str()
        all_candles: list[Candle] = []

        end_time: Optional[int] = None
        remaining = limit

        while remaining > 0:
            batch_size = min(remaining, _BINANCE_MAX_KLINES_PER_REQUEST)
            params: dict[str, Any] = {
                "symbol": exchange_symbol,
                "interval": interval,
                "limit": batch_size,
            }
            if end_time is not None:
                params["endTime"] = end_time

            try:
                raw_klines = await self._rest_client.request_json(
                    "GET", "/api/v3/klines", params=params, signed=False,
                )
            except Exception:
                _logger.exception("Failed to fetch %s klines (fetched %d so far)", timeframe.value, len(all_candles))
                break

            if not raw_klines:
                break

            batch_candles = _parse_rest_klines(raw_klines, timeframe)
            if not batch_candles:
                break

            all_candles = batch_candles + all_candles
            remaining -= len(batch_candles)

            # Next page: move end_time before earliest candle
            earliest_open_ms = int(raw_klines[0][0])
            end_time = earliest_open_ms - 1

            if len(batch_candles) < batch_size:
                break  # No more data available

        return all_candles


def _parse_rest_klines(raw_klines: list, timeframe: Timeframe) -> list[Candle]:
    """Parse Binance REST /api/v3/klines response into Candle objects.

    Binance REST kline format: [open_time, open, high, low, close, volume,
    close_time, quote_volume, trades, taker_buy_base, taker_buy_quote, ignore]
    """
    candles: list[Candle] = []
    for row in raw_klines:
        try:
            open_time_ms = int(row[0])
            candle = Candle(
                timestamp=datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc),
                open=Decimal(str(row[1])),
                high=Decimal(str(row[2])),
                low=Decimal(str(row[3])),
                close=Decimal(str(row[4])),
                volume=Decimal(str(row[5])),
                closed=True,
            )
            candles.append(candle)
        except (IndexError, ValueError, TypeError):
            _logger.warning("Skipping malformed REST kline row: %s", row)
            continue
    return candles
