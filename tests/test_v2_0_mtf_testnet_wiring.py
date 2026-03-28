"""
Integration tests for v2.0 MTF pipeline on mock testnet data.

Covers:
1. Mock kline WS events for M15/H1/H4/D1 -> MTF aggregator receives candles
   -> BtcUsdtMtfV20Strategy returns valid Intent (not an error)
2. UTC alignment: 4H closed candle arrives at correct UTC boundary
3. STALE handling: one TF marked stale -> strategy returns HOLD, no crash
4. Gap warning: missing M15 candle -> WARNING logged with correct fields
5. Startup priming: mock REST history -> MTF aggregator warmup complete
   -> startup gate transitions to READY
"""
import asyncio
import json
import logging
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Optional

import pytest

from mctp.adapters import BinanceCredentials, BinanceSpotTestnetAdapterV1, BinanceSpotTestnetConfigV1
from mctp.core.constants import V20_MTF_REQUIRED_M15_CANDLES
from mctp.core.enums import IntentType, Market, Timeframe
from mctp.core.types import Intent, PortfolioSnapshot, Symbol
from mctp.indicators.models import Candle
from mctp.runtime.events import KlineEvent
from mctp.runtime.mtf_kline_manager import MtfKlineManager, MTF_TIMEFRAMES
from mctp.runtime.streams import QueueStreamTransport
from mctp.runtime.testnet import TestnetRuntime, TestnetRuntimeConfig, TestnetRuntimeStatus
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.snapshot_store import SnapshotStore
from mctp.strategy import BtcUsdtMtfV20Strategy
from mctp.strategy.mtf_live import LiveMtfAggregator
from mctp.streams.base import StreamType

BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
START = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def _candle(
    ts: datetime,
    close: Decimal = Decimal("40000"),
    *,
    open_: Decimal | None = None,
    high: Decimal | None = None,
    low: Decimal | None = None,
    volume: Decimal = Decimal("100"),
) -> Candle:
    return Candle(
        timestamp=ts,
        open=open_ or close - Decimal("100"),
        high=high or close + Decimal("50"),
        low=low or close - Decimal("150"),
        close=close,
        volume=volume,
        closed=True,
    )


def _m15_series(start: datetime, count: int, base_close: Decimal = Decimal("40000")) -> list[Candle]:
    """Generate a sequential series of M15 candles."""
    candles = []
    for i in range(count):
        ts = start + timedelta(minutes=15 * i)
        close = base_close + Decimal(str(i))
        candles.append(_candle(ts, close))
    return candles


class FakeRestClient:
    """Fake REST client that returns mock kline data."""
    def __init__(self, kline_data: dict[str, list] | None = None) -> None:
        self.calls: list[tuple] = []
        self.account_balances = {"BTC": Decimal("0"), "USDT": Decimal("10000"), "BNB": Decimal("1")}
        self.locked_balances = {"BTC": Decimal("0"), "USDT": Decimal("0"), "BNB": Decimal("0")}
        self.open_oco_order_ids: list[str] = []
        self.order_submit_status = "NEW"
        self._kline_data = kline_data or {}

    async def request_json(self, method, path, *, params=None, signed=False):
        safe_params = dict(params or {})
        self.calls.append((method, path, safe_params, signed))
        if path == "/api/v3/klines" and method == "GET":
            interval = safe_params.get("interval", "15m")
            return self._kline_data.get(interval, [])
        if path == "/api/v3/order" and method == "POST":
            return {"status": self.order_submit_status}
        if path == "/api/v3/order" and method == "DELETE":
            return {"status": "CANCELED"}
        if path == "/api/v3/userDataStream" and method == "POST":
            return {"listenKey": "listen-key"}
        if path == "/api/v3/userDataStream" and method in {"PUT", "DELETE"}:
            return {}
        if path == "/api/v3/orderList/oco":
            return {"orderListId": "oco-1"}
        if path == "/api/v3/openOrderList":
            return [{} for _ in self.open_oco_order_ids]
        if path == "/api/v3/openOrders":
            return []
        if path == "/api/v3/account":
            return {
                "balances": [
                    {"asset": a, "free": str(v), "locked": str(self.locked_balances.get(a, Decimal("0")))}
                    for a, v in self.account_balances.items()
                ]
            }
        if path == "/api/v3/exchangeInfo":
            return {
                "symbols": [{
                    "status": "TRADING",
                    "filters": [
                        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "1000"},
                        {"filterType": "MIN_NOTIONAL", "minNotional": "10"},
                    ],
                }]
            }
        raise AssertionError(f"Unhandled fake REST call: {method} {path}")


def _make_rest_kline_rows(candles: list[Candle]) -> list[list]:
    """Convert Candle objects to Binance REST kline format."""
    rows = []
    for c in candles:
        open_time_ms = int(c.timestamp.timestamp() * 1000)
        rows.append([
            open_time_ms,
            str(c.open), str(c.high), str(c.low), str(c.close), str(c.volume),
            open_time_ms + 899999,  # close time
            "0", 0, "0", "0", "0",
        ])
    return rows


def _runtime_with_mtf(
    tmp_path,
    rest_client: FakeRestClient,
    *,
    mtf_transports: dict[Timeframe, QueueStreamTransport] | None = None,
) -> TestnetRuntime:
    adapter = BinanceSpotTestnetAdapterV1(
        BinanceSpotTestnetConfigV1(BinanceCredentials(api_key="k", api_secret="s")),
        rest_client=rest_client,
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
    )
    # Inject rest_client for MTF kline manager
    adapter._rest_client = rest_client
    config = TestnetRuntimeConfig(
        symbol=BTCUSDT,
        timeframe=Timeframe.M15,
        instrument_info={
            "lot_size": Decimal("0.001"),
            "min_qty": Decimal("0.001"),
            "max_qty": Decimal("1000"),
            "min_notional": Decimal("10"),
        },
        initial_balances={"BTC": Decimal("0"), "USDT": Decimal("10000")},
    )
    transports = mtf_transports or {}
    runtime = TestnetRuntime(
        config=config,
        strategy=BtcUsdtMtfV20Strategy(),
        executor=adapter,
        snapshot_store=SnapshotStore(str(tmp_path / "snapshot.json")),
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        accounting_store=AccountingStore(str(tmp_path / "accounting.json")),
        kline_transport=QueueStreamTransport(),
        book_transport=QueueStreamTransport(),
        bnb_transport=QueueStreamTransport(),
        user_transport=QueueStreamTransport(),
        mtf_kline_transports=transports,
    )
    return runtime


# ═══════════════════════════════════════════════════════════════════════════
# Test 1: Mock kline WS events -> MTF aggregator -> strategy returns Intent
# ═══════════════════════════════════════════════════════════════════════════

def test_mtf_aggregator_receives_candles_and_strategy_returns_intent():
    """Live candles from all 4 TFs feed into aggregator; strategy returns valid Intent."""
    agg = LiveMtfAggregator()

    # Prime with enough M15 for warmup
    m15 = _m15_series(START, V20_MTF_REQUIRED_M15_CANDLES)
    agg.prime_from_m15_history(m15)

    assert agg.warmup_complete is True

    # Simulate incremental live candles
    next_m15_ts = START + timedelta(minutes=15 * V20_MTF_REQUIRED_M15_CANDLES)
    new_m15 = _candle(next_m15_ts, Decimal("41000"))
    agg.on_candle(Timeframe.M15, new_m15)

    new_h1_ts = next_m15_ts.replace(minute=0)
    new_h1 = _candle(new_h1_ts, Decimal("41000"))
    agg.on_candle(Timeframe.H1, new_h1)

    new_h4_ts = next_m15_ts.replace(hour=(next_m15_ts.hour // 4) * 4, minute=0)
    new_h4 = _candle(new_h4_ts, Decimal("41000"))
    agg.on_candle(Timeframe.H4, new_h4)

    new_d1_ts = next_m15_ts.replace(hour=0, minute=0)
    new_d1 = _candle(new_d1_ts, Decimal("41000"))
    agg.on_candle(Timeframe.D1, new_d1)

    candle_map = agg.build_strategy_candles()
    assert Timeframe.M15 in candle_map
    assert Timeframe.H1 in candle_map
    assert Timeframe.H4 in candle_map
    assert Timeframe.D1 in candle_map
    assert Timeframe.W1 in candle_map
    assert Timeframe.MONTHLY in candle_map

    # Strategy should return a valid Intent (HOLD since conditions won't align)
    strategy = BtcUsdtMtfV20Strategy()
    from mctp.strategy.models import StrategyInput

    snapshot = PortfolioSnapshot(
        symbol=BTCUSDT,
        held_qty=Decimal("0"),
        avg_cost_basis=Decimal("0"),
        free_quote=Decimal("10000"),
        quote_asset="USDT",
        is_in_position=False,
        meaningful_position=False,
    )
    input_ = StrategyInput(
        snapshot=snapshot,
        indicators={},
        candles=candle_map,
    )
    intent = strategy.on_candle(input_)
    assert isinstance(intent, Intent)
    assert intent.type in {IntentType.BUY, IntentType.SELL, IntentType.HOLD}


# ═══════════════════════════════════════════════════════════════════════════
# Test 2: UTC alignment for H4 candle boundary
# ═══════════════════════════════════════════════════════════════════════════

def test_h4_candle_utc_alignment_validated():
    """H4 closed candle at correct UTC boundary is accepted without warning."""
    agg = LiveMtfAggregator()

    # Valid H4 boundaries: 00:00, 04:00, 08:00, 12:00, 16:00, 20:00
    for hour in (0, 4, 8, 12, 16, 20):
        ts = datetime(2024, 6, 15, hour, 0, 0, tzinfo=timezone.utc)
        candle = _candle(ts)
        agg.on_candle(Timeframe.H4, candle)

    assert len(agg.build_strategy_candles()[Timeframe.H4]) == 6


def test_h4_candle_non_utc_boundary_still_accepted_with_warning(caplog):
    """H4 candle at non-standard boundary is accepted but logged."""
    agg = LiveMtfAggregator()
    bad_ts = datetime(2024, 6, 15, 3, 0, 0, tzinfo=timezone.utc)
    with caplog.at_level(logging.WARNING):
        agg.on_candle(Timeframe.H4, _candle(bad_ts))
    assert len(agg.build_strategy_candles()[Timeframe.H4]) == 1
    assert "does not align" in caplog.text


# ═══════════════════════════════════════════════════════════════════════════
# Test 3: STALE handling — strategy returns HOLD, no crash
# ═══════════════════════════════════════════════════════════════════════════

def test_stale_tf_causes_aggregator_to_report_stale():
    """When any TF is marked stale, aggregator.any_stale is True."""
    agg = LiveMtfAggregator()
    assert agg.any_stale is False

    agg.mark_stale(Timeframe.H4)
    assert agg.any_stale is True
    assert Timeframe.H4 in agg.stale_timeframes

    agg.clear_stale(Timeframe.H4)
    assert agg.any_stale is False


@pytest.mark.asyncio
async def test_stale_tf_strategy_returns_hold_no_crash(tmp_path):
    """When one TF is stale, _handle_kline returns early (HOLD), no crash."""
    rest = FakeRestClient()
    mtf_transports = {tf: QueueStreamTransport() for tf in MTF_TIMEFRAMES}
    runtime = _runtime_with_mtf(tmp_path, rest, mtf_transports=mtf_transports)
    await runtime.start()
    try:
        # Mark H4 as stale
        runtime.mtf_aggregator.mark_stale(Timeframe.H4)
        assert runtime.mtf_aggregator.any_stale is True

        # Feed enough M15 candles past warmup_bars
        for i in range(25):
            ts = START + timedelta(minutes=15 * i)
            candle = _candle(ts)
            kline_event = KlineEvent(timeframe=Timeframe.M15, candle=candle)
            await runtime._handle_kline(kline_event)

        # Strategy should not have been called (stale guard)
        assert runtime.strategy_call_count == 0
    finally:
        await runtime.shutdown()


# ═══════════════════════════════════════════════════════════════════════════
# Test 4: Gap warning — missing M15 candle -> WARNING log with correct fields
# ═══════════════════════════════════════════════════════════════════════════

def test_m15_gap_detected_logs_warning(caplog):
    """Missing M15 candle triggers structured WARNING log."""
    agg = LiveMtfAggregator()

    # First candle
    ts1 = START
    agg.on_candle(Timeframe.M15, _candle(ts1))

    # Skip one candle (gap: expected ts1+15m but got ts1+30m)
    ts_gap = ts1 + timedelta(minutes=30)
    with caplog.at_level(logging.WARNING):
        agg.on_candle(Timeframe.M15, _candle(ts_gap))

    # Verify structured log fields
    assert "MTF_M15_GAP_DETECTED" in caplog.text
    assert "expected_ts" in caplog.text
    assert "received_ts" in caplog.text

    # Parse the logged JSON
    for record in caplog.records:
        if "MTF_M15_GAP_DETECTED" in record.getMessage():
            logged_data = json.loads(record.getMessage())
            assert logged_data["event_type"] == "MTF_M15_GAP_DETECTED"
            assert logged_data["symbol"] == "BTCUSDT"
            expected = (ts1 + timedelta(minutes=15)).isoformat()
            assert logged_data["expected_ts"] == expected
            assert logged_data["received_ts"] == ts_gap.isoformat()
            break
    else:
        pytest.fail("MTF_M15_GAP_DETECTED structured log not found")


def test_m15_sequential_no_gap_warning(caplog):
    """Sequential M15 candles produce no gap warning."""
    agg = LiveMtfAggregator()
    for i in range(5):
        ts = START + timedelta(minutes=15 * i)
        with caplog.at_level(logging.WARNING):
            agg.on_candle(Timeframe.M15, _candle(ts))
    assert "MTF_M15_GAP_DETECTED" not in caplog.text


# ═══════════════════════════════════════════════════════════════════════════
# Test 5: Startup priming — mock REST -> warmup complete -> READY
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_startup_priming_warmup_complete_transitions_to_ready(tmp_path):
    """REST priming with sufficient data -> warmup complete -> READY."""
    # Generate enough M15 candles for full warmup
    m15_candles = _m15_series(START, V20_MTF_REQUIRED_M15_CANDLES)
    m15_rows = _make_rest_kline_rows(m15_candles)

    # Generate matching higher TF candles
    h1_candles = []
    for i in range(V20_MTF_REQUIRED_M15_CANDLES // 4):
        ts = START + timedelta(hours=i)
        h1_candles.append(_candle(ts))
    h1_rows = _make_rest_kline_rows(h1_candles)

    h4_candles = []
    for i in range(V20_MTF_REQUIRED_M15_CANDLES // 16):
        ts = START + timedelta(hours=4 * i)
        h4_candles.append(_candle(ts))
    h4_rows = _make_rest_kline_rows(h4_candles)

    d1_candles = []
    for i in range(200):
        ts = START + timedelta(days=i)
        d1_candles.append(_candle(ts))
    d1_rows = _make_rest_kline_rows(d1_candles)

    rest = FakeRestClient(kline_data={
        "15m": m15_rows,
        "1h": h1_rows,
        "4h": h4_rows,
        "1d": d1_rows,
    })

    mtf_transports = {tf: QueueStreamTransport() for tf in MTF_TIMEFRAMES}
    runtime = _runtime_with_mtf(tmp_path, rest, mtf_transports=mtf_transports)
    await runtime.start()
    try:
        # With full REST priming, warmup should be complete
        assert runtime.mtf_aggregator.warmup_complete is True
        # Runtime should be READY
        assert runtime.status == TestnetRuntimeStatus.READY
        # Candle counts should be populated
        counts = runtime.mtf_aggregator.candle_counts()
        assert counts[Timeframe.M15] >= V20_MTF_REQUIRED_M15_CANDLES
        assert counts[Timeframe.H1] >= 1
        assert counts[Timeframe.H4] >= 1
        assert counts[Timeframe.D1] >= 1
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_startup_without_rest_data_stays_starting(tmp_path):
    """No REST data -> warmup incomplete -> stays in STARTING."""
    rest = FakeRestClient()  # No kline data
    mtf_transports = {tf: QueueStreamTransport() for tf in MTF_TIMEFRAMES}
    runtime = _runtime_with_mtf(tmp_path, rest, mtf_transports=mtf_transports)
    await runtime.start()
    try:
        assert runtime.mtf_aggregator.warmup_complete is False
        # Runtime stays in STARTING because warmup not complete
        assert runtime.status == TestnetRuntimeStatus.STARTING
    finally:
        await runtime.shutdown()


# ═══════════════════════════════════════════════════════════════════════════
# Test 6: Independent lifecycle per TF channel
# ═══════════════════════════════════════════════════════════════════════════

def test_mtf_kline_manager_creates_independent_channels():
    """MtfKlineManager creates one channel per timeframe with independent state."""
    agg = LiveMtfAggregator()
    transports = {tf: QueueStreamTransport() for tf in MTF_TIMEFRAMES}
    mgr = MtfKlineManager(BTCUSDT, agg, kline_transports=transports)
    channels = mgr.build_channels()

    assert len(channels) == 4
    assert set(channels.keys()) == set(MTF_TIMEFRAMES)

    # Each channel has independent state
    for tf, ch in channels.items():
        assert ch.state.stream_type == StreamType.KLINE
        assert ch.state.is_connected is False
        assert ch.state.last_update_at is None


@pytest.mark.asyncio
async def test_mtf_channels_connect_disconnect_independently():
    """Each TF channel connects and disconnects independently."""
    agg = LiveMtfAggregator()
    transports = {tf: QueueStreamTransport() for tf in MTF_TIMEFRAMES}
    mgr = MtfKlineManager(BTCUSDT, agg, kline_transports=transports)
    mgr.build_channels()
    await mgr.connect_all()

    for ch in mgr.channels.values():
        assert ch.state.is_connected is True

    await mgr.disconnect_all()
    for ch in mgr.channels.values():
        assert ch.state.is_connected is False


# ═══════════════════════════════════════════════════════════════════════════
# Test 7: Per-TF staleness evaluation
# ═══════════════════════════════════════════════════════════════════════════

def test_per_tf_staleness_evaluation():
    """Per-TF staleness tracks each timeframe independently."""
    agg = LiveMtfAggregator()
    transports = {tf: QueueStreamTransport() for tf in MTF_TIMEFRAMES}
    mgr = MtfKlineManager(BTCUSDT, agg, kline_transports=transports)
    mgr.build_channels()

    now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    # Touch M15 recently but not H4
    mgr._last_touch[Timeframe.M15] = now - timedelta(seconds=60)
    mgr._last_touch[Timeframe.H4] = now - timedelta(hours=5)  # stale (threshold ~4h5m)

    result = mgr.evaluate_staleness(now)
    assert result[Timeframe.M15] is False
    assert result[Timeframe.H4] is True
    assert agg.any_stale is True
    assert Timeframe.H4 in agg.stale_timeframes


# ═══════════════════════════════════════════════════════════════════════════
# Test 8: Aggregator prime_from_m15_history
# ═══════════════════════════════════════════════════════════════════════════

def test_aggregator_prime_derives_all_timeframes():
    """Priming from M15 history derives H1, H4, D1 candles."""
    agg = LiveMtfAggregator()
    # Use enough history to cross both weekly and monthly UTC bucket boundaries.
    count = 96 * 70  # 70 full days of M15 candles
    candles = _m15_series(START, count)
    agg.prime_from_m15_history(candles)

    counts = agg.candle_counts()
    assert counts[Timeframe.M15] == count
    assert counts[Timeframe.H1] > 0  # 288/4 = 72 H1
    assert counts[Timeframe.H4] > 0  # 288/16 = 18 H4
    assert counts[Timeframe.D1] > 0  # 288/96 = 3 D1
    assert counts[Timeframe.W1] > 0
    assert counts[Timeframe.MONTHLY] > 0


# ═══════════════════════════════════════════════════════════════════════════
# Test 9: Full pipeline: MTF events from transports through aggregator
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_full_mtf_pipeline_from_transport_to_aggregator():
    """Kline events published to transports are received and aggregated."""
    agg = LiveMtfAggregator()
    transports = {tf: QueueStreamTransport() for tf in MTF_TIMEFRAMES}
    mgr = MtfKlineManager(BTCUSDT, agg, kline_transports=transports)
    mgr.build_channels()
    await mgr.connect_all()

    # Publish a closed M15 kline payload
    ts = START
    m15_payload = {
        "e": "kline",
        "E": int(ts.timestamp() * 1000),
        "k": {
            "T": int(ts.timestamp() * 1000),
            "i": "15m",
            "o": "39900",
            "h": "40050",
            "l": "39850",
            "c": "40000",
            "v": "100",
            "x": True,
        },
    }
    await transports[Timeframe.M15].publish(m15_payload)

    # Publish a closed H1 kline payload
    h1_ts = START
    h1_payload = {
        "e": "kline",
        "E": int(h1_ts.timestamp() * 1000),
        "k": {
            "T": int(h1_ts.timestamp() * 1000),
            "i": "1h",
            "o": "39900",
            "h": "40050",
            "l": "39850",
            "c": "40000",
            "v": "400",
            "x": True,
        },
    }
    await transports[Timeframe.H1].publish(h1_payload)

    events = await mgr.receive_and_process()
    assert len(events) == 2

    counts = agg.candle_counts()
    assert counts[Timeframe.M15] == 1
    assert counts[Timeframe.H1] == 1

    await mgr.disconnect_all()


# ═══════════════════════════════════════════════════════════════════════════
# Test 10: Unclosed candle is filtered out
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_unclosed_candle_is_filtered():
    """Unclosed candle from WS should not be added to aggregator."""
    agg = LiveMtfAggregator()
    transports = {tf: QueueStreamTransport() for tf in MTF_TIMEFRAMES}
    mgr = MtfKlineManager(BTCUSDT, agg, kline_transports=transports)
    mgr.build_channels()
    await mgr.connect_all()

    ts = START
    unclosed_payload = {
        "e": "kline",
        "E": int(ts.timestamp() * 1000),
        "k": {
            "T": int(ts.timestamp() * 1000),
            "i": "15m",
            "o": "39900",
            "h": "40050",
            "l": "39850",
            "c": "40000",
            "v": "100",
            "x": False,  # not closed
        },
    }
    await transports[Timeframe.M15].publish(unclosed_payload)

    events = await mgr.receive_and_process()
    assert len(events) == 0  # Unclosed candle filtered
    assert agg.candle_counts()[Timeframe.M15] == 0

    await mgr.disconnect_all()
