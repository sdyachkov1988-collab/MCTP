from datetime import datetime, timedelta, timezone
from decimal import Decimal
import logging

import pytest

from mctp.core.enums import IntentType, Market, Timeframe
from mctp.core.types import Intent, Symbol
from mctp.indicators import IndicatorEngine
from mctp.indicators.models import Candle
from mctp.runtime import (
    BnbTickerEvent,
    BookTickerEvent,
    EmaCrossSmokeStrategy,
    KlineEvent,
    OutboundAccountPositionEvent,
    PaperRuntime,
    PaperRuntimeConfig,
    PaperRuntimeStatus,
    adapt_binance_payload,
)
from mctp.runtime.streams import QueueStreamTransport, WebSocketJsonTransport
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.snapshot_store import SnapshotStore
from mctp.streams.base import StreamType


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
START = datetime(2026, 3, 22, 0, 0, 0, tzinfo=timezone.utc)


def _instrument_info() -> dict:
    return {
        "lot_size": Decimal("0.001"),
        "min_qty": Decimal("0.001"),
        "max_qty": Decimal("1000"),
        "min_notional": Decimal("10"),
    }


def _candle(index: int, close: Decimal) -> Candle:
    return Candle(
        timestamp=START + timedelta(minutes=index),
        open=close,
        high=close + Decimal("1"),
        low=close - Decimal("1"),
        close=close,
        volume=Decimal("10"),
    )


def _runtime(tmp_path, strategy, initial_balances: dict[str, Decimal] | None = None) -> PaperRuntime:
    return PaperRuntime(
        config=PaperRuntimeConfig(
            symbol=BTCUSDT,
            timeframe=Timeframe.M15,
            instrument_info=_instrument_info(),
            initial_balances=initial_balances or {"BTC": Decimal("0"), "USDT": Decimal("1000")},
        ),
        strategy=strategy,
        snapshot_store=SnapshotStore(str(tmp_path / "snapshot.json")),
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        accounting_store=AccountingStore(str(tmp_path / "accounting.json")),
    )


def _binance_kline_payload(close: str, minutes: int = 0) -> dict:
    event_time = int((START + timedelta(minutes=minutes)).timestamp() * 1000)
    close_time = int((START + timedelta(minutes=minutes, seconds=59)).timestamp() * 1000)
    return {
        "e": "kline",
        "E": event_time,
        "k": {
            "t": event_time,
            "T": close_time,
            "i": "15m",
            "o": "100.0",
            "h": "101.0",
            "l": "99.0",
            "c": close,
            "v": "10.0",
            "x": True,
        },
    }


def _binance_book_payload(minutes: int = 0, bid: str = "100.0", ask: str = "101.0") -> dict:
    event_time = int((START + timedelta(minutes=minutes)).timestamp() * 1000)
    return {"e": "bookTicker", "E": event_time, "b": bid, "a": ask}


def _binance_bnb_payload(minutes: int = 0, bid: str = "300.0", ask: str = "302.0") -> dict:
    event_time = int((START + timedelta(minutes=minutes)).timestamp() * 1000)
    return {"e": "bookTicker", "E": event_time, "b": bid, "a": ask}


class RecordingStrategy(EmaCrossSmokeStrategy):
    def __init__(self) -> None:
        self.inputs = []

    def on_candle(self, input) -> Intent:
        self.inputs.append(input)
        return super().on_candle(input)


def test_binance_kline_payload_is_adapted_to_internal_event():
    event = adapt_binance_payload(StreamType.KLINE, _binance_kline_payload("110.0"), timeframe=Timeframe.M15)
    assert isinstance(event, KlineEvent)
    assert event.timeframe == Timeframe.M15
    assert event.candle.close == Decimal("110.0")


def test_binance_book_ticker_payload_is_adapted_to_internal_event():
    event = adapt_binance_payload(StreamType.BOOK_TICKER, _binance_book_payload())
    assert isinstance(event, BookTickerEvent)
    assert event.bid == Decimal("100.0")
    assert event.ask == Decimal("101.0")


def test_binance_bnb_ticker_payload_is_adapted_to_internal_event():
    event = adapt_binance_payload(StreamType.BNB_TICKER, _binance_bnb_payload())
    assert isinstance(event, BnbTickerEvent)
    assert event.price == Decimal("301.0")


@pytest.mark.asyncio
async def test_runtime_constructs_strategy_input_and_calls_strategy(tmp_path):
    strategy = RecordingStrategy()
    runtime = _runtime(tmp_path, strategy)
    await runtime.start()
    await runtime.channels[StreamType.BOOK_TICKER].publish(
        BookTickerEvent(timestamp=START, bid=Decimal("100"), ask=Decimal("101"))
    )
    for index in range(21):
        await runtime.channels[StreamType.KLINE].publish(
            KlineEvent(timeframe=Timeframe.M15, candle=_candle(index, Decimal("100") + Decimal(index)))
        )
    await runtime.process_all_available()
    assert runtime.last_strategy_input is not None
    assert len(strategy.inputs) == 1
    assert Timeframe.M15 in runtime.last_strategy_input.candles


def test_ema_cross_smoke_strategy_returns_buy_and_sell_deterministically():
    strategy = EmaCrossSmokeStrategy()
    buy_intent = strategy.on_candle(
        type("Input", (), {"indicators": {"ema_9": Decimal("12"), "ema_21": Decimal("10")}, "snapshot": type("S", (), {"symbol": BTCUSDT, "timestamp": START})()})()
    )
    sell_intent = strategy.on_candle(
        type("Input", (), {"indicators": {"ema_9": Decimal("9"), "ema_21": Decimal("10")}, "snapshot": type("S", (), {"symbol": BTCUSDT, "timestamp": START})()})()
    )
    assert buy_intent.type == IntentType.BUY
    assert sell_intent.type == IntentType.SELL


@pytest.mark.asyncio
async def test_runtime_passes_intent_into_paper_pipeline(tmp_path):
    strategy = RecordingStrategy()
    runtime = _runtime(tmp_path, strategy)
    await runtime.start()
    await runtime.channels[StreamType.BOOK_TICKER].publish(
        BookTickerEvent(timestamp=START, bid=Decimal("100"), ask=Decimal("101"))
    )
    for index in range(21):
        await runtime.channels[StreamType.KLINE].publish(
            KlineEvent(timeframe=Timeframe.M15, candle=_candle(index, Decimal("100") + Decimal(index)))
        )
    await runtime.process_all_available()
    assert runtime.last_intent is not None and runtime.last_intent.type == IntentType.BUY
    assert runtime.portfolio.snapshot.held_qty > Decimal("0")
    assert len(runtime.portfolio.accounting.fill_history) == 1


@pytest.mark.asyncio
async def test_runtime_market_stream_path_consumes_adapted_events_end_to_end(tmp_path):
    strategy = RecordingStrategy()
    runtime = PaperRuntime(
        config=PaperRuntimeConfig(
            symbol=BTCUSDT,
            timeframe=Timeframe.M15,
            instrument_info=_instrument_info(),
            initial_balances={"BTC": Decimal("0"), "USDT": Decimal("1000")},
        ),
        strategy=strategy,
        snapshot_store=SnapshotStore(str(tmp_path / "snapshot.json")),
        balance_cache_store=BalanceCacheStore(str(tmp_path / "balances.json")),
        accounting_store=AccountingStore(str(tmp_path / "accounting.json")),
        kline_transport=QueueStreamTransport(),
        book_transport=QueueStreamTransport(),
        bnb_transport=QueueStreamTransport(),
    )
    await runtime.start()
    await runtime.channels[StreamType.BOOK_TICKER].publish(_binance_book_payload())
    await runtime.channels[StreamType.BNB_TICKER].publish(_binance_bnb_payload())
    for index in range(21):
        await runtime.channels[StreamType.KLINE].publish(
            _binance_kline_payload(str(Decimal("100") + Decimal(index)), minutes=index)
        )
    await runtime.process_all_available()
    assert runtime.last_strategy_input is not None
    assert isinstance(runtime.last_strategy_input.candles[Timeframe.M15][-1], Candle)
    assert runtime.last_intent is not None


@pytest.mark.asyncio
async def test_runtime_business_handlers_no_longer_depend_on_raw_dict_payloads(tmp_path):
    runtime = _runtime(tmp_path, RecordingStrategy())
    await runtime.start()
    with pytest.raises(AssertionError):
        await runtime._handle_kline({"raw": "payload"})


@pytest.mark.asyncio
async def test_four_stream_channel_abstractions_exist_independently(tmp_path):
    runtime = _runtime(tmp_path, EmaCrossSmokeStrategy())
    await runtime.start()
    assert set(runtime.channels) == {
        StreamType.KLINE,
        StreamType.BOOK_TICKER,
        StreamType.BNB_TICKER,
        StreamType.USER_DATA,
    }
    assert len({id(channel) for channel in runtime.channels.values()}) == 4


@pytest.mark.asyncio
async def test_ping_pong_handling_is_present_for_channels(tmp_path):
    runtime = _runtime(tmp_path, EmaCrossSmokeStrategy())
    await runtime.start()
    await runtime.ping_all(START)
    for channel in runtime.channels.values():
        assert channel.transport.ping_count == 1
        assert channel.transport.pong_count == 1
        assert channel.last_ping_at == START
        assert channel.last_pong_at == START


@pytest.mark.asyncio
async def test_mock_user_data_flow_produces_fills(tmp_path):
    runtime = _runtime(tmp_path, RecordingStrategy())
    await runtime.start()
    await runtime.channels[StreamType.BOOK_TICKER].publish(
        BookTickerEvent(timestamp=START, bid=Decimal("100"), ask=Decimal("101"))
    )
    await runtime.channels[StreamType.BNB_TICKER].publish(
        BnbTickerEvent(timestamp=START, price=Decimal("300"))
    )
    for index in range(21):
        await runtime.channels[StreamType.KLINE].publish(
            KlineEvent(timeframe=Timeframe.M15, candle=_candle(index, Decimal("100") + Decimal(index)))
        )
    await runtime.process_all_available()
    assert len(runtime.handled_fills) == 1
    assert runtime.handled_fills[0].side.name == "BUY"


@pytest.mark.asyncio
async def test_outbound_account_position_updates_balance_cache(tmp_path):
    runtime = _runtime(tmp_path, EmaCrossSmokeStrategy())
    await runtime.start()
    balances = {"BTC": Decimal("0.1"), "USDT": Decimal("950")}
    await runtime.channels[StreamType.USER_DATA].publish(
        OutboundAccountPositionEvent(timestamp=START, balances=balances)
    )
    await runtime.process_all_available()
    loaded = runtime.balance_cache_store.load()
    assert loaded is not None
    cached_balances, updated_at = loaded
    assert cached_balances == balances
    assert updated_at == START


@pytest.mark.asyncio
async def test_stale_kline_causes_halt(tmp_path):
    runtime = _runtime(tmp_path, EmaCrossSmokeStrategy())
    await runtime.start()
    runtime.channels[StreamType.KLINE].touch(START)
    runtime.channels[StreamType.BOOK_TICKER].touch(START)
    runtime.channels[StreamType.BNB_TICKER].touch(START)
    runtime.channels[StreamType.USER_DATA].touch(START)
    await runtime.evaluate_staleness(START + timedelta(seconds=500))
    assert runtime.status == PaperRuntimeStatus.HALT
    assert runtime.channels[StreamType.KLINE].state.is_stale is True


@pytest.mark.asyncio
async def test_stale_book_ticker_sets_stale_flag_but_runtime_continues(tmp_path):
    runtime = _runtime(tmp_path, EmaCrossSmokeStrategy())
    await runtime.start()
    runtime.channels[StreamType.KLINE].touch(START + timedelta(seconds=200))
    runtime.channels[StreamType.BOOK_TICKER].touch(START)
    runtime.channels[StreamType.BNB_TICKER].touch(START + timedelta(seconds=200))
    runtime.channels[StreamType.USER_DATA].touch(START + timedelta(seconds=200))
    await runtime.evaluate_staleness(START + timedelta(seconds=20))
    assert runtime.channels[StreamType.BOOK_TICKER].state.is_stale is True
    assert runtime.status == PaperRuntimeStatus.RUNNING


@pytest.mark.asyncio
async def test_runtime_flow_triggers_stale_evaluation_and_halts_on_stale_kline(tmp_path):
    runtime = _runtime(tmp_path, EmaCrossSmokeStrategy())
    await runtime.start()
    runtime.channels[StreamType.KLINE].touch(START)
    runtime.channels[StreamType.BNB_TICKER].touch(START + timedelta(seconds=500))
    runtime.channels[StreamType.USER_DATA].touch(START + timedelta(seconds=500))
    await runtime.channels[StreamType.BOOK_TICKER].publish(
        BookTickerEvent(timestamp=START + timedelta(seconds=500), bid=Decimal("100"), ask=Decimal("101"))
    )
    await runtime.process_all_available()
    assert runtime.last_stale_check_at == START + timedelta(seconds=500)
    assert runtime.status == PaperRuntimeStatus.HALT


@pytest.mark.asyncio
async def test_runtime_flow_marks_book_ticker_stale_but_continues(tmp_path):
    runtime = _runtime(tmp_path, EmaCrossSmokeStrategy())
    await runtime.start()
    runtime.channels[StreamType.BOOK_TICKER].touch(START)
    runtime.channels[StreamType.BNB_TICKER].touch(START + timedelta(seconds=20))
    runtime.channels[StreamType.USER_DATA].touch(START + timedelta(seconds=20))
    candle = Candle(
        timestamp=START + timedelta(seconds=20),
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=Decimal("10"),
    )
    await runtime.channels[StreamType.KLINE].publish(KlineEvent(timeframe=Timeframe.M15, candle=candle))
    await runtime.process_all_available()
    assert runtime.channels[StreamType.BOOK_TICKER].state.is_stale is True
    assert runtime.status == PaperRuntimeStatus.RUNNING


@pytest.mark.asyncio
async def test_graceful_shutdown_persists_financial_truth_before_cleanup(tmp_path):
    runtime = _runtime(tmp_path, RecordingStrategy())
    await runtime.start()
    await runtime.channels[StreamType.BOOK_TICKER].publish(
        BookTickerEvent(timestamp=START, bid=Decimal("100"), ask=Decimal("101"))
    )
    for index in range(21):
        await runtime.channels[StreamType.KLINE].publish(
            KlineEvent(timeframe=Timeframe.M15, candle=_candle(index, Decimal("100") + Decimal(index)))
        )
    await runtime.process_all_available()
    assert runtime.transient_events
    await runtime.shutdown()
    assert runtime.shutdown_steps == ["persist_financial_truth", "clear_transient_state"]
    assert runtime.snapshot_store.exists()
    assert runtime.balance_cache_store.exists()
    assert runtime.accounting_store.exists()
    assert runtime.transient_events == []
    assert runtime.status == PaperRuntimeStatus.STOPPED


@pytest.mark.asyncio
async def test_runtime_uses_indicator_engine_values_in_strategy_input(tmp_path):
    strategy = RecordingStrategy()
    runtime = _runtime(tmp_path, strategy)
    await runtime.start()
    await runtime.channels[StreamType.BOOK_TICKER].publish(
        BookTickerEvent(timestamp=START, bid=Decimal("100"), ask=Decimal("101"))
    )
    candles = [_candle(index, Decimal("100") + Decimal(index)) for index in range(21)]
    for candle in candles:
        await runtime.channels[StreamType.KLINE].publish(
            KlineEvent(timeframe=Timeframe.M15, candle=candle)
        )
    await runtime.process_all_available()
    indicators = runtime.last_strategy_input.indicators
    engine = IndicatorEngine()
    assert indicators["ema_9"] == engine.ema(candles, 9)
    assert indicators["ema_21"] == engine.ema(candles, 21)


def test_websocket_transport_path_exists_for_real_read_only_mode():
    transport = WebSocketJsonTransport()
    assert hasattr(transport, "connect")
    assert hasattr(transport, "receive")


@pytest.mark.asyncio
async def test_paper_runtime_emits_human_readable_operator_logs(tmp_path, caplog):
    strategy = RecordingStrategy()
    runtime = _runtime(tmp_path, strategy)
    with caplog.at_level(logging.INFO, logger="mctp.runtime.paper"):
        await runtime.start()
        await runtime.channels[StreamType.BOOK_TICKER].publish(
            BookTickerEvent(timestamp=START, bid=Decimal("100"), ask=Decimal("101"))
        )
        for index in range(21):
            await runtime.channels[StreamType.KLINE].publish(
                KlineEvent(timeframe=Timeframe.M15, candle=_candle(index, Decimal("100") + Decimal(index)))
            )
        await runtime.process_all_available()
        await runtime.shutdown()

    assert "paper runtime started" in caplog.text
    assert "paper runtime closed candle received" in caplog.text
    assert "paper runtime strategy called" in caplog.text
    assert "paper runtime intent produced" in caplog.text
    assert "paper runtime risk approved" in caplog.text
    assert "paper runtime order submitted" in caplog.text
    assert "paper runtime fill handled" in caplog.text
    assert "paper runtime persisted financial truth" in caplog.text
    assert "paper runtime stopped" in caplog.text
