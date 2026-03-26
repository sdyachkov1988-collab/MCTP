import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional

from mctp.core.constants import (
    BASE_RISK_PCT,
    BNB_TICKER_STALE_SECONDS,
    BOOK_TICKER_STALE_SECONDS,
    KLINE_STALE_SECONDS,
    PAPER_SMOKE_ORDER_QTY,
    PAPER_RUNTIME_HEARTBEAT_SECONDS,
    STREAM_PING_SECONDS,
    USER_DATA_STALE_SECONDS,
)
from mctp.core.enums import ExecutionResult, IntentType, OperationalMode, OrderType, Side, Timeframe
from mctp.core.order import Order
from mctp.core.types import PortfolioSnapshot, Symbol
from mctp.execution.paper import SpotPaperExecutor
from mctp.indicators import IndicatorEngine
from mctp.indicators.models import Candle
from mctp.portfolio.meaningful import is_meaningful_position
from mctp.portfolio.tracker import PortfolioTracker
from mctp.risk.adaptive import AdaptiveRiskController
from mctp.risk.config import RiskConfig
from mctp.risk.layer import RiskLayer
from mctp.runtime.events import (
    BnbTickerEvent,
    BookTickerEvent,
    KlineEvent,
    MockExecutionReportEvent,
    OutboundAccountPositionEvent,
)
from mctp.runtime.streams import (
    BinanceBnbTickerStreamChannel,
    BinanceBookTickerStreamChannel,
    BinanceKlineStreamChannel,
    MockUserDataStreamChannel,
    QueueStreamTransport,
    StreamChannel,
)
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.snapshot_store import SnapshotStore
from mctp.sizing.config import SizerConfig
from mctp.sizing.sizer import PositionSizer
from mctp.strategy import StrategyBase, StrategyInput, build_closed_mtf_candle_map_from_m15
from mctp.streams.base import StreamType, refresh_stale_flags


class PaperRuntimeStatus(Enum):
    RUNNING = "RUNNING"
    HALT = "HALT"
    STOPPED = "STOPPED"


@dataclass
class PaperRuntimeConfig:
    symbol: Symbol
    timeframe: Timeframe
    instrument_info: dict
    initial_balances: dict[str, Decimal]
    order_quantity: Decimal = PAPER_SMOKE_ORDER_QTY
    warmup_bars: int = 21
    sizer_config: SizerConfig = field(
        default_factory=lambda: SizerConfig(risk_pct=BASE_RISK_PCT, use_early_risk_cap=True)
    )
    ping_interval_seconds: int = STREAM_PING_SECONDS
    heartbeat_interval_seconds: int = PAPER_RUNTIME_HEARTBEAT_SECONDS
    stale_thresholds_seconds: dict[StreamType, int] = field(
        default_factory=lambda: {
            StreamType.KLINE: KLINE_STALE_SECONDS,
            StreamType.BOOK_TICKER: BOOK_TICKER_STALE_SECONDS,
            StreamType.BNB_TICKER: BNB_TICKER_STALE_SECONDS,
            StreamType.USER_DATA: USER_DATA_STALE_SECONDS,
        }
    )


class PaperRuntime:
    def __init__(
        self,
        config: PaperRuntimeConfig,
        strategy: StrategyBase,
        snapshot_store: SnapshotStore,
        balance_cache_store: BalanceCacheStore,
        accounting_store: AccountingStore,
        risk_layer: Optional[RiskLayer] = None,
        indicator_engine: Optional[IndicatorEngine] = None,
        kline_transport: Optional[QueueStreamTransport] = None,
        book_transport: Optional[QueueStreamTransport] = None,
        bnb_transport: Optional[QueueStreamTransport] = None,
        user_transport: Optional[QueueStreamTransport] = None,
    ) -> None:
        self.config = config
        self.strategy = strategy
        self.indicator_engine = indicator_engine or IndicatorEngine()
        self.risk_layer = risk_layer or RiskLayer(RiskConfig())
        initial_equity = config.initial_balances.get(config.symbol.quote, Decimal("0"))
        self.adaptive_risk = AdaptiveRiskController(initial_equity=initial_equity, is_live=True)
        initial_held_qty = config.initial_balances.get(config.symbol.base, Decimal("0"))
        self.position_sizer = PositionSizer(config.sizer_config)
        self.snapshot_store = snapshot_store
        self.balance_cache_store = balance_cache_store
        self.accounting_store = accounting_store
        initial_snapshot = PortfolioSnapshot(
            symbol=config.symbol,
            held_qty=initial_held_qty,
            avg_cost_basis=Decimal("0"),
            free_quote=config.initial_balances.get(config.symbol.quote, Decimal("0")),
            quote_asset=config.symbol.quote,
            is_in_position=initial_held_qty > Decimal("0"),
            meaningful_position=is_meaningful_position(initial_held_qty, self._lot_size()),
            timestamp=datetime.now(timezone.utc),
        )
        self.executor = SpotPaperExecutor(
            initial_balances=dict(config.initial_balances),
            snapshot_store=snapshot_store,
            balance_cache_store=balance_cache_store,
        )
        self.portfolio = PortfolioTracker(
            initial_snapshot=initial_snapshot,
            equity_tracker=None,
            snapshot_store=snapshot_store,
            bnb_price_provider=lambda: self.current_bnb_price,
            lot_size_provider=self._lot_size,
        )
        self.channels: dict[StreamType, StreamChannel] = {
            StreamType.KLINE: BinanceKlineStreamChannel(config.symbol, config.timeframe, kline_transport or QueueStreamTransport()),
            StreamType.BOOK_TICKER: BinanceBookTickerStreamChannel(config.symbol, book_transport or QueueStreamTransport()),
            StreamType.BNB_TICKER: BinanceBnbTickerStreamChannel(bnb_transport or QueueStreamTransport()),
            StreamType.USER_DATA: MockUserDataStreamChannel(user_transport or QueueStreamTransport()),
        }
        self.status = PaperRuntimeStatus.RUNNING
        self.candles: dict[Timeframe, list[Candle]] = {config.timeframe: []}
        self.last_strategy_input: Optional[StrategyInput] = None
        self.last_intent = None
        self.strategy_call_count = 0
        self.current_bid: Optional[Decimal] = None
        self.current_ask: Optional[Decimal] = None
        self.current_bnb_price: Optional[Decimal] = None
        self.handled_fills = []
        self.submitted_order_quantities: list[Decimal] = []
        self.transient_events: list[object] = []
        self.shutdown_steps: list[str] = []
        self.last_stale_check_at: Optional[datetime] = None
        self.current_runtime_time = initial_snapshot.timestamp
        self._heartbeat_task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        for channel in self.channels.values():
            await channel.connect()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def ping_all(self, now: Optional[datetime] = None) -> None:
        for channel in self.channels.values():
            await channel.ping(now)
            await channel.pong(now)

    async def process_all_available(self) -> None:
        processed = True
        while processed:
            processed = False
            for stream_type in (
                StreamType.BOOK_TICKER,
                StreamType.BNB_TICKER,
                StreamType.KLINE,
                StreamType.USER_DATA,
            ):
                event = await self.channels[stream_type].receive()
                if event is None:
                    continue
                processed = True
                self.transient_events.append(event)
                await self._dispatch(stream_type, event)
                await self._runtime_stale_checkpoint(event)

    async def evaluate_staleness(self, now: datetime) -> None:
        flags = refresh_stale_flags(
            {stream_type: channel.state for stream_type, channel in self.channels.items()},
            self.config.stale_thresholds_seconds,
            now,
        )
        self.last_stale_check_at = now
        effective_flags = {
            StreamType.KLINE: flags.kline and self.channels[StreamType.KLINE].state.last_update_at is not None,
            StreamType.BOOK_TICKER: (
                flags.book_ticker and self.channels[StreamType.BOOK_TICKER].state.last_update_at is not None
            ),
            StreamType.BNB_TICKER: (
                flags.bnb_ticker and self.channels[StreamType.BNB_TICKER].state.last_update_at is not None
            ),
            StreamType.USER_DATA: (
                flags.user_data and self.channels[StreamType.USER_DATA].state.last_update_at is not None
            ),
        }
        self.channels[StreamType.KLINE].state.is_stale = effective_flags[StreamType.KLINE]
        self.channels[StreamType.BOOK_TICKER].state.is_stale = effective_flags[StreamType.BOOK_TICKER]
        self.channels[StreamType.BNB_TICKER].state.is_stale = effective_flags[StreamType.BNB_TICKER]
        self.channels[StreamType.USER_DATA].state.is_stale = effective_flags[StreamType.USER_DATA]
        if effective_flags[StreamType.KLINE]:
            self.status = PaperRuntimeStatus.HALT

    async def tick(self, now: Optional[datetime] = None) -> None:
        checkpoint = now if now is not None else datetime.now(timezone.utc)
        self.current_runtime_time = checkpoint
        await self.evaluate_staleness(checkpoint)

    async def shutdown(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
        self.shutdown_steps.append("persist_financial_truth")
        await self._persist_financial_truth()
        self.shutdown_steps.append("clear_transient_state")
        self.transient_events.clear()
        self.handled_fills.clear()
        for channel in self.channels.values():
            await channel.disconnect()
        self.status = PaperRuntimeStatus.STOPPED

    async def _dispatch(self, stream_type: StreamType, event: object) -> None:
        if stream_type == StreamType.KLINE:
            await self._handle_kline(event)
            return
        if stream_type == StreamType.BOOK_TICKER:
            await self._handle_book_ticker(event)
            return
        if stream_type == StreamType.BNB_TICKER:
            await self._handle_bnb_ticker(event)
            return
        await self._handle_user_data(event)

    async def _runtime_stale_checkpoint(self, event: object) -> None:
        timestamp = self._event_timestamp(event)
        if timestamp is None:
            return
        self.current_runtime_time = timestamp
        await self.evaluate_staleness(timestamp)

    async def _handle_kline(self, event: object) -> None:
        if not isinstance(event, KlineEvent):
            raise AssertionError("KLINE channel must receive KlineEvent")
        self.channels[StreamType.KLINE].touch(event.candle.timestamp)
        if not event.candle.closed:
            return
        history = self.candles.setdefault(event.timeframe, [])
        history.append(event.candle)
        if self.adaptive_risk.should_reset_daily(event.candle.timestamp):
            self.adaptive_risk.reset_daily(
                equity=self._current_equity(mark_price=event.candle.close),
                now=event.candle.timestamp,
            )
        if len(history) < self.config.warmup_bars:
            return
        indicators = self._build_indicators(history)
        snapshot = indicators["snapshot"]
        self.adaptive_risk.update_atr_context(snapshot.atr, event.candle.close)
        strategy_input = StrategyInput(
            snapshot=self.portfolio.snapshot,
            indicators=indicators,
            candles=self._build_strategy_candles(event.timeframe, history),
            onchain=None,
        )
        self.last_strategy_input = strategy_input
        self.strategy_call_count += 1
        intent = self.strategy.on_candle(strategy_input)
        self.last_intent = intent
        if self.status != PaperRuntimeStatus.RUNNING:
            return
        await self._execute_intent(intent, event.candle.timestamp)

    async def _handle_book_ticker(self, event: object) -> None:
        if not isinstance(event, BookTickerEvent):
            raise AssertionError("BOOK_TICKER channel must receive BookTickerEvent")
        self.channels[StreamType.BOOK_TICKER].touch(event.timestamp)
        self.current_bid = event.bid
        self.current_ask = event.ask

    async def _handle_bnb_ticker(self, event: object) -> None:
        if not isinstance(event, BnbTickerEvent):
            raise AssertionError("BNB_TICKER channel must receive BnbTickerEvent")
        self.channels[StreamType.BNB_TICKER].touch(event.timestamp)
        self.current_bnb_price = event.price

    async def _handle_user_data(self, event: object) -> None:
        channel = self.channels[StreamType.USER_DATA]
        if isinstance(event, MockExecutionReportEvent):
            channel.touch(event.fill.filled_at)
            pnl = None
            if event.fill.side == Side.SELL:
                pnl = self.portfolio.realized_pnl(event.fill)
            self.portfolio.on_fill(event.fill)
            self.handled_fills.append(event.fill)
            if pnl is not None:
                self.adaptive_risk.on_trade_result(
                    pnl.net_pnl,
                    self._current_equity(mark_price=self.current_bid or event.fill.fill_price),
                    now=event.fill.filled_at,
                )
            return
        if isinstance(event, OutboundAccountPositionEvent):
            channel.touch(event.timestamp)
            self.balance_cache_store.save(event.balances, event.timestamp)
            return
        raise AssertionError("USER_DATA channel received unknown event")

    @staticmethod
    def _event_timestamp(event: object) -> Optional[datetime]:
        if isinstance(event, KlineEvent):
            return event.candle.timestamp
        if isinstance(event, BookTickerEvent):
            return event.timestamp
        if isinstance(event, BnbTickerEvent):
            return event.timestamp
        if isinstance(event, MockExecutionReportEvent):
            return event.fill.filled_at
        if isinstance(event, OutboundAccountPositionEvent):
            return event.timestamp
        return None

    def _build_indicators(self, candles: list[Candle]) -> dict[str, object]:
        snapshot = self.indicator_engine.snapshot(candles, ema_period=9, atr_period=14)
        return {
            "ema_9": self.indicator_engine.ema(candles, 9),
            "ema_21": self.indicator_engine.ema(candles, 21),
            "snapshot": snapshot,
        }

    @staticmethod
    def _build_strategy_candles(timeframe: Timeframe, candles: list[Candle]) -> dict[Timeframe, list[Candle]]:
        if timeframe == Timeframe.M15:
            return build_closed_mtf_candle_map_from_m15(candles)
        return {timeframe: list(candles)}

    async def _execute_intent(self, intent, timestamp: datetime) -> None:
        if intent.type == IntentType.HOLD:
            return
        if self._entry_blocked(intent):
            return
        risk_result = self.risk_layer.check(intent, self.portfolio.snapshot, self.config.instrument_info)
        if not risk_result.approved:
            return
        order = self._order_from_intent(intent, timestamp)
        if order is None:
            return
        self.current_runtime_time = timestamp
        self.executor.set_event_time(timestamp)
        execution_price = self._price_for_intent(intent)
        self.executor.set_price(self.config.symbol, execution_price)
        result = await self.executor.submit_order(order)
        if result != ExecutionResult.FILLED:
            return
        fills = await self.executor.get_fills(order.client_order_id)
        balances = await self.executor.get_balances()
        for fill in fills:
            await self.channels[StreamType.USER_DATA].publish(MockExecutionReportEvent(fill=fill))
        await self.channels[StreamType.USER_DATA].publish(
            OutboundAccountPositionEvent(timestamp=timestamp, balances=balances)
        )

    async def _heartbeat_loop(self) -> None:
        interval = self.config.heartbeat_interval_seconds
        while True:
            await asyncio.sleep(interval)
            checkpoint = self.current_runtime_time + timedelta(seconds=interval)
            await self.tick(checkpoint)

    def _price_for_intent(self, intent) -> Decimal:
        if intent.type == IntentType.BUY and self.current_ask is not None:
            return self.current_ask
        if intent.type == IntentType.SELL and self.current_bid is not None:
            return self.current_bid
        if self.current_ask is not None and self.current_bid is not None:
            return (self.current_ask + self.current_bid) / Decimal("2")
        raise ValueError("book ticker price is required before execution")

    def _order_from_intent(self, intent, timestamp: datetime) -> Optional[Order]:
        if intent.type == IntentType.BUY:
            quantity = self._buy_quantity()
            if quantity is None:
                return None
            side = Side.BUY
        elif intent.type == IntentType.SELL:
            quantity = self.portfolio.snapshot.held_qty
            if quantity <= Decimal("0"):
                return None
            side = Side.SELL
        else:
            return None
        order = Order(
            symbol=self.config.symbol,
            side=side,
            order_type=OrderType.MARKET,
            quantity=quantity,
            created_at=timestamp,
            reason=intent.reason,
        )
        self.submitted_order_quantities.append(quantity)
        return order

    def _buy_quantity(self) -> Optional[Decimal]:
        price = self.current_ask
        candles = self.candles.get(self.config.timeframe, [])
        if price is None:
            return None
        if candles:
            snapshot = self.indicator_engine.snapshot(candles, ema_period=9, atr_period=14)
            atr_value = snapshot.atr
            if atr_value is not None and atr_value > Decimal("0"):
                stop_distance_pct = atr_value / price
                sizer_result = self.position_sizer.calculate(
                    snapshot=self.portfolio.snapshot,
                    stop_distance_pct=stop_distance_pct,
                    instrument_info=self.config.instrument_info,
                    current_price=price,
                    risk_multipliers=self.adaptive_risk.get_risk_multipliers(),
                )
                if not sizer_result.approved or sizer_result.quantity is None:
                    return None
                quantity = sizer_result.quantity
            else:
                quantity = self.config.order_quantity
        else:
            quantity = self.config.order_quantity
        affordable = self.portfolio.snapshot.free_quote / price
        if quantity > affordable:
            quantity = self._quantized_quantity(affordable)
        if quantity <= Decimal("0"):
            return None
        return quantity

    def _quantized_quantity(self, raw_quantity: Decimal) -> Decimal:
        lot_size = self.config.instrument_info.get("lot_size", Decimal("0"))
        if not isinstance(lot_size, Decimal):
            lot_size = Decimal(str(lot_size))
        if lot_size <= Decimal("0"):
            return raw_quantity
        return (raw_quantity // lot_size) * lot_size

    def _current_equity(self, mark_price: Decimal) -> Decimal:
        return self.portfolio.snapshot.free_quote + (self.portfolio.snapshot.held_qty * mark_price)

    def _entry_blocked(self, intent) -> bool:
        if intent.type != IntentType.BUY:
            return False
        return self.adaptive_risk.operational_mode in {
            OperationalMode.PAUSE_NEW_ENTRIES,
            OperationalMode.CLOSE_ONLY,
            OperationalMode.STOP,
        }

    def _lot_size(self) -> Optional[Decimal]:
        lot_size = self.config.instrument_info.get("lot_size")
        if lot_size is None:
            return None
        if isinstance(lot_size, Decimal):
            return lot_size
        return Decimal(str(lot_size))

    async def _persist_financial_truth(self) -> None:
        self.snapshot_store.save(self.portfolio.snapshot)
        balances = await self.executor.get_balances()
        self.balance_cache_store.save(balances, self.current_runtime_time)
        self.accounting_store.save(self.portfolio.accounting.fill_history)
