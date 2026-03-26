import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Optional

from mctp.adapters import BinanceDelistingDetectorV1, BinanceSpotTestnetAdapterV1
from mctp.core.constants import (
    ASSET_BNB,
    BASE_RISK_PCT,
    BNB_NEAR_ZERO_THRESHOLD,
    BNB_TICKER_STALE_SECONDS,
    BOOK_TICKER_STALE_SECONDS,
    CLOCK_DRIFT_INFO_SECONDS,
    CRITICAL_EXTERNAL_OCO_CANCEL_CODE,
    CRITICAL_DRAWDOWN_STOP_CODE,
    CRITICAL_HEARTBEAT_TIMEOUT_CODE,
    CRITICAL_IP_BAN_CODE,
    CRITICAL_MISSING_BASIS_CODE,
    CRITICAL_BACKGROUND_TASK_FAILURE_CODE,
    CRITICAL_DIRECT_SELL_OCO_CANCEL_FAILED_CODE,
    CRITICAL_RESTART_OUTSTANDING_ORDER_CODE,
    CRITICAL_RESTART_PARTIAL_FILL_CODE,
    CRITICAL_RUNTIME_CRASH_CODE,
    CRITICAL_STORAGE_UNAVAILABLE_CODE,
    CRITICAL_STARTUP_OCO_AMBIGUITY_CODE,
    CRITICAL_STARTUP_PROTECTION_CONFLICT_CODE,
    DAILY_LOSS_LIMIT_PCT,
    DELISTING_FORCE_EXIT_REASON,
    EXCHANGE_LIST_STATUS_ALL_DONE,
    EXCHANGE_ORDER_STATUS_REJECTED,
    HEARTBEAT_TIMEOUT_SECONDS,
    INFO_CLOCK_DRIFT_CODE,
    INFO_DELISTING_ANNOUNCED_CODE,
    INFO_POST_ONLY_REJECTED_CODE,
    KLINE_STALE_SECONDS,
    MAX_DRAWDOWN_STOP_PCT,
    MAX_DRAWDOWN_WARNING_PCT,
    MAX_SLIPPAGE_PCT,
    MISSING_BASIS_CLOSE_REASON,
    N_DELIST,
    PAPER_RUNTIME_HEARTBEAT_SECONDS,
    STARTUP_BOOK_BID_TIMEOUT_SECONDS,
    STREAM_PING_SECONDS,
    STARTUP_GAP_RISK_SELL_REASON,
    SYMBOL_CHANGE_SELL_REASON,
    T_CANCEL,
    RISK_REDUCTION_MULTIPLIER,
    USER_STREAM_KEEPALIVE_SECONDS,
    USER_DATA_STALE_SECONDS,
    CONSECUTIVE_LOSSES_REDUCE,
    WARNING_BNB_NEAR_ZERO_CODE,
    WARNING_CONSECUTIVE_LOSSES_CODE,
    WARNING_DAILY_LOSS_LIMIT_CODE,
    WARNING_DRAWDOWN_CODE,
    WARNING_MANUAL_TRADE_DETECTED_CODE,
    WARNING_PERSISTENT_DUST_CODE,
    WARNING_REGIME_UNKNOWN_CODE,
    WARNING_STRATEGY_DEGRADATION_CODE,
    WARNING_ZERO_BASIS_CODE,
)
from mctp.core.enums import AlertSeverity, BasisRecoveryState, ExecutionResult, IntentType, OperationalMode, OrderType, ProtectionMode, RecoveryMode, Side, SymbolChangeStage, Timeframe
from mctp.core.order import Order
from mctp.core.types import PortfolioSnapshot, Symbol
from mctp.execution.oco import OCOOrder
from mctp.indicators import IndicatorEngine
from mctp.indicators.models import Candle
from mctp.portfolio.meaningful import is_meaningful_position
from mctp.portfolio.tracker import PortfolioTracker
from mctp.portfolio.updater import CostBasisUpdater
from mctp.risk.adaptive import AdaptiveRiskController
from mctp.risk.config import RiskConfig
from mctp.risk.layer import RiskLayer
from mctp.runtime.alerting import AlertDispatcher, JsonFileAlertChannel, MemoryAlertChannel
from mctp.runtime.events import (
    BnbTickerEvent,
    BookTickerEvent,
    ExecutionReportEvent,
    KlineEvent,
    OCOListStatusEvent,
    OutboundAccountPositionEvent,
    RuntimeAlertEvent,
)
from mctp.runtime.observability import ObservabilityHub
from mctp.runtime.safety import RecoveryModeController
from mctp.runtime.streams import WebSocketJsonTransport
from mctp.runtime.testnet_adapters import adapt_binance_testnet_payload
from mctp.runtime.testnet_exchange_boundary import (
    is_external_oco_cancellation as boundary_is_external_oco_cancellation,
    is_terminal_exchange_order_status,
)
from mctp.runtime.testnet_recovery import TestnetRecoveryHelper
from mctp.runtime.testnet_safety_state import TestnetSafetyStateHelper
from mctp.runtime.testnet_stream_health import TestnetStreamHealthHelper
from mctp.runtime.testnet_streams import (
    BinanceSpotTestnetBnbTickerChannel,
    BinanceSpotTestnetBookTickerChannel,
    BinanceSpotTestnetKlineChannel,
    BinanceSpotTestnetUserDataChannel,
    ReconnectableStreamChannel,
)
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.snapshot_store import SnapshotStore
from mctp.sizing.config import SizerConfig
from mctp.sizing.models import RiskMultipliers
from mctp.sizing.sizer import PositionSizer
from mctp.strategy import StrategyBase, StrategyInput
from mctp.streams.base import StreamType
from mctp.runtime.mtf_kline_manager import MtfKlineManager, MTF_TIMEFRAMES
from mctp.runtime.testnet_trade_flow import TestnetTradeFlowHelper
from mctp.strategy.mtf_live import LiveMtfAggregator

_logger = logging.getLogger(__name__)


class TestnetRuntimeStatus(Enum):
    STARTING = "STARTING"
    READY = "READY"
    HALT = "HALT"
    STOPPED = "STOPPED"


@dataclass
class TestnetRuntimeConfig:
    __test__ = False

    symbol: Symbol
    timeframe: Timeframe
    instrument_info: dict
    initial_balances: dict[str, Decimal]
    warmup_bars: int = 21
    ping_interval_seconds: int = STREAM_PING_SECONDS
    heartbeat_interval_seconds: int = PAPER_RUNTIME_HEARTBEAT_SECONDS
    heartbeat_watchdog_interval_seconds: int = PAPER_RUNTIME_HEARTBEAT_SECONDS
    listen_key_keepalive_seconds: int = USER_STREAM_KEEPALIVE_SECONDS
    structured_log_path: Optional[str] = None
    audit_log_path: Optional[str] = None
    primary_alert_path: Optional[str] = None
    backup_alert_path: Optional[str] = None
    startup_software_trailing_active: bool = False
    startup_software_stop_active: bool = False
    startup_stop_price: Optional[Decimal] = None
    stale_thresholds_seconds: dict[StreamType, int] = field(
        default_factory=lambda: {
            StreamType.KLINE: KLINE_STALE_SECONDS,
            StreamType.BOOK_TICKER: BOOK_TICKER_STALE_SECONDS,
            StreamType.BNB_TICKER: BNB_TICKER_STALE_SECONDS,
            StreamType.USER_DATA: USER_DATA_STALE_SECONDS,
        }
    )
    sizer_config: SizerConfig = field(default_factory=lambda: SizerConfig(risk_pct=BASE_RISK_PCT))


class TestnetRuntime:
    __test__ = False

    def __init__(
        self,
        config: TestnetRuntimeConfig,
        strategy: StrategyBase,
        executor: BinanceSpotTestnetAdapterV1,
        snapshot_store: SnapshotStore,
        balance_cache_store: BalanceCacheStore,
        accounting_store: AccountingStore,
        detector: Optional[BinanceDelistingDetectorV1] = None,
        indicator_engine: Optional[IndicatorEngine] = None,
        kline_transport: Optional[Any] = None,
        book_transport: Optional[Any] = None,
        bnb_transport: Optional[Any] = None,
        user_transport: Optional[Any] = None,
        observability: Optional[ObservabilityHub] = None,
        alert_dispatcher: Optional[AlertDispatcher] = None,
        mtf_kline_transports: Optional[dict[Timeframe, Any]] = None,
    ) -> None:
        self.config = config
        self.strategy = strategy
        self.executor = executor
        self.detector = detector
        self.snapshot_store = snapshot_store
        self.balance_cache_store = balance_cache_store
        self.accounting_store = accounting_store
        self.observability = observability or ObservabilityHub(
            structured_log_path=config.structured_log_path,
            audit_log_path=config.audit_log_path,
        )
        self.alert_dispatcher = alert_dispatcher or AlertDispatcher(
            JsonFileAlertChannel(config.primary_alert_path, "primary")
            if config.primary_alert_path is not None
            else MemoryAlertChannel("primary"),
            JsonFileAlertChannel(config.backup_alert_path, "backup")
            if config.backup_alert_path is not None
            else MemoryAlertChannel("backup"),
        )
        self.indicator_engine = indicator_engine or IndicatorEngine()
        self.risk_layer = RiskLayer(RiskConfig())
        self.adaptive_risk = AdaptiveRiskController(
            initial_equity=config.initial_balances.get(config.symbol.quote, Decimal("0")),
            is_live=True,
        )
        initial_held_qty = config.initial_balances.get(config.symbol.base, Decimal("0"))
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
        self.portfolio = PortfolioTracker(
            initial_snapshot=initial_snapshot,
            equity_tracker=None,
            snapshot_store=snapshot_store,
            bnb_price_provider=lambda: self.current_bnb_price,
            lot_size_provider=self._lot_size,
        )
        self.position_sizer = PositionSizer(config.sizer_config)
        self.candles: dict[Timeframe, list[Candle]] = {config.timeframe: []}
        self.status = TestnetRuntimeStatus.STARTING
        self.current_bid: Optional[Decimal] = None
        self.current_ask: Optional[Decimal] = None
        self.current_bnb_price: Optional[Decimal] = None
        self.current_runtime_time = initial_snapshot.timestamp
        self.last_strategy_input: Optional[StrategyInput] = None
        self.last_intent = None
        self.strategy_call_count = 0
        self._heartbeat_task: Optional[asyncio.Task[None]] = None
        self._heartbeat_watchdog_task: Optional[asyncio.Task[None]] = None
        self._shutting_down = False
        self._kline_transport = kline_transport or WebSocketJsonTransport()
        self._book_transport = book_transport or WebSocketJsonTransport()
        self._bnb_transport = bnb_transport or WebSocketJsonTransport()
        self._user_transport = user_transport or WebSocketJsonTransport()
        self.mtf_aggregator = LiveMtfAggregator()
        self.mtf_kline_manager = MtfKlineManager(
            symbol=config.symbol,
            aggregator=self.mtf_aggregator,
            kline_transports=mtf_kline_transports or {},
            rest_client=getattr(executor, "_rest_client", None),
            primary_kline_transport=self._kline_transport,
        )
        self.channels: dict[StreamType, ReconnectableStreamChannel] = {}
        self.last_delisting_signal = None
        self.protection_mode = ProtectionMode.NONE
        self.software_stop_active = False
        self.active_oco_order_id: Optional[str] = None
        self.last_alert: Optional[RuntimeAlertEvent] = None
        self.pending_symbol_change: Optional[Symbol] = None
        self.symbol_change_stage = SymbolChangeStage.IDLE
        self.restart_required = False
        self.startup_checks_completed = False
        self.basis_recovery_state = BasisRecoveryState.NONE
        self.zero_basis_buy_blocked = False
        self.manual_trade_detected = False
        self.manual_trade_prompt_required = False
        self.reconciliation_runs = 0
        self.last_reconciliation_applied_bnb_rate: Optional[Decimal] = None
        self.last_cancel_code: Optional[int] = None
        self._restart_state_loaded = False
        self._startup_previous_snapshot: Optional[PortfolioSnapshot] = None
        self.last_heartbeat_at: Optional[datetime] = None
        self._heartbeat_timeout_active = False
        self.operational_mode = OperationalMode.RUN
        self.recovery_mode_controller = RecoveryModeController(mode=RecoveryMode.NORMAL, live_activation_enabled=False)
        self._peak_equity = initial_snapshot.free_quote
        self._drawdown_warning_active = False
        self._drawdown_stop_active = False
        self._drawdown_loss_mult = Decimal("1")
        self._manual_resume_required = False
        self._bnb_guard_active = False
        self._daily_loss_pause_alert_active = False
        self._regime_unknown = False
        self._regime_unknown_alert_active = False
        self._regime_mult_override = Decimal("1")
        self._anomaly_mult_override = Decimal("1")
        self._delisting_close_only_active = False
        self._delisting_sell_submitted = False
        self.pending_order_client_id: Optional[str] = None
        self.pending_order_side: Optional[Side] = None
        self._last_balance_truth_anchor_snapshot: Optional[PortfolioSnapshot] = None
        self._reconciliation_fill_anchor_snapshot: Optional[PortfolioSnapshot] = None
        self._user_data_stale_fail_safe_active = False
        self._status_enum = TestnetRuntimeStatus
        self._symbol_change_stage_enum = SymbolChangeStage
        self._recovery_helper = TestnetRecoveryHelper(self)
        self._stream_health_helper = TestnetStreamHealthHelper(self)
        self._trade_flow_helper = TestnetTradeFlowHelper(self)
        self._safety_state_helper = TestnetSafetyStateHelper(self)

    def _spawn_critical_background_task(self, name: str, coroutine: Any) -> asyncio.Task[None]:
        task = asyncio.create_task(coroutine)
        task.add_done_callback(lambda completed, task_name=name: self._handle_background_task_completion(task_name, completed))
        return task

    def _handle_background_task_completion(self, name: str, task: asyncio.Task[None]) -> None:
        if self._shutting_down:
            return
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            self._handle_runtime_exception(
                RuntimeError(f"critical background task exited unexpectedly: {name}"),
                code=CRITICAL_BACKGROUND_TASK_FAILURE_CODE,
            )
        else:
            self._handle_runtime_exception(
                RuntimeError(f"critical background task failed: {name}: {exc}"),
                code=CRITICAL_BACKGROUND_TASK_FAILURE_CODE,
            )
        for background_task in (self._heartbeat_task, self._heartbeat_watchdog_task):
            if background_task is None or background_task is task or background_task.done():
                continue
            background_task.cancel()
        self._emit_runtime_event("background_task_failed", audit=True)

    def _set_pending_order(self, order: Order) -> None:
        self.pending_order_client_id = order.client_order_id
        self.pending_order_side = order.side

    def _clear_pending_order(self, client_order_id: Optional[str] = None) -> None:
        if client_order_id is not None and self.pending_order_client_id != client_order_id:
            return
        self.pending_order_client_id = None
        self.pending_order_side = None

    def _has_pending_order(self) -> bool:
        return self.pending_order_client_id is not None

    @staticmethod
    def _is_terminal_exchange_order_status(status: str) -> bool:
        return is_terminal_exchange_order_status(status)

    async def _ensure_no_active_oco_before_direct_sell(self, timestamp: datetime) -> bool:
        if self.active_oco_order_id is None:
            return True
        self._emit_runtime_event("direct_sell_requires_oco_cancel", audit=True)
        active_oco_id = self.active_oco_order_id
        try:
            cancel_result = await self.executor.cancel_oco(active_oco_id)
        except Exception as exc:
            if active_oco_id is not None and await self._resolve_non_cancelled_oco_state(active_oco_id, timestamp):
                self._emit_runtime_event("direct_sell_oco_cancel_exception_resolved", audit=True)
                return self.portfolio.snapshot.held_qty > Decimal("0")
            self._raise_alert(
                AlertSeverity.CRITICAL,
                CRITICAL_DIRECT_SELL_OCO_CANCEL_FAILED_CODE,
                "Active exchange OCO could not be cancelled before direct SELL",
                timestamp=timestamp,
            )
            self._handle_runtime_exception(exc, code=CRITICAL_DIRECT_SELL_OCO_CANCEL_FAILED_CODE)
            return False
        if cancel_result != ExecutionResult.CANCELLED:
            if active_oco_id is not None and await self._resolve_non_cancelled_oco_state(active_oco_id, timestamp):
                return self.portfolio.snapshot.held_qty > Decimal("0")
            self._raise_alert(
                AlertSeverity.CRITICAL,
                CRITICAL_DIRECT_SELL_OCO_CANCEL_FAILED_CODE,
                "Active exchange OCO could not be cancelled before direct SELL",
                timestamp=timestamp,
            )
            self._emit_runtime_event("direct_sell_oco_cancel_failed", audit=True)
            self.status = TestnetRuntimeStatus.HALT
            return False
        cancelled_oco_id = active_oco_id
        self.active_oco_order_id = None
        self.protection_mode = ProtectionMode.NONE
        self.software_stop_active = False
        if cancelled_oco_id is not None:
            self.executor.remove_local_oco(cancelled_oco_id)
        self._emit_runtime_event("direct_sell_oco_cancelled", audit=True)
        return True

    async def _resolve_non_cancelled_oco_state(self, list_order_id: str, timestamp: datetime) -> bool:
        exchange_open_oco_ids = set(await self.executor.get_open_oco_order_ids(self.config.symbol))
        if list_order_id in exchange_open_oco_ids:
            return False
        local_oco = self.executor.load_local_active_ocos().get(list_order_id)
        resolved_by_fill = False
        if local_oco is not None:
            resolved = await self._resolve_filled_oco_leg(local_oco)
            if resolved is not None:
                fills, _ = resolved
                self.executor.remember_exchange_fills(fills[0].order_id, fills)
                self._apply_reconciled_fills(fills, self._load_cached_bnb_rate())
                resolved_by_fill = True
        self.active_oco_order_id = None
        self.protection_mode = ProtectionMode.NONE
        self.software_stop_active = False
        self.executor.remove_local_oco(list_order_id)
        if resolved_by_fill:
            refresh_record = await self.executor.refresh_account_snapshot("ttl")
            self._apply_exchange_balance_truth(
                refresh_record.balances,
                refresh_record.locked_balances,
                refresh_record.fetched_at,
            )
            self._emit_runtime_event("direct_sell_oco_already_filled", audit=True)
        else:
            self._emit_runtime_event("direct_sell_oco_already_terminal", audit=True)
        self._evaluate_safety_controls(timestamp)
        return True

    async def start(self) -> None:
        self._emit_runtime_event("runtime_starting", audit=True)
        self._hydrate_restart_state()
        previous_snapshot = self.portfolio.snapshot
        self._startup_previous_snapshot = previous_snapshot
        try:
            startup_record = await self.executor.refresh_account_snapshot("startup")
        except Exception as exc:
            self._handle_runtime_exception(exc)
            raise
        self._apply_exchange_balance_truth(
            startup_record.balances,
            startup_record.locked_balances,
            startup_record.fetched_at,
        )
        await self._check_delisting()
        self.channels = {
            StreamType.KLINE: BinanceSpotTestnetKlineChannel(
                self.config.symbol,
                self.config.timeframe,
                self._kline_transport,
                lambda payload: adapt_binance_testnet_payload(StreamType.KLINE, payload, timeframe=self.config.timeframe),
            ),
            StreamType.BOOK_TICKER: BinanceSpotTestnetBookTickerChannel(
                self.config.symbol,
                self._book_transport,
                lambda payload: adapt_binance_testnet_payload(StreamType.BOOK_TICKER, payload),
            ),
            StreamType.BNB_TICKER: BinanceSpotTestnetBnbTickerChannel(
                self._bnb_transport,
                lambda payload: adapt_binance_testnet_payload(StreamType.BNB_TICKER, payload),
            ),
            StreamType.USER_DATA: BinanceSpotTestnetUserDataChannel(
                self.executor,
                self._user_transport,
                lambda payload: adapt_binance_testnet_payload(StreamType.USER_DATA, payload, symbol=self.config.symbol),
            ),
        }
        for channel in self.channels.values():
            await channel.connect()
        # MTF kline channels: independent lifecycle per timeframe
        self.mtf_kline_manager.build_channels()
        await self.mtf_kline_manager.connect_all()
        # REST priming: fetch historical klines for all 4 TF
        await self.mtf_kline_manager.prime_from_rest()
        self.last_heartbeat_at = datetime.now(timezone.utc)
        self._heartbeat_task = self._spawn_critical_background_task("heartbeat_loop", self._heartbeat_loop())
        self._heartbeat_watchdog_task = self._spawn_critical_background_task(
            "heartbeat_watchdog_loop",
            self._heartbeat_watchdog_loop(),
        )
        await self._run_startup_sync()
        self._evaluate_safety_controls(self.current_runtime_time)
        self.startup_checks_completed = True
        if self.status == TestnetRuntimeStatus.STARTING:
            if self._requires_mtf_warmup() and not self.mtf_aggregator.warmup_complete:
                _logger.warning(
                    "MTF warmup incomplete at startup; remaining in STARTING. "
                    "Candle counts: %s",
                    self.mtf_aggregator.candle_counts(),
                )
                self._emit_runtime_event("runtime_starting_warmup_pending", audit=True)
            else:
                self.status = TestnetRuntimeStatus.READY
                self._emit_runtime_event("runtime_ready", audit=True)

    async def shutdown(self) -> None:
        self._shutting_down = True
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._heartbeat_watchdog_task is not None:
            self._heartbeat_watchdog_task.cancel()
            try:
                await self._heartbeat_watchdog_task
            except (asyncio.CancelledError, Exception):
                pass
        for channel in self.channels.values():
            await channel.disconnect()
        await self.mtf_kline_manager.disconnect_all()
        self.status = TestnetRuntimeStatus.STOPPED
        self._emit_runtime_event("runtime_stopped", audit=True)

    async def ping_all(self, now: Optional[datetime] = None) -> None:
        for channel in self.channels.values():
            await channel.ping(now)
            await channel.pong(now)
        await self.mtf_kline_manager.ping_all(now)

    async def process_all_available(self) -> None:
        # Process MTF kline channels (independent per-TF lifecycle)
        try:
            mtf_events = await self.mtf_kline_manager.receive_and_process()
            for mtf_event in mtf_events:
                # Route M15 events to legacy kline handler for backward compat
                if mtf_event.timeframe == Timeframe.M15:
                    self.channels[StreamType.KLINE].touch(mtf_event.candle.timestamp)
                # Check warmup transition: if aggregator just became warm, transition to READY
                if (
                    (self.mtf_aggregator.warmup_complete or not self._requires_mtf_warmup())
                    and self.startup_checks_completed
                    and self.status == TestnetRuntimeStatus.STARTING
                ):
                    self.status = TestnetRuntimeStatus.READY
                    self._emit_runtime_event("mtf_warmup_complete", audit=True)
        except Exception as exc:
            self._handle_runtime_exception(exc)
            return

        processed = True
        processed_any = False
        while processed:
            processed = False
            reconnect_refresh_required = False
            for stream_type in (StreamType.BOOK_TICKER, StreamType.BNB_TICKER, StreamType.KLINE, StreamType.USER_DATA):
                event = await self.channels[stream_type].receive()
                channel = self.channels[stream_type]
                if channel.reconnect_count > 0:
                    reconnect_refresh_required = True
                    channel.reconnect_count = 0
                if event is None:
                    continue
                processed = True
                processed_any = True
                try:
                    await self._dispatch(stream_type, event)
                    await self._stale_checkpoint(event)
                except Exception as exc:
                    self._handle_runtime_exception(exc)
                    return
            if reconnect_refresh_required:
                previous_snapshot = self.portfolio.snapshot
                try:
                    reconnect_record = await self.executor.refresh_account_snapshot("reconnect")
                except Exception as exc:
                    self._handle_runtime_exception(exc)
                    raise
                self._apply_exchange_balance_truth(
                    reconnect_record.balances,
                    reconnect_record.locked_balances,
                    reconnect_record.fetched_at,
                )
                await self._run_restart_reconciliation(previous_snapshot, restart_reason="reconnect")
                await self._check_delisting()
            if not processed and not reconnect_refresh_required and not processed_any:
                ttl_record = await self.executor.refresh_account_snapshot_if_due()
                if ttl_record is not None:
                    self._apply_exchange_balance_truth(
                        ttl_record.balances,
                        ttl_record.locked_balances,
                        ttl_record.fetched_at,
                    )

    async def submit_oco(self, oco: OCOOrder, *, require_market_reference: bool = True) -> str:
        self._validate_oco_pre_submit(oco, require_market_reference=require_market_reference)
        order_list_id = await self.executor.submit_oco(oco)
        self.active_oco_order_id = order_list_id
        self.protection_mode = ProtectionMode.EXCHANGE_OCO
        self.software_stop_active = False
        return order_list_id

    async def evaluate_staleness(self, now: datetime, *, enforce_user_data_fail_safe: bool = True) -> None:
        await self._stream_health_helper.evaluate_staleness(
            now,
            enforce_user_data_fail_safe=enforce_user_data_fail_safe,
        )

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.heartbeat_interval_seconds)
            await self.emit_heartbeat_observability()

    async def _heartbeat_watchdog_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.heartbeat_watchdog_interval_seconds)
            self.check_heartbeat_timeout(datetime.now(timezone.utc))

    async def emit_heartbeat_observability(self) -> None:
        now = datetime.now(timezone.utc)
        await self.evaluate_staleness(now)
        self.mtf_kline_manager.evaluate_staleness(now)
        self.last_heartbeat_at = now
        self._heartbeat_timeout_active = False
        self._evaluate_safety_controls(now)
        self.observability.record_heartbeat(
            now,
            self.config.symbol.to_exchange_str(),
            self.status.value,
            self._stale_flags(),
        )
        self.observability.record_memory(
            now,
            self.config.symbol.to_exchange_str(),
        )

    async def _check_delisting(self) -> None:
        if self.detector is None:
            return
        self.last_delisting_signal = await self.detector.check_symbol(self.config.symbol)
        if self.last_delisting_signal is not None and not self.last_delisting_signal.listed:
            self._raise_alert(
                AlertSeverity.INFO,
                INFO_DELISTING_ANNOUNCED_CODE,
                self.last_delisting_signal.details,
            )
        days_until_delisting = (
            getattr(self.last_delisting_signal, "days_until_delisting", None)
            if self.last_delisting_signal is not None
            else None
        )
        delisting_window_active = (
            self.last_delisting_signal is not None
            and days_until_delisting is not None
            and days_until_delisting <= N_DELIST
        )
        self._delisting_close_only_active = delisting_window_active
        if not delisting_window_active:
            self._delisting_sell_submitted = False
        if (
            delisting_window_active
            and self.portfolio.snapshot.held_qty > Decimal("0")
            and not self._delisting_sell_submitted
        ):
            self._emit_runtime_event("delisting_force_exit_attempt", audit=True)
            if not await self._ensure_no_active_oco_before_direct_sell(self.current_runtime_time):
                self._delisting_sell_submitted = False
                self._emit_runtime_event("delisting_force_exit_failed", audit=True)
                self._evaluate_safety_controls(self.current_runtime_time)
                return
            order = Order(
                symbol=self.config.symbol,
                side=Side.SELL,
                order_type=OrderType.MARKET,
                quantity=self.portfolio.snapshot.held_qty,
                created_at=self.current_runtime_time,
                reason=DELISTING_FORCE_EXIT_REASON,
            )
            result = await self.executor.submit_order(order)
            if result in {ExecutionResult.ACCEPTED, ExecutionResult.PARTIAL_FILL, ExecutionResult.FILLED}:
                if result in {ExecutionResult.ACCEPTED, ExecutionResult.PARTIAL_FILL}:
                    self._set_pending_order(order)
                self._delisting_sell_submitted = True
                self._emit_runtime_event("delisting_force_exit", audit=True)
            else:
                self._clear_pending_order(order.client_order_id)
                self._delisting_sell_submitted = False
                self._emit_runtime_event("delisting_force_exit_failed", audit=True)
            self._evaluate_safety_controls(self.current_runtime_time)

    async def _dispatch(self, stream_type: StreamType, event: object) -> None:
        if stream_type == StreamType.KLINE:
            await self._handle_kline(event)
            return
        if stream_type == StreamType.BOOK_TICKER:
            await self._handle_book(event)
            return
        if stream_type == StreamType.BNB_TICKER:
            await self._handle_bnb(event)
            return
        await self._handle_user(event)

    async def _stale_checkpoint(self, event: object) -> None:
        await self._stream_health_helper.stale_checkpoint(event)

    async def _handle_kline(self, event: object) -> None:
        assert isinstance(event, KlineEvent)
        self.channels[StreamType.KLINE].touch(event.candle.timestamp)
        if not event.candle.closed:
            return
        history = self.candles.setdefault(event.timeframe, [])
        history.append(event.candle)
        # MTF aggregator is fed exclusively by MtfKlineManager — do NOT feed here
        if len(history) < self.config.warmup_bars:
            return
        # If any TF is stale, strategy must return HOLD (do not crash)
        if self.mtf_aggregator.any_stale:
            return
        if self._stream_health_helper.user_data_stream_is_stale_at(event.candle.timestamp):
            self._stream_health_helper.trigger_user_data_stale_fail_safe(event.candle.timestamp)
            return
        # Build full MTF candle map from aggregator
        mtf_candles = self.mtf_aggregator.build_strategy_candles()
        # Fallback: if MTF aggregator has no data, use legacy single-TF path
        if not mtf_candles.get(Timeframe.M15):
            mtf_candles = {event.timeframe: list(history)}
        indicators = {
            "ema_9": self.indicator_engine.ema(history, 9),
            "ema_21": self.indicator_engine.ema(history, 21),
            "snapshot": self.indicator_engine.snapshot(history, ema_period=9, atr_period=14),
        }
        self.last_strategy_input = StrategyInput(
            snapshot=self.portfolio.snapshot,
            indicators=indicators,
            candles=mtf_candles,
            onchain=None,
        )
        self.strategy_call_count += 1
        self.last_intent = self.strategy.on_candle(self.last_strategy_input)
        await self._execute_intent(self.last_intent, event.candle.timestamp)

    async def _handle_book(self, event: object) -> None:
        assert isinstance(event, BookTickerEvent)
        self.channels[StreamType.BOOK_TICKER].touch(event.timestamp)
        self.current_bid = event.bid
        self.current_ask = event.ask
        self._evaluate_safety_controls(event.timestamp)

    async def _handle_bnb(self, event: object) -> None:
        assert isinstance(event, BnbTickerEvent)
        self.channels[StreamType.BNB_TICKER].touch(event.timestamp)
        self.current_bnb_price = event.price
        self._evaluate_safety_controls(event.timestamp)

    async def _handle_user(self, event: object) -> None:
        if isinstance(event, OutboundAccountPositionEvent):
            self.channels[StreamType.USER_DATA].touch(event.timestamp)
            self._last_balance_truth_anchor_snapshot = self.portfolio.snapshot
            self.executor.handle_user_data_event(event)
            balances, locked_balances = self.executor.get_cached_balance_state()
            self._apply_exchange_balance_truth(balances, locked_balances, event.timestamp)
            self._evaluate_safety_controls(event.timestamp)
            return
        if isinstance(event, OCOListStatusEvent):
            self.channels[StreamType.USER_DATA].touch(event.timestamp)
            self._handle_oco_status(event)
            return
        assert isinstance(event, ExecutionReportEvent)
        self.channels[StreamType.USER_DATA].touch(event.timestamp)
        accepted_fill = self.executor.handle_user_data_event(event)
        if self._is_terminal_exchange_order_status(event.order_status):
            self._clear_pending_order(event.client_order_id)
        elif self.pending_order_client_id == event.client_order_id:
            self.pending_order_side = event.fill.side if event.fill is not None else self.pending_order_side
        if accepted_fill is not None:
            pnl = None
            if accepted_fill.side == Side.SELL:
                pnl = self.portfolio.realized_pnl(accepted_fill)
            if self._fill_already_reflected_in_exchange_truth(accepted_fill):
                self._record_fill_without_reapplying_exchange_balances(accepted_fill)
            else:
                self.portfolio.on_fill(accepted_fill)
            self._persist_accounting_history()
            if pnl is not None:
                mark_price = self.current_bid or accepted_fill.fill_price
                current_equity = self.portfolio.snapshot.free_quote + (self.portfolio.snapshot.held_qty * mark_price)
                self.adaptive_risk.on_trade_result(pnl.net_pnl, current_equity, now=accepted_fill.filled_at)
                self.observability.performance_monitor.observe_trade(pnl.net_pnl)
            self._update_symbol_change_progress(event.timestamp)
            self._evaluate_safety_controls(event.timestamp)
        self._last_balance_truth_anchor_snapshot = None

    async def _execute_intent(self, intent, timestamp: datetime) -> None:
        await self._trade_flow_helper.execute_intent(intent, timestamp)

    async def request_symbol_change(self, new_symbol: Symbol) -> None:
        await self._trade_flow_helper.request_symbol_change(new_symbol)

    def apply_symbol_change_config(self) -> None:
        self._trade_flow_helper.apply_symbol_change_config()

    def provide_manual_basis(self, basis: Decimal) -> None:
        self._trade_flow_helper.provide_manual_basis(basis)

    def apply_manual_trade_basis_adjustment(self, basis: Decimal) -> None:
        self._trade_flow_helper.apply_manual_trade_basis_adjustment(basis)

    def declare_zero_basis(self) -> None:
        self._trade_flow_helper.declare_zero_basis()

    def confirm_zero_basis_for_new_entries(self) -> None:
        self._trade_flow_helper.confirm_zero_basis_for_new_entries()

    async def request_missing_basis_immediate_close(self) -> None:
        await self._trade_flow_helper.request_missing_basis_immediate_close()

    def _order_quantity(self, intent) -> tuple[Optional[Decimal], Optional[Any]]:
        return self._trade_flow_helper.order_quantity(intent)

    def _requires_mtf_warmup(self) -> bool:
        """Check if the current strategy requires MTF warmup data."""
        return getattr(self.strategy, "requires_mtf_warmup", False)

    def _lot_size(self) -> Optional[Decimal]:
        lot_size = self.config.instrument_info.get("lot_size")
        if isinstance(lot_size, Decimal):
            return lot_size
        if lot_size is None:
            return None
        return Decimal(str(lot_size))

    def _is_meaningful_position(self, held_qty: Decimal) -> bool:
        return is_meaningful_position(held_qty, self._lot_size())

    def _apply_exchange_balance_truth(
        self,
        balances: dict[str, Decimal],
        locked_balances: dict[str, Decimal],
        timestamp: datetime,
    ) -> None:
        self._recovery_helper.apply_exchange_balance_truth(balances, locked_balances, timestamp)

    def _fill_already_reflected_in_exchange_truth(self, fill: Any) -> bool:
        anchor_snapshot = self._last_balance_truth_anchor_snapshot
        if anchor_snapshot is None:
            return False
        bnb_rate_at_fill = self._resolve_fill_bnb_rate(fill)
        projected_snapshot = CostBasisUpdater.apply_fill(
            anchor_snapshot,
            fill,
            bnb_rate_at_fill,
            self._lot_size(),
        )
        current_snapshot = self.portfolio.snapshot
        return (
            projected_snapshot.held_qty == current_snapshot.held_qty
            and projected_snapshot.free_quote == current_snapshot.free_quote
        )

    def _record_fill_without_reapplying_exchange_balances(
        self,
        fill: Any,
        *,
        anchor_snapshot: Optional[PortfolioSnapshot] = None,
    ) -> PortfolioSnapshot:
        bnb_rate_at_fill = self._resolve_fill_bnb_rate(fill)
        projected_snapshot = CostBasisUpdater.apply_fill(
            anchor_snapshot or self._last_balance_truth_anchor_snapshot or self.portfolio.snapshot,
            fill,
            bnb_rate_at_fill,
            self._lot_size(),
        )
        self.portfolio.accounting.record_fill(fill, bnb_rate_at_fill)
        self.portfolio.replace_snapshot(
            avg_cost_basis=projected_snapshot.avg_cost_basis,
            is_in_position=projected_snapshot.is_in_position,
            meaningful_position=projected_snapshot.meaningful_position,
            scale_in_count=projected_snapshot.scale_in_count,
            timestamp=projected_snapshot.timestamp,
        )
        return projected_snapshot

    def _resolve_fill_bnb_rate(self, fill: Any) -> Optional[Decimal]:
        if getattr(fill, "commission_asset", None) is None or fill.commission_asset.value != ASSET_BNB:
            return self.current_bnb_price
        if self.current_bnb_price is not None and self.current_bnb_price > Decimal("0"):
            return self.current_bnb_price
        return self._load_cached_bnb_rate()

    async def _run_startup_sync(self) -> None:
        await self._recovery_helper.run_startup_sync()

    async def _run_restart_reconciliation(self, previous_snapshot: PortfolioSnapshot, restart_reason: str) -> None:
        await self._recovery_helper.run_restart_reconciliation(previous_snapshot, restart_reason)

    def _apply_startup_oco_consistency(self, open_oco_order_ids: list[str]) -> None:
        self._recovery_helper.apply_startup_oco_consistency(open_oco_order_ids)

    async def _handle_restart_protection_without_exchange_oco(self) -> bool:
        return await self._recovery_helper.handle_restart_protection_without_exchange_oco()

    def _check_missing_basis_at_startup(self) -> None:
        self._recovery_helper.check_missing_basis_at_startup()

    def _resume_after_startup_block_if_possible(self) -> None:
        self._recovery_helper.resume_after_startup_block_if_possible()

    def _handle_oco_status(self, event: OCOListStatusEvent) -> None:
        if event.list_order_id != self.active_oco_order_id:
            return
        if self._is_external_oco_cancellation(event) and self.portfolio.snapshot.held_qty > Decimal("0"):
            self.active_oco_order_id = None
            self.protection_mode = ProtectionMode.SOFTWARE_STOP
            self.software_stop_active = True
            self._raise_alert(
                AlertSeverity.CRITICAL,
                CRITICAL_EXTERNAL_OCO_CANCEL_CODE,
                "Exchange OCO was cancelled while position remained exposed; software-stop reactivated",
                timestamp=event.timestamp,
            )
            self._emit_runtime_event("external_oco_cancelled", audit=True)
            return
        if event.list_order_status == EXCHANGE_LIST_STATUS_ALL_DONE:
            self.active_oco_order_id = None
            self.protection_mode = ProtectionMode.NONE
            self.software_stop_active = False
            self.executor.remove_local_oco(event.list_order_id)

    @staticmethod
    def _is_external_oco_cancellation(event: OCOListStatusEvent) -> bool:
        return boundary_is_external_oco_cancellation(event.list_status_type, event.list_order_status)

    def _update_symbol_change_progress(self, timestamp: datetime) -> None:
        if self.pending_symbol_change is None:
            return
        if self.symbol_change_stage != SymbolChangeStage.AWAITING_ZERO:
            return
        if self.portfolio.snapshot.held_qty > Decimal("0"):
            return
        self._advance_symbol_change_to_config_update(timestamp)
        self._emit_runtime_event("symbol_change_zero_reached", audit=True)

    def _advance_symbol_change_to_config_update(self, timestamp: datetime) -> None:
        self.portfolio.replace_snapshot(
            avg_cost_basis=Decimal("0"),
            scale_in_count=0,
            timestamp=timestamp,
        )
        self.symbol_change_stage = SymbolChangeStage.AWAITING_CONFIG_UPDATE

    async def _ensure_startup_best_bid(self) -> Optional[Decimal]:
        return await self._recovery_helper.ensure_startup_best_bid()

    def _hydrate_restart_state(self) -> None:
        self._recovery_helper.hydrate_restart_state()

    def _validate_oco_pre_submit(self, oco: OCOOrder, *, require_market_reference: bool) -> None:
        if oco.symbol != self.config.symbol:
            raise ValueError("OCO symbol must match runtime symbol")
        if oco.quantity > self.portfolio.snapshot.held_qty:
            raise ValueError("OCO quantity cannot exceed held position size")
        if not require_market_reference:
            return
        book_channel = self.channels.get(StreamType.BOOK_TICKER)
        if self.current_bid is None:
            raise ValueError("current market reference is unavailable for OCO validation")
        if book_channel is None or book_channel.state.last_update_at is None or book_channel.state.is_stale:
            raise ValueError("current market reference is stale for OCO validation")
        if oco.tp_price <= self.current_bid:
            raise ValueError("OCO take-profit must be above current market for protective SELL OCO")
        if oco.sl_stop_price >= self.current_bid:
            raise ValueError("OCO stop-loss must be below current market for protective SELL OCO")

    async def _reconcile_local_ocos(self) -> bool:
        return await self._recovery_helper.reconcile_local_ocos()

    async def _reconcile_outstanding_plain_orders(self) -> bool:
        return await self._recovery_helper.reconcile_outstanding_plain_orders()

    async def _resolve_filled_oco_leg(self, oco: OCOOrder) -> Optional[tuple[list[Any], str]]:
        return await self._recovery_helper.resolve_filled_oco_leg(oco)

    def _load_cached_bnb_rate(self) -> Optional[Decimal]:
        return self._recovery_helper.load_cached_bnb_rate()

    def _apply_reconciled_fills(self, fills: list[Any], cached_bnb_rate: Optional[Decimal]) -> None:
        self._recovery_helper.apply_reconciled_fills(fills, cached_bnb_rate)

    def _persist_accounting_history(self) -> None:
        self._recovery_helper.persist_accounting_history()

    def _detect_manual_trade(self, previous_snapshot: PortfolioSnapshot, position_change_explained: bool) -> None:
        self._recovery_helper.detect_manual_trade(previous_snapshot, position_change_explained)

    async def _reconcile_missing_exchange_oco(self) -> None:
        await self._recovery_helper.reconcile_missing_exchange_oco()

    def _raise_alert(
        self,
        severity: AlertSeverity,
        code: str,
        message: str,
        context: Optional[dict[str, Any]] = None,
        *,
        timestamp: Optional[datetime] = None,
    ) -> RuntimeAlertEvent:
        alert_time = timestamp or self.current_runtime_time
        dispatched = self.alert_dispatcher.dispatch(
            alert_time,
            severity,
            code,
            message,
            self.config.symbol.to_exchange_str(),
            context=context,
        )
        if self.last_alert is None or self._alert_priority(dispatched.severity) >= self._alert_priority(self.last_alert.severity):
            self.last_alert = RuntimeAlertEvent(
                timestamp=dispatched.timestamp,
                severity=dispatched.severity,
                code=dispatched.code,
                message=dispatched.message,
            )
        self.observability.emit(
            {
                "timestamp": dispatched.timestamp,
                "event_type": "alert",
                "symbol": dispatched.symbol,
                "intent": None,
                "risk_result": None,
                "sizer_result": None,
                "execution_result": None,
                "severity": dispatched.severity,
                "code": dispatched.code,
                "message": dispatched.message,
                "context": dispatched.context,
                "delivered_via": list(dispatched.delivered_via),
            },
            audit=True,
        )
        return self.last_alert

    @staticmethod
    def _alert_priority(severity: AlertSeverity) -> int:
        priorities = {
            AlertSeverity.INFO: 1,
            AlertSeverity.WARNING: 2,
            AlertSeverity.CRITICAL: 3,
        }
        return priorities[severity]

    def _save_snapshot_or_alert(self) -> None:
        if self.snapshot_store is None:
            return
        try:
            self.snapshot_store.save(self.portfolio.snapshot)
        except Exception:
            self._raise_alert(
                AlertSeverity.CRITICAL,
                CRITICAL_STORAGE_UNAVAILABLE_CODE,
                "Snapshot storage is unavailable",
            )
            self.status = TestnetRuntimeStatus.HALT

    def _handle_runtime_exception(self, exc: Exception, *, code: Optional[str] = None) -> None:
        message = str(exc)
        resolved_code = code or CRITICAL_RUNTIME_CRASH_CODE
        if code is None and ("418" in message or "ip ban" in message.lower()):
            resolved_code = CRITICAL_IP_BAN_CODE
        self._raise_alert(
            AlertSeverity.CRITICAL,
            resolved_code,
            message or exc.__class__.__name__,
            context={"exception_type": exc.__class__.__name__},
        )
        self.status = TestnetRuntimeStatus.HALT

    def check_heartbeat_timeout(self, now: Optional[datetime] = None) -> None:
        if self.last_heartbeat_at is None:
            return
        observed_now = now or datetime.now(timezone.utc)
        if (observed_now - self.last_heartbeat_at).total_seconds() <= HEARTBEAT_TIMEOUT_SECONDS:
            return
        if self._heartbeat_timeout_active:
            return
        self._heartbeat_timeout_active = True
        self._raise_alert(
            AlertSeverity.CRITICAL,
            CRITICAL_HEARTBEAT_TIMEOUT_CODE,
            "Heartbeat timeout detected",
            context={"last_heartbeat_at": self.last_heartbeat_at},
            timestamp=observed_now,
        )

    def _observe_clock_drift(self, observed_timestamp: datetime, now: Optional[datetime] = None) -> None:
        observed_now = now or datetime.now(timezone.utc)
        drift_seconds = abs((observed_now - observed_timestamp).total_seconds())
        if drift_seconds <= CLOCK_DRIFT_INFO_SECONDS:
            return
        self._raise_alert(
            AlertSeverity.INFO,
            INFO_CLOCK_DRIFT_CODE,
            "Clock drift detected",
            context={"drift_seconds": Decimal(str(drift_seconds))},
            timestamp=observed_now,
        )

    def report_post_only_rejected(self, client_order_id: str) -> None:
        self._raise_alert(
            AlertSeverity.INFO,
            INFO_POST_ONLY_REJECTED_CODE,
            "Post-only order was rejected",
            context={"client_order_id": client_order_id},
        )

    def _current_equity(self) -> Optional[Decimal]:
        return self._safety_state_helper.current_equity()

    def _evaluate_warning_conditions(self) -> None:
        self._safety_state_helper.evaluate_warning_conditions()

    def _effective_risk_multipliers(self) -> RiskMultipliers:
        return self._safety_state_helper.effective_risk_multipliers()

    def set_regime_state(
        self,
        regime_unknown: bool,
        *,
        regime_mult: Optional[Decimal] = None,
        anomaly_mult: Optional[Decimal] = None,
    ) -> None:
        self._safety_state_helper.set_regime_state(
            regime_unknown,
            regime_mult=regime_mult,
            anomaly_mult=anomaly_mult,
        )

    def manual_resume_after_stop(self) -> None:
        self._safety_state_helper.manual_resume_after_stop()

    def _control_equity(self) -> Decimal:
        return self._safety_state_helper.control_equity()

    def _evaluate_safety_controls(self, timestamp: datetime) -> None:
        self._safety_state_helper.evaluate_safety_controls(timestamp)

    def _decision_state(self) -> dict[str, Any]:
        return {
            "portfolio_snapshot": self.portfolio.snapshot,
            "stale_flags": self._stale_flags(),
            "status": self.status.value,
            "operational_mode": self.operational_mode.value,
            "recovery_mode": self.recovery_mode_controller.mode.value,
        }

    def _stale_flags(self) -> dict[str, bool]:
        return {
            stream_type.value: channel.state.is_stale
            for stream_type, channel in self.channels.items()
        }

    def _log_decision(
        self,
        intent: Any,
        before_state: dict[str, Any],
        after_state: dict[str, Any],
        risk_result: Any,
        sizer_result: Any,
        execution_result: Any,
        rejection_reason: Optional[str],
    ) -> None:
        record = {
            "timestamp": self.current_runtime_time,
            "event_type": "decision_cycle",
            "symbol": self.config.symbol.to_exchange_str(),
            "intent": intent,
            "risk_result": risk_result,
            "sizer_result": sizer_result,
            "execution_result": execution_result,
            "before_state": before_state,
            "after_state": after_state,
            "rejection_reason": rejection_reason,
        }
        self.observability.emit(record, audit=True)

    def _emit_runtime_event(self, event_type: str, *, audit: bool = False) -> None:
        self.observability.emit(
            {
                "timestamp": self.current_runtime_time,
                "event_type": event_type,
                "symbol": self.config.symbol.to_exchange_str(),
                "intent": None,
                "risk_result": None,
                "sizer_result": None,
                "execution_result": None,
                "status": self.status.value,
                "operational_mode": self.operational_mode.value,
                "recovery_mode": self.recovery_mode_controller.mode.value,
                "stale_flags": self._stale_flags(),
                "spm": self.observability.performance_monitor.snapshot(),
            },
            audit=audit,
        )
