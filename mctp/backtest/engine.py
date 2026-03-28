from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Callable, Optional

from mctp.backtest.analytics import analyze_backtest
from mctp.backtest.config import BacktestConfig
from mctp.backtest.market_replay import BacktestCandle, MarketReplay
from mctp.backtest.mtf_builder import IncrementalMtfBacktestBuilder
from mctp.backtest.rolling_indicators import RollingBacktestIndicators
from mctp.backtest.results import BacktestExecution, BacktestResult, ClosedTrade, EquityCurvePoint
from mctp.core.constants import (
    SL_EXECUTION_BUFFER,
    SPOT_BACKTEST_FREE_QUOTE_EPSILON,
    STRATEGY_ID_LEGACY_EMA_CROSS,
    STRATEGY_ID_V20_BTCUSDT_MTF,
)
from mctp.core.enums import CommissionAsset, IntentType, OrderType, QuantityMode, Side
from mctp.core.order import Fill, Order
from mctp.core.types import Intent, PortfolioSnapshot
from mctp.execution.oco import OCOOrder, OCOStatus
from mctp.indicators import Candle, IndicatorEngine
from mctp.indicators.models import IndicatorSnapshot
from mctp.portfolio.equity import EquityTracker
from mctp.portfolio.meaningful import is_meaningful_position
from mctp.portfolio.tracker import PortfolioTracker
from mctp.risk.adaptive import AdaptiveRiskController
from mctp.risk.layer import RiskLayer
from mctp.sizing.sizer import PositionSizer
from mctp.strategy import BtcUsdtMtfV20Strategy, StrategyInput, build_closed_mtf_candle_map_from_m15, required_m15_history_for_v20_btcusdt_mtf


@dataclass
class PendingEntryOrder:
    order: Order
    submitted_at: object


@dataclass
class OpenTrade:
    entry_fill: Fill


@dataclass
class ProtectiveExitState:
    active_oco: Optional[OCOOrder]
    open_trade: Optional[OpenTrade]
    trade_count_delta: int
    realized_pnl_delta: Decimal


@dataclass(frozen=True)
class BacktestProgress:
    processed_candles: int
    total_candles: int
    percent_complete: int
    candle_timestamp: object
    execution_count: int
    trade_count: int


class BacktestEngine:
    def __init__(self, config: BacktestConfig) -> None:
        self._config = config
        self._market_replay = MarketReplay(config.spread_bps)
        self._risk_layer = RiskLayer(config.risk_config)
        self._sizer = PositionSizer(config.sizer_config)
        self._indicator_engine = IndicatorEngine()

    def run(
        self,
        candles: list[BacktestCandle],
        *,
        progress_callback: Optional[Callable[[BacktestProgress], None]] = None,
    ) -> BacktestResult:
        if self._config.strategy_id == STRATEGY_ID_V20_BTCUSDT_MTF:
            return self._run_v20_btcusdt_mtf(candles, progress_callback=progress_callback)
        return self._run_legacy_ema_cross(candles, progress_callback=progress_callback)

    def _run_legacy_ema_cross(
        self,
        candles: list[BacktestCandle],
        *,
        progress_callback: Optional[Callable[[BacktestProgress], None]] = None,
    ) -> BacktestResult:
        if len(candles) < self._config.required_warmup_bars:
            return self._empty_result(candles)

        first_quote = self._market_replay.quote_for_candle(candles[0])
        initial_snapshot = PortfolioSnapshot(
            symbol=self._config.symbol,
            held_qty=self._config.initial_base,
            avg_cost_basis=Decimal("0"),
            free_quote=self._config.initial_quote,
            quote_asset=self._config.quote_asset,
            is_in_position=self._config.initial_base > Decimal("0"),
            meaningful_position=is_meaningful_position(self._config.initial_base, self._lot_size()),
            timestamp=candles[0].timestamp,
        )
        tracker = PortfolioTracker(
            initial_snapshot,
            EquityTracker(self._config.initial_quote + (self._config.initial_base * first_quote.mid)),
            bnb_price_provider=lambda: self._current_bnb_rate,
            lot_size_provider=self._lot_size,
        )
        risk_controller = AdaptiveRiskController(
            initial_equity=self._config.initial_quote + (self._config.initial_base * first_quote.mid)
        )

        executions: list[BacktestExecution] = []
        equity_curve: list[EquityCurvePoint] = []
        closed_trades: list[ClosedTrade] = []
        active_oco: Optional[OCOOrder] = None
        open_trade: Optional[OpenTrade] = None
        pending_entry: Optional[PendingEntryOrder] = None
        previous_close: Optional[Decimal] = None
        previous_ema: Optional[Decimal] = None
        realized_pnl_total = Decimal("0")
        trade_count = 0
        cancelled_order_count = 0
        self._current_bnb_rate: Optional[Decimal] = None
        order_counter = 0
        trade_counter = 0
        latest_indicators: Optional[IndicatorSnapshot] = None
        progress_milestones = self._progress_milestones(len(candles))
        rolling_indicators = RollingBacktestIndicators(
            ema_period=self._config.ema_period,
            atr_period=self._config.atr_period,
        )

        for index, candle in enumerate(candles):
            self._current_bnb_rate = candle.bnb_rate
            quote = self._market_replay.quote_for_candle(candle)
            latest_indicators = rolling_indicators.update(self._indicator_candle(candle))
            ema = latest_indicators.ema
            atr = latest_indicators.atr
            risk_controller.update_atr_context(atr, candle.close)
            current_equity = tracker.snapshot.free_quote + (tracker.snapshot.held_qty * quote.bid)

            if risk_controller.should_reset_daily(candle.timestamp):
                risk_controller.reset_daily(equity=current_equity, now=candle.timestamp)

            if pending_entry is not None:
                if self._market_replay.limit_buy_hit(candle, pending_entry.order.price or Decimal("0")):
                    order_counter += 1
                    trade_counter_id = trade_counter + 1
                    fill = self._make_fill(
                        order_id=pending_entry.order.client_order_id,
                        trade_id=f"bt-trade-{trade_counter_id}",
                        side=Side.BUY,
                        quantity=pending_entry.order.quantity,
                        fill_price=pending_entry.order.price or quote.ask,
                        timestamp=candle.timestamp,
                    )
                    tracker.on_fill(fill)
                    executions.append(self._execution_from_fill(fill, "LIMIT_ENTRY", quote.mid))
                    open_trade = OpenTrade(entry_fill=fill)
                    equity_curve.append(self._equity_point(candle.timestamp, tracker.snapshot, quote.bid, "FILL"))
                    active_oco = self._make_oco(tracker.snapshot.held_qty, fill.fill_price, atr, candle.timestamp, order_counter)
                    pending_entry = None
                    trade_counter += 1
                elif candle.timestamp - pending_entry.submitted_at >= timedelta(seconds=self._config.cancel_after_seconds):
                    pending_entry = None
                    cancelled_order_count += 1

            if active_oco is not None and not active_oco.is_terminal:
                active_oco = self._process_oco(active_oco, candle, executions, quote.mid)
                if active_oco is not None and active_oco.is_terminal:
                    trade_count += 1
                    last_fill = active_oco.all_fills[-1]
                    pnl = tracker.realized_pnl(last_fill)
                    realized_pnl_total += pnl.net_pnl
                    tracker.on_fill(last_fill)
                    equity_curve.append(self._equity_point(candle.timestamp, tracker.snapshot, quote.bid, "FILL"))
                    if open_trade is not None:
                        closed_trades.append(
                            self._closed_trade_from_fills(open_trade.entry_fill, last_fill, pnl.gross_pnl, pnl.net_pnl)
                        )
                        open_trade = None
                    exit_equity = tracker.snapshot.free_quote + (tracker.snapshot.held_qty * quote.bid)
                    risk_controller.on_trade_result(pnl.net_pnl, exit_equity, now=candle.timestamp)

            if index + 1 < self._config.required_warmup_bars:
                equity_curve.append(self._equity_point(candle.timestamp, tracker.snapshot, quote.bid, "HOLD"))
                previous_close = candle.close
                previous_ema = ema
                self._emit_progress_if_needed(
                    progress_callback=progress_callback,
                    milestones=progress_milestones,
                    processed_candles=index + 1,
                    total_candles=len(candles),
                    candle=candle,
                    execution_count=len(executions),
                    trade_count=trade_count,
                )
                continue

            if (
                previous_close is not None
                and previous_ema is not None
                and atr is not None
                and ema is not None
                and not tracker.snapshot.is_in_position
                and pending_entry is None
                and (active_oco is None or active_oco.is_terminal)
                and previous_close <= previous_ema
                and candle.close > ema
            ):
                intent = Intent(
                    type=IntentType.BUY,
                    symbol=self._config.symbol,
                    quantity_mode=QuantityMode.FULL,
                    timestamp=candle.timestamp,
                    reason="ema_cross_above",
                )
                risk_result = self._risk_layer.check(intent, tracker.snapshot, self._config.instrument_info)
                if risk_result.approved:
                    stop_distance_pct = atr / candle.close
                    sizer_result = self._sizer.calculate(
                        tracker.snapshot,
                        stop_distance_pct=stop_distance_pct,
                        instrument_info=self._config.instrument_info,
                        current_price=quote.ask,
                        risk_multipliers=risk_controller.get_risk_multipliers(),
                    )
                    if sizer_result.approved and sizer_result.quantity is not None:
                        final_quantity = self._cap_buy_quantity_to_affordable(
                            tracker.snapshot,
                            sizer_result.quantity,
                            quote.ask if self._config.entry_order_type == OrderType.MARKET else (
                                quote.bid * (Decimal("1") - self._config.entry_limit_discount_pct)
                            ),
                        )
                        if final_quantity is None:
                            equity_curve.append(self._equity_point(candle.timestamp, tracker.snapshot, quote.bid, "HOLD"))
                            previous_close = candle.close
                            previous_ema = ema
                            continue
                        order_counter += 1
                        if self._config.entry_order_type == OrderType.MARKET:
                            trade_counter += 1
                            fill = self._make_fill(
                                order_id=f"bt-order-{order_counter}",
                                trade_id=f"bt-trade-{trade_counter}",
                                side=Side.BUY,
                                quantity=final_quantity,
                                fill_price=quote.ask,
                                timestamp=candle.timestamp,
                            )
                            tracker.on_fill(fill)
                            self._assert_non_negative_free_quote(tracker.snapshot)
                            executions.append(self._execution_from_fill(fill, "MARKET_ENTRY", quote.mid))
                            open_trade = OpenTrade(entry_fill=fill)
                            equity_curve.append(self._equity_point(candle.timestamp, tracker.snapshot, quote.bid, "FILL"))
                            active_oco = self._make_oco(
                                tracker.snapshot.held_qty,
                                fill.fill_price,
                                atr,
                                candle.timestamp,
                                order_counter,
                            )
                        else:
                            limit_price = quote.bid * (Decimal("1") - self._config.entry_limit_discount_pct)
                            pending_entry = PendingEntryOrder(
                                order=Order(
                                    symbol=self._config.symbol,
                                    side=Side.BUY,
                                    order_type=OrderType.LIMIT,
                                    quantity=final_quantity,
                                    price=limit_price,
                                    created_at=candle.timestamp,
                                    client_order_id=f"bt-order-{order_counter}",
                                    reason="ema_cross_limit_entry",
                                ),
                                submitted_at=candle.timestamp,
                            )

            equity_curve.append(self._equity_point(candle.timestamp, tracker.snapshot, quote.bid, "HOLD"))
            previous_close = candle.close
            previous_ema = ema
            self._emit_progress_if_needed(
                progress_callback=progress_callback,
                milestones=progress_milestones,
                processed_candles=index + 1,
                total_candles=len(candles),
                candle=candle,
                execution_count=len(executions),
                trade_count=trade_count,
            )

        final_quote = self._market_replay.quote_for_candle(candles[-1])
        final_equity_snapshot = EquityTracker.make_snapshot(
            free_quote=tracker.snapshot.free_quote,
            held_qty=tracker.snapshot.held_qty,
            current_price=final_quote.bid,
            is_in_position=tracker.snapshot.is_in_position,
            now=candles[-1].timestamp,
            meaningful_position=tracker.snapshot.meaningful_position,
        )
        end_equity = final_equity_snapshot.total_equity
        unrealized_pnl = (
            tracker.snapshot.held_qty * (final_quote.bid - tracker.snapshot.avg_cost_basis)
            if tracker.snapshot.held_qty > Decimal("0")
            else Decimal("0")
        )
        result = BacktestResult(
            start_equity=self._config.initial_quote + (self._config.initial_base * first_quote.mid),
            end_equity=end_equity,
            realized_pnl_total=realized_pnl_total,
            unrealized_pnl=unrealized_pnl,
            fee_drag_quote_total=tracker.accounting.fee_drag_quote_total,
            execution_count=len(executions),
            trade_count=trade_count,
            cancelled_order_count=cancelled_order_count,
            warmup_bars=self._config.required_warmup_bars,
            final_snapshot=tracker.snapshot,
            final_equity_snapshot=final_equity_snapshot,
            executions=executions,
            accounting_history=tracker.accounting.fill_history,
            equity_curve=equity_curve,
            closed_trades=closed_trades,
            latest_ema=latest_indicators.ema if latest_indicators is not None else None,
            latest_atr=latest_indicators.atr if latest_indicators is not None else None,
            latest_indicators=latest_indicators,
        )
        result.analytics = analyze_backtest(result)
        return result

    def _run_v20_btcusdt_mtf(
        self,
        candles: list[BacktestCandle],
        *,
        progress_callback: Optional[Callable[[BacktestProgress], None]] = None,
    ) -> BacktestResult:
        required_warmup_bars = max(self._config.required_warmup_bars, required_m15_history_for_v20_btcusdt_mtf())
        if len(candles) < required_warmup_bars:
            return self._empty_result(candles, warmup_bars=required_warmup_bars)

        first_quote = self._market_replay.quote_for_candle(candles[0])
        initial_snapshot = PortfolioSnapshot(
            symbol=self._config.symbol,
            held_qty=self._config.initial_base,
            avg_cost_basis=Decimal("0"),
            free_quote=self._config.initial_quote,
            quote_asset=self._config.quote_asset,
            is_in_position=self._config.initial_base > Decimal("0"),
            meaningful_position=is_meaningful_position(self._config.initial_base, self._lot_size()),
            timestamp=candles[0].timestamp,
        )
        tracker = PortfolioTracker(
            initial_snapshot,
            EquityTracker(self._config.initial_quote + (self._config.initial_base * first_quote.mid)),
            bnb_price_provider=lambda: self._current_bnb_rate,
            lot_size_provider=self._lot_size,
        )
        risk_controller = AdaptiveRiskController(
            initial_equity=self._config.initial_quote + (self._config.initial_base * first_quote.mid)
        )
        strategy = BtcUsdtMtfV20Strategy(self._indicator_engine)

        executions: list[BacktestExecution] = []
        equity_curve: list[EquityCurvePoint] = []
        closed_trades: list[ClosedTrade] = []
        active_oco: Optional[OCOOrder] = None
        open_trade: Optional[OpenTrade] = None
        pending_entry: Optional[PendingEntryOrder] = None
        realized_pnl_total = Decimal("0")
        trade_count = 0
        cancelled_order_count = 0
        self._current_bnb_rate: Optional[Decimal] = None
        order_counter = 0
        trade_counter = 0
        latest_indicators: Optional[IndicatorSnapshot] = None
        progress_milestones = self._progress_milestones(len(candles))
        mtf_builder = IncrementalMtfBacktestBuilder()
        rolling_indicators = RollingBacktestIndicators(
            ema_period=self._config.ema_period,
            atr_period=self._config.atr_period,
        )
        indicator_count = 0

        for index, candle in enumerate(candles):
            self._current_bnb_rate = candle.bnb_rate
            quote = self._market_replay.quote_for_candle(candle)
            indicator_candle = self._indicator_candle(candle)
            indicator_count += 1
            latest_indicators = rolling_indicators.update(indicator_candle)
            mtf_builder.append(indicator_candle)
            current_equity = tracker.snapshot.free_quote + (tracker.snapshot.held_qty * quote.bid)

            if risk_controller.should_reset_daily(candle.timestamp):
                risk_controller.reset_daily(equity=current_equity, now=candle.timestamp)

            if indicator_count < required_warmup_bars:
                equity_curve.append(self._equity_point(candle.timestamp, tracker.snapshot, quote.bid, "HOLD"))
                self._emit_progress_if_needed(
                    progress_callback=progress_callback,
                    milestones=progress_milestones,
                    processed_candles=index + 1,
                    total_candles=len(candles),
                    candle=candle,
                    execution_count=len(executions),
                    trade_count=trade_count,
                )
                continue

            assert latest_indicators is not None
            atr = latest_indicators.atr
            risk_controller.update_atr_context(atr, candle.close)

            if pending_entry is not None:
                if self._market_replay.limit_buy_hit(candle, pending_entry.order.price or Decimal("0")):
                    order_counter += 1
                    trade_counter_id = trade_counter + 1
                    fill = self._make_fill(
                        order_id=pending_entry.order.client_order_id,
                        trade_id=f"bt-trade-{trade_counter_id}",
                        side=Side.BUY,
                        quantity=pending_entry.order.quantity,
                        fill_price=pending_entry.order.price or quote.ask,
                        timestamp=candle.timestamp,
                    )
                    tracker.on_fill(fill)
                    self._assert_non_negative_free_quote(tracker.snapshot)
                    executions.append(self._execution_from_fill(fill, "STRATEGY_ENTRY", quote.mid))
                    open_trade = OpenTrade(entry_fill=fill)
                    equity_curve.append(self._equity_point(candle.timestamp, tracker.snapshot, quote.bid, "FILL"))
                    active_oco = self._make_oco(
                        tracker.snapshot.held_qty,
                        fill.fill_price,
                        atr,
                        candle.timestamp,
                        order_counter,
                    )
                    pending_entry = None
                    trade_counter += 1
                elif candle.timestamp - pending_entry.submitted_at >= timedelta(seconds=self._config.cancel_after_seconds):
                    pending_entry = None
                    cancelled_order_count += 1

            if active_oco is not None and not active_oco.is_terminal:
                protective_exit = self._finalize_active_oco_exit(
                    tracker=tracker,
                    active_oco=active_oco,
                    open_trade=open_trade,
                    candle=candle,
                    executions=executions,
                    equity_curve=equity_curve,
                    closed_trades=closed_trades,
                    risk_controller=risk_controller,
                    reference_price=quote.mid,
                    mark_price=quote.bid,
                )
                active_oco = protective_exit.active_oco
                open_trade = protective_exit.open_trade
                trade_count += protective_exit.trade_count_delta
                realized_pnl_total += protective_exit.realized_pnl_delta
                if protective_exit.trade_count_delta > 0:
                    continue

            strategy_input = StrategyInput(
                snapshot=tracker.snapshot,
                indicators={"snapshot": latest_indicators},
                candles=mtf_builder.candle_map(),
                onchain=None,
            )
            intent = strategy.on_candle(strategy_input)

            if intent.type == IntentType.BUY and not tracker.snapshot.is_in_position and pending_entry is None:
                if atr is None:
                    equity_curve.append(self._equity_point(candle.timestamp, tracker.snapshot, quote.bid, "HOLD"))
                    continue
                risk_result = self._risk_layer.check(intent, tracker.snapshot, self._config.instrument_info)
                if risk_result.approved:
                    stop_distance_pct = atr / candle.close
                    sizer_result = self._sizer.calculate(
                        tracker.snapshot,
                        stop_distance_pct=stop_distance_pct,
                        instrument_info=self._config.instrument_info,
                        current_price=quote.ask,
                        risk_multipliers=risk_controller.get_risk_multipliers(),
                    )
                    if sizer_result.approved and sizer_result.quantity is not None:
                        candidate_price = (
                            quote.ask
                            if self._config.entry_order_type == OrderType.MARKET
                            else quote.bid * (Decimal("1") - self._config.entry_limit_discount_pct)
                        )
                        final_quantity = self._cap_buy_quantity_to_affordable(
                            tracker.snapshot,
                            sizer_result.quantity,
                            candidate_price,
                        )
                        if final_quantity is not None:
                            order_counter += 1
                            if self._config.entry_order_type == OrderType.MARKET:
                                trade_counter += 1
                                fill = self._make_fill(
                                    order_id=f"bt-order-{order_counter}",
                                    trade_id=f"bt-trade-{trade_counter}",
                                    side=Side.BUY,
                                    quantity=final_quantity,
                                    fill_price=quote.ask,
                                    timestamp=candle.timestamp,
                                )
                                tracker.on_fill(fill)
                                self._assert_non_negative_free_quote(tracker.snapshot)
                                executions.append(self._execution_from_fill(fill, "STRATEGY_ENTRY", quote.mid))
                                open_trade = OpenTrade(entry_fill=fill)
                                equity_curve.append(self._equity_point(candle.timestamp, tracker.snapshot, quote.bid, "FILL"))
                                active_oco = self._make_oco(
                                    tracker.snapshot.held_qty,
                                    fill.fill_price,
                                    atr,
                                    candle.timestamp,
                                    order_counter,
                                )
                                continue
                            limit_price = quote.bid * (Decimal("1") - self._config.entry_limit_discount_pct)
                            pending_entry = PendingEntryOrder(
                                order=Order(
                                    symbol=self._config.symbol,
                                    side=Side.BUY,
                                    order_type=OrderType.LIMIT,
                                    quantity=final_quantity,
                                    price=limit_price,
                                    created_at=candle.timestamp,
                                    client_order_id=f"bt-order-{order_counter}",
                                    reason=intent.reason or "v20_mtf_limit_entry",
                                ),
                                submitted_at=candle.timestamp,
                            )

            if intent.type == IntentType.SELL and tracker.snapshot.is_in_position and tracker.snapshot.held_qty > Decimal("0"):
                if active_oco is not None and not active_oco.is_terminal:
                    active_oco.status = OCOStatus.CANCELLED
                    active_oco.updated_at = candle.timestamp
                trade_counter += 1
                open_trade, realized_delta = self._close_open_trade_with_fill(
                    tracker=tracker,
                    open_trade=open_trade,
                    fill=self._make_fill(
                        order_id=f"bt-order-exit-{trade_counter}",
                        trade_id=f"bt-trade-exit-{trade_counter}",
                        side=Side.SELL,
                        quantity=tracker.snapshot.held_qty,
                        fill_price=quote.bid,
                        timestamp=candle.timestamp,
                    ),
                    executions=executions,
                    equity_curve=equity_curve,
                    closed_trades=closed_trades,
                    reference_price=quote.mid,
                    mark_price=quote.bid,
                    reason="STRATEGY_EXIT",
                    exit_reason="STRATEGY_EXIT",
                    close_trade_by_suffix=False,
                )
                active_oco = None
                realized_pnl_total += realized_delta
                trade_count += 1
                exit_equity = tracker.snapshot.free_quote + (tracker.snapshot.held_qty * quote.bid)
                risk_controller.on_trade_result(realized_delta, exit_equity, now=candle.timestamp)
                self._emit_progress_if_needed(
                    progress_callback=progress_callback,
                    milestones=progress_milestones,
                    processed_candles=index + 1,
                    total_candles=len(candles),
                    candle=candle,
                    execution_count=len(executions),
                    trade_count=trade_count,
                )
                continue

            equity_curve.append(self._equity_point(candle.timestamp, tracker.snapshot, quote.bid, "HOLD"))
            self._emit_progress_if_needed(
                progress_callback=progress_callback,
                milestones=progress_milestones,
                processed_candles=index + 1,
                total_candles=len(candles),
                candle=candle,
                execution_count=len(executions),
                trade_count=trade_count,
            )

        final_quote = self._market_replay.quote_for_candle(candles[-1])
        final_equity_snapshot = EquityTracker.make_snapshot(
            free_quote=tracker.snapshot.free_quote,
            held_qty=tracker.snapshot.held_qty,
            current_price=final_quote.bid,
            is_in_position=tracker.snapshot.is_in_position,
            now=candles[-1].timestamp,
            meaningful_position=tracker.snapshot.meaningful_position,
        )
        end_equity = final_equity_snapshot.total_equity
        unrealized_pnl = (
            tracker.snapshot.held_qty * (final_quote.bid - tracker.snapshot.avg_cost_basis)
            if tracker.snapshot.held_qty > Decimal("0")
            else Decimal("0")
        )
        result = BacktestResult(
            start_equity=self._config.initial_quote + (self._config.initial_base * first_quote.mid),
            end_equity=end_equity,
            realized_pnl_total=realized_pnl_total,
            unrealized_pnl=unrealized_pnl,
            fee_drag_quote_total=tracker.accounting.fee_drag_quote_total,
            execution_count=len(executions),
            trade_count=trade_count,
            cancelled_order_count=cancelled_order_count,
            warmup_bars=required_warmup_bars,
            final_snapshot=tracker.snapshot,
            final_equity_snapshot=final_equity_snapshot,
            executions=executions,
            accounting_history=tracker.accounting.fill_history,
            equity_curve=equity_curve,
            closed_trades=closed_trades,
            latest_ema=latest_indicators.ema if latest_indicators is not None else None,
            latest_atr=latest_indicators.atr if latest_indicators is not None else None,
            latest_indicators=latest_indicators,
        )
        result.analytics = analyze_backtest(result)
        return result

    def _empty_result(self, candles: list[BacktestCandle], warmup_bars: Optional[int] = None) -> BacktestResult:
        last_candle = candles[-1]
        snapshot = PortfolioSnapshot(
            symbol=self._config.symbol,
            held_qty=self._config.initial_base,
            avg_cost_basis=Decimal("0"),
            free_quote=self._config.initial_quote,
            quote_asset=self._config.quote_asset,
            is_in_position=self._config.initial_base > Decimal("0"),
            meaningful_position=is_meaningful_position(self._config.initial_base, self._lot_size()),
            timestamp=last_candle.timestamp,
        )
        quote = self._market_replay.quote_for_candle(last_candle)
        equity_snapshot = EquityTracker.make_snapshot(
            free_quote=snapshot.free_quote,
            held_qty=snapshot.held_qty,
            current_price=quote.bid,
            is_in_position=snapshot.is_in_position,
            meaningful_position=snapshot.meaningful_position,
            now=last_candle.timestamp,
        )
        result = BacktestResult(
            start_equity=self._config.initial_quote + (self._config.initial_base * quote.mid),
            end_equity=equity_snapshot.total_equity,
            realized_pnl_total=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            fee_drag_quote_total=Decimal("0"),
            execution_count=0,
            trade_count=0,
            cancelled_order_count=0,
            warmup_bars=self._config.required_warmup_bars if warmup_bars is None else warmup_bars,
            final_snapshot=snapshot,
            final_equity_snapshot=equity_snapshot,
            executions=[],
            accounting_history=[],
            equity_curve=[self._equity_point(last_candle.timestamp, snapshot, quote.bid, "HOLD")],
            closed_trades=[],
            latest_ema=None,
            latest_atr=None,
            latest_indicators=None,
        )
        result.analytics = analyze_backtest(result)
        return result

    @staticmethod
    def _indicator_candle(candle: BacktestCandle) -> Candle:
        return Candle(
            timestamp=candle.timestamp,
            open=candle.open,
            high=candle.high,
            low=candle.low,
            close=candle.close,
            volume=candle.volume,
        )

    def _lot_size(self) -> Optional[Decimal]:
        lot_size = self._config.instrument_info.get("lot_size")
        if lot_size is None:
            return None
        if isinstance(lot_size, Decimal):
            return lot_size
        return Decimal(str(lot_size))

    def _make_fill(
        self,
        order_id: str,
        trade_id: str,
        side: Side,
        quantity: Decimal,
        fill_price: Decimal,
        timestamp,
    ) -> Fill:
        quote_qty = quantity * fill_price
        commission_quote = quote_qty * self._config.fee_rate
        if self._config.commission_asset == CommissionAsset.QUOTE:
            commission = commission_quote
        elif self._config.commission_asset == CommissionAsset.BASE:
            commission = quantity * self._config.fee_rate
        else:
            if self._current_bnb_rate is None or self._current_bnb_rate <= Decimal("0"):
                raise ValueError("bnb_rate required and must be > 0 for BNB commission")
            commission = commission_quote / self._current_bnb_rate
        return Fill(
            order_id=order_id,
            symbol=self._config.symbol,
            side=side,
            base_qty_filled=quantity,
            quote_qty_filled=quote_qty,
            fill_price=fill_price,
            commission=commission,
            commission_asset=self._config.commission_asset,
            trade_id=trade_id,
            filled_at=timestamp,
        )

    def _execution_from_fill(
        self,
        fill: Fill,
        reason: str,
        reference_price: Decimal,
    ) -> BacktestExecution:
        slippage_quote = self._slippage_quote(fill.side, fill.base_qty_filled, fill.fill_price, reference_price)
        return BacktestExecution(
            timestamp=fill.filled_at,
            side=fill.side,
            quantity=fill.base_qty_filled,
            fill_price=fill.fill_price,
            commission=fill.commission,
            commission_asset=fill.commission_asset,
            reason=reason,
            order_id=fill.order_id,
            trade_id=fill.trade_id,
            reference_price=reference_price,
            slippage_quote=slippage_quote,
        )

    def _make_oco(
        self,
        quantity: Decimal,
        entry_price: Decimal,
        atr: Optional[Decimal],
        timestamp,
        order_counter: int,
    ) -> Optional[OCOOrder]:
        if atr is None or quantity <= Decimal("0"):
            return None
        sl_stop = entry_price - (atr * self._config.sl_atr_multiplier)
        tp_price = entry_price + (atr * self._config.tp_atr_multiplier)
        if sl_stop <= Decimal("0"):
            return None
        return OCOOrder(
            symbol=self._config.symbol,
            tp_price=tp_price,
            sl_stop_price=sl_stop,
            sl_limit_price=sl_stop * (Decimal("1") - SL_EXECUTION_BUFFER),
            quantity=quantity,
            list_order_id=f"bt-oco-{order_counter}",
            created_at=timestamp,
            updated_at=timestamp,
        )

    def _process_oco(
        self,
        active_oco: OCOOrder,
        candle: BacktestCandle,
        executions: list[BacktestExecution],
        reference_price: Decimal,
    ) -> Optional[OCOOrder]:
        triggered_leg = self._resolve_intrabar_protective_exit_leg(
            position_side=Side.BUY,
            tp_hit=self._market_replay.tp_hit(candle, active_oco.tp_price),
            sl_hit=self._market_replay.sl_hit(candle, active_oco.sl_stop_price),
        )
        if triggered_leg == "TP":
            fill = self._make_fill(
                order_id=active_oco.list_order_id,
                trade_id=f"{active_oco.list_order_id}-tp",
                side=Side.SELL,
                quantity=active_oco.remaining_qty,
                fill_price=active_oco.tp_price,
                timestamp=candle.timestamp,
            )
            active_oco.tp_fills.append(fill)
            active_oco.status = OCOStatus.TP_FILLED
            active_oco.updated_at = candle.timestamp
            executions.append(self._execution_from_fill(fill, "OCO_TP", reference_price))
            return active_oco
        if triggered_leg == "SL":
            fill = self._make_fill(
                order_id=active_oco.list_order_id,
                trade_id=f"{active_oco.list_order_id}-sl",
                side=Side.SELL,
                quantity=active_oco.remaining_qty,
                fill_price=active_oco.sl_limit_price,
                timestamp=candle.timestamp,
            )
            active_oco.sl_fills.append(fill)
            active_oco.status = (
                OCOStatus.PARTIAL_TP_THEN_SL
                if active_oco.tp_filled_qty > Decimal("0")
                else OCOStatus.SL_TRIGGERED
            )
            active_oco.updated_at = candle.timestamp
            executions.append(self._execution_from_fill(fill, "OCO_SL", reference_price))
            return active_oco
        return active_oco

    @staticmethod
    def _resolve_intrabar_protective_exit_leg(
        *,
        position_side: Side,
        tp_hit: bool,
        sl_hit: bool,
    ) -> Optional[str]:
        if position_side not in {Side.BUY, Side.SELL}:
            raise ValueError("position_side must be BUY or SELL for protective-exit resolution")
        if tp_hit and sl_hit:
            # OHLC data cannot prove intrabar ordering here, so settle the
            # ambiguity conservatively instead of overstating strategy quality.
            return "SL"
        if tp_hit:
            return "TP"
        if sl_hit:
            return "SL"
        return None

    @staticmethod
    def _equity_point(
        timestamp,
        snapshot: PortfolioSnapshot,
        mark_price: Decimal,
        point_type: str,
    ) -> EquityCurvePoint:
        equity = snapshot.free_quote + (snapshot.held_qty * mark_price)
        return EquityCurvePoint(timestamp=timestamp, equity=equity, point_type=point_type)

    @staticmethod
    def _slippage_quote(
        side: Side,
        quantity: Decimal,
        fill_price: Decimal,
        reference_price: Decimal,
    ) -> Decimal:
        if side == Side.BUY:
            return (fill_price - reference_price) * quantity
        return (reference_price - fill_price) * quantity

    @staticmethod
    def _closed_trade_from_fills(
        entry_fill: Fill,
        exit_fill: Fill,
        gross_pnl: Decimal,
        net_pnl: Decimal,
        *,
        exit_reason: Optional[str] = None,
        close_trade_by_suffix: bool = True,
    ) -> ClosedTrade:
        entry_notional = entry_fill.quote_qty_filled
        return ClosedTrade(
            trade_id=entry_fill.trade_id,
            entry_timestamp=entry_fill.filled_at,
            exit_timestamp=exit_fill.filled_at,
            quantity=exit_fill.base_qty_filled,
            entry_price=entry_fill.fill_price,
            exit_price=exit_fill.fill_price,
            gross_pnl=gross_pnl,
            net_pnl=net_pnl,
            return_pct=(net_pnl / entry_notional) if entry_notional > Decimal("0") else Decimal("0"),
            exit_reason=(
                exit_reason
                if exit_reason is not None
                else ("OCO_TP" if close_trade_by_suffix and exit_fill.trade_id.endswith("-tp") else "OCO_SL")
            ),
        )

    def _finalize_active_oco_exit(
        self,
        *,
        tracker: PortfolioTracker,
        active_oco: OCOOrder,
        open_trade: Optional[OpenTrade],
        candle: BacktestCandle,
        executions: list[BacktestExecution],
        equity_curve: list[EquityCurvePoint],
        closed_trades: list[ClosedTrade],
        risk_controller: AdaptiveRiskController,
        reference_price: Decimal,
        mark_price: Decimal,
    ) -> ProtectiveExitState:
        updated_oco = self._process_oco(active_oco, candle, executions, reference_price)
        if updated_oco is None or not updated_oco.is_terminal:
            return ProtectiveExitState(
                active_oco=updated_oco,
                open_trade=open_trade,
                trade_count_delta=0,
                realized_pnl_delta=Decimal("0"),
            )

        exit_fill = updated_oco.all_fills[-1]
        open_trade, realized_delta = self._close_open_trade_with_fill(
            tracker=tracker,
            open_trade=open_trade,
            fill=exit_fill,
            executions=executions,
            equity_curve=equity_curve,
            closed_trades=closed_trades,
            reference_price=reference_price,
            mark_price=mark_price,
            reason=None,
            exit_reason=None,
            close_trade_by_suffix=True,
        )
        exit_equity = tracker.snapshot.free_quote + (tracker.snapshot.held_qty * mark_price)
        risk_controller.on_trade_result(realized_delta, exit_equity, now=candle.timestamp)
        return ProtectiveExitState(
            active_oco=updated_oco,
            open_trade=open_trade,
            trade_count_delta=1,
            realized_pnl_delta=realized_delta,
        )

    def _close_open_trade_with_fill(
        self,
        *,
        tracker: PortfolioTracker,
        open_trade: Optional[OpenTrade],
        fill: Fill,
        executions: list[BacktestExecution],
        equity_curve: list[EquityCurvePoint],
        closed_trades: Optional[list[ClosedTrade]],
        reference_price: Decimal,
        mark_price: Decimal,
        reason: Optional[str],
        exit_reason: Optional[str],
        close_trade_by_suffix: bool,
    ) -> tuple[Optional[OpenTrade], Decimal]:
        pnl = tracker.realized_pnl(fill)
        tracker.on_fill(fill)
        if reason is not None:
            executions.append(self._execution_from_fill(fill, reason, reference_price))
        equity_curve.append(self._equity_point(fill.filled_at, tracker.snapshot, mark_price, "FILL"))
        if open_trade is not None and closed_trades is not None:
            closed_trades.append(
                self._closed_trade_from_fills(
                    open_trade.entry_fill,
                    fill,
                    pnl.gross_pnl,
                    pnl.net_pnl,
                    exit_reason=exit_reason,
                    close_trade_by_suffix=close_trade_by_suffix,
                )
            )
            open_trade = None
        elif open_trade is not None:
            open_trade = None
        return open_trade, pnl.net_pnl

    def _cap_buy_quantity_to_affordable(
        self,
        snapshot: PortfolioSnapshot,
        requested_quantity: Decimal,
        execution_price: Decimal,
    ) -> Optional[Decimal]:
        if requested_quantity <= Decimal("0"):
            return None
        if execution_price <= Decimal("0"):
            return None

        lot_size = self._lot_size() or Decimal("0")
        min_qty = self._instrument_decimal("min_qty", Decimal("0"))
        max_qty = self._instrument_decimal("max_qty", requested_quantity)
        min_notional = self._instrument_decimal("min_notional", Decimal("0"))

        quote_multiplier = Decimal("1")
        if self._config.commission_asset == CommissionAsset.QUOTE:
            quote_multiplier += self._config.fee_rate

        max_affordable_quantity = snapshot.free_quote / (execution_price * quote_multiplier)
        capped_quantity = min(requested_quantity, max_qty, max_affordable_quantity)
        if capped_quantity <= Decimal("0"):
            return None

        quantity = self._quantize_down(capped_quantity, lot_size)
        if quantity < min_qty:
            return None
        if quantity * execution_price < min_notional:
            return None
        if self._buy_quote_outflow(quantity, execution_price) > snapshot.free_quote:
            return None
        return quantity

    def _buy_quote_outflow(self, quantity: Decimal, execution_price: Decimal) -> Decimal:
        quote_outflow = quantity * execution_price
        if self._config.commission_asset == CommissionAsset.QUOTE:
            quote_outflow += quote_outflow * self._config.fee_rate
        return quote_outflow

    @staticmethod
    def _quantize_down(quantity: Decimal, lot_size: Decimal) -> Decimal:
        if lot_size <= Decimal("0"):
            return quantity
        return (quantity // lot_size) * lot_size

    def _instrument_decimal(self, key: str, default: Decimal) -> Decimal:
        value = self._config.instrument_info.get(key, default)
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    @staticmethod
    def _assert_non_negative_free_quote(snapshot: PortfolioSnapshot) -> None:
        if snapshot.free_quote < -SPOT_BACKTEST_FREE_QUOTE_EPSILON:
            raise AssertionError("spot backtest buy made free_quote materially negative")

    @staticmethod
    def _progress_milestones(total_candles: int) -> set[int]:
        if total_candles <= 0:
            return set()
        return {max(1, (total_candles * pct) // 10) for pct in range(1, 11)} | {total_candles}

    @staticmethod
    def _emit_progress_if_needed(
        *,
        progress_callback: Optional[Callable[[BacktestProgress], None]],
        milestones: set[int],
        processed_candles: int,
        total_candles: int,
        candle: BacktestCandle,
        execution_count: int,
        trade_count: int,
    ) -> None:
        if progress_callback is None or processed_candles not in milestones:
            return
        percent_complete = int((processed_candles * 100) / total_candles) if total_candles > 0 else 100
        progress_callback(
            BacktestProgress(
                processed_candles=processed_candles,
                total_candles=total_candles,
                percent_complete=percent_complete,
                candle_timestamp=candle.timestamp,
                execution_count=execution_count,
                trade_count=trade_count,
            )
        )
