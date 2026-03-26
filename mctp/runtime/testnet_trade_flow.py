from __future__ import annotations

import time
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from mctp.core.constants import (
    EXCHANGE_ORDER_STATUS_REJECTED,
    MISSING_BASIS_CLOSE_REASON,
    SYMBOL_CHANGE_SELL_REASON,
    WARNING_ZERO_BASIS_CODE,
)
from mctp.core.enums import AlertSeverity, BasisRecoveryState, ExecutionResult, IntentType, OperationalMode, OrderType, Side, SymbolChangeStage
from mctp.core.order import Order
from mctp.core.types import Symbol


class TestnetTradeFlowHelper:
    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime

    async def execute_intent(self, intent: Any, timestamp: datetime) -> None:
        runtime = self.runtime
        before_state = runtime._decision_state()
        risk_result = None
        sizer_result = None
        execution_result = None
        if runtime.status != runtime._status_enum.READY:
            runtime._log_decision(intent, before_state, runtime._decision_state(), None, None, None, "runtime_not_ready")
            return
        if intent.type == IntentType.HOLD:
            runtime._log_decision(intent, before_state, runtime._decision_state(), None, None, None, None)
            return
        if runtime._has_pending_order():
            runtime._log_decision(
                intent,
                before_state,
                runtime._decision_state(),
                None,
                None,
                None,
                "pending_order_in_flight",
            )
            return
        if runtime.operational_mode == OperationalMode.STOP:
            runtime._log_decision(intent, before_state, runtime._decision_state(), None, None, None, "operational_mode_stop")
            return
        if intent.type == IntentType.BUY and runtime.operational_mode in {
            OperationalMode.PAUSE_NEW_ENTRIES,
            OperationalMode.CLOSE_ONLY,
        }:
            runtime._log_decision(
                intent,
                before_state,
                runtime._decision_state(),
                None,
                None,
                None,
                runtime.operational_mode.value.lower(),
            )
            return
        if intent.type == IntentType.BUY and runtime.zero_basis_buy_blocked:
            runtime._log_decision(intent, before_state, runtime._decision_state(), None, None, None, "zero_basis_buy_blocked")
            return
        risk_started = time.perf_counter_ns()
        risk_result = runtime.risk_layer.check(intent, runtime.portfolio.snapshot, runtime.config.instrument_info)
        runtime.observability.record_latency(
            timestamp,
            runtime.config.symbol.to_exchange_str(),
            "risk_check",
            (time.perf_counter_ns() - risk_started) // 1000,
        )
        if not risk_result.approved:
            runtime._log_decision(
                intent,
                before_state,
                runtime._decision_state(),
                risk_result,
                None,
                None,
                risk_result.rejection_reason.value if risk_result.rejection_reason is not None else None,
            )
            return
        sizer_started = time.perf_counter_ns()
        quantity, sizer_result = runtime._order_quantity(intent)
        runtime.observability.record_latency(
            timestamp,
            runtime.config.symbol.to_exchange_str(),
            "position_sizing",
            (time.perf_counter_ns() - sizer_started) // 1000,
        )
        if quantity is None:
            runtime._log_decision(
                intent,
                before_state,
                runtime._decision_state(),
                risk_result,
                sizer_result,
                None,
                sizer_result.rejection_reason if sizer_result is not None else "quantity_unavailable",
            )
            return
        order = Order(
            symbol=runtime.config.symbol,
            side=Side.BUY if intent.type == IntentType.BUY else Side.SELL,
            order_type=OrderType.MARKET,
            quantity=quantity,
            created_at=timestamp,
            reason=intent.reason,
        )
        if order.side == Side.SELL and not await runtime._ensure_no_active_oco_before_direct_sell(timestamp):
            runtime._log_decision(
                intent,
                before_state,
                runtime._decision_state(),
                risk_result,
                sizer_result,
                None,
                "active_oco_cancel_required",
            )
            return
        execution_started = time.perf_counter_ns()
        try:
            execution_result = await runtime.executor.submit_order(order)
        except Exception as exc:
            runtime._handle_runtime_exception(exc)
            raise
        if execution_result in {ExecutionResult.ACCEPTED, ExecutionResult.PARTIAL_FILL}:
            runtime._set_pending_order(order)
        else:
            runtime._clear_pending_order(order.client_order_id)
        runtime.observability.record_latency(
            timestamp,
            runtime.config.symbol.to_exchange_str(),
            "execution_submit",
            (time.perf_counter_ns() - execution_started) // 1000,
        )
        runtime._log_decision(intent, before_state, runtime._decision_state(), risk_result, sizer_result, execution_result, None)
        if execution_result.value == EXCHANGE_ORDER_STATUS_REJECTED and order.post_only:
            runtime.report_post_only_rejected(order.client_order_id)

    def order_quantity(self, intent: Any) -> tuple[Optional[Decimal], Optional[Any]]:
        runtime = self.runtime
        if intent.type == IntentType.SELL:
            quantity = runtime.portfolio.snapshot.held_qty if runtime.portfolio.snapshot.held_qty > Decimal("0") else None
            return quantity, None
        if runtime._effective_risk_multipliers().regime_mult == Decimal("0"):
            return None, None
        if runtime.current_ask is None:
            return None, None
        snapshot = runtime.indicator_engine.snapshot(runtime.candles[runtime.config.timeframe], ema_period=9, atr_period=14)
        if snapshot.atr is None or snapshot.atr <= Decimal("0"):
            return None, None
        result = runtime.position_sizer.calculate(
            runtime.portfolio.snapshot,
            stop_distance_pct=snapshot.atr / runtime.current_ask,
            instrument_info=runtime.config.instrument_info,
            current_price=runtime.current_ask,
            risk_multipliers=runtime._effective_risk_multipliers(),
        )
        return (result.quantity if result.approved else None), result

    async def request_symbol_change(self, new_symbol: Symbol) -> None:
        runtime = self.runtime
        if new_symbol == runtime.config.symbol:
            return
        if runtime.pending_symbol_change is not None and runtime.pending_symbol_change != new_symbol:
            raise ValueError("another symbol change is already pending")
        if runtime._has_pending_order():
            raise ValueError("symbol change is blocked while an order is still pending")
        if runtime.active_oco_order_id is not None:
            raise ValueError("symbol change is blocked while exchange protection is still active")
        unresolved_basis = runtime.manual_trade_prompt_required or (
            runtime.portfolio.snapshot.held_qty > Decimal("0")
            and runtime.portfolio.snapshot.avg_cost_basis <= Decimal("0")
            and runtime.basis_recovery_state in {
                BasisRecoveryState.MISSING,
                BasisRecoveryState.ZERO_DECLARED,
                BasisRecoveryState.CLOSE_PENDING,
            }
        )
        if unresolved_basis:
            raise ValueError("symbol change is blocked while basis obligations remain unresolved")
        runtime.pending_symbol_change = new_symbol
        runtime.status = runtime._status_enum.HALT
        if runtime.portfolio.snapshot.held_qty > Decimal("0"):
            runtime.symbol_change_stage = SymbolChangeStage.AWAITING_ZERO
            if not await runtime._ensure_no_active_oco_before_direct_sell(runtime.current_runtime_time):
                return
            order = Order(
                symbol=runtime.config.symbol,
                side=Side.SELL,
                order_type=OrderType.MARKET,
                quantity=runtime.portfolio.snapshot.held_qty,
                created_at=runtime.current_runtime_time,
                reason=SYMBOL_CHANGE_SELL_REASON,
            )
            result = await runtime.executor.submit_order(order)
            if result in {ExecutionResult.ACCEPTED, ExecutionResult.PARTIAL_FILL}:
                runtime._set_pending_order(order)
            return
        runtime._advance_symbol_change_to_config_update(runtime.current_runtime_time)

    def apply_symbol_change_config(self) -> None:
        runtime = self.runtime
        if runtime.pending_symbol_change is None:
            raise ValueError("no symbol change is pending")
        if runtime.symbol_change_stage != SymbolChangeStage.AWAITING_CONFIG_UPDATE:
            raise ValueError("symbol change requires zero position and basis reset before config update")
        if runtime.portfolio.snapshot.held_qty > Decimal("0") or runtime.manual_trade_prompt_required:
            raise ValueError("symbol change cannot be applied while position or operator obligations remain")
        runtime.config.symbol = runtime.pending_symbol_change
        runtime.symbol_change_stage = SymbolChangeStage.RESTART_REQUIRED
        runtime.restart_required = True

    def provide_manual_basis(self, basis: Decimal) -> None:
        runtime = self.runtime
        if basis <= Decimal("0"):
            raise ValueError("manual basis must be > 0")
        runtime.portfolio.replace_snapshot(
            avg_cost_basis=basis,
            timestamp=runtime.current_runtime_time,
        )
        runtime.basis_recovery_state = BasisRecoveryState.MANUAL
        runtime.manual_trade_detected = False
        runtime.manual_trade_prompt_required = False
        runtime.last_alert = None
        runtime._resume_after_startup_block_if_possible()

    def apply_manual_trade_basis_adjustment(self, basis: Decimal) -> None:
        runtime = self.runtime
        if not runtime.manual_trade_prompt_required:
            raise ValueError("no manual trade basis adjustment is pending")
        if basis < Decimal("0"):
            raise ValueError("manual trade basis must be >= 0")
        runtime.portfolio.replace_snapshot(
            avg_cost_basis=basis,
            timestamp=runtime.current_runtime_time,
        )
        runtime.manual_trade_detected = False
        runtime.manual_trade_prompt_required = False
        runtime.last_alert = None
        runtime._resume_after_startup_block_if_possible()

    def declare_zero_basis(self) -> None:
        runtime = self.runtime
        runtime.basis_recovery_state = BasisRecoveryState.ZERO_DECLARED
        runtime.zero_basis_buy_blocked = True
        runtime.last_alert = None
        runtime._raise_alert(
            AlertSeverity.WARNING,
            WARNING_ZERO_BASIS_CODE,
            "Zero basis declared; existing position stays managed but new BUY entries remain blocked until explicit confirmation",
        )
        runtime._resume_after_startup_block_if_possible()

    def confirm_zero_basis_for_new_entries(self) -> None:
        runtime = self.runtime
        if runtime.basis_recovery_state != BasisRecoveryState.ZERO_DECLARED:
            raise ValueError("zero basis has not been declared")
        runtime.zero_basis_buy_blocked = False

    async def request_missing_basis_immediate_close(self) -> None:
        runtime = self.runtime
        if runtime.portfolio.snapshot.held_qty <= Decimal("0"):
            raise ValueError("no open position to close")
        runtime.basis_recovery_state = BasisRecoveryState.CLOSE_PENDING
        if not await runtime._ensure_no_active_oco_before_direct_sell(runtime.current_runtime_time):
            return
        order = Order(
            symbol=runtime.config.symbol,
            side=Side.SELL,
            order_type=OrderType.MARKET,
            quantity=runtime.portfolio.snapshot.held_qty,
            created_at=runtime.current_runtime_time,
            reason=MISSING_BASIS_CLOSE_REASON,
        )
        result = await runtime.executor.submit_order(order)
        if result in {ExecutionResult.ACCEPTED, ExecutionResult.PARTIAL_FILL}:
            runtime._set_pending_order(order)
        runtime.status = runtime._status_enum.HALT
