from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from mctp.core.constants import (
    ASSET_BNB,
    CRITICAL_EXTERNAL_OCO_CANCEL_CODE,
    CRITICAL_MISSING_BASIS_CODE,
    CRITICAL_RESTART_OUTSTANDING_ORDER_CODE,
    CRITICAL_RESTART_PARTIAL_FILL_CODE,
    CRITICAL_STARTUP_OCO_AMBIGUITY_CODE,
    CRITICAL_STARTUP_PROTECTION_CONFLICT_CODE,
    CRITICAL_STORAGE_UNAVAILABLE_CODE,
    EXCHANGE_FILLED_OR_PARTIAL_ORDER_STATUSES,
    EXCHANGE_OPEN_SUBMIT_ORDER_STATUSES,
    EXCHANGE_ORDER_STATUS_FILLED,
    EXCHANGE_ORDER_STATUS_PARTIALLY_FILLED,
    MAX_SLIPPAGE_PCT,
    STARTUP_BOOK_BID_TIMEOUT_SECONDS,
    STARTUP_GAP_RISK_SELL_REASON,
    T_CANCEL,
    WARNING_MANUAL_TRADE_DETECTED_CODE,
)
from mctp.core.enums import AlertSeverity, BasisRecoveryState, ExecutionResult, OrderType, ProtectionMode, Side
from mctp.core.order import Order
from mctp.core.types import PortfolioSnapshot
from mctp.execution.oco import OCOOrder
from mctp.streams.base import StreamType


class TestnetRecoveryHelper:
    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime

    def apply_exchange_balance_truth(
        self,
        balances: dict[str, Decimal],
        locked_balances: dict[str, Decimal],
        timestamp: datetime,
    ) -> None:
        runtime = self.runtime
        free_base_balance = balances.get(runtime.config.symbol.base, Decimal("0"))
        locked_base_balance = locked_balances.get(runtime.config.symbol.base, Decimal("0"))
        effective_held_base = free_base_balance + locked_base_balance
        free_quote_balance = balances.get(runtime.config.symbol.quote, Decimal("0"))
        meaningful = runtime._is_meaningful_position(effective_held_base)
        avg_cost_basis = runtime.portfolio.snapshot.avg_cost_basis
        scale_in_count = runtime.portfolio.snapshot.scale_in_count
        if effective_held_base <= Decimal("0"):
            avg_cost_basis = Decimal("0")
            scale_in_count = 0
        runtime.portfolio.replace_snapshot(
            held_qty=effective_held_base,
            free_quote=free_quote_balance,
            avg_cost_basis=avg_cost_basis,
            is_in_position=effective_held_base > Decimal("0"),
            meaningful_position=meaningful,
            scale_in_count=scale_in_count,
            timestamp=timestamp,
        )
        runtime.current_runtime_time = timestamp

    async def run_startup_sync(self) -> None:
        runtime = self.runtime
        previous_snapshot = runtime._startup_previous_snapshot or runtime.portfolio.snapshot
        await self.run_restart_reconciliation(previous_snapshot, restart_reason="startup")
        if runtime.status == runtime._status_enum.HALT and runtime.manual_trade_prompt_required:
            return
        open_oco_order_ids = await runtime.executor.get_open_oco_order_ids(runtime.config.symbol)
        self.apply_startup_oco_consistency(open_oco_order_ids)
        if runtime.status == runtime._status_enum.HALT:
            return
        if (
            runtime.portfolio.snapshot.held_qty > Decimal("0")
            and runtime.config.startup_software_trailing_active
            and not open_oco_order_ids
            and runtime.config.startup_stop_price is not None
        ):
            protection_handled = await self.handle_restart_protection_without_exchange_oco()
            if protection_handled:
                return
        self.check_missing_basis_at_startup()
        runtime._evaluate_warning_conditions()

    async def run_restart_reconciliation(self, previous_snapshot: PortfolioSnapshot, restart_reason: str) -> None:
        runtime = self.runtime
        runtime._emit_runtime_event(f"restart_reconciliation_{restart_reason}", audit=True)
        runtime.reconciliation_runs += 1
        runtime._reconciliation_fill_anchor_snapshot = previous_snapshot
        try:
            position_change_explained = await self.reconcile_local_ocos()
            order_state_explained = await self.reconcile_outstanding_plain_orders()
            await self.reconcile_missing_exchange_oco()
            self.detect_manual_trade(previous_snapshot, position_change_explained or order_state_explained)
        finally:
            runtime._reconciliation_fill_anchor_snapshot = None

    def apply_startup_oco_consistency(self, open_oco_order_ids: list[str]) -> None:
        runtime = self.runtime
        if not open_oco_order_ids:
            if runtime.config.startup_software_stop_active:
                runtime.protection_mode = ProtectionMode.SOFTWARE_STOP
                runtime.software_stop_active = True
            return
        local_active_ocos = runtime.executor.load_local_active_ocos()
        if len(open_oco_order_ids) > 1 or open_oco_order_ids[0] not in local_active_ocos:
            runtime.active_oco_order_id = None
            runtime.protection_mode = ProtectionMode.NONE
            runtime.software_stop_active = False
            runtime._raise_alert(
                AlertSeverity.CRITICAL,
                CRITICAL_STARTUP_OCO_AMBIGUITY_CODE,
                "Startup found ambiguous exchange OCO protection context",
            )
            runtime._emit_runtime_event("startup_oco_ambiguity", audit=True)
            runtime.status = runtime._status_enum.HALT
            return
        runtime.active_oco_order_id = open_oco_order_ids[0]
        if runtime.config.startup_software_stop_active:
            runtime.protection_mode = ProtectionMode.EXCHANGE_OCO
            runtime.software_stop_active = False
            runtime._raise_alert(
                AlertSeverity.CRITICAL,
                CRITICAL_STARTUP_PROTECTION_CONFLICT_CODE,
                "Startup detected both exchange OCO and software-stop primary protection",
            )
            runtime._emit_runtime_event("startup_protection_conflict", audit=True)
            runtime.status = runtime._status_enum.HALT
            return
        runtime.protection_mode = ProtectionMode.EXCHANGE_OCO
        runtime.software_stop_active = False

    async def handle_restart_protection_without_exchange_oco(self) -> bool:
        runtime = self.runtime
        stop_price = runtime.config.startup_stop_price
        assert stop_price is not None
        best_bid = await self.ensure_startup_best_bid()
        if best_bid is not None:
            market_threshold = stop_price * (Decimal("1") - MAX_SLIPPAGE_PCT)
            order_type = OrderType.MARKET if best_bid >= market_threshold else OrderType.LIMIT
            order = Order(
                symbol=runtime.config.symbol,
                side=Side.SELL,
                order_type=order_type,
                quantity=runtime.portfolio.snapshot.held_qty,
                price=stop_price if order_type == OrderType.LIMIT else None,
                created_at=runtime.current_runtime_time,
                reason=STARTUP_GAP_RISK_SELL_REASON,
            )
            result = await runtime.executor.submit_order(order)
            if result in {ExecutionResult.ACCEPTED, ExecutionResult.PARTIAL_FILL}:
                runtime._set_pending_order(order)
            runtime.status = runtime._status_enum.HALT
            runtime._emit_runtime_event("startup_gap_risk_sell", audit=True)
            return True
        oco = OCOOrder(
            symbol=runtime.config.symbol,
            tp_price=stop_price,
            sl_stop_price=stop_price,
            sl_limit_price=stop_price * (Decimal("1") - MAX_SLIPPAGE_PCT),
            quantity=runtime.portfolio.snapshot.held_qty,
            created_at=runtime.current_runtime_time,
            updated_at=runtime.current_runtime_time,
        )
        await runtime.submit_oco(oco, require_market_reference=False)
        runtime.status = runtime._status_enum.HALT
        runtime._emit_runtime_event("startup_fallback_oco", audit=True)
        return True

    def check_missing_basis_at_startup(self) -> None:
        runtime = self.runtime
        if runtime.manual_trade_prompt_required:
            return
        if runtime.portfolio.snapshot.held_qty <= Decimal("0"):
            runtime.basis_recovery_state = BasisRecoveryState.NONE
            return
        if runtime.portfolio.snapshot.avg_cost_basis > Decimal("0"):
            runtime.basis_recovery_state = BasisRecoveryState.NONE
            return
        runtime.basis_recovery_state = BasisRecoveryState.MISSING
        runtime.status = runtime._status_enum.HALT
        runtime._raise_alert(
            AlertSeverity.CRITICAL,
            CRITICAL_MISSING_BASIS_CODE,
            "Open position exists but basis is missing",
        )
        runtime._emit_runtime_event("missing_basis_block", audit=True)

    def resume_after_startup_block_if_possible(self) -> None:
        runtime = self.runtime
        if not runtime.startup_checks_completed:
            return
        if runtime.status != runtime._status_enum.HALT:
            return
        if runtime.last_alert is not None and runtime.last_alert.severity == AlertSeverity.CRITICAL:
            return
        if runtime.basis_recovery_state == BasisRecoveryState.MISSING:
            return
        if runtime.manual_trade_prompt_required:
            return
        if runtime.symbol_change_stage == runtime._symbol_change_stage_enum.AWAITING_ZERO:
            return
        runtime.status = runtime._status_enum.READY

    async def ensure_startup_best_bid(self) -> Optional[Decimal]:
        runtime = self.runtime
        if runtime.current_bid is not None:
            return runtime.current_bid
        book_channel = runtime.channels.get(StreamType.BOOK_TICKER)
        if book_channel is None:
            return None
        try:
            event = await asyncio.wait_for(
                book_channel.receive(),
                timeout=STARTUP_BOOK_BID_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            return None
        if event is None:
            return None
        await runtime._dispatch(StreamType.BOOK_TICKER, event)
        await runtime._stale_checkpoint(event)
        return runtime.current_bid

    def hydrate_restart_state(self) -> None:
        runtime = self.runtime
        records = runtime.accounting_store.load()
        if records is not None:
            runtime.portfolio.accounting.restore_history(records)
        if runtime.snapshot_store.exists():
            persisted_snapshot = runtime.snapshot_store.load()
            if persisted_snapshot is not None:
                runtime.portfolio.restore_snapshot(persisted_snapshot)
                runtime.current_runtime_time = persisted_snapshot.timestamp
                runtime._restart_state_loaded = True

    async def reconcile_local_ocos(self) -> bool:
        runtime = self.runtime
        reconciled = False
        for list_order_id, oco in runtime.executor.load_local_active_ocos().items():
            if oco.symbol != runtime.config.symbol:
                continue
            resolved = await self.resolve_filled_oco_leg(oco)
            if resolved is None:
                continue
            fills, opposite_client_order_id = resolved
            cached_bnb_rate = self.load_cached_bnb_rate()
            runtime.executor.remember_exchange_fills(fills[0].order_id, fills)
            self.apply_reconciled_fills(fills, cached_bnb_rate)
            await runtime.executor.cancel_order_with_code(opposite_client_order_id, T_CANCEL)
            runtime.last_cancel_code = T_CANCEL
            runtime.executor.remove_local_oco(list_order_id)
            runtime.active_oco_order_id = None
            runtime.protection_mode = ProtectionMode.NONE
            runtime.software_stop_active = False
            reconciled = True
            runtime._emit_runtime_event("oco_outage_fill_reconciled", audit=True)
        return reconciled

    async def reconcile_outstanding_plain_orders(self) -> bool:
        runtime = self.runtime
        explained = False
        local_active_orders = runtime.executor.load_local_active_orders()
        if not runtime._restart_state_loaded and not local_active_orders:
            return False
        exchange_open_orders = await runtime.executor.get_open_order_snapshots(runtime.config.symbol)
        exchange_open_by_id = {snapshot.client_order_id: snapshot for snapshot in exchange_open_orders}
        local_order_ids = set(local_active_orders)

        for client_order_id, snapshot in exchange_open_by_id.items():
            if client_order_id in local_order_ids:
                continue
            runtime._clear_pending_order()
            if snapshot.executed_qty > Decimal("0") or snapshot.status == EXCHANGE_ORDER_STATUS_PARTIALLY_FILLED:
                runtime._raise_alert(
                    AlertSeverity.CRITICAL,
                    CRITICAL_RESTART_PARTIAL_FILL_CODE,
                    "Restart found unknown exchange partial-fill state for open order",
                    context={"client_order_id": client_order_id, "status": snapshot.status},
                )
                runtime._emit_runtime_event("restart_unknown_partial_fill", audit=True)
            else:
                runtime._raise_alert(
                    AlertSeverity.CRITICAL,
                    CRITICAL_RESTART_OUTSTANDING_ORDER_CODE,
                    "Restart found unknown exchange open order for runtime symbol",
                    context={"client_order_id": client_order_id, "status": snapshot.status},
                )
                runtime._emit_runtime_event("restart_unknown_open_order", audit=True)
            runtime.status = runtime._status_enum.HALT
            explained = True

        for client_order_id, order in local_active_orders.items():
            if order.symbol != runtime.config.symbol:
                continue
            exchange_snapshot = await runtime.executor.get_exchange_order_snapshot(runtime.config.symbol, client_order_id)
            if exchange_snapshot is None:
                continue
            if exchange_snapshot.status == EXCHANGE_ORDER_STATUS_FILLED:
                fills = await runtime.executor.get_exchange_fills_for_order(runtime.config.symbol, client_order_id)
                if not fills:
                    runtime._raise_alert(
                        AlertSeverity.CRITICAL,
                        CRITICAL_RESTART_PARTIAL_FILL_CODE,
                        "Restart found executed order without reconstructable exchange fill history",
                        context={"client_order_id": client_order_id},
                    )
                    runtime._emit_runtime_event("restart_missing_fill_history", audit=True)
                    runtime.status = runtime._status_enum.HALT
                    explained = True
                    continue
                runtime.executor.remember_exchange_fills(client_order_id, fills)
                self.apply_reconciled_fills(fills, self.load_cached_bnb_rate())
                runtime.executor.remove_local_order(client_order_id)
                runtime._clear_pending_order(client_order_id)
                runtime._emit_runtime_event("restart_filled_order_reconciled", audit=True)
                explained = True
                continue
            if exchange_snapshot.status in EXCHANGE_OPEN_SUBMIT_ORDER_STATUSES:
                runtime._set_pending_order(order)
                runtime._raise_alert(
                    AlertSeverity.CRITICAL,
                    CRITICAL_RESTART_OUTSTANDING_ORDER_CODE,
                    "Restart found outstanding local order still open on exchange",
                    context={"client_order_id": client_order_id, "status": exchange_snapshot.status},
                )
                runtime._emit_runtime_event("restart_outstanding_local_order", audit=True)
                runtime.status = runtime._status_enum.HALT
                explained = True
                continue
            if exchange_snapshot.status == EXCHANGE_ORDER_STATUS_PARTIALLY_FILLED or exchange_snapshot.executed_qty > Decimal("0"):
                fills = await runtime.executor.get_exchange_fills_for_order(runtime.config.symbol, client_order_id)
                if fills:
                    runtime.executor.remember_exchange_fills(client_order_id, fills)
                    self.apply_reconciled_fills(fills, self.load_cached_bnb_rate())
                runtime._set_pending_order(order)
                runtime._raise_alert(
                    AlertSeverity.CRITICAL,
                    CRITICAL_RESTART_PARTIAL_FILL_CODE,
                    "Restart found local order with partial-fill-related exchange state",
                    context={
                        "client_order_id": client_order_id,
                        "status": exchange_snapshot.status,
                        "executed_qty": exchange_snapshot.executed_qty,
                    },
                )
                runtime._emit_runtime_event("restart_partial_fill_detected", audit=True)
                runtime.status = runtime._status_enum.HALT
                explained = True
                continue
            runtime.executor.remove_local_order(client_order_id)
            runtime._clear_pending_order(client_order_id)

        return explained

    async def resolve_filled_oco_leg(self, oco: OCOOrder) -> Optional[tuple[list[Any], str]]:
        runtime = self.runtime
        if not oco.tp_client_order_id or not oco.sl_client_order_id:
            return None
        tp_snapshot = await runtime.executor.get_exchange_order_snapshot(runtime.config.symbol, oco.tp_client_order_id)
        sl_snapshot = await runtime.executor.get_exchange_order_snapshot(runtime.config.symbol, oco.sl_client_order_id)
        if tp_snapshot is None or sl_snapshot is None:
            return None
        tp_filled = tp_snapshot.status in EXCHANGE_FILLED_OR_PARTIAL_ORDER_STATUSES and tp_snapshot.executed_qty > Decimal("0")
        sl_filled = sl_snapshot.status in EXCHANGE_FILLED_OR_PARTIAL_ORDER_STATUSES and sl_snapshot.executed_qty > Decimal("0")
        if tp_filled == sl_filled:
            return None
        if tp_filled:
            fills = await runtime.executor.get_exchange_fills_for_order(runtime.config.symbol, oco.tp_client_order_id)
            return (fills, oco.sl_client_order_id) if fills else None
        fills = await runtime.executor.get_exchange_fills_for_order(runtime.config.symbol, oco.sl_client_order_id)
        return (fills, oco.tp_client_order_id) if fills else None

    def load_cached_bnb_rate(self) -> Optional[Decimal]:
        runtime = self.runtime
        records = runtime.accounting_store.load()
        if records is not None:
            for record in reversed(records):
                if record.bnb_rate_at_fill is not None:
                    return record.bnb_rate_at_fill
        return runtime.current_bnb_price

    def apply_reconciled_fills(self, fills: list[Any], cached_bnb_rate: Optional[Decimal]) -> None:
        runtime = self.runtime
        existing_trade_ids = {record.trade_id for record in runtime.portfolio.accounting.fill_history}
        anchor_snapshot = runtime._reconciliation_fill_anchor_snapshot or runtime.portfolio.snapshot
        applied_any = False
        previous_bnb = runtime.current_bnb_price
        try:
            for fill in fills:
                if fill.trade_id in existing_trade_ids:
                    continue
                if (
                    getattr(fill, "commission_asset", None) is not None
                    and fill.commission_asset.value == ASSET_BNB
                    and cached_bnb_rate is not None
                ):
                    runtime.current_bnb_price = cached_bnb_rate
                    runtime.last_reconciliation_applied_bnb_rate = cached_bnb_rate
                anchor_snapshot = runtime._record_fill_without_reapplying_exchange_balances(
                    fill,
                    anchor_snapshot=anchor_snapshot,
                )
                runtime._reconciliation_fill_anchor_snapshot = anchor_snapshot
                existing_trade_ids.add(fill.trade_id)
                applied_any = True
            if applied_any:
                self.persist_accounting_history()
        finally:
            runtime.current_bnb_price = previous_bnb

    def persist_accounting_history(self) -> None:
        runtime = self.runtime
        try:
            runtime.accounting_store.save(runtime.portfolio.accounting.fill_history)
        except Exception:
            runtime._raise_alert(
                AlertSeverity.CRITICAL,
                CRITICAL_STORAGE_UNAVAILABLE_CODE,
                "Accounting storage is unavailable",
            )
            runtime.status = runtime._status_enum.HALT

    def detect_manual_trade(self, previous_snapshot: PortfolioSnapshot, position_change_explained: bool) -> None:
        runtime = self.runtime
        if not runtime._restart_state_loaded:
            return
        if position_change_explained:
            return
        lot_size = runtime._lot_size() or Decimal("0")
        delta = abs(runtime.portfolio.snapshot.held_qty - previous_snapshot.held_qty)
        if delta <= lot_size:
            return
        runtime.manual_trade_detected = True
        runtime.manual_trade_prompt_required = True
        runtime.status = runtime._status_enum.HALT
        runtime._raise_alert(
            AlertSeverity.WARNING,
            WARNING_MANUAL_TRADE_DETECTED_CODE,
            "Exchange-side position change detected outside platform flow; operator basis adjustment is required",
        )
        runtime._emit_runtime_event("manual_trade_detected", audit=True)

    async def reconcile_missing_exchange_oco(self) -> None:
        runtime = self.runtime
        local_ocos = runtime.executor.load_local_active_ocos()
        if not local_ocos or runtime.portfolio.snapshot.held_qty <= Decimal("0"):
            return
        exchange_open_oco_ids = set(await runtime.executor.get_open_oco_order_ids(runtime.config.symbol))
        for list_order_id in local_ocos:
            if list_order_id in exchange_open_oco_ids:
                continue
            runtime.active_oco_order_id = None
            runtime.protection_mode = ProtectionMode.SOFTWARE_STOP
            runtime.software_stop_active = True
            runtime._raise_alert(
                AlertSeverity.CRITICAL,
                CRITICAL_EXTERNAL_OCO_CANCEL_CODE,
                "Exchange OCO was cancelled while position remained exposed; software-stop reactivated",
            )
            runtime._emit_runtime_event("missing_exchange_oco_recovered_with_software_stop", audit=True)
            return
