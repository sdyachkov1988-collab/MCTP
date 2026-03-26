"""
SpotPaperExecutor — реализация ExecutionInterface (v0.4).
Симулирует исполнение локально: MARKET, LIMIT, OCO.
Реальное биржевое API — в v1.0 (SpotLiveExecutor).
"""
import asyncio
import uuid
from decimal import Decimal
from datetime import datetime, timezone
from typing import Optional

from mctp.core.interfaces import ExecutionInterface
from mctp.core.types import Symbol, PortfolioSnapshot
from mctp.core.enums import ExecutionResult, Side, OrderType, CommissionAsset
from mctp.core.order import Order, Fill
from mctp.core.constants import DEFAULT_FEE_RATE, T_CANCEL
from mctp.core.exceptions import MCTPError
from mctp.execution.lifecycle import OrderRecord, TERMINAL_STATES
from mctp.execution.oco import OCOOrder, OCOStatus, OCOTriggerResult, TERMINAL_OCO_STATUSES
from mctp.storage.snapshot_store import SnapshotStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.order_store import OrderStore


class SpotPaperExecutor(ExecutionInterface):
    """
    Бумажный исполнитель для спота.
    - MARKET: немедленный филл по текущей цене
    - LIMIT: ожидает достижения цены через set_price()
    - OCO: симулируется локально; проверяется при каждом set_price()
    - Балансы и OrderRecord хранятся в памяти
    """

    def __init__(
        self,
        initial_balances: dict[str, Decimal],
        snapshot_store: Optional[SnapshotStore] = None,
        balance_cache_store: Optional[BalanceCacheStore] = None,
        order_store: Optional[OrderStore] = None,
    ):
        for asset, balance in initial_balances.items():
            if not isinstance(balance, Decimal):
                raise AssertionError(f"Balance for {asset} must be Decimal")
        self._balances: dict[str, Decimal]    = dict(initial_balances)
        self._locked:   dict[str, Decimal]    = {}
        self._open_orders: dict[str, Order]   = {}
        self._current_prices: dict[str, Decimal] = {}
        self._instrument_info: dict[str, dict]   = {}
        self._order_records: dict[str, OrderRecord] = {}
        self._active_ocos:   dict[str, OCOOrder]    = {}
        self._snapshot_store:     Optional[SnapshotStore]     = snapshot_store
        self._balance_cache_store: Optional[BalanceCacheStore] = balance_cache_store
        self._order_store:        Optional[OrderStore]        = order_store
        self._event_time: Optional[datetime] = None

    # ── Управление состоянием (только paper) ─────────────────────────────────

    def set_price(self, symbol: Symbol, price: Decimal) -> list[OCOTriggerResult]:
        """Установить цену. Проверяет LIMIT-ордера и OCO. Возвращает OCO-результаты."""
        if not isinstance(price, Decimal):
            raise AssertionError("Price must be Decimal")
        symbol_str = symbol.to_exchange_str()
        self._current_prices[symbol_str] = price
        self._check_limit_orders(symbol_str, price, symbol)
        return self._check_ocos(symbol_str, price, symbol)

    def set_instrument_info(self, symbol: Symbol, info: dict) -> None:
        self._instrument_info[symbol.to_exchange_str()] = info

    def set_event_time(self, event_time: datetime) -> None:
        if event_time.tzinfo is None:
            raise ValueError("event_time must be UTC-aware")
        self._event_time = event_time

    # ── ExecutionInterface ───────────────────────────────────────────────────

    async def submit_order(self, order: Order) -> ExecutionResult:
        record = OrderRecord(client_order_id=order.client_order_id)
        self._order_records[order.client_order_id] = record

        symbol_str = order.symbol.to_exchange_str()

        if order.order_type == OrderType.MARKET:
            price = self._current_prices.get(symbol_str)
            if price is None:
                record.mark_rejected()
                return ExecutionResult.REJECTED
            fill = self._make_fill(order, price)
            record.apply_fill(fill)
            return ExecutionResult.FILLED

        if order.order_type == OrderType.LIMIT:
            self._open_orders[order.client_order_id] = order
            if order.side == Side.BUY:
                cost = order.quantity * (order.price or Decimal("0"))
                self._lock_balance(order.symbol.quote, cost)
            else:
                self._lock_balance(order.symbol.base, order.quantity)
            record.apply_result(ExecutionResult.ACCEPTED)
            if self._order_store is not None:
                self._order_store.save_order(order)
            return ExecutionResult.ACCEPTED

        record.mark_rejected()
        return ExecutionResult.REJECTED

    async def cancel_order(self, client_order_id: str) -> ExecutionResult:
        async def _do() -> ExecutionResult:
            if client_order_id not in self._open_orders:
                return ExecutionResult.REJECTED
            order = self._open_orders.pop(client_order_id)
            if order.side == Side.BUY:
                cost = order.quantity * (order.price or Decimal("0"))
                self._unlock_balance(order.symbol.quote, cost)
            else:
                self._unlock_balance(order.symbol.base, order.quantity)
            rec = self._order_records.get(client_order_id)
            if rec and not rec.is_terminal:
                rec.mark_cancelled()
            return ExecutionResult.CANCELLED

        try:
            result = await asyncio.wait_for(_do(), timeout=float(T_CANCEL))
        except asyncio.TimeoutError:
            return ExecutionResult.REJECTED
        if result == ExecutionResult.CANCELLED and self._order_store is not None:
            self._order_store.remove_order(client_order_id)
        return result

    async def get_balances(self) -> dict[str, Decimal]:
        result = {}
        for asset, total in self._balances.items():
            locked = self._locked.get(asset, Decimal("0"))
            result[asset] = total - locked
        return result

    async def get_instrument_info(self, symbol: Symbol) -> dict:
        return self._instrument_info.get(symbol.to_exchange_str(), {})

    async def get_fills(self, client_order_id: str) -> list[Fill]:
        rec = self._order_records.get(client_order_id)
        return list(rec.fills) if rec else []

    # ── Persistence ──────────────────────────────────────────────────────────

    async def persist_fill_state(self, snapshot: PortfolioSnapshot) -> None:
        """
        Вызывается оркестратором после каждого CostBasisUpdater.apply_fill().
        Сохраняет snapshot и текущий кеш балансов если хранилища заданы.
        """
        if self._snapshot_store is not None:
            self._snapshot_store.save(snapshot)
        if self._balance_cache_store is not None:
            self._balance_cache_store.save(
                await self.get_balances(),
                self._now(),
            )

    def restore_from_storage(self) -> Optional[PortfolioSnapshot]:
        """
        Восстановить последний snapshot из хранилища.
        Возвращает None если store не задан или файл не существует.
        """
        if self._snapshot_store is None:
            return None
        if self._snapshot_store.exists():
            return self._snapshot_store.load()
        return None

    # ── OCO public API ───────────────────────────────────────────────────────

    def submit_oco(self, oco: OCOOrder) -> str:
        if oco.is_terminal:
            raise MCTPError("Cannot submit a terminal OCO order")
        self._active_ocos[oco.list_order_id] = oco
        if self._order_store is not None:
            self._order_store.save_oco(oco)
        return oco.list_order_id

    async def cancel_oco(self, list_order_id: str) -> ExecutionResult:
        async def _do() -> ExecutionResult:
            oco = self._active_ocos.get(list_order_id)
            if oco is None:
                return ExecutionResult.REJECTED
            if oco.is_terminal:
                return ExecutionResult.CANCELLED   # noop
            oco.status     = OCOStatus.CANCELLED
            oco.updated_at = self._now()
            return ExecutionResult.CANCELLED

        try:
            result = await asyncio.wait_for(_do(), timeout=float(T_CANCEL))
        except asyncio.TimeoutError:
            return ExecutionResult.REJECTED
        if result == ExecutionResult.CANCELLED and self._order_store is not None:
            self._order_store.remove_oco(list_order_id)
        return result

    def check_oco_status(self, list_order_id: str) -> OCOStatus:
        oco = self._active_ocos.get(list_order_id)
        if oco is None:
            return OCOStatus.CANCELLED
        return oco.check_status()

    def get_oco(self, list_order_id: str) -> Optional[OCOOrder]:
        """Завершённые OCO остаются видимыми (не удаляются из _active_ocos)."""
        return self._active_ocos.get(list_order_id)

    def simulate_partial_tp_fill(
        self, list_order_id: str, partial_qty: Decimal
    ) -> Optional[Fill]:
        """
        Paper-only. Частично исполнить TP-плечо.
        partial_qty должен быть > 0 и < remaining_qty.
        """
        oco = self._active_ocos.get(list_order_id)
        if oco is None or oco.is_terminal:
            return None
        if partial_qty <= Decimal("0") or partial_qty >= oco.remaining_qty:
            return None
        fill = self._make_sell_fill(oco.symbol, partial_qty, oco.tp_price, is_partial=True)
        oco.tp_fills.append(fill)
        oco.status     = OCOStatus.TP_PARTIAL
        oco.updated_at = self._now()
        return fill

    def get_all_oco_fills(self, list_order_id: str) -> list[Fill]:
        oco = self._active_ocos.get(list_order_id)
        return oco.all_fills if oco else []

    # ── Внутренние методы ────────────────────────────────────────────────────

    def _make_fill(self, order: Order, fill_price: Decimal) -> Fill:
        """Создать Fill и обновить балансы для Order (BUY или SELL)."""
        if order.side == Side.BUY:
            base_qty  = order.quantity
            quote_qty = base_qty * fill_price
            commission = quote_qty * DEFAULT_FEE_RATE
            fill = Fill(
                order_id=order.client_order_id,
                symbol=order.symbol,
                side=order.side,
                base_qty_filled=base_qty,
                quote_qty_filled=quote_qty,
                fill_price=fill_price,
                commission=commission,
                commission_asset=CommissionAsset.QUOTE,
                filled_at=self._event_time or order.created_at,
            )
            self._balances[order.symbol.base] = (
                self._balances.get(order.symbol.base, Decimal("0")) + base_qty
            )
            self._balances[order.symbol.quote] = (
                self._balances.get(order.symbol.quote, Decimal("0"))
                - quote_qty - commission
            )
        else:
            fill = self._make_sell_fill(
                order.symbol, order.quantity, fill_price,
                order_id=order.client_order_id,
                filled_at=self._event_time or order.created_at,
            )
        return fill

    def _make_sell_fill(
        self,
        symbol: Symbol,
        qty: Decimal,
        price: Decimal,
        is_partial: bool = False,
        order_id: Optional[str] = None,
        filled_at: Optional[datetime] = None,
    ) -> Fill:
        """Создать SELL Fill с QUOTE комиссией 0.1% и обновить балансы."""
        quote_qty  = qty * price
        commission = quote_qty * DEFAULT_FEE_RATE
        fill = Fill(
            order_id=order_id or str(uuid.uuid4()),
            symbol=symbol,
            side=Side.SELL,
            base_qty_filled=qty,
            quote_qty_filled=quote_qty,
            fill_price=price,
            commission=commission,
            commission_asset=CommissionAsset.QUOTE,
            is_partial=is_partial,
            filled_at=filled_at or self._now(),
        )
        self._balances[symbol.base] = (
            self._balances.get(symbol.base, Decimal("0")) - qty
        )
        self._balances[symbol.quote] = (
            self._balances.get(symbol.quote, Decimal("0")) + quote_qty - commission
        )
        return fill

    def _check_limit_orders(
        self, symbol_str: str, price: Decimal, symbol: Symbol
    ) -> None:
        """Исполнить LIMIT-ордера, чья цена достигнута."""
        to_fill = []
        for coid, order in self._open_orders.items():
            if order.symbol.to_exchange_str() != symbol_str:
                continue
            if order.side == Side.BUY and price <= (order.price or Decimal("0")):
                to_fill.append(coid)
            elif order.side == Side.SELL and price >= (order.price or Decimal("0")):
                to_fill.append(coid)

        for coid in to_fill:
            order = self._open_orders.pop(coid)
            # Разблокировать резерв
            if order.side == Side.BUY:
                cost = order.quantity * (order.price or Decimal("0"))
                self._unlock_balance(order.symbol.quote, cost)
            else:
                self._unlock_balance(order.symbol.base, order.quantity)
            # Исполнить по цене ордера, не по триггерной цене (Минор 2)
            fill = self._make_fill(order, order.price or price)
            rec  = self._order_records.get(coid)
            if rec and not rec.is_terminal:
                rec.apply_fill(fill)

    def _check_ocos(
        self, symbol_str: str, price: Decimal, symbol: Symbol
    ) -> list[OCOTriggerResult]:
        """
        Проверить активные OCO. TP приоритет если оба условия выполняются.
        Завершённые OCO остаются в _active_ocos.
        """
        results: list[OCOTriggerResult] = []

        for oco in self._active_ocos.values():
            if oco.symbol.to_exchange_str() != symbol_str:
                continue
            if oco.is_terminal:
                continue

            tp_hit = price >= oco.tp_price
            sl_hit = price <= oco.sl_stop_price

            if tp_hit:
                qty = oco.remaining_qty
                if qty > Decimal("0"):
                    fill = self._make_sell_fill(symbol, qty, oco.tp_price)
                    oco.tp_fills.append(fill)
                    oco.status     = OCOStatus.TP_FILLED
                    oco.updated_at = self._now()
                    if self._order_store is not None:
                        self._order_store.remove_oco(oco.list_order_id)
                    results.append(OCOTriggerResult(
                        triggered_leg="TP",
                        new_fills=[fill],
                        cancelled_leg="SL",
                        final_status=OCOStatus.TP_FILLED,
                        resolved=True,
                    ))

            elif sl_hit:
                qty = oco.remaining_qty
                if qty > Decimal("0"):
                    fill = self._make_sell_fill(symbol, qty, oco.sl_limit_price)
                    oco.sl_fills.append(fill)
                    if oco.tp_filled_qty > Decimal("0"):
                        oco.status = OCOStatus.PARTIAL_TP_THEN_SL
                    else:
                        oco.status = OCOStatus.SL_TRIGGERED
                    oco.updated_at = self._now()
                    if self._order_store is not None:
                        self._order_store.remove_oco(oco.list_order_id)
                    results.append(OCOTriggerResult(
                        triggered_leg="SL",
                        new_fills=[fill],
                        cancelled_leg="TP",
                        final_status=oco.status,
                        resolved=True,
                    ))

        return results

    def _lock_balance(self, asset: str, amount: Decimal) -> None:
        self._locked[asset] = self._locked.get(asset, Decimal("0")) + amount

    def _unlock_balance(self, asset: str, amount: Decimal) -> None:
        current = self._locked.get(asset, Decimal("0"))
        self._locked[asset] = max(Decimal("0"), current - amount)

    def _now(self) -> datetime:
        return self._event_time or datetime.now(timezone.utc)
