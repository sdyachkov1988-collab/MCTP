"""
OrderTracker — управление активными ордерами в памяти (v0.7).
Разделяет обычные ордера и GTC scale-in ордера.
"""
from typing import Optional

from mctp.core.order import Order


class OrderTracker:
    """
    GTC scale-in ордер — LIMIT с TimeInForce.GTC для добавления к позиции.
    Определяется флагом is_scale_in при регистрации.
    """

    def __init__(self) -> None:
        self._orders:     dict[str, Order] = {}   # обычные
        self._scale_ins:  dict[str, Order] = {}   # GTC scale-in

    # ── Public API ────────────────────────────────────────────────────────────

    def register(self, order: Order, is_scale_in: bool = False) -> None:
        """Зарегистрировать ордер. GTC scale-in сохраняется отдельно."""
        if is_scale_in:
            self._scale_ins[order.client_order_id] = order
        else:
            self._orders[order.client_order_id] = order

    def unregister(self, client_order_id: str) -> None:
        """Убрать ордер из трекера (обычный или scale-in). Молча если не найден."""
        self._orders.pop(client_order_id, None)
        self._scale_ins.pop(client_order_id, None)

    def get_order(self, client_order_id: str) -> Optional[Order]:
        """Найти ордер в обоих хранилищах."""
        return self._orders.get(client_order_id) or self._scale_ins.get(client_order_id)

    def is_scale_in(self, client_order_id: str) -> bool:
        """True если ордер зарегистрирован как scale-in."""
        return client_order_id in self._scale_ins

    def active_orders(self) -> list[Order]:
        """Все обычные (не scale-in) активные ордера."""
        return list(self._orders.values())

    def active_scale_ins(self) -> list[Order]:
        """Все активные GTC scale-in ордера."""
        return list(self._scale_ins.values())

    def all_active(self) -> list[Order]:
        """Все активные ордера (обычные + scale-in)."""
        return list(self._orders.values()) + list(self._scale_ins.values())

    def count(self) -> int:
        """Суммарное количество активных ордеров."""
        return len(self._orders) + len(self._scale_ins)
