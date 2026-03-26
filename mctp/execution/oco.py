"""
OCO (One-Cancels-the-Other) симуляция для paper режима.
Реальный OCO API — в v1.0 (SpotLiveExecutor).
Контракт 17: TP исполняется как LIMIT, SL — по sl_limit_price.
"""
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from uuid import uuid4

from mctp.core.types import Symbol
from mctp.core.order import Fill
from mctp.core.constants import SL_EXECUTION_BUFFER


class OCOStatus(Enum):
    ACTIVE              = "ACTIVE"
    TP_PARTIAL          = "TP_PARTIAL"
    TP_FILLED           = "TP_FILLED"
    SL_TRIGGERED        = "SL_TRIGGERED"
    PARTIAL_TP_THEN_SL  = "PARTIAL_TP_THEN_SL"
    CANCELLED           = "CANCELLED"


TERMINAL_OCO_STATUSES = frozenset({
    OCOStatus.TP_FILLED,
    OCOStatus.SL_TRIGGERED,
    OCOStatus.PARTIAL_TP_THEN_SL,
    OCOStatus.CANCELLED,
})


@dataclass
class OCOOrder:
    symbol: Symbol
    tp_price: Decimal
    sl_stop_price: Decimal
    sl_limit_price: Decimal       # = sl_stop_price × (1 − SL_EXECUTION_BUFFER)
    quantity: Decimal
    tp_client_order_id: Optional[str] = None
    sl_client_order_id: Optional[str] = None
    list_order_id: str   = field(default_factory=lambda: str(uuid4()))
    status: OCOStatus    = OCOStatus.ACTIVE
    tp_fills: list       = field(default_factory=list)
    sl_fills: list       = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self):
        if self.created_at.tzinfo is None:
            raise ValueError("OCOOrder.created_at must be UTC-aware")
        if self.updated_at.tzinfo is None:
            raise ValueError("OCOOrder.updated_at must be UTC-aware")
        for name, val in [
            ("tp_price",       self.tp_price),
            ("sl_stop_price",  self.sl_stop_price),
            ("sl_limit_price", self.sl_limit_price),
            ("quantity",       self.quantity),
        ]:
            if not isinstance(val, Decimal):
                raise AssertionError(f"OCOOrder.{name} must be Decimal")
            if val <= Decimal("0"):
                raise ValueError(f"OCOOrder.{name} must be > 0, got {val}")
        if self.sl_limit_price >= self.sl_stop_price:
            raise ValueError(
                f"sl_limit_price {self.sl_limit_price} must be < sl_stop_price {self.sl_stop_price}"
            )

    # ── Properties ───────────────────────────────────────────────────────────

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_OCO_STATUSES

    @property
    def tp_filled_qty(self) -> Decimal:
        return sum((f.base_qty_filled for f in self.tp_fills), Decimal("0"))

    @property
    def sl_filled_qty(self) -> Decimal:
        return sum((f.base_qty_filled for f in self.sl_fills), Decimal("0"))

    @property
    def remaining_qty(self) -> Decimal:
        return max(Decimal("0"), self.quantity - self.tp_filled_qty - self.sl_filled_qty)

    @property
    def all_fills(self) -> list:
        return self.tp_fills + self.sl_fills

    def check_status(self) -> OCOStatus:
        """Вернуть текущий статус. Вызывать перед отменой плеча."""
        return self.status


@dataclass
class OCOTriggerResult:
    triggered_leg: Optional[str]              # "TP" | "SL" | None
    new_fills: list       = field(default_factory=list)
    cancelled_leg: Optional[str] = None       # "SL" | "TP" | None
    final_status: OCOStatus = OCOStatus.ACTIVE
    resolved: bool = False
